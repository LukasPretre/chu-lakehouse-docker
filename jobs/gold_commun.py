"""
Outils communs aux jobs de dimensions du Gold (J23, J24, J25). Ce n'est pas un job : il est
importé par les jobs (import gold_commun), qui sont dans le même dossier /jobs.

charger_dimension() applique la règle du Livrable 1 (annexe B.3) : SCD1, et des clés de
substitution qui ne sont jamais renumérotées, pour que les clés déjà stockées dans les faits
restent justes d'un chargement à l'autre.
"""
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window, functions as F

GOLD_DB = os.environ.get("GOLD_DB", "chu_gold")
SILVER = "hdfs://namenode:8020/silver"
NON_RENSEIGNE = "Non renseigné"   # Livrable 1, §6.2 : attribut descriptif vide


def session(nom):
    spark = (SparkSession.builder.appName(nom).enableHiveSupport()
             .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
             .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
             .config("spark.hadoop.dfs.replication", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    return spark


def lire_silver(spark, entite, colonnes):
    """Lit une table Silver et s'arrête net si une colonne attendue manque."""
    df = spark.read.parquet(f"{SILVER}/{entite}")
    manquantes = [c for c in colonnes if c not in df.columns]
    if manquantes:
        raise SystemExit(f"GOLD ERREUR silver/{entite} : colonne(s) absente(s) {manquantes} ; "
                         f"colonnes trouvées : {df.columns}")
    return df


def texte(colonne, defaut=None, vides=()):
    """Texte sans espaces superflus ; vide (ou l'une des valeurs `vides`) -> defaut, NULL sinon."""
    v = F.trim(F.col(colonne))
    v = F.when(v.isin("", *vides), F.lit(None)).otherwise(v)
    return F.coalesce(v, F.lit(defaut)) if defaut is not None else v


def charger_dimension(spark, table, cle, naturelle, source, inconnu, longueurs, job):
    """
    Charge <GOLD_DB>.<table> depuis `source` (clé naturelle + attributs, noms du DDL) :
      - clé naturelle déjà connue : même clé, attributs mis à jour (SCD1) ;
      - nouvelle clé naturelle : plus grande clé existante + 1, + 2... (dans l'ordre de la clé naturelle) ;
      - clé naturelle disparue du Silver : ligne conservée telle quelle (des faits peuvent y renvoyer) ;
      - ligne -1 « Inconnu » (`inconnu`, dans l'ordre : clé, clé naturelle, attributs).
    La nouvelle version est d'abord écrite dans <table>_nouveau et contrôlée ; elle ne remplace
    la table que si tous les contrôles passent (sinon le Gold précédent est conservé, L1 §6.3).
    `longueurs` : taille maximale des colonnes texte dans PostgreSQL, sinon J34 échouerait.
    Renvoie True si tout est conforme.
    """
    run_id = os.environ.get("RUN_ID") or f"{job}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    cible, tampon = f"{GOLD_DB}.{table}", f"{GOLD_DB}.{table}_nouveau"
    attributs = [c for c in source.columns if c != naturelle]
    colonnes = [cle, naturelle] + attributs
    source = source.filter(F.col(naturelle).isNotNull()).dropDuplicates([naturelle])
    spark.sql(f"DROP TABLE IF EXISTS {tampon}")

    if spark.catalog.tableExists(cible):
        ancienne = spark.table(cible).filter(F.col(cle) != -1)
        max_cle = ancienne.agg(F.max(cle)).first()[0] or 0
        jointe = source.join(ancienne.select(naturelle, cle), naturelle, "left")
        conservees = (ancienne.join(source.select(naturelle), naturelle, "left_anti")
                      .select(*colonnes, "run_id", "date_chargement")
                      .withColumn("_origine", F.lit("conservee")))
    else:
        ancienne, max_cle, conservees = None, 0, None
        jointe = source.withColumn(cle, F.lit(None).cast("int"))

    connues = jointe.filter(F.col(cle).isNotNull()).withColumn("_origine", F.lit("mise_a_jour"))
    nouvelles = (jointe.filter(F.col(cle).isNull()).drop(cle)
                 .withColumn(cle, (F.lit(max_cle) + F.row_number().over(Window.orderBy(naturelle))).cast("int"))
                 .withColumn("_origine", F.lit("nouvelle")))
    lignes = connues.select(*colonnes, "_origine").unionByName(nouvelles.select(*colonnes, "_origine"))
    lignes = lignes.unionByName(spark.createDataFrame([inconnu], lignes.select(*colonnes).schema)
                                .withColumn("_origine", F.lit("inconnu")))
    lignes = (lignes.withColumn("run_id", F.lit(run_id))
              .withColumn("date_chargement", F.current_timestamp()))
    if conservees is not None:
        lignes = lignes.unionByName(conservees)
    lignes.write.mode("overwrite").format("parquet").saveAsTable(tampon)

    # ------------------------------------------------------------ contrôles sur la nouvelle version
    t = spark.table(tampon)
    origine = {r["_origine"]: r["count"] for r in t.groupBy("_origine").count().collect()}
    reelles = t.filter(F.col(cle) != -1)
    controles = {
        "doublons_cle": t.count() - t.select(cle).distinct().count(),
        "doublons_cle_naturelle": reelles.count() - reelles.select(naturelle).distinct().count(),
        "lignes_inconnu": t.filter(F.col(cle) == -1).count(),
        "cles_modifiees": 0 if ancienne is None else (
            ancienne.select(naturelle, F.col(cle).alias("ancienne"))
            .join(t.select(naturelle, cle), naturelle).filter(F.col(cle) != F.col("ancienne")).count()),
    }
    attendu = {"doublons_cle": 0, "doublons_cle_naturelle": 0, "lignes_inconnu": 1, "cles_modifiees": 0}
    for colonne, taille in longueurs.items():
        controles[f"trop_long_{colonne}"] = t.filter(F.length(colonne) > taille).count()
        attendu[f"trop_long_{colonne}"] = 0

    ok = True
    for nom, valeur in controles.items():
        statut = "OK" if valeur == attendu[nom] else f"ECART (attendu {attendu[nom]})"
        ok = ok and statut == "OK"
        print(f"GOLD {table} {nom:32s} = {valeur:>8}  {statut}", flush=True)

    resume = (f"nouvelles={origine.get('nouvelle', 0)} mises_a_jour={origine.get('mise_a_jour', 0)} "
              f"conservees={origine.get('conservee', 0)}")
    if ok:
        t.drop("_origine").write.mode("overwrite").format("parquet").saveAsTable(cible)
        n = spark.table(cible).count()
        print(f"GOLD {table} table={cible} lignes={n} {resume} run_id={run_id}", flush=True)
    else:
        print(f"GOLD ERREUR {table} : écart(s) ci-dessus, {cible} n'est PAS remplacée ({resume})", flush=True)
    spark.sql(f"DROP TABLE IF EXISTS {tampon}")
    return ok