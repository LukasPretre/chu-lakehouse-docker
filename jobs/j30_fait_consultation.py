"""
J30 - Gold : fait_consultation (Hive, base chu_gold).

Entrée  : Silver hdfs://namenode:8020/silver/consultation
          + dimensions Hive chu_gold.dim_patient, dim_professionnel, dim_diagnostic,
            dim_temps, dim_etablissement (membre 2, J20 à J25 et J22)
Sortie  : table Hive chu_gold.fait_consultation, Parquet,
          partitionnée par annee, 16 buckets sur id_patient (choix validé par le benchmark).

Règles
- Une ligne Silver = une ligne de fait : aucune ligne n'est perdue. Une clé introuvable
  dans une dimension devient -1 (« Inconnu »), et elle est comptée.
- id_date = AAAAMMJJ (entier), comme la contrainte ck_temps_cle du DDL.
- duree_minutes vient du Silver (déjà contrôlée : fin - début, + 1440 au passage de minuit).
- heure_debut / heure_fin sont stockées en texte HH:mm:ss (Hive n'a pas de type TIME) ;
  J34 les convertit en TIME pour PostgreSQL.
- La colonne de partition annee n'existe pas dans le DDL PostgreSQL : J34 ne la copie pas.
- id_etablissement (règle Q2, décision du groupe, le Livrable 1 ne précisant pas le choix) :
  organisations du professionnel dans le Silver activité (J15) ;
    une seule -> celle-là ;
    plusieurs -> mode d'exercice Salarié, puis Libéral, puis Bénévole ; puis établissement
                 de santé (identifiant en F, avec FINESS) avant cabinet (C, R) ; puis plus petit
                 identifiant (choix arbitraire mais stable) ;
    aucune     -> -1 (« Inconnu »).
  L'organisation donne ensuite la version SCD2 de dim_etablissement valable à la date de la
  consultation, à défaut la version courante. Si dim_etablissement n'existe pas encore (J22),
  id_etablissement vaut -1 et le job continue (contrôle « q2_dim_etablissement_absente »).
- Rechargement complet à chaque exécution (rejouable) : le fait est recalculé à partir du
  Silver et des dimensions courantes, dont les clés sont stables.

Modes
- J30_MODE=normal (défaut) : lit les dimensions dans chu_gold ; s'arrête si l'une manque.
- J30_MODE=test : construit des dimensions provisoires en mémoire à partir du Silver,
  écrit dans chu_test.fait_consultation. Sert à valider le job avant les vraies dimensions.

Sortie console : lignes « GOLDQC,indicateur,valeur » puis « GOLD fait_consultation ... ».
"""
import os
import sys
import time
from datetime import datetime, timezone

_T = [time.time()]


def chrono(etape):
    """Durée de chaque étape (secondes depuis l'étape précédente), pour trouver le goulot."""
    maintenant = time.time()
    print(f"GOLDTEMPS,{etape},{maintenant - _T[0]:.1f}", flush=True)
    _T[0] = maintenant

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

MODE = os.environ.get("J30_MODE", "normal")
BASE_DIM = "chu_gold"
BASE_SORTIE = "chu_test" if MODE == "test" else "chu_gold"
SILVER = "hdfs://namenode:8020/silver"
NB_BUCKETS = 16
RUN_ID = os.environ.get("RUN_ID", "J30_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))

spark = (SparkSession.builder.appName("j30_fait_consultation").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC")
         # 8 partitions de redistribution au lieu de 200 par défaut : avec 2 cœurs et 1 million de
         # lignes, 200 petites tâches par étape coûtent plus que le calcul. Mesuré sur la VM le
         # 05/11 : 112 s -> 77 s (jointures 62 s -> 36 s). À augmenter si le volume grandit.
         .config("spark.sql.shuffle.partitions", os.environ.get("J30_PARTITIONS", "8"))
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
chrono("demarrage_session")


def qc(indicateur, valeur):
    print(f"GOLDQC,{indicateur},{valeur}", flush=True)


def stop(message):
    print(f"GOLD ERREUR {message}", flush=True)
    spark.stop()
    sys.exit(1)


def table_existe(base, table):
    return spark.catalog.tableExists(f"{base}.{table}")


def verifier_unicite(df, cle, nom):
    """Une clé naturelle dupliquée dans une dimension dupliquerait les lignes du fait."""
    doublons = df.groupBy(cle).count().filter("count > 1").count()
    qc(f"{nom}_doublons_cle_naturelle", doublons)
    if doublons:
        stop(f"{nom} : {doublons} clé(s) naturelle(s) en double ({cle}) : corriger la dimension")


# ---------------------------------------------------------------- dimensions
def dim_provisoire(chemin, cle, id_nom):
    """Mode test : clé de substitution provisoire 1..n, sans ligne -1 (le coalesce la remplace).
    Numérotation distribuée (zipWithIndex) : un row_number sans partition regrouperait tout
    sur un seul exécuteur."""
    df = spark.read.parquet(f"{SILVER}/{chemin}").select(cle).distinct()
    return (df.rdd.zipWithIndex().map(lambda t: (t[0][0], t[1] + 1))
              .toDF(f"{cle} string, {id_nom} long").withColumn(id_nom, F.col(id_nom).cast("int")))


if MODE == "test":
    dim_patient = dim_provisoire("patient", "patient_pseudo", "id_patient")
    dim_prof = (dim_provisoire("professionnel", "professionnel_pseudo", "id_professionnel")
                .withColumnRenamed("professionnel_pseudo", "identifiant_pseudo"))
    dim_diag = dim_provisoire("diagnostic", "code_diagnostic", "id_diagnostic")
    dates_connues = None
    # établissement provisoire : une version unique, valable depuis toujours
    dim_etab = (dim_provisoire("etablissement", "identifiant_organisation", "id_etablissement")
                .withColumn("date_debut", F.lit(None).cast("date"))
                .withColumn("date_fin", F.lit(None).cast("date"))
                .withColumn("est_courant", F.lit(True)))
elif MODE == "normal":
    manquantes = [t for t in ("dim_patient", "dim_professionnel", "dim_diagnostic", "dim_temps")
                  if not table_existe(BASE_DIM, t)]
    if manquantes:
        stop(f"dimension(s) absente(s) dans {BASE_DIM} : {', '.join(manquantes)} "
             f"(J20 à J25 de la membre 2). Pour tester avant : J30_MODE=test")
    dim_patient = spark.table(f"{BASE_DIM}.dim_patient").select("patient_pseudo", "id_patient")
    dim_prof = spark.table(f"{BASE_DIM}.dim_professionnel").select("identifiant_pseudo", "id_professionnel")
    dim_diag = spark.table(f"{BASE_DIM}.dim_diagnostic").select("code_diagnostic", "id_diagnostic")
    dates_connues = (spark.table(f"{BASE_DIM}.dim_temps").select("id_date")
                     .filter("id_date <> -1").withColumn("date_connue", F.lit(True)))
    dim_etab = (spark.table(f"{BASE_DIM}.dim_etablissement")
                .select("identifiant_organisation", "id_etablissement", "date_debut", "date_fin", "est_courant")
                if table_existe(BASE_DIM, "dim_etablissement") else None)
else:
    stop(f"J30_MODE inconnu : {MODE} (normal ou test)")

# Les lignes -1 ne servent pas à la jointure (elles n'ont pas de clé naturelle réelle).
dim_patient = dim_patient.filter("id_patient <> -1")
dim_prof = dim_prof.filter("id_professionnel <> -1")
dim_diag = dim_diag.filter("id_diagnostic <> -1")
# Unicité des clés naturelles : garantie par J23 à J25 (gold_commun.py ne remplace une dimension
# que si elle n'a aucun doublon). Revérifiée seulement à la demande (J30_VERIF_DIMS=1), car
# coûteuse (un regroupement par dimension) et l'objectif est de 2 minutes par job.
if os.environ.get("J30_VERIF_DIMS") == "1":
    verifier_unicite(dim_patient, "patient_pseudo", "dim_patient")
    verifier_unicite(dim_prof, "identifiant_pseudo", "dim_professionnel")
    verifier_unicite(dim_diag, "code_diagnostic", "dim_diagnostic")

# ---------------------------------------------------------------- Silver
silver = spark.read.parquet(f"{SILVER}/consultation")
types = dict(silver.dtypes)


def heure_texte(col):
    """HH:mm:ss en texte, que le Silver stocke l'heure en timestamp ou en texte."""
    if types.get(col) == "timestamp":
        return F.date_format(F.col(col), "HH:mm:ss")
    return F.col(col).cast("string")


base = silver.select(
    F.col("id_consultation").cast("long").alias("id_consultation_long"),
    F.coalesce(F.date_format("date_consultation", "yyyyMMdd").cast("int"), F.lit(-1)).alias("id_date"),
    "patient_pseudo", "professionnel_pseudo", "code_diagnostic",
    heure_texte("heure_debut").alias("heure_debut"),
    heure_texte("heure_fin").alias("heure_fin"),
    F.col("duree_minutes").cast("short").alias("duree_minutes"),
    F.col("date_consultation").cast("date").alias("dt"),
)

# Volume du Silver et id_consultation INTEGER dans le DDL (aucun débordement) : un seul passage.
st = base.agg(F.count(F.lit(1)).alias("n"),
              F.sum(F.when(F.col("id_consultation_long") > 2147483647, 1).otherwise(0)).alias("trop")).first()
nb_silver, trop_grands = st["n"], st["trop"] or 0
qc("lignes_silver", nb_silver)
qc("id_consultation_hors_integer", trop_grands)
chrono("lecture_silver")
if trop_grands:
    stop(f"{trop_grands} id_consultation dépassent la capacité d'un INTEGER")

# ---------------------------------------------------------------- établissement (règle Q2)
def organisation_par_professionnel():
    """Une organisation par professionnel selon la règle Q2, avec le nombre d'organisations."""
    try:
        act = spark.read.parquet(f"{SILVER}/activite_professionnel")
    except Exception:
        qc("q2_activite_absente", 1)
        return None
    act = act.filter(F.col("identifiant_organisation").isNotNull())
    priorite_mode = (F.when(F.col("mode_exercice") == "Salarié", 1).when(F.col("mode_exercice") == "Libéral", 2)
                      .when(F.col("mode_exercice") == "Bénévole", 3).otherwise(4))
    priorite_type = F.when(F.col("identifiant_organisation").startswith("F"), 0).otherwise(1)
    # un seul regroupement : le minimum d'une structure ordonnée (mode, type, identifiant)
    # donne l'organisation prioritaire ; le même passage compte les organisations.
    return (act.groupBy("professionnel_pseudo")
               .agg(F.min(F.struct(priorite_mode.alias("m"), priorite_type.alias("t"),
                                   F.col("identifiant_organisation").alias("o"))).alias("choix"),
                    F.countDistinct("identifiant_organisation").alias("q2_nb_org"))
               .select("professionnel_pseudo", F.col("choix.o").alias("identifiant_organisation"), "q2_nb_org"))


org = organisation_par_professionnel()
if org is not None:
    base = base.join(F.broadcast(org), "professionnel_pseudo", "left")
else:
    base = (base.withColumn("identifiant_organisation", F.lit(None).cast("string"))
                .withColumn("q2_nb_org", F.lit(None).cast("long")))

if dim_etab is None:
    qc("q2_dim_etablissement_absente", 1)
    base = base.withColumn("id_etablissement", F.lit(None).cast("int"))
else:
    versions = dim_etab.filter("id_etablissement <> -1")
    # version valable à la date de la consultation, sinon version courante (SCD2)
    valable = ((F.col("date_debut").isNull() | (F.col("date_debut") <= F.col("dt")))
               & (F.col("date_fin").isNull() | (F.col("dt") <= F.col("date_fin"))))
    cand = (base.select("id_consultation_long", "dt", "identifiant_organisation")
                .filter("identifiant_organisation IS NOT NULL")
                .join(versions, "identifiant_organisation")
                .withColumn("ordre", F.when(valable, 0).when(F.col("est_courant"), 1).otherwise(2))
                .filter("ordre < 2"))
    w_v = Window.partitionBy("id_consultation_long").orderBy("ordre", F.desc("date_debut"))
    cand = (cand.withColumn("rn", F.row_number().over(w_v)).filter("rn = 1")
                .select("id_consultation_long", "id_etablissement"))
    base = base.join(cand, "id_consultation_long", "left")

# ---------------------------------------------------------------- clés
dim_prof = dim_prof.withColumnRenamed("identifiant_pseudo", "professionnel_pseudo")
fait = (base
        .join(F.broadcast(dim_diag), "code_diagnostic", "left")
        .join(dim_patient, "patient_pseudo", "left")
        .join(dim_prof, "professionnel_pseudo", "left"))

if dates_connues is not None:
    fait = fait.join(F.broadcast(dates_connues), "id_date", "left")
    fait = fait.withColumn("id_date", F.when(F.col("date_connue"), F.col("id_date")).otherwise(F.lit(-1)))

fait = fait.select(
    F.col("id_consultation_long").cast("int").alias("id_consultation"),
    F.col("id_date").cast("int").alias("id_date"),
    F.coalesce("id_patient", F.lit(-1)).cast("int").alias("id_patient"),
    F.coalesce("id_professionnel", F.lit(-1)).cast("int").alias("id_professionnel"),
    F.coalesce("id_diagnostic", F.lit(-1)).cast("int").alias("id_diagnostic"),
    F.coalesce("id_etablissement", F.lit(-1)).cast("int").alias("id_etablissement"),
    F.col("identifiant_organisation").alias("q2_org"),
    F.col("q2_nb_org"),
    "heure_debut", "heure_fin", "duree_minutes",
    F.lit(1).cast("short").alias("nb_consultations"),
    F.lit(RUN_ID).alias("run_id"),
    F.current_timestamp().alias("date_chargement"),
).withColumn("annee", F.when(F.col("id_date") > 0, (F.col("id_date") / 10000).cast("int")).otherwise(F.lit(-1)))

fait = fait.cache()

# ---------------------------------------------------------------- contrôles (un seul passage)
def nb_si(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


st = fait.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.countDistinct("id_consultation").alias("distincts"),
    *[nb_si(F.col(c) == -1).alias("inconnu_" + c)
      for c in ("id_date", "id_patient", "id_professionnel", "id_diagnostic", "id_etablissement")],
    nb_si(~F.col("duree_minutes").between(0, 1440)).alias("duree_hors"),
    nb_si(F.col("duree_minutes").isNull()).alias("duree_nulle"),
    nb_si(F.col("q2_nb_org").isNull()).alias("q2_aucune"),
    nb_si(F.col("q2_nb_org") == 1).alias("q2_une"),
    nb_si(F.col("q2_nb_org") > 1).alias("q2_plusieurs"),
    nb_si(F.col("q2_org").startswith("F")).alias("q2_en_F"),
    nb_si(F.col("q2_org").isNotNull() & (F.col("id_etablissement") == -1)).alias("q2_sans_dim"),
).first()
chrono("jointures_et_controles")
qc("q2_consultations_sans_organisation", st["q2_aucune"])
qc("q2_consultations_une_organisation", st["q2_une"])
qc("q2_consultations_plusieurs_organisations", st["q2_plusieurs"])
qc("q2_organisation_choisie_en_F", st["q2_en_F"])
qc("q2_organisation_absente_de_dim_etablissement", st["q2_sans_dim"])
nb_fait = st["lignes"]
qc("lignes_fait", nb_fait)
qc("perte_silver_fait", nb_silver - nb_fait)
qc("doublons_id_consultation", nb_fait - st["distincts"])
for cle in ("id_date", "id_patient", "id_professionnel", "id_diagnostic", "id_etablissement"):
    qc(f"inconnu_{cle}", st["inconnu_" + cle])
qc("duree_hors_0_1440", st["duree_hors"])
qc("duree_nulle_absente", st["duree_nulle"])
fait = fait.drop("q2_org", "q2_nb_org")

if nb_fait != nb_silver:
    stop(f"lignes perdues ou dupliquées : Silver={nb_silver}, fait={nb_fait}")

# ---------------------------------------------------------------- écriture
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BASE_SORTIE}")
(fait.repartition(NB_BUCKETS, "id_patient")
     .write.mode("overwrite").format("parquet")
     .partitionBy("annee")
     .bucketBy(NB_BUCKETS, "id_patient").sortBy("id_patient")
     .saveAsTable(f"{BASE_SORTIE}.fait_consultation"))
chrono("ecriture_hive")

relue = spark.table(f"{BASE_SORTIE}.fait_consultation")
par_annee = relue.groupBy("annee").count().orderBy("annee").collect()   # un seul passage
relu = sum(l["count"] for l in par_annee)
qc("lignes_relues_hive", relu)
chrono("relecture")
for ligne in par_annee:
    qc(f"lignes_annee_{ligne['annee']}", ligne["count"])

print(f"GOLD fait_consultation mode={MODE} table={BASE_SORTIE}.fait_consultation "
      f"lignes={relu} run_id={RUN_ID}", flush=True)
spark.stop()
