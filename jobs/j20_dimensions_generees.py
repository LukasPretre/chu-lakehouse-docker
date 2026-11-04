"""
J20 - Dimensions générées : DIM_TEMPS.

Source : aucune, le calendrier est généré.
Cible  : table Hive <GOLD_DB>.dim_temps, recopiée dans PostgreSQL (chu_gold_db) par J34.

Règles (Livrable 1, annexe A et B.3) :
  - un jour par ligne, du 01/01/1970 au 31/12/2030, plus la ligne -1 « Inconnu » ;
  - id_date au format AAAAMMJJ (2020-03-15 -> 20200315) ;
  - semaine ISO 8601, libellés des jours et des mois en français ;
  - est_ferie : les 11 jours fériés nationaux (Code du travail, art. L3133-1).
    Le 8 mai n'est de nouveau férié qu'à partir de 1982 (loi du 2 octobre 1981).
    Les jours propres à l'Alsace-Moselle (Vendredi saint, 26 décembre) ne sont pas comptés.

DIM_DEMOGRAPHIE, aussi rattachée à J20 dans le L1, n'est pas créée ici : elle n'est pas
dans le modèle logique (décision d'équipe en attente).

Lancement (racine du dépôt, sur la VM) :
  docker exec chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /jobs/j20_dimensions_generees.py
Variables facultatives : GOLD_DB (base Hive, défaut chu_gold), RUN_ID (fourni par Airflow).
"""
import os
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession, functions as F

GOLD_DB = os.environ.get("GOLD_DB", "chu_gold")
DEBUT, FIN = date(1970, 1, 1), date(2030, 12, 31)
RUN_ID = os.environ.get("RUN_ID") or "J20_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Dans l'ordre de dayofweek() de Spark : 1 = dimanche ... 7 = samedi
JOURS = ["Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"]
MOIS = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet",
        "Août", "Septembre", "Octobre", "Novembre", "Décembre"]


def paques(annee):
    """Dimanche de Pâques du calendrier grégorien (algorithme de Meeus, Jones et Butcher)."""
    a, b, c = annee % 19, annee // 100, annee % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    n = h + l - 7 * m + 114
    return date(annee, n // 31, n % 31 + 1)


def jours_feries(annee):
    """Ensemble des dates fériées d'une année (un ensemble : l'Ascension tombe parfois un 1er ou un 8 mai)."""
    p = paques(annee)
    feries = {
        date(annee, 1, 1),        # Jour de l'an
        p + timedelta(days=1),    # Lundi de Pâques
        date(annee, 5, 1),        # Fête du travail
        p + timedelta(days=39),   # Ascension
        p + timedelta(days=50),   # Lundi de Pentecôte
        date(annee, 7, 14),       # Fête nationale
        date(annee, 8, 15),       # Assomption
        date(annee, 11, 1),       # Toussaint
        date(annee, 11, 11),      # Armistice
        date(annee, 12, 25),      # Noël
    }
    if annee >= 1982:
        feries.add(date(annee, 5, 8))  # Victoire 1945
    return feries


def construire_dim_temps(spark):
    feries = sorted(d for a in range(DEBUT.year, FIN.year + 1) for d in jours_feries(a))
    df_feries = (spark.createDataFrame([(d,) for d in feries], "date_complete DATE")
                 .withColumn("est_ferie", F.lit(True)))

    jours = spark.sql(f"SELECT explode(sequence(DATE'{DEBUT}', DATE'{FIN}', INTERVAL 1 DAY)) AS date_complete")
    d = F.col("date_complete")
    noms_jours = F.array(*[F.lit(j) for j in JOURS])
    noms_mois = F.array(*[F.lit(m) for m in MOIS])

    calendrier = (jours.join(F.broadcast(df_feries), "date_complete", "left")
                  .select((F.year(d) * 10000 + F.month(d) * 100 + F.dayofmonth(d)).alias("id_date"),
                          d,
                          F.dayofmonth(d).cast("smallint").alias("jour"),
                          F.element_at(noms_jours, F.dayofweek(d)).alias("jour_semaine"),
                          F.weekofyear(d).cast("smallint").alias("semaine_iso"),   # semaine ISO 8601
                          F.month(d).cast("smallint").alias("mois"),
                          F.element_at(noms_mois, F.month(d)).alias("libelle_mois"),
                          F.quarter(d).cast("smallint").alias("trimestre"),
                          F.year(d).cast("smallint").alias("annee"),
                          F.coalesce("est_ferie", F.lit(False)).alias("est_ferie")))

    inconnu = spark.createDataFrame(
        [(-1, date(1900, 1, 1), 0, "Inconnu", 0, 0, "Inconnu", 0, 0, False)], calendrier.schema)

    return (calendrier.unionByName(inconnu)
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("date_chargement", F.current_timestamp()))


def controler(dim):
    """Contrôles de fin de job ; renvoie False si un écart est trouvé."""
    nb_jours = (FIN - DEBUT).days + 1
    nb_feries = sum(len(jours_feries(a)) for a in range(DEBUT.year, FIN.year + 1))
    r = dim.agg(F.count("*").alias("lignes"),
                F.countDistinct("id_date").alias("cles"),
                F.sum(F.when(F.col("id_date") == -1, 1).otherwise(0)).alias("inconnu"),
                F.sum(F.col("est_ferie").cast("int")).alias("feries")).first()
    attendu = {"lignes": nb_jours + 1, "cles": nb_jours + 1, "inconnu": 1, "feries": nb_feries}
    ok = True
    for nom, valeur in attendu.items():
        statut = "OK" if r[nom] == valeur else "ECART (attendu %d)" % valeur
        ok = ok and statut == "OK"
        print(f"GOLD dim_temps {nom:8s} = {r[nom]:>6}  {statut}")
    return ok


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("J20_dimensions_generees").enableHiveSupport()
             .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
             .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
             .config("spark.hadoop.dfs.replication", "1")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_DB}.dim_temps")
    (construire_dim_temps(spark).coalesce(1)
     .write.mode("overwrite").format("parquet").saveAsTable(f"{GOLD_DB}.dim_temps"))

    dim = spark.table(f"{GOLD_DB}.dim_temps")
    ok = controler(dim)
    print(f"GOLD dim_temps table={GOLD_DB}.dim_temps run_id={RUN_ID}")
    dim.filter("annee = 2024 AND est_ferie").orderBy("id_date") \
       .select("id_date", "jour_semaine", "jour", "libelle_mois").show(12, truncate=False)
    spark.stop()
    sys.exit(0 if ok else 1)