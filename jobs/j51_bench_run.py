"""
J51 - Mesure des temps de reponse sur les trois versions de la consultation.

Chaque requete est executee RUNS fois par variante ; la mediane est la valeur retenue
(la premiere execution paie le cache froid). Resultat : lignes "BENCH,...", a recuperer :
  docker exec chu_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /jobs/j51_bench_run.py 2>&1 | grep '^BENCH,' > bench_resultats.csv

Les requetes ci-dessous sont des requetes d'essai portant sur la seule table consultation.
Les requetes definitives (jointures avec dimensions, indicateurs du Livrable 1) se rajoutent
dans le dictionnaire REQUETES : le texte utilise {t} pour le nom de la table de faits.
"""
import statistics
import time
from pyspark.sql import SparkSession

RUNS = 3
VARIANTES = ["consultation_plat", "consultation_part", "consultation_part_bucket"]
ANNEE_TEST = 2019   # a ajuster selon la repartition affichee par j50

REQUETES = {
    "Q1_filtre_annee": """
        SELECT COUNT(*) AS nb, AVG(duree_minutes) AS duree_moy
        FROM chu_bench.{t} WHERE annee = %d""" % ANNEE_TEST,
    "Q2_consultations_par_mois": """
        SELECT mois, COUNT(*) AS nb
        FROM chu_bench.{t} WHERE annee = %d GROUP BY mois ORDER BY mois""" % ANNEE_TEST,
    "Q3_top_patients": """
        SELECT patient_pseudo, COUNT(*) AS nb
        FROM chu_bench.{t} GROUP BY patient_pseudo ORDER BY nb DESC LIMIT 20""",
    "Q4_patients_plus_de_3_visites_par_an": """
        SELECT COUNT(*) AS nb_patients FROM (
          SELECT patient_pseudo, annee FROM chu_bench.{t}
          GROUP BY patient_pseudo, annee HAVING COUNT(*) > 3) x""",
    "Q5_duree_par_professionnel_et_diagnostic": """
        SELECT professionnel_pseudo, code_diagnostic, COUNT(*) AS nb, AVG(duree_minutes) AS duree_moy
        FROM chu_bench.{t} WHERE annee BETWEEN %d AND %d
        GROUP BY professionnel_pseudo, code_diagnostic ORDER BY nb DESC LIMIT 50""" % (ANNEE_TEST - 1, ANNEE_TEST),
}

spark = (SparkSession.builder.appName("j51_bench_run").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

# Echauffement : la toute premiere requete paie le demarrage de la session et du Metastore
spark.sql("SELECT COUNT(*) FROM chu_bench.consultation_plat").collect()

print("BENCH,variante,requete,run,secondes,lignes_resultat")
for nom, sql in REQUETES.items():
    for v in VARIANTES:
        temps = []
        for run in range(1, RUNS + 1):
            spark.catalog.clearCache()
            t0 = time.time()
            n = len(spark.sql(sql.format(t=v)).collect())
            dt = time.time() - t0
            temps.append(dt)
            print("BENCH,%s,%s,%d,%.2f,%d" % (v, nom, run, dt, n))
        print("BENCH,%s,%s,mediane,%.2f," % (v, nom, statistics.median(temps)))
