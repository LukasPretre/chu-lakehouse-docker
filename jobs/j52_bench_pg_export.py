"""
J52 - Export de la consultation (Silver) en CSV sur HDFS, pour le benchmark PostgreSQL.

Meme jeu de donnees que J50 (meme facteur BENCH_SCALE, memes regles de duplication), pour que
les mesures Hive/Spark et PostgreSQL portent sur les memes lignes.
Sortie : hdfs:///bench/pg_export/part-*.csv (sans en-tete, colonnes dans l'ordre de
sql/bench_pg_01_plat.sql). Le chargement dans PostgreSQL est fait par bench_pg.sh prepare.

Lancement (le plus simple : ./bench_pg.sh prepare) :
  docker exec -e BENCH_SCALE=1 chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /jobs/j52_bench_pg_export.py
"""
import os
from pyspark.sql import SparkSession, functions as F

SCALE = int(os.environ.get("BENCH_SCALE", "1"))
SORTIE = "hdfs://namenode:8020/bench/pg_export"

spark = (SparkSession.builder.appName("j52_bench_pg_export")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

src = (spark.read.parquet("hdfs://namenode:8020/silver/consultation")
       .withColumn("annee", F.year("date_consultation"))
       .withColumn("mois", F.month("date_consultation")))

if SCALE > 1:
    k = F.explode(F.sequence(F.lit(0), F.lit(SCALE - 1))).alias("copie")
    src = (src.select("*", k)
           .withColumn("id_consultation", F.col("id_consultation").cast("long") * SCALE + F.col("copie"))
           .withColumn("patient_pseudo", F.when(F.col("copie") == 0, F.col("patient_pseudo"))
                       .otherwise(F.sha2(F.concat(F.col("patient_pseudo"), F.col("copie").cast("string")), 256)))
           .drop("copie"))

out = src.select("id_consultation", "date_consultation", "heure_debut", "heure_fin", "duree_minutes",
                 "patient_pseudo", "professionnel_pseudo", "code_diagnostic", "annee", "mois")
n = out.count()
(out.coalesce(4).write.mode("overwrite").option("header", "false").csv(SORTIE))
print("PGEXPORT facteur=%d lignes=%d -> %s" % (SCALE, n, SORTIE))
