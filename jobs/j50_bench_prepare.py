"""
J50 - Preparation du banc d'essai : trois versions physiques de la consultation.

Source : /silver/consultation (Silver S1, 1 027 157 lignes).
Cible  : base Hive chu_bench (separee du Gold, jetable).

  consultation_plat         Parquet, sans partition ni bucket        (reference "avant")
  consultation_part         Parquet, partitionne par annee           (elagage des partitions)
  consultation_part_bucket  Parquet, partition annee + 16 buckets    (jointures et agregats par patient)
                            sur patient_pseudo, tries par patient

Les buckets crees par Spark (bucketBy) sont au format Spark, pas au format Hive :
ils servent aux requetes Spark, pas a HiveServer2.

Lancement (BENCH_SCALE=10 pour 10 fois plus de lignes) :
  docker exec -e BENCH_SCALE=1 chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /jobs/j50_bench_prepare.py
"""
import os
from pyspark.sql import SparkSession, functions as F

NB_BUCKETS = 16
# Facteur de multiplication des donnees de test (1 = donnees reelles).
# Les copies recoivent un id_consultation et un patient_pseudo differents ; dates, diagnostics
# et professionnels sont conserves. A DECLARER dans le rapport (donnees de test dupliquees).
SCALE = int(os.environ.get("BENCH_SCALE", "1"))

spark = (SparkSession.builder.appName("j50_bench_prepare").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

spark.sql("CREATE DATABASE IF NOT EXISTS chu_bench")

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
total = src.count()
print("BENCHPREP facteur=%d source=%d lignes" % (SCALE, total))
print("BENCHPREP repartition par annee :")
for r in src.groupBy("annee").count().orderBy("annee").collect():
    print("BENCHPREP   annee=%s lignes=%d" % (r["annee"], r["count"]))


def ecrire(df, nom, **options):
    spark.sql("DROP TABLE IF EXISTS chu_bench.%s" % nom)
    w = df.write.mode("overwrite").format("parquet")
    if "partition" in options:
        w = w.partitionBy(options["partition"])
    if "buckets" in options:
        w = w.bucketBy(options["buckets"], options["bucket_col"]).sortBy(options["bucket_col"])
    w.saveAsTable("chu_bench.%s" % nom)
    n = spark.table("chu_bench.%s" % nom).count()
    print("BENCHPREP %s : %d lignes %s" % (nom, n, "OK" if n == total else "ECART"))


# 1. Reference : une seule table, quelques gros fichiers
ecrire(src.coalesce(max(8, SCALE)), "consultation_plat")

# 2. Partitionnee par annee (un repartition par annee evite des centaines de petits fichiers)
ecrire(src.repartition("annee"), "consultation_part", partition="annee")

# 3. Partition par annee + buckets sur le patient
ecrire(src.repartition(NB_BUCKETS, "patient_pseudo"), "consultation_part_bucket",
       partition="annee", buckets=NB_BUCKETS, bucket_col="patient_pseudo")

print("BENCHPREP termine")
