"""
Contrôle des volumes du Bronze S2 (dernière partition ingest_date, celle que lisent J14 à J16).
Volumes attendus : ceux des fichiers réels, sauf si la CI a écrit un Bronze synthétique
(fichier /bronze/_ci/attendu_bronze_s2, écrit par tests/ci_bronze_s2.py).
Sortie : « BRONZE S2 <table> <n> lignes OK | ECART (attendu ...) | ABSENT ».
"""
import json

from pyspark.sql import SparkSession, functions as F

H = "hdfs://namenode:8020"
spark = (SparkSession.builder.appName("check_bronze_s2")
         .config("spark.hadoop.dfs.replication", "1").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

attendu = {"etablissement_sante": 416665, "professionnel_sante": 1048575,
           "activite_professionnel_sante": 1823588, "hospitalisation": 2479}
try:
    attendu = json.loads(spark.read.text(f"{H}/bronze/_ci/attendu_bronze_s2").first()[0])
    print("BRONZE S2 volumes attendus : Bronze synthétique de la CI")
except Exception:
    pass

for t, n in attendu.items():
    try:
        b = spark.read.parquet(f"{H}/bronze/s2/{t}")
        if "ingest_date" in b.columns:
            b = b.filter(F.col("ingest_date") == b.agg(F.max("ingest_date")).first()[0])
        c = b.count()
        print(f"BRONZE S2 {t:30s} {c:>9} lignes  {'OK' if c == n else 'ECART (attendu %d)' % n}")
    except Exception:
        print(f"BRONZE S2 {t:30s} ABSENT")
spark.stop()
