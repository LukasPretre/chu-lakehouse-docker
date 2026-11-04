from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("check_bronze_s2")
         .config("spark.hadoop.dfs.replication", "1").getOrCreate())
attendu = {"etablissement_sante": 416665, "professionnel_sante": 1048575,
           "activite_professionnel_sante": 1823588, "hospitalisation": 2479}
for t, n in attendu.items():
    try:
        c = spark.read.parquet(f"hdfs://namenode:8020/bronze/s2/{t}").count()
        print(f"BRONZE S2 {t:30s} {c:>9} lignes  {'OK' if c == n else 'ECART (attendu %d)' % n}")
    except Exception:
        print(f"BRONZE S2 {t:30s} ABSENT")
spark.stop()
