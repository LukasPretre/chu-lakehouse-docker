from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("check_bronze_hdfs")
         .config("spark.hadoop.dfs.replication", "1").getOrCreate())
attendu = {"Diagnostic": 15490, "Specialites": 93, "Patient": 100000,
           "Professionnel_de_sante": 1048575, "Consultation": 1027157}
for t, n in attendu.items():
    try:
        c = spark.read.parquet(f"hdfs://namenode:8020/bronze/s1/{t}").count()
        print(f"BRONZE {t:25s} {c:>9} lignes  {'OK' if c == n else 'ECART (attendu %d)' % n}")
    except Exception:
        print(f"BRONZE {t:25s} ABSENT")
spark.stop()
