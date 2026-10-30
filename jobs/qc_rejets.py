from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("qc_rejets").getOrCreate()
H = "hdfs://namenode:8020"
r = spark.read.parquet(f"{H}/silver/_rejets/consultation")
r.groupBy("code_rejet").count().show()
b = spark.read.parquet(f"{H}/bronze/s1/Consultation")
b.join(r, b.Num_consultation == r.id_consultation) \
 .select("Num_consultation", "Date", "Heure_debut", "Heure_fin", "code_rejet").show(10, False)
spark.stop()
