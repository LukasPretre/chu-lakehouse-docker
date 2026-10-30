import os, hmac, hashlib
from pyspark.sql import SparkSession, functions as F, types as T

KEY = os.environ["HMAC_KEY_PATIENTS"].encode()
spark = SparkSession.builder.appName("J10_silver_patient").config("spark.hadoop.dfs.replication","1").getOrCreate()

@F.udf(T.StringType())
def pseudo(v):
    return None if v is None else hmac.new(KEY, str(v).encode(), hashlib.sha256).hexdigest()

b = spark.read.parquet("hdfs://namenode:8020/bronze/s1/Patient")
t = (b.withColumn("date_naissance", F.to_date("Date", "M/d/yyyy"))
       .withColumn("sexe_std",
           F.when(F.lower("Sexe").isin("female", "f", "femme"), "F")
            .when(F.lower("Sexe").isin("male", "m", "homme"), "M"))
       .withColumn("code_rejet",
           F.when(F.col("Id_patient").isNull(), "ID_NULL")
            .when(F.col("date_naissance").isNull(), "DATE_INVALIDE")
            .when(F.col("sexe_std").isNull(), "SEXE_INVALIDE")))

rejets = t.filter("code_rejet IS NOT NULL").select(
    pseudo("Id_patient").alias("patient_pseudo"), "code_rejet")
ok = (t.filter("code_rejet IS NULL").dropDuplicates(["Id_patient"])
        .select(pseudo("Id_patient").alias("patient_pseudo"),
                F.col("sexe_std").alias("sexe"),
                F.year("date_naissance").alias("annee_naissance"),
                "ingest_date"))

ok.write.mode("overwrite").parquet("hdfs://namenode:8020/silver/patient")
rejets.write.mode("overwrite").parquet("hdfs://namenode:8020/silver/_rejets/patient")

n_bronze, n_ok, n_rej = b.count(), ok.count(), rejets.count()
print(f"SILVER bronze={n_bronze} valides={n_ok} rejetes={n_rej} doublons={n_bronze-n_ok-n_rej}")
ok.printSchema()
ok.show(3, truncate=False)
spark.stop()
