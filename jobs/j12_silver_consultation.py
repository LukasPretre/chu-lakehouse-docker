import os, hmac, hashlib
from pyspark.sql import SparkSession, functions as F, types as T

H = "hdfs://namenode:8020"
spark = (SparkSession.builder.appName("J12_silver_consultation")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

def make_pseudo(key):
    k = key.encode()
    @F.udf(T.StringType())
    def f(v):
        return None if v is None else hmac.new(k, str(v).strip().encode(), hashlib.sha256).hexdigest()
    return f

pseudo_pat = make_pseudo(os.environ["HMAC_KEY_PATIENTS"])
pseudo_pro = make_pseudo(os.environ["HMAC_KEY_PROFESSIONNELS"])

b = spark.read.parquet(f"{H}/bronze/s1/Consultation")
t = (b.withColumn("d_ms", F.col("Date").cast("long"))
      .withColumn("hd_ms", F.col("Heure_debut").cast("long"))
      .withColumn("hf_ms", F.col("Heure_fin").cast("long"))
      .withColumn("code_rejet",
          F.when(F.col("Num_consultation").isNull(), "ID_NUL")
           .when(F.col("Id_patient").isNull(), "PATIENT_NUL")
           .when(F.col("Id_prof_sante").isNull() | (F.trim("Id_prof_sante") == ""), "PRO_NUL")
           .when(F.col("d_ms").isNull(), "DATE_INVALIDE")
           .when(F.col("hd_ms").isNull() | F.col("hf_ms").isNull(), "HEURE_NULLE")))

rej = (t.filter("code_rejet IS NOT NULL")
        .select(F.col("Num_consultation").alias("id_consultation"), "code_rejet"))
ok = (t.filter("code_rejet IS NULL").dropDuplicates(["Num_consultation"])
       .select(F.col("Num_consultation").alias("id_consultation"),
               F.to_date(F.timestamp_seconds((F.col("d_ms") / 1000).cast("long"))).alias("date_consultation"),
               F.date_format(F.timestamp_seconds((F.col("hd_ms") / 1000).cast("long")), "HH:mm:ss").alias("heure_debut"),
               F.date_format(F.timestamp_seconds((F.col("hf_ms") / 1000).cast("long")), "HH:mm:ss").alias("heure_fin"),
               ((F.col("hf_ms") - F.col("hd_ms") + F.when(F.col("hf_ms") < F.col("hd_ms"), 86400000).otherwise(0)) / 60000).cast("int").alias("duree_minutes"),
               pseudo_pat("Id_patient").alias("patient_pseudo"),
               pseudo_pro("Id_prof_sante").alias("professionnel_pseudo"),
               F.upper(F.trim("Code_diag")).alias("code_diagnostic"),
               "ingest_date"))

ok.write.mode("overwrite").parquet(f"{H}/silver/consultation")
rej.write.mode("overwrite").parquet(f"{H}/silver/_rejets/consultation")

res = spark.read.parquet(f"{H}/silver/consultation")
nb, no, nr = b.count(), res.count(), rej.count()
print(f"SILVER consultation bronze={nb} valides={no} rejetes={nr} doublons={nb-no-nr}")

# Contrôles d'intégrité référentielle (informatif : les orphelins deviendront la clé -1 « Inconnu » en Gold)
pat = spark.read.parquet(f"{H}/silver/patient").select("patient_pseudo")
pro = spark.read.parquet(f"{H}/silver/professionnel").select("professionnel_pseudo")
dia = spark.read.parquet(f"{H}/silver/diagnostic").select("code_diagnostic")
print("SILVER orphelins patient    =", res.join(pat, "patient_pseudo", "left_anti").count())
print("SILVER orphelins profession =", res.join(pro, "professionnel_pseudo", "left_anti").count())
print("SILVER orphelins diagnostic =", res.join(dia, "code_diagnostic", "left_anti").count())
res.agg(F.min("date_consultation").alias("date_min"), F.max("date_consultation").alias("date_max"),
        F.round(F.avg("duree_minutes"), 1).alias("duree_moy_min")).show()
res.show(3, truncate=False)
spark.stop()
