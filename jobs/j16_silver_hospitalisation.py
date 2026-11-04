import os, hmac, hashlib
from pyspark.sql import SparkSession, functions as F, types as T, Window

KEY = os.environ["HMAC_KEY_PATIENTS"].encode()
spark = (SparkSession.builder.appName("J16_silver_hospitalisation")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .getOrCreate())

@F.udf(T.StringType())
def pseudo(v):  # identique a J10 (meme cle, pas de strip)
    return None if v is None else hmac.new(KEY, str(v).encode(), hashlib.sha256).hexdigest()

def nettoyer(c):
    t = F.trim(F.col(c))
    return F.when(t == "", None).otherwise(t)

H = "hdfs://namenode:8020"
b_all = spark.read.parquet(H + "/bronze/s2/hospitalisation")
last = b_all.agg(F.max("ingest_date")).first()[0]
b = b_all.filter(F.col("ingest_date") == last)

cd = F.upper(F.regexp_replace(F.col("Code_diagnostic"), r"\s+", ""))
d = b.select(
    nettoyer("Num_Hospitalisation").alias("num_hospitalisation"),
    nettoyer("Id_patient").alias("id_patient"),
    nettoyer("identifiant_organisation").alias("identifiant_organisation"),
    F.when(cd == "", None).otherwise(cd).alias("code_diagnostic"),
    nettoyer("Suite_diagnostic_consultation").alias("suite_diagnostic_consultation"),
    nettoyer("Date_Entree").alias("date_brut"),
    nettoyer("Jour_Hospitalisation").alias("duree_brut"),
    "ingest_date")

d = (d.withColumn("date_entree",
         F.when(F.col("date_brut").rlike(r"^[0-9]{4,6}$"),
                F.expr("date_add(to_date('1899-12-30'), cast(date_brut as int))"))
          .otherwise(F.to_date("date_brut", "dd/MM/yyyy")))
       .withColumn("duree_sejour_jours", F.col("duree_brut").cast("int"))
       .withColumn("date_sortie",
         F.when(F.col("duree_sejour_jours").isNotNull(),
                F.expr("date_add(date_entree, duree_sejour_jours)")))
       .withColumn("patient_pseudo", pseudo("id_patient"))
       .withColumn("code_rejet",
         F.when(F.col("num_hospitalisation").isNull(), "NUM_NUL")
          .when(F.col("date_entree").isNull(), "DATE_ENTREE_INVALIDE")
          .when(F.col("duree_sejour_jours") < 0, "DUREE_NEGATIVE")))

rejets = d.filter("code_rejet IS NOT NULL").select(
    "num_hospitalisation", "patient_pseudo", "identifiant_organisation", "code_rejet")

w = Window.partitionBy("num_hospitalisation").orderBy(F.col("ingest_date").desc())
ok = (d.filter("code_rejet IS NULL")
        .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
        .select("num_hospitalisation", "patient_pseudo", "identifiant_organisation",
                "code_diagnostic", "suite_diagnostic_consultation", "date_entree",
                "duree_sejour_jours", "date_sortie", "ingest_date"))

ok.write.mode("overwrite").parquet(H + "/silver/hospitalisation")
rejets.write.mode("overwrite").parquet(H + "/silver/_rejets/hospitalisation")

n_bronze, n_ok, n_rej = b.count(), ok.count(), rejets.count()
print(f"SILVER hospitalisation bronze={n_bronze} valides={n_ok} rejetes={n_rej} doublons={n_bronze-n_ok-n_rej}")
for r in rejets.groupBy("code_rejet").count().collect():
    print(f"SILVER hospitalisation rejet {r['code_rejet']} = {r['count']}")

pat = spark.read.parquet(H + "/silver/patient").select("patient_pseudo").distinct()
eta = spark.read.parquet(H + "/silver/etablissement").select("identifiant_organisation").distinct()
print("SILVER hospitalisation patients introuvables =", ok.join(pat, "patient_pseudo", "left_anti").count())
print("SILVER hospitalisation etablissements introuvables =", ok.join(eta, "identifiant_organisation", "left_anti").count())
s = ok.agg(F.min("date_entree"), F.max("date_entree"), F.min("duree_sejour_jours"),
           F.max("duree_sejour_jours"), F.sum(F.col("duree_sejour_jours").isNull().cast("int"))).first()
print("SILVER hospitalisation date_entree min/max =", s[0], s[1], "| duree min/max =", s[2], s[3], "| durees nulles =", s[4])
spark.stop()
