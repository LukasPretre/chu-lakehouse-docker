import os, hmac, hashlib
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

H = "hdfs://namenode:8020"
KEY = os.environ["HMAC_KEY_PROFESSIONNELS"].encode()
spark = (SparkSession.builder.appName("J15_silver_activite_professionnel")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

@F.udf(T.StringType())
def pseudo(v):   # même fonction que J11 (clé professionnels, valeur rognée)
    return None if v is None else hmac.new(KEY, str(v).strip().encode(), hashlib.sha256).hexdigest()

def nettoyer(c):
    v = F.trim(F.col(c))
    return F.when(v == "", F.lit(None)).otherwise(v)

b = spark.read.parquet(f"{H}/bronze/s2/activite_professionnel_sante")
b = b.filter(F.col("ingest_date") == b.agg(F.max("ingest_date")).first()[0])

t = b.select(nettoyer("identifiant").alias("id"),
             nettoyer("identifiant_organisation").alias("identifiant_organisation"),
             nettoyer("mode_exercice").alias("mode_exercice"), "ingest_date")
# S1 (et donc J11/J12) a perdu le zéro initial des ADELI : on retire ce zéro ici pour retrouver la même forme
t = t.withColumn("id_s1", F.when(F.col("id").rlike("^0[0-9]{8}$"), F.substring("id", 2, 8)).otherwise(F.col("id")))

ids = (t.select("id", "id_s1").where("id IS NOT NULL").distinct()
        .withColumn("p_brut", pseudo("id")).withColumn("p_s1", pseudo("id_s1"))
        .persist(StorageLevel.DISK_ONLY))
pro = spark.read.parquet(f"{H}/silver/professionnel").select(F.col("professionnel_pseudo").alias("pp")).distinct()
n_ids = ids.count()
n_brut = ids.join(pro, ids.p_brut == pro.pp).count()
n_s1 = ids.join(pro, ids.p_s1 == pro.pp).count()
print(f"SILVER activite identifiants distincts={n_ids} retrouves_sans_correction={n_brut} retrouves_apres_retrait_zero={n_s1}")

etab = spark.read.parquet(f"{H}/silver/etablissement").select(F.col("identifiant_organisation").alias("eo")).distinct()
t = (t.join(ids.select("id", F.col("p_s1").alias("professionnel_pseudo")), "id", "left")
      .join(pro.withColumn("pro_ok", F.lit(1)), F.col("professionnel_pseudo") == F.col("pp"), "left")
      .join(etab.withColumn("eo_ok", F.lit(1)), F.col("identifiant_organisation") == F.col("eo"), "left")
      .withColumn("code_rejet",
          F.when(F.col("id").isNull(), "ID_NUL")
           .when(F.col("pro_ok").isNull(), "PROFESSIONNEL_ORPHELIN")
           .when(F.col("identifiant_organisation").isNotNull() & F.col("eo_ok").isNull(), "ORGANISATION_ORPHELINE")))

ok = (t.filter("code_rejet IS NULL")
       .select("professionnel_pseudo", "identifiant_organisation", "mode_exercice", "ingest_date")
       .dropDuplicates(["professionnel_pseudo", "identifiant_organisation", "mode_exercice"]))
rej = t.filter("code_rejet IS NOT NULL").select("professionnel_pseudo", "identifiant_organisation", "code_rejet")

ok.write.mode("overwrite").parquet(f"{H}/silver/activite_professionnel")
rej.write.mode("overwrite").parquet(f"{H}/silver/_rejets/activite_professionnel")

res = spark.read.parquet(f"{H}/silver/activite_professionnel")
nb, no, nr = b.count(), res.count(), rej.count()
print(f"SILVER activite_professionnel bronze={nb} valides={no} rejetes={nr} doublons={nb-no-nr}")
for r in rej.groupBy("code_rejet").count().collect():
    print(f"SILVER activite_professionnel rejet {r[0]} = {r[1]}")
print("SILVER activite_professionnel sans organisation (liberaux) =", res.filter("identifiant_organisation IS NULL").count())
print("SILVER activite_professionnel mode_exercice =",
      sorted([(r[0], r[1]) for r in res.groupBy("mode_exercice").count().collect()], key=lambda x: str(x[0])))
spark.stop()
