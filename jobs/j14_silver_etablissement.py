import sys
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/jobs")
from lib_geo import departement_depuis_commune, departement_depuis_code_postal  # J05

H = "hdfs://namenode:8020"
spark = (SparkSession.builder.appName("J14_silver_etablissement")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

def nettoyer(c):
    v = F.trim(F.col(c))
    return F.when(v == "", F.lit(None)).otherwise(v)

def zeros(col, n):
    # zéros initiaux perdus par Excel : on complète seulement les valeurs numériques trop courtes
    return F.when(col.rlike("^[0-9]+$") & (F.length(col) < n), F.lpad(col, n, "0")).otherwise(col)

b = spark.read.parquet(f"{H}/bronze/s2/etablissement_sante")
b = b.filter(F.col("ingest_date") == b.agg(F.max("ingest_date")).first()[0])

t = b.select(
    nettoyer("identifiant_organisation").alias("identifiant_organisation"),
    zeros(nettoyer("finess_site"), 9).alias("finess_site"),
    zeros(nettoyer("finess_etablissement_juridique"), 9).alias("finess_juridique"),
    zeros(nettoyer("siret_site"), 14).alias("siret_site"),
    zeros(nettoyer("siren_site"), 9).alias("siren_site"),
    nettoyer("raison_sociale_site").alias("raison_sociale"),
    nettoyer("enseigne_commerciale_site").alias("enseigne"),
    zeros(nettoyer("code_commune"), 5).alias("code_commune"),
    nettoyer("commune").alias("commune"),
    zeros(nettoyer("code_postal"), 5).alias("code_postal"),
    nettoyer("pays").alias("pays"),
    "ingest_date")

t = (t.withColumn("dep_commune", departement_depuis_commune("code_commune"))
      .withColumn("dep_postal", departement_depuis_code_postal("code_postal"))
      .withColumn("code_departement", F.coalesce("dep_commune", "dep_postal"))
      .withColumn("origine_departement",
                  F.when(F.col("dep_commune").isNotNull(), F.lit("commune"))
                   .when(F.col("dep_postal").isNotNull(), F.lit("code_postal")))
      .withColumn("categorie", F.lit("Non renseignée"))   # aucune colonne source (limite à écrire dans le rapport)
      .withColumn("code_rejet",
         F.when(F.col("identifiant_organisation").isNull(), "ID_NUL")
          .when(F.col("raison_sociale").isNull(), "RAISON_SOCIALE_NULLE")
          .when(~F.coalesce(F.col("code_postal").rlike("^[0-9]{5}$"), F.lit(False)), "CODE_POSTAL_INVALIDE")))

cols = ["identifiant_organisation", "finess_site", "finess_juridique", "siret_site", "siren_site",
        "raison_sociale", "enseigne", "categorie", "code_commune", "commune", "code_postal",
        "code_departement", "origine_departement", "pays", "ingest_date"]
ok = t.filter("code_rejet IS NULL").dropDuplicates(["identifiant_organisation"]).select(*cols)
rej = t.filter("code_rejet IS NOT NULL").select("identifiant_organisation", "code_rejet")

ok.write.mode("overwrite").parquet(f"{H}/silver/etablissement")
rej.write.mode("overwrite").parquet(f"{H}/silver/_rejets/etablissement")

res = spark.read.parquet(f"{H}/silver/etablissement")
nb, no, nr = b.count(), res.count(), rej.count()
print(f"SILVER etablissement bronze={nb} valides={no} rejetes={nr} doublons={nb-no-nr}")
for r in rej.groupBy("code_rejet").count().collect():
    print(f"SILVER etablissement rejet {r[0]} = {r[1]}")
print("SILVER etablissement origine departement =",
      sorted([(r[0], r[1]) for r in res.groupBy("origine_departement").count().collect()], key=lambda x: str(x[0])))
print("SILVER etablissement prefixes identifiant =",
      sorted([(r[0], r[1]) for r in res.groupBy(F.substring("identifiant_organisation", 1, 1).alias("p")).count().collect()]))
spark.stop()
