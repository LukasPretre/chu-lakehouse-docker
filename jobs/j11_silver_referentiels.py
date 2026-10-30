import os, hmac, hashlib
from pyspark.sql import SparkSession, functions as F, types as T

KEY = os.environ["HMAC_KEY_PROFESSIONNELS"].encode()
H = "hdfs://namenode:8020"
spark = (SparkSession.builder.appName("J11_silver_referentiels")
         .config("spark.hadoop.dfs.replication", "1").getOrCreate())

@F.udf(T.StringType())
def pseudo(v):
    return None if v is None else hmac.new(KEY, str(v).strip().encode(), hashlib.sha256).hexdigest()

def vide(c):
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")

def sortie(nom, bronze, ok, rej):
    ok.write.mode("overwrite").parquet(f"{H}/silver/{nom}")
    rej.write.mode("overwrite").parquet(f"{H}/silver/_rejets/{nom}")
    nb, no, nr = bronze.count(), ok.count(), rej.count()
    print(f"SILVER {nom:15s} bronze={nb} valides={no} rejetes={nr} doublons={nb-no-nr}")

# --- Diagnostic -> futur DIM_DIAGNOSTIC
b = spark.read.parquet(f"{H}/bronze/s1/Diagnostic")
rej = b.filter(vide("Code_diag")).select(F.lit("CODE_NUL").alias("code_rejet"))
ok = (b.filter(~vide("Code_diag"))
        .select(F.upper(F.trim("Code_diag")).alias("code_diagnostic"),
                F.trim("Diagnostic").alias("libelle_diagnostic"))
        .dropDuplicates(["code_diagnostic"]))
sortie("diagnostic", b, ok, rej)

# --- Specialites (sert à enrichir les professionnels)
b = spark.read.parquet(f"{H}/bronze/s1/Specialites")
rej = b.filter(vide("Code_specialite")).select(F.lit("CODE_NUL").alias("code_rejet"))
spec = (b.filter(~vide("Code_specialite"))
          .select(F.trim("Code_specialite").alias("code_specialite"),
                  F.trim("Fonction").alias("fonction"),
                  F.trim("Specialite").alias("specialite"))
          .dropDuplicates(["code_specialite"]))
sortie("specialites", b, spec, rej)

# --- Professionnel_de_sante -> futur DIM_PROFESSIONNEL (nom, prénom, civilité supprimés)
b = spark.read.parquet(f"{H}/bronze/s1/Professionnel_de_sante")
rej = b.filter(vide("Identifiant")).select(F.lit("ID_NUL").alias("code_rejet"))
sp = spec.select(F.col("code_specialite").alias("sp_code"), "fonction", "specialite")
ok = (b.filter(~vide("Identifiant")).dropDuplicates(["Identifiant"])
        .join(sp, F.trim(F.col("Code_specialite")) == F.col("sp_code"), "left")
        .select(pseudo("Identifiant").alias("professionnel_pseudo"),
                F.col("Type_identifiant").alias("type_identifiant"),
                F.col("Categorie_professionnelle").alias("categorie_professionnelle"),
                F.col("Profession").alias("profession"),
                F.coalesce("fonction", F.lit("Inconnue")).alias("fonction"),
                F.coalesce("specialite", F.lit("Inconnue")).alias("specialite")))
sortie("professionnel", b, ok, rej)
ok.printSchema()
ok.show(3, truncate=False)
spark.stop()
