"""J18 : Silver deces (fichier INSEE). Source : Bronze S4 /bronze/s4/deces/ingest_date=<dernier>/deces.csv.
RGPD : nom, prenom, lieu, code lieu et pays de naissance ne sont jamais conserves ; date_naissance sert seulement
a calculer l'age puis disparait ; le numero d'acte et la date de naissance ne sont conserves que dans l'empreinte HMAC secrete (cle_deces).
Les deces ne sont jamais rapproches des patients du CHU.
Test sur un echantillon : docker exec -e J18_ECHANTILLON=500000 ... (ecrit dans /silver/_test, pas dans /silver)."""
import os, sys, hashlib
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/jobs")
from lib_geo import departement_depuis_commune, table_departements  # J05

H = "hdfs://namenode:8020"
BRONZE = H + "/bronze/s4/deces"
ECHANTILLON = int(os.environ.get("J18_ECHANTILLON", "0"))          # 0 = fichier entier
SORTIE = H + ("/silver/_test/deces" if ECHANTILLON else "/silver/deces")
REJETS = H + ("/silver/_test/_rejets_deces" if ECHANTILLON else "/silver/_rejets/deces")

# cle_deces = HMAC-SHA256 avec la cle secrete HMAC_KEY_PATIENTS (.env) : sans la cle, personne ne peut la recalculer a
# partir du fichier INSEE public. Fonctions natives de Spark (un UDF Python serait trop lent sur 25 M de lignes).
KEY = os.environ["HMAC_KEY_PATIENTS"].encode()
if len(KEY) > 64:
    KEY = hashlib.sha256(KEY).digest()
KEY = KEY + bytes(64 - len(KEY))
IPAD = bytes(b ^ 0x36 for b in KEY).hex()
OPAD = bytes(b ^ 0x5C for b in KEY).hex()


def hmac_sha256(msg):
    interne = F.sha2(F.concat(F.unhex(F.lit(IPAD)), F.encode(msg, "UTF-8")), 256)
    return F.sha2(F.concat(F.unhex(F.lit(OPAD)), F.unhex(interne)), 256)


spark = (SparkSession.builder.appName("J18_silver_deces")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED").getOrCreate())


def dernier_ingest():
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(H), spark._jsc.hadoopConfiguration())
    noms = sorted(s.getPath().getName() for s in fs.listStatus(jvm.org.apache.hadoop.fs.Path(BRONZE)))
    return [n for n in noms if n.startswith("ingest_date=")][-1]


def nettoyer(c):
    v = F.trim(F.col(c))
    return F.when(v == "", F.lit(None)).otherwise(v)


dossier = dernier_ingest()
raw = (spark.read.option("header", True).option("sep", ",").option("encoding", "UTF-8")
       .csv(f"{BRONZE}/{dossier}/deces.csv"))
if ECHANTILLON:
    raw = raw.limit(ECHANTILLON)

# on ne garde que les colonnes utiles : nom, prenom, lieu et pays de naissance ne sont jamais lus plus loin
d = raw.select(nettoyer("sexe").alias("sexe_brut"), nettoyer("date_naissance").alias("naiss"),
               nettoyer("date_deces").alias("deces"), nettoyer("code_lieu_deces").alias("lieu_brut"),
               nettoyer("numero_acte_deces").alias("acte"))

# ------------------------------------------------------------------ nettoyage
lieu = F.when(F.col("lieu_brut").rlike("^[0-9]+$") & (F.length("lieu_brut") < 5), F.lpad("lieu_brut", 5, "0")) \
        .otherwise(F.upper(F.col("lieu_brut")))
dd = F.to_date(F.col("deces"), "yyyy-MM-dd")      # NULL si absent ou invalide
dn = F.to_date(F.col("naiss"), "yyyy-MM-dd")      # NULL si jour ou mois inconnu (00) ou invalide
yn = F.when(F.col("naiss").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"), F.substring("naiss", 1, 4).cast("int"))

age_brut = (F.when(dd.isNull() | yn.isNull(), F.lit(None))
             .when(dn.isNotNull(), F.floor(F.months_between(dd, dn) / 12).cast("int"))
             .otherwise(F.year(dd) - yn))                       # jour ou mois inconnu : calcul sur l'annee
avant_naissance = F.coalesce((dn.isNotNull() & (dd < dn)) |
                             (dn.isNull() & yn.isNotNull() & (yn > 0) & (F.year(dd) < yn)), F.lit(False))

# departements valides du referentiel J05 : un code deduit hors de cette liste est ignore (region inconnue)
valides = [r[0] for r in table_departements(spark).select("code_departement").collect()]
ref = table_departements(spark).filter("code_region_insee IS NOT NULL") \
        .select(F.col("code_departement").alias("dep"), F.col("code_region_insee").alias("region_ref"))

t = (d.withColumn("code_lieu_deces", F.when(lieu.rlike("^([0-9]{5}|2[AB][0-9]{3})$"), lieu))
      .withColumn("cle_deces", hmac_sha256(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in ("deces", "lieu_brut", "acte", "naiss", "sexe_brut")])))
      .withColumn("sexe", F.when(F.col("sexe_brut") == "1", "M").when(F.col("sexe_brut") == "2", "F").otherwise("I"))
      .withColumn("date_deces", dd)
      .withColumn("annee_deces", F.year(dd))
      .withColumn("age_au_deces", F.when(age_brut.between(0, 125), age_brut))
      .withColumn("dep", departement_depuis_commune("code_lieu_deces"))
      .withColumn("dep", F.when(F.col("dep").isin(valides), F.col("dep")))
      .withColumn("code_rejet", F.when(dd.isNull(), "DATE_DECES_INVALIDE")
                                 .when(avant_naissance, "DECES_AVANT_NAISSANCE")))
t = (t.join(F.broadcast(ref), "dep", "left")
      .withColumn("code_region", F.coalesce(F.col("region_ref"), F.lit("ZZ")))   # 99 = Etranger, ZZ = Inconnu
      .withColumn("ingest_date", F.lit(dossier.split("=")[1])))

ok = (t.filter("code_rejet IS NULL").dropDuplicates(["cle_deces"])
        .select("cle_deces", "sexe", "date_deces", "age_au_deces", "code_lieu_deces", "code_region",
                "ingest_date", "annee_deces"))
rej = t.filter("code_rejet IS NOT NULL").select("cle_deces", "code_rejet")

ok.write.mode("overwrite").partitionBy("annee_deces").parquet(SORTIE)
rej.write.mode("overwrite").parquet(REJETS)

# ------------------------------------------------------------------ comptes et anomalies
res = spark.read.parquet(SORTIE)
nb, no, nr = raw.count(), res.count(), spark.read.parquet(REJETS).count()
print(f"SILVER deces bronze={nb} valides={no} rejetes={nr} doublons={nb-no-nr}" +
      (f" (ECHANTILLON {ECHANTILLON})" if ECHANTILLON else ""))
for r in spark.read.parquet(REJETS).groupBy("code_rejet").count().orderBy("code_rejet").collect():
    print(f"SILVER deces rejet {r[0]} = {r[1]}")
a = res.agg(F.count(F.when(F.col("age_au_deces").isNull(), 1)).alias("age_null"),
            F.count(F.when(F.col("sexe").isNull(), 1)).alias("sexe_null"),
            F.count(F.when(F.col("code_lieu_deces").isNull(), 1)).alias("lieu_null"),
            F.count(F.when(F.col("code_region") == "ZZ", 1)).alias("region_zz"),
            F.count(F.when(F.col("code_region") == "99", 1)).alias("etranger"),
            F.min("date_deces").alias("dmin"), F.max("date_deces").alias("dmax"),
            F.min("age_au_deces").alias("amin"), F.max("age_au_deces").alias("amax")).first()
print(f"SILVER deces age NULL (hors 0-125 ou naissance inconnue) = {a['age_null']} | sexe NULL = {a['sexe_null']} | "
      f"code lieu NULL = {a['lieu_null']}")
print(f"SILVER deces region ZZ (inconnue) = {a['region_zz']} | Etranger (99) = {a['etranger']}")
print(f"SILVER deces date_deces min/max = {a['dmin']} {a['dmax']} | age min/max = {a['amin']} {a['amax']}")
print("SILVER deces deces en 2019 =", res.filter("annee_deces = 2019").count())
print("SILVER deces oppositions : aucun fichier d'oppositions fourni, 0 deces retire")
spark.stop()
