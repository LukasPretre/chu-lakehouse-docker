"""
J32 - Gold : fait_deces (Hive, base chu_gold).

Entree  : Silver hdfs://namenode:8020/silver/deces (J18), partitionne par annee_deces
          + dimensions Hive chu_gold.dim_temps et dim_region (si elles existent)
Sortie  : table Hive chu_gold.fait_deces (Parquet), colonnes du DDL (sql/gold_ddl.sql).

Regles
- Le Silver garde tout le fichier INSEE (1882 a mars 2020). Le fait ne charge que la plage
  J32_ANNEE_MIN a J32_ANNEE_MAX (defaut 2015 a 2020, decision de l'equipe, periode des autres faits).
  ATTENTION : 2020 s'arrete au 30 mars 2020 (fin du fichier source).
- Un deces = une ligne (nb_deces = 1). Aucune ligne de la plage n'est perdue.
- id_date = AAAAMMJJ du deces ; -1 si la date est absente de dim_temps.
- id_region = code INSEE de la region du lieu de deces converti en entier (comme dim_region, J21) ;
  99 = Etranger ; -1 = Inconnu (code ZZ). Un code absent de dim_region donne aussi -1 (compte).
- sexe : F, M ou I (inconnu). tranche_age : 0-4, 5-9, ..., 90-94, 95+ ; NULL si l'age est inconnu.
- id_deces : numero 1..n, unique : rang du deces dans son annee (ordre de cle_deces) + decalage
  (nombre de deces des annees precedentes). Pas de donnee nominative : cle_deces n'est pas copiee.
- Rechargement complet a chaque execution (rejouable).

Modes
- J32_MODE=normal (defaut) : lit dim_temps (et dim_region si elle existe) dans chu_gold.
- J32_MODE=test : pas de dimension, ecrit dans chu_test.fait_deces.

Sortie console : lignes GOLDQC,indicateur,valeur puis GOLD fait_deces ...
"""
import os
import sys
import time
from datetime import datetime, timezone

_T = [time.time()]


def chrono(etape):
    maintenant = time.time()
    print(f"GOLDTEMPS,{etape},{maintenant - _T[0]:.1f}", flush=True)
    _T[0] = maintenant


from pyspark.sql import SparkSession, Window, functions as F

MODE = os.environ.get("J32_MODE", "normal")
ANNEE_MIN = int(os.environ.get("J32_ANNEE_MIN", "2015"))
ANNEE_MAX = int(os.environ.get("J32_ANNEE_MAX", "2020"))
BASE_DIM = "chu_gold"
BASE_SORTIE = "chu_test" if MODE == "test" else "chu_gold"
SILVER = "hdfs://namenode:8020/silver"
RUN_ID = os.environ.get("RUN_ID", "J32_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))

spark = (SparkSession.builder.appName("j32_fait_deces").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.environ.get("J32_PARTITIONS", "16"))
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
chrono("demarrage_session")


def qc(indicateur, valeur):
    print(f"GOLDQC,{indicateur},{valeur}", flush=True)


def stop(message):
    print(f"GOLD ERREUR {message}", flush=True)
    spark.stop()
    sys.exit(1)


def table_existe(base, table):
    return spark.catalog.tableExists(f"{base}.{table}")


qc("plage_annees", f"{ANNEE_MIN}-{ANNEE_MAX}")
if ANNEE_MIN > ANNEE_MAX:
    stop(f"plage invalide : J32_ANNEE_MIN={ANNEE_MIN} > J32_ANNEE_MAX={ANNEE_MAX}")

# ---------------------------------------------------------------- dimensions
dates_connues, regions_connues = None, None
if MODE == "normal":
    if not table_existe(BASE_DIM, "dim_temps"):
        stop(f"dim_temps absente dans {BASE_DIM} (J20). Pour tester avant : J32_MODE=test")
    dates_connues = (spark.table(f"{BASE_DIM}.dim_temps").select("id_date")
                     .filter("id_date <> -1").withColumn("date_connue", F.lit(True)))
    if table_existe(BASE_DIM, "dim_region"):
        regions_connues = (spark.table(f"{BASE_DIM}.dim_region").select(F.col("id_region").alias("id_region_dim"))
                           .filter("id_region_dim <> -1").withColumn("region_connue", F.lit(True)))
    else:
        qc("q_dim_region_absente", 1)
elif MODE != "test":
    stop(f"J32_MODE inconnu : {MODE} (normal ou test)")

# ---------------------------------------------------------------- Silver (plage d'annees)
try:
    silver = spark.read.parquet(f"{SILVER}/deces")
except Exception:
    print("GOLD fait_deces ignoré : silver/deces absent (lancer J18)", flush=True)
    spark.stop()
    sys.exit(0)
attendues = ["cle_deces", "sexe", "date_deces", "age_au_deces", "code_region", "annee_deces"]
manquantes = [c for c in attendues if c not in silver.columns]
if manquantes:
    stop(f"silver/deces : colonne(s) absente(s) {manquantes} ; colonnes trouvees : {silver.columns}")
silver = silver.filter(F.col("annee_deces").between(ANNEE_MIN, ANNEE_MAX))
par_annee = sorted((r["annee_deces"], r["count"]) for r in silver.groupBy("annee_deces").count().collect())
nb_silver = sum(n for _, n in par_annee)
qc("lignes_silver_plage", nb_silver)
for annee, n in par_annee:
    qc(f"silver_annee_{annee}", n)
chrono("lecture_silver")
if nb_silver == 0:
    stop("aucun deces dans la plage demandee")
if nb_silver >= 2147483647:
    stop("plus de deces que ne peut en numeroter un INTEGER")
decalage, cumul = [], 0
for annee, n in par_annee:
    decalage.append((annee, cumul))
    cumul += n
decalages = spark.createDataFrame(decalage, "annee_deces int, decalage long")

# ---------------------------------------------------------------- cles et attributs
age = F.col("age_au_deces")
tranche = (F.when(age.isNull(), F.lit(None).cast("string"))
            .when(age >= 95, F.lit("95+"))
            .otherwise(F.concat((F.floor(age / 5) * 5).cast("string"), F.lit("-"),
                                (F.floor(age / 5) * 5 + 4).cast("string"))))
region = F.when(F.col("code_region").rlike("^[0-9]{2}$"), F.col("code_region").cast("int")).otherwise(F.lit(-1))
w = Window.partitionBy("annee_deces").orderBy("cle_deces")
base = (silver.join(F.broadcast(decalages), "annee_deces")
        .withColumn("id_deces", (F.row_number().over(w) + F.col("decalage")).cast("int"))
        .select("id_deces", "annee_deces",
                F.date_format("date_deces", "yyyyMMdd").cast("int").alias("id_date"),
                region.alias("id_region"),
                F.when(F.col("sexe").isin("F", "M", "I"), F.col("sexe")).otherwise(F.lit("I")).alias("sexe"),
                tranche.alias("tranche_age")))
if dates_connues is not None:
    base = (base.join(F.broadcast(dates_connues), "id_date", "left")
                .withColumn("id_date", F.when(F.col("date_connue"), F.col("id_date")).otherwise(F.lit(-1)))
                .drop("date_connue"))
if regions_connues is not None:
    base = (base.join(F.broadcast(regions_connues), F.col("id_region") == F.col("id_region_dim"), "left")
                .withColumn("region_hors_dim", (F.col("id_region") != -1) & F.col("region_connue").isNull())
                .withColumn("id_region", F.when(F.col("region_connue").isNotNull(), F.col("id_region")).otherwise(F.lit(-1)))
                .drop("id_region_dim", "region_connue"))
else:
    base = base.withColumn("region_hors_dim", F.lit(False))

fait = base.select(
    "id_deces", F.coalesce("id_date", F.lit(-1)).cast("int").alias("id_date"),
    F.col("id_region").cast("int").alias("id_region"), "sexe", "tranche_age",
    F.lit(1).cast("short").alias("nb_deces"),
    F.lit(RUN_ID).alias("run_id"), F.current_timestamp().alias("date_chargement"),
    "annee_deces", "region_hors_dim",
).cache()


# ---------------------------------------------------------------- controles (un seul passage)
def nb_si(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


st = fait.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.countDistinct("id_deces").alias("distincts"),
    F.min("id_deces").alias("id_min"), F.max("id_deces").alias("id_max"),
    nb_si(F.col("id_date") == -1).alias("inconnu_id_date"),
    nb_si(F.col("id_region") == -1).alias("inconnu_id_region"),
    nb_si(F.col("id_region") == 99).alias("etranger"),
    nb_si(F.col("region_hors_dim")).alias("region_hors_dim_region"),
    nb_si(F.col("sexe") == "I").alias("sexe_inconnu"),
    nb_si(F.col("tranche_age").isNull()).alias("tranche_age_nulle"),
    nb_si(~F.col("sexe").isin("F", "M", "I")).alias("sexe_invalide"),
).first()
chrono("jointures_et_controles")
nb_fait = st["lignes"]
qc("lignes_fait", nb_fait)
qc("perte_silver_fait", nb_silver - nb_fait)
qc("doublons_id_deces", nb_fait - st["distincts"])
qc("id_deces_min_max", f"{st['id_min']}_{st['id_max']}")
qc("inconnu_id_date", st["inconnu_id_date"])
qc("inconnu_id_region", st["inconnu_id_region"])
qc("etranger_id_region_99", st["etranger"])
qc("region_absente_de_dim_region", st["region_hors_dim_region"])
qc("sexe_inconnu_I", st["sexe_inconnu"])
qc("tranche_age_nulle", st["tranche_age_nulle"])
if nb_fait != nb_silver:
    stop(f"lignes perdues ou dupliquees : Silver={nb_silver}, fait={nb_fait}")
if st["distincts"] != nb_fait:
    stop("id_deces en double : la cle primaire PostgreSQL serait refusee")
if st["sexe_invalide"]:
    stop("sexe hors F, M, I : refuse par le DDL")

# ---------------------------------------------------------------- ecriture
fait = fait.drop("annee_deces", "region_hors_dim")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BASE_SORTIE}")
fait.write.mode("overwrite").format("parquet").saveAsTable(f"{BASE_SORTIE}.fait_deces")
chrono("ecriture_hive")
relue = spark.table(f"{BASE_SORTIE}.fait_deces")
relu = relue.count()
qc("lignes_relues_hive", relu)
tr = {r["tranche_age"]: r["count"] for r in relue.groupBy("tranche_age").count().collect()}
qc("tranches_age", sorted(((k or "NULL"), v) for k, v in tr.items()))
chrono("relecture")
print(f"GOLD fait_deces mode={MODE} table={BASE_SORTIE}.fait_deces plage={ANNEE_MIN}-{ANNEE_MAX} "
      f"lignes={relu} run_id={RUN_ID}", flush=True)
spark.stop()
