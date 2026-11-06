"""
J31 - Gold : fait_hospitalisation (Hive, base chu_gold).

Entree  : Silver hdfs://namenode:8020/silver/hospitalisation (J16)
          + dimensions Hive chu_gold.dim_patient, dim_diagnostic, dim_temps, dim_etablissement
Sortie  : table Hive chu_gold.fait_hospitalisation (Parquet), colonnes du DDL (sql/gold_ddl.sql).

Regles
- Une ligne Silver = une ligne de fait : aucune ligne n'est perdue. Une cle introuvable dans une
  dimension devient -1 (Inconnu), et elle est comptee.
- id_hospitalisation = num_hospitalisation (cle degeneree, VARCHAR(20)).
- id_date_entree / id_date_sortie = AAAAMMJJ ; id_date_sortie = -1 si la duree est inconnue ;
  une date absente de dim_temps donne aussi -1.
- id_etablissement : identifiant_organisation -> version SCD2 de dim_etablissement valable a la date
  d'entree, a defaut la version courante ; -1 si introuvable ou si dim_etablissement n'existe pas encore
  (controle q_dim_etablissement_absente).
- Rechargement complet a chaque execution (rejouable).

Modes
- J31_MODE=normal (defaut) : lit les dimensions dans chu_gold ; s'arrete si l'une manque.
- J31_MODE=test : dimensions provisoires construites en memoire a partir du Silver, ecrit dans
  chu_test.fait_hospitalisation. Sert a valider le job avant les vraies dimensions.

Sortie console : lignes GOLDQC,indicateur,valeur puis GOLD fait_hospitalisation ...
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


from pyspark.sql import SparkSession, functions as F

MODE = os.environ.get("J31_MODE", "normal")
BASE_DIM = "chu_gold"
BASE_SORTIE = "chu_test" if MODE == "test" else "chu_gold"
SILVER = "hdfs://namenode:8020/silver"
RUN_ID = os.environ.get("RUN_ID", "J31_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))

spark = (SparkSession.builder.appName("j31_fait_hospitalisation").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.environ.get("J31_PARTITIONS", "8"))
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


def dim_provisoire(chemin, cle, id_nom):
    """Mode test : cle de substitution provisoire 1..n (zipWithIndex : numerotation distribuee)."""
    df = spark.read.parquet(f"{SILVER}/{chemin}").select(cle).distinct()
    return (df.rdd.zipWithIndex().map(lambda t: (t[0][0], t[1] + 1))
              .toDF(f"{cle} string, {id_nom} long").withColumn(id_nom, F.col(id_nom).cast("int")))


# ---------------------------------------------------------------- dimensions
if MODE == "test":
    dim_patient = dim_provisoire("patient", "patient_pseudo", "id_patient")
    dim_diag = dim_provisoire("diagnostic", "code_diagnostic", "id_diagnostic")
    dates_connues = None
    dim_etab = (dim_provisoire("etablissement", "identifiant_organisation", "id_etablissement")
                .withColumn("date_debut", F.lit(None).cast("date"))
                .withColumn("date_fin", F.lit(None).cast("date"))
                .withColumn("est_courant", F.lit(True)))
elif MODE == "normal":
    manquantes = [t for t in ("dim_patient", "dim_diagnostic", "dim_temps") if not table_existe(BASE_DIM, t)]
    if manquantes:
        stop(f"dimension(s) absente(s) dans {BASE_DIM} : {', '.join(manquantes)} "
             f"(J20 a J25 de la membre 2). Pour tester avant : J31_MODE=test")
    dim_patient = spark.table(f"{BASE_DIM}.dim_patient").select("patient_pseudo", "id_patient")
    dim_diag = spark.table(f"{BASE_DIM}.dim_diagnostic").select("code_diagnostic", "id_diagnostic")
    dates_connues = (spark.table(f"{BASE_DIM}.dim_temps").select("id_date")
                     .filter("id_date <> -1").withColumn("date_connue", F.lit(True)))
    dim_etab = (spark.table(f"{BASE_DIM}.dim_etablissement")
                .select("identifiant_organisation", "id_etablissement", "date_debut", "date_fin", "est_courant")
                if table_existe(BASE_DIM, "dim_etablissement") else None)
else:
    stop(f"J31_MODE inconnu : {MODE} (normal ou test)")

# les lignes -1 ne servent pas a la jointure
dim_patient = dim_patient.filter("id_patient <> -1")
dim_diag = dim_diag.filter("id_diagnostic <> -1")

# ---------------------------------------------------------------- Silver
try:
    silver = spark.read.parquet(f"{SILVER}/hospitalisation")
except Exception:
    print("GOLD fait_hospitalisation ignoré : silver/hospitalisation absent (lancer J16)", flush=True)
    spark.stop()
    sys.exit(0)
base = silver.select(
    F.col("num_hospitalisation").alias("id_hospitalisation"),
    "patient_pseudo", "identifiant_organisation", "code_diagnostic",
    F.date_format("date_entree", "yyyyMMdd").cast("int").alias("id_date_entree"),
    F.date_format("date_sortie", "yyyyMMdd").cast("int").alias("id_date_sortie"),
    F.col("duree_sejour_jours").cast("short").alias("duree_sejour_jours"),
    F.col("date_entree").cast("date").alias("dt"),
)
nb_silver = base.count()
qc("lignes_silver", nb_silver)
chrono("lecture_silver")

# ---------------------------------------------------------------- etablissement (version SCD2 a la date d'entree)
if dim_etab is None:
    qc("q_dim_etablissement_absente", 1)
    base = base.withColumn("id_etablissement", F.lit(None).cast("int"))
else:
    versions = dim_etab.filter("id_etablissement <> -1")
    valable = ((F.col("date_debut").isNull() | (F.col("date_debut") <= F.col("dt")))
               & (F.col("date_fin").isNull() | (F.col("dt") <= F.col("date_fin"))))
    cand = (base.select("id_hospitalisation", "dt", "identifiant_organisation")
                .filter("identifiant_organisation IS NOT NULL")
                .join(versions, "identifiant_organisation")
                .withColumn("ordre", F.when(valable, 0).when(F.col("est_courant"), 1).otherwise(2))
                .filter("ordre < 2"))
    # une seule version par hospitalisation : la valable a la date, sinon la courante
    meilleur = (cand.groupBy("id_hospitalisation")
                    .agg(F.min(F.struct("ordre", F.col("id_etablissement").alias("e"))).alias("m"))
                    .select("id_hospitalisation", F.col("m.e").alias("id_etablissement")))
    base = base.join(meilleur, "id_hospitalisation", "left")

# ---------------------------------------------------------------- cles
fait = (base.join(F.broadcast(dim_diag), "code_diagnostic", "left")
            .join(dim_patient, "patient_pseudo", "left"))
if dates_connues is not None:
    fait = (fait.join(F.broadcast(dates_connues.withColumnRenamed("id_date", "id_date_entree")
                                  .withColumnRenamed("date_connue", "entree_connue")), "id_date_entree", "left")
                .join(F.broadcast(dates_connues.withColumnRenamed("id_date", "id_date_sortie")
                                  .withColumnRenamed("date_connue", "sortie_connue")), "id_date_sortie", "left")
                .withColumn("id_date_entree", F.when(F.col("entree_connue"), F.col("id_date_entree")).otherwise(F.lit(-1)))
                .withColumn("id_date_sortie", F.when(F.col("sortie_connue"), F.col("id_date_sortie")).otherwise(F.lit(-1))))
fait = fait.select(
    "id_hospitalisation",
    F.coalesce("id_date_entree", F.lit(-1)).cast("int").alias("id_date_entree"),
    F.coalesce("id_date_sortie", F.lit(-1)).cast("int").alias("id_date_sortie"),
    F.coalesce("id_patient", F.lit(-1)).cast("int").alias("id_patient"),
    F.coalesce("id_etablissement", F.lit(-1)).cast("int").alias("id_etablissement"),
    F.coalesce("id_diagnostic", F.lit(-1)).cast("int").alias("id_diagnostic"),
    "duree_sejour_jours",
    F.lit(1).cast("short").alias("nb_hospitalisations"),
    F.lit(RUN_ID).alias("run_id"),
    F.current_timestamp().alias("date_chargement"),
).cache()


# ---------------------------------------------------------------- controles (un seul passage)
def nb_si(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


st = fait.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.countDistinct("id_hospitalisation").alias("distincts"),
    *[nb_si(F.col(c) == -1).alias("inconnu_" + c)
      for c in ("id_date_entree", "id_date_sortie", "id_patient", "id_etablissement", "id_diagnostic")],
    nb_si(F.col("duree_sejour_jours").isNull()).alias("duree_nulle"),
    nb_si(F.col("duree_sejour_jours") < 0).alias("duree_negative"),
    nb_si((F.col("id_date_sortie") != -1) & (F.col("id_date_sortie") < F.col("id_date_entree"))).alias("sortie_avant_entree"),
    nb_si(F.length("id_hospitalisation") > 20).alias("id_trop_long"),
).first()
chrono("jointures_et_controles")
nb_fait = st["lignes"]
qc("lignes_fait", nb_fait)
qc("perte_silver_fait", nb_silver - nb_fait)
qc("doublons_id_hospitalisation", nb_fait - st["distincts"])
for c in ("id_date_entree", "id_date_sortie", "id_patient", "id_etablissement", "id_diagnostic"):
    qc(f"inconnu_{c}", st["inconnu_" + c])
qc("duree_nulle", st["duree_nulle"])
qc("duree_negative", st["duree_negative"])
qc("sortie_avant_entree", st["sortie_avant_entree"])
qc("id_hospitalisation_plus_de_20_caracteres", st["id_trop_long"])

if nb_fait != nb_silver:
    stop(f"lignes perdues ou dupliquees : Silver={nb_silver}, fait={nb_fait}")
if st["distincts"] != nb_fait:
    stop("id_hospitalisation en double : la cle primaire PostgreSQL serait refusee")
if st["id_trop_long"] or st["duree_negative"] or st["sortie_avant_entree"]:
    stop("valeur refusee par les contraintes du DDL (id trop long, duree negative ou sortie avant entree)")

# ---------------------------------------------------------------- ecriture
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BASE_SORTIE}")
fait.write.mode("overwrite").format("parquet").saveAsTable(f"{BASE_SORTIE}.fait_hospitalisation")
chrono("ecriture_hive")
relu = spark.table(f"{BASE_SORTIE}.fait_hospitalisation").count()
qc("lignes_relues_hive", relu)
chrono("relecture")
print(f"GOLD fait_hospitalisation mode={MODE} table={BASE_SORTIE}.fait_hospitalisation "
      f"lignes={relu} run_id={RUN_ID}", flush=True)
spark.stop()
