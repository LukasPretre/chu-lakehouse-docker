"""
J30 - Gold : fait_consultation (Hive, base chu_gold).

Entrée  : Silver hdfs://namenode:8020/silver/consultation
          + dimensions Hive chu_gold.dim_patient, dim_professionnel, dim_diagnostic,
            dim_temps, dim_etablissement (membre 2, J20 à J25 et J22)
Sortie  : table Hive chu_gold.fait_consultation, Parquet,
          partitionnée par annee, 16 buckets sur id_patient (choix validé par le benchmark).

Règles
- Une ligne Silver = une ligne de fait : aucune ligne n'est perdue. Une clé introuvable
  dans une dimension devient -1 (« Inconnu »), et elle est comptée.
- id_date = AAAAMMJJ (entier), comme la contrainte ck_temps_cle du DDL.
- duree_minutes vient du Silver (déjà contrôlée : fin - début, + 1440 au passage de minuit).
- heure_debut / heure_fin sont stockées en texte HH:mm:ss (Hive n'a pas de type TIME) ;
  J34 les convertit en TIME pour PostgreSQL.
- La colonne de partition annee n'existe pas dans le DDL PostgreSQL : J34 ne la copie pas.
- id_etablissement = -1 pour l'instant : la règle Q2 du Livrable 1 (établissement déduit
  de l'activité du professionnel, J15) sera branchée quand le Silver du membre 3 existera.
- Rechargement complet à chaque exécution (rejouable) : le fait est recalculé à partir du
  Silver et des dimensions courantes, dont les clés sont stables.

Modes
- J30_MODE=normal (défaut) : lit les dimensions dans chu_gold ; s'arrête si l'une manque.
- J30_MODE=test : construit des dimensions provisoires en mémoire à partir du Silver,
  écrit dans chu_test.fait_consultation. Sert à valider le job avant les vraies dimensions.

Sortie console : lignes « GOLDQC,indicateur,valeur » puis « GOLD fait_consultation ... ».
"""
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

MODE = os.environ.get("J30_MODE", "normal")
BASE_DIM = "chu_gold"
BASE_SORTIE = "chu_test" if MODE == "test" else "chu_gold"
SILVER = "hdfs://namenode:8020/silver"
NB_BUCKETS = 16
RUN_ID = os.environ.get("RUN_ID", "J30_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))

spark = (SparkSession.builder.appName("j30_fait_consultation").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def qc(indicateur, valeur):
    print(f"GOLDQC,{indicateur},{valeur}", flush=True)


def stop(message):
    print(f"GOLD ERREUR {message}", flush=True)
    spark.stop()
    sys.exit(1)


def table_existe(base, table):
    return spark.catalog.tableExists(f"{base}.{table}")


def verifier_unicite(df, cle, nom):
    """Une clé naturelle dupliquée dans une dimension dupliquerait les lignes du fait."""
    doublons = df.groupBy(cle).count().filter("count > 1").count()
    qc(f"{nom}_doublons_cle_naturelle", doublons)
    if doublons:
        stop(f"{nom} : {doublons} clé(s) naturelle(s) en double ({cle}) : corriger la dimension")


# ---------------------------------------------------------------- dimensions
def dim_provisoire(chemin, cle, id_nom):
    """Mode test : clé de substitution provisoire 1..n, sans ligne -1 (le coalesce la remplace)."""
    df = spark.read.parquet(f"{SILVER}/{chemin}").select(cle).distinct()
    return df.withColumn(id_nom, F.row_number().over(Window.orderBy(cle)).cast("int"))


if MODE == "test":
    dim_patient = dim_provisoire("patient", "patient_pseudo", "id_patient")
    dim_prof = (dim_provisoire("professionnel", "professionnel_pseudo", "id_professionnel")
                .withColumnRenamed("professionnel_pseudo", "identifiant_pseudo"))
    dim_diag = dim_provisoire("diagnostic", "code_diagnostic", "id_diagnostic")
    dates_connues = None
elif MODE == "normal":
    manquantes = [t for t in ("dim_patient", "dim_professionnel", "dim_diagnostic", "dim_temps")
                  if not table_existe(BASE_DIM, t)]
    if manquantes:
        stop(f"dimension(s) absente(s) dans {BASE_DIM} : {', '.join(manquantes)} "
             f"(J20 à J25 de la membre 2). Pour tester avant : J30_MODE=test")
    dim_patient = spark.table(f"{BASE_DIM}.dim_patient").select("patient_pseudo", "id_patient")
    dim_prof = spark.table(f"{BASE_DIM}.dim_professionnel").select("identifiant_pseudo", "id_professionnel")
    dim_diag = spark.table(f"{BASE_DIM}.dim_diagnostic").select("code_diagnostic", "id_diagnostic")
    dates_connues = (spark.table(f"{BASE_DIM}.dim_temps").select("id_date")
                     .filter("id_date <> -1").withColumn("date_connue", F.lit(True)))
else:
    stop(f"J30_MODE inconnu : {MODE} (normal ou test)")

# Les lignes -1 ne servent pas à la jointure (elles n'ont pas de clé naturelle réelle).
dim_patient = dim_patient.filter("id_patient <> -1")
dim_prof = dim_prof.filter("id_professionnel <> -1")
dim_diag = dim_diag.filter("id_diagnostic <> -1")
verifier_unicite(dim_patient, "patient_pseudo", "dim_patient")
verifier_unicite(dim_prof, "identifiant_pseudo", "dim_professionnel")
verifier_unicite(dim_diag, "code_diagnostic", "dim_diagnostic")

# ---------------------------------------------------------------- Silver
silver = spark.read.parquet(f"{SILVER}/consultation")
nb_silver = silver.count()
qc("lignes_silver", nb_silver)
types = dict(silver.dtypes)


def heure_texte(col):
    """HH:mm:ss en texte, que le Silver stocke l'heure en timestamp ou en texte."""
    if types.get(col) == "timestamp":
        return F.date_format(F.col(col), "HH:mm:ss")
    return F.col(col).cast("string")


base = silver.select(
    F.col("id_consultation").cast("long").alias("id_consultation_long"),
    F.coalesce(F.date_format("date_consultation", "yyyyMMdd").cast("int"), F.lit(-1)).alias("id_date"),
    "patient_pseudo", "professionnel_pseudo", "code_diagnostic",
    heure_texte("heure_debut").alias("heure_debut"),
    heure_texte("heure_fin").alias("heure_fin"),
    F.col("duree_minutes").cast("short").alias("duree_minutes"),
)

# id_consultation est INTEGER dans le DDL : vérifier qu'aucune valeur ne déborde.
trop_grands = base.filter(F.col("id_consultation_long") > 2147483647).count()
qc("id_consultation_hors_integer", trop_grands)
if trop_grands:
    stop(f"{trop_grands} id_consultation dépassent la capacité d'un INTEGER")

# ---------------------------------------------------------------- clés
dim_prof = dim_prof.withColumnRenamed("identifiant_pseudo", "professionnel_pseudo")
fait = (base
        .join(F.broadcast(dim_diag), "code_diagnostic", "left")
        .join(dim_patient, "patient_pseudo", "left")
        .join(dim_prof, "professionnel_pseudo", "left"))

if dates_connues is not None:
    fait = fait.join(F.broadcast(dates_connues), "id_date", "left")
    fait = fait.withColumn("id_date", F.when(F.col("date_connue"), F.col("id_date")).otherwise(F.lit(-1)))

fait = fait.select(
    F.col("id_consultation_long").cast("int").alias("id_consultation"),
    F.col("id_date").cast("int").alias("id_date"),
    F.coalesce("id_patient", F.lit(-1)).cast("int").alias("id_patient"),
    F.coalesce("id_professionnel", F.lit(-1)).cast("int").alias("id_professionnel"),
    F.coalesce("id_diagnostic", F.lit(-1)).cast("int").alias("id_diagnostic"),
    F.lit(-1).cast("int").alias("id_etablissement"),   # règle Q2 à brancher après J15
    "heure_debut", "heure_fin", "duree_minutes",
    F.lit(1).cast("short").alias("nb_consultations"),
    F.lit(RUN_ID).alias("run_id"),
    F.current_timestamp().alias("date_chargement"),
).withColumn("annee", F.when(F.col("id_date") > 0, (F.col("id_date") / 10000).cast("int")).otherwise(F.lit(-1)))

fait = fait.cache()

# ---------------------------------------------------------------- contrôles
nb_fait = fait.count()
qc("lignes_fait", nb_fait)
qc("perte_silver_fait", nb_silver - nb_fait)
qc("doublons_id_consultation", nb_fait - fait.select("id_consultation").distinct().count())
for cle in ("id_date", "id_patient", "id_professionnel", "id_diagnostic", "id_etablissement"):
    qc(f"inconnu_{cle}", fait.filter(F.col(cle) == -1).count())
qc("duree_hors_0_1440", fait.filter(~F.col("duree_minutes").between(0, 1440)).count())
qc("duree_nulle_absente", fait.filter(F.col("duree_minutes").isNull()).count())

if nb_fait != nb_silver:
    stop(f"lignes perdues ou dupliquées : Silver={nb_silver}, fait={nb_fait}")

# ---------------------------------------------------------------- écriture
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BASE_SORTIE}")
(fait.repartition(NB_BUCKETS, "id_patient")
     .write.mode("overwrite").format("parquet")
     .partitionBy("annee")
     .bucketBy(NB_BUCKETS, "id_patient").sortBy("id_patient")
     .saveAsTable(f"{BASE_SORTIE}.fait_consultation"))

relu = spark.table(f"{BASE_SORTIE}.fait_consultation").count()
qc("lignes_relues_hive", relu)
for ligne in (spark.table(f"{BASE_SORTIE}.fait_consultation")
              .groupBy("annee").count().orderBy("annee").collect()):
    qc(f"lignes_annee_{ligne['annee']}", ligne["count"])

print(f"GOLD fait_consultation mode={MODE} table={BASE_SORTIE}.fait_consultation "
      f"lignes={relu} run_id={RUN_ID}", flush=True)
spark.stop()
