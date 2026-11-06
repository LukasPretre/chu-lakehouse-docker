"""
J33 - Gold : fait_satisfaction (Hive, base chu_gold).

Entree  : Silver hdfs://namenode:8020/silver/esatis (J17)
          + dimensions Hive chu_gold.dim_temps et dim_etablissement (finess_site = finess_geo)
Sortie  : table Hive chu_gold.fait_satisfaction (Parquet), colonnes du DDL (sql/gold_ddl.sql).

Regles
- Une mesure par etablissement, annee de campagne et type d'enquete (48H ou CA).
- id_date = 1er janvier de l'annee de campagne (decision de l'equipe), au format AAAAMMJJ.
- id_etablissement : finess_geo du Silver = finess_site de dim_etablissement ; version SCD2 valable
  a cette date, a defaut la version courante ; -1 si introuvable ou si dim_etablissement n'existe pas
  encore (controle q_dim_etablissement_absente). Les lignes en -1 sont conservees une par une.
- Si plusieurs lignes du Silver aboutissent au meme (id_date, id_etablissement, type_enquete), elles sont
  regroupees : scores et taux ponderes par nb_reponses (jamais de moyenne simple), nb_reponses additionne,
  classement et evolution pris sur la ligne qui a le plus de reponses. Le nombre de regroupements est compte.
- scores en DECIMAL(5,2), deja ramenes a NULL hors 0-100 par J17.
- id_satisfaction : numero 1..n, ordre (id_date, type_enquete, id_etablissement, finess_geo).
- Rechargement complet a chaque execution (rejouable).

Modes
- J33_MODE=normal (defaut) : lit dim_temps (et dim_etablissement si elle existe) dans chu_gold.
- J33_MODE=test : dimension etablissement provisoire construite a partir du Silver, ecrit dans
  chu_test.fait_satisfaction.

Sortie console : lignes GOLDQC,indicateur,valeur puis GOLD fait_satisfaction ...
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

MODE = os.environ.get("J33_MODE", "normal")
BASE_DIM = "chu_gold"
BASE_SORTIE = "chu_test" if MODE == "test" else "chu_gold"
SILVER = "hdfs://namenode:8020/silver"
RUN_ID = os.environ.get("RUN_ID", "J33_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
SCORES = ["score_global", "score_accueil", "score_pec_infirmier", "score_pec_medical",
          "score_chambre", "score_repas", "score_sortie"]
MESURES = SCORES + ["taux_recommandation"]

spark = (SparkSession.builder.appName("j33_fait_satisfaction").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.environ.get("J33_PARTITIONS", "8"))
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


# ---------------------------------------------------------------- dimensions
if MODE == "test":
    dates_connues = None
    prov = (spark.read.parquet(f"{SILVER}/etablissement").filter("finess_geo IS NOT NULL")
            .select("finess_geo").distinct())
    dim_etab = (prov.rdd.zipWithIndex().map(lambda t: (t[0][0], t[1] + 1))
                .toDF("finess_site string, id_etablissement long")
                .withColumn("id_etablissement", F.col("id_etablissement").cast("int"))
                .withColumn("date_debut", F.lit(None).cast("date"))
                .withColumn("date_fin", F.lit(None).cast("date"))
                .withColumn("est_courant", F.lit(True)))
elif MODE == "normal":
    if not table_existe(BASE_DIM, "dim_temps"):
        stop(f"dim_temps absente dans {BASE_DIM} (J20). Pour tester avant : J33_MODE=test")
    dates_connues = (spark.table(f"{BASE_DIM}.dim_temps").select("id_date")
                     .filter("id_date <> -1").withColumn("date_connue", F.lit(True)))
    dim_etab = (spark.table(f"{BASE_DIM}.dim_etablissement")
                .select("finess_site", "id_etablissement", "date_debut", "date_fin", "est_courant")
                if table_existe(BASE_DIM, "dim_etablissement") else None)
else:
    stop(f"J33_MODE inconnu : {MODE} (normal ou test)")

# ---------------------------------------------------------------- Silver
silver = spark.read.parquet(f"{SILVER}/esatis")
attendues = ["annee", "type_enquete", "finess_geo", "classement", "evolution", "nb_reponses"] + MESURES
manquantes = [c for c in attendues if c not in silver.columns]
if manquantes:
    stop(f"silver/esatis : colonne(s) absente(s) {manquantes} ; colonnes trouvees : {silver.columns}")
base = silver.select(
    "annee", "type_enquete", "finess_geo", "classement", "evolution",
    F.col("nb_reponses").cast("int").alias("nb_reponses"),
    *[F.col(c).cast("double").alias(c) for c in MESURES],
).withColumn("id_date", (F.col("annee") * 10000 + 101).cast("int")) \
 .withColumn("dt", F.to_date(F.concat_ws("-", F.col("annee").cast("string"), F.lit("01"), F.lit("01"))))
nb_silver = base.count()
qc("lignes_silver", nb_silver)
chrono("lecture_silver")

# ---------------------------------------------------------------- etablissement (finess_geo = finess_site, SCD2)
if dim_etab is None:
    qc("q_dim_etablissement_absente", 1)
    base = base.withColumn("id_etablissement", F.lit(-1))
else:
    versions = dim_etab.filter("id_etablissement <> -1").filter("finess_site IS NOT NULL")
    valable = ((F.col("date_debut").isNull() | (F.col("date_debut") <= F.col("dt")))
               & (F.col("date_fin").isNull() | (F.col("dt") <= F.col("date_fin"))))
    cand = (base.select("annee", "type_enquete", "finess_geo", "dt")
                .join(versions, F.col("finess_geo") == F.col("finess_site"))
                .withColumn("ordre", F.when(valable, 0).when(F.col("est_courant"), 1).otherwise(2))
                .filter("ordre < 2"))
    meilleur = (cand.groupBy("annee", "type_enquete", "finess_geo")
                    .agg(F.min(F.struct("ordre", F.col("id_etablissement").alias("e"))).alias("m"))
                    .select("annee", "type_enquete", "finess_geo", F.col("m.e").alias("id_etablissement")))
    base = (base.join(meilleur, ["annee", "type_enquete", "finess_geo"], "left")
                .withColumn("id_etablissement", F.coalesce("id_etablissement", F.lit(-1))))

# date absente de dim_temps -> -1
if dates_connues is not None:
    base = (base.join(F.broadcast(dates_connues), "id_date", "left")
                .withColumn("id_date", F.when(F.col("date_connue"), F.col("id_date")).otherwise(F.lit(-1))))

# ---------------------------------------------------------------- regroupement pondere (cas ou plusieurs lignes -> 1 etablissement)
def pondere(c):
    w = F.coalesce(F.col("nb_reponses"), F.lit(0)).cast("double")
    numerateur = F.sum(F.when(F.col(c).isNotNull(), F.col(c) * w))
    denominateur = F.sum(F.when(F.col(c).isNotNull(), w))
    return F.when(denominateur > 0, numerateur / denominateur).otherwise(F.avg(c))


connus = base.filter("id_etablissement <> -1")
inconnus = base.filter("id_etablissement = -1")
regroupes = (connus.groupBy("id_date", "id_etablissement", "type_enquete")
             .agg(F.count(F.lit(1)).alias("nb_lignes"), F.min("finess_geo").alias("finess_geo"),
                  F.sum("nb_reponses").cast("int").alias("nb_reponses"),
                  F.max_by("classement", F.coalesce(F.col("nb_reponses"), F.lit(0))).alias("classement"),
                  F.max_by("evolution", F.coalesce(F.col("nb_reponses"), F.lit(0))).alias("evolution"),
                  *[pondere(c).alias(c) for c in MESURES]))
colonnes_fusion = ["id_date", "id_etablissement", "type_enquete", "finess_geo", "nb_lignes", "nb_reponses",
                   "classement", "evolution"] + MESURES
isoles = inconnus.withColumn("nb_lignes", F.lit(1).cast("long")).select(*colonnes_fusion)
tout = regroupes.select(*colonnes_fusion).unionByName(isoles)

w_id = Window.orderBy("id_date", "type_enquete", "id_etablissement", "finess_geo")
fait = tout.withColumn("id_satisfaction", F.row_number().over(w_id).cast("int")).select(
    "id_satisfaction", F.col("id_date").cast("int").alias("id_date"),
    F.col("id_etablissement").cast("int").alias("id_etablissement"),
    "type_enquete", "classement", "evolution", "nb_reponses",
    *[F.col(c).cast("decimal(5,2)").alias(c) for c in MESURES],
    F.lit(RUN_ID).alias("run_id"), F.current_timestamp().alias("date_chargement"),
    "nb_lignes", "finess_geo",
).cache()


# ---------------------------------------------------------------- controles (un seul passage)
def nb_si(condition):
    return F.sum(F.when(condition, 1).otherwise(0))


hors = None
for c in MESURES:
    cond = F.col(c).isNotNull() & ((F.col(c) < 0) | (F.col(c) > 100))
    hors = cond if hors is None else (hors | cond)
st = fait.agg(
    F.count(F.lit(1)).alias("lignes"),
    F.countDistinct("id_satisfaction").alias("distincts"),
    nb_si(F.col("id_date") == -1).alias("inconnu_id_date"),
    nb_si(F.col("id_etablissement") == -1).alias("inconnu_id_etablissement"),
    nb_si(F.col("nb_lignes") > 1).alias("regroupements"),
    nb_si(~F.col("type_enquete").isin("48H", "CA")).alias("type_invalide"),
    nb_si(F.col("classement").isNotNull() & ~F.col("classement").isin("A", "B", "C", "D", "DI", "NV", "NR")).alias("classement_invalide"),
    nb_si(F.length("evolution") > 30).alias("evolution_trop_longue"),
    nb_si(F.col("nb_reponses") < 0).alias("reponses_negatives"),
    nb_si(hors).alias("score_hors_0_100"),
    nb_si(F.col("score_global").isNull()).alias("score_global_nul"),
).first()
chrono("jointures_et_controles")
nb_fait = st["lignes"]
qc("lignes_fait", nb_fait)
qc("lignes_fusionnees_ponderees", nb_silver - nb_fait)
qc("regroupements_ponderes", st["regroupements"])
qc("inconnu_id_date", st["inconnu_id_date"])
qc("inconnu_id_etablissement", st["inconnu_id_etablissement"])
qc("score_global_nul", st["score_global_nul"])
for ligne in (fait.groupBy("id_date", "type_enquete")
                  .agg(F.count(F.lit(1)).alias("n"), nb_si(F.col("id_etablissement") == -1).alias("inconnus"))
                  .orderBy("id_date", "type_enquete").collect()):
    qc(f"lignes_{ligne['id_date']}_{ligne['type_enquete']}_dont_etablissement_inconnu",
       f"{ligne['n']}_{ligne['inconnus']}")

if st["distincts"] != nb_fait:
    stop("id_satisfaction en double")
if nb_fait > nb_silver:
    stop(f"plus de lignes dans le fait ({nb_fait}) que dans le Silver ({nb_silver})")
if (st["type_invalide"] or st["classement_invalide"] or st["evolution_trop_longue"]
        or st["reponses_negatives"] or st["score_hors_0_100"]):
    stop("valeur refusee par les contraintes du DDL (type, classement, evolution, reponses ou score)")
doublons = (fait.filter("id_etablissement <> -1")
                .groupBy("id_date", "id_etablissement", "type_enquete").count().filter("count > 1").count())
qc("doublons_etablissement_annee_enquete", doublons)
if doublons:
    stop("plusieurs mesures pour un meme etablissement, une meme annee et une meme enquete")

# ---------------------------------------------------------------- ecriture
fait = fait.drop("nb_lignes", "finess_geo")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BASE_SORTIE}")
fait.write.mode("overwrite").format("parquet").saveAsTable(f"{BASE_SORTIE}.fait_satisfaction")
chrono("ecriture_hive")
relu = spark.table(f"{BASE_SORTIE}.fait_satisfaction").count()
qc("lignes_relues_hive", relu)
chrono("relecture")
print(f"GOLD fait_satisfaction mode={MODE} table={BASE_SORTIE}.fait_satisfaction "
      f"lignes={relu} run_id={RUN_ID}", flush=True)
spark.stop()
