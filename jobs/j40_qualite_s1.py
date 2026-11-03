"""
J40 (partie S1) - Rapport de qualite du Silver de la source S1.

Indicateurs produits (une ligne "QUALITE,entite,indicateur,colonne,valeur" par mesure) :
  lignes                 nombre de lignes de la table Silver
  bronze_lignes          lignes de la table Bronze correspondante (controle de volume, QC02/QC10)
  rejets                 lignes rejetees dans /silver/_rejets/<entite>, ventilees par code (QC03 a QC08)
  perte                  bronze - silver - rejets : doit etre 0
  completude_pct         pourcentage de valeurs renseignees par colonne (vide ou espaces = non renseigne)
  doublons_cle           lignes - cles distinctes (QC04)
  orphelins              references introuvables : consultation vers patient, professionnel, diagnostic (QC08)
  hors_domaine           valeurs hors plage (QC06, QC07)

Resultat : lignes affichees, et un CSV dans hdfs:///silver/_qualite/s1 (un seul fichier).
Recuperation :
  docker exec chu_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /jobs/j40_qualite_s1.py 2>&1 | grep '^QUALITE,' > docs/qualite/qualite_s1.csv

Lecture seule sur Bronze et Silver : sans danger pour les donnees.
"""
import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

spark = (SparkSession.builder.appName("j40_qualite_s1")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

H = "hdfs://namenode:8020"
RUN_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
LIGNES = []


def add(entite, indicateur, colonne, valeur):
    LIGNES.append((RUN_ID, entite, indicateur, colonne, str(valeur)))
    print("QUALITE,%s,%s,%s,%s" % (entite, indicateur, colonne, valeur))


def lire(chemin):
    try:
        return spark.read.parquet(H + chemin)
    except Exception:
        return None


# entite Silver : (cle naturelle, table Bronze correspondante)
ENTITES = {
    "patient":       ("patient_pseudo",        "Patient"),
    "professionnel": ("professionnel_pseudo",  "Professionnel_de_sante"),
    "diagnostic":    ("code_diagnostic",       "Diagnostic"),
    "specialites":   ("code_specialite",       "Specialites"),
    "consultation":  ("id_consultation",       "Consultation"),
}
SILVER = {}

for ent, (cle, bronze) in ENTITES.items():
    df = lire("/silver/" + ent)
    if df is None:
        add(ent, "lecture", "", "IMPOSSIBLE")
        continue
    SILVER[ent] = df
    n = df.count()
    add(ent, "lignes", "", n)

    # Volume : Bronze, rejets, perte
    b = lire("/bronze/s1/" + bronze)
    nb = b.count() if b is not None else None
    add(ent, "bronze_lignes", "", nb if nb is not None else "INCONNU")
    rej = lire("/silver/_rejets/" + ent)
    nrej = 0
    if rej is not None:
        for r in rej.groupBy("code_rejet").count().collect():
            add(ent, "rejets", r["code_rejet"], r["count"])
            nrej += r["count"]
    add(ent, "rejets", "TOTAL", nrej)
    if nb is not None:
        add(ent, "perte", "bronze-silver-rejets", nb - n - nrej)

    # Completude par colonne (hors colonne technique ingest_date)
    exprs = []
    for c in df.columns:
        if c == "ingest_date":
            continue
        manque = F.col(c).isNull()
        if isinstance(df.schema[c].dataType, StringType):
            manque = manque | (F.trim(F.col(c)) == "")
        exprs.append(F.sum(F.when(manque, 1).otherwise(0)).alias(c))
    manq = df.agg(*exprs).collect()[0]
    for c in manq.asDict():
        pct = 100.0 * (n - manq[c]) / n if n else 0.0
        add(ent, "completude_pct", c, "%.2f" % pct)

    # Doublons sur la cle naturelle
    add(ent, "doublons_cle", cle, n - df.select(cle).distinct().count())

# Orphelins : references de la consultation
cons = SILVER.get("consultation")
if cons is not None:
    for col, ref, refcol in (("patient_pseudo", "patient", "patient_pseudo"),
                             ("professionnel_pseudo", "professionnel", "professionnel_pseudo"),
                             ("code_diagnostic", "diagnostic", "code_diagnostic")):
        if ref in SILVER:
            o = (cons.filter(F.col(col).isNotNull())
                 .join(SILVER[ref].select(F.col(refcol).alias(col)).distinct(), col, "left_anti").count())
            add("consultation", "orphelins", col + "->" + ref, o)

    # Valeurs hors domaine / incoherences
    st = cons.agg(
        F.sum(F.when(F.col("duree_minutes") < 0, 1).otherwise(0)).alias("duree_negative"),
        F.sum(F.when(F.col("duree_minutes") > 1440, 1).otherwise(0)).alias("duree_sup_24h"),
        F.sum(F.when(F.col("date_consultation") > F.current_date(), 1).otherwise(0)).alias("date_future"),
        F.sum(F.when(F.col("heure_fin") < F.col("heure_debut"), 1).otherwise(0)).alias("heure_fin_avant_debut"),
        F.min("date_consultation").alias("date_min"), F.max("date_consultation").alias("date_max"),
        F.min("duree_minutes").alias("duree_min"), F.max("duree_minutes").alias("duree_max"),
    ).collect()[0]
    for k, v in st.asDict().items():
        add("consultation", "hors_domaine" if k in ("duree_negative", "duree_sup_24h", "date_future", "heure_fin_avant_debut") else "etendue", k, v)
    if "patient" in SILVER:
        pass

pat = SILVER.get("patient")
if pat is not None:
    an = F.year(F.current_date())
    st = pat.agg(
        F.sum(F.when((F.col("annee_naissance") < 1900) | (F.col("annee_naissance") > an), 1).otherwise(0)).alias("annee_naissance_hors_1900_aujourdhui"),
        F.min("annee_naissance").alias("annee_naissance_min"), F.max("annee_naissance").alias("annee_naissance_max"),
    ).collect()[0]
    for k, v in st.asDict().items():
        add("patient", "hors_domaine" if k.startswith("annee_naissance_hors") else "etendue", k, v)
    for r in pat.groupBy("sexe").count().orderBy(F.desc("count")).limit(10).collect():
        add("patient", "repartition_sexe", r["sexe"], r["count"])

# Fichier CSV unique dans HDFS (dossier ouvert a tous : /silver est en 777)
(spark.createDataFrame(LIGNES, ["run_id", "entite", "indicateur", "colonne", "valeur"])
 .coalesce(1).write.mode("overwrite").option("header", "true").csv(H + "/silver/_qualite/s1"))
print("QUALITE_FIN run_id=%s lignes=%d" % (RUN_ID, len(LIGNES)))
