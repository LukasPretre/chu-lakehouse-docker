"""
J40 (partie S2) - Rapport de qualité du Silver de la source S2 (fichiers déposés) :
établissements (J14), activité des professionnels (J15), hospitalisations (J16).

Indicateurs (une ligne « QUALITE,entite,indicateur,colonne,valeur » par mesure) :
  lignes, bronze_lignes      volumes Silver et Bronze (dernière partition ingest_date, comme J14 à J16)
  rejets, perte              rejets par code ; bronze - silver - rejets (doit valoir 0)
  completude_pct             % de valeurs renseignées par colonne (vide ou espaces = non renseigné)
  valeur_par_defaut          nombre de « Non renseigné(e) » : complété par défaut, donc pas une vraie valeur
  doublons_cle               lignes - clés distinctes (clé naturelle de chaque entité)
  finess_*                   cohérence des codes FINESS : présence, format (9 caractères : 2 pour le
                             département, 2A/2B en Corse, puis 7 chiffres), département du FINESS
                             = département de l'établissement, doublons
  orphelins                  références introuvables (activité, hospitalisation)
  hors_domaine, etendue      durées et dates des hospitalisations
  troncature_s1              comparaison du fichier S2 professionnel_sante avec la table S1

Les colonnes facultatives sont contrôlées si elles existent (sinon « ABSENTE ») : le job ne
casse pas si un nom change. Lecture seule sur Bronze et Silver.
Résultat : lignes affichées, et un CSV dans hdfs:///silver/_qualite/s2. Récupération :
  docker exec chu_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
      /jobs/j40_qualite_s2.py 2>&1 | grep '^QUALITE,' > docs/qualite/qualite_s2.csv
"""
import datetime
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

H = os.environ.get("HDFS_BASE", "hdfs://namenode:8020")
spark = (SparkSession.builder.appName("j40_qualite_s2")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
RUN_ID = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
LIGNES = []
TECHNIQUES = {"ingest_date", "run_id", "date_chargement"}
DEFAUTS = ["Non renseigné", "Non renseignée"]
FINESS = r"^([0-9]{2}|2A|2B)[0-9]{7}$"


def add(entite, indicateur, colonne, valeur):
    LIGNES.append((RUN_ID, entite, indicateur, colonne, str(valeur)))
    print("QUALITE,%s,%s,%s,%s" % (entite, indicateur, colonne, valeur), flush=True)


def lire(chemin):
    try:
        return spark.read.parquet(H + chemin)
    except Exception:
        return None


def derniere_partition(df):
    if df is None or "ingest_date" not in df.columns:
        return df
    return df.filter(F.col("ingest_date") == df.agg(F.max("ingest_date")).first()[0])


def premiere(df, candidates):
    return next((c for c in candidates if c in df.columns), None)


# entité Silver : (clé naturelle, table Bronze S2)
ENTITES = {
    "etablissement":          (["identifiant_organisation"], "etablissement_sante"),
    "activite_professionnel": (["professionnel_pseudo", "identifiant_organisation", "mode_exercice"],
                               "activite_professionnel_sante"),
    "hospitalisation":        (["num_hospitalisation"], "hospitalisation"),
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

    b = derniere_partition(lire("/bronze/s2/" + bronze))
    nb = b.count() if b is not None else None
    add(ent, "bronze_lignes", "", nb if nb is not None else "INCONNU")
    rej = lire("/silver/_rejets/" + ent)
    nrej = 0
    if rej is not None and "code_rejet" in rej.columns:
        for r in rej.groupBy("code_rejet").count().orderBy(F.desc("count")).collect():
            add(ent, "rejets", r["code_rejet"], r["count"])
            nrej += r["count"]
    add(ent, "rejets", "TOTAL", nrej)
    if nb is not None:
        add(ent, "perte", "bronze-silver-rejets", nb - n - nrej)

    # Complétude et valeurs par défaut, colonne par colonne
    colonnes = [c for c in df.columns if c not in TECHNIQUES]
    exprs = []
    for c in colonnes:
        texte = isinstance(df.schema[c].dataType, StringType)
        manque = F.col(c).isNull() | (F.trim(F.col(c)) == "") if texte else F.col(c).isNull()
        exprs.append(F.sum(F.when(manque, 1).otherwise(0)).alias("m_" + c))
        if texte:
            exprs.append(F.sum(F.when(F.trim(F.col(c)).isin(*DEFAUTS), 1).otherwise(0)).alias("d_" + c))
    res = df.agg(*exprs).collect()[0].asDict()
    for c in colonnes:
        add(ent, "completude_pct", c, "%.2f" % (100.0 * (n - res["m_" + c]) / n if n else 0.0))
        if res.get("d_" + c):
            add(ent, "valeur_par_defaut", c, res["d_" + c])

    presentes = [c for c in cle if c in df.columns]
    add(ent, "doublons_cle", "+".join(presentes), n - df.select(*presentes).distinct().count())

# ---------------------------------------------------------------- FINESS (établissements)
etab = SILVER.get("etablissement")
if etab is not None:
    dep = premiere(etab, ["code_departement"])
    for c in ("finess_site", "finess_juridique", "finess_etablissement_juridique"):
        if c not in etab.columns:
            continue
        v = F.upper(F.trim(F.col(c)))
        st = etab.agg(
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias("nul"),
            F.sum(F.when(F.col(c).isNotNull() & ~v.rlike(FINESS), 1).otherwise(0)).alias("format_invalide"),
            F.sum(F.when(F.col(c).isNotNull() & (F.length(F.trim(F.col(c))) != 9), 1).otherwise(0)).alias("longueur_diff_9"),
            # département comparé seulement pour un FINESS bien formé (pas de double comptage)
            *([F.sum(F.when(v.rlike(FINESS) & F.col(dep).isNotNull()
                            & (v.substr(1, 2) != F.upper(F.col(dep)).substr(1, 2)), 1).otherwise(0))
               .alias("departement_incoherent")] if dep else []),
        ).collect()[0].asDict()
        for k, val in st.items():
            add("etablissement", "finess_" + k, c, val)
        non_nuls = etab.filter(F.col(c).isNotNull())
        add("etablissement", "finess_doublons", c, non_nuls.count() - non_nuls.select(c).distinct().count())
    if "finess_site" not in etab.columns:
        add("etablissement", "finess_nul", "finess_site", "ABSENTE")
    if "identifiant_organisation" in etab.columns:
        for r in (etab.groupBy(F.substring("identifiant_organisation", 1, 1).alias("p"))
                  .count().orderBy("p").collect()):
            add("etablissement", "prefixe_identifiant", r["p"], r["count"])

# ---------------------------------------------------------------- orphelins
REF = {
    "etablissement": ("etablissement", "identifiant_organisation"),
    "patient":       ("patient", "patient_pseudo"),
    "professionnel": ("professionnel", "professionnel_pseudo"),
    "diagnostic":    ("diagnostic", "code_diagnostic"),
}
for ref in ("patient", "professionnel", "diagnostic"):
    if ref not in SILVER:
        d = lire("/silver/" + ref)
        if d is not None:
            SILVER[ref] = d


def orphelins(ent, col, ref):
    src, refcol = REF[ref]
    if ent not in SILVER or src not in SILVER:
        return
    if col not in SILVER[ent].columns or refcol not in SILVER[src].columns:
        add(ent, "orphelins", col + "->" + ref, "ABSENTE")
        return
    o = (SILVER[ent].filter(F.col(col).isNotNull())
         .join(SILVER[src].select(F.col(refcol).alias(col)).distinct(), col, "left_anti").count())
    add(ent, "orphelins", col + "->" + ref, o)


orphelins("activite_professionnel", "professionnel_pseudo", "professionnel")
orphelins("activite_professionnel", "identifiant_organisation", "etablissement")
orphelins("hospitalisation", "patient_pseudo", "patient")
orphelins("hospitalisation", "identifiant_organisation", "etablissement")
orphelins("hospitalisation", "code_diagnostic", "diagnostic")

act = SILVER.get("activite_professionnel")
if act is not None:
    if "identifiant_organisation" in act.columns:
        add("activite_professionnel", "sans_organisation", "identifiant_organisation",
            act.filter(F.col("identifiant_organisation").isNull()).count())
    if "mode_exercice" in act.columns:
        for r in act.groupBy("mode_exercice").count().orderBy(F.desc("count")).collect():
            add("activite_professionnel", "repartition_mode_exercice", r["mode_exercice"], r["count"])

# ---------------------------------------------------------------- hospitalisations : domaine
hosp = SILVER.get("hospitalisation")
if hosp is not None:
    d_ent = premiere(hosp, ["date_entree", "date_entree_hospitalisation"])
    duree = premiere(hosp, ["duree_sejour_jours", "jour_hospitalisation", "duree_jours", "duree"])
    if d_ent:
        st = hosp.agg(F.min(d_ent).alias("min"), F.max(d_ent).alias("max"),
                      F.sum(F.when(F.col(d_ent) > F.current_date(), 1).otherwise(0)).alias("future"),
                      F.sum(F.when(F.col(d_ent).isNull(), 1).otherwise(0)).alias("nulle")).collect()[0]
        add("hospitalisation", "etendue", d_ent + "_min", st["min"])
        add("hospitalisation", "etendue", d_ent + "_max", st["max"])
        add("hospitalisation", "hors_domaine", d_ent + "_future", st["future"])
        add("hospitalisation", "hors_domaine", d_ent + "_nulle", st["nulle"])
    else:
        add("hospitalisation", "etendue", "date_entree", "ABSENTE")
    if duree:
        st = hosp.agg(F.min(duree).alias("min"), F.max(duree).alias("max"),
                      F.sum(F.when((F.col(duree) < 0) | (F.col(duree) > 365), 1).otherwise(0)).alias("hors")).collect()[0]
        add("hospitalisation", "etendue", duree + "_min", st["min"])
        add("hospitalisation", "etendue", duree + "_max", st["max"])
        add("hospitalisation", "hors_domaine", duree + "_hors_0_365", st["hors"])
    else:
        add("hospitalisation", "etendue", "duree", "ABSENTE")
    if "identifiant_organisation" in hosp.columns:
        add("hospitalisation", "etablissements_distincts", "identifiant_organisation",
            hosp.select("identifiant_organisation").distinct().count())

# ---------------------------------------------------------------- troncature S1
b_pro = derniere_partition(lire("/bronze/s2/professionnel_sante"))
s1_pro = SILVER.get("professionnel")
if b_pro is not None:
    add("professionnel_sante_s2", "bronze_lignes", "", b_pro.count())
if s1_pro is not None:
    add("professionnel_s1", "lignes", "", s1_pro.count())
if act is not None and "professionnel_pseudo" in act.columns:
    rej = lire("/silver/_rejets/activite_professionnel")
    if rej is not None and "code_rejet" in rej.columns and "professionnel_pseudo" in rej.columns:
        absents = (rej.filter(F.col("code_rejet") == "PROFESSIONNEL_ORPHELIN")
                   .select("professionnel_pseudo").distinct().count())
        add("activite_professionnel", "troncature_s1", "professionnels_absents_de_s1", absents)
    add("activite_professionnel", "troncature_s1", "professionnels_retrouves",
        act.select("professionnel_pseudo").distinct().count())

(spark.createDataFrame(LIGNES, ["run_id", "entite", "indicateur", "colonne", "valeur"])
 .coalesce(1).write.mode("overwrite").option("header", "true").csv(H + "/silver/_qualite/s2"))
print("QUALITE_FIN run_id=%s lignes=%d" % (RUN_ID, len(LIGNES)), flush=True)
spark.stop()
