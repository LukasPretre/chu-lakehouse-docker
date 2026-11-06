"""J17 : Silver e-Satis (HAS). Sources : Bronze S3 /bronze/s3/esatis/ingest_date=<dernier>/<fichier d'origine>.
Chaque fichier est lu avec ses parametres (format, encodage, separateur) puis ses colonnes sont rangees
dans le modele par une table de correspondance (CONFIG). Les .xlsx sont lus avec la bibliotheque standard
(zipfile + xml) : aucune dependance a installer. Sorties : /silver/esatis et /silver/_rejets/esatis."""
import io, re, sys, zipfile
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession, functions as F, Window, types as T

sys.path.insert(0, "/jobs")
from lib_geo import table_departements  # J05 : referentiel des departements et regions

H = "hdfs://namenode:8020"
BRONZE = H + "/bronze/s3/esatis"

# ------------------------------------------------------------------ table de configuration
MAP_48H = {"finess_juridique": "finess", "finess_geo": "finess_geo", "region": "region",
           "score_global": "score_all_rea_ajust", "score_accueil": "score_accueil_rea_ajust",
           "score_pec_infirmier": "score_PECinf_rea_ajust", "score_pec_medical": "score_PECmed_rea_ajust",
           "score_chambre": "score_chambre_rea_ajust", "score_repas": "score_repas_rea_ajust",
           "score_sortie": "score_sortie_rea_ajust", "nb_reponses": "nb_rep_score_all_rea_ajust",
           "taux_recommandation": "taux_reco_brut", "classement": "classement", "evolution": "evolution"}
# CA : seuls Global, Accueil et Sortie sont stockes ; les autres themes restent a NULL
MAP_CA = {"finess_juridique": "finess", "finess_geo": "finess_geo", "region": "region",
          "score_global": "score_all_ajust", "score_accueil": "score_ACC_ajust",
          "score_sortie": "score_OVS_ajust", "nb_reponses": "nb_rep_score_all_ajust",
          "taux_recommandation": "taux_reco_brut", "classement": "classement", "evolution": "evolution"}
CONFIG = [  # fichier d'origine, annee, type d'enquete, format, encodage, separateur, correspondance
    ("ESATIS48H_MCO_recueil2017_donnees.csv", 2017, "48H", "csv", "windows-1252", ";", MAP_48H),
    ("resultats-esatis48h-mco-open-data-2019.csv", 2019, "48H", "csv", "windows-1252", ";", MAP_48H),
    ("resultats-esatisca-mco-open-data-2019.csv", 2019, "CA", "csv", "windows-1252", ";", MAP_CA),
    ("resultats-esatis48h-mco-open-data-2020.xlsx", 2020, "48H", "xlsx", None, None, MAP_48H),
    ("resultats-esatisca-mco-open-data-2020.xlsx", 2020, "CA", "xlsx", None, None, MAP_CA),
]
CIBLES = ["finess_juridique", "finess_geo", "region", "score_global", "score_accueil", "score_pec_infirmier",
          "score_pec_medical", "score_chambre", "score_repas", "score_sortie", "nb_reponses",
          "taux_recommandation", "classement", "evolution"]
SCORES = ["score_global", "score_accueil", "score_pec_infirmier", "score_pec_medical",
          "score_chambre", "score_repas", "score_sortie"]
FINESS_OK = r"^([0-9]{2}|2[AB])[0-9]{7}$"   # 2 chiffres ou 2A/2B (Corse) + 7 chiffres


# ------------------------------------------------------------------ lecture d'un .xlsx (stdlib)
M = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

def _col_index(ref):
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group(0):
        n = n * 26 + (ord(ch) - 64)
    return n - 1

def lire_xlsx(src):
    """Renvoie {nom_feuille: [lignes]} ; une ligne est une liste de textes (None si cellule vide)."""
    z = zipfile.ZipFile(src)
    ss = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("{%s}si" % M):
            ss.append("".join(t.text or "" for t in si.iter("{%s}t" % M)))
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = {r.get("Id"): r.get("Target") for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
    out = {}
    for s in wb.find("{%s}sheets" % M):
        t = rels[s.get("{%s}id" % R)].lstrip("/")
        chemin = t if t.startswith("xl/") else "xl/" + t
        lignes = []
        for row in ET.fromstring(z.read(chemin)).iter("{%s}row" % M):
            vals = {}
            for c in row.findall("{%s}c" % M):
                typ, v = c.get("t"), c.find("{%s}v" % M)
                if typ == "s":
                    val = ss[int(v.text)] if v is not None else None
                elif typ == "inlineStr":
                    val = "".join(x.text or "" for x in c.iter("{%s}t" % M))
                else:
                    val = v.text if v is not None else None
                vals[_col_index(c.get("r"))] = val
            if vals:
                lignes.append([vals.get(i) for i in range(max(vals) + 1)])
        out[s.get("name")] = lignes
    return out


# ------------------------------------------------------------------ session
spark = (SparkSession.builder.appName("J17_silver_esatis")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())

def dernier_ingest():
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(H), spark._jsc.hadoopConfiguration())
    noms = sorted(s.getPath().getName() for s in fs.listStatus(jvm.org.apache.hadoop.fs.Path(BRONZE)))
    noms = [n for n in noms if n.startswith("ingest_date=")]
    return noms[-1]

def lire_fichier(chemin, fmt, encodage, sep):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(H), spark._jsc.hadoopConfiguration())
    if not fs.exists(jvm.org.apache.hadoop.fs.Path(chemin)):
        raise SystemExit(f"SILVER esatis ERREUR : fichier absent du Bronze : {chemin} "
                         "(verifier le flux NiFi J03 ou la liste CONFIG)")
    if fmt == "csv":
        return (spark.read.option("header", True).option("sep", sep).option("encoding", encodage)
                .csv(chemin))
    # xlsx : fichier de quelques centaines de Ko, lu sur le driver
    contenu = spark.sparkContext.binaryFiles(chemin).collect()[0][1]
    feuille = list(lire_xlsx(io.BytesIO(contenu)).values())[0]
    entete = [str(c).strip() for c in feuille[0]]
    lignes = [(r + [None] * len(entete))[:len(entete)] for r in feuille[1:]]
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in entete])
    return spark.createDataFrame(lignes, schema)

def normaliser(col):
    """Libelle normalise : majuscules, sans accents, ponctuation et espaces reduits."""
    c = F.regexp_replace(F.lower(F.trim(col)), "œ", "oe")
    c = F.translate(c, "àâäáãçéèêëíìîïñóòôöõúùûüýÿ", "aaaaaceeeeiiiinooooouuuuyy")
    c = F.regexp_replace(c, "[^a-z0-9]+", " ")
    return F.upper(F.trim(c))


dossier = dernier_ingest()
parts = []
for fichier, annee, type_enq, fmt, enc, sep, mapping in CONFIG:
    df = lire_fichier(f"{BRONZE}/{dossier}/{fichier}", fmt, enc, sep)
    cols = {c.strip(): c for c in df.columns}
    sel = [(F.trim(F.col("`" + cols[mapping[c]] + "`")).alias(c) if c in mapping and mapping[c] in cols
            else F.lit(None).cast("string").alias(c)) for c in CIBLES]
    parts.append(df.select(*sel).withColumn("annee", F.lit(annee)).withColumn("type_enquete", F.lit(type_enq))
                   .withColumn("source_fichier", F.lit(fichier)))
    print(f"SILVER esatis lu {fichier} lignes={df.count()}", flush=True)
brut = parts[0]
for p in parts[1:]:
    brut = brut.unionByName(p)
brut = brut.withColumn("ingest_date", F.lit(dossier.split("=")[1]))


# ------------------------------------------------------------------ nettoyage
def vide_en_null(c):
    return F.when(F.trim(c) == "", None).otherwise(F.trim(c))

def decimal(c):
    v = F.regexp_replace(vide_en_null(c), ",", ".")
    return F.when(v.rlike(r"^-?[0-9]+(\.[0-9]+)?$"), v.cast("double"))

def finess9(c):  # zeros initiaux perdus : completer seulement les valeurs purement numeriques
    v = vide_en_null(c)
    return F.when(v.rlike("^[0-9]+$") & (F.length(v) < 9), F.lpad(v, 9, "0")).otherwise(F.upper(v))

t = (brut.withColumn("finess_geo", finess9(F.col("finess_geo")))
         .withColumn("finess_juridique", finess9(F.col("finess_juridique")))
         .withColumn("classement", F.upper(vide_en_null(F.col("classement"))))
         .withColumn("classement", F.when(F.col("classement").isin("A", "B", "C", "D", "DI", "NV", "NR"),
                                          F.col("classement")))
         .withColumn("evolution", F.regexp_replace(vide_en_null(F.col("evolution")), r"^[0-9]+\s*-\s*", ""))
         .withColumn("nb_reponses", F.when(vide_en_null(F.col("nb_reponses")).rlike("^[0-9]+$"),
                                           vide_en_null(F.col("nb_reponses")).cast("int")))
         .withColumn("taux_recommandation", decimal(F.col("taux_recommandation")))
         .withColumn("region_norm", normaliser(F.col("region")))
         .withColumn("region_norm", F.when(F.col("region_norm") == "PACA", F.lit("PROVENCE ALPES COTE D AZUR"))
                                     .otherwise(F.col("region_norm"))))
inexploitable = F.col("classement").isin("DI", "NV", "NR")
for s in SCORES:
    t = t.withColumn(s, F.when(inexploitable, None).otherwise(decimal(F.col(s))))

# REF_REGION : libelle de region normalise -> code region INSEE (referentiel J05)
ref = (table_departements(spark).filter("code_region_insee IS NOT NULL")
       .select(normaliser(F.col("libelle_region")).alias("region_norm"), F.col("code_region_insee").alias("code_region"))
       .distinct())
t = t.join(F.broadcast(ref), "region_norm", "left")
try:
    eta_reg = (spark.read.parquet(H + "/silver/etablissement")
               .filter("finess_geo IS NOT NULL AND code_region_insee <> 'ZZ'")
               .select("finess_geo", F.col("code_region_insee").alias("code_region_eta"))
               .dropDuplicates(["finess_geo"]))
    t = (t.join(eta_reg, "finess_geo", "left")
          .withColumn("code_region", F.coalesce("code_region", "code_region_eta")).drop("code_region_eta"))
except Exception as e:
    print("SILVER esatis repli region par etablissement impossible :", str(e)[:120])

# score hors 0-100 (QC05 du L1) : seul le score fautif passe a NULL, la ligne est conservee et comptee en anomalie
def score_hors(s):
    return F.col(s).isNotNull() & ((F.col(s) < 0) | (F.col(s) > 100))
anomalies = t.agg(*[F.sum(F.when(score_hors(s), 1).otherwise(0)).alias(s) for s in SCORES]).first()
for s in SCORES:
    t = t.withColumn(s, F.when(score_hors(s), F.lit(None)).otherwise(F.col(s)))
print("SILVER esatis score hors 0-100 mis a NULL =", sum(anomalies[s] or 0 for s in SCORES),
      {s: anomalies[s] for s in SCORES if anomalies[s]})
cle = ["annee", "type_enquete", "finess_geo"]
t = t.withColumn("code_rejet",
        F.when(F.col("finess_geo").isNull(), "FINESS_GEO_NUL")
         .when(~F.col("finess_geo").rlike(FINESS_OK), "FINESS_GEO_INVALIDE"))
# doublon de cle : on garde la ligne la plus documentee (plus grand nb_reponses), les autres vont en rejets
w = Window.partitionBy(*cle).orderBy(F.col("nb_reponses").desc_nulls_last(), F.col("source_fichier"))
valides = t.filter("code_rejet IS NULL").withColumn("rang", F.row_number().over(w))
doublons = valides.filter("rang > 1").withColumn("code_rejet", F.lit("DOUBLON_CLE"))

colonnes = ["annee", "type_enquete", "finess_geo", "finess_juridique", "code_region"] + SCORES + \
           ["nb_reponses", "taux_recommandation", "classement", "evolution", "ingest_date"]
ok = valides.filter("rang = 1").select(*colonnes)
rej = (t.filter("code_rejet IS NOT NULL").unionByName(doublons.drop("rang"))
         .select("annee", "type_enquete", "finess_geo", "code_rejet"))

ok.write.mode("overwrite").parquet(H + "/silver/esatis")
rej.write.mode("overwrite").parquet(H + "/silver/_rejets/esatis")

# ------------------------------------------------------------------ comptes et anomalies
res = spark.read.parquet(H + "/silver/esatis")
nb, no, nr = brut.count(), res.count(), rej.count()
print(f"SILVER esatis bronze={nb} valides={no} rejetes={nr} doublons={nb-no-nr}")
for r in rej.groupBy("code_rejet").count().orderBy("code_rejet").collect():
    print(f"SILVER esatis rejet {r[0]} = {r[1]}")
for r in res.groupBy("annee", "type_enquete").count().orderBy("annee", "type_enquete").collect():
    print(f"SILVER esatis lignes {r[0]} {r[1]} = {r[2]}")
print("SILVER esatis region introuvable (code_region NULL) =", res.filter("code_region IS NULL").count())
ok_diag = valides.filter("rang = 1")
for r in (ok_diag.filter("code_region IS NULL").groupBy("region").count()
           .orderBy(F.desc("count")).limit(10).collect()):
    print(f"SILVER esatis   libelle de region sans code : {r[0]} = {r[1]}")
print("SILVER esatis scores renseignes par annee/type :")
for r in (res.groupBy("annee", "type_enquete")
             .agg(F.count("*").alias("n"), F.count("score_global").alias("global"),
                  F.count("taux_recommandation").alias("taux")).orderBy("annee", "type_enquete").collect()):
    print(f"SILVER esatis   {r['annee']} {r['type_enquete']} lignes={r['n']} score_global={r['global']} taux_reco={r['taux']}")

# taux de rapprochement des FINESS e-Satis avec le referentiel des etablissements (finess_geo de J14)
try:
    eta = spark.read.parquet(H + "/silver/etablissement").filter("finess_geo IS NOT NULL") \
               .select("finess_geo").distinct().withColumn("rapproche", F.lit(1))
    rap = res.join(eta, "finess_geo", "left")
    for r in (rap.groupBy("annee", "type_enquete")
                 .agg(F.count("*").alias("n"), F.count("rapproche").alias("r")).orderBy("annee", "type_enquete").collect()):
        print(f"SILVER esatis rapprochement FINESS {r['annee']} {r['type_enquete']} = {r['r']}/{r['n']}")
except Exception as e:
    print("SILVER esatis rapprochement FINESS non calcule :", str(e)[:120])
spark.stop()
