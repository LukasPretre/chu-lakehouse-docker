"""
CI - Bronze S1 synthétique.

Écrit dans HDFS les cinq tables Bronze de la source S1 comme le ferait NiFi J01 (mêmes
noms de colonnes que la base source), avec des anomalies connues, puis le fichier des
résultats attendus du Silver que lit jobs/check_silver.py.

Aucune donnée réelle : la CI GitHub ne doit jamais voir la source (RGPD, HDS).
Sécurité : le script refuse de tourner si un Bronze S1 existe déjà, pour ne jamais
mélanger ces fausses données avec les vraies sur une VM.

Lancement (fait par la CI, .github/workflows/ci.yml) :
  docker cp tests/ci_bronze_s1.py chu_spark_master:/tmp/ci_bronze_s1.py
  docker exec chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /tmp/ci_bronze_s1.py
"""
import json
import os
import random
import sys
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession

H = os.environ.get("HDFS_BASE", "hdfs://namenode:8020")
TABLES = ["Patient", "Specialites", "Professionnel_de_sante", "Diagnostic", "Consultation"]
INGEST = date.today().isoformat()
ATTENDU = f"{H}/bronze/_ci/attendu_silver"

spark = (SparkSession.builder.appName("CI_bronze_s1_synthetique")
         .config("spark.hadoop.dfs.replication", "1").getOrCreate())
jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.Path(H + "/").getFileSystem(spark._jsc.hadoopConfiguration())
deja = [t for t in TABLES if fs.exists(jvm.org.apache.hadoop.fs.Path(f"{H}/bronze/s1/{t}"))]
if deja:
    print(f"[ECHEC] Bronze S1 déjà présent ({', '.join(deja)}) : données synthétiques non écrites", file=sys.stderr)
    sys.exit(1)

rnd = random.Random(42)  # données reproductibles d'une exécution à l'autre


def ms_jour(d):
    """Date -> millisecondes depuis 1970 (format des dates dans le Bronze J01)."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def ms_heure(h, m):
    """Heure du jour -> millisecondes depuis minuit (format des heures dans le Bronze J01)."""
    return (h * 60 + m) * 60000


def ecrire(nom, lignes, schema):
    df = spark.createDataFrame(lignes, schema)
    df.coalesce(1).write.mode("overwrite").parquet(f"{H}/bronze/s1/{nom}/ingest_date={INGEST}")
    print(f"CI bronze {nom:25s} {len(lignes):>5} lignes")


# ----------------------------------------------------------------- Patient
# 195 valides, 7 rejets (2 sans identifiant, 3 dates invalides, 2 sexes invalides), 5 doublons
SCHEMA_PATIENT = ("Id_patient INT, Nom STRING, Prenom STRING, Sexe STRING, Adresse STRING, Ville STRING, "
                  "Code_postal STRING, Pays STRING, EMail STRING, Tel STRING, Date STRING, Age INT, "
                  "Num_Secu STRING, Groupe_sanguin STRING, Poid INT, Taille INT")


ALEA = object()  # valeur par défaut : tirée au hasard (None veut dire « valeur nulle »)


def patient(i, sexe=ALEA, naissance=ALEA):
    sexe = rnd.choice(["female", "male", "Female", "Male", "F", "M"]) if sexe is ALEA else sexe
    if naissance is ALEA:
        naissance = f"{rnd.randint(1, 12)}/{rnd.randint(1, 28)}/{rnd.randint(1930, 2010)}"
    return (i, f"Nom{i}", f"Prenom{i}", sexe, f"{i} rue de Test", "Arras", "62000", "France",
            f"p{i}@exemple.test", "0600000000", naissance, 40, f"1{i or 0:014d}", "A+", 70, 170)


patients = [patient(i) for i in range(1, 196)]
patients += [patient(196, naissance="pas-une-date"), patient(197, naissance=""), patient(198, naissance=None)]
patients += [patient(199, sexe="inconnu"), patient(200, sexe="")]
patients += [patient(None), patient(None)]
patients += patients[:5]
ecrire("Patient", patients, SCHEMA_PATIENT)

# ----------------------------------------------------------------- Specialites
# 10 valides, 1 rejet (code vide)
specialites = [(f"S{i:02d}", f"Fonction {i}", f"Specialite {i}") for i in range(1, 11)]
specialites.append(("  ", "Fonction vide", "Specialite vide"))
ecrire("Specialites", specialites, "Code_specialite STRING, Fonction STRING, Specialite STRING")

# ----------------------------------------------------------------- Professionnel_de_sante
# 100 valides (dont 10 avec une spécialité hors référentiel), 1 rejet (identifiant vide), 3 doublons
SCHEMA_PRO = ("Identifiant STRING, Civilite STRING, Nom STRING, Prenom STRING, Commune STRING, "
              "Type_identifiant STRING, Categorie_professionnelle STRING, Profession STRING, Code_specialite STRING")
pros = [(f"P{i:04d}", "Dr", f"Nom{i}", f"Prenom{i}", "Lille", "RPPS", "Civil", "Médecin",
         f"S{rnd.randint(1, 10):02d}" if i <= 90 else "S99") for i in range(1, 101)]
pros.append(("", "Dr", "Sans", "Identifiant", "Lille", "RPPS", "Civil", "Médecin", "S01"))
pros += pros[:3]
ecrire("Professionnel_de_sante", pros, SCHEMA_PRO)

# ----------------------------------------------------------------- Diagnostic
# 50 valides, 1 rejet (code nul), 2 doublons après nettoyage (espaces, minuscules)
diagnostics = [(f"D{i:03d}", f"Diagnostic {i}") for i in range(1, 51)]
diagnostics += [(None, "Sans code"), (" d001 ", "Diagnostic 1 bis"), ("D002", "Diagnostic 2 bis")]
ecrire("Diagnostic", diagnostics, "Code_diag STRING, Diagnostic STRING")

# ----------------------------------------------------------------- Consultation
# 1000 valides (dont 10 qui passent minuit), 10 rejets (2 par motif), 5 doublons
SCHEMA_CONSULT = ("Num_consultation INT, Id_patient INT, Id_prof_sante STRING, Code_diag STRING, "
                  "Motif STRING, Id_mut INT, Date LONG, Heure_debut LONG, Heure_fin LONG")


def consultation(num, patient_id=None, pro=None, jour=None, debut=None, fin=None):
    if jour is None:
        jour = ms_jour(date.fromordinal(rnd.randint(date(2015, 1, 1).toordinal(), date(2023, 12, 31).toordinal())))
    if debut is None:
        h, m = rnd.randint(8, 17), rnd.choice([0, 15, 30, 45])
        debut, fin = ms_heure(h, m), ms_heure(h, m) + rnd.choice([15, 20, 30, 45]) * 60000
    return (num, patient_id if patient_id is not None else rnd.randint(1, 200),
            pro if pro is not None else f"P{rnd.randint(1, 100):04d}",
            rnd.choice([f"D{rnd.randint(1, 50):03d}"] * 49 + ["Z999"]),  # quelques diagnostics inconnus
            "Motif de test", 1, jour, debut, fin)


consultations = [consultation(n) for n in range(1, 991)]
consultations += [consultation(n, debut=ms_heure(23, 50), fin=ms_heure(0, 20)) for n in range(991, 1001)]
rejets = [consultation(1001), consultation(1002), consultation(1003), consultation(1004),
          consultation(1005, pro=""), consultation(1006, pro=""),
          consultation(1007), consultation(1008), consultation(1009), consultation(1010)]
rejets[0] = (None,) + rejets[0][1:]                       # ID_NUL
rejets[1] = (None,) + rejets[1][1:]
rejets[2] = rejets[2][:1] + (None,) + rejets[2][2:]       # PATIENT_NUL
rejets[3] = rejets[3][:1] + (None,) + rejets[3][2:]
rejets[6] = rejets[6][:6] + (None,) + rejets[6][7:]       # DATE_INVALIDE
rejets[7] = rejets[7][:6] + (None,) + rejets[7][7:]
rejets[8] = rejets[8][:8] + (None,)                       # HEURE_NULLE
rejets[9] = rejets[9][:8] + (None,)
consultations += rejets + consultations[:5]
ecrire("Consultation", consultations, SCHEMA_CONSULT)

# ----------------------------------------------------------------- Résultats attendus du Silver
attendu = {
    "patient":       {"valides": 195,  "rejetes": 7},
    "specialites":   {"valides": 10,   "rejetes": 1},
    "professionnel": {"valides": 100,  "rejetes": 1},
    "diagnostic":    {"valides": 50,   "rejetes": 1},
    "consultation":  {"valides": 1000, "rejetes": 10, "passage_minuit": 10},
}
spark.createDataFrame([(json.dumps(attendu),)], "value STRING").coalesce(1) \
     .write.mode("overwrite").text(ATTENDU)
print(f"CI attendus Silver écrits dans {ATTENDU}")
spark.stop()