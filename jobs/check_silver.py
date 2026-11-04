"""
Contrôles de la couche Silver (lancés par ./test_silver.sh et par la CI).

Pour chaque table Silver présente :
  - table non vide, clé naturelle non nulle et unique ;
  - pseudonymes au format HMAC-SHA256 (64 caractères hexadécimaux) ;
  - RGPD : aucune colonne nominative ou identifiant direct (Livrable 1, §7.1) ;
  - règles de domaine (sexe, année de naissance, durée de consultation, passage de minuit).
Si le fichier des résultats attendus de la CI existe (/bronze/_ci/attendu_silver, écrit par
tests/ci_bronze_s1.py), les volumes de lignes valides et rejetées sont comparés exactement.

Une table absente est ignorée (SKIP), sauf avec STRICT=1. Les tables de la source S1 sont
toujours obligatoires.
Sortie : une ligne « SILVERTEST|OK|... », « SILVERTEST|ECHEC|... » ou « SILVERTEST|SKIP|... » par contrôle.
"""
import json
import os
import sys
from datetime import date

from pyspark.sql import SparkSession, functions as F

H = os.environ.get("HDFS_BASE", "hdfs://namenode:8020")
STRICT = os.environ.get("STRICT", "0") == "1"
HMAC = "^[0-9a-f]{64}$"

# entité -> clé naturelle, colonnes pseudonymisées, colonnes interdites (comparées sans casse)
ENTITES = {
    # Source S1 (obligatoires)
    "patient":       (["patient_pseudo"], ["patient_pseudo"],
                      ["id_patient", "nom", "prenom", "adresse", "ville", "code_postal", "pays",
                       "email", "tel", "num_secu"]),
    "professionnel": (["professionnel_pseudo"], ["professionnel_pseudo"],
                      ["identifiant", "nom", "prenom", "civilite", "commune"]),
    "specialites":   (["code_specialite"], [], []),
    "diagnostic":    (["code_diagnostic"], [], []),
    "consultation":  (["id_consultation"], ["patient_pseudo", "professionnel_pseudo"],
                      ["id_patient", "id_prof_sante"]),
    # Sources fichiers (noms du Livrable 1, annexe B.2), contrôlées dès qu'elles existent
    "etablissement":          (["identifiant_organisation"], [], []),
    "activite_professionnel": (["identifiant_pseudo", "identifiant_organisation"], ["identifiant_pseudo"],
                               ["identifiant", "nom", "prenom"]),
    "hospitalisation":        (["num_hospitalisation"], ["patient_pseudo"], ["id_patient"]),
    "esatis":                 (["annee", "type_enquete", "finess_geo"], [], []),
    "ref_departement":        (["code_departement"], [], []),
    "deces":                  (["cle_deces"], [],
                               ["nom", "prenom", "date_naissance", "lieu_naissance", "code_lieu_naissance",
                                "pays_naissance", "numero_acte_deces"]),
}
OBLIGATOIRES = {"patient", "professionnel", "specialites", "diagnostic", "consultation"}

spark = (SparkSession.builder.appName("check_silver")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
echecs = 0


def resultat(ok, message):
    global echecs
    if ok is None:
        print(f"SILVERTEST|SKIP|{message}")
        return
    echecs += 0 if ok else 1
    print(f"SILVERTEST|{'OK' if ok else 'ECHEC'}|{message}")


def lire(chemin):
    try:
        return spark.read.parquet(chemin)
    except Exception:
        return None


try:
    attendus = json.loads(spark.read.text(f"{H}/bronze/_ci/attendu_silver").first()["value"])
    print("SILVERTEST|OK|fichier des résultats attendus de la CI trouvé : comparaison exacte des volumes")
except Exception:
    attendus = {}

for entite, (cle, pseudos, interdites) in ENTITES.items():
    df = lire(f"{H}/silver/{entite}")
    if df is None:
        if entite in OBLIGATOIRES or STRICT:
            resultat(False, f"{entite} : table Silver absente (/silver/{entite})")
        else:
            resultat(None, f"{entite} : table Silver pas encore produite")
        continue
    df = df.cache()
    colonnes = set(df.columns)
    n = df.count()
    resultat(n > 0, f"{entite} : {n} lignes")

    manquantes = [c for c in cle + pseudos if c not in colonnes]
    if manquantes:
        resultat(False, f"{entite} : colonnes du Livrable 1 absentes : {', '.join(manquantes)}")
        continue

    nulles = df.filter(" OR ".join(f"{c} IS NULL" for c in cle)).count()
    doublons = n - df.select(*cle).distinct().count()
    resultat(nulles == 0 and doublons == 0,
             f"{entite} : clé {' + '.join(cle)} non nulle et unique (nulles={nulles}, doublons={doublons})")

    for p in pseudos:
        mauvais = df.filter(F.col(p).isNotNull() & ~F.col(p).rlike(HMAC)).count()
        resultat(mauvais == 0, f"{entite} : {p} au format HMAC-SHA256 ({mauvais} valeur(s) hors format)")

    presentes = sorted(c for c in colonnes if c.lower() in interdites)
    if interdites:
        resultat(not presentes, f"{entite} : aucune donnée nominative (RGPD)"
                 + (f" ; trouvées : {', '.join(presentes)}" if presentes else ""))

    # Règles de domaine
    if entite == "patient":
        hors = df.filter(F.col("sexe").isNotNull() & ~F.col("sexe").isin("F", "M", "I")).count()
        resultat(hors == 0, f"patient : sexe dans F, M, I ({hors} hors domaine)")
        hors = df.filter(~F.col("annee_naissance").between(1880, date.today().year)).count()
        resultat(hors == 0, f"patient : année de naissance plausible ({hors} hors 1880-{date.today().year})")
    if entite == "consultation":
        hors = df.filter(F.col("date_consultation").isNull()).count()
        resultat(hors == 0, f"consultation : date renseignée ({hors} sans date)")
        hors = df.filter(~F.col("duree_minutes").between(0, 1439)).count()
        resultat(hors == 0, f"consultation : durée entre 0 et 1439 minutes ({hors} hors domaine)")
        sec = lambda c: F.unix_timestamp(F.col(c), "HH:mm:ss")
        calcul = F.floor(((sec("heure_fin") - sec("heure_debut") + 86400) % 86400) / 60)
        hors = df.filter(F.abs(calcul - F.col("duree_minutes")) > 1).count()
        resultat(hors == 0, f"consultation : durée = fin - début, + 1440 au passage de minuit ({hors} incohérente(s))")

    # Volumes exacts sur les données synthétiques de la CI
    if entite in attendus:
        a = attendus[entite]
        rej = lire(f"{H}/silver/_rejets/{entite}")
        nr = rej.count() if rej is not None else -1
        resultat(n == a["valides"] and nr == a["rejetes"],
                 f"{entite} : volumes attendus (valides {n}/{a['valides']}, rejetées {nr}/{a['rejetes']})")
        if "passage_minuit" in a:
            nm = df.filter(F.col("heure_fin") < F.col("heure_debut")).count()
            resultat(nm == a["passage_minuit"], f"{entite} : consultations passant minuit ({nm}/{a['passage_minuit']})")
    df.unpersist()

spark.stop()
sys.exit(1 if echecs else 0)