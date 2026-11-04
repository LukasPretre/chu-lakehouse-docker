"""
CI - Bronze S2 synthétique (fichiers déposés : établissements, professionnels, activité,
hospitalisations).

Écrit dans HDFS les quatre tables Bronze S2 comme le ferait NiFi J02 (mêmes noms de colonnes
que les fichiers CSV), avec des anomalies connues, puis les volumes attendus du Bronze que lit
jobs/check_bronze_s2.py. Les identifiants sont cohérents avec tests/ci_bronze_s1.py :
professionnels P0001 à P0100, patients 1 à 195, diagnostics D001 à D050.

Aucune donnée réelle : la CI GitHub ne doit jamais voir les sources (RGPD, HDS).
Sécurité : le script refuse de tourner si un Bronze S2 existe déjà, pour ne jamais
mélanger ces fausses données avec les vraies sur une VM.

Résultats attendus après ./run_silver_s2.sh (vérifiés par la CI) :
  etablissement           bronze=68  valides=62  rejetes=4  doublons=2  finess_geo=40  ZZ=1
  activite_professionnel  bronze=92  valides=85  rejetes=5  doublons=2
  hospitalisation         bronze=36  valides=32  rejetes=3  doublons=1  patients introuvables=1

Lancement (fait par la CI, .github/workflows/ci.yml, après le Bronze S1 synthétique) :
  docker cp tests/ci_bronze_s2.py chu_spark_master:/tmp/ci_bronze_s2.py
  docker exec chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /tmp/ci_bronze_s2.py
"""
import json
import os
import sys
from datetime import date

from pyspark.sql import SparkSession

H = os.environ.get("HDFS_BASE", "hdfs://namenode:8020")
TABLES = ["etablissement_sante", "professionnel_sante", "activite_professionnel_sante", "hospitalisation"]
INGEST = date.today().isoformat()
ATTENDU = f"{H}/bronze/_ci/attendu_bronze_s2"

spark = (SparkSession.builder.appName("CI_bronze_s2_synthetique")
         .config("spark.hadoop.dfs.replication", "1").getOrCreate())
jvm = spark._jvm
fs = jvm.org.apache.hadoop.fs.Path(H + "/").getFileSystem(spark._jsc.hadoopConfiguration())
deja = [t for t in TABLES if fs.exists(jvm.org.apache.hadoop.fs.Path(f"{H}/bronze/s2/{t}"))]
if deja:
    print(f"[ECHEC] Bronze S2 déjà présent ({', '.join(deja)}) : données synthétiques non écrites", file=sys.stderr)
    sys.exit(1)

volumes = {}


def ecrire(nom, colonnes, lignes):
    schema = ", ".join(f"{c} STRING" for c in colonnes)   # NiFi écrit tout en texte
    spark.createDataFrame(lignes, schema).coalesce(1).write.mode("overwrite") \
         .parquet(f"{H}/bronze/s2/{nom}/ingest_date={INGEST}")
    volumes[nom] = len(lignes)
    print(f"CI bronze s2 {nom:30s} {len(lignes):>4} lignes")


# ----------------------------------------------------------------- etablissement_sante
# 62 valides : 40 en F (FINESS dans l'identifiant), 10 en C, 10 en R, Saint-Barthélemy (région
# inconnue ZZ), une commune corse numérique (repli sur le code postal) ;
# 4 rejets (1 ID_NUL, 2 RAISON_SOCIALE_NULLE, 1 CODE_POSTAL_INVALIDE) ; 2 doublons exacts.
COL_ETAB = ["identifiant_organisation", "finess_site", "finess_etablissement_juridique", "siret_site",
            "siren_site", "raison_sociale_site", "enseigne_commerciale_site", "code_commune", "commune",
            "code_postal", "pays", "adresse", "telephone", "email"]
LIEUX = [("59", "59350", "Lille", "59000"), ("62", "62041", "Arras", "62000"), ("75", "75056", "Paris", "75001"),
         ("13", "13055", "Marseille", "13001"), ("2A", "2A004", "Ajaccio", "20000"), ("2B", "2B033", "Bastia", "20200"),
         ("69", "69123", "Lyon", "69001"), ("974", "97411", "Saint-Denis", "97400"), ("33", "33063", "Bordeaux", "33000"),
         ("67", "67482", "Strasbourg", "67000")]


def etab(ident, dep, commune_code, commune, cp, raison="Établissement de test", finess_jur=None):
    return (ident, None, finess_jur, None, None, raison, None, commune_code, commune, cp, "France",
            "1 rue de Test", "0300000000", "contact@exemple.test")


etabs, orgs_f = [], []
for i in range(40):
    dep, cc, com, cp = LIEUX[i % 10]
    prefixe = dep if dep in ("2A", "2B") else dep[:2]
    ident = f"F{prefixe}{i:07d}"
    orgs_f.append(ident)
    etabs.append(etab(ident, dep, "" if i % 3 == 0 else cc, com, cp, f"Hôpital {i}", f"{prefixe}{9000000 + i:07d}"))
etabs += [etab(f"C{i:010d}", "59", "59350", "Lille", "59000", f"Cabinet {i}") for i in range(10)]
etabs += [etab(f"R{i:010d}", "62", "62041", "Arras", "62000", f"Cabinet libéral {i}") for i in range(10)]
etabs.append(etab("R9000000001", "977", "", "Saint-Barthélemy", "97133", "Cabinet Saint-Barthélemy"))
etabs.append(etab("R9000000002", "2A", "20004", "Ajaccio", "20000", "Cabinet commune corse numérique"))
etabs += [etab(None, "59", "59350", "Lille", "59000", "Sans identifiant"),
          etab("C9000000001", "59", "59350", "Lille", "59000", ""),
          etab("C9000000002", "59", "59350", "Lille", "59000", None),
          etab("C9000000003", "59", "59350", "Lille", "ABCDE", "Code postal invalide")]
etabs += etabs[1:3]
ecrire("etablissement_sante", COL_ETAB, etabs)

# ----------------------------------------------------------------- professionnel_sante
# Copie partielle du fichier des professionnels (non utilisée par le Silver : J15 s'appuie sur S1)
ecrire("professionnel_sante", ["identifiant", "nom", "prenom", "profession"],
       [(f"P{i:04d}", f"Nom{i}", f"Prenom{i}", "Médecin") for i in range(1, 101)])

# ----------------------------------------------------------------- activite_professionnel_sante
# 85 valides : 60 salariés, 20 libéraux sans organisation, 5 professionnels avec un second mode
# d'exercice dans la même organisation ; 5 rejets (3 PROFESSIONNEL_ORPHELIN,
# 1 ORGANISATION_ORPHELINE, 1 ID_NUL) ; 2 doublons exacts.
act = [(f"P{p:04d}", orgs_f[p % 40], "Salarié") for p in range(1, 61)]
act += [(f"P{p:04d}", None, "Libéral") for p in range(61, 81)]
act += [(f"P{p:04d}", orgs_f[p % 40], "Libéral") for p in range(1, 6)]
act += [("P9999", orgs_f[k], "Salarié") for k in range(3)]
act += [("P0090", "F999999999", "Salarié"), (None, orgs_f[1], "Salarié")]
act += act[:2]
ecrire("activite_professionnel_sante", ["identifiant", "identifiant_organisation", "mode_exercice"], act)

# ----------------------------------------------------------------- hospitalisation
# 32 valides (dont une date au format numérique Excel et un patient inconnu de S1) ;
# 3 rejets (NUM_NUL, DATE_ENTREE_INVALIDE, DUREE_NEGATIVE) ; 1 doublon de numéro.
COL_HOSP = ["Num_Hospitalisation", "Id_patient", "identifiant_organisation", "Code_diagnostic",
            "Suite_diagnostic_consultation", "Date_Entree", "Jour_Hospitalisation"]
hosp = [(f"H{i:04d}", str(i), orgs_f[i % 40], f"D{i:03d}" if i % 5 else f" d{i:03d} ", "Oui",
         f"{1 + i % 28:02d}/{1 + i % 12:02d}/{2016 + i % 6}", str(1 + i % 30)) for i in range(1, 31)]
hosp.append(("H0031", "31", orgs_f[31], "D031", "Non", "43466", "3"))       # 43466 = 01/01/2019 (Excel)
hosp.append(("H0032", "9999", orgs_f[32], "D032", "Non", "15/06/2020", "2"))  # patient absent de S1
hosp += [(None, "33", orgs_f[33], "D033", "Oui", "10/10/2020", "4"),
         ("H0034", "34", orgs_f[34], "D034", "Oui", "31/02/2019", "4"),
         ("H0035", "35", orgs_f[35], "D035", "Oui", "10/10/2020", "-2")]
hosp.append(hosp[0])
ecrire("hospitalisation", COL_HOSP, hosp)

spark.createDataFrame([(json.dumps(volumes),)], "value STRING").coalesce(1).write.mode("overwrite").text(ATTENDU)
print(f"CI attendus Bronze S2 écrits dans {ATTENDU} : {volumes}")
spark.stop()
