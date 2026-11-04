"""
J05 - Référentiel département -> région (Silver).

Sortie : hdfs://namenode:8020/silver/ref_departement (Parquet, 110 lignes) :
code_departement, libelle_departement, code_region_insee, libelle_region, type, run_id.
Source : jobs/lib_geo.py, généré depuis @etalab/decoupage-administratif 6.0.0
(construit sur le code officiel géographique de l'INSEE).

Utilisation par les autres jobs : J22 relie code_region_insee à dim_region pour obtenir
id_region ; J14, J17, J18 déduisent le département avec les fonctions de lib_geo.
Le job contrôle la table et teste les fonctions de dérivation sur des cas connus.
Sortie console : lignes « J05QC,indicateur,valeur » puis « SILVER ref_departement ... ».
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/jobs")
from pyspark.sql import SparkSession, functions as F
from lib_geo import table_departements, departement_depuis_commune, departement_depuis_code_postal

SORTIE = "hdfs://namenode:8020/silver/ref_departement"
RUN_ID = os.environ.get("RUN_ID", "J05_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))

spark = (SparkSession.builder.appName("j05_ref_departement")
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
erreurs = []


def qc(indicateur, valeur, attendu=None):
    print(f"J05QC,{indicateur},{valeur}", flush=True)
    if attendu is not None and valeur != attendu:
        erreurs.append(f"{indicateur}={valeur} (attendu {attendu})")


ref = table_departements(spark).withColumn("run_id", F.lit(RUN_ID))
qc("lignes", ref.count(), 110)
qc("departements_rattaches", ref.filter("type = 'DEPARTEMENT'").count(), 101)
qc("regions_distinctes", ref.filter("type = 'DEPARTEMENT'").select("code_region_insee").distinct().count(), 18)
qc("collectivites_outre_mer", ref.filter("type = 'COM'").count(), 8)
qc("doublons_code", ref.count() - ref.select("code_departement").distinct().count(), 0)

# Tests des fonctions de dérivation : (valeur, attendu)
cas_commune = [("75056", "75"), ("2A004", "2A"), ("2b033", "2B"), ("97411", "974"), ("97608", "976"),
               ("1053", "01"), ("99134", "99"), ("", None), ("ABCDE", None),
               ("20004", None), ("00000", None), ("98611", "986")]
cas_postal = [("75001", "75"), ("20000", "2A"), ("20200", "2B"), ("97400", "974"), ("1000", "01"),
              ("6000", "06"), ("", None), ("7500", "07"), ("98000", None), ("00100", None)]
for nom, cas, fonction in (("commune", cas_commune, departement_depuis_commune),
                           ("code_postal", cas_postal, departement_depuis_code_postal)):
    df = spark.createDataFrame([(v, a) for v, a in cas], "valeur string, attendu string")
    res = df.withColumn("obtenu", fonction("valeur")).collect()
    ko = [f"{r['valeur']}->{r['obtenu']}" for r in res if r["obtenu"] != r["attendu"]]
    qc(f"test_{nom}_echecs", len(ko), 0)
    for k in ko:
        print(f"J05QC,test_{nom}_echec,{k}", flush=True)

if erreurs:
    print("SILVER ERREUR ref_departement " + " ; ".join(erreurs), flush=True)
    spark.stop()
    sys.exit(1)

ref.coalesce(1).write.mode("overwrite").parquet(SORTIE)
relu = spark.read.parquet(SORTIE).count()
qc("lignes_relues", relu, 110)
print(f"SILVER ref_departement valides={relu} rejetes=0 run_id={RUN_ID}", flush=True)
spark.stop()
