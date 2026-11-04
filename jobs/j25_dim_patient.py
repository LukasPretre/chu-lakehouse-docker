"""
J25 - Gold : DIM_PATIENT depuis silver.patient (J10). SCD1, clés stables (voir gold_commun.py).

Colonnes (sql/gold_ddl.sql) : id_patient, patient_pseudo, sexe, annee_naissance, run_id, date_chargement.
Règles (Livrable 1, annexe A) :
  - patient_pseudo : HMAC-SHA256 du Silver, repris tel quel (aucun identifiant en clair) ;
  - sexe : F ou M, autre valeur ou vide -> I ;
  - ligne -1 « Inconnu ».
Utilisée par J30 (fait_consultation) et J31 (fait_hospitalisation) par la clé naturelle patient_pseudo.

Lancement (racine du dépôt, sur la VM) :
  docker exec chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /jobs/j25_dim_patient.py
"""
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/jobs")  # gold_commun.py est à côté, dans /jobs
import gold_commun as G  # noqa: E402

spark = G.session("J25_dim_patient")
silver = G.lire_silver(spark, "patient", ["patient_pseudo", "sexe", "annee_naissance"])

source = silver.select(
    F.trim("patient_pseudo").alias("patient_pseudo"),
    F.when(F.upper(F.trim("sexe")).isin("F", "M"), F.upper(F.trim("sexe"))).otherwise(F.lit("I")).alias("sexe"),
    F.col("annee_naissance").cast("int").alias("annee_naissance"),
)

ok = G.charger_dimension(
    spark, "dim_patient", "id_patient", "patient_pseudo", source,
    inconnu=(-1, "INCONNU", "I", None),
    longueurs={"patient_pseudo": 64, "sexe": 1},
    job="J25",
)
spark.stop()
sys.exit(0 if ok else 1)