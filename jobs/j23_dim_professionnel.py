"""
J23 - Gold : DIM_PROFESSIONNEL depuis silver.professionnel (J11). SCD1, clés stables (voir gold_commun.py).

Colonnes (sql/gold_ddl.sql) : id_professionnel, identifiant_pseudo, type_identifiant,
categorie_professionnelle, profession, fonction, specialite, run_id, date_chargement.
Règles (Livrable 1, annexe A) :
  - identifiant_pseudo : professionnel_pseudo du Silver (HMAC-SHA256), nom et prénom exclus ;
  - type_identifiant, categorie_professionnelle : valeurs de la source, vide -> NULL ;
  - profession, fonction, specialite : vide -> « Non renseigné ». J11 écrit « Inconnue » quand le
    code spécialité est absent du référentiel : remplacé aussi par « Non renseigné » ;
  - ligne -1 « Inconnu ».
Utilisée par J30 (fait_consultation) par la clé naturelle identifiant_pseudo.

Lancement (racine du dépôt, sur la VM) :
  docker exec chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /jobs/j23_dim_professionnel.py
"""
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/jobs")  # gold_commun.py est à côté, dans /jobs
import gold_commun as G  # noqa: E402

NR = G.NON_RENSEIGNE
spark = G.session("J23_dim_professionnel")
silver = G.lire_silver(spark, "professionnel", [
    "professionnel_pseudo", "type_identifiant", "categorie_professionnelle", "profession", "fonction", "specialite"])

source = silver.select(
    F.trim("professionnel_pseudo").alias("identifiant_pseudo"),
    G.texte("type_identifiant").alias("type_identifiant"),
    G.texte("categorie_professionnelle").alias("categorie_professionnelle"),
    G.texte("profession", NR).alias("profession"),
    G.texte("fonction", NR, vides=("Inconnue",)).alias("fonction"),
    G.texte("specialite", NR, vides=("Inconnue",)).alias("specialite"),
)

ok = G.charger_dimension(
    spark, "dim_professionnel", "id_professionnel", "identifiant_pseudo", source,
    inconnu=(-1, "INCONNU", "Inconnu", "Inconnu", "Inconnu", "Inconnu", "Inconnu"),
    longueurs={"identifiant_pseudo": 64, "type_identifiant": 10, "categorie_professionnelle": 30,
               "profession": 60, "fonction": 30, "specialite": 80},
    job="J23",
)
spark.stop()
sys.exit(0 if ok else 1)