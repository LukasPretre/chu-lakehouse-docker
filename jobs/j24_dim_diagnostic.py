"""
J24 - Gold : DIM_DIAGNOSTIC depuis silver.diagnostic (J11). SCD1, clés stables (voir gold_commun.py).

Colonnes (sql/gold_ddl.sql) : id_diagnostic, code_diagnostic, libelle_diagnostic, run_id, date_chargement.
Règles (Livrable 1, annexe A) :
  - code_diagnostic : code du Silver (déjà en majuscules, sans espaces) ;
  - libelle_diagnostic : vide -> « Non renseigné » (colonne NOT NULL) ;
  - ligne -1 « Inconnu ».
Utilisée par J30 (fait_consultation) et J31 (fait_hospitalisation) par la clé naturelle code_diagnostic.

Lancement (racine du dépôt, sur la VM) :
  docker exec chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 /jobs/j24_dim_diagnostic.py
"""
import sys

sys.path.insert(0, "/jobs")  # gold_commun.py est à côté, dans /jobs
import gold_commun as G  # noqa: E402

spark = G.session("J24_dim_diagnostic")
silver = G.lire_silver(spark, "diagnostic", ["code_diagnostic", "libelle_diagnostic"])

source = silver.select(
    G.texte("code_diagnostic").alias("code_diagnostic"),
    G.texte("libelle_diagnostic", G.NON_RENSEIGNE).alias("libelle_diagnostic"),
)

ok = G.charger_dimension(
    spark, "dim_diagnostic", "id_diagnostic", "code_diagnostic", source,
    inconnu=(-1, "INCONNU", "Inconnu"),
    longueurs={"code_diagnostic": 10, "libelle_diagnostic": 255},
    job="J24",
)
spark.stop()
sys.exit(0 if ok else 1)