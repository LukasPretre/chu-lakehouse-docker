"""
J21 - Gold : DIM_REGION dans Hive, copie telle quelle de la table PostgreSQL.

dim_region est tenue dans PostgreSQL : ses 20 lignes (18 régions INSEE, Étranger, Inconnu -1)
sont insérées par sql/gold_ddl.sql. J21 les recopie dans <GOLD_DB>.dim_region pour que les faits
calculés dans Spark (J32 décès) retrouvent les clés. id_region = code INSEE en entier.
J34 ne recopie jamais cette table dans l'autre sens.

Lancement (racine du dépôt, sur la VM ; le pilote JDBC et les identifiants sont passés au job) :
  docker cp drivers/postgresql-42.7.3.jar chu_spark_master:/tmp/postgresql.jar
  docker exec -e PG_USER -e PG_PASSWORD chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --jars /tmp/postgresql.jar /jobs/j21_dim_region.py
(./run_pipeline.sh et LANCER_JOBS=1 ./test_gold.sh le font tout seuls.)
"""
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/jobs")
import gold_commun as G  # noqa: E402
import pg_commun as P  # noqa: E402

COLONNES = ["id_region", "code_region_insee", "libelle_region", "run_id", "date_chargement"]

spark = G.session("J21_dim_region")
pg = P.lire(spark, "public.dim_region").select(*COLONNES).cache()
n_pg = pg.count()
(pg.withColumn("code_region_insee", F.trim("code_region_insee"))
   .coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"{G.GOLD_DB}.dim_region"))

hive = spark.table(f"{G.GOLD_DB}.dim_region")
controles = {
    "lignes_hive_egal_pg": (hive.count(), n_pg),
    "lignes_inconnu": (hive.filter("id_region = -1").count(), 1),
    "doublons_cle": (hive.count() - hive.select("id_region").distinct().count(), 0),
}
ok = True
for nom, (valeur, attendu) in controles.items():
    statut = "OK" if valeur == attendu else f"ECART (attendu {attendu})"
    ok = ok and statut == "OK"
    print(f"GOLD dim_region {nom:22s} = {valeur:>6}  {statut}", flush=True)
print(f"GOLD dim_region table={G.GOLD_DB}.dim_region lignes={hive.count()} (copie de PostgreSQL)", flush=True)
spark.stop()
sys.exit(0 if ok else 1)