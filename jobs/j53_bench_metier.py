"""
J53 - Benchmark des requêtes métier sur le Gold Hive (Spark SQL), avant / après
partitionnement et buckets. Livrable 2 : mesures et graphiques comparatifs.

Variantes de fait_consultation (mêmes données, seul le stockage change) :
  plat         chu_bench.fait_consultation_plat   : ni partition ni bucket (référence « avant »)
  part         chu_bench.fait_consultation_part   : partitionnée par annee
  part_bucket  chu_gold.fait_consultation         : partition annee + 16 buckets id_patient (J30, « après »)
Les deux copies sont (re)créées si absentes, ou si BENCH_RECREER=1.

Requêtes : M2 (I2, consultations par diagnostic et trimestre en 2019) et M3 (I6, activité par
profession et spécialité, 2017 à 2022), reprises de sql/gold_requetes_metier.sql.
Différence avec la version PostgreSQL : en Hive la colonne de partition est `annee`, pas
`id_date`. On ajoute donc un filtre sur `annee` (redondant avec celui sur id_date) pour que
Spark n'ouvre que les partitions utiles. Sans lui, la partition ne servirait à rien.
M1 (établissement) et M4 à M7 (autres faits) s'ajouteront quand leurs tables existeront.

Protocole (comme J51) : une requête d'échauffement, puis 3 exécutions par requête et par
variante, médiane retenue. Le résultat est entièrement calculé (collect). Les résultats des
trois variantes doivent être identiques (contrôle d'égalité affiché).

Sortie : « BENCHM,variante,requete,run,secondes,lignes », puis « BENCHM_MED,... » et
« BENCHM_EGALITE,... ». Exemple :
  ... spark-submit ... /jobs/j53_bench_metier.py 2>&1 | grep '^BENCHM' > docs/benchmark/bench_metier_spark.csv
"""
import os
import statistics
import sys
import time

from pyspark.sql import SparkSession, functions as F

GOLD = os.environ.get("GOLD_DB", "chu_gold")
BENCH = os.environ.get("BENCH_DB", "chu_bench")
RUNS = int(os.environ.get("BENCH_RUNS", "3"))
METASTORE = os.environ.get("METASTORE_URI", "thrift://hive-metastore:9083")

b = SparkSession.builder.appName("j53_bench_metier").enableHiveSupport()
if METASTORE:
    b = (b.config("spark.hadoop.hive.metastore.uris", METASTORE)
          .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
          .config("spark.hadoop.dfs.replication", "1"))
spark = b.config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for t in ("fait_consultation", "dim_temps", "dim_diagnostic", "dim_professionnel"):
    if not spark.catalog.tableExists(f"{GOLD}.{t}"):
        print(f"BENCHM_ERREUR table absente : {GOLD}.{t} (lancer J20 à J25 puis J30)", flush=True)
        sys.exit(1)

# ---------------------------------------------------------------- variantes
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BENCH}")
source = spark.table(f"{GOLD}.fait_consultation")
recreer = os.environ.get("BENCH_RECREER") == "1"
if recreer or not spark.catalog.tableExists(f"{BENCH}.fait_consultation_plat"):
    source.repartition(8).write.mode("overwrite").format("parquet").saveAsTable(f"{BENCH}.fait_consultation_plat")
if recreer or not spark.catalog.tableExists(f"{BENCH}.fait_consultation_part"):
    (source.repartition("annee").write.mode("overwrite").format("parquet")
           .partitionBy("annee").saveAsTable(f"{BENCH}.fait_consultation_part"))

VARIANTES = {
    "plat": f"{BENCH}.fait_consultation_plat",
    "part": f"{BENCH}.fait_consultation_part",
    "part_bucket": f"{GOLD}.fait_consultation",
}
n_ref = spark.table(VARIANTES["part_bucket"]).count()
for v, table in VARIANTES.items():
    n = spark.table(table).count()
    print(f"BENCHM_VARIANTE,{v},{table},{n}", flush=True)
    if n != n_ref:
        print(f"BENCHM_ERREUR {v} : {n} lignes au lieu de {n_ref} (relancer avec BENCH_RECREER=1)", flush=True)
        sys.exit(1)

# ---------------------------------------------------------------- requêtes
REQUETES = {
    "M2_diagnostic_trimestre": """
        SELECT d.code_diagnostic, d.libelle_diagnostic, t.annee, t.trimestre,
               COUNT(*) AS nb_consultations,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee, t.trimestre), 3) AS taux_pct
        FROM {fait} f
        JOIN {gold}.dim_temps      t ON t.id_date = f.id_date
        JOIN {gold}.dim_diagnostic d ON d.id_diagnostic = f.id_diagnostic
        WHERE f.id_date BETWEEN 20190101 AND 20191231
          AND f.annee = 2019
        GROUP BY d.code_diagnostic, d.libelle_diagnostic, t.annee, t.trimestre""",
    "M3_professionnel_annee": """
        SELECT p.profession, p.specialite, t.annee,
               COUNT(*)                           AS nb_consultations,
               COUNT(DISTINCT f.id_professionnel) AS nb_professionnels,
               ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT f.id_professionnel), 2) AS consultations_par_professionnel,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee), 3) AS taux_pct,
               COUNT(DISTINCT f.id_etablissement) AS nb_etablissements
        FROM {fait} f
        JOIN {gold}.dim_temps         t ON t.id_date = f.id_date
        JOIN {gold}.dim_professionnel p ON p.id_professionnel = f.id_professionnel
        WHERE f.id_date BETWEEN 20170101 AND 20221231
          AND f.annee BETWEEN 2017 AND 2022
        GROUP BY p.profession, p.specialite, t.annee""",
}


def executer(sql):
    debut = time.perf_counter()
    lignes = spark.sql(sql).collect()
    return time.perf_counter() - debut, lignes


# Échauffement : charge les métadonnées et le code, hors mesure
executer(REQUETES["M2_diagnostic_trimestre"].format(fait=VARIANTES["plat"], gold=GOLD))

medianes = {}
for nom, modele in REQUETES.items():
    empreintes = {}
    for v, table in VARIANTES.items():
        sql = modele.format(fait=table, gold=GOLD)
        temps = []
        for run in range(1, RUNS + 1):
            s, lignes = executer(sql)
            temps.append(s)
            print(f"BENCHM,{v},{nom},{run},{s:.3f},{len(lignes)}", flush=True)
        medianes[(v, nom)] = statistics.median(temps)
        print(f"BENCHM_MED,{v},{nom},{medianes[(v, nom)]:.3f}", flush=True)
        # Empreinte du résultat : nombre de lignes et total des consultations
        empreintes[v] = (len(lignes), sum(r["nb_consultations"] for r in lignes))
    egal = len(set(empreintes.values())) == 1
    print(f"BENCHM_EGALITE,{nom},{'OK' if egal else 'ECART'},{empreintes}", flush=True)

for nom in REQUETES:
    ref = medianes[("plat", nom)]
    for v in VARIANTES:
        print(f"BENCHM_GAIN,{v},{nom},{ref / medianes[(v, nom)]:.2f}", flush=True)
spark.stop()
