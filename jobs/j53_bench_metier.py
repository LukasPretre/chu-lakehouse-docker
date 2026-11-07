"""
J53 - Benchmark des requêtes métier sur le Gold Hive (Spark SQL), avant / après
partitionnement et buckets. Livrable 2 : mesures et graphiques comparatifs.

Variantes de fait_consultation (mêmes données, seul le stockage change) :
  plat         chu_bench.fait_consultation_plat   : ni partition ni bucket (référence « avant »)
  part         chu_bench.fait_consultation_part   : partitionnée par annee
  part_bucket  chu_gold.fait_consultation         : partition annee + 16 buckets id_patient (J30, « après »)
Les deux copies sont (re)créées si absentes, si elles ne viennent pas du même chargement que
chu_gold.fait_consultation (run_id différent), ou si BENCH_RECREER=1.

Requêtes : M1 à M7 de sql/gold_requetes_metier.sql.
- M1 à M5 lisent fait_consultation : elles sont mesurées sur les trois variantes.
- M6 (décès) et M7 (satisfaction) ne lisent pas fait_consultation : une seule mesure (« gold »).
- Une requête dont une table manque est ignorée (BENCHM_IGNOREE).
Différence avec la version PostgreSQL : en Hive la colonne de partition de fait_consultation
est `annee`, pas `id_date`. On ajoute donc un filtre sur `annee` (redondant avec celui sur
id_date) pour que Spark n'ouvre que les partitions utiles. Sans lui, la partition ne servirait à rien.

Protocole (comme J51) : une requête d'échauffement, puis 3 exécutions par requête et par
variante, médiane retenue. Le résultat est entièrement calculé (collect). Les résultats des
variantes doivent être identiques (contrôle d'égalité sur le contenu complet du résultat).

Sortie : « BENCHM,variante,requete,run,secondes,lignes », puis « BENCHM_MED,... »,
« BENCHM_EGALITE,... » et « BENCHM_GAIN,... ». Exemple :
  ... spark-submit ... /jobs/j53_bench_metier.py 2>&1 | grep '^BENCHM' > docs/benchmark/bench_metier_spark.csv
"""
import hashlib
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


def existe(table):
    return spark.catalog.tableExists(f"{GOLD}.{table}")


for t in ("fait_consultation", "dim_temps", "dim_diagnostic", "dim_professionnel"):
    if not existe(t):
        print(f"BENCHM_ERREUR table absente : {GOLD}.{t} (lancer J20 à J25 puis J30)", flush=True)
        sys.exit(1)

# ---------------------------------------------------------------- variantes
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BENCH}")
source = spark.table(f"{GOLD}.fait_consultation")
run_source = source.select("run_id").first()[0]


def a_recreer(table):
    if os.environ.get("BENCH_RECREER") == "1" or not spark.catalog.tableExists(table):
        return True
    return spark.table(table).select("run_id").first()[0] != run_source   # copie d'un ancien chargement


if a_recreer(f"{BENCH}.fait_consultation_plat"):
    source.repartition(8).write.mode("overwrite").format("parquet").saveAsTable(f"{BENCH}.fait_consultation_plat")
    print(f"BENCHM_COPIE,plat,recréée depuis {run_source}", flush=True)
if a_recreer(f"{BENCH}.fait_consultation_part"):
    (source.repartition("annee").write.mode("overwrite").format("parquet")
           .partitionBy("annee").saveAsTable(f"{BENCH}.fait_consultation_part"))
    print(f"BENCHM_COPIE,part,recréée depuis {run_source}", flush=True)

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
# (nom, tables Gold nécessaires, texte) ; {fait} = variante de fait_consultation, {gold} = base Gold.
# Sans {fait} dans le texte, la requête n'est mesurée qu'une fois (variante « gold »).
REQUETES = [
    ("M1_etablissement_mois", ("dim_etablissement", "dim_region"), """
        SELECT r.libelle_region, e.identifiant_organisation, e.raison_sociale, t.annee, t.mois,
               COUNT(*) AS nb_consultations,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee, t.mois), 2) AS taux_pct
        FROM {fait} f
        JOIN {gold}.dim_temps         t ON t.id_date = f.id_date
        JOIN {gold}.dim_etablissement e ON e.id_etablissement = f.id_etablissement
        JOIN {gold}.dim_region        r ON r.id_region = e.id_region
        WHERE f.id_date BETWEEN 20190101 AND 20191231
          AND f.annee = 2019
        GROUP BY r.libelle_region, e.identifiant_organisation, e.raison_sociale, t.annee, t.mois"""),
    ("M2_diagnostic_trimestre", (), """
        SELECT d.code_diagnostic, d.libelle_diagnostic, t.annee, t.trimestre,
               COUNT(*) AS nb_consultations,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee, t.trimestre), 3) AS taux_pct
        FROM {fait} f
        JOIN {gold}.dim_temps      t ON t.id_date = f.id_date
        JOIN {gold}.dim_diagnostic d ON d.id_diagnostic = f.id_diagnostic
        WHERE f.id_date BETWEEN 20190101 AND 20191231
          AND f.annee = 2019
        GROUP BY d.code_diagnostic, d.libelle_diagnostic, t.annee, t.trimestre"""),
    ("M3_professionnel_annee", (), """
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
        GROUP BY p.profession, p.specialite, t.annee"""),
    ("M4a_hospitalisation_trimestre", ("fait_hospitalisation",), """
        WITH hospit AS (
            SELECT t.annee, t.trimestre, COUNT(*) AS nb_hospitalisations,
                   COUNT(DISTINCT h.id_patient) AS nb_patients_hospitalises
            FROM {gold}.fait_hospitalisation h
            JOIN {gold}.dim_temps t ON t.id_date = h.id_date_entree
            WHERE h.id_date_entree BETWEEN 20190101 AND 20191231
            GROUP BY t.annee, t.trimestre
        ), file_active AS (
            SELECT t.annee, t.trimestre, COUNT(DISTINCT f.id_patient) AS nb_patients_file_active
            FROM {fait} f
            JOIN {gold}.dim_temps t ON t.id_date = f.id_date
            WHERE f.id_date BETWEEN 20190101 AND 20191231
              AND f.annee = 2019
            GROUP BY t.annee, t.trimestre
        )
        SELECT a.annee, a.trimestre, a.nb_patients_file_active,
               COALESCE(h.nb_hospitalisations, 0) AS nb_hospitalisations,
               COALESCE(h.nb_patients_hospitalises, 0) AS nb_patients_hospitalises,
               ROUND(100.0 * COALESCE(h.nb_patients_hospitalises, 0)
                     / NULLIF(a.nb_patients_file_active, 0), 3) AS taux_hospitalisation_pct
        FROM file_active a
        LEFT JOIN hospit h ON h.annee = a.annee AND h.trimestre = a.trimestre"""),
    ("M4b_hospitalisation_diagnostic", ("fait_hospitalisation",), """
        WITH hospit AS (
            SELECT t.annee, h.id_diagnostic, COUNT(*) AS nb_hospitalisations,
                   COUNT(DISTINCT h.id_patient) AS nb_patients_hospitalises
            FROM {gold}.fait_hospitalisation h
            JOIN {gold}.dim_temps t ON t.id_date = h.id_date_entree
            WHERE h.id_date_entree BETWEEN 20160101 AND 20211231
            GROUP BY t.annee, h.id_diagnostic
        ), file_active AS (
            SELECT t.annee, f.id_diagnostic, COUNT(DISTINCT f.id_patient) AS nb_patients_file_active
            FROM {fait} f
            JOIN {gold}.dim_temps t ON t.id_date = f.id_date
            WHERE f.id_date BETWEEN 20160101 AND 20211231
              AND f.annee BETWEEN 2016 AND 2021
            GROUP BY t.annee, f.id_diagnostic
        )
        SELECT h.annee, d.code_diagnostic, d.libelle_diagnostic, h.nb_hospitalisations,
               h.nb_patients_hospitalises, a.nb_patients_file_active,
               ROUND(100.0 * h.nb_patients_hospitalises / NULLIF(a.nb_patients_file_active, 0), 2)
                   AS taux_hospitalisation_pct
        FROM hospit h
        JOIN {gold}.dim_diagnostic d ON d.id_diagnostic = h.id_diagnostic
        LEFT JOIN file_active a ON a.annee = h.annee AND a.id_diagnostic = h.id_diagnostic"""),
    ("M5_hospitalisation_sexe_age", ("fait_hospitalisation", "dim_patient"), """
        WITH evenements AS (
            SELECT 'H' AS source, h.id_patient, t.annee
            FROM {gold}.fait_hospitalisation h
            JOIN {gold}.dim_temps t ON t.id_date = h.id_date_entree
            WHERE h.id_date_entree BETWEEN 20190101 AND 20191231
            UNION ALL
            SELECT 'C' AS source, f.id_patient, t.annee
            FROM {fait} f
            JOIN {gold}.dim_temps t ON t.id_date = f.id_date
            WHERE f.id_date BETWEEN 20190101 AND 20191231
              AND f.annee = 2019
        ), classes AS (
            SELECT e.source, e.id_patient, p.sexe,
                   CASE
                     WHEN p.annee_naissance IS NULL OR e.annee - p.annee_naissance < 0 THEN 'Inconnu'
                     WHEN e.annee - p.annee_naissance >= 90 THEN '90+'
                     ELSE CONCAT(CAST(FLOOR((e.annee - p.annee_naissance) / 10) * 10 AS INT), '-',
                                 CAST(FLOOR((e.annee - p.annee_naissance) / 10) * 10 + 9 AS INT))
                   END AS tranche_age
            FROM evenements e
            JOIN {gold}.dim_patient p ON p.id_patient = e.id_patient
        )
        SELECT sexe, tranche_age,
               COUNT(DISTINCT CASE WHEN source = 'C' THEN id_patient END) AS nb_patients_file_active,
               COUNT(DISTINCT CASE WHEN source = 'H' THEN id_patient END) AS nb_patients_hospitalises,
               SUM(CASE WHEN source = 'H' THEN 1 ELSE 0 END)             AS nb_hospitalisations,
               ROUND(100.0 * COUNT(DISTINCT CASE WHEN source = 'H' THEN id_patient END)
                     / NULLIF(COUNT(DISTINCT CASE WHEN source = 'C' THEN id_patient END), 0), 3)
                   AS taux_hospitalisation_pct
        FROM classes
        GROUP BY sexe, tranche_age"""),
    ("M6_deces_region_mois", ("fait_deces", "dim_region"), """
        SELECT r.code_region_insee, r.libelle_region, t.mois,
               COUNT(*) AS nb_deces,
               SUM(COUNT(*)) OVER (PARTITION BY r.code_region_insee) AS nb_deces_annee_region,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.mois), 2) AS part_du_mois_pct
        FROM {gold}.fait_deces dc
        JOIN {gold}.dim_temps  t ON t.id_date = dc.id_date
        JOIN {gold}.dim_region r ON r.id_region = dc.id_region
        WHERE dc.id_date BETWEEN 20190101 AND 20191231
        GROUP BY r.code_region_insee, r.libelle_region, t.mois"""),
    ("M7_satisfaction_region", ("fait_satisfaction", "dim_etablissement", "dim_region"), """
        SELECT r.code_region_insee, r.libelle_region, s.type_enquete,
               COUNT(DISTINCT s.id_etablissement) AS nb_etablissements,
               SUM(s.nb_reponses)                 AS nb_reponses,
               ROUND(SUM(s.score_global * s.nb_reponses) / NULLIF(SUM(s.nb_reponses), 0), 2)
                   AS score_global_pondere,
               ROUND(SUM(s.taux_recommandation * s.nb_reponses)
                     / NULLIF(SUM(CASE WHEN s.taux_recommandation IS NOT NULL THEN s.nb_reponses END), 0), 2)
                   AS taux_recommandation_pondere
        FROM {gold}.fait_satisfaction s
        JOIN {gold}.dim_etablissement e ON e.id_etablissement = s.id_etablissement
        JOIN {gold}.dim_region        r ON r.id_region = e.id_region
        WHERE s.id_date BETWEEN 20200101 AND 20201231
          AND s.score_global IS NOT NULL
          AND s.nb_reponses > 0
        GROUP BY r.code_region_insee, r.libelle_region, s.type_enquete"""),
]


def executer(sql):
    debut = time.perf_counter()
    lignes = spark.sql(sql).collect()
    return time.perf_counter() - debut, lignes


def empreinte(lignes):
    """Nombre de lignes et empreinte du contenu complet, indépendante de l'ordre des lignes."""
    contenu = sorted(repr(tuple(r)) for r in lignes)
    return len(lignes), hashlib.sha256("\n".join(contenu).encode()).hexdigest()[:12]


# Échauffement : charge les métadonnées et le code, hors mesure
executer(REQUETES[1][2].format(fait=VARIANTES["plat"], gold=GOLD))

medianes, mesurees = {}, []
for nom, tables, modele in REQUETES:
    manquantes = [t for t in tables if not existe(t)]
    if manquantes:
        print(f"BENCHM_IGNOREE,{nom},table(s) absente(s) : {' '.join(manquantes)}", flush=True)
        continue
    variantes = VARIANTES if "{fait}" in modele else {"gold": None}
    empreintes = {}
    for v, table in variantes.items():
        sql = modele.format(fait=table, gold=GOLD)
        temps = []
        for run in range(1, RUNS + 1):
            s, lignes = executer(sql)
            temps.append(s)
            print(f"BENCHM,{v},{nom},{run},{s:.3f},{len(lignes)}", flush=True)
        medianes[(v, nom)] = statistics.median(temps)
        print(f"BENCHM_MED,{v},{nom},{medianes[(v, nom)]:.3f}", flush=True)
        empreintes[v] = empreinte(lignes)
    if len(variantes) > 1:
        egal = len(set(empreintes.values())) == 1
        print(f"BENCHM_EGALITE,{nom},{'OK' if egal else 'ECART'},{empreintes}", flush=True)
        mesurees.append(nom)

for nom in mesurees:
    ref = medianes[("plat", nom)]
    for v in VARIANTES:
        print(f"BENCHM_GAIN,{v},{nom},{ref / medianes[(v, nom)]:.2f}", flush=True)
spark.stop()
