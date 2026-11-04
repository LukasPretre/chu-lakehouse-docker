#!/usr/bin/env bash
# Benchmark PostgreSQL : table plate, indexee, partitionnee par date.
#
#   ./bench_pg.sh prepare   exporte la consultation (Spark, J52), la charge dans PostgreSQL et construit
#                           les 3 variantes. BENCH_SCALE=10 ./bench_pg.sh prepare : donnees multipliees
#                           (a declarer dans le rapport : donnees de test dupliquees).
#   ./bench_pg.sh run       mesure 6 requetes x 3 variantes x 3 executions (EXPLAIN ANALYZE),
#                           ecrit les lignes "PGBENCH,..." a rediriger :
#                           ./bench_pg.sh run > docs/benchmark/bench_pg_x1.csv
#
# Variables : PG_USER (defaut chu_admin, lu dans .env), PG_DB (defaut chu_gold_db),
#             PGBENCH_PSQL (commande psql complete, pour tester hors Docker),
#             BENCH_PG_CSV (fichier CSV local a charger a la place de l'export HDFS, pour tester).
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] && { set -a; . ./.env; set +a; }
PG_USER="${PG_USER:-chu_admin}"; PG_DB="${PG_DB:-chu_gold_db}"
PG_CONTAINER="${PG_CONTAINER:-chu_postgres}"; RUNS=3
VARIANTES="consultation_plat consultation_idx consultation_part"

psql_run() {
  if [ -n "${PGBENCH_PSQL:-}" ]; then $PGBENCH_PSQL "$@"
  else docker exec -i "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" "$@"; fi
}

prepare() {
  if [ -z "${BENCH_PG_CSV:-}" ]; then
    echo "== Export Spark (J52)" >&2
    docker exec chu_namenode hdfs dfs -mkdir -p /bench >/dev/null 2>&1 || true
    docker exec chu_namenode hdfs dfs -chmod -R 777 /bench >/dev/null 2>&1 || true
    local log
    log=$(docker exec -e BENCH_SCALE="${BENCH_SCALE:-1}" chu_spark_master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 /jobs/j52_bench_pg_export.py 2>&1) || true
    if ! grep '^PGEXPORT' <<<"$log" >&2; then
      echo "[ECHEC] export Spark ; fin de la sortie :" >&2
      grep -v -E 'WARN|INFO' <<<"$log" | tail -25 >&2
      exit 1
    fi
  fi
  echo "== Table de reference" >&2
  psql_run -v ON_ERROR_STOP=1 -q < sql/bench_pg_01_plat.sql
  echo "== Chargement (COPY)" >&2
  if [ -n "${BENCH_PG_CSV:-}" ]; then cat "$BENCH_PG_CSV"
  else docker exec chu_namenode hdfs dfs -cat '/bench/pg_export/part-*.csv'; fi \
    | psql_run -v ON_ERROR_STOP=1 -q -c "COPY bench.consultation_plat FROM STDIN WITH (FORMAT csv)"
  echo "== Variantes (index, partitions) et ANALYZE" >&2
  psql_run -v ON_ERROR_STOP=1 -q < sql/bench_pg_02_variantes.sql >&2
  echo "[OK] pret : ./bench_pg.sh run" >&2
}

mediane() { sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }

run() {
  local patient
  patient=$(psql_run -tA -c "SELECT patient_pseudo FROM bench.consultation_plat OFFSET 123456 LIMIT 1")
  [ -n "$patient" ] || patient=$(psql_run -tA -c "SELECT patient_pseudo FROM bench.consultation_plat LIMIT 1")

  declare -A Q
  Q[Q1_filtre_annee]="SELECT count(*), avg(duree_minutes) FROM bench.__T__ WHERE date_consultation >= '2019-01-01' AND date_consultation < '2020-01-01'"
  Q[Q2_consultations_par_mois]="SELECT mois, count(*) FROM bench.__T__ WHERE date_consultation >= '2019-01-01' AND date_consultation < '2020-01-01' GROUP BY mois ORDER BY mois"
  Q[Q3_top_patients]="SELECT patient_pseudo, count(*) AS nb FROM bench.__T__ GROUP BY patient_pseudo ORDER BY nb DESC LIMIT 20"
  Q[Q4_patients_plus_de_3_visites_par_an]="SELECT count(*) FROM (SELECT patient_pseudo, annee FROM bench.__T__ GROUP BY patient_pseudo, annee HAVING count(*) > 3) x"
  Q[Q5_duree_par_professionnel_et_diagnostic]="SELECT professionnel_pseudo, code_diagnostic, count(*) AS nb, avg(duree_minutes) FROM bench.__T__ WHERE date_consultation >= '2018-01-01' AND date_consultation < '2020-01-01' GROUP BY professionnel_pseudo, code_diagnostic ORDER BY nb DESC LIMIT 50"
  Q[Q6_historique_d_un_patient]="SELECT * FROM bench.__T__ WHERE patient_pseudo = '${patient}' ORDER BY date_consultation"

  echo "PGBENCH,variante,requete,run,ms,seq_scans,index_scans,partitions_lues"
  local nom v r out ms seq idx parts temps
  for nom in $(printf '%s\n' "${!Q[@]}" | sort); do
    for v in $VARIANTES; do
      temps=""
      for r in $(seq 1 $RUNS); do
        out=$(psql_run -tA -c "EXPLAIN (ANALYZE, BUFFERS) ${Q[$nom]//__T__/$v}")
        ms=$(sed -n 's/^Execution Time: \([0-9.]*\) ms.*/\1/p' <<<"$out")
        seq=$(grep -c 'Seq Scan' <<<"$out" || true)
        idx=$(grep -c -E 'Index Scan|Index Only Scan|Bitmap Index Scan' <<<"$out" || true)
        parts=$( { grep -o -w -E 'consultation_part_(20[0-9]{2}|defaut)' <<<"$out" || true; } | sort -u | wc -l)
        echo "PGBENCH,$v,$nom,$r,$ms,$seq,$idx,$parts"
        temps="$temps$ms"$'\n'
      done
      echo "PGBENCH,$v,$nom,mediane,$(printf '%s' "$temps" | mediane),,,"
    done
  done
}

case "${1:-}" in
  prepare) prepare ;;
  run)     run ;;
  *) echo "usage : $0 prepare | run" >&2; exit 2 ;;
esac
