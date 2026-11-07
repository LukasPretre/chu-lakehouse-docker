#!/usr/bin/env bash
# Benchmark PostgreSQL des requêtes métier (sql/gold_requetes_metier.sql) sur le Gold publié par J34.
#
#   ./bench_metier_pg.sh avant_index     mesure M1 à M7 et écrit docs/benchmark/bench_metier_pg_avant_index.csv
#   ./bench_metier_pg.sh apres_index     idem, après sql/gold_index_partition.sql
#
# Méthode (comme bench_pg.sh) : ANALYZE des tables, puis pour chaque requête une exécution
# d'échauffement non comptée et 3 exécutions mesurées par EXPLAIN (ANALYZE) ; on garde le
# temps d'exécution ("Execution Time") et la médiane des 3.
# Sortie : PGMETIER,<etiquette>,<requete>,<execution>,<ms> puis PGMETIER,<etiquette>,<requete>,mediane,<ms>
# Variables : PG_USER (lu dans .env), PG_DB (défaut chu_gold_db), PG_CONTAINER (défaut chu_postgres),
#             PGBENCH_PSQL (commande psql complète, pour tester hors Docker), RUNS (défaut 3).
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] && { set -a; . ./.env; set +a; }
PG_USER="${PG_USER:-chu_admin}"; PG_DB="${PG_DB:-chu_gold_db}"
PG_CONTAINER="${PG_CONTAINER:-chu_postgres}"; RUNS="${RUNS:-3}"
ETIQUETTE="${1:?usage : ./bench_metier_pg.sh <etiquette>   (ex. avant_index, apres_index)}"
SQL=sql/gold_requetes_metier.sql
SORTIE="docs/benchmark/bench_metier_pg_${ETIQUETTE}.csv"
[ -f "$SQL" ] || { echo "[ECHEC] $SQL introuvable" >&2; exit 1; }

psql_run() {
  if [ -n "${PGBENCH_PSQL:-}" ]; then $PGBENCH_PSQL "$@"
  else docker exec -i "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" "$@"; fi
}

# Découpe le fichier : une requête par bloc « -- Mx (Iy) », terminée par un point-virgule.
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
awk -v dir="$TMP" '
  /^-- M[0-9]+[a-z]? / { nom = $2; buf = ""; next }
  /^--/ || /^[[:space:]]*$/ { next }
  nom != "" { buf = buf $0 "\n"
              if ($0 ~ /;[[:space:]]*$/) { sub(/;[[:space:]]*\n$/, "\n", buf)
                                           f = dir "/" nom ".sql"; printf "%s", buf > f; close(f)
                                           print nom > (dir "/liste"); nom = "" } }
' "$SQL"
[ -s "$TMP/liste" ] || { echo "[ECHEC] aucune requête trouvée dans $SQL" >&2; exit 1; }

mkdir -p docs/benchmark
echo "== ANALYZE des tables du Gold" >&2
psql_run -X -q -c "ANALYZE dim_temps, dim_patient, dim_professionnel, dim_diagnostic, dim_region,
  dim_etablissement, fait_consultation, fait_hospitalisation, fait_deces, fait_satisfaction;" >/dev/null

temps() {   # temps d'exécution (ms) d'une requête, lu dans EXPLAIN ANALYZE
  { echo "EXPLAIN (ANALYZE)"; cat "$1"; echo ";"; } | psql_run -X -q -At -v ON_ERROR_STOP=1 -f - \
    | sed -n 's/^Execution Time: \([0-9.]*\) ms$/\1/p'
}

: > "$SORTIE"
while read -r q; do
  temps "$TMP/$q.sql" >/dev/null          # échauffement (cache), non compté
  mesures=()
  for i in $(seq 1 "$RUNS"); do
    ms=$(temps "$TMP/$q.sql")
    [ -n "$ms" ] || { echo "[ECHEC] $q : pas de temps d'exécution (requête en erreur ?)" >&2; exit 1; }
    mesures+=("$ms")
    echo "PGMETIER,$ETIQUETTE,$q,$i,$ms" | tee -a "$SORTIE"
  done
  med=$(printf '%s\n' "${mesures[@]}" | sort -g | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
  echo "PGMETIER,$ETIQUETTE,$q,mediane,$med" | tee -a "$SORTIE"
done < "$TMP/liste"
echo "[OK] $(grep -c ',mediane,' "$SORTIE") requêtes mesurées, résultats dans $SORTIE" >&2
