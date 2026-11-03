#!/usr/bin/env bash
# Tests de la couche Silver : lance jobs/check_silver.py sur le cluster Spark et résume le résultat.
#
# Usage (racine du dépôt, après ./run_silver_s1.sh) :
#   ./test_silver.sh
#   STRICT=1 ./test_silver.sh     une table Silver absente (fichiers, décès...) compte comme un échec
#
# Code de retour : 0 si tous les contrôles passent, 1 sinon (utilisé par la CI).

set -u
cd "$(dirname "$0")" || exit 1
[ -f .env ] && { set -a; . ./.env; set +a; }

SPARK=${SPARK_MASTER_CONTAINER:-chu_spark_master}
MASTER=${SPARK_MASTER_URL:-spark://spark-master:7077}

PASS=0; FAIL=0; SKIP=0
ok()   { echo "  [OK]    $1"; PASS=$((PASS+1)); }
ko()   { echo "  [ECHEC] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP]  $1"; SKIP=$((SKIP+1)); }

# resume PREFIXE "sortie" code_retour : affiche les lignes PREFIXE|STATUT|message d'un job Spark
resume() {
  local prefixe=$1 sortie=$2 rc=$3 lignes statut message
  lignes=$(grep -oE "${prefixe}\|(OK|ECHEC|SKIP)\|.*" <<<"$sortie")
  if [ -z "$lignes" ]; then
    ko "le job de contrôle n'a rien produit (code $rc), fin de la sortie :"
    tail -25 <<<"$sortie" | sed 's/^/          /'
    return
  fi
  while IFS='|' read -r _ statut message; do
    case $statut in
      OK) ok "$message" ;;
      SKIP) skip "$message" ;;
      *) ko "$message" ;;
    esac
  done <<<"$lignes"
  if [ "$rc" -ne 0 ] && ! grep -q "|ECHEC|" <<<"$lignes"; then
    ko "le job de contrôle s'est arrêté en erreur (code $rc), fin de la sortie :"
    tail -15 <<<"$sortie" | sed 's/^/          /'
  fi
}

echo "== Tests Silver (jobs/check_silver.py)"
sortie=$(docker exec -e STRICT="${STRICT:-0}" "$SPARK" \
         /opt/spark/bin/spark-submit --master "$MASTER" /jobs/check_silver.py 2>&1)
resume SILVERTEST "$sortie" $?

echo
echo "Résultat Silver : $PASS réussis, $FAIL en échec, $SKIP ignorés"
[ "$FAIL" -eq 0 ]