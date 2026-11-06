#!/usr/bin/env bash
# Chaîne complète du lakehouse, dans l'ordre, puis contrôles :
#   1. Silver S1 (J10 à J12)            ./run_silver_s1.sh
#   2. Silver S2 (J14 à J16)            ./run_silver_s2.sh   (ignoré si le Bronze S2 est absent)
#   3. Référentiel géographique (J05)
#   4. Dimensions du Gold (J20, J25, J23, J24) puis faits (J30 à J33)
#   5. Tests : test_silver.sh, test_gold.sh
#   6. Rapports qualité (J40 S1 et S2) dans docs/qualite/   (avec QUALITE=1)
#
# Usage (racine du dépôt) :
#   ./run_pipeline.sh              chaîne complète et tests
#   QUALITE=1 ./run_pipeline.sh    idem, puis régénère docs/qualite/qualite_s1.csv et qualite_s2.csv
#   DEPUIS=4 ./run_pipeline.sh     reprend à l'étape 4 (Silver déjà à jour)
# Prérequis : pile démarrée (./bootstrap.sh), Bronze S1 chargé (NiFi J01), Bronze S2 si possible (NiFi J02).
# Rejouable : chaque job réécrit ses propres tables ; les clés du Gold restent stables.
# S'arrête à la première étape en échec. Aucun secret n'est affiché.

set -euo pipefail
cd "$(dirname "$0")"

SPARK=chu_spark_master
MASTER=spark://spark-master:7077
DEPUIS=${DEPUIS:-1}
debut=$(date +%s)

etape() { printf '\n######## %s\n' "$1"; }
echec() { printf '[ECHEC] %s\n' "$1" >&2; exit 1; }

# job fichier.py motif : lance un job Spark, affiche ses lignes de contrôle, échoue si le code
# retour n'est pas 0, si aucune ligne ne correspond au motif, ou si une ligne d'erreur apparaît.
# duree job debut : affiche la durée du job ; au-delà de 2 minutes (objectif du projet), le signale
duree() {
  local s=$(( $(date +%s) - $2 ))
  if [ "$s" -gt 120 ]; then echo "[ATTENTION] $1 : ${s} s (objectif : 2 min par job)"
  else echo "[..] $1 : ${s} s"; fi
}

job() {
  local fichier=$1 motif=$2 rc=0 out t0
  t0=$(date +%s)
  echo "== $fichier"
  out=$(docker exec "$SPARK" /opt/spark/bin/spark-submit --master "$MASTER" "/jobs/$fichier" 2>&1) || rc=$?
  grep -E "$motif" <<<"$out" | grep -v -E '^GOLDQC,lignes_annee_' || true
  if [ "$rc" -ne 0 ] || ! grep -q -E "$motif" <<<"$out" || grep -q -E 'ERREUR|ECART' <<<"$out"; then
    echo "[ECHEC] $fichier (code $rc), fin de la sortie :" >&2
    tail -20 <<<"$out" >&2
    exit 1
  fi
  duree "$fichier" "$t0"
}

[ -f .env ] || echec ".env introuvable (lancer ./bootstrap.sh)"
docker inspect -f '{{.State.Running}}' "$SPARK" 2>/dev/null | grep -q true || echec "conteneur $SPARK arrêté (docker compose up -d)"

if [ "$DEPUIS" -le 1 ]; then
  etape "1. Silver S1 (J10, J11, J12)"
  ./run_silver_s1.sh
fi

if [ "$DEPUIS" -le 2 ]; then
  etape "2. Silver S2 (J14, J15, J16)"
  if docker exec chu_namenode hdfs dfs -test -d /bronze/s2 2>/dev/null; then
    ./run_silver_s2.sh
  else
    echo "[..] Bronze S2 absent (NiFi J02 pas encore lancé) : étape ignorée"
  fi
fi

if [ "$DEPUIS" -le 3 ]; then
  etape "3. Référentiel département -> région (J05)"
  job j05_ref_departement.py '^(SILVER|J05QC,test_)'
fi

if [ "$DEPUIS" -le 4 ]; then
  etape "4. Gold : dimensions puis faits"
  job j20_dimensions_generees.py '^GOLD dim_temps table='
  job j25_dim_patient.py         '^GOLD dim_patient (table=|cles_modifiees)'
  job j23_dim_professionnel.py   '^GOLD dim_professionnel (table=|cles_modifiees)'
  job j24_dim_diagnostic.py      '^GOLD dim_diagnostic (table=|cles_modifiees)'
  job j30_fait_consultation.py   '^GOLD(QC,(perte|inconnu|doublons)| fait_consultation)'
  job j31_fait_hospitalisation.py '^GOLD(QC,(perte|inconnu|doublons)| fait_hospitalisation)'
  job j32_fait_deces.py           '^GOLD(QC,(perte|inconnu|doublons)| fait_deces)'
  job j33_fait_satisfaction.py    '^GOLD(QC,(perte|inconnu|doublons)| fait_satisfaction)'
fi

if [ "$DEPUIS" -le 5 ]; then
  etape "5. Tests Silver et Gold"
  ./test_silver.sh | tail -3
  ./test_gold.sh   | tail -3
fi

if [ "${QUALITE:-0}" = "1" ]; then
  etape "6. Rapports qualité (docs/qualite/)"
  mkdir -p docs/qualite
  docker exec "$SPARK" /opt/spark/bin/spark-submit --master "$MASTER" /jobs/j40_qualite_s1.py 2>&1 \
    | grep '^QUALITE,' > docs/qualite/qualite_s1.csv
  docker exec "$SPARK" /opt/spark/bin/spark-submit --master "$MASTER" /jobs/j40_qualite_s2.py 2>&1 \
    | grep '^QUALITE,' > docs/qualite/qualite_s2.csv
  echo "[OK] qualite_s1.csv ($(wc -l < docs/qualite/qualite_s1.csv) lignes), qualite_s2.csv ($(wc -l < docs/qualite/qualite_s2.csv) lignes)"
fi

printf '\n[OK] Chaîne terminée en %d min %02d s\n' $(( ($(date +%s) - debut) / 60 )) $(( ($(date +%s) - debut) % 60 ))
