#!/usr/bin/env bash
# Chaîne complète du lakehouse, dans l'ordre, puis contrôles :
#   1. Silver S1 (J10 à J12)            ./run_silver_s1.sh
#   2. Silver S2 (J14 à J16)            ./run_silver_s2.sh   (ignoré si le Bronze S2 est absent)
#      puis Silver S3 e-Satis (J17) et S4 décès (J18), chacun ignoré si son Bronze est absent
#      (J18 : environ 6 min pour 25 millions de lignes, au-dessus de l'objectif de 2 min)
#   3. Référentiel géographique (J05)
#   4. Dimensions du Gold (J20, J21, J22, J25, J23, J24), faits (J30 à J33), copie vers PostgreSQL (J34)
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

# Jobs qui lisent ou écrivent PostgreSQL : pilote JDBC copié dans le conteneur, identifiants
# PostgreSQL passés par l'environnement (jamais sur la ligne de commande).
JOBS_PG=" j21_dim_region.py j22_dim_etablissement.py j34_copie_gold_pg.py "
PILOTE_JDBC=drivers/postgresql-42.7.3.jar
# Job qui pseudonymise avec la clé HMAC des patients (transmise par l'environnement).
JOBS_CLE=" j18_silver_deces.py "

job() {
  local fichier=$1 motif=$2 rc=0 out t0
  t0=$(date +%s)
  echo "== $fichier"
  if [[ $JOBS_PG == *" $fichier "* ]]; then
    docker cp "$PILOTE_JDBC" "$SPARK:/tmp/postgresql.jar" >/dev/null || echec "pilote JDBC $PILOTE_JDBC introuvable (./bootstrap.sh)"
    out=$(docker exec -e PG_USER -e PG_PASSWORD "$SPARK" /opt/spark/bin/spark-submit --master "$MASTER" \
          --jars /tmp/postgresql.jar "/jobs/$fichier" 2>&1) || rc=$?
  elif [[ $JOBS_CLE == *" $fichier "* ]]; then
    [ -n "${HMAC_KEY_PATIENTS:-}" ] || echec "HMAC_KEY_PATIENTS absente de .env"
    out=$(docker exec -e HMAC_KEY_PATIENTS "$SPARK" /opt/spark/bin/spark-submit --master "$MASTER" "/jobs/$fichier" 2>&1) || rc=$?
  else
    out=$(docker exec "$SPARK" /opt/spark/bin/spark-submit --master "$MASTER" "/jobs/$fichier" 2>&1) || rc=$?
  fi
  grep -E "$motif" <<<"$out" | grep -v -E '^GOLDQC,lignes_annee_' || true
  if [ "$rc" -ne 0 ] || ! grep -q -E "$motif" <<<"$out" || grep -q -E 'ERREUR|ECART' <<<"$out"; then
    echo "[ECHEC] $fichier (code $rc), fin de la sortie :" >&2
    tail -20 <<<"$out" >&2
    exit 1
  fi
  duree "$fichier" "$t0"
}

[ -f .env ] || echec ".env introuvable (lancer ./bootstrap.sh)"
set -a; . ./.env; set +a   # PG_USER et PG_PASSWORD pour J21, J22 et J34
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

if [ "$DEPUIS" -le 2 ]; then
  etape "2b. Silver S3 e-Satis (J17) et S4 décès (J18)"
  if docker exec chu_namenode hdfs dfs -test -d /bronze/s3/esatis 2>/dev/null; then
    job j17_silver_esatis.py '^SILVER esatis bronze='
  else
    echo "[..] Bronze S3 absent (NiFi J03 pas encore lancé) : J17 ignoré"
  fi
  if docker exec chu_namenode hdfs dfs -test -d /bronze/s4/deces 2>/dev/null; then
    job j18_silver_deces.py '^SILVER deces bronze='
  else
    echo "[..] Bronze S4 absent (NiFi J04 pas encore lancé) : J18 ignoré"
  fi
fi

if [ "$DEPUIS" -le 3 ]; then
  etape "3. Référentiel département -> région (J05)"
  job j05_ref_departement.py '^(SILVER|J05QC,test_)'
fi

if [ "$DEPUIS" -le 4 ]; then
  etape "4. Gold : dimensions, faits, copie vers PostgreSQL"
  job j20_dimensions_generees.py '^GOLD dim_temps table='
  job j21_dim_region.py          '^GOLD dim_region table='
  job j22_dim_etablissement.py   '^GOLD dim_etablissement (table=|ignoré)'
  job j25_dim_patient.py         '^GOLD dim_patient (table=|cles_modifiees)'
  job j23_dim_professionnel.py   '^GOLD dim_professionnel (table=|cles_modifiees)'
  job j24_dim_diagnostic.py      '^GOLD dim_diagnostic (table=|cles_modifiees)'
  job j30_fait_consultation.py   '^GOLD(QC,(perte|inconnu|doublons)| fait_consultation)'
  job j31_fait_hospitalisation.py '^GOLD(QC,(perte|inconnu|doublons)| fait_hospitalisation)'
  job j32_fait_deces.py           '^GOLD(QC,(perte|inconnu|doublons)| fait_deces)'
  job j33_fait_satisfaction.py    '^GOLD(QC,(perte|inconnu|doublons)| fait_satisfaction)'
  job j34_copie_gold_pg.py       '^GOLD J34 '
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