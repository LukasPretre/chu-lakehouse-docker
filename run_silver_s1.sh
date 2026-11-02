#!/usr/bin/env bash
# Lance les jobs Silver de la source S1 dans l'ordre : J10 (patient),
# J11 (référentiels), J12 (consultation). Prérequis : les 5 tables sont en Bronze.
#
# Usage (racine du dépôt) : ./run_silver_s1.sh
# Les clés HMAC sont lues dans .env et transmises à Spark par l'environnement
# (jamais sur la ligne de commande).

set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || { echo "[ECHEC] .env introuvable (lancer ./bootstrap.sh)" >&2; exit 1; }
set -a; . ./.env; set +a
: "${HMAC_KEY_PATIENTS:?HMAC_KEY_PATIENTS absente de .env}"
: "${HMAC_KEY_PROFESSIONNELS:?HMAC_KEY_PROFESSIONNELS absente de .env}"

SPARK=chu_spark_master
MASTER=spark://spark-master:7077

for t in Diagnostic Specialites Patient Professionnel_de_sante Consultation; do
  docker exec chu_namenode hdfs dfs -test -d "/bronze/s1/$t" 2>/dev/null \
    || { echo "[ECHEC] Bronze manquant : $t (lancer d'abord les flux NiFi J01)" >&2; exit 1; }
done
echo "[OK] Bronze S1 présent dans HDFS"

lancer() {
  local job=$1
  echo "== $job"
  local out
  out=$(docker exec -e HMAC_KEY_PATIENTS -e HMAC_KEY_PROFESSIONNELS "$SPARK" \
        /opt/spark/bin/spark-submit --master "$MASTER" "/jobs/$job" 2>&1) || true
  if ! grep -E '^SILVER' <<<"$out"; then
    echo "[ECHEC] $job : aucune ligne SILVER, fin de la sortie :" >&2
    tail -25 <<<"$out" >&2
    exit 1
  fi
}

lancer j10_silver_patient.py
lancer j11_silver_referentiels.py
lancer j12_silver_consultation.py
echo "[OK] Silver S1 terminé. Rejets éventuels dans /silver/_rejets/ (HDFS)."
