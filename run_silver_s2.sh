#!/usr/bin/env bash
# Lance les jobs Silver de la source S2 (fichiers déposés) dans l'ordre :
#   J14 (établissements), J15 (activité des professionnels), puis J16 (hospitalisations)
#   dès que son fichier jobs/j16_*.py existe.
# Prérequis :
#   - Bronze S2 chargé par le flux NiFi J02 (CSV dans data/depot_s2/), comptes conformes ;
#   - Silver S1 professionnel (J11, ./run_silver_s1.sh) : J15 y rattache les professionnels.
#
# Usage (racine du dépôt) : ./run_silver_s2.sh
# Les clés HMAC sont lues dans .env et transmises à Spark par l'environnement
# (jamais sur la ligne de commande ni à l'écran).

set -euo pipefail
cd "$(dirname "$0")"

[ -f .env ] || { echo "[ECHEC] .env introuvable (lancer ./bootstrap.sh)" >&2; exit 1; }
set -a; . ./.env; set +a
: "${HMAC_KEY_PATIENTS:?HMAC_KEY_PATIENTS absente de .env}"
: "${HMAC_KEY_PROFESSIONNELS:?HMAC_KEY_PROFESSIONNELS absente de .env}"

SPARK=chu_spark_master
MASTER=spark://spark-master:7077

soumettre() {  # soumettre job.py : sortie complète du job, code retour conservé
  docker exec -e HMAC_KEY_PATIENTS -e HMAC_KEY_PROFESSIONNELS "$SPARK" \
    /opt/spark/bin/spark-submit --master "$MASTER" "/jobs/$1" 2>&1
}

# 1. Prérequis : Silver S1 professionnel
docker exec chu_namenode hdfs dfs -test -d /silver/professionnel 2>/dev/null \
  || { echo "[ECHEC] Silver S1 professionnel absent (lancer d'abord ./run_silver_s1.sh)" >&2; exit 1; }
echo "[OK] Silver S1 professionnel présent"

# 2. Prérequis : Bronze S2 complet et sans doublon (jobs/check_bronze_s2.py)
echo "== contrôle du Bronze S2"
rc=0; out=$(soumettre check_bronze_s2.py) || rc=$?
controle=$(grep -E '^BRONZE S2' <<<"$out" || true)
if [ -z "$controle" ]; then
  echo "[ECHEC] check_bronze_s2.py n'a rien produit (code $rc), fin de la sortie :" >&2
  tail -25 <<<"$out" >&2; exit 1
fi
echo "$controle"
if grep -q -E 'ECART|ABSENT' <<<"$controle"; then
  echo "[ECHEC] Bronze S2 incomplet ou en double : vérifier le flux NiFi J02 et data/depot_s2/" >&2
  exit 1
fi
echo "[OK] Bronze S2 conforme"

# 3. Jobs Silver S2
# duree job debut : affiche la durée du job ; au-delà de 2 minutes (objectif du projet), le signale
duree() {
  local s=$(( $(date +%s) - $2 ))
  if [ "$s" -gt 120 ]; then echo "[ATTENTION] $1 : ${s} s (objectif : 2 min par job)"
  else echo "[..] $1 : ${s} s"; fi
}

lancer() {
  local job=$1 rc=0 out t0
  t0=$(date +%s)
  echo "== $job"
  out=$(soumettre "$job") || rc=$?
  grep -E '^SILVER' <<<"$out" || true
  if [ "$rc" -ne 0 ] || ! grep -q -E '^SILVER' <<<"$out" || grep -q -E '^SILVER ERREUR' <<<"$out"; then
    echo "[ECHEC] $job (code $rc), fin de la sortie :" >&2
    tail -25 <<<"$out" >&2
    exit 1
  fi
  duree "$job" "$t0"
}

lancer j14_silver_etablissement.py           # J14 avant J15 : J15 lit /silver/etablissement
lancer j15_silver_activite_professionnel.py

j16=$(ls jobs/j16_*.py 2>/dev/null | head -1 || true)
if [ -n "$j16" ]; then
  lancer "$(basename "$j16")"
else
  echo "[..] J16 (hospitalisations) pas encore dans jobs/ : ignoré"
fi

echo "[OK] Silver S2 terminé. Rejets éventuels dans /silver/_rejets/ (HDFS). Contrôle : ./test_silver.sh"
