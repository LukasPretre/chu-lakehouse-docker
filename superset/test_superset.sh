#!/usr/bin/env bash
# Tests de Superset. Deux usages :
#   ./superset/test_superset.sh            seul (CI GitHub ou à la main) : affiche son résultat
#   . ./superset/test_superset.sh          inclus dans fichier_test.sh : utilise ses compteurs
# Aucun secret n'est affiché.

if ! declare -F ok >/dev/null; then
  SEUL=1; PASS=0; FAIL=0; SKIP=0
  ok()   { echo "  [OK]    $1"; PASS=$((PASS+1)); }
  ko()   { echo "  [ECHEC] $1"; FAIL=$((FAIL+1)); }
  skip() { echo "  [SKIP]  $1"; SKIP=$((SKIP+1)); }
  cd "$(dirname "$0")/.." || exit 1
fi

_sup_env() { { grep "^$1=" .env 2>/dev/null || true; } | head -1 | cut -d= -f2-; }
_SUP_PGUSER=$(_sup_env PG_USER)
_SUP_RO=$(_sup_env SUPERSET_PG_RO_PASSWORD)
_sup_psql() {  # _sup_psql base requête  (en tant que propriétaire)
  docker exec chu_postgres psql -U "$_SUP_PGUSER" -d "$1" -tAc "$2" 2>/dev/null
}
_sup_ro() {    # _sup_ro base requête  (en tant que superset_lecture, par mot de passe)
  docker exec -e PGPASSWORD="$_SUP_RO" chu_postgres \
    psql -h localhost -U superset_lecture -d "$1" -v ON_ERROR_STOP=1 -tAc "$2" >/dev/null 2>&1
}

echo "== 8. Superset"
if [ "$(docker inspect -f '{{.State.Running}}' chu_superset 2>/dev/null)" != "true" ]; then
  skip "Superset non installé (./superset/init_superset.sh)"
else
  ok "conteneur chu_superset démarré"
  curl -fsS http://localhost:8088/health >/dev/null 2>&1 \
    && ok "Superset répond (http://localhost:8088/health)" || ko "Superset ne répond pas sur le port 8088"
  [ "$(_sup_psql postgres "SELECT 1 FROM pg_database WHERE datname='superset'")" = "1" ] \
    && ok "base de paramètres « superset » présente" || ko "base « superset » absente"
  [ "$(_sup_psql superset "SELECT 1 FROM dbs WHERE database_name='CHU Gold'")" = "1" ] \
    && ok "connexion « CHU Gold » enregistrée dans Superset" || ko "connexion « CHU Gold » absente"
  if [ -z "$_SUP_RO" ]; then
    ko "SUPERSET_PG_RO_PASSWORD absent de .env"
  else
    _sup_ro chu_gold_db "SELECT count(*) FROM dim_region" \
      && ok "superset_lecture lit le Gold (chu_gold_db)" || ko "superset_lecture ne lit pas chu_gold_db"
    _sup_ro chu_gold_db "DELETE FROM dim_region WHERE false" \
      && ko "superset_lecture peut modifier le Gold (il devrait être en lecture seule)" \
      || ok "superset_lecture ne peut pas modifier le Gold"
    _sup_ro chu_s1_db "SELECT 1 FROM public.\"Patient\" LIMIT 1" \
      && ko "superset_lecture lit la source S1 non pseudonymisée" \
      || ok "superset_lecture n'accède pas à la source S1 (RGPD)"
  fi
fi

if [ "${SEUL:-0}" = "1" ]; then
  echo
  echo "Résultat Superset : $PASS réussis, $FAIL en échec, $SKIP ignorés"
  [ "$FAIL" -eq 0 ]
fi
