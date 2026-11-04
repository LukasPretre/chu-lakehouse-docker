#!/usr/bin/env bash
# Installe et initialise Superset (rejouable). Lancé par bootstrap.sh, ou seul :
#   ./superset/init_superset.sh          (tout)
#   ./superset/init_superset.sh --env    (seulement compléter .env)
# 1. complète .env (clé secrète, administrateur, utilisateur PostgreSQL en lecture seule)
# 2. crée la base « superset » et le rôle superset_lecture (SELECT seulement sur chu_gold_db)
# 3. construit et démarre le conteneur chu_superset
# 4. migre la base, crée l'administrateur, enregistre la connexion « CHU Gold »
# Aucun secret n'est affiché.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { echo "ERREUR : .env absent, lancer d'abord ./bootstrap.sh"; exit 1; }

secret() { openssl rand -hex 24 2>/dev/null || python3 -c "import secrets; print(secrets.token_hex(24))"; }
ajouter_env() {  # ajoute VAR=valeur à .env si VAR n'y est pas
  grep -q "^$1=" .env || { printf '%s=%s\n' "$1" "$2" >> .env; echo "  .env : $1 ajoutée"; }
}
echo "== 1. Variables Superset dans .env"
ajouter_env SUPERSET_SECRET_KEY "$(secret)"
ajouter_env SUPERSET_ADMIN_USER "admin"
ajouter_env SUPERSET_ADMIN_PASSWORD "$(secret)"
ajouter_env SUPERSET_PG_RO_PASSWORD "$(secret)"
# --env : seulement compléter .env (appelé par bootstrap.sh avant le démarrage de la pile)
[ "${1:-}" = "--env" ] && exit 0
set -a; . ./.env; set +a

echo "== 2. Base « superset » et rôle en lecture seule"
docker exec chu_postgres sh -c \
  'psql -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='"'superset'"'" | grep -q 1 \
   || createdb -U "$POSTGRES_USER" superset'
docker exec -i -e PW="$SUPERSET_PG_RO_PASSWORD" chu_postgres sh -c \
  'psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d chu_gold_db -v pw="$PW" -v proprio="$POSTGRES_USER"' <<'SQL'
SELECT 'CREATE ROLE superset_lecture LOGIN' WHERE NOT EXISTS
  (SELECT 1 FROM pg_roles WHERE rolname = 'superset_lecture') \gexec
ALTER ROLE superset_lecture PASSWORD :'pw';
GRANT CONNECT ON DATABASE chu_gold_db TO superset_lecture;
GRANT USAGE ON SCHEMA public TO superset_lecture;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO superset_lecture;
-- les tables recréées plus tard par gold_ddl.sql seront lisibles aussi
ALTER DEFAULT PRIVILEGES FOR ROLE :"proprio" IN SCHEMA public GRANT SELECT ON TABLES TO superset_lecture;
SQL
echo "  rôle superset_lecture : lecture seule sur chu_gold_db"

echo "== 3. Construction et démarrage de chu_superset (la 1re fois : plusieurs minutes)"
docker compose up -d --build superset
for i in $(seq 1 60); do
  curl -fsS http://localhost:8088/health >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { echo "ERREUR : Superset ne répond pas, voir docker logs chu_superset"; exit 1; }
  sleep 5
done
echo "  Superset répond sur le port 8088"

echo "== 4. Initialisation"
docker exec chu_superset superset db upgrade >/dev/null 2>&1
docker exec chu_superset sh -c 'superset fab create-admin --username "$SUPERSET_ADMIN_USER" \
  --firstname Admin --lastname CHU --email admin@chu.local --password "$SUPERSET_ADMIN_PASSWORD"' >/dev/null 2>&1 || true
docker exec chu_superset superset init >/dev/null 2>&1
docker exec chu_superset sh -c 'superset set-database-uri --database_name "CHU Gold" \
  --uri "postgresql+psycopg2://superset_lecture:${SUPERSET_PG_RO_PASSWORD}@postgres:5432/chu_gold_db"' >/dev/null 2>&1 \
  && echo "  connexion « CHU Gold » enregistrée (lecture seule)" \
  || echo "  connexion à ajouter à la main : Paramètres > Connexions aux bases (voir README)"

echo "Superset prêt : http://${HOST_IP:-localhost}:8088 (identifiant : ${SUPERSET_ADMIN_USER}, mot de passe : variable SUPERSET_ADMIN_PASSWORD de .env)"
