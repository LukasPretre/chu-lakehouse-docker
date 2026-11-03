#!/usr/bin/env bash
# Montage de la pile CHU Lakehouse sur une VM Ubuntu vide.
#
# Usage (depuis la racine du dépôt cloné) :
#   ./bootstrap.sh                       monte la pile (sans les données sources)
#   ./bootstrap.sh /chemin/vers/DATA2023 monte la pile et restaure le dump PostgreSQL
#
# Options (variables d'environnement) :
#   FORMAT_HDFS=1 ./bootstrap.sh   force le formatage du namenode.
#                                  ATTENTION : efface tout le contenu de HDFS.
#
# Le script peut être relancé sans risque : chaque étape vérifie d'abord son état.
# Les secrets sont écrits dans .env (ignoré par Git), jamais affichés.

set -euo pipefail
cd "$(dirname "$0")"

DUMP="${1:-}"
JDBC_VERSION=42.7.3
JDBC_JAR="drivers/postgresql-${JDBC_VERSION}.jar"
JDBC_URL="https://repo1.maven.org/maven2/org/postgresql/postgresql/${JDBC_VERSION}/postgresql-${JDBC_VERSION}.jar"
PG_CONTAINER=chu_postgres
NN_CONTAINER=chu_namenode

etape() { printf '\n== %s\n' "$1"; }
ok()    { printf '  [OK]        %s\n' "$1"; }
info()  { printf '  [..]        %s\n' "$1"; }
avert() { printf '  [ATTENTION] %s\n' "$1"; }
fail()  { printf '  [ECHEC]     %s\n' "$1" >&2; exit 1; }

# attendre "description" secondes commande [arguments...]
attendre() {
  local desc=$1 max=$2 t=0; shift 2
  until "$@" >/dev/null 2>&1; do
    t=$((t + 3))
    [ "$t" -ge "$max" ] && fail "délai dépassé : $desc"
    sleep 3
  done
  ok "$desc"
}

hdfs_ok() {
  local r
  r=$(docker exec "$NN_CONTAINER" hdfs dfsadmin -report 2>/dev/null) || return 1
  grep -Eq 'Live datanodes \([1-9]' <<<"$r"
}

# ---------------------------------------------------------------- 0. Prérequis
etape "0. Prérequis"
command -v docker  >/dev/null || fail "Docker n'est pas installé"
docker info >/dev/null 2>&1   || fail "Docker inaccessible : sudo usermod -aG docker \$USER, puis se reconnecter"
docker compose version >/dev/null 2>&1 || fail "le plugin « docker compose » est absent"
command -v curl    >/dev/null || fail "curl est absent (sudo apt install curl)"
command -v openssl >/dev/null || fail "openssl est absent (sudo apt install openssl)"
[ -f docker-compose.yml ]     || fail "docker-compose.yml introuvable : lancer le script depuis la racine du dépôt"

mem_mo=$(awk '/MemTotal/ {print int($2 / 1024)}' /proc/meminfo)
if [ "$mem_mo" -ge 7000 ]; then ok "RAM : ${mem_mo} Mo"
else avert "RAM : ${mem_mo} Mo seulement (8 Go conseillés), la pile risque de manquer de mémoire"; fi

disque_go=$(df -BG --output=avail . | tail -1 | tr -dc '0-9')
if [ "${disque_go:-0}" -ge 15 ]; then ok "disque libre : ${disque_go} Go"
else avert "disque libre : ${disque_go:-?} Go seulement (15 Go conseillés)"; fi

# ---------------------------------------------------------------- 1. .env
etape "1. Fichier .env (secrets, jamais dans Git)"
if [ ! -f .env ]; then
  if [ -f .env.example ]; then cp .env.example .env; else : > .env; fi
  info ".env créé"
fi
chmod 600 .env

# fixer NOM VALEUR : n'écrit la variable que si elle est absente ou vide
fixer() {
  if grep -q "^$1=." .env; then return 0; fi
  sed -i "/^$1=/d" .env
  if [ -s .env ] && [ -n "$(tail -c1 .env)" ]; then echo >> .env; fi
  echo "$1=$2" >> .env
  info "$1 généré"
}

ip_vm=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i = 1; i <= NF; i++) if ($i == "src") {print $(i + 1); exit}}' || true)
[ -n "$ip_vm" ] || ip_vm=$(hostname -I | awk '{print $1}')

fixer PG_USER chu_admin
fixer PG_PASSWORD "$(openssl rand -hex 16)"
fixer NIFI_USER admin
fixer NIFI_PASSWORD "$(openssl rand -hex 16)"
fixer HOST_IP "$ip_vm"
fixer HMAC_KEY_PATIENTS "$(openssl rand -hex 32)"
fixer HMAC_KEY_PROFESSIONNELS "$(openssl rand -hex 32)"

set -a; . ./.env; set +a
if [ "$HOST_IP" != "$ip_vm" ]; then
  avert "HOST_IP=$HOST_IP dans .env, mais l'IP actuelle de la VM est $ip_vm (à corriger si l'adresse NiFi est refusée)"
else
  ok "HOST_IP = $HOST_IP"
fi
ok ".env complet (les mots de passe sont dans .env, à ne jamais commiter)"

# ---------------------------------------------------------------- 2. Dossiers et driver
etape "2. Dossiers et driver JDBC"
mkdir -p data drivers jobs hadoop-conf nifi-flows
chmod 777 data 2>/dev/null || avert "chmod 777 data impossible (dossier créé par root ? sudo chown -R \$USER data)"
if [ ! -s "$JDBC_JAR" ]; then
  info "téléchargement du driver PostgreSQL ${JDBC_VERSION}"
  curl -fL --retry 3 -o "$JDBC_JAR" "$JDBC_URL" >/dev/null 2>&1 || fail "téléchargement du driver impossible ($JDBC_URL)"
fi
ok "$JDBC_JAR"
[ -f hadoop-conf/core-site.xml ] || fail "hadoop-conf/core-site.xml absent du dépôt (faire un git pull)"
ok "configuration Hadoop présente"

# ---------------------------------------------------------------- 3. HDFS
etape "3. HDFS : formatage du namenode (première fois seulement)"
if [ "${FORMAT_HDFS:-0}" = "1" ]; then
  avert "FORMAT_HDFS=1 : le contenu de HDFS va être effacé"
  docker compose rm -sf namenode datanode >/dev/null 2>&1 || true
  docker volume ls -q | grep -E '_(namenode|datanode)_data$' | xargs -r docker volume rm >/dev/null || true
fi
if docker volume ls -q | grep -q '_namenode_data$'; then
  ok "volume namenode déjà présent : formatage ignoré"
else
  info "formatage du namenode"
  out=$(docker compose run --rm --no-deps namenode hdfs namenode -format -force -nonInteractive 2>&1) \
    || { echo "$out" | tail -15; fail "formatage du namenode impossible"; }
  ok "namenode formaté"
fi

# ---------------------------------------------------------------- 4. PostgreSQL
etape "4. PostgreSQL et base du metastore Hive"
docker compose up -d postgres >/dev/null 2>&1 || fail "docker compose up postgres a échoué"
attendre "PostgreSQL prêt" 120 docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER"

existe_base() {
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$1'" 2>/dev/null
}
for b in chu_s1_db chu_gold_db; do
  if [ "$(existe_base "$b")" = "1" ]; then ok "base $b présente"
  else avert "base $b absente (le dossier postgres-init/ est-il monté ? volume déjà créé avant ?)"; fi
done
if [ "$(existe_base metastore)" = "1" ]; then
  ok "base metastore présente"
else
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -c "CREATE DATABASE metastore;" >/dev/null \
    || fail "création de la base metastore impossible"
  ok "base metastore créée"
fi

# ---------------------------------------------------------------- 5. Pile complète
etape "5. Démarrage de toute la pile"
# Le Metastore Hive initialise son schéma à chaque démarrage sauf si IS_RESUME=true
# (docker-compose.yml : IS_RESUME: "${HIVE_IS_RESUME:-true}"). Première fois : false,
# puis on le recrée avec true, sinon il échoue au premier redémarrage du conteneur.
schema_hive_present() {
  [ "$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d metastore -tAc \
      "SELECT 1 FROM information_schema.tables WHERE table_name='BUCKETING_COLS'" 2>/dev/null)" = "1" ]
}
INIT_HIVE=0
if schema_hive_present; then
  export HIVE_IS_RESUME=true
  ok "schéma du Metastore déjà présent"
else
  export HIVE_IS_RESUME=false
  INIT_HIVE=1
  info "schéma du Metastore à créer au premier démarrage"
fi
docker compose up -d >/dev/null 2>&1 || fail "docker compose up -d a échoué (voir : docker compose config)"
ok "conteneurs lancés"
if [ "$INIT_HIVE" = "1" ]; then
  attendre "schéma du Metastore créé" 240 schema_hive_present
  export HIVE_IS_RESUME=true
  docker compose up -d hive-metastore >/dev/null 2>&1 || fail "relance du Metastore impossible"
  ok "Metastore relancé en mode normal (IS_RESUME=true)"
fi
attendre "datanode HDFS enregistré" 180 hdfs_ok
timeout 120 docker exec "$NN_CONTAINER" hdfs dfsadmin -safemode wait >/dev/null 2>&1 || avert "namenode encore en mode sécurisé"
docker exec "$NN_CONTAINER" hdfs dfs -mkdir -p /bronze /silver /warehouse >/dev/null
docker exec "$NN_CONTAINER" hdfs dfs -chmod -R 777 /bronze /silver /warehouse >/dev/null
ok "dossiers HDFS /bronze /silver /warehouse"

# ---------------------------------------------------------------- 6. Données sources
etape "6. Données sources (dump PostgreSQL)"
nb=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d chu_s1_db -tAc 'SELECT count(*) FROM public."Consultation"' 2>/dev/null || true)
if [[ "${nb:-}" =~ ^[0-9]+$ ]] && [ "$nb" -gt 0 ]; then
  ok "source déjà restaurée (Consultation : $nb lignes)"
elif [ -n "$DUMP" ]; then
  [ -f "$DUMP" ] || fail "dump introuvable : $DUMP"
  info "restauration de $DUMP (1 à 3 minutes)"
  docker cp "$DUMP" "$PG_CONTAINER:/tmp/DATA2023" >/dev/null
  docker exec "$PG_CONTAINER" sh -c 'pg_restore -U "$POSTGRES_USER" -d chu_s1_db --no-owner --no-privileges /tmp/DATA2023' >/dev/null 2>&1 || true
  docker exec "$PG_CONTAINER" rm -f /tmp/DATA2023
  nb=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d chu_s1_db -tAc 'SELECT count(*) FROM public."Consultation"' 2>/dev/null || true)
  [[ "${nb:-}" =~ ^[0-9]+$ ]] && [ "$nb" -gt 0 ] || fail "restauration non confirmée (table Consultation vide ou absente)"
  ok "source restaurée (Consultation : $nb lignes ; attendu 1027157)"
else
  avert "dump non fourni : relancer avec ./bootstrap.sh /chemin/vers/DATA2023 (à demander à l'équipe, jamais dans Git)"
fi

# ---------------------------------------------------------------- 7. Suite
etape "7. Terminé. Suite à faire à la main"
cat <<EOF
  Interfaces (remplacer par l'IP de la VM si besoin) :
    NiFi        https://${HOST_IP}:8443   (utilisateur : ${NIFI_USER}, mot de passe dans .env ; démarrage : 1 à 3 minutes)
    HDFS        http://${HOST_IP}:9870
    Spark       http://${HOST_IP}:8080

  1. NiFi : importer nifi-flows/J01.json (icône « Process Group », puis Upload),
     ouvrir le service DBCPConnectionPool, ressaisir PG_PASSWORD, l'activer,
     puis faire « Run Once » sur chaque GenerateFlowFile.
  2. Silver de la source S1 : ./run_silver_s1.sh
  3. Contrôle de la pile : SMOKE_COUNTS=1 ./fichier_test.sh
EOF
