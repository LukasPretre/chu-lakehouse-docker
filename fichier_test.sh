#!/usr/bin/env bash
# Tests de l'infrastructure CHU : PostgreSQL, NiFi, Spark.
# Usage : ./fichier_test.sh        (depuis le dossier du docker-compose.yml)
#
# Les secrets ne sont pas écrits ici : ils sont lus dans le fichier .env
# (ignoré par Git) ou dans les variables d'environnement :
#   NIFI_USER, NIFI_PASSWORD, PG_USER, PG_PASSWORD
#
# Variables facultatives : NIFI_URL, SPARK_UI, SMOKE_JDBC=1 (test Spark -> PostgreSQL)

set -u

# ---------- CONFIG ----------
[ -f .env ] && { set -a; . ./.env; set +a; }

PG_CONTAINER=${PG_CONTAINER:-chu_postgres}
NIFI_CONTAINER=${NIFI_CONTAINER:-chu_nifi}
SPARK_MASTER_CONTAINER=${SPARK_MASTER_CONTAINER:-chu_spark_master}
SPARK_WORKER_CONTAINER=${SPARK_WORKER_CONTAINER:-chu_spark_worker}
NAMENODE_CONTAINER=${NAMENODE_CONTAINER:-chu_namenode}
DATANODE_CONTAINER=${DATANODE_CONTAINER:-chu_datanode}
HIVE_CONTAINER=${HIVE_CONTAINER:-chu_hive_metastore}

PG_USER=${PG_USER:-chu_admin}
PG_PASSWORD=${PG_PASSWORD:-}
PG_HOST=${PG_HOST:-postgres}
NIFI_USER=${NIFI_USER:-admin}
NIFI_PASSWORD=${NIFI_PASSWORD:-}
NIFI_URL=${NIFI_URL:-https://localhost:8443}
SPARK_UI=${SPARK_UI:-http://localhost:8080}
SPARK_MASTER_URL=${SPARK_MASTER_URL:-spark://spark-master:7077}

# ---------- OUTILS ----------
PASS=0; FAIL=0; SKIP=0
ok()   { echo "  [OK]    $1"; PASS=$((PASS+1)); }
ko()   { echo "  [ECHEC] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP]  $1"; SKIP=$((SKIP+1)); }

# check "description" commande [arguments...]
check() {
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$desc"; else ko "$desc"; fi
}

# wait_for secondes commande [arguments...]
wait_for() {
  local max=$1 t=0; shift
  until "$@" >/dev/null 2>&1; do
    t=$((t+2)); [ "$t" -ge "$max" ] && return 1
    sleep 2
  done
}

TMPFILES=()
cleanup() { for f in "${TMPFILES[@]:-}"; do [ -n "$f" ] && rm -f "$f"; done; }
trap cleanup EXIT

# ---------- FONCTIONS DE TEST ----------
running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]; }

pg_db_exists() {
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$1'" | grep -q 1
}

# Écrit et relit une table temporaire (rien ne reste dans la base)
pg_rw() {
  printf '%s\n' \
    "CREATE TEMP TABLE smoke_test(id int);" \
    "INSERT INTO smoke_test VALUES (42);" \
    "SELECT id FROM smoke_test;" \
  | docker exec -i "$PG_CONTAINER" psql -U "$PG_USER" -d "$1" -v ON_ERROR_STOP=1 -tA \
  | grep -q '^42$'
}

# Teste une connexion TCP depuis un conteneur vers un hôte:port
tcp_from() { docker exec "$1" bash -c "exec 3<>/dev/tcp/$2/$3"; }

nifi_ui() { [ "$(curl -ks -o /dev/null -w '%{http_code}' "$NIFI_URL/nifi/")" = "200" ]; }

NIFI_TOKEN=""
nifi_login() {
  NIFI_TOKEN=$(curl -ks -X POST "$NIFI_URL/nifi-api/access/token" \
    --data-urlencode "username=$NIFI_USER" --data-urlencode "password=$NIFI_PASSWORD")
  [ "${#NIFI_TOKEN}" -gt 100 ] && [[ "$NIFI_TOKEN" != *"<"* ]]
}
nifi_api() {
  curl -ks -H "Authorization: Bearer $NIFI_TOKEN" "$NIFI_URL/nifi-api/flow/about" | grep -q '"version"'
}

spark_workers() {
  local n
  n=$(curl -s "$SPARK_UI/json/" | grep -Eo '"aliveworkers"[^0-9]*[0-9]+' | grep -Eo '[0-9]+$')
  [ "${n:-0}" -ge 1 ]
}

# Calcul distribué fourni avec Spark (ne demande pas Python)
spark_pi() {
  local out
  out=$(timeout 240 docker exec "$SPARK_MASTER_CONTAINER" bash -c \
    "/opt/spark/bin/spark-submit --master $SPARK_MASTER_URL \
     --class org.apache.spark.examples.SparkPi \
     /opt/spark/examples/jars/spark-examples_*.jar 20" 2>&1)
  echo "$out" | grep -q "Pi is roughly"
}

# Envoie un script Python dans le conteneur Spark et l'exécute
run_pyspark() {   # run_pyspark fichier_local marqueur_attendu [options spark-submit]
  local f=$1 marker=$2; shift 2
  chmod 644 "$f"; docker cp "$f" "$SPARK_MASTER_CONTAINER:/tmp/smoke_job.py" || return 1
  timeout 300 docker exec -e PG_USER -e PG_PASSWORD "$SPARK_MASTER_CONTAINER" \
    /opt/spark/bin/spark-submit --master "$SPARK_MASTER_URL" "$@" /tmp/smoke_job.py 2>&1 \
  | grep -q "$marker"
}

pyspark_basic() {
  local f; f=$(mktemp --suffix=.py); TMPFILES+=("$f")
  cat > "$f" <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke_basic").getOrCreate()
df = spark.createDataFrame([("F", 1), ("M", 2), ("F", 3)], ["sexe", "n"])
res = {r["sexe"]: r["total"] for r in df.groupBy("sexe").sum("n").withColumnRenamed("sum(n)", "total").collect()}
assert res == {"F": 4, "M": 2}, res
print("PYSPARK_OK")
spark.stop()
EOF
  run_pyspark "$f" PYSPARK_OK
}

pyspark_jdbc() {
  local f; f=$(mktemp --suffix=.py); TMPFILES+=("$f")
  cat > "$f" <<EOF
import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke_jdbc").getOrCreate()
df = (spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://${PG_HOST}:5432/chu_s1_db")
      .option("query", "SELECT 1 AS ok")
      .option("user", os.environ["PG_USER"])
      .option("password", os.environ["PG_PASSWORD"])
      .option("driver", "org.postgresql.Driver")
      .load())
assert df.collect()[0]["ok"] == 1
print("JDBC_OK")
spark.stop()
EOF
  run_pyspark "$f" JDBC_OK --packages org.postgresql:postgresql:42.7.3 --conf spark.jars.ivy=/tmp/.ivy2
}

# ---------- FONCTIONS HDFS / HIVE ----------
hdfs_datanodes() { docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin -report 2>/dev/null | grep -Eq "Live datanodes \([1-9]"; }

hdfs_rw() {
  echo smoke_ok | docker exec -i "$NAMENODE_CONTAINER" hdfs dfs -put -f - /bronze/_smoke_test.txt >/dev/null 2>&1 || return 1
  local r; r=$(docker exec "$NAMENODE_CONTAINER" hdfs dfs -cat /bronze/_smoke_test.txt 2>/dev/null)
  docker exec "$NAMENODE_CONTAINER" hdfs dfs -rm -f /bronze/_smoke_test.txt >/dev/null 2>&1
  [ "$r" = "smoke_ok" ]
}

# Spark crée puis relit une table Hive (script jobs/hive_smoke.py monté dans /jobs)
hive_smoke() {
  timeout 300 docker exec "$SPARK_MASTER_CONTAINER" /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER_URL" /jobs/hive_smoke.py 2>&1 | grep -q "HIVE_SMOKE lignes = 10"
}

# Comptes Bronze = source (script jobs/check_bronze.py monté dans /jobs)
bronze_counts() {
  local out
  out=$(timeout 300 docker exec "$SPARK_MASTER_CONTAINER" /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER_URL" /jobs/check_bronze.py 2>&1 | grep "^BRONZE")
  [ -n "$out" ] && ! echo "$out" | grep -q -E "ECART|ABSENT"
}

# ---------- EXÉCUTION ----------
echo "== 1. Conteneurs =="
for c in "$PG_CONTAINER" "$NIFI_CONTAINER" "$SPARK_MASTER_CONTAINER" "$SPARK_WORKER_CONTAINER" \
         "$NAMENODE_CONTAINER" "$DATANODE_CONTAINER" "$HIVE_CONTAINER"; do
  check "conteneur $c démarré" running "$c"
done

echo "== 2. PostgreSQL =="
check "PostgreSQL accepte les connexions" docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER"
check "base chu_s1_db (source) créée" pg_db_exists chu_s1_db
check "base chu_gold_db (entrepôt) créée" pg_db_exists chu_gold_db
check "écriture et lecture dans chu_gold_db" pg_rw chu_gold_db

echo "== 3. NiFi (le démarrage peut durer 1 à 3 minutes) =="
check "interface NiFi accessible ($NIFI_URL)" wait_for 240 nifi_ui
if [ -n "$NIFI_PASSWORD" ]; then
  check "connexion à l'API NiFi (jeton)" nifi_login
  check "API NiFi répond (version)" nifi_api
else
  skip "connexion API NiFi : définir NIFI_PASSWORD dans .env"
fi
check "NiFi atteint PostgreSQL ($PG_HOST:5432)" tcp_from "$NIFI_CONTAINER" "$PG_HOST" 5432

echo "== 4. Spark =="
check "Spark Master voit au moins un worker" wait_for 60 spark_workers
check "calcul distribué SparkPi sur le cluster" spark_pi
check "Spark atteint PostgreSQL ($PG_HOST:5432)" tcp_from "$SPARK_MASTER_CONTAINER" "$PG_HOST" 5432
if docker exec "$SPARK_MASTER_CONTAINER" which python3 >/dev/null 2>&1; then
  check "PySpark : traitement simple sur le cluster" pyspark_basic
  if [ "${SMOKE_JDBC:-0}" = "1" ] && [ -n "$PG_PASSWORD" ]; then
    check "PySpark : lecture JDBC dans PostgreSQL" pyspark_jdbc
  else
    skip "lecture JDBC : lancer avec SMOKE_JDBC=1 et PG_PASSWORD dans .env (télécharge le driver)"
  fi
else
  skip "PySpark absent de l'image Spark (utiliser une image avec Python, par exemple le tag -python3)"
fi

echo "== 5. HDFS =="
check "HDFS : au moins un datanode actif" hdfs_datanodes
check "HDFS : écriture puis relecture d'un fichier" hdfs_rw
check "NiFi atteint le namenode (namenode:8020)" tcp_from "$NIFI_CONTAINER" namenode 8020
check "NiFi voit la configuration Hadoop" docker exec "$NIFI_CONTAINER" test -f /opt/nifi/hadoop-conf/core-site.xml
check "Spark atteint le namenode (namenode:8020)" tcp_from "$SPARK_MASTER_CONTAINER" namenode 8020
for t in Diagnostic Specialites Patient Professionnel_de_sante Consultation; do
  check "Bronze S1 présent dans HDFS : $t" docker exec "$NAMENODE_CONTAINER" hdfs dfs -test -d "/bronze/s1/$t"
done

echo "== 6. Hive Metastore =="
check "Spark atteint le metastore (hive-metastore:9083)" tcp_from "$SPARK_MASTER_CONTAINER" hive-metastore 9083
if [ "${SMOKE_HIVE:-0}" = "1" ]; then
  check "Hive : Spark crée et relit une table sur HDFS" hive_smoke
else
  skip "test Hive complet : lancer avec SMOKE_HIVE=1"
fi

echo "== 7. Qualité des données =="
if [ "${SMOKE_COUNTS:-0}" = "1" ]; then
  check "Bronze = source (5 tables, comptes identiques)" bronze_counts
else
  skip "comptes Bronze = source : lancer avec SMOKE_COUNTS=1"
fi

# Pour les prochains outils, ajouter ici un bloc de test :
#   Airflow  : curl http://localhost:8082/health
#   Superset : curl http://localhost:8088/health

echo
echo "Résultat : $PASS réussis, $FAIL en échec, $SKIP ignorés"
[ "$FAIL" -eq 0 ]
