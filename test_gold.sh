#!/usr/bin/env bash
# Tests de la couche Gold : PostgreSQL chu_gold_db (structure et contraintes) et tables Hive.
#
# Usage (racine du dépôt) :
#   ./test_gold.sh                    contrôles seuls, ne modifie rien
#   LANCER_JOBS=1 ./test_gold.sh      lance d'abord les jobs Gold présents dans jobs/ (liste JOBS_GOLD)
#   APPLIQUER_DDL=1 ./test_gold.sh    rejoue d'abord sql/gold_ddl.sql : EFFACE le Gold PostgreSQL
# Options :
#   STRICT=1        une table Gold absente compte comme un échec
#   COMPARER_PG=1   compare les volumes Hive et PostgreSQL (QC10, une fois J34 en place)
#   GOLD_DB=...     base Hive du Gold (défaut chu_gold)
#
# Les contrôles de contraintes PostgreSQL tournent dans des transactions annulées : rien n'est écrit.
# Code de retour : 0 si tous les contrôles passent, 1 sinon (utilisé par la CI).

set -u
cd "$(dirname "$0")" || exit 1
[ -f .env ] && { set -a; . ./.env; set +a; }

PG_CONTAINER=${PG_CONTAINER:-chu_postgres}
PG_USER=${PG_USER:-chu_admin}
PG_DB=${PG_DB:-chu_gold_db}
SPARK=${SPARK_MASTER_CONTAINER:-chu_spark_master}
MASTER=${SPARK_MASTER_URL:-spark://spark-master:7077}
GOLD_DB=${GOLD_DB:-chu_gold}

# Jobs Gold lancés avec LANCER_JOBS=1, dans l'ordre. Ajouter les suivants ici (J23, J24, J25...).
JOBS_GOLD=(j20_dimensions_generees.py j25_dim_patient.py j23_dim_professionnel.py j24_dim_diagnostic.py j30_fait_consultation.py j31_fait_hospitalisation.py j33_fait_satisfaction.py)

TABLES="dim_temps dim_region dim_etablissement dim_patient dim_professionnel dim_diagnostic
        fait_consultation fait_hospitalisation fait_deces fait_satisfaction"

# ---------- OUTILS ----------
PASS=0; FAIL=0; SKIP=0
ok()   { echo "  [OK]    $1"; PASS=$((PASS+1)); }
ko()   { echo "  [ECHEC] $1"; FAIL=$((FAIL+1)); }
skip() { echo "  [SKIP]  $1"; SKIP=$((SKIP+1)); }

psql_gold() {
  docker exec -i -e PGOPTIONS="-c client_min_messages=warning" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d "$PG_DB" -X -q -v ON_ERROR_STOP=1 "$@"
}
valeur() { psql_gold -tAc "$1" 2>/dev/null; }

# egal "description" attendu "requête"
egal() {
  local obtenu; obtenu=$(valeur "$3")
  if [ "$obtenu" = "$2" ]; then ok "$1"; else ko "$1 (obtenu : ${obtenu:-erreur}, attendu : $2)"; fi
}
# accepte "description" "sql" : doit passer (dans une transaction annulée)
accepte() {
  local err
  if err=$(printf 'BEGIN;\n%s\nROLLBACK;\n' "$2" | psql_gold 2>&1 >/dev/null); then ok "$1"
  else ko "$1 : refusé à tort ($(grep -m1 ERROR <<<"$err"))"; fi
}
# refuse "description" contrainte "sql" : doit échouer, et sur cette contrainte-là
refuse() {
  local err
  if err=$(printf 'BEGIN;\n%s\nROLLBACK;\n' "$3" | psql_gold 2>&1 >/dev/null); then ko "$1 : accepté à tort"
  elif grep -q "\"$2\"" <<<"$err"; then ok "$1"
  else ko "$1 : refusé pour une autre raison ($(grep -m1 ERROR <<<"$err"))"; fi
}

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

metastore_ok() { docker exec "$SPARK" bash -c "exec 3<>/dev/tcp/hive-metastore/9083" >/dev/null 2>&1; }

liste_sql() { printf "'%s'," $TABLES | sed 's/,$//'; }

# ---------- 0. PRÉPARATION ----------
if [ "${APPLIQUER_DDL:-0}" = "1" ]; then
  echo "== 0. Application de sql/gold_ddl.sql (le Gold PostgreSQL est recréé)"
  if err=$(psql_gold < sql/gold_ddl.sql 2>&1); then ok "sql/gold_ddl.sql appliqué"
  else ko "sql/gold_ddl.sql en erreur : $(grep -m1 ERROR <<<"$err")"; fi
fi

# ---------- 1. STRUCTURE POSTGRESQL ----------
echo "== 1. PostgreSQL $PG_DB : structure"
egal "base $PG_DB accessible" 1 "SELECT 1"
egal "les 10 tables du modèle sont présentes" 10 \
  "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ($(liste_sql))"
egal "15 clés étrangères (les 15 liens du MCD)" 15 \
  "SELECT count(*) FROM information_schema.table_constraints WHERE table_schema = 'public' AND constraint_type = 'FOREIGN KEY'"
for d in dim_temps:id_date dim_region:id_region dim_etablissement:id_etablissement \
         dim_patient:id_patient dim_professionnel:id_professionnel dim_diagnostic:id_diagnostic; do
  egal "ligne -1 « Inconnu » dans ${d%%:*}" 1 "SELECT count(*) FROM ${d%%:*} WHERE ${d#*:} = -1"
done
egal "dim_region : 18 régions + Étranger + Inconnu" 20 "SELECT count(*) FROM dim_region"

# ---------- 2. CONTRAINTES POSTGRESQL ----------
echo "== 2. PostgreSQL $PG_DB : contraintes (transactions annulées, rien n'est écrit)"
C="INSERT INTO fait_consultation (id_consultation, id_date, id_patient, id_professionnel, id_diagnostic, id_etablissement)"
T="INSERT INTO dim_temps (id_date, date_complete, jour, jour_semaine, semaine_iso, mois, libelle_mois, trimestre, annee, est_ferie)"
E="INSERT INTO dim_etablissement (id_etablissement, identifiant_organisation, raison_sociale, code_postal, id_region, date_debut"
accepte "une consultation rattachée aux lignes -1 est acceptée" "$C VALUES (-999999, -1, -1, -1, -1, -1);"
refuse  "un patient inexistant est refusé (clé étrangère)" fait_consultation_id_patient_fkey \
        "$C VALUES (-999999, -1, -999999, -1, -1, -1);"
refuse  "un id_date différent de la date est refusé" ck_temps_cle \
        "$T VALUES (21000102, DATE '2100-01-01', 1, 'Vendredi', 53, 1, 'Janvier', 1, 2100, TRUE);"
refuse  "un sexe hors F, M, I est refusé" ck_patient_sexe \
        "INSERT INTO dim_patient (id_patient, patient_pseudo, sexe) VALUES (-999999, repeat('x', 64), 'X');"
refuse  "SCD2 : deux versions courantes du même établissement sont refusées" uq_etablissement_courant \
        "$E) VALUES (-999999, 'TEST_GOLD', 'Test', '00000', -1, DATE '2000-01-01');
         $E) VALUES (-999998, 'TEST_GOLD', 'Test', '00000', -1, DATE '2010-01-01');"
refuse  "SCD2 : une version fermée ne peut pas rester courante" ck_etablissement_courant \
        "$E, date_fin, est_courant) VALUES (-999999, 'TEST_GOLD', 'Test', '00000', -1, DATE '2000-01-01', DATE '2009-12-31', TRUE);"
refuse  "un séjour qui sort avant d'entrer est refusé" ck_hospitalisation_dates \
        "$T VALUES (21000101, DATE '2100-01-01', 1, 'Vendredi', 53, 1, 'Janvier', 1, 2100, FALSE),
                   (21000102, DATE '2100-01-02', 2, 'Samedi', 53, 1, 'Janvier', 1, 2100, FALSE);
         INSERT INTO fait_hospitalisation (id_hospitalisation, id_date_entree, id_date_sortie, id_patient, id_etablissement, id_diagnostic)
         VALUES ('TEST_GOLD', 21000102, 21000101, -1, -1, -1);"
refuse  "un score de satisfaction hors 0-100 est refusé" ck_satisfaction_scores \
        "INSERT INTO fait_satisfaction (id_satisfaction, id_date, id_etablissement, type_enquete, score_global) VALUES (-999999, -1, -1, '48H', 150);"
accepte "deux satisfactions d'établissements inconnus (-1) la même année sont acceptées" \
        "INSERT INTO fait_satisfaction (id_satisfaction, id_date, id_etablissement, type_enquete) VALUES (-999999, -1, -1, '48H'), (-999998, -1, -1, '48H');"

# ---------- 3. JOBS GOLD ----------
echo "== 3. Hive : Metastore et jobs Gold"
t=0; until metastore_ok || [ "$t" -ge 120 ]; do sleep 3; t=$((t+3)); done
if metastore_ok; then ok "Metastore Hive joignable (hive-metastore:9083)"; else ko "Metastore Hive injoignable après 120 s"; fi
if [ "${LANCER_JOBS:-0}" = "1" ]; then
  for job in "${JOBS_GOLD[@]}"; do
    if [ ! -f "jobs/$job" ]; then skip "jobs/$job absent de cette branche"; continue; fi
    sortie=$(docker exec -e GOLD_DB="$GOLD_DB" "$SPARK" \
             /opt/spark/bin/spark-submit --master "$MASTER" "/jobs/$job" 2>&1)
    rc=$?
    if [ "$rc" -eq 0 ]; then ok "$job terminé"
    else ko "$job en échec (code $rc), fin de la sortie :"; grep -E '^GOLD|Error|Exception' <<<"$sortie" | tail -15 | sed 's/^/          /'; fi
  done
else
  skip "jobs Gold non relancés (LANCER_JOBS=1 pour les lancer)"
fi

# ---------- 4. CONTENU DU GOLD HIVE ----------
echo "== 4. Hive $GOLD_DB : contenu du Gold (jobs/check_gold.py)"
colonnes=$(valeur "SELECT string_agg(table_name || '.' || column_name || '.' ||
                   CASE WHEN is_nullable = 'NO' AND column_default IS NULL AND is_identity = 'NO' THEN '1' ELSE '0' END,
                   ' ' ORDER BY table_name, ordinal_position)
                   FROM information_schema.columns WHERE table_schema = 'public' AND table_name IN ($(liste_sql))")
comptes=$(valeur "SELECT concat_ws(' ', $(for t in $TABLES; do printf "'%s:' || (SELECT count(*) FROM %s)," "$t" "$t"; done | sed 's/,$//'))")
sortie=$(docker exec -e GOLD_DB="$GOLD_DB" -e STRICT="${STRICT:-0}" -e COMPARER_PG="${COMPARER_PG:-0}" \
         -e PG_COLONNES="$colonnes" -e PG_COMPTES="$comptes" "$SPARK" \
         /opt/spark/bin/spark-submit --master "$MASTER" /jobs/check_gold.py 2>&1)
resume GOLDTEST "$sortie" $?

echo
echo "Résultat Gold : $PASS réussis, $FAIL en échec, $SKIP ignorés"
[ "$FAIL" -eq 0 ]