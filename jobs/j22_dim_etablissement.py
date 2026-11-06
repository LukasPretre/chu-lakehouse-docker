"""
J22 - Gold : DIM_ETABLISSEMENT, historisée en SCD2 dans PostgreSQL, puis copiée dans Hive.

Source : silver.etablissement (J14), une ligne par identifiant_organisation.
1. Spark nettoie le Silver et l'écrit dans la table de travail etl.stg_etablissement (JDBC).
2. Le SCD2 est appliqué en SQL dans PostgreSQL, en une seule transaction :
   - établissement inconnu jusqu'ici : première version, valable depuis le 01/01/1900
     (pour que les faits anciens la retrouvent) ;
   - attribut suivi modifié : la version courante est fermée la veille de DATE_EFFET et une
     nouvelle version commence à DATE_EFFET (si la version courante a commencé à DATE_EFFET,
     elle est corrigée sur place : pas de version d'un jour) ;
   - établissement disparu du Silver : version courante fermée ; réapparu : nouvelle version ;
   - rien de changé : aucune écriture.
   Contrôles avant validation : versions courantes = lignes du Silver, une seule version courante
   par établissement, ligne -1 présente, versions courantes identiques au Silver. Sinon : annulation.
3. public.dim_etablissement est recopiée dans <GOLD_DB>.dim_etablissement, avec les mêmes clés.

Règles de colonnes (sql/gold_ddl.sql, Livrable 1 annexe A) :
  - finess_site = FINESS géographique, pris dans finess_geo du Silver (la colonne finess_site du
    fichier source est inutilisable) : c'est la clé de rattachement d'e-Satis (J17) ;
  - FINESS, SIRET, SIREN hors format -> NULL, compté en anomalie (QC05) ;
  - raison_sociale, code_postal, categorie vides -> « Non renseigné » ;
  - id_region = code_region_insee en entier, -1 si ZZ ou inconnu de dim_region.
Faits : version valable à la date du fait = identifiant_organisation identique et
date du fait entre date_debut et date_fin.

Variables facultatives : DATE_EFFET (AAAA-MM-JJ, défaut : aujourd'hui), GOLD_DB, RUN_ID.
Lancement (le pilote JDBC et les identifiants sont passés au job) :
  docker cp drivers/postgresql-42.7.3.jar chu_spark_master:/tmp/postgresql.jar
  docker exec -e PG_USER -e PG_PASSWORD chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --jars /tmp/postgresql.jar /jobs/j22_dim_etablissement.py
(./run_pipeline.sh et LANCER_JOBS=1 ./test_gold.sh le font tout seuls.)
"""
import os
import sys
from datetime import date, datetime, timezone

from pyspark.sql import functions as F

sys.path.insert(0, "/jobs")
import gold_commun as G  # noqa: E402
import pg_commun as P  # noqa: E402

NR = G.NON_RENSEIGNE
FINESS = r"^([0-9]{2}|2[AB])[0-9]{7}$"
DATE_EFFET = date.fromisoformat(os.environ.get("DATE_EFFET") or date.today().isoformat())
RUN_ID = os.environ.get("RUN_ID") or f"J22_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
TRAVAIL = f"{P.SCHEMA_TRAVAIL}.stg_etablissement"

# Attributs suivis : un changement de l'un d'eux crée une nouvelle version.
SUIVIS = ["finess_site", "finess_juridique", "siret_site", "siren_site", "raison_sociale", "enseigne",
          "categorie", "code_commune", "commune", "code_postal", "code_departement", "id_region"]
# Tailles des colonnes texte de sql/gold_ddl.sql : au-delà, PostgreSQL refuserait la ligne.
LONGUEURS = {"identifiant_organisation": 30, "raison_sociale": 150, "enseigne": 150, "categorie": 30,
             "code_commune": 30, "commune": 60, "code_postal": 30, "code_departement": 30}
COLONNES_SILVER = ["identifiant_organisation", "finess_geo", "finess_juridique", "siret_site", "siren_site",
                   "raison_sociale", "enseigne", "categorie", "code_commune", "commune", "code_postal",
                   "code_departement", "code_region_insee"]


def ligne(nom, valeur, attendu=None):
    """Affiche un contrôle ; renvoie False en cas d'écart."""
    statut = "" if attendu is None else ("OK" if valeur == attendu else f"ECART (attendu {attendu})")
    print(f"GOLD dim_etablissement {nom:34s} = {valeur:>8}  {statut}", flush=True)
    return attendu is None or valeur == attendu


def format_ou_nul(colonne, motif):
    v = G.texte(colonne)
    return F.when(v.rlike(motif), v)


spark = G.session("J22_dim_etablissement")
try:
    silver = spark.read.parquet(f"{G.SILVER}/etablissement")
except Exception:
    print("GOLD dim_etablissement ignoré : silver/etablissement absent (lancer J14)", flush=True)
    spark.stop()
    sys.exit(0)
manquantes = [c for c in COLONNES_SILVER if c not in silver.columns]
if manquantes:
    raise SystemExit(f"GOLD ERREUR silver/etablissement : colonne(s) absente(s) {manquantes}")

# ------------------------------------------------------------ 1. Silver nettoyé -> table de travail
source = (silver.select(
    G.texte("identifiant_organisation").alias("identifiant_organisation"),
    format_ou_nul("finess_geo", FINESS).alias("finess_site"),
    format_ou_nul("finess_juridique", FINESS).alias("finess_juridique"),
    format_ou_nul("siret_site", r"^[0-9]{14}$").alias("siret_site"),
    format_ou_nul("siren_site", r"^[0-9]{9}$").alias("siren_site"),
    G.texte("raison_sociale", NR).alias("raison_sociale"),
    G.texte("enseigne").alias("enseigne"),
    G.texte("categorie", NR).alias("categorie"),
    G.texte("code_commune").alias("code_commune"),
    G.texte("commune").alias("commune"),
    G.texte("code_postal", NR).alias("code_postal"),
    G.texte("code_departement").alias("code_departement"),
    F.when(F.trim("code_region_insee").rlike("^[0-9]{2}$"), F.trim("code_region_insee").cast("int"))
     .otherwise(F.lit(-1)).alias("id_region"),
    # valeurs d'origine, pour compter les anomalies de format
    G.texte("finess_geo").alias("_finess_geo"), G.texte("finess_juridique").alias("_finess_juridique"),
    G.texte("siret_site").alias("_siret_site"), G.texte("siren_site").alias("_siren_site"))
    .filter(F.col("identifiant_organisation").isNotNull())
    .dropDuplicates(["identifiant_organisation"]).cache())

ok = ligne("lignes_silver", source.count())
for cible, origine in (("finess_site", "_finess_geo"), ("finess_juridique", "_finess_juridique"),
                       ("siret_site", "_siret_site"), ("siren_site", "_siren_site")):
    ligne(f"anomalie_format_{cible}", source.filter(F.col(origine).isNotNull() & F.col(cible).isNull()).count())
for colonne, taille in LONGUEURS.items():
    ok = ligne(f"trop_long_{colonne}", source.filter(F.length(colonne) > taille).count(), 0) and ok
if not ok:
    print("GOLD ERREUR dim_etablissement : valeur(s) trop longue(s) pour PostgreSQL, rien n'est modifié", flush=True)
    spark.stop()
    sys.exit(1)

P.preparer_schema_travail(spark)
P.ecrire_travail(source.select("identifiant_organisation", *SUIVIS), "stg_etablissement")

# ------------------------------------------------------------ 2. SCD2 dans PostgreSQL (une transaction)
d = f"DATE '{DATE_EFFET.isoformat()}'"
courante = ", ".join(f"c.{c}" for c in SUIVIS)
nouvelle = ", ".join(f"s.{c}" if c != "id_region" else "COALESCE(r.id_region, -1)" for c in SUIVIS)
attributs = ", ".join(SUIVIS)
trace = f"run_id = '{RUN_ID}', date_chargement = now()"

cx = P.Connexion(spark)
try:
    cx.executer(f"""
        CREATE TEMP TABLE scd ON COMMIT DROP AS
        SELECT s.identifiant_organisation,
               {", ".join(f"s.{c}" for c in SUIVIS if c != "id_region")},
               COALESCE(r.id_region, -1) AS id_region,
               c.id_etablissement AS id_courant, c.date_debut AS debut_courant,
               (SELECT max(h.date_fin) FROM dim_etablissement h
                 WHERE h.identifiant_organisation = s.identifiant_organisation) AS derniere_fin,
               CASE WHEN c.id_etablissement IS NOT NULL
                         AND ROW({courante}) IS NOT DISTINCT FROM ROW({nouvelle}) THEN 'inchange'
                    WHEN c.id_etablissement IS NOT NULL THEN 'modifie'
                    WHEN EXISTS (SELECT 1 FROM dim_etablissement h
                                  WHERE h.identifiant_organisation = s.identifiant_organisation) THEN 'reapparu'
                    ELSE 'nouveau' END AS statut
        FROM {TRAVAIL} s
        LEFT JOIN dim_region r ON r.id_region = s.id_region
        LEFT JOIN dim_etablissement c
               ON c.identifiant_organisation = s.identifiant_organisation AND c.est_courant""")
    statuts = dict(cx.lignes("SELECT statut, count(*) FROM scd GROUP BY statut"))

    # version courante commencée à DATE_EFFET (relance du même jour) : corrigée sur place
    corrigees = cx.executer(f"""
        UPDATE dim_etablissement e SET {", ".join(f"{c} = s.{c}" for c in SUIVIS)}, {trace}
        FROM scd s WHERE s.statut = 'modifie' AND s.debut_courant >= {d} AND e.id_etablissement = s.id_courant""")
    # version courante plus ancienne : fermée la veille de DATE_EFFET
    fermees_modif = cx.executer(f"""
        UPDATE dim_etablissement e SET date_fin = {d} - 1, est_courant = FALSE, {trace}
        FROM scd s WHERE s.statut = 'modifie' AND s.debut_courant < {d} AND e.id_etablissement = s.id_courant""")
    # établissement disparu du Silver : version courante fermée
    fermees_disparus = cx.executer(f"""
        UPDATE dim_etablissement e SET date_fin = GREATEST(e.date_debut, {d} - 1), est_courant = FALSE, {trace}
        WHERE e.est_courant AND e.id_etablissement <> -1
          AND NOT EXISTS (SELECT 1 FROM {TRAVAIL} s WHERE s.identifiant_organisation = e.identifiant_organisation)""")
    inserees = cx.executer(f"""
        INSERT INTO dim_etablissement (identifiant_organisation, {attributs}, date_debut, date_fin, est_courant, run_id)
        SELECT identifiant_organisation, {attributs},
               CASE statut WHEN 'nouveau' THEN DATE '1900-01-01'
                           WHEN 'reapparu' THEN GREATEST({d}, derniere_fin + 1)
                           ELSE {d} END,
               DATE '9999-12-31', TRUE, '{RUN_ID}'
        FROM scd WHERE statut IN ('nouveau', 'reapparu') OR (statut = 'modifie' AND debut_courant < {d})""")

    controles = [
        ligne("versions_courantes_egal_silver",
              cx.valeur("SELECT count(*) FROM dim_etablissement WHERE est_courant AND id_etablissement <> -1"),
              cx.valeur(f"SELECT count(*) FROM {TRAVAIL}")),
        ligne("doublons_version_courante", cx.valeur(
            "SELECT count(*) FROM (SELECT identifiant_organisation FROM dim_etablissement "
            "WHERE est_courant GROUP BY 1 HAVING count(*) > 1) x"), 0),
        ligne("lignes_inconnu", cx.valeur("SELECT count(*) FROM dim_etablissement WHERE id_etablissement = -1"), 1),
        ligne("versions_courantes_differentes", cx.valeur(f"""
            SELECT count(*) FROM scd s JOIN dim_etablissement e
              ON e.identifiant_organisation = s.identifiant_organisation AND e.est_courant
            WHERE ROW({", ".join(f"e.{c}" for c in SUIVIS)}) IS DISTINCT FROM ROW({", ".join(f"s.{c}" for c in SUIVIS)})"""), 0),
    ]
    for nom in ("nouveau", "modifie", "reapparu", "inchange"):
        ligne(f"etablissements_{nom}", int(statuts.get(nom, 0)))
    ligne("versions_corrigees_sur_place", corrigees)
    ligne("versions_fermees", fermees_modif + fermees_disparus)
    ligne("versions_inserees", inserees)
    if not all(controles):
        cx.annuler()
        print("GOLD ERREUR dim_etablissement : contrôle(s) en écart, transaction annulée", flush=True)
        spark.stop()
        sys.exit(1)
    cx.valider()
    cx.executer(f"DROP TABLE IF EXISTS {TRAVAIL}")
    cx.valider()
except Exception as erreur:
    cx.annuler()
    print("GOLD ERREUR dim_etablissement : " + P.message_erreur(erreur), flush=True)
    spark.stop()
    sys.exit(1)
finally:
    cx.fermer()

# ------------------------------------------------------------ 3. copie vers Hive, mêmes clés
pg = P.lire(spark, "public.dim_etablissement").cache()
n_pg = pg.count()
pg.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(f"{G.GOLD_DB}.dim_etablissement")
hive = spark.table(f"{G.GOLD_DB}.dim_etablissement")
ok = ligne("lignes_hive_egal_pg", hive.count(), n_pg)
courantes = hive.filter("est_courant AND id_etablissement <> -1").count()
print(f"GOLD dim_etablissement table={G.GOLD_DB}.dim_etablissement versions={n_pg} courantes={courantes} "
      f"date_effet={DATE_EFFET} run_id={RUN_ID}", flush=True)
spark.stop()
sys.exit(0 if ok else 1)