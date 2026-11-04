"""
Contrôles du Gold Hive (lancés par ./test_gold.sh et par la CI).

Dimensions présentes : clé de substitution non nulle et unique, une seule ligne -1 « Inconnu »,
clé naturelle unique ; dim_temps sans jour manquant ; dim_etablissement : une seule version
courante par établissement (SCD2).
Faits présents : clé unique, clés étrangères non nulles et toutes présentes dans leur
dimension (sinon la copie J34 vers PostgreSQL échoue sur les clés étrangères), compteurs à 1.
Compatibilité avec PostgreSQL (PG_COLONNES, fourni par test_gold.sh) : chaque colonne Hive
existe dans la table PostgreSQL et les colonnes obligatoires sont produites.
Volumes Hive = PostgreSQL (QC10) avec COMPARER_PG=1, une fois J34 en place (PG_COMPTES).

Une table absente est ignorée (SKIP), sauf avec STRICT=1.
Sortie : une ligne « GOLDTEST|OK|... », « GOLDTEST|ECHEC|... » ou « GOLDTEST|SKIP|... » par contrôle.
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F

GOLD_DB = os.environ.get("GOLD_DB", "chu_gold")
STRICT = os.environ.get("STRICT", "0") == "1"
COMPARER_PG = os.environ.get("COMPARER_PG", "0") == "1"

# dimension -> (clé de substitution, clé naturelle)
DIMENSIONS = {
    "dim_temps":         ("id_date", ["date_complete"]),
    "dim_region":        ("id_region", ["code_region_insee"]),
    "dim_etablissement": ("id_etablissement", ["identifiant_organisation", "date_debut"]),
    "dim_patient":       ("id_patient", ["patient_pseudo"]),
    "dim_professionnel": ("id_professionnel", ["identifiant_pseudo"]),
    "dim_diagnostic":    ("id_diagnostic", ["code_diagnostic"]),
}
# fait -> (clé, compteur, {clé étrangère: dimension})
FAITS = {
    "fait_consultation": ("id_consultation", "nb_consultations", {
        "id_date": "dim_temps", "id_patient": "dim_patient", "id_professionnel": "dim_professionnel",
        "id_diagnostic": "dim_diagnostic", "id_etablissement": "dim_etablissement"}),
    "fait_hospitalisation": ("id_hospitalisation", "nb_hospitalisations", {
        "id_date_entree": "dim_temps", "id_date_sortie": "dim_temps", "id_patient": "dim_patient",
        "id_etablissement": "dim_etablissement", "id_diagnostic": "dim_diagnostic"}),
    "fait_deces": ("id_deces", "nb_deces", {"id_date": "dim_temps", "id_region": "dim_region"}),
    "fait_satisfaction": ("id_satisfaction", None, {
        "id_date": "dim_temps", "id_etablissement": "dim_etablissement"}),
}


def lire_pg(variable, separateur):
    """PG_COLONNES « table.colonne.obligatoire ... » ou PG_COMPTES « table:n ... »."""
    valeurs = {}
    for element in os.environ.get(variable, "").split():
        if separateur == ".":
            table, colonne, obligatoire = element.split(".")
            valeurs.setdefault(table, {})[colonne] = obligatoire == "1"
        else:
            table, n = element.split(":")
            valeurs[table] = int(n)
    return valeurs


PG_COLONNES = lire_pg("PG_COLONNES", ".")
PG_COMPTES = lire_pg("PG_COMPTES", ":")

spark = (SparkSession.builder.appName("check_gold").enableHiveSupport()
         .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
         .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/warehouse")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
echecs = 0


def resultat(ok, message):
    global echecs
    if ok is None:
        print(f"GOLDTEST|SKIP|{message}")
        return
    echecs += 0 if ok else 1
    print(f"GOLDTEST|{'OK' if ok else 'ECHEC'}|{message}")


try:
    bases = [b.name for b in spark.catalog.listDatabases()]
except Exception as erreur:
    resultat(False, "Metastore Hive inaccessible depuis Spark : " + str(erreur).strip().splitlines()[0][:200])
    spark.stop()
    sys.exit(1)


def table(nom):
    if GOLD_DB in bases and spark.catalog.tableExists(f"{GOLD_DB}.{nom}"):
        return spark.table(f"{GOLD_DB}.{nom}").cache()
    return None


tables = {nom: table(nom) for nom in list(DIMENSIONS) + list(FAITS)}
presentes = [t for t, df in tables.items() if df is not None]
print(f"GOLDTEST|OK|base Hive {GOLD_DB} : {len(presentes)} table(s) Gold présente(s) sur 10")
for nom in [t for t, df in tables.items() if df is None]:
    resultat(False if STRICT else None, f"{nom} : table Hive pas encore produite")

# ----------------------------------------------------------------- Dimensions
for nom, (cle, naturelle) in DIMENSIONS.items():
    df = tables[nom]
    if df is None:
        continue
    n = df.count()
    nulles = df.filter(F.col(cle).isNull()).count()
    doublons = n - df.select(cle).distinct().count()
    resultat(n > 1 and nulles == 0 and doublons == 0,
             f"{nom} : {n} lignes, clé {cle} non nulle et unique (nulles={nulles}, doublons={doublons})")
    inconnu = df.filter(F.col(cle) == -1).count()
    resultat(inconnu == 1, f"{nom} : une ligne -1 « Inconnu » ({inconnu} trouvée(s))")
    reelles = df.filter(F.col(cle) != -1)
    doublons = reelles.count() - reelles.select(*naturelle).distinct().count()
    resultat(doublons == 0, f"{nom} : clé naturelle {' + '.join(naturelle)} unique ({doublons} doublon(s))")

    if nom == "dim_temps":
        r = reelles.agg(F.count("*").alias("n"), F.min("date_complete").alias("debut"),
                        F.max("date_complete").alias("fin")).first()
        attendu = (r["fin"] - r["debut"]).days + 1
        resultat(r["n"] == attendu, f"dim_temps : aucun jour manquant du {r['debut']} au {r['fin']} ({r['n']}/{attendu})")
        d = F.col("date_complete")
        faux = reelles.filter(F.col("id_date") != F.year(d) * 10000 + F.month(d) * 100 + F.dayofmonth(d)).count()
        resultat(faux == 0, f"dim_temps : id_date = AAAAMMJJ de la date ({faux} incohérent(s))")
    if nom == "dim_etablissement":
        plusieurs = (reelles.filter("est_courant").groupBy("identifiant_organisation")
                     .count().filter("count > 1").count())
        resultat(plusieurs == 0, f"dim_etablissement : une seule version courante par établissement ({plusieurs} en défaut)")
        faux = reelles.filter(F.col("est_courant") != (F.col("date_fin") == F.lit("9999-12-31").cast("date"))).count()
        resultat(faux == 0, f"dim_etablissement : version courante = date_fin au 31/12/9999 ({faux} incohérente(s))")

# ----------------------------------------------------------------- Faits
for nom, (cle, compteur, etrangeres) in FAITS.items():
    df = tables[nom]
    if df is None:
        continue
    n = df.count()
    doublons = n - df.select(cle).distinct().count()
    resultat(n > 0 and doublons == 0, f"{nom} : {n} lignes, clé {cle} unique ({doublons} doublon(s))")
    for fk, dim in etrangeres.items():
        nulles = df.filter(F.col(fk).isNull()).count()
        if tables[dim] is None:
            if nulles:
                resultat(False, f"{nom}.{fk} : {nulles} clé(s) nulle(s)")
            else:
                resultat(False if STRICT else None, f"{nom}.{fk} : {dim} absente, rattachement non vérifiable")
            continue
        cle_dim = DIMENSIONS[dim][0]
        orphelins = (df.select(F.col(fk).alias("k")).distinct()
                     .join(tables[dim].select(F.col(cle_dim).alias("k")), "k", "left_anti").count())
        resultat(nulles == 0 and orphelins == 0,
                 f"{nom}.{fk} -> {dim} : aucune clé nulle ni orpheline (nulles={nulles}, valeurs orphelines={orphelins})")
        if fk != "id_date_sortie":
            part = df.filter(F.col(fk) == -1).count()
            print(f"GOLDTEST|OK|{nom}.{fk} : {part} ligne(s) en -1 « Inconnu » ({100 * part / max(n, 1):.1f} %)")
    if compteur:
        faux = df.filter(F.col(compteur) != 1).count()
        resultat(faux == 0, f"{nom} : {compteur} vaut 1 sur chaque ligne ({faux} en écart)")
    if nom == "fait_satisfaction":
        reels = df.filter(F.col("id_etablissement") != -1)
        doublons = reels.count() - reels.select("id_date", "id_etablissement", "type_enquete").distinct().count()
        resultat(doublons == 0, f"fait_satisfaction : une mesure par établissement, année et enquête ({doublons} doublon(s))")

# ----------------------------------------------------------------- Compatibilité avec PostgreSQL (J34)
if not PG_COLONNES:
    resultat(None, "colonnes Hive / PostgreSQL : PG_COLONNES non fourni (lancer via ./test_gold.sh)")
for nom in presentes if PG_COLONNES else []:
    if nom not in PG_COLONNES:
        resultat(False, f"{nom} : table absente de PostgreSQL (rejouer sql/gold_ddl.sql)")
        continue

    # Les colonnes de partition Hive (annee de J30) ne sont pas copiées par J34 : elles ne comptent pas.
    partitions = {c.name for c in spark.catalog.listColumns(f"{GOLD_DB}.{nom}") if c.isPartition}
    hive = set(tables[nom].columns) - partitions
    en_trop = sorted(hive - set(PG_COLONNES[nom]))
    
    manquantes = sorted(c for c, obligatoire in PG_COLONNES[nom].items() if obligatoire and c not in hive)
    detail = "".join([f" ; absentes de PostgreSQL : {', '.join(en_trop)}" if en_trop else "",
                      f" ; obligatoires non produites : {', '.join(manquantes)}" if manquantes else ""])
    resultat(not en_trop and not manquantes, f"{nom} : colonnes Hive compatibles avec PostgreSQL{detail}")

# ----------------------------------------------------------------- Volumes Hive = PostgreSQL (QC10)
if not COMPARER_PG:
    resultat(None, "volumes Hive = PostgreSQL (QC10) : à activer après J34 avec COMPARER_PG=1")
for nom in presentes if COMPARER_PG else []:
    n_hive, n_pg = tables[nom].count(), PG_COMPTES.get(nom, -1)
    resultat(n_hive == n_pg, f"{nom} : mêmes volumes dans Hive et PostgreSQL ({n_hive} / {n_pg})")

spark.stop()
sys.exit(1 if echecs else 0)