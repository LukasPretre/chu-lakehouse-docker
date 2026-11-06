"""
J34 - Copie du Gold Hive vers PostgreSQL (chu_gold_db), pour Superset et le benchmark.

Pour chaque table présente dans Hive, dimensions d'abord puis faits :
  1. empreinte de la table calculée des deux côtés (Spark sur Hive, PostgreSQL sur public.<table>) :
     si elles sont égales, la table est déjà à jour et rien n'est écrit ;
  2. sinon, Spark écrit la table Hive dans une table de travail etl.stg_<table> (JDBC), et
     PostgreSQL met à jour public.<table>, en une transaction par table :
     INSERT ... ON CONFLICT (clé primaire) DO UPDATE, seulement pour les lignes qui ont changé ;
     pour les faits, les lignes absentes de Hive sont aussi retirées (PostgreSQL = Hive, QC10) ;
     fait_satisfaction est remplacée en entier (DELETE puis INSERT, voir REMPLACEES) ;
  3. contrôle : même nombre de lignes dans Hive et dans PostgreSQL, sinon la transaction est annulée.
Le job s'arrête à la première table en erreur.

Pourquoi l'empreinte : recopier chaque table entière dans etl à chaque passage dépassait les
2 minutes (environ 1 million de lignes pour dim_professionnel et fait_consultation, 3 millions
pour fait_deces), même quand rien n'avait changé. Calculer l'empreinte ne demande qu'une lecture
de chaque côté. Elle porte sur la clé et les colonnes comparées, sous leur forme texte PostgreSQL
(col::text). Le premier chargement, ou un passage après un vrai changement, reste complet.

Jamais copiées : dim_region et dim_etablissement, tenues dans PostgreSQL (J21, J22) ; jamais de
TRUNCATE ni de DROP sur les tables du Gold.
Colonnes : celles de la table PostgreSQL qui existent dans Hive. La colonne de partition Hive
« annee » de fait_consultation n'existe pas dans PostgreSQL : elle n'est pas copiée. Les types
sont convertis vers ceux de PostgreSQL (heures « HH:mm:ss » en TIME, entiers en SMALLINT...),
sauf les textes : une valeur trop longue est refusée au lieu d'être tronquée.
La clé primaire est lue dans PostgreSQL : le job suit le partitionnement par date
(clé (id_consultation, id_date), sql/gold_index_partition.sql) sans modification.

Lancement (le pilote JDBC et les identifiants sont passés au job) :
  docker cp drivers/postgresql-42.7.3.jar chu_spark_master:/tmp/postgresql.jar
  docker exec -e PG_USER -e PG_PASSWORD chu_spark_master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 --jars /tmp/postgresql.jar /jobs/j34_copie_gold_pg.py
(./run_pipeline.sh et LANCER_JOBS=1 ./test_gold.sh le font tout seuls.)
"""
import re
import sys
import time

from pyspark.sql import functions as F

sys.path.insert(0, "/jobs")
import gold_commun as G  # noqa: E402
import pg_commun as P  # noqa: E402

DIMENSIONS = ["dim_temps", "dim_patient", "dim_professionnel", "dim_diagnostic"]
FAITS = ["fait_consultation", "fait_hospitalisation", "fait_deces", "fait_satisfaction"]
TECHNIQUES = {"run_id", "date_chargement"}   # seules, elles ne justifient pas de réécrire une ligne
# Faits dont la clé est un simple numéro de rang (J33) : d'un chargement à l'autre, le même numéro
# peut désigner une autre mesure, et la mise à jour ligne à ligne heurterait l'index unique
# uq_satisfaction_mesure. Leur contenu est donc remplacé en entier (DELETE puis INSERT, même
# transaction : Superset voit l'ancien contenu jusqu'au COMMIT).
REMPLACEES = {"fait_satisfaction"}
ENTIERS = ("smallint", "integer", "bigint")
SEP, NUL = "|", "\\N"   # séparateur et valeur nulle dans l'empreinte (comme COPY)


def colonnes_pg(cx, table):
    """[(colonne, type PostgreSQL)] et colonnes de la clé primaire de public.<table>."""
    colonnes = cx.lignes(f"""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
        WHERE a.attrelid = 'public.{table}'::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum""")
    cle = [r[0] for r in cx.lignes(f"""
        SELECT a.attname FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'public.{table}'::regclass AND i.indisprimary""")]
    return colonnes, cle


def forme_texte(nom, type_pg):
    """Valeur Hive écrite comme PostgreSQL l'écrit en texte (col::text) pour ce type."""
    c = F.col(nom)
    if type_pg.startswith("character("):          # CHAR(n) : ::text retire les espaces de fin
        return F.rtrim(c.cast("string"))
    if type_pg in ENTIERS:
        return c.cast("bigint").cast("string")
    decimal = re.fullmatch(r"numeric\((\d+),(\d+)\)", type_pg)
    if decimal:
        return c.cast(f"decimal({decimal.group(1)},{decimal.group(2)})").cast("string")
    if type_pg in ("date", "boolean"):
        return c.cast(type_pg).cast("string")
    return c.cast("string")                         # textes, heures « HH:mm:ss »


def empreintes(cx, hive, table, colonnes):
    """(lignes, empreinte) de la table Hive, puis de public.<table>, sur les colonnes données.

    Empreinte = somme, sur toutes les lignes, des 48 premiers bits du md5 de la ligne écrite en
    texte (col::text, valeurs séparées par « | », NULL écrit \\N), calculée par Spark d'un côté et
    par PostgreSQL de l'autre. Elle ne dépend pas de l'ordre des lignes ; deux tables différentes
    n'ont la même empreinte qu'avec une chance sur 2^48.
    """
    textes_pg = ", ".join(f"coalesce({n}::text, '{NUL}')" for n, _ in colonnes)
    pg = cx.lignes(f"""
        SELECT count(*), coalesce(sum(('x' || substr(md5(concat_ws('{SEP}', {textes_pg})), 1, 12))
                                      ::bit(48)::bigint), 0)
        FROM public.{table}""")[0]
    ligne = F.md5(F.concat_ws(SEP, *[F.coalesce(forme_texte(n, t), F.lit(NUL)) for n, t in colonnes]))
    h = hive.agg(F.count(F.lit(1)).alias("n"),
                 F.coalesce(F.sum(F.conv(F.substring(ligne, 1, 12), 16, 10).cast("decimal(38,0)")),
                            F.lit(0).cast("decimal(38,0)")).alias("e")).first()
    return (int(h["n"]), str(h["e"])), (int(pg[0]), str(pg[1]))


def copier(spark, table, est_fait):
    t0 = time.time()
    hive = spark.table(f"{G.GOLD_DB}.{table}")
    cx = P.Connexion(spark)
    try:
        colonnes, cle = colonnes_pg(cx, table)
        colonnes = [(n, t) for n, t in colonnes if n in hive.columns]
        noms = [n for n, _ in colonnes]
        if not cle or any(k not in noms for k in cle):
            raise RuntimeError(f"clé primaire {cle} absente des colonnes Hive")
        a_jour = [n for n in noms if n not in cle]
        comparees = [n for n in a_jour if n not in TECHNIQUES]
        types = dict(colonnes)

        # 1. même contenu dans Hive et dans PostgreSQL (hors run_id et date_chargement) : rien à faire
        (n_hive, e_hive), (n_pg, e_pg) = empreintes(cx, hive, table, [(n, types[n]) for n in cle + comparees])
        if (n_hive, e_hive) == (n_pg, e_pg):
            cx.annuler()
            print(f"GOLD J34 {table:22s} hive={n_hive} pg={n_pg} identiques (empreinte) "
                  f"duree={round(time.time() - t0)}s OK", flush=True)
            return True

        # 2. sinon, copie complète dans la table de travail puis mise à jour
        # conversion vers le type PostgreSQL, sauf pour les textes (assignation : refus si trop long)
        valeur = {n: f"s.{n}" if t.startswith(("character", "text")) else f"s.{n}::{t}" for n, t in colonnes}
        travail = f"{P.SCHEMA_TRAVAIL}.stg_{table}"
        P.ecrire_travail(hive.select(*noms), f"stg_{table}")

        retirees = 0
        if table in REMPLACEES:
            retirees = cx.executer(f"DELETE FROM public.{table}")
            maj = ""
        elif a_jour:
            maj = (f"ON CONFLICT ({', '.join(cle)}) DO UPDATE SET {', '.join(f'{n} = EXCLUDED.{n}' for n in a_jour)}"
                   + (f" WHERE ROW({', '.join(f't.{n}' for n in comparees)}) IS DISTINCT FROM "
                      f"ROW({', '.join(f'EXCLUDED.{n}' for n in comparees)})" if comparees else ""))
        else:
            maj = f"ON CONFLICT ({', '.join(cle)}) DO NOTHING"
        ecrites = cx.executer(f"""
            INSERT INTO public.{table} AS t ({', '.join(noms)})
            SELECT {', '.join(valeur[n] for n in noms)} FROM {travail} s
            {maj}""")
        if est_fait and table not in REMPLACEES:
            retirees = cx.executer(f"""
                DELETE FROM public.{table} f WHERE NOT EXISTS (
                  SELECT 1 FROM {travail} s WHERE {' AND '.join(f'f.{k} = {valeur[k]}' for k in cle)})""")
        n_pg = cx.valeur(f"SELECT count(*) FROM public.{table}")
        duree = round(time.time() - t0)
        if n_pg != n_hive:
            cx.annuler()
            print(f"GOLD J34 {table:22s} hive={n_hive} pg={n_pg} ECART : transaction annulée", flush=True)
            return False
        cx.valider()
        cx.executer(f"DROP TABLE IF EXISTS {travail}")
        cx.valider()
        mode = " (contenu remplacé)" if table in REMPLACEES else ""
        print(f"GOLD J34 {table:22s} hive={n_hive} pg={n_pg} ecrites={ecrites} retirees={retirees}{mode} "
              f"duree={duree}s OK", flush=True)
        return True
    except Exception as erreur:
        cx.annuler()
        print(f"GOLD ERREUR J34 {table} : {P.message_erreur(erreur)}", flush=True)
        return False
    finally:
        cx.fermer()


spark = G.session("J34_copie_gold_pg")
P.preparer_schema_travail(spark)
ok, copiees = True, 0
for table in DIMENSIONS + FAITS:
    if not spark.catalog.tableExists(f"{G.GOLD_DB}.{table}"):
        print(f"GOLD J34 {table:22s} ignorée : absente de Hive", flush=True)
        continue
    if not copier(spark, table, table in FAITS):
        ok = False
        break
    copiees += 1
print(f"GOLD J34 copie terminee tables={copiees} statut={'OK' if ok else 'ERREUR'}", flush=True)
spark.stop()
sys.exit(0 if ok else 1)