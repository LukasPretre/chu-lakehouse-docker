"""
Accès à PostgreSQL (chu_gold_db) depuis Spark, pour J21, J22 et J34. Ce n'est pas un job : il est
importé par les jobs (import pg_commun), qui sont dans le même dossier /jobs.

- Pilote : drivers/postgresql-42.7.3.jar, copié dans le conteneur et passé par spark-submit --jars
  (run_pipeline.sh et test_gold.sh le font pour ces trois jobs).
- Identifiants : variables PG_USER et PG_PASSWORD de l'environnement du conteneur
  (docker exec -e PG_USER -e PG_PASSWORD), jamais sur la ligne de commande ni dans les journaux.
- Tables de travail : schéma « etl », hors du schéma public que lit Superset (rôle superset_lecture).
"""
import os

PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/chu_gold_db")
SCHEMA_TRAVAIL = "etl"


def _identifiants():
    try:
        return os.environ["PG_USER"], os.environ["PG_PASSWORD"]
    except KeyError as absente:
        raise SystemExit(f"GOLD ERREUR variable {absente} absente : lancer le job avec "
                         "docker exec -e PG_USER -e PG_PASSWORD (voir run_pipeline.sh)")


def _options():
    user, mot_de_passe = _identifiants()
    separateur = "&" if "?" in PG_URL else "?"
    return {"url": f"{PG_URL}{separateur}reWriteBatchedInserts=true", "user": user,
            "password": mot_de_passe, "driver": "org.postgresql.Driver"}


def lire(spark, table):
    """Lit une table PostgreSQL (ou une requête entre parenthèses avec un alias) dans Spark."""
    return spark.read.format("jdbc").options(**_options()).option("dbtable", table).load()


def ecrire_travail(df, nom, partitions=4):
    """Écrit df dans etl.<nom>, table de travail recréée à chaque lot (aucune clé étrangère)."""
    (df.repartition(partitions).write.format("jdbc").options(**_options())
       .option("dbtable", f"{SCHEMA_TRAVAIL}.{nom}").option("batchsize", 10000)
       .mode("overwrite").save())


class Connexion:
    """Connexion JDBC du driver Spark, en transaction : rien n'est écrit avant valider()."""

    def __init__(self, spark):
        user, mot_de_passe = _identifiants()
        proprietes = spark._jvm.java.util.Properties()
        proprietes.setProperty("user", user)
        proprietes.setProperty("password", mot_de_passe)
        self.c = spark._jvm.org.postgresql.Driver().connect(PG_URL, proprietes)
        if self.c is None:
            raise SystemExit(f"GOLD ERREUR connexion PostgreSQL impossible ({PG_URL})")
        self.c.setAutoCommit(False)

    def executer(self, sql):
        """Exécute une instruction ; renvoie le nombre de lignes touchées."""
        st = self.c.createStatement()
        try:
            return st.executeUpdate(sql)
        finally:
            st.close()

    def lignes(self, sql):
        """Renvoie le résultat d'une requête : liste de tuples de chaînes (ou None)."""
        st = self.c.createStatement()
        rs = st.executeQuery(sql)
        try:
            n = rs.getMetaData().getColumnCount()
            resultat = []
            while rs.next():
                resultat.append(tuple(rs.getString(i) for i in range(1, n + 1)))
            return resultat
        finally:
            rs.close()
            st.close()

    def valeur(self, sql):
        """Première colonne de la première ligne, en entier si possible."""
        v = self.lignes(sql)[0][0]
        try:
            return int(v)
        except (TypeError, ValueError):
            return v

    def valider(self):
        self.c.commit()

    def annuler(self):
        self.c.rollback()

    def fermer(self):
        self.c.close()


def message_erreur(erreur):
    """Ligne utile d'une erreur PostgreSQL remontée par py4j (sinon la première ligne)."""
    lignes = [l.strip() for l in str(erreur).splitlines() if l.strip()]
    for l in lignes:
        if "ERROR:" in l or "ERREUR:" in l:
            return l[l.find("ERR"):][:300]
    return (lignes[0] if lignes else repr(erreur))[:300]


def preparer_schema_travail(spark):
    cx = Connexion(spark)
    cx.executer(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_TRAVAIL}")
    cx.valider()
    cx.fermer()