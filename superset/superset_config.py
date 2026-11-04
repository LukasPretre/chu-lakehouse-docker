# Configuration Superset du projet CHU Lakehouse.
# Les secrets viennent de l'environnement du conteneur (fichier .env), jamais de ce fichier.
import os
from urllib.parse import quote_plus

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Base de paramètres de Superset (tableaux de bord, utilisateurs) : base « superset »
# du PostgreSQL du projet, distincte du Gold.
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://{}:{}@postgres:5432/superset".format(
    quote_plus(os.environ["PG_USER"]), quote_plus(os.environ["PG_PASSWORD"]))

# Pas de Redis : cache en mémoire, suffisant pour une seule VM.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

SUPERSET_WEBSERVER_TIMEOUT = 120
ROW_LIMIT = 50000
BABEL_DEFAULT_LOCALE = "fr"
