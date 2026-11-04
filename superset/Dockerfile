# Image Superset du projet : l'image officielle (jusqu'à 6.1) ne contient aucun pilote de base,
# on ajoute psycopg2 pour PostgreSQL (base de paramètres « superset » et Gold chu_gold_db).
ARG SUPERSET_VERSION=5.0.0
FROM apache/superset:${SUPERSET_VERSION}
USER root
# Selon la version, Python est dans /app/.venv (installé avec uv) ou dans le système.
RUN set -e; \
    PY=$( [ -x /app/.venv/bin/python ] && echo /app/.venv/bin/python || command -v python ); \
    if command -v uv >/dev/null 2>&1; then \
        uv pip install --python "$PY" --no-cache psycopg2-binary; \
    else \
        "$PY" -m pip install --no-cache-dir psycopg2-binary \
        || { "$PY" -m ensurepip && "$PY" -m pip install --no-cache-dir psycopg2-binary; }; \
    fi; \
    "$PY" -c "import psycopg2; print('psycopg2', psycopg2.__version__)"
USER superset
