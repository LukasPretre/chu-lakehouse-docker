# CHU Lakehouse – Groupe 3

Projet CESI A4, bloc « Stockage et Big Data » : l'entrepôt de données (lakehouse) du CHU.
Livrable 2 : implémentation physique et évaluation de performance.

Équipe : Flora COZETTE, Lukas PRÊTRE, Fayçal AÏT TAHT.

## Ce que fait le projet

Quatre sources de données sont chargées, nettoyées puis rangées dans un modèle en étoile :

| Source | Contenu |
|---|---|
| S1 | base PostgreSQL : patients, professionnels, diagnostics, consultations |
| S2 | fichiers CSV : établissements, activité des professionnels, hospitalisations |
| S3 | satisfaction des patients (e-Satis 2017, 2019, 2020) |
| S4 | fichier des décès de l'INSEE |

Les données passent par trois couches :

- **Bronze** : données brutes, copiées par NiFi dans HDFS ;
- **Silver** : données nettoyées et pseudonymisées par Spark ;
- **Gold** : 6 dimensions et 4 faits dans Hive, publiés dans PostgreSQL pour Superset.

Outils : Docker Compose, NiFi 1.25, HDFS 3.3.6, Spark 3.5.1, Hive Metastore 3.1.3, PostgreSQL 15, Superset 5.0.

## Contenu du dépôt

| Dossier | Contenu |
|---|---|
| `jobs/` | jobs Spark (Silver, dimensions, faits, qualité, benchmarks) |
| `sql/` | création des tables du Gold, partitions et index, requêtes métier |
| `nifi-flows/` | flux NiFi d'ingestion (J01 à J04) |
| `docs/` | résultats des benchmarks et des contrôles qualité |
| `superset/` | installation de Superset |
| `tests/` | données fictives pour les tests |

## Installation

Sur une VM Ubuntu (10 Go de mémoire conseillés, 50 Go de disque) :

```bash
git clone https://github.com/LukasPretre/chu-lakehouse-docker.git
cd chu-lakehouse-docker
./bootstrap.sh /chemin/vers/DATA2023      # installe la pile et restaure la base S1
SMOKE_COUNTS=1 ./fichier_test.sh          # vérifie que tout fonctionne
```

`bootstrap.sh` crée le fichier `.env` avec les mots de passe et les clés de pseudonymisation.
Ce fichier n'est jamais mis dans Git.

## Charger les données

Dans NiFi (`https://<IP de la VM>:8443`), importer les flux de `nifi-flows/` :

1. **J01 (S1)** : activer la connexion à la base, puis lancer le flux.
2. **J02 à J04 (S2 à S4)** : déposer les fichiers dans `data/depot_s2/`, `data/depot_s3/` et `data/depot_s4/`, démarrer le flux, puis l'arrêter une fois les fichiers arrivés.

## Lancer la chaîne

```bash
./run_pipeline.sh               # toute la chaîne (Silver, Gold, PostgreSQL) et les tests
DEPUIS=4 ./run_pipeline.sh      # reprendre directement au Gold
```

Une seule fois, après le premier chargement, appliquer le partitionnement et l'index du Gold PostgreSQL :

```bash
docker exec -i chu_postgres psql -U chu_admin -d chu_gold_db < sql/gold_index_partition.sql
```

## Benchmarks

```bash
./bench_pg.sh run                       # étude préliminaire PostgreSQL
./bench_metier_pg.sh apres_index        # requêtes métier dans PostgreSQL
docker exec chu_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /jobs/j53_bench_metier.py   # requêtes métier dans Spark
```

Les résultats sont dans `docs/benchmark/`.

## Adresses utiles

| Service | Adresse |
|---|---|
| NiFi | `https://<IP>:8443` |
| Superset | `http://<IP>:8088` |
| Spark | `http://<IP>:8080` |
| HDFS | `http://<IP>:9870` |
| PostgreSQL | `<IP>`, port 5432 |

Les identifiants sont dans le fichier `.env` de la VM.

## Règles de l'équipe

- jamais de `.env`, de clé ou de donnée dans Git ;
- une branche et une pull request par sujet, relue par un autre membre ;
- jamais `docker compose down -v` (cela efface les données).
