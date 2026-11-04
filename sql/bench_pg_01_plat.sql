-- Benchmark PostgreSQL, etape 1 : table de reference, sans index ni partition.
-- Chargee ensuite par COPY depuis l'export J52 (bench_pg.sh prepare).
CREATE SCHEMA IF NOT EXISTS bench;
DROP TABLE IF EXISTS bench.consultation_plat, bench.consultation_idx, bench.consultation_part CASCADE;

CREATE TABLE bench.consultation_plat (
    id_consultation      bigint,
    date_consultation    date,
    heure_debut          time,
    heure_fin            time,
    duree_minutes        integer,
    patient_pseudo       varchar(64),
    professionnel_pseudo varchar(64),
    code_diagnostic      varchar(10),
    annee                smallint,
    mois                 smallint
);
