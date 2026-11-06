-- =============================================================================
-- CHU Lakehouse : partitionnement et index du Gold PostgreSQL (base chu_gold_db)
--
-- Usage (racine du dépôt, sur la VM), après sql/gold_ddl.sql :
--   docker exec -i chu_postgres psql -U "$PG_USER" -d chu_gold_db -v ON_ERROR_STOP=1 < sql/gold_index_partition.sql
--
-- Le script est rejouable et GARDE LES DONNÉES : la table est recréée, partitionnée, avec ses
-- lignes recopiées, dans une seule transaction (en cas d'erreur, rien ne change). Rejoué sur une
-- table déjà partitionnée, il ne recrée rien et vérifie seulement les index.
-- Rejouer sql/gold_ddl.sql remet fait_consultation en table simple : relancer ensuite ce script.
-- (APPLIQUER_DDL=1 ./test_gold.sh enchaîne les deux.)
--
-- 1. fait_consultation partitionnée par plages de id_date (AAAAMMJJ), une partition par année :
--    - 2015 à 2023 (période des consultations), plus une partition par défaut pour le reste,
--      dont la date inconnue (-1) ;
--    - décision d'équipe (fiche membre 2, §4) : par date dans PostgreSQL, par année dans Hive ;
--    - les requêtes métier filtrent toutes sur f.id_date (sql/gold_requetes_metier.sql) :
--      PostgreSQL ne lit que les partitions de la période (benchmark de Lukas, docs/benchmark/,
--      bench_pg_x10.csv : filtre sur une année 926 ms -> 142 ms, consultations par mois 883 -> 209 ms) ;
--    - la clé primaire doit contenir la colonne de partition : PRIMARY KEY (id_consultation, id_date).
--      PostgreSQL ne garantit donc plus seul l'unicité de id_consultation : J30 la contrôle
--      (GOLDQC doublons_id_consultation = 0) et test_gold.sh aussi ;
--    - les partitions sont rangées dans le schéma gold_partitions : Superset (rôle
--      superset_lecture) ne voit que les 10 tables du modèle dans public, et lit les partitions
--      à travers la table mère.
-- 2. Index secondaires, seulement ceux qu'une mesure justifie (un index ralentit les écritures et,
--    mesuré, certaines lectures : index sur la date +18 % sur le filtre par année) :
--    - fait_consultation (id_patient) : historique d'un patient 1 021 ms -> 0,5 ms (x 1 900),
--      top des patients 21,0 s -> 5,4 s. Créé sur la table mère, PostgreSQL le propage à chaque
--      partition.
--    Pas d'autre index : les requêtes métier M1 à M7 agrègent une période entière, que le
--    partitionnement suffit à cibler, et l'index (professionnel, diagnostic) n'a rien changé
--    (Q5 : 6 040 ms -> 6 035 ms). Lukas mesure les requêtes métier avant et après ce script.
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS gold_partitions;
COMMENT ON SCHEMA gold_partitions IS
    'Partitions des faits du Gold : lues à travers les tables mères de public, jamais directement';

DO $$
DECLARE
    annee   integer;
    avant   bigint;
    apres   bigint;
BEGIN
    IF (SELECT relkind FROM pg_class WHERE oid = 'public.fait_consultation'::regclass) = 'p' THEN
        RAISE NOTICE 'fait_consultation est déjà partitionnée : rien à recréer';
        RETURN;
    END IF;

    -- l'ancienne table est mise de côté (sa clé primaire libère son nom), puis supprimée à la fin
    ALTER TABLE public.fait_consultation RENAME TO fait_consultation_avant;
    ALTER INDEX public.fait_consultation_pkey RENAME TO fait_consultation_avant_pkey;

    -- mêmes colonnes, valeurs par défaut, CHECK et commentaires que dans sql/gold_ddl.sql ;
    -- les clés étrangères gardent leur nom (test_gold.sh s'en sert)
    CREATE TABLE public.fait_consultation (
        LIKE public.fait_consultation_avant INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING COMMENTS,
        CONSTRAINT fait_consultation_pkey PRIMARY KEY (id_consultation, id_date),
        CONSTRAINT fait_consultation_id_date_fkey
            FOREIGN KEY (id_date) REFERENCES public.dim_temps (id_date),
        CONSTRAINT fait_consultation_id_patient_fkey
            FOREIGN KEY (id_patient) REFERENCES public.dim_patient (id_patient),
        CONSTRAINT fait_consultation_id_professionnel_fkey
            FOREIGN KEY (id_professionnel) REFERENCES public.dim_professionnel (id_professionnel),
        CONSTRAINT fait_consultation_id_diagnostic_fkey
            FOREIGN KEY (id_diagnostic) REFERENCES public.dim_diagnostic (id_diagnostic),
        CONSTRAINT fait_consultation_id_etablissement_fkey
            FOREIGN KEY (id_etablissement) REFERENCES public.dim_etablissement (id_etablissement)
    ) PARTITION BY RANGE (id_date);

    FOR annee IN 2015..2023 LOOP
        EXECUTE format('CREATE TABLE gold_partitions.fait_consultation_%s PARTITION OF public.fait_consultation '
                       'FOR VALUES FROM (%s) TO (%s)', annee, annee * 10000, (annee + 1) * 10000);
    END LOOP;
    CREATE TABLE gold_partitions.fait_consultation_defaut PARTITION OF public.fait_consultation DEFAULT;

    INSERT INTO public.fait_consultation SELECT * FROM public.fait_consultation_avant;
    SELECT count(*) INTO avant FROM public.fait_consultation_avant;
    SELECT count(*) INTO apres FROM public.fait_consultation;
    IF avant <> apres THEN
        RAISE EXCEPTION 'fait_consultation : % lignes avant, % après : rien n''est changé', avant, apres;
    END IF;
    DROP TABLE public.fait_consultation_avant;

    COMMENT ON TABLE public.fait_consultation IS
        'Une ligne par consultation (J30) ; partitionnée par année sur id_date (sql/gold_index_partition.sql)';
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset_lecture') THEN
        GRANT SELECT ON public.fait_consultation TO superset_lecture;
    END IF;
    RAISE NOTICE 'fait_consultation partitionnée : % lignes recopiées', apres;
END $$;

CREATE INDEX IF NOT EXISTS idx_consultation_patient ON public.fait_consultation (id_patient);

ANALYZE public.fait_consultation;

COMMIT;

-- Contrôle : lignes par partition (la partition par défaut ne devrait contenir que des dates -1)
SELECT replace(i.inhrelid::regclass::text, 'gold_partitions.', '') AS partition,
       pg_get_expr(c.relpartbound, c.oid)                          AS bornes,
       COALESCE(n.lignes, 0)                                       AS lignes
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
LEFT JOIN (SELECT tableoid, count(*) AS lignes FROM public.fait_consultation GROUP BY tableoid) n
       ON n.tableoid = i.inhrelid
WHERE i.inhparent = 'public.fait_consultation'::regclass
ORDER BY 1;