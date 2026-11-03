-- Benchmark PostgreSQL, etape 2 : les deux variantes optimisees, construites depuis la table plate.
--
-- consultation_idx  : memes donnees + index B-tree choisis d'apres les requetes
--     (date_consultation)                      filtres par periode (Q1, Q2)
--     (patient_pseudo)                         recherche et regroupement par patient (Q3, Q4, Q6)
--     (professionnel_pseudo, code_diagnostic)  regroupement par professionnel et diagnostic (Q5)
--   Chaque index coute de la place et ralentit les ecritures : on n'indexe que ce que les requetes utilisent.
--
-- consultation_part : partitionnement par ANNEE sur date_consultation (RANGE). Choisi parce que
--   les analyses filtrent presque toujours sur une periode et que la mise a jour est trimestrielle :
--   on ne recharge que les partitions de l'annee concernee. Memes index, crees sur la table mere
--   (PostgreSQL les propage a chaque partition).

CREATE TABLE bench.consultation_idx (LIKE bench.consultation_plat);
INSERT INTO bench.consultation_idx SELECT * FROM bench.consultation_plat;
CREATE INDEX idx_c_date    ON bench.consultation_idx (date_consultation);
CREATE INDEX idx_c_patient ON bench.consultation_idx (patient_pseudo);
CREATE INDEX idx_c_prof_dx ON bench.consultation_idx (professionnel_pseudo, code_diagnostic);

CREATE TABLE bench.consultation_part (LIKE bench.consultation_plat) PARTITION BY RANGE (date_consultation);
DO $$
DECLARE y int; ymin int; ymax int;
BEGIN
    SELECT extract(year FROM min(date_consultation))::int, extract(year FROM max(date_consultation))::int
      INTO ymin, ymax FROM bench.consultation_plat;
    FOR y IN ymin..ymax LOOP
        EXECUTE format('CREATE TABLE bench.consultation_part_%s PARTITION OF bench.consultation_part FOR VALUES FROM (%L) TO (%L)',
                       y, make_date(y, 1, 1), make_date(y + 1, 1, 1));
    END LOOP;
END $$;
CREATE TABLE bench.consultation_part_defaut PARTITION OF bench.consultation_part DEFAULT;
INSERT INTO bench.consultation_part SELECT * FROM bench.consultation_plat;
CREATE INDEX idx_p_date    ON bench.consultation_part (date_consultation);
CREATE INDEX idx_p_patient ON bench.consultation_part (patient_pseudo);
CREATE INDEX idx_p_prof_dx ON bench.consultation_part (professionnel_pseudo, code_diagnostic);

ANALYZE bench.consultation_plat;
ANALYZE bench.consultation_idx;
ANALYZE bench.consultation_part;

-- Lignes et taille totale (donnees + index) de chaque variante
SELECT 'consultation_plat' AS variante, count(*) AS lignes,
       pg_size_pretty(pg_total_relation_size('bench.consultation_plat')) AS taille FROM bench.consultation_plat
UNION ALL
SELECT 'consultation_idx', count(*), pg_size_pretty(pg_total_relation_size('bench.consultation_idx')) FROM bench.consultation_idx
UNION ALL
SELECT 'consultation_part', count(*),
       pg_size_pretty((SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c
                       JOIN pg_inherits i ON i.inhrelid = c.oid WHERE i.inhparent = 'bench.consultation_part'::regclass)::bigint)
FROM bench.consultation_part;
