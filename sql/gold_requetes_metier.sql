-- =====================================================================================
-- Requêtes métier du Gold (Livrable 2) : indicateurs I1 à I8 des besoins utilisateurs
-- (Livrable 1, « Analyse des sources »). Base : chu_gold_db (PostgreSQL).
-- SQL standard (CAST, fonctions de fenêtre) : les mêmes requêtes tournent aussi en Spark SQL
-- sur chu_gold, en remplaçant le nom de base si besoin.
--
-- Conventions
-- - Période Y fixée à l'année 2019 pour que les mesures du benchmark soient comparables.
--   Le filtre porte directement sur fait_*.id_date (AAAAMMJJ) : c'est la clé de partition,
--   PostgreSQL ne lit donc que les partitions utiles.
-- - « Taux » = part (en %) du groupe dans le total de la même période. HYPOTHÈSE à confirmer
--   avec la définition du Livrable 1 ; si le taux doit être rapporté à une population,
--   seul le dénominateur change.
-- - Les clés -1 (« Inconnu ») sont conservées : elles apparaissent comme un groupe à part.
--
-- - Taux d'hospitalisation (M4, M5) : rapporté à la FILE ACTIVE, c'est-à-dire au nombre de
--   patients distincts vus en consultation sur la même période et le même groupe
--   (Livrable 1, I3 : « fait_consultation (file active) »). HYPOTHÈSE pour I4 et I5 :
--   même dénominateur, restreint au diagnostic ou au sexe et à la tranche d'âge.
-- - Deux faits ne sont jamais joints ligne à ligne : chacun est agrégé à part (CTE),
--   puis les agrégats sont joints, sinon les lignes se multiplient.
--
-- M1 à M3 : fait_consultation. M4, M5 : fait_hospitalisation + fait_consultation.
-- M6 : fait_deces (optionnelle). M7 : fait_satisfaction (optionnelle).
-- =====================================================================================


-- -------------------------------------------------------------------------------------
-- M1 (I1) : taux de consultation par établissement et région, par mois, sur la période Y.
-- Tables : fait_consultation, dim_etablissement, dim_region, dim_temps.
-- La version SCD2 de l'établissement est celle portée par le fait (id_etablissement).
-- Pour « un établissement X » : décommenter le filtre sur identifiant_organisation.
-- Tant que la règle Q2 n'est pas branchée (J15), toutes les lignes sont à -1 (Inconnu).
-- -------------------------------------------------------------------------------------
SELECT r.libelle_region,
       e.identifiant_organisation,
       e.raison_sociale,
       t.annee,
       t.mois,
       COUNT(*) AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee, t.mois), 2) AS taux_pct
FROM fait_consultation f
JOIN dim_temps         t ON t.id_date = f.id_date
JOIN dim_etablissement e ON e.id_etablissement = f.id_etablissement
JOIN dim_region        r ON r.id_region = e.id_region
WHERE f.id_date BETWEEN 20190101 AND 20191231
--  AND e.identifiant_organisation = 'F010000024'
GROUP BY r.libelle_region, e.identifiant_organisation, e.raison_sociale, t.annee, t.mois
ORDER BY t.annee, t.mois, nb_consultations DESC;


-- -------------------------------------------------------------------------------------
-- M2 (I2) : taux de consultation par diagnostic, par trimestre, sur la période Y.
-- Tables : fait_consultation, dim_diagnostic, dim_temps.
-- Pour « un diagnostic X » : décommenter le filtre sur code_diagnostic.
-- -------------------------------------------------------------------------------------
SELECT d.code_diagnostic,
       d.libelle_diagnostic,
       t.annee,
       t.trimestre,
       COUNT(*) AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee, t.trimestre), 3) AS taux_pct
FROM fait_consultation f
JOIN dim_temps      t ON t.id_date = f.id_date
JOIN dim_diagnostic d ON d.id_diagnostic = f.id_diagnostic
WHERE f.id_date BETWEEN 20190101 AND 20191231
--  AND d.code_diagnostic = 'S02800'
GROUP BY d.code_diagnostic, d.libelle_diagnostic, t.annee, t.trimestre
ORDER BY t.annee, t.trimestre, nb_consultations DESC;


-- -------------------------------------------------------------------------------------
-- M3 (I6) : taux de consultation par professionnel (profession, spécialité) et par année,
-- sur la période 2017 à 2022.
-- Tables : fait_consultation, dim_professionnel, dim_etablissement, dim_temps.
-- taux_pct = part des consultations de l'année ; consultations_par_professionnel =
-- consultations / nombre de professionnels distincts du groupe (activité moyenne).
-- -------------------------------------------------------------------------------------
SELECT p.profession,
       p.specialite,
       t.annee,
       COUNT(*)                               AS nb_consultations,
       COUNT(DISTINCT f.id_professionnel)     AS nb_professionnels,
       ROUND(CAST(COUNT(*) AS NUMERIC) / COUNT(DISTINCT f.id_professionnel), 2)
                                              AS consultations_par_professionnel,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee), 3) AS taux_pct,
       COUNT(DISTINCT f.id_etablissement)     AS nb_etablissements
FROM fait_consultation f
JOIN dim_temps         t ON t.id_date = f.id_date
JOIN dim_professionnel p ON p.id_professionnel = f.id_professionnel
WHERE f.id_date BETWEEN 20170101 AND 20221231
GROUP BY p.profession, p.specialite, t.annee
ORDER BY t.annee, nb_consultations DESC;


-- -------------------------------------------------------------------------------------
-- M4a (I3) : taux global d'hospitalisation par trimestre, sur la période Y.
-- Tables : fait_hospitalisation, fait_consultation (file active), dim_temps.
-- La période d'une hospitalisation est celle de sa date d'entrée.
-- -------------------------------------------------------------------------------------
WITH hospit AS (
    SELECT t.annee, t.trimestre,
           COUNT(*)                     AS nb_hospitalisations,
           COUNT(DISTINCT h.id_patient) AS nb_patients_hospitalises
    FROM fait_hospitalisation h
    JOIN dim_temps t ON t.id_date = h.id_date_entree
    WHERE h.id_date_entree BETWEEN 20190101 AND 20191231
    GROUP BY t.annee, t.trimestre
), file_active AS (
    SELECT t.annee, t.trimestre,
           COUNT(DISTINCT f.id_patient) AS nb_patients_file_active
    FROM fait_consultation f
    JOIN dim_temps t ON t.id_date = f.id_date
    WHERE f.id_date BETWEEN 20190101 AND 20191231
    GROUP BY t.annee, t.trimestre
)
SELECT a.annee, a.trimestre,
       a.nb_patients_file_active,
       COALESCE(h.nb_hospitalisations, 0)      AS nb_hospitalisations,
       COALESCE(h.nb_patients_hospitalises, 0) AS nb_patients_hospitalises,
       ROUND(100.0 * COALESCE(h.nb_patients_hospitalises, 0)
             / NULLIF(a.nb_patients_file_active, 0), 3) AS taux_hospitalisation_pct
FROM file_active a
LEFT JOIN hospit h ON h.annee = a.annee AND h.trimestre = a.trimestre
ORDER BY a.annee, a.trimestre;


-- -------------------------------------------------------------------------------------
-- M4b (I4) : taux d'hospitalisation par diagnostic et par année (2016 à 2021, période
-- couverte par les hospitalisations).
-- Dénominateur : patients distincts vus en consultation pour ce diagnostic la même année.
-- Tables : fait_hospitalisation, fait_consultation, dim_diagnostic, dim_temps.
-- -------------------------------------------------------------------------------------
WITH hospit AS (
    SELECT t.annee, h.id_diagnostic,
           COUNT(*)                     AS nb_hospitalisations,
           COUNT(DISTINCT h.id_patient) AS nb_patients_hospitalises
    FROM fait_hospitalisation h
    JOIN dim_temps t ON t.id_date = h.id_date_entree
    WHERE h.id_date_entree BETWEEN 20160101 AND 20211231
    GROUP BY t.annee, h.id_diagnostic
), file_active AS (
    SELECT t.annee, f.id_diagnostic,
           COUNT(DISTINCT f.id_patient) AS nb_patients_file_active
    FROM fait_consultation f
    JOIN dim_temps t ON t.id_date = f.id_date
    WHERE f.id_date BETWEEN 20160101 AND 20211231
    GROUP BY t.annee, f.id_diagnostic
)
SELECT h.annee, d.code_diagnostic, d.libelle_diagnostic,
       h.nb_hospitalisations,
       h.nb_patients_hospitalises,
       a.nb_patients_file_active,
       ROUND(100.0 * h.nb_patients_hospitalises / NULLIF(a.nb_patients_file_active, 0), 2)
                                                AS taux_hospitalisation_pct
FROM hospit h
JOIN dim_diagnostic d   ON d.id_diagnostic = h.id_diagnostic
LEFT JOIN file_active a ON a.annee = h.annee AND a.id_diagnostic = h.id_diagnostic
ORDER BY h.annee, h.nb_hospitalisations DESC;


-- -------------------------------------------------------------------------------------
-- M5 (I5) : taux d'hospitalisation par sexe et tranche d'âge, sur la période Y.
-- Pas de dim_demographie : sexe et année de naissance viennent de dim_patient ;
-- âge = année de l'événement - année de naissance, tranches de 10 ans, 90+ regroupés.
-- Tables : fait_hospitalisation, fait_consultation, dim_patient, dim_temps.
-- -------------------------------------------------------------------------------------
WITH evenements AS (
    SELECT 'H' AS source, h.id_patient, t.annee
    FROM fait_hospitalisation h
    JOIN dim_temps t ON t.id_date = h.id_date_entree
    WHERE h.id_date_entree BETWEEN 20190101 AND 20191231
    UNION ALL
    SELECT 'C' AS source, f.id_patient, t.annee
    FROM fait_consultation f
    JOIN dim_temps t ON t.id_date = f.id_date
    WHERE f.id_date BETWEEN 20190101 AND 20191231
), classes AS (
    SELECT e.source, e.id_patient, p.sexe,
           CASE
             WHEN p.annee_naissance IS NULL OR e.annee - p.annee_naissance < 0 THEN 'Inconnu'
             WHEN e.annee - p.annee_naissance >= 90 THEN '90+'
             ELSE CONCAT(CAST(FLOOR((e.annee - p.annee_naissance) / 10) * 10 AS INTEGER), '-',
                         CAST(FLOOR((e.annee - p.annee_naissance) / 10) * 10 + 9 AS INTEGER))
           END AS tranche_age
    FROM evenements e
    JOIN dim_patient p ON p.id_patient = e.id_patient
)
SELECT sexe, tranche_age,
       COUNT(DISTINCT CASE WHEN source = 'C' THEN id_patient END) AS nb_patients_file_active,
       COUNT(DISTINCT CASE WHEN source = 'H' THEN id_patient END) AS nb_patients_hospitalises,
       SUM(CASE WHEN source = 'H' THEN 1 ELSE 0 END)             AS nb_hospitalisations,
       ROUND(100.0 * COUNT(DISTINCT CASE WHEN source = 'H' THEN id_patient END)
             / NULLIF(COUNT(DISTINCT CASE WHEN source = 'C' THEN id_patient END), 0), 3)
                                                                  AS taux_hospitalisation_pct
FROM classes
GROUP BY sexe, tranche_age
ORDER BY sexe, tranche_age;


-- -------------------------------------------------------------------------------------
-- M6 (I7, optionnelle) : nombre de décès par région en 2019, détail mensuel.
-- Tables : fait_deces, dim_region, dim_temps. Région = lieu du décès.
-- Le fichier des décès n'est jamais relié aux patients.
-- -------------------------------------------------------------------------------------
SELECT r.code_region_insee,
       r.libelle_region,
       t.mois,
       COUNT(*) AS nb_deces,
       SUM(COUNT(*)) OVER (PARTITION BY r.code_region_insee) AS nb_deces_annee_region,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.mois), 2) AS part_du_mois_pct
FROM fait_deces dc
JOIN dim_temps  t ON t.id_date = dc.id_date
JOIN dim_region r ON r.id_region = dc.id_region
WHERE dc.id_date BETWEEN 20190101 AND 20191231
GROUP BY r.code_region_insee, r.libelle_region, t.mois
ORDER BY r.code_region_insee, t.mois;


-- -------------------------------------------------------------------------------------
-- M7 (I8, optionnelle) : satisfaction par région en 2020, par type d'enquête.
-- Scores PONDÉRÉS par le nombre de réponses (pas de moyenne simple) ; les lignes sans
-- score ou sans réponse (classement NV, NR) sont exclues du calcul.
-- id_date = 1er janvier de l'année de campagne (décision du groupe).
-- Tables : fait_satisfaction, dim_etablissement, dim_region.
-- -------------------------------------------------------------------------------------
SELECT r.code_region_insee,
       r.libelle_region,
       s.type_enquete,
       COUNT(DISTINCT s.id_etablissement) AS nb_etablissements,
       SUM(s.nb_reponses)                 AS nb_reponses,
       ROUND(SUM(s.score_global * s.nb_reponses) / NULLIF(SUM(s.nb_reponses), 0), 2)
                                          AS score_global_pondere,
       ROUND(SUM(s.taux_recommandation * s.nb_reponses)
             / NULLIF(SUM(CASE WHEN s.taux_recommandation IS NOT NULL THEN s.nb_reponses END), 0), 2)
                                          AS taux_recommandation_pondere
FROM fait_satisfaction s
JOIN dim_etablissement e ON e.id_etablissement = s.id_etablissement
JOIN dim_region        r ON r.id_region = e.id_region
WHERE s.id_date BETWEEN 20200101 AND 20201231
  AND s.score_global IS NOT NULL
  AND s.nb_reponses > 0
GROUP BY r.code_region_insee, r.libelle_region, s.type_enquete
ORDER BY score_global_pondere DESC;
