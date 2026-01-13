-- ============================================================================
-- SAFETWIN X5 - VUES ANALYTIQUES SAFETY GRAPH
-- ============================================================================
-- Base: 2,853,583 records HSE multi-juridictionnels
-- Sources: OSHA USA (1.6M), Eurostat EU-27 (425K), CNESST Québec (794K)
-- Version: 1.0.0
-- Date: 2026-01-13
-- ============================================================================

-- ============================================================================
-- SECTION 1: VUES CNESST QUÉBEC (793,737 records)
-- ============================================================================

-- Vue 1.1: Statistiques par secteur SCIAN et année
CREATE OR REPLACE VIEW safetwin_cnesst_sector_stats AS
SELECT 
    "SECTEUR_SCIAN" as secteur_scian,
    "ANNEE" as annee,
    COUNT(*) as total_lesions,
    SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as lesions_tms,
    SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as lesions_psy,
    SUM(CASE WHEN "IND_LESION_COVID_19" = 'OUI' THEN 1 ELSE 0 END) as lesions_covid,
    SUM(CASE WHEN "IND_LESION_SURDITE" = 'OUI' THEN 1 ELSE 0 END) as lesions_surdite,
    SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) as lesions_machine,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_tms,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_psy
FROM cnesst_lesions_quebec
GROUP BY "SECTEUR_SCIAN", "ANNEE"
ORDER BY "SECTEUR_SCIAN", "ANNEE";

-- Vue 1.2: Top secteurs par volume de lésions (agrégé toutes années)
CREATE OR REPLACE VIEW safetwin_cnesst_top_sectors AS
SELECT 
    "SECTEUR_SCIAN" as secteur_scian,
    COUNT(*) as total_lesions,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total,
    SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as lesions_tms,
    SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as lesions_psy,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_tms,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_psy,
    MIN("ANNEE") as annee_min,
    MAX("ANNEE") as annee_max
FROM cnesst_lesions_quebec
GROUP BY "SECTEUR_SCIAN"
ORDER BY total_lesions DESC;

-- Vue 1.3: Analyse par nature de lésion
CREATE OR REPLACE VIEW safetwin_cnesst_nature_lesion AS
SELECT 
    "NATURE_LESION" as nature_lesion,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total,
    COUNT(DISTINCT "SECTEUR_SCIAN") as nb_secteurs_touches
FROM cnesst_lesions_quebec
GROUP BY "NATURE_LESION"
ORDER BY total DESC
LIMIT 20;

-- Vue 1.4: Analyse par siège de lésion (partie du corps)
CREATE OR REPLACE VIEW safetwin_cnesst_siege_lesion AS
SELECT 
    "SIEGE_LESION" as siege_lesion,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total,
    SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as avec_tms
FROM cnesst_lesions_quebec
GROUP BY "SIEGE_LESION"
ORDER BY total DESC
LIMIT 20;

-- Vue 1.5: Analyse par agent causal
CREATE OR REPLACE VIEW safetwin_cnesst_agent_causal AS
SELECT 
    "AGENT_CAUSAL_LESION" as agent_causal,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total
FROM cnesst_lesions_quebec
GROUP BY "AGENT_CAUSAL_LESION"
ORDER BY total DESC
LIMIT 20;

-- Vue 1.6: Analyse par genre d'accident
CREATE OR REPLACE VIEW safetwin_cnesst_genre_accident AS
SELECT 
    "GENRE" as genre_accident,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total
FROM cnesst_lesions_quebec
GROUP BY "GENRE"
ORDER BY total DESC
LIMIT 20;

-- Vue 1.7: Analyse démographique (sexe et âge)
CREATE OR REPLACE VIEW safetwin_cnesst_demographie AS
SELECT 
    "SEXE_PERS_PHYS" as sexe,
    "GROUPE_AGE" as groupe_age,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total,
    SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as lesions_tms,
    SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as lesions_psy
FROM cnesst_lesions_quebec
GROUP BY "SEXE_PERS_PHYS", "GROUPE_AGE"
ORDER BY total DESC;

-- Vue 1.8: Tendances annuelles globales
CREATE OR REPLACE VIEW safetwin_cnesst_tendances AS
SELECT 
    "ANNEE" as annee,
    COUNT(*) as total_lesions,
    SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) as tms,
    SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) as psy,
    SUM(CASE WHEN "IND_LESION_COVID_19" = 'OUI' THEN 1 ELSE 0 END) as covid,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_tms,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_psy
FROM cnesst_lesions_quebec
GROUP BY "ANNEE"
ORDER BY "ANNEE";

-- Vue 1.9: Profil de risque par secteur (pour SafeTwin enrichment)
CREATE OR REPLACE VIEW safetwin_sector_risk_profile AS
SELECT 
    "SECTEUR_SCIAN" as secteur_scian,
    COUNT(*) as total_lesions,
    
    -- Indicateurs de fréquence
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2) as pct_total_qc,
    
    -- Types de lésions
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as risk_tms_pct,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as risk_psy_pct,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_MACHINE" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as risk_machine_pct,
    ROUND(100.0 * SUM(CASE WHEN "IND_LESION_SURDITE" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2) as risk_surdite_pct,
    
    -- Calcul du score de risque global (0-100)
    ROUND(
        (LOG(COUNT(*) + 1) / LOG((SELECT MAX(cnt) FROM (SELECT COUNT(*) as cnt FROM cnesst_lesions_quebec GROUP BY "SECTEUR_SCIAN") t) + 1)) * 40 +
        (SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0)) * 30 +
        (SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0)) * 30
    , 1) as risk_score,
    
    -- Classification du risque
    CASE 
        WHEN COUNT(*) > 50000 THEN 'CRITIQUE'
        WHEN COUNT(*) > 20000 THEN 'ÉLEVÉ'
        WHEN COUNT(*) > 5000 THEN 'MODÉRÉ'
        ELSE 'FAIBLE'
    END as risk_level
    
FROM cnesst_lesions_quebec
GROUP BY "SECTEUR_SCIAN"
ORDER BY total_lesions DESC;


-- ============================================================================
-- SECTION 2: VUES OSHA USA (1,635,164 records)
-- ============================================================================

-- Vue 2.1: Statistiques par état américain
CREATE OR REPLACE VIEW safetwin_osha_state_stats AS
SELECT 
    state as etat,
    COUNT(*) as total_incidents,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM osha_injuries_raw), 2) as pct_total
FROM osha_injuries_raw
WHERE state IS NOT NULL
GROUP BY state
ORDER BY total_incidents DESC;

-- Vue 2.2: Statistiques par industrie NAICS
CREATE OR REPLACE VIEW safetwin_osha_naics_stats AS
SELECT 
    naics_code,
    industry_description,
    COUNT(*) as total_incidents,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM osha_injuries_raw), 2) as pct_total
FROM osha_injuries_raw
WHERE naics_code IS NOT NULL
GROUP BY naics_code, industry_description
ORDER BY total_incidents DESC
LIMIT 50;


-- ============================================================================
-- SECTION 3: VUES MULTI-JURIDICTIONNELLES (2,853,583 records)
-- ============================================================================

-- Vue 3.1: Résumé global Safety Graph
CREATE OR REPLACE VIEW safetwin_global_summary AS
SELECT 
    'OSHA USA' as source,
    '🇺🇸' as flag,
    (SELECT COUNT(*) FROM osha_injuries_raw) as total_records,
    '2016-2021' as periode,
    'NAICS' as classification
UNION ALL
SELECT 
    'Eurostat EU-27' as source,
    '🇪🇺' as flag,
    (SELECT COUNT(*) FROM eurostat_esaw) as total_records,
    '2010-2022' as periode,
    'NACE' as classification
UNION ALL
SELECT 
    'CNESST Québec' as source,
    '🇨🇦' as flag,
    (SELECT COUNT(*) FROM cnesst_lesions_quebec) as total_records,
    '2017-2023' as periode,
    'SCIAN' as classification;

-- Vue 3.2: Total consolidé
CREATE OR REPLACE VIEW safetwin_total_records AS
SELECT 
    (SELECT COUNT(*) FROM osha_injuries_raw) +
    (SELECT COUNT(*) FROM eurostat_esaw) +
    (SELECT COUNT(*) FROM cnesst_lesions_quebec) as total_global,
    (SELECT COUNT(*) FROM osha_injuries_raw) as osha_usa,
    (SELECT COUNT(*) FROM eurostat_esaw) as eurostat_eu,
    (SELECT COUNT(*) FROM cnesst_lesions_quebec) as cnesst_qc;


-- ============================================================================
-- SECTION 4: FONCTIONS POUR SAFETWIN X5
-- ============================================================================

-- Fonction 4.1: Obtenir le profil de risque d'un secteur SCIAN
CREATE OR REPLACE FUNCTION get_sector_risk_profile(p_secteur TEXT)
RETURNS TABLE (
    secteur TEXT,
    total_lesions BIGINT,
    pct_total NUMERIC,
    risk_tms NUMERIC,
    risk_psy NUMERIC,
    risk_level TEXT,
    top_nature TEXT,
    top_siege TEXT,
    top_agent TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p_secteur,
        COUNT(*)::BIGINT,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cnesst_lesions_quebec), 2),
        ROUND(100.0 * SUM(CASE WHEN "IND_LESION_TMS" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2),
        ROUND(100.0 * SUM(CASE WHEN "IND_LESION_PSY" = 'OUI' THEN 1 ELSE 0 END) / COUNT(*), 2),
        CASE 
            WHEN COUNT(*) > 50000 THEN 'CRITIQUE'
            WHEN COUNT(*) > 20000 THEN 'ÉLEVÉ'
            WHEN COUNT(*) > 5000 THEN 'MODÉRÉ'
            ELSE 'FAIBLE'
        END,
        (SELECT "NATURE_LESION" FROM cnesst_lesions_quebec WHERE "SECTEUR_SCIAN" = p_secteur GROUP BY "NATURE_LESION" ORDER BY COUNT(*) DESC LIMIT 1),
        (SELECT "SIEGE_LESION" FROM cnesst_lesions_quebec WHERE "SECTEUR_SCIAN" = p_secteur GROUP BY "SIEGE_LESION" ORDER BY COUNT(*) DESC LIMIT 1),
        (SELECT "AGENT_CAUSAL_LESION" FROM cnesst_lesions_quebec WHERE "SECTEUR_SCIAN" = p_secteur GROUP BY "AGENT_CAUSAL_LESION" ORDER BY COUNT(*) DESC LIMIT 1)
    FROM cnesst_lesions_quebec
    WHERE "SECTEUR_SCIAN" = p_secteur;
END;
$$ LANGUAGE plpgsql;

-- Fonction 4.2: Comparer un taux avec la moyenne provinciale
CREATE OR REPLACE FUNCTION compare_to_provincial_avg(
    p_secteur TEXT,
    p_indicateur TEXT  -- 'TMS', 'PSY', 'MACHINE', 'SURDITE'
)
RETURNS TABLE (
    secteur TEXT,
    taux_secteur NUMERIC,
    taux_provincial NUMERIC,
    ecart NUMERIC,
    statut TEXT
) AS $$
DECLARE
    v_col TEXT;
BEGIN
    v_col := 'IND_LESION_' || p_indicateur;
    
    RETURN QUERY EXECUTE format('
        WITH secteur_stats AS (
            SELECT 
                ROUND(100.0 * SUM(CASE WHEN %I = ''OUI'' THEN 1 ELSE 0 END) / COUNT(*), 2) as taux
            FROM cnesst_lesions_quebec
            WHERE "SECTEUR_SCIAN" = $1
        ),
        provincial_stats AS (
            SELECT 
                ROUND(100.0 * SUM(CASE WHEN %I = ''OUI'' THEN 1 ELSE 0 END) / COUNT(*), 2) as taux
            FROM cnesst_lesions_quebec
        )
        SELECT 
            $1::TEXT,
            s.taux,
            p.taux,
            ROUND(s.taux - p.taux, 2),
            CASE 
                WHEN s.taux > p.taux * 1.2 THEN ''⚠️ AU-DESSUS (+20%%)''
                WHEN s.taux < p.taux * 0.8 THEN ''✅ EN-DESSOUS (-20%%)''
                ELSE ''➡️ DANS LA MOYENNE''
            END
        FROM secteur_stats s, provincial_stats p
    ', v_col, v_col) USING p_secteur;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- SECTION 5: INDEX POUR PERFORMANCE
-- ============================================================================

-- Index sur les colonnes fréquemment utilisées
CREATE INDEX IF NOT EXISTS idx_cnesst_secteur ON cnesst_lesions_quebec("SECTEUR_SCIAN");
CREATE INDEX IF NOT EXISTS idx_cnesst_annee ON cnesst_lesions_quebec("ANNEE");
CREATE INDEX IF NOT EXISTS idx_cnesst_tms ON cnesst_lesions_quebec("IND_LESION_TMS");
CREATE INDEX IF NOT EXISTS idx_cnesst_psy ON cnesst_lesions_quebec("IND_LESION_PSY");
CREATE INDEX IF NOT EXISTS idx_osha_state ON osha_injuries_raw(state);
CREATE INDEX IF NOT EXISTS idx_osha_naics ON osha_injuries_raw(naics_code);


-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================
-- Pour exécuter: psql -d safety_graph -f safetwin_analytics_views.sql
-- ============================================================================
