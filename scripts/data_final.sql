CREATE OR REPLACE VIEW dataset_final AS
SELECT 
    s.siren,
    s.denomination,
    s.naf_code,
    s.effectif,
    s.ville,
    -- Feature : Âge de l'entreprise au moment de l'analyse (2026)
    (2026 - YEAR(s.date_creation)) AS anciennete,
    
    -- Features Macro : Contexte de l'année de création
    m.taux_chomage,
    m.croissance_pib,
    
    -- TARGET : 1 si une procédure BODACC existe, sinon 0
    CASE 
        WHEN b.siren IS NOT NULL THEN 1 
        ELSE 0 
    END AS target_defaut

FROM sirene s
-- Jointure avec la macro (basée sur l'année de création pour voir le contexte de départ)
LEFT JOIN macro_regional m ON YEAR(s.date_creation) = m.annee
-- Jointure avec le BODACC pour identifier les défauts
LEFT JOIN (
    SELECT DISTINCT siren FROM bodacc_procedures
) b ON s.siren = b.siren;