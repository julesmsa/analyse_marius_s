-- Analyse suivi du programme de FID --Suivi du programme de fidélité

-- Etape 1 : Nombre de clients Club (Jules Le Club + / Jules Le Club) avec distinction par RFM à fin Mai 

-- Nombre de Clients actifs 3 ans 
-- nombre de clients Club 

SET dtdeb = Date('2023-05-01');
SET dtfin = DAte('2024-05-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb, $dtfin;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.STAT_clt_julclub3ans AS 
WITH tab0 AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, STATUT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT
,datediff(YEAR,DATE_DERNIER_ACHAT,$dtfin) AS ACTIF_CLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE DATE_DERNIER_ACHAT IS NOT NULL AND datediff(MONTH,DATE_DERNIER_ACHAT,$dtfin)<=36)
SELECT * FROM (
(SELECT '00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '01-SEGMENT RFM' AS Typo, CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
     WHEN id_macro_segment = '02' THEN '02_TBC'
     WHEN id_macro_segment = '03' THEN '03_BC'
     WHEN id_macro_segment = '04' THEN '04_MOY'
     WHEN id_macro_segment = '05' THEN '05_TAP'
     WHEN id_macro_segment = '06' THEN '06_TIEDE'
     WHEN id_macro_segment = '07' THEN '07_TPURG'
     WHEN id_macro_segment = '09' THEN '08_NCV'
     WHEN id_macro_segment = '08' THEN '09_NAC'
     WHEN id_macro_segment = '10' THEN '10_INA12'
     WHEN id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)) 
ORDER BY 1,2; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.STAT_clt_julclub3ans ORDER BY 1,2;  


--- Actifs 2 ans 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.STAT_clt_julclub2ans AS 
WITH tab0 AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, STATUT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT
,datediff(YEAR,DATE_DERNIER_ACHAT,$dtfin) AS ACTIF_CLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE DATE_DERNIER_ACHAT IS NOT NULL AND datediff(MONTH,DATE_DERNIER_ACHAT,$dtfin)<=24)
SELECT * FROM (
(SELECT '00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '01-SEGMENT RFM' AS Typo, CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
     WHEN id_macro_segment = '02' THEN '02_TBC'
     WHEN id_macro_segment = '03' THEN '03_BC'
     WHEN id_macro_segment = '04' THEN '04_MOY'
     WHEN id_macro_segment = '05' THEN '05_TAP'
     WHEN id_macro_segment = '06' THEN '06_TIEDE'
     WHEN id_macro_segment = '07' THEN '07_TPURG'
     WHEN id_macro_segment = '09' THEN '08_NCV'
     WHEN id_macro_segment = '08' THEN '09_NAC'
     WHEN id_macro_segment = '10' THEN '10_INA12'
     WHEN id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)) 
ORDER BY 1,2; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.STAT_clt_julclub2ans ORDER BY 1,2; 


-- Actifs 12 Mois 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.STAT_clt_julclub1ans AS 
WITH tab0 AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, STATUT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT
,datediff(YEAR,DATE_DERNIER_ACHAT,$dtfin) AS ACTIF_CLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE DATE_DERNIER_ACHAT IS NOT NULL AND datediff(MONTH,DATE_DERNIER_ACHAT,$dtfin)<=12)
SELECT * FROM (
(SELECT '00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '01-SEGMENT RFM' AS Typo, CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
     WHEN id_macro_segment = '02' THEN '02_TBC'
     WHEN id_macro_segment = '03' THEN '03_BC'
     WHEN id_macro_segment = '04' THEN '04_MOY'
     WHEN id_macro_segment = '05' THEN '05_TAP'
     WHEN id_macro_segment = '06' THEN '06_TIEDE'
     WHEN id_macro_segment = '07' THEN '07_TPURG'
     WHEN id_macro_segment = '09' THEN '08_NCV'
     WHEN id_macro_segment = '08' THEN '09_NAC'
     WHEN id_macro_segment = '10' THEN '10_INA12'
     WHEN id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)) 
ORDER BY 1,2; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.STAT_clt_julclub1ans ORDER BY 1,2; 




 /**** Analyse performance des codes AM sur période  *****/ 

 -- Nouvelles vision avec données en provenance de BO 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.TEMP_COUPON_AM_FID ;


-- utilisation de l'OBT Client 

WITH Tab0 AS (SELECT DISTINCT CODE_CLIENT , DATE_RECRUTEMENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT , Code_coupon , code_type_coupon, code_am, code_status, date_debut_validite, date_fin_validite, 
                dateh_creation_coupon, description_longue, type_magasin,
                description_courte, est_utilise, date_ticket, id_ticket, id_magasin_utilisation, lib_magasin_utilisation, montant_remise, 
                montant_ttc, montant_marge_sortie 
        FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT 
  WHERE ID_NIVEAU = 3 AND code_am IN ('101623','301906','130146','130147','130148','130145') AND YEAR(date_debut_validite)>=2024)
SELECT code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
--,Count(DISTINCT CASE WHEN est_utilise=1 THEN Code_coupon End) AS Nb_coupon_Utilise
--,Count(DISTINCT CASE WHEN code_status='E' THEN Code_coupon End) AS Nb_coupon_Emis
,Count(DISTINCT CASE WHEN code_status='U' THEN Code_coupon End) AS Nb_coupon_Utilise
,Count(DISTINCT CASE WHEN code_status='U' AND type_magasin='WEB' THEN Code_coupon End) AS Nb_coupon_Utilise_web
,Count(DISTINCT CASE WHEN code_status='U' AND type_magasin='MAG' THEN Code_coupon End) AS Nb_coupon_Utilise_mag
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt 
,Count(DISTINCT CASE WHEN id_ticket IS NOT null THEN id_ticket End) AS Nb_ticket
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS SUM_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS SUM_montant_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND type_magasin='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND type_magasin='MAG' then code_client end ) AS nb_newclt_mag
FROM Tab0
GROUP BY 1,2
ORDER BY 1,2;  
  



select * from dhb_prod.acq.stl_historique_caisses_v2 where numticket='14104095' and codemagasin = 13  


select * from dhb_prod.acq.stl_remise_detaillees where numticket='14119057' and codemagasin = 15

select * from dhb_prod.acq.stl_remise_detaillees where numticket='14104095' and codemagasin = 13


SELECT DISTINCT CODE_CLIENT , DATE_RECRUTEMENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT , Code_coupon , code_type_coupon, code_am, code_status, date_debut_validite, date_fin_validite, 
                dateh_creation_coupon, description_longue, type_magasin,
                description_courte, est_utilise, date_ticket, id_ticket, id_magasin_utilisation, lib_magasin_utilisation, montant_remise, 
                montant_ttc, montant_marge_sortie 
        FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT 
  WHERE ID_NIVEAU = 3 AND code_am IN ('130147','130148')
  
  
  
  SELECT * FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE 
  WHERE CODE_AM IN ('130147','130148'); 
 
  SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE 
  WHERE CODE_AM IN ('130147','130148');  
 
 
 
  
   SELECT * FROM DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE  
    WHERE CODE_AM IN ('130147','130148') AND CODE_STATUS='U' ; 
  

   
  SELECT CODE_AM, CODE_STATUS , count(DISTINCT CODE_COUPON) AS nb_coupon 
  FROM DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE  
    WHERE CODE_AM IN ('130147','130148') 
   GROUP BY 1,2
  ORDER BY 1,2;
   
   
   
   
   


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE WHERE code_client='102710008749'; 



SELECT DISTINCT STATUT FROM DATA_MESH_PROD_CLIENT.SHARED.CLIENT_DENORMALISEE WHERE code_client='102710008749'; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.V_OBT_CLIENT; 

SELECT * FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT WHERE ID_NIVEAU = 3 AND est_utilise=1; 

SELECT DISTINCT Code_coupon , code_type_coupon, code_am, utilisation, code_status, date_debut_validite, date_fin_validite, 
                dateh_creation_coupon, description_longue, 
                description_courte, est_utilise, date_ticket, id_ticket, id_magasin_utilisation, lib_magasin_utilisation, montant_remise, 
                montant_ttc, montant_marge_sortie 
        FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT WHERE ID_NIVEAU = 3 ;         

SELECT * FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT WHERE ID_NIVEAU = 1 AND CODE_CLIENT='037310000735'; 

SELECT code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN code_status='E' THEN Code_coupon End) AS Nb_coupon_Emis
,Count(DISTINCT CASE WHEN code_status='U' THEN Code_coupon End) AS Nb_coupon_Utilise
,Count(DISTINCT CASE WHEN id_ticket IS NOT null THEN id_ticket End) AS Nb_coupon_Utilise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS SUM_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS SUM_montant_ttc
FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT WHERE ID_NIVEAU = 3 
GROUP BY 1,2
ORDER BY 1,2;



SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN;












