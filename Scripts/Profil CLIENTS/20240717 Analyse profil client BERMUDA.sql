--- Analyse du changement de stratégie de prix

-- Parametre des dates 
SET dtdeb = Date('2024-01-01');
SET dtfin = DAte('2024-06-30');
-- SET tag_etud='Bermuda';


SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT * FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE;
--SELECT DISTINCT LIB_FAMILLE_ACHAT FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE;
    
-- Table identification des clients de l'études 
-- Dans ce cas, nous allons designé LES CLIENTS ayant acheté le produit correspondant ! 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.IDCLT_ETUDE AS
SELECT DISTINCT CODE_CLIENT AS Id_Client, LIB_FAMILLE_ACHAT AS ref_achat, 1 AS ref_top_achat
from DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE vd
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
AND LIB_FAMILLE_ACHAT IN ('Bermuda') -- listes des familles a analyse 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2);

 /*  -- analyse par REFCO produits si liste REF_CO est disponible dans une table 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.IDCLT_ETUDE AS
SELECT DISTINCT CODE_CLIENT AS Id_Client, LIB_FAMILLE_ACHAT AS ref_achat, 1 AS ref_top_achat
from DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE vd
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
AND vd.CODE_REFERENCE IN (SELECT DISTINCT CODE_REFERENCE FROM TAB_REFCO) -- listes des familles a analyse 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2);
 */
 
-- table des informations Client sur la période d'étude 
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS AS
WITH info_clt AS ( SELECT DISTINCT Code_client AS idclient, id_titre, date_naissance, age, gender, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt, date_recrutement
FROM DATA_MESH_PROD_client.SHARED.T_CLIENT_DENORMALISEE WHERE (code_pays = $PAYS1 or code_pays = $PAYS2) AND date_suppression_client IS NULL ),
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT WHERE id_niveau=5 AND DATE_PARTITION=DATE_FROM_PARTS(YEAR($dtfin) , MONTH($dtfin), 1)),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
id.ref_achat, 
id.ref_top_achat, 
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign
from DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE vd
LEFT join DATA_MESH_PROD_CLIENT.WORK.IDCLT_ETUDE id ON vd.CODE_CLIENT=id.ID_CLIENT
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
SELECT a.*, b.*, DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI
,datediff(MONTH ,date_recrutement,$dtfin) AS ANCIENNETE_CLIENT
,CASE WHEN Date(date_recrutement) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN '02-Nouveaux' ELSE '01-Anciens' END AS Type_client
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin),2) AS AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
	 , CASE WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('75','77','78','91','92','93','94','95')             then  '01_Ile de France' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('02', '59', '60', '62', '80')                                      then  '02_Hauts-de-France'
              WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('18', '28', '36', '37', '41', '45' )       						then  '03_Centre-Val de Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('14', '27', '50', '61', '76')                                      then  '04_Normandie'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('44', '49', '53', '72', '85')                                      then  '05_Pays de la Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('22','29','35','56')       						then  '06_Bretagne'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87')                                      then  '07_Nouvelle-Aquitaine'	
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('08', '10', '51', '52', '54', '55', '57', '67', '68', '88')					then  '08_Grand Est'
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('01','03','07','15','26','38','42','43','63','69','73','74' ) then  '09_Auvergne-Rhone-Alpes' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('21', '25', '39','58','70','71', '89', '90' ) then  '10_Bourgogne-Franche-Comte' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82' )                                      then  '11_Occitanie' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('04', '05', '06', '13', '83', '84')                                      then  '12_Provence-Alpes-Cote-d-Azur' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('20')											then  '13_Corse' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 3) in ('971','972','973','974','975','976','986','987','988') then  '14_Outre-mer' 
			WHEN pays_clt='FRA' AND code_postal = '98000' 	then  '15_Monaco'
			WHEN pays_clt='BEL'  	then  '20_BELGIQUE'
			ELSE '99_AUTRES/NC' END AS REGION -- a completer avec les informations de la BEL     
,CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM     
,CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM
,CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete, 
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2
FROM tickets a
INNER JOIN info_clt b ON a.CODE_CLIENT=b.idclient
LEFT JOIN tabtg SEG ON a.CODE_CLIENT=seg.CODE_CLIENT;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ AS
WITH DD_VLZ AS (SELECT DISTINCT 
CODE_CLIENT, CROSSCANAL_12_24_MARKET,CROSSCANAL_12_MARKET,CROSSCANAL_24_36_MARKET,CROSSCANAL_24_MARKET,CROSSCANAL_36_MARKET,PRICE_SENSITIVITY_MARKET,PROMO_SENSITIVITY_MARKET, 
AGE,ANIMAL_OWNER,CARREAU,CAT_OWNER,CROSSCANAL_12,CROSSCANAL_12_24,CROSSCANAL_24,CROSSCANAL_24_36,CROSSCANAL_36,CSR_ENVIRONMENT,CSR_HEALTHY,CSR_ORGANIC,DEDUP,DOG_OWNER,FAMILY_MATURITY, 
FAMILY_TOP_AGE,FAMILY_TOP_CHILD,FAMILY_TOP_COUPLE,FAMILY_TOP_INFANT,FAMILY_TOP_MAX_CHILD,FAMILY_TOP_NEWBORN,FAMILY_TOP_PRIMARY_SCHOOL,FAMILY_TOP_TEENAGER,GENDER,HOME_DELIVERY,HOUSEHOLD_TOP_AGE, 
HOUSEHOLD_TOP_AGE_TYPE,HOUSEHOLD_TOP_CHILD,HOUSEHOLD_TOP_COUPLE,HOUSEHOLD_TOP_INFANT,HOUSEHOLD_TOP_KIDS,HOUSEHOLD_TOP_MATURITY,HOUSEHOLD_TOP_MATURITY_TYPE,HOUSEHOLD_TOP_MAX_CHILD,HOUSEHOLD_TOP_NEWBORN, 
HOUSEHOLD_TOP_PRIMARYSCHOOL, HOUSEHOLD_TOP_TEENAGER,HOUSING_SCORE,IRIS,IS_B2B,LIFESTYLE_SEGMENT,MAG_FAVORITEDAY1,MAG_FAVORITEDAY2,MAG_FAVORITEDAY3,MAG_FAVORITEHOUR1,MAG_FAVORITEHOUR2,MAG_FAVORITEHOUR3, 
OPTIN_MAIL_VLZ, OPTIN_PRINT_VLZ,OPTIN_PUSH_APP_VLZ,OPTIN_SMS_VLZ,OPTIN_TEL_VLZ,OTHER_ANIMAL_OWNER,PRICE_SENSITIVITY,PROMO_SENSITIVITY,RECTANGLE,RELOCATION,RIP_SCORE,SECOND_HAND,SIRET_ENRICHED, 
SQUARE_SID,STORE_ACTIVITY,TOP_GARDEN,TOP_KIDS,TOP_OWNER,ZONE_ACTIVITY_MAG,ZONE_ACTIVITY_STATUS 
FROM DATA_MESH_PROD_client.SHARED.T_OBT_SCORE_VALIUZ WHERE id_niveau=2), 
DD_JULES AS (SELECT DISTINCT 
CODE_CLIENT, CROSSCANAL_12_24_MARKET,CROSSCANAL_12_MARKET,CROSSCANAL_24_36_MARKET,CROSSCANAL_24_MARKET,CROSSCANAL_36_MARKET,PRICE_SENSITIVITY_MARKET,PROMO_SENSITIVITY_MARKET, 
AGE,ANIMAL_OWNER,CARREAU,CAT_OWNER,CROSSCANAL_12,CROSSCANAL_12_24,CROSSCANAL_24,CROSSCANAL_24_36,CROSSCANAL_36,CSR_ENVIRONMENT,CSR_HEALTHY,CSR_ORGANIC,DEDUP,DOG_OWNER,FAMILY_MATURITY, 
FAMILY_TOP_AGE,FAMILY_TOP_CHILD,FAMILY_TOP_COUPLE,FAMILY_TOP_INFANT,FAMILY_TOP_MAX_CHILD,FAMILY_TOP_NEWBORN,FAMILY_TOP_PRIMARY_SCHOOL,FAMILY_TOP_TEENAGER,GENDER,HOME_DELIVERY,HOUSEHOLD_TOP_AGE, 
HOUSEHOLD_TOP_AGE_TYPE,HOUSEHOLD_TOP_CHILD,HOUSEHOLD_TOP_COUPLE,HOUSEHOLD_TOP_INFANT,HOUSEHOLD_TOP_KIDS,HOUSEHOLD_TOP_MATURITY,HOUSEHOLD_TOP_MATURITY_TYPE,HOUSEHOLD_TOP_MAX_CHILD,HOUSEHOLD_TOP_NEWBORN, 
HOUSEHOLD_TOP_PRIMARYSCHOOL, HOUSEHOLD_TOP_TEENAGER,HOUSING_SCORE,IRIS,IS_B2B,LIFESTYLE_SEGMENT,MAG_FAVORITEDAY1,MAG_FAVORITEDAY2,MAG_FAVORITEDAY3,MAG_FAVORITEHOUR1,MAG_FAVORITEHOUR2,MAG_FAVORITEHOUR3, 
OPTIN_MAIL_VLZ, OPTIN_PRINT_VLZ,OPTIN_PUSH_APP_VLZ,OPTIN_SMS_VLZ,OPTIN_TEL_VLZ,OTHER_ANIMAL_OWNER,PRICE_SENSITIVITY,PROMO_SENSITIVITY,RECTANGLE,RELOCATION,RIP_SCORE,SECOND_HAND,SIRET_ENRICHED, 
SQUARE_SID,STORE_ACTIVITY,TOP_GARDEN,TOP_KIDS,TOP_OWNER,ZONE_ACTIVITY_MAG,ZONE_ACTIVITY_STATUS 
FROM DATA_MESH_PROD_client.SHARED.T_OBT_SCORE_VALIUZ WHERE id_niveau=1), 
DD_MKT AS (SELECT DISTINCT 
CODE_CLIENT, CROSSCANAL_12_24_MARKET,CROSSCANAL_12_MARKET,CROSSCANAL_24_36_MARKET,CROSSCANAL_24_MARKET,CROSSCANAL_36_MARKET,PRICE_SENSITIVITY_MARKET,PROMO_SENSITIVITY_MARKET, 
AGE,ANIMAL_OWNER,CARREAU,CAT_OWNER,CROSSCANAL_12,CROSSCANAL_12_24,CROSSCANAL_24,CROSSCANAL_24_36,CROSSCANAL_36,CSR_ENVIRONMENT,CSR_HEALTHY,CSR_ORGANIC,DEDUP,DOG_OWNER,FAMILY_MATURITY, 
FAMILY_TOP_AGE,FAMILY_TOP_CHILD,FAMILY_TOP_COUPLE,FAMILY_TOP_INFANT,FAMILY_TOP_MAX_CHILD,FAMILY_TOP_NEWBORN,FAMILY_TOP_PRIMARY_SCHOOL,FAMILY_TOP_TEENAGER,GENDER,HOME_DELIVERY,HOUSEHOLD_TOP_AGE, 
HOUSEHOLD_TOP_AGE_TYPE,HOUSEHOLD_TOP_CHILD,HOUSEHOLD_TOP_COUPLE,HOUSEHOLD_TOP_INFANT,HOUSEHOLD_TOP_KIDS,HOUSEHOLD_TOP_MATURITY,HOUSEHOLD_TOP_MATURITY_TYPE,HOUSEHOLD_TOP_MAX_CHILD,HOUSEHOLD_TOP_NEWBORN, 
HOUSEHOLD_TOP_PRIMARYSCHOOL, HOUSEHOLD_TOP_TEENAGER,HOUSING_SCORE,IRIS,IS_B2B,LIFESTYLE_SEGMENT,MAG_FAVORITEDAY1,MAG_FAVORITEDAY2,MAG_FAVORITEDAY3,MAG_FAVORITEHOUR1,MAG_FAVORITEHOUR2,MAG_FAVORITEHOUR3, 
OPTIN_MAIL_VLZ, OPTIN_PRINT_VLZ,OPTIN_PUSH_APP_VLZ,OPTIN_SMS_VLZ,OPTIN_TEL_VLZ,OTHER_ANIMAL_OWNER,PRICE_SENSITIVITY,PROMO_SENSITIVITY,RECTANGLE,RELOCATION,RIP_SCORE,SECOND_HAND,SIRET_ENRICHED, 
SQUARE_SID,STORE_ACTIVITY,TOP_GARDEN,TOP_KIDS,TOP_OWNER,ZONE_ACTIVITY_MAG,ZONE_ACTIVITY_STATUS 
FROM DATA_MESH_PROD_client.SHARED.T_OBT_SCORE_VALIUZ WHERE id_niveau=3),
Synth_vlz AS (SELECT DISTINCT 
COALESCE(a.CODE_CLIENT, b.CODE_CLIENT, c.CODE_CLIENT) as  ID_CLIENT, 
COALESCE(a.CROSSCANAL_12_24_MARKET, b.CROSSCANAL_12_24_MARKET, c.CROSSCANAL_12_24_MARKET) as CROSSCANAL_12_24_MARKET, 
COALESCE(a.CROSSCANAL_12_MARKET, b.CROSSCANAL_12_MARKET, c.CROSSCANAL_12_MARKET) as CROSSCANAL_12_MARKET, 
COALESCE(a.CROSSCANAL_24_36_MARKET, b.CROSSCANAL_24_36_MARKET, c.CROSSCANAL_24_36_MARKET) as CROSSCANAL_24_36_MARKET, 
COALESCE(a.CROSSCANAL_24_MARKET, b.CROSSCANAL_24_MARKET, c.CROSSCANAL_24_MARKET) as CROSSCANAL_24_MARKET, 
COALESCE(a.CROSSCANAL_36_MARKET, b.CROSSCANAL_36_MARKET, c.CROSSCANAL_36_MARKET) as CROSSCANAL_36_MARKET, 
COALESCE(a.PRICE_SENSITIVITY_MARKET, b.PRICE_SENSITIVITY_MARKET, c.PRICE_SENSITIVITY_MARKET) as PRICE_SENSITIVITY_MARKET, 
COALESCE(a.PROMO_SENSITIVITY_MARKET, b.PROMO_SENSITIVITY_MARKET, c.PROMO_SENSITIVITY_MARKET) as PROMO_SENSITIVITY_MARKET, 
COALESCE(a.AGE, b.AGE, c.AGE) as AGE_VLZ, 
COALESCE(a.ANIMAL_OWNER, b.ANIMAL_OWNER, c.ANIMAL_OWNER) as ANIMAL_OWNER, 
COALESCE(a.CARREAU, b.CARREAU, c.CARREAU) as CARREAU, 
COALESCE(a.CAT_OWNER, b.CAT_OWNER, c.CAT_OWNER) as CAT_OWNER, 
COALESCE(a.CROSSCANAL_12, b.CROSSCANAL_12, c.CROSSCANAL_12) as CROSSCANAL_12, 
COALESCE(a.CROSSCANAL_12_24, b.CROSSCANAL_12_24, c.CROSSCANAL_12_24) as CROSSCANAL_12_24, 
COALESCE(a.CROSSCANAL_24, b.CROSSCANAL_24, c.CROSSCANAL_24) as CROSSCANAL_24, 
COALESCE(a.CROSSCANAL_24_36, b.CROSSCANAL_24_36, c.CROSSCANAL_24_36) as CROSSCANAL_24_36, 
COALESCE(a.CROSSCANAL_36, b.CROSSCANAL_36, c.CROSSCANAL_36) as CROSSCANAL_36, 
COALESCE(a.CSR_ENVIRONMENT, b.CSR_ENVIRONMENT, c.CSR_ENVIRONMENT) as CSR_ENVIRONMENT, 
COALESCE(a.CSR_HEALTHY, b.CSR_HEALTHY, c.CSR_HEALTHY) as CSR_HEALTHY, 
COALESCE(a.CSR_ORGANIC, b.CSR_ORGANIC, c.CSR_ORGANIC) as CSR_ORGANIC, 
COALESCE(a.DEDUP, b.DEDUP, c.DEDUP) as DEDUP, 
COALESCE(a.DOG_OWNER, b.DOG_OWNER, c.DOG_OWNER) as DOG_OWNER, 
COALESCE(a.FAMILY_MATURITY, b.FAMILY_MATURITY, c.FAMILY_MATURITY) as FAMILY_MATURITY, 
COALESCE(a.FAMILY_TOP_AGE, b.FAMILY_TOP_AGE, c.FAMILY_TOP_AGE) as FAMILY_TOP_AGE, 
COALESCE(a.FAMILY_TOP_CHILD, b.FAMILY_TOP_CHILD, c.FAMILY_TOP_CHILD) as FAMILY_TOP_CHILD, 
COALESCE(a.FAMILY_TOP_COUPLE, b.FAMILY_TOP_COUPLE, c.FAMILY_TOP_COUPLE) as FAMILY_TOP_COUPLE, 
COALESCE(a.FAMILY_TOP_INFANT, b.FAMILY_TOP_INFANT, c.FAMILY_TOP_INFANT) as FAMILY_TOP_INFANT, 
COALESCE(a.FAMILY_TOP_MAX_CHILD, b.FAMILY_TOP_MAX_CHILD, c.FAMILY_TOP_MAX_CHILD) as FAMILY_TOP_MAX_CHILD, 
COALESCE(a.FAMILY_TOP_NEWBORN, b.FAMILY_TOP_NEWBORN, c.FAMILY_TOP_NEWBORN) as FAMILY_TOP_NEWBORN, 
COALESCE(a.FAMILY_TOP_PRIMARY_SCHOOL, b.FAMILY_TOP_PRIMARY_SCHOOL, c.FAMILY_TOP_PRIMARY_SCHOOL) as FAMILY_TOP_PRIMARY_SCHOOL, 
COALESCE(a.FAMILY_TOP_TEENAGER, b.FAMILY_TOP_TEENAGER, c.FAMILY_TOP_TEENAGER) as FAMILY_TOP_TEENAGER, 
COALESCE(a.GENDER, b.GENDER, c.GENDER) as GENDER_VLZ, 
COALESCE(a.HOME_DELIVERY, b.HOME_DELIVERY, c.HOME_DELIVERY) as HOME_DELIVERY, 
COALESCE(a.HOUSEHOLD_TOP_AGE, b.HOUSEHOLD_TOP_AGE, c.HOUSEHOLD_TOP_AGE) as HOUSEHOLD_TOP_AGE, 
COALESCE(a.HOUSEHOLD_TOP_AGE_TYPE, b.HOUSEHOLD_TOP_AGE_TYPE, c.HOUSEHOLD_TOP_AGE_TYPE) as HOUSEHOLD_TOP_AGE_TYPE, 
COALESCE(a.HOUSEHOLD_TOP_CHILD, b.HOUSEHOLD_TOP_CHILD, c.HOUSEHOLD_TOP_CHILD) as HOUSEHOLD_TOP_CHILD, 
COALESCE(a.HOUSEHOLD_TOP_COUPLE, b.HOUSEHOLD_TOP_COUPLE, c.HOUSEHOLD_TOP_COUPLE) as HOUSEHOLD_TOP_COUPLE, 
COALESCE(a.HOUSEHOLD_TOP_INFANT, b.HOUSEHOLD_TOP_INFANT, c.HOUSEHOLD_TOP_INFANT) as HOUSEHOLD_TOP_INFANT, 
COALESCE(a.HOUSEHOLD_TOP_KIDS, b.HOUSEHOLD_TOP_KIDS, c.HOUSEHOLD_TOP_KIDS) as HOUSEHOLD_TOP_KIDS, 
COALESCE(a.HOUSEHOLD_TOP_MATURITY, b.HOUSEHOLD_TOP_MATURITY, c.HOUSEHOLD_TOP_MATURITY) as HOUSEHOLD_TOP_MATURITY, 
COALESCE(a.HOUSEHOLD_TOP_MATURITY_TYPE, b.HOUSEHOLD_TOP_MATURITY_TYPE, c.HOUSEHOLD_TOP_MATURITY_TYPE) as HOUSEHOLD_TOP_MATURITY_TYPE, 
COALESCE(a.HOUSEHOLD_TOP_MAX_CHILD, b.HOUSEHOLD_TOP_MAX_CHILD, c.HOUSEHOLD_TOP_MAX_CHILD) as HOUSEHOLD_TOP_MAX_CHILD, 
COALESCE(a.HOUSEHOLD_TOP_NEWBORN, b.HOUSEHOLD_TOP_NEWBORN, c.HOUSEHOLD_TOP_NEWBORN) as HOUSEHOLD_TOP_NEWBORN, 
COALESCE(a.HOUSEHOLD_TOP_PRIMARYSCHOOL, b.HOUSEHOLD_TOP_PRIMARYSCHOOL, c.HOUSEHOLD_TOP_PRIMARYSCHOOL) as HOUSEHOLD_TOP_PRIMARYSCHOOL, 
COALESCE(a.HOUSEHOLD_TOP_TEENAGER, b.HOUSEHOLD_TOP_TEENAGER, c.HOUSEHOLD_TOP_TEENAGER) as HOUSEHOLD_TOP_TEENAGER, 
COALESCE(a.HOUSING_SCORE, b.HOUSING_SCORE, c.HOUSING_SCORE) as HOUSING_SCORE, 
COALESCE(a.IRIS, b.IRIS, c.IRIS) as IRIS, 
COALESCE(a.IS_B2B, b.IS_B2B, c.IS_B2B) as IS_B2B, 
COALESCE(a.LIFESTYLE_SEGMENT, b.LIFESTYLE_SEGMENT, c.LIFESTYLE_SEGMENT) as LIFESTYLE_SEGMENT, 
COALESCE(a.MAG_FAVORITEDAY1, b.MAG_FAVORITEDAY1, c.MAG_FAVORITEDAY1) as MAG_FAVORITEDAY1, 
COALESCE(a.MAG_FAVORITEDAY2, b.MAG_FAVORITEDAY2, c.MAG_FAVORITEDAY2) as MAG_FAVORITEDAY2, 
COALESCE(a.MAG_FAVORITEDAY3, b.MAG_FAVORITEDAY3, c.MAG_FAVORITEDAY3) as MAG_FAVORITEDAY3, 
COALESCE(a.MAG_FAVORITEHOUR1, b.MAG_FAVORITEHOUR1, c.MAG_FAVORITEHOUR1) as MAG_FAVORITEHOUR1, 
COALESCE(a.MAG_FAVORITEHOUR2, b.MAG_FAVORITEHOUR2, c.MAG_FAVORITEHOUR2) as MAG_FAVORITEHOUR2, 
COALESCE(a.MAG_FAVORITEHOUR3, b.MAG_FAVORITEHOUR3, c.MAG_FAVORITEHOUR3) as MAG_FAVORITEHOUR3, 
COALESCE(a.OPTIN_MAIL_VLZ, b.OPTIN_MAIL_VLZ, c.OPTIN_MAIL_VLZ) as OPTIN_MAIL_VLZ, 
COALESCE(a.OPTIN_PRINT_VLZ, b.OPTIN_PRINT_VLZ, c.OPTIN_PRINT_VLZ) as OPTIN_PRINT_VLZ, 
COALESCE(a.OPTIN_PUSH_APP_VLZ, b.OPTIN_PUSH_APP_VLZ, c.OPTIN_PUSH_APP_VLZ) as OPTIN_PUSH_APP_VLZ, 
COALESCE(a.OPTIN_SMS_VLZ, b.OPTIN_SMS_VLZ, c.OPTIN_SMS_VLZ) as OPTIN_SMS_VLZ, 
COALESCE(a.OPTIN_TEL_VLZ, b.OPTIN_TEL_VLZ, c.OPTIN_TEL_VLZ) as OPTIN_TEL_VLZ, 
COALESCE(a.OTHER_ANIMAL_OWNER, b.OTHER_ANIMAL_OWNER, c.OTHER_ANIMAL_OWNER) as OTHER_ANIMAL_OWNER, 
COALESCE(c.PRICE_SENSITIVITY, b.PRICE_SENSITIVITY, a.PRICE_SENSITIVITY) as PRICE_SENSITIVITY, -- ON privilégie le marché, l'espace Jules puis valiuz
COALESCE(c.PROMO_SENSITIVITY, b.PROMO_SENSITIVITY, a.PROMO_SENSITIVITY) as PROMO_SENSITIVITY, 
COALESCE(a.RECTANGLE, b.RECTANGLE, c.RECTANGLE) as RECTANGLE, 
COALESCE(a.RELOCATION, b.RELOCATION, c.RELOCATION) as RELOCATION, 
COALESCE(a.RIP_SCORE, b.RIP_SCORE, c.RIP_SCORE) as RIP_SCORE, 
COALESCE(a.SECOND_HAND, b.SECOND_HAND, c.SECOND_HAND) as SECOND_HAND, 
COALESCE(a.SIRET_ENRICHED, b.SIRET_ENRICHED, c.SIRET_ENRICHED) as SIRET_ENRICHED, 
COALESCE(a.SQUARE_SID, b.SQUARE_SID, c.SQUARE_SID) as SQUARE_SID, 
COALESCE(a.STORE_ACTIVITY, b.STORE_ACTIVITY, c.STORE_ACTIVITY) as STORE_ACTIVITY, 
COALESCE(a.TOP_GARDEN, b.TOP_GARDEN, c.TOP_GARDEN) as TOP_GARDEN, 
COALESCE(a.TOP_KIDS, b.TOP_KIDS, c.TOP_KIDS) as TOP_KIDS, 
COALESCE(a.TOP_OWNER, b.TOP_OWNER, c.TOP_OWNER) as TOP_OWNER, 
COALESCE(a.ZONE_ACTIVITY_MAG, b.ZONE_ACTIVITY_MAG, c.ZONE_ACTIVITY_MAG) as ZONE_ACTIVITY_MAG, 
COALESCE(a.ZONE_ACTIVITY_STATUS, b.ZONE_ACTIVITY_STATUS, c.ZONE_ACTIVITY_STATUS) as ZONE_ACTIVITY_STATUS
FROM DD_VLZ a 
FULL JOIN DD_JULES b ON a.CODE_CLIENT=b.CODE_CLIENT
FULL JOIN DD_MKT c ON a.CODE_CLIENT=c.CODE_CLIENT )
SELECT *
FROM Synth_vlz ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ LIMIT 10; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2 AS
SELECT a.*, AGE_VLZ, GENDER_VLZ,
case when SUBSTRING(SQUARE_SID, 1, 3) in ('157','158','159','160','161','162','163','164','165','166') THEN '01-PARISIENS'
     when SUBSTRING(SQUARE_SID, 1, 3) in ('167','168','169','170','171','172','173','174','175','176') THEN '02-URBAINS'
     when SUBSTRING(SQUARE_SID, 1, 3) in ('177','178','179','180') THEN '03-PERIURBAINS'
     when SUBSTRING(SQUARE_SID, 1, 3) in ('181','182','183') THEN '04-RURAUX'
     else '99-NON RENSEIGNE' END as SQUARE_SID_V2,
Case when STORE_ACTIVITY='STORE_ACTIVE' then '01-ACTIFS' 
     when STORE_ACTIVITY='STORE_LEAVER_NEARBY' then '02-ABANDONNISTES PROCHES' 
     when STORE_ACTIVITY='STORE_LEAVER' then '03-ABANDONNISTES' 
     when STORE_ACTIVITY='STORE_OUT_OF_SCOPE' then '04-ABANDONNISTES HORS SCOPE' 
     when STORE_ACTIVITY='STORE_INACTIVE' then '05-INACTIFS' 
     else '99-NON RENSEIGNE' END as STORE_ACTIVITY_V2,
Case when ZONE_ACTIVITY_STATUS='LOYAL' then '01-LOYAL' 
     when ZONE_ACTIVITY_STATUS='NEW_ON_ZONE' then '02-NOUVEAU SUR ZONE' 
     when ZONE_ACTIVITY_STATUS='LEAVER' then '03-ABANDONNISTES' 
     when ZONE_ACTIVITY_STATUS='OUT_OF_SCOPE' then '04- HORS ZONE' 
     when ZONE_ACTIVITY_STATUS='INACTIVE' then '05-INACTIFS' 
      else '99-NON RENSEIGNE' END as ZONE_ACTIVITY_STATUS_V2,
case when HOUSEHOLD_TOP_MATURITY='1' then '01-Jeune célibataire'
     when HOUSEHOLD_TOP_MATURITY='2' then '02-Jeune couple'
     when HOUSEHOLD_TOP_MATURITY='3' then '03-Famille avec bébés'
     when HOUSEHOLD_TOP_MATURITY='4' then '04-Famille avec jeunes enfants'
     when HOUSEHOLD_TOP_MATURITY='5' then '05-Famille avec adolescents'
     when HOUSEHOLD_TOP_MATURITY='6' then '06-Couple adulte'
     when HOUSEHOLD_TOP_MATURITY='7' then '07-Couple senior'
     when HOUSEHOLD_TOP_MATURITY='8' then '08-Grand-parent'
     when HOUSEHOLD_TOP_MATURITY='9' then '09-Senior célibataire'
     ELSE '99-NON RENSEIGNE' END AS FAMILY_MATURITY_V2,      
case when price_sensitivity is null then '99-NON RENSEIGNE'
else price_sensitivity end as price_V2, 
case when promo_sensitivity='INSENSIBLE' then '01-INSENSIBLE'
     when promo_sensitivity='PROMOPHOBE' then '02-PROMOPHOBE'
     when promo_sensitivity='PROMOSENSIBLE' then '03-PROMOSENSIBLE'
     when promo_sensitivity='OPPORTUNISTE' then '04-OPPORTUNISTE'
     when promo_sensitivity='PROMOPHILE' then '05-PROMOPHILE'
     ELSE '99-NON RENSEIGNE' END AS promo_V2, 
case when lifestyle_segment is null or lifestyle_segment IN ('',' ','-1','V') then 'Z-NON RENSEIGNE'
else lifestyle_segment end as lifestyle_segment_V2, 
case when CROSSCANAL_12 is null OR CROSSCANAL_12 IN ('',' ','-1','V') then 'Z-NON RENSEIGNE'
else CROSSCANAL_12 end as CROSSCANAL_12_V2, 
case when CROSSCANAL_12_24 is null OR CROSSCANAL_12_24 IN ('',' ','-1','V') then 'Z-NON RENSEIGNE'
else CROSSCANAL_12_24 end as CROSSCANAL_12_24_V2, 
case when CROSSCANAL_24 is null OR CROSSCANAL_24 IN ('',' ','-1','V') then 'Z-NON RENSEIGNE'
else CROSSCANAL_24 end as CROSSCANAL_24_V2, 
case when CROSSCANAL_24_36 is null OR CROSSCANAL_24_36 IN ('',' ','-1','V') then 'Z-NON RENSEIGNE'
else CROSSCANAL_24_36 end as CROSSCANAL_24_36_V2, 
case when CROSSCANAL_36 is null OR CROSSCANAL_36 IN ('',' ','-1','V') then 'Z-NON RENSEIGNE'
else CROSSCANAL_36 end as CROSSCANAL_36_V2,
case when CSR_ENVIRONMENT is null or CSR_ENVIRONMENT IN ('',' ','-1','V') then '99-NON RENSEIGNE'
else CSR_ENVIRONMENT end as CSR_ENVIRONMENT_V2, 
case when CSR_HEALTHY is null or CSR_HEALTHY IN ('',' ','-1','V') then '99-NON RENSEIGNE'
else CSR_HEALTHY end as CSR_HEALTHY_V2, 
case when CSR_ORGANIC is null or CSR_ORGANIC IN ('',' ','-1','V') then '99-NON RENSEIGNE'
else CSR_ORGANIC end as CSR_ORGANIC_V2
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ b ON a.CODE_CLIENT=b.ID_CLIENT ;


-- Calcul des indicateurs de performances Clients 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS AS
SELECT *
FROM ( 
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '04_SEGMENT_RFM' AS typo_clt,  SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '05_OMNICANALITE' AS typo_clt, 
 CASE WHEN lib_segment_omni ='MAG' then '01-MAG' 
      When lib_segment_omni ='WEB' then '02-WEB'
      When lib_segment_omni ='OMNI' then '03-OMNI' ELSE '99-NON RENSEIGNE' end as modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '06_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '07A_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen    
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION 
SELECT '07B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS nb_clt_ref
    ,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS nb_ticket_ref
    ,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS CA_ref
	,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS qte_achete_ref
    ,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS Marge_ref
    ,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS Mnt_remise_ref 
    ,AVG(CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then anciennete_client end) AS nb_newclt_ref   
    ,AVG(anciennete_client) AS nb_clt_glb
    ,AVG(anciennete_client) AS nb_ticket_glb
    ,AVG(anciennete_client) AS CA_glb
	,AVG(anciennete_client) AS qte_achete_glb
    ,AVG(anciennete_client) AS Marge_glb
    ,AVG(anciennete_client) AS Mnt_remise_glb    
    ,AVG(CASE WHEN Date(date_recrutement)=Date(date_ticket) then anciennete_client end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 THEN anciennete_client END) AS age_moyen_ref   
    ,AVG(anciennete_client) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '08A_AGE' AS typo_clt,  CLASSE_AGE AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '08B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_clt_ref
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_ticket_ref
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS CA_ref
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS qte_achete_ref
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Marge_ref
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Mnt_remise_ref 
    ,AVG(CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 end) AS nb_newclt_ref   
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_clt_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_ticket_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS CA_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS qte_achete_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Marge_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Mnt_remise_glb    
    ,AVG(CASE WHEN Date(date_recrutement)=Date(date_ticket) AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '09_OPTIN_SMS' AS typo_clt,  CASE WHEN est_optin_sms_com=1 or est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '10_OPTIN_EMAIL' AS typo_clt,  CASE WHEN est_optin_email_com=1 or est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '11_SQUARE_SID' AS typo_clt,  SQUARE_SID_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '12_STORE_ACTIVITY' AS typo_clt,  STORE_ACTIVITY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '14_FAMILY_MATURITY_V2' AS typo_clt,  FAMILY_MATURITY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '15_PRICE_V2' AS typo_clt,  PRICE_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '16_PROMO_V2' AS typo_clt,  PROMO_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '17_LIFESTYLE_SEGMENT_V2' AS typo_clt,  LIFESTYLE_SEGMENT_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '18_CROSSCANAL_12_V2' AS typo_clt,  CROSSCANAL_12_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen    
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '19_CSR_ENVIRONMENT_V2' AS typo_clt,  CSR_ENVIRONMENT_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '20_CSR_HEALTHY_V2' AS typo_clt,  CSR_HEALTHY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '21_CSR_ORGANIC_V2' AS typo_clt,  CSR_ORGANIC_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen    
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '22_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '23_FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2 
UNION
SELECT '24_REGION' AS typo_clt,  REGION AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN CODE_CLIENT END) AS nb_clt_ref
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 THEN id_ticket END) AS nb_ticket_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_TTC END) AS CA_ref
	,SUM(CASE WHEN ref_top_achat=1 THEN QUANTITE_LIGNE END) AS qte_achete_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN MONTANT_MARGE_SORTIE END) AS Marge_ref
    ,SUM(CASE WHEN ref_top_achat=1 THEN montant_remise END) AS Mnt_remise_ref 
    ,COUNT(DISTINCT CASE WHEN ref_top_achat=1 AND Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt_ref
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2 ) ORDER BY 1,2 ;   
   
   SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS ORDER BY 1,2 ;
  
  -- Calcul des indicateurs
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL AS  
  WITH tab0 AS (SELECT *, 
 MAX(nb_clt_ref) OVER() AS max_clt_ref,
 MAX(nb_clt_glb) OVER() AS max_clt_glb,  
 SUM(CASE WHEN modalite NOT IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR') THEN nb_clt_ref end) OVER (Partition BY typo_clt) AS soustot_ref,
 SUM(nb_clt_ref) OVER (Partition BY typo_clt) AS tot_ref, 
 SUM(CASE WHEN modalite NOT IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR') THEN nb_clt_glb end) OVER (Partition BY typo_clt) AS soustot_glb,
 SUM(nb_clt_glb) OVER (Partition BY typo_clt) AS tot_glb
     FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS ORDER BY 1,2 ),
tab1 AS (SELECT *, 
CASE WHEN modalite IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR','99_AUTRES/NC') THEN max_clt_ref ELSE LEAST(soustot_ref,max_clt_ref) END AS total_ref,
CASE WHEN modalite IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR','99_AUTRES/NC') THEN max_clt_glb ELSE LEAST(soustot_glb,max_clt_glb) END AS total_glb
FROM tab0),
trgfd AS (SELECT * 
,CASE WHEN nb_clt_glb IS NOT NULL AND nb_clt_glb>0 THEN Round(nb_clt_ref/nb_clt_glb,4) END AS part_clt_ref
,CASE WHEN total_ref IS NOT NULL AND total_ref>0 THEN Round(nb_clt_ref/total_ref,4) END AS poids_clt_ref
,CASE WHEN nb_clt_ref IS NOT NULL AND nb_clt_ref>0 THEN Round(CA_ref/nb_clt_ref,4) END AS CA_par_clt_ref
,CASE WHEN nb_clt_ref IS NOT NULL AND nb_clt_ref>0 THEN Round(nb_ticket_ref/nb_clt_ref,4) END AS freq_clt_ref   
,CASE WHEN nb_ticket_ref IS NOT NULL AND nb_ticket_ref>0 THEN Round(CA_ref/nb_ticket_ref,4) END AS panier_clt_ref    
,CASE WHEN nb_ticket_ref IS NOT NULL AND nb_ticket_ref>0 THEN Round(qte_achete_ref/nb_ticket_ref,4) END AS idv_clt_ref        
,CASE WHEN qte_achete_ref IS NOT NULL AND qte_achete_ref>0 THEN Round(CA_ref/qte_achete_ref,4) END AS pvm_clt_ref      
,CASE WHEN CA_ref IS NOT NULL AND CA_ref>0 THEN Round(Marge_ref/CA_ref,4) END AS txmarge_clt_ref   
,CASE WHEN CA_ref IS NOT NULL AND CA_ref>0 THEN Round(Mnt_remise_ref/(Mnt_remise_ref+CA_ref),4) END AS txremise_clt_ref
,CASE WHEN total_glb IS NOT NULL AND total_glb>0 THEN Round(nb_clt_glb/total_glb,4) END AS poids_clt_glb
,CASE WHEN nb_clt_glb IS NOT NULL AND nb_clt_glb>0 THEN Round(CA_glb/nb_clt_glb,4) END AS CA_par_clt_glb
,CASE WHEN nb_clt_glb IS NOT NULL AND nb_clt_glb>0 THEN Round(nb_ticket_glb/nb_clt_glb,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(CA_glb/nb_ticket_glb,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(qte_achete_glb/nb_ticket_glb,4) END AS idv_clt_glb        
,CASE WHEN qte_achete_glb IS NOT NULL AND qte_achete_glb>0 THEN Round(CA_glb/qte_achete_glb,4) END AS pvm_clt_glb      
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Marge_glb/CA_glb,4) END AS txmarge_clt_glb   
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Mnt_remise_glb/(CA_glb + Mnt_remise_glb),4) END AS txremise_clt_glb
FROM tab1 ORDER BY 1,2)
SELECT DISTINCT 
TYPO_CLT,
MODALITE,
MIN_DATE_TICKET,
MAX_DATE_TICKET,
NB_CLT_REF,
NB_TICKET_REF,
CA_REF,
QTE_ACHETE_REF,
MARGE_REF,
MNT_REMISE_REF,
NB_NEWCLT_REF,
NB_CLT_GLB,
NB_TICKET_GLB,
CA_GLB,
QTE_ACHETE_GLB,
MARGE_GLB,
MNT_REMISE_GLB,
NB_NEWCLT,
AGE_MOYEN_REF,
AGE_MOYEN,
SOUSTOT_REF,
TOT_REF,
SOUSTOT_GLB,
TOT_GLB,
TOTAL_REF,
TOTAL_GLB,
PART_CLT_REF,
POIDS_CLT_REF,
CA_PAR_CLT_REF,
FREQ_CLT_REF,
PANIER_CLT_REF,
IDV_CLT_REF,
PVM_CLT_REF,
TXMARGE_CLT_REF,
TXREMISE_CLT_REF,
POIDS_CLT_GLB,
CA_PAR_CLT_GLB,
FREQ_CLT_GLB,
PANIER_CLT_GLB,
IDV_CLT_GLB,
PVM_CLT_GLB,
TXMARGE_CLT_GLB,
TXREMISE_CLT_GLB
FROM trgfd ; 

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL ORDER BY 1,2; 



