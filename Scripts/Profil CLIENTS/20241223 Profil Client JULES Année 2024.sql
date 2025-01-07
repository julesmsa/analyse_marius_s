-- Profil Client JULES Année N versus N-1 12 Mois Glissant à la date du 01

-- Parametre des dates actifs 12 mois glissant 

SET dtfin = DAte('2024-12-20'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dtdeb = DAte('2024-01-01'); -- to_date(dateadd('year', -1, $dtfin)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

/*** Creation de la table permettant d'identifier les clients pour le profil ***/   
    
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS AS
WITH segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin) 
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt    
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_client IS NOT NULL AND code_client !='0' AND (date_suppression_client is null or date_suppression_client > $dtfin) ),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS montant_remise,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
TYPE_EMPLACEMENT,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,     
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                    from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
      THEN 1         
    ELSE 0 END AS annul_ticket
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb) AND (DATE($dtfin)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0') 
SELECT a.*, 
b.*,  ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI
,datediff(MONTH ,date_recrutement,$dtfin) AS ANCIENNETE_CLIENT
,CASE WHEN Date(date_recrutement) BETWEEN DATE($dtdeb) AND (DATE($dtfin)-1) THEN '02-Nouveaux' ELSE '01-Anciens' END AS Type_client
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
     -- WHEN id_macro_segment = '10' THEN '10_INA12'
     -- WHEN id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM  -- On estime que tous les clients actifs ont effectué au moins un achat et sont donc segmentés    
,CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     -- WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' -- On estime que tous les clients actifs ont effectué au moins un achat et sont donc segmentés 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM
,CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'a: [0-12] mois'  END  AS Tr_anciennete, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2
FROM tickets a
INNER JOIN info_clt b ON a.CODE_CLIENT=b.idclt
LEFT JOIN segrfm c ON a.code_client=c.code_client
LEFT JOIN segomni e ON a.code_client=e.code_client;  
       
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.CLT_SELECT AS
SELECT DISTINCT CODE_CLIENT 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
-- WHERE Qte_pos>0 
;  -- ON selectionne les clients ayant au moins un tickets actifs sur la période d'Analyse pas de notion de tickets NO Stress

SELECT count(DISTINCT Code_client) AS nbclt FROM DATA_MESH_PROD_CLIENT.WORK.CLT_SELECT ; 


/*
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
SELECT *,
DATE($dtfin) AS DATE_CALCUL
FROM Synth_vlz ; 
*/

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ LIMIT 10; 


-- SELECT DISTINCT ZONE_ACTIVITY_STATUS FROM DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ ;



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
INNER JOIN DATA_MESH_PROD_CLIENT.WORK.CLT_SELECT clt ON a.CODE_CLIENT=clt.CODE_CLIENT  
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ b ON a.CODE_CLIENT=b.ID_CLIENT ;

-- Calcul des indicateurs de performances Clients 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS AS
SELECT *
FROM ( 
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '01_GENRE' AS typo_clt, CASE 	
 WHEN GENRE='H' THEN '01-Hommes'
 WHEN GENRE='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '04_SEGMENT_RFM' AS typo_clt,  SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
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
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '06_CANAL_ACHAT' AS typo_clt, PERIMETRE AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '07A_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION 
SELECT '07B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,AVG(anciennete_client) AS nb_clt_glb
    ,AVG(anciennete_client) AS nb_ticket_glb
    ,AVG(anciennete_client) AS CA_glb
	,AVG(anciennete_client) AS qte_achete_glb
    ,AVG(anciennete_client) AS Marge_glb
    ,AVG(anciennete_client) AS Mnt_remise_glb    
    ,AVG(CASE WHEN Date(date_recrutement)=Date(date_ticket) then anciennete_client end) AS nb_newclt  
    ,AVG(anciennete_client) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '08A_AGE' AS typo_clt,  CASE WHEN CLASSE_AGE IN ('80-84','85-89','90-94','95-99') THEN '80 et +' ELSE CLASSE_AGE END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '08B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_clt_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_ticket_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS CA_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS qte_achete_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Marge_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Mnt_remise_glb    
    ,AVG(CASE WHEN Date(date_recrutement)=Date(date_ticket) AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '09_OPTIN_SMS' AS typo_clt,  CASE WHEN est_optin_sms_com=1 or est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '10_OPTIN_EMAIL' AS typo_clt,  CASE WHEN est_optin_email_com=1 or est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '11_SQUARE_SID' AS typo_clt,  SQUARE_SID_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '12_STORE_ACTIVITY' AS typo_clt,  STORE_ACTIVITY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '14_FAMILY_MATURITY_V2' AS typo_clt,  FAMILY_MATURITY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '15_PRICE_V2' AS typo_clt,  PRICE_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '16_PROMO_V2' AS typo_clt,  PROMO_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '17_LIFESTYLE_SEGMENT_V2' AS typo_clt,  LIFESTYLE_SEGMENT_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '18_CROSSCANAL_12_V2' AS typo_clt,  CROSSCANAL_12_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '19_CSR_ENVIRONMENT_V2' AS typo_clt,  CSR_ENVIRONMENT_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '20_CSR_HEALTHY_V2' AS typo_clt,  CSR_HEALTHY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '21_CSR_ORGANIC_V2' AS typo_clt,  CSR_ORGANIC_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
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
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2
UNION
SELECT '23_FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2 
UNION
SELECT '24_REGION' AS typo_clt,  REGION AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2 ) ORDER BY 1,2 ;   
   
   SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS ORDER BY 1,2 ;
  
  -- Calcul des indicateurs
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL AS  
  WITH tab0 AS (SELECT *, 
 MAX(nb_clt_glb) OVER() AS max_clt_glb,   
 SUM(CASE WHEN modalite NOT IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR') THEN nb_clt_glb end) OVER (Partition BY typo_clt) AS soustot_glb,
 SUM(nb_clt_glb) OVER (Partition BY typo_clt) AS tot_glb
     FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS ORDER BY 1,2 ),
tab1 AS (SELECT *, 
CASE WHEN modalite IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR','99_AUTRES/NC') THEN max_clt_glb ELSE LEAST(soustot_glb,max_clt_glb) END AS total_glb
FROM tab0),
trgfd AS (SELECT * 
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
NB_CLT_GLB,
NB_TICKET_GLB,
CA_GLB,
QTE_ACHETE_GLB,
MARGE_GLB,
MNT_REMISE_GLB,
NB_NEWCLT,
AGE_MOYEN,
SOUSTOT_GLB,
TOT_GLB,
TOTAL_GLB,
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

---- Analyse Sur la Période N-1 

SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb));
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 
SELECT $dtdeb_Nm1 , $dtfin_Nm1 ; 


/*** Creation de la table permettant d'identifier les clients pour le profil ***/   
    
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Nm1 AS
WITH segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin_Nm1) 
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt    
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_client IS NOT NULL AND code_client !='0' AND (date_suppression_client is null or date_suppression_client > $dtfin_Nm1) ),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS montant_remise,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
TYPE_EMPLACEMENT,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,     
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                    from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
      THEN 1         
    ELSE 0 END AS annul_ticket
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb_Nm1) AND (DATE($dtfin_Nm1)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')  
SELECT a.*, 
b.*,  ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI
,datediff(MONTH ,date_recrutement,$dtfin_Nm1) AS ANCIENNETE_CLIENT
,CASE WHEN Date(date_recrutement) BETWEEN DATE($dtdeb_Nm1) AND (DATE($dtfin_Nm1)-1) THEN '02-Nouveaux' ELSE '01-Anciens' END AS Type_client
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin_Nm1),2) AS AGE_C
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
     -- WHEN id_macro_segment = '10' THEN '10_INA12'
     -- WHEN id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM     
,CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     -- WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM
,CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'a: [0-12] mois'  END  AS Tr_anciennete, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2
FROM tickets a
INNER JOIN info_clt b ON a.CODE_CLIENT=b.idclt
LEFT JOIN segrfm c ON a.code_client=c.code_client
LEFT JOIN segomni e ON a.code_client=e.code_client;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Nm1 ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.CLT_SELECT_Nm1 AS
SELECT DISTINCT CODE_CLIENT 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Nm1
-- WHERE Qte_pos>0 
;  -- ON selectionne les clients ayant au moins un tickets actifs sur la période d'Analyse

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLT_SELECT_Nm1 ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1 AS
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
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Nm1 a 
INNER JOIN DATA_MESH_PROD_CLIENT.WORK.CLT_SELECT_Nm1 clt ON a.CODE_CLIENT=clt.CODE_CLIENT  
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_VALIUZ b ON a.CODE_CLIENT=b.ID_CLIENT ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1 ; 

-- Calcul des indicateurs de performances Clients 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS_Nm1 AS
SELECT *
FROM ( 
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '01_GENRE' AS typo_clt, CASE 	
 WHEN GENRE='H' THEN '01-Hommes'
 WHEN GENRE='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '04_SEGMENT_RFM' AS typo_clt,  SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '05_OMNICANALITE' AS typo_clt, 
 CASE WHEN lib_segment_omni ='MAG' then '01-MAG' 
      When lib_segment_omni ='WEB' then '02-WEB'
      When lib_segment_omni ='OMNI' then '03-OMNI' ELSE '99-NON RENSEIGNE' end as modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '06_CANAL_ACHAT' AS typo_clt, PERIMETRE AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '07A_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION 
SELECT '07B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,AVG(anciennete_client) AS nb_clt_glb
    ,AVG(anciennete_client) AS nb_ticket_glb
    ,AVG(anciennete_client) AS CA_glb
	,AVG(anciennete_client) AS qte_achete_glb
    ,AVG(anciennete_client) AS Marge_glb
    ,AVG(anciennete_client) AS Mnt_remise_glb    
    ,AVG(CASE WHEN Date(date_recrutement)=Date(date_ticket) then anciennete_client end) AS nb_newclt  
    ,AVG(anciennete_client) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '08A_AGE' AS typo_clt, CASE WHEN CLASSE_AGE IN ('80-84','85-89','90-94','95-99') THEN '80 et +' ELSE CLASSE_AGE END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '08B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_clt_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_ticket_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS CA_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS qte_achete_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Marge_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Mnt_remise_glb    
    ,AVG(CASE WHEN Date(date_recrutement)=Date(date_ticket) AND AGE_C BETWEEN 15 AND 99 THEN AGE_C2 end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '09_OPTIN_SMS' AS typo_clt,  CASE WHEN est_optin_sms_com=1 or est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '10_OPTIN_EMAIL' AS typo_clt,  CASE WHEN est_optin_email_com=1 or est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '11_SQUARE_SID' AS typo_clt,  SQUARE_SID_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '12_STORE_ACTIVITY' AS typo_clt,  STORE_ACTIVITY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '14_FAMILY_MATURITY_V2' AS typo_clt,  FAMILY_MATURITY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '15_PRICE_V2' AS typo_clt,  PRICE_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '16_PROMO_V2' AS typo_clt,  PROMO_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '17_LIFESTYLE_SEGMENT_V2' AS typo_clt,  LIFESTYLE_SEGMENT_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '18_CROSSCANAL_12_V2' AS typo_clt,  CROSSCANAL_12_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '19_CSR_ENVIRONMENT_V2' AS typo_clt,  CSR_ENVIRONMENT_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '20_CSR_HEALTHY_V2' AS typo_clt,  CSR_HEALTHY_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '21_CSR_ORGANIC_V2' AS typo_clt,  CSR_ORGANIC_V2 AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '22_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2
UNION
SELECT '23_FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2 
UNION
SELECT '24_REGION' AS typo_clt,  REGION AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt  
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2_Nm1
    GROUP BY 1,2 ) ORDER BY 1,2 ;   
   
   SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS_Nm1 ORDER BY 1,2 ;
  
  -- Calcul des indicateurs
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL_Nm1 AS  
  WITH tab0 AS (SELECT *, 
 MAX(nb_clt_glb) OVER() AS max_clt_glb,   
 SUM(CASE WHEN modalite NOT IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR') THEN nb_clt_glb end) OVER (Partition BY typo_clt) AS soustot_glb,
 SUM(nb_clt_glb) OVER (Partition BY typo_clt) AS tot_glb
     FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_BASE_TICKETS_Nm1 ORDER BY 1,2 ),
tab1 AS (SELECT *, 
CASE WHEN modalite IN ('03-Autres/NC','09_Non_Segmentes','12_NOSEG','99-NON RENSEIGNE','z: Non def','99-NR/NC','Z-NON RENSEIGNE','Z-NC/NR','99_AUTRES/NC') THEN max_clt_glb ELSE LEAST(soustot_glb,max_clt_glb) END AS total_glb
FROM tab0),
trgfd AS (SELECT * 
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
MIN_DATE_TICKET as MIN_DATE_TICKET_Nm1,
MAX_DATE_TICKET as MAX_DATE_TICKET_Nm1,
NB_CLT_GLB as NB_CLT_GLB_Nm1,
NB_TICKET_GLB as NB_TICKET_GLB_Nm1,
CA_GLB as CA_GLB_Nm1,
QTE_ACHETE_GLB as QTE_ACHETE_GLB_Nm1,
MARGE_GLB as MARGE_GLB_Nm1,
MNT_REMISE_GLB as MNT_REMISE_GLB_Nm1,
NB_NEWCLT as NB_NEWCLT_Nm1,
AGE_MOYEN as AGE_MOYEN_Nm1,
SOUSTOT_GLB as SOUSTOT_GLB_Nm1,
TOT_GLB as TOT_GLB_Nm1,
TOTAL_GLB as TOTAL_GLB_Nm1,
POIDS_CLT_GLB as POIDS_CLT_GLB_Nm1,
CA_PAR_CLT_GLB as CA_PAR_CLT_GLB_Nm1,
FREQ_CLT_GLB as FREQ_CLT_GLB_Nm1,
PANIER_CLT_GLB as PANIER_CLT_GLB_Nm1,
IDV_CLT_GLB as IDV_CLT_GLB_Nm1,
PVM_CLT_GLB as PVM_CLT_GLB_Nm1,
TXMARGE_CLT_GLB as TXMARGE_CLT_GLB_Nm1,
TXREMISE_CLT_GLB as TXREMISE_CLT_GLB_Nm1
FROM trgfd ;

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL_Nm1 ORDER BY 1,2;

--- Regroupement des informations et vcalcul des ecart et autres 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL_GLOBAL AS 
SELECT a.*,
NB_CLT_GLB_Nm1,
NB_TICKET_GLB_Nm1,
CA_GLB_Nm1,
QTE_ACHETE_GLB_Nm1,
MARGE_GLB_Nm1,
MNT_REMISE_GLB_Nm1,
NB_NEWCLT_Nm1,
AGE_MOYEN_Nm1,
SOUSTOT_GLB_Nm1,
TOT_GLB_Nm1,
TOTAL_GLB_Nm1,
POIDS_CLT_GLB_Nm1,
CA_PAR_CLT_GLB_Nm1,
FREQ_CLT_GLB_Nm1,
PANIER_CLT_GLB_Nm1,
IDV_CLT_GLB_Nm1,
PVM_CLT_GLB_Nm1,
TXMARGE_CLT_GLB_Nm1,
TXREMISE_CLT_GLB_Nm1,
CASE WHEN NB_CLT_GLB_Nm1 IS NOT NULL AND NB_CLT_GLB_Nm1>0 THEN ROUND((NB_CLT_GLB - NB_CLT_GLB_Nm1)/NB_CLT_GLB_Nm1,4) END AS EVOL_NB_CLT,
CASE WHEN NB_TICKET_GLB_Nm1 IS NOT NULL AND NB_TICKET_GLB_Nm1>0 THEN ROUND((NB_TICKET_GLB - NB_TICKET_GLB_Nm1)/NB_TICKET_GLB_Nm1,4) END AS EVOL_NB_TICKET,
CASE WHEN CA_GLB_Nm1 IS NOT NULL AND CA_GLB_Nm1>0 THEN ROUND((CA_GLB - CA_GLB_Nm1)/CA_GLB_Nm1,4) END AS EVOL_CA,
CASE WHEN QTE_ACHETE_GLB_Nm1 IS NOT NULL OR QTE_ACHETE_GLB_Nm1>0 THEN ROUND((QTE_ACHETE_GLB - QTE_ACHETE_GLB_Nm1)/QTE_ACHETE_GLB_Nm1,4) END AS EVOL_QTE_ACHETE,
CASE WHEN MARGE_GLB_Nm1 IS NOT NULL AND MARGE_GLB_Nm1>0 THEN ROUND((MARGE_GLB - MARGE_GLB_Nm1)/MARGE_GLB_Nm1,4) END AS EVOL_MARGE,
CASE WHEN MNT_REMISE_GLB_Nm1 IS NOT NULL AND MNT_REMISE_GLB_Nm1>0 THEN ROUND((MNT_REMISE_GLB - MNT_REMISE_GLB_Nm1)/MNT_REMISE_GLB_Nm1,4) END AS EVOL_MNT_REMISE,
CASE WHEN NB_NEWCLT_Nm1 IS NOT NULL AND NB_NEWCLT_Nm1>0 THEN ROUND((NB_NEWCLT - NB_NEWCLT_Nm1)/NB_NEWCLT_Nm1,4) END AS EVOL_NB_NEWCLT,
ROUND((AGE_MOYEN - AGE_MOYEN_Nm1),1) AS Ecart_AGE_MOYEN, 
ROUND((POIDS_CLT_GLB - POIDS_CLT_GLB_Nm1),4)*100 AS Ecart_POIDS_CLT_GLB,
ROUND((CA_PAR_CLT_GLB - CA_PAR_CLT_GLB_Nm1),4) AS Ecart_CA_PAR_CLT,
ROUND((FREQ_CLT_GLB - FREQ_CLT_GLB_Nm1),4) AS Ecart_FREQ_CLT,
ROUND((PANIER_CLT_GLB - PANIER_CLT_GLB_Nm1),4) AS Ecart_PANIER_CLT,
ROUND((IDV_CLT_GLB - IDV_CLT_GLB_Nm1),4) AS Ecart_IDV_CLT,
ROUND((PVM_CLT_GLB - PVM_CLT_GLB_Nm1),4) AS Ecart_PVM_CLT,
ROUND((TXMARGE_CLT_GLB - TXMARGE_CLT_GLB_Nm1),4)*100 AS Ecart_TXMARGE_CLT,
ROUND((TXREMISE_CLT_GLB - TXREMISE_CLT_GLB_Nm1),4)*100 AS Ecart_TXREMISE_CLT,
DATE($dtfin) AS DATE_CALCUL
FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL_Nm1 b ON a.TYPO_CLT=b.TYPO_CLT AND a.MODALITE=b.MODALITE ; 

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.KPICLT_PROFIL_GLOBAL ORDER BY 1,2;

-- EXTRACTION DES DONNEES

-- SELECT * FROM DHB_PROD.DNR.DN_VENTE WHERE id_ticket IN ('1-57-1-20240425-14116029' ,'' ) ;

-- SELECT * FROM DHB_PROD.DNR.DN_VENTE WHERE id_ticket IN ('1-57-1-20240425-14116029' ,'' ) ;





