-- Etablir le profil client des actifs 2023

  -- SCRIPT POUR L'ANNEE EN COURS 

SET dtdeb_EXON = Date('2023-05-01');
SET dtfin_EXON = DAte('2024-04-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_EXON, $dtfin_EXON;

SET dtdeb_EXONm1 = to_date(dateadd('year', -1, $dtdeb_EXON));
SET dtfin_EXONm1 = to_date(dateadd('year', -1, $dtfin_EXON)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb_EXON, $dtfin_EXON, $dtdeb_EXONm1, $dtfin_EXONm1, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_TEST_SML_EXON AS
WITH delai_achat AS ( SELECT *, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC) AS lag_date_ticket,
  	DATEDIFF(month, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC),date_ticket) as DELAI_DERNIER_ACHAT
  	FROM (
	SELECT DISTINCT
		vd.CODE_CLIENT,
		vd.date_ticket 
		from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
where vd.ID_ORG_ENSEIGNE IN (1,3) AND date_ticket<=$dtfin_EXON
)),
ACTIVITE_CLT AS (
  SELECT * , 
		CASE 
		WHEN DELAI_DERNIER_ACHAT <=12 THEN 'ACTIF 12MR' 
		WHEN DELAI_DERNIER_ACHAT >12 THEN 'REACTIVATION APRES 12MR' 
		WHEN DELAI_DERNIER_ACHAT IS NULL THEN 'NOUVEAU'
	ELSE 'ND' 
	END AS ACTIVITE_CLT
	FROM delai_achat),
adress_clt AS ( SELECT DISTINCT code_client, id_type_contact, valeur_contact,  
num_voie, type_voie, libelle_voie, code_postal, ville, code_pays, qualite_adresse
FROM DATA_MESH_PROD_CLIENT.HUB.DMD_CLI_Contact),
base_nps as (select code_client,             
    avg(valeur_reponse) as note_nps_moy
    --, table_nps.LIBELLE_ENQUETE, table_nps.LIBELLE_QUESTION, table_nps.CODE_QUESTION,    
from DATA_MESH_PROD_CLIENT.HUB.DMF_CLI_NPS
WHERE libelle_question like '%ecommand%'
AND  DATEH_CREATION_REPONSE_NPS BETWEEN DATE($dtdeb_EXON) AND DATE($dtfin_EXON)
group by 1),
info_clt AS (
SELECT a.*,
nom, prenom, id_magasin, id_magasin_courant, date_naissance,age, id_titre, titre, b.GENDER AS genre_clt, date_premier_achat, type_client, code_type_client,
date_dernier_achat, date_premier_achat_mag, date_dernier_achat_mag, date_premier_achat_ecom, date_dernier_achat_ecom, canal_entree,  code_pays AS pays_clt,
date_recrutement, code_postal, est_optin_sms_com, est_optin_email_com, est_valeur_optin_courrier_fid, est_valeur_optout_courrier_com, est_optin_sms_fid, 
est_optin_email_fid, ecommerce, wallet, est_valide_telephone, est_valide_email, source_recrutement
FROM DATA_MESH_PROD_CLIENT.WORK.RFM_DATAVIZ_HISTORIQUE a
JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE b ON a.MASTER_CUSTOMER_ID = b.CODE_CLIENT  
WHERE MONTH(DATE_PARTITION)=MONTH(DATE($dtfin_EXON))  AND YEAR(DATE_PARTITION)=YEAR(DATE($dtfin_EXON)) AND b.date_suppression_client IS NULL ),
produit as (
    select distinct ref.ID_REFERENCE, ref.ID_FAMILLE_ACHAT, fam.LIB_FAMILLE_ACHAT,
        G.LIB_GROUPE_FAMILLE
    from DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_REFERENCE ref
    join DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_FAMILLE_ACHAT fam 
        on ref.ID_FAMILLE_ACHAT = fam.ID_FAMILLE_ACHAT
    INNER JOIN DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_GROUPE_FAMILLE_ACHAT G
        ON G.ID_GROUPE_FAMILLE = fam.id_groupe_famille
    where ref.est_version_courante = 1 and ref.id_marque = 'JUL'),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
mag.type_emplacement, mag.code_magasin AS code_mag, mag.lib_magasin, mag.lib_statut, mag.id_concept, mag.lib_enseigne, mag.code_pays,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE, 
act.ACTIVITE_CLT
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
LEFT join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
LEFT JOIN ACTIVITE_CLT act ON vd.CODE_CLIENT=act.CODE_CLIENT AND vd.date_ticket=act.date_ticket
where vd.date_ticket BETWEEN DATE($dtdeb_EXON) AND DATE($dtfin_EXON) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  )  
SELECT a.*, b.*, c.note_nps_moy 
,datediff(MONTH ,date_premier_achat,$dtfin_EXON) AS ANCIENNETE_CLIENT
,datediff(MONTH ,date_premier_achat_mag,$dtfin_EXON) AS ANCIENNETE_CLIENT_MAG
,datediff(MONTH ,date_premier_achat_Ecom,$dtfin_EXON) AS ANCIENNETE_CLIENT_WEB
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin_EXON),2) AS AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
,code_postal AS CODEPOSTAL
	 , CASE WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('75','77','78','91','92','93','94','95')             then  '01FRA_01_Ile de France' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('02', '59', '60', '62', '80')                                      then  '01FRA_02_Hauts-de-France'
              WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('18', '28', '36', '37', '41', '45' )       						then  '01FRA_03_Centre-Val de Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('14', '27', '50', '61', '76')                                      then  '01FRA_04_Normandie'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('44', '49', '53', '72', '85')                                      then  '01FRA_05_Pays de la Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('22','29','35','56')       						then  '01FRA_06_Bretagne'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87')                                      then  '01FRA_07_Nouvelle-Aquitaine'	
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('08', '10', '51', '52', '54', '55', '57', '67', '68', '88')					then  '01FRA_08_Grand Est'
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('01','03','07','15','26','38','42','43','63','69','73','74' ) then  '01FRA_09_Auvergne-Rh ne-Alpes' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('21', '25', '39','58','70','71', '89', '90' ) then  '01FRA_10_Bourgogne-Franche-Comt ' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82' )                                      then  '01FRA_11_Occitanie' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('04', '05', '06', '13', '83', '84')                                      then  '01FRA_12_Provence-Alpes-C te d Azur' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('20')											then  '01FRA_13_Corse' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 3) in ('971','972','973','974','975','976','986','987','988') then  '01FRA_14_Outre-mer' 
			WHEN pays_clt='FRA' AND code_postal = '98000' 	then  '01FRA_15_Monaco'
			WHEN pays_clt='BEL'  	then  '02BELGIQUE'
			ELSE '01FRA_99_AUTRES/NC' END AS REGION -- a completer avec les informations de la BEL
,CASE 
            WHEN LIB_FAMILLE_ACHAT = 'Bermuda' THEN 'Bermuda'
             WHEN LIB_FAMILLE_ACHAT = 'Pantalon Denim' THEN 'Pantalon Denim'
            WHEN LIB_FAMILLE_ACHAT = 'Underwear' THEN 'Underwear'
            ELSE LIB_GROUPE_FAMILLE
          END AS LIB_GROUPE_FAMILLE_V2,           
CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM,     
CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM,
Max(CASE WHEN ACTIVITE_CLT='NOUVEAU' THEN 1 ELSE 0 END) over (partition by a.CODE_Client) as TOP_RECRUE,
Max(CASE WHEN ACTIVITE_CLT='REACTIVATION APRES 12MR' THEN 1 ELSE 0 END) over (partition by a.CODE_Client) as TOP_REACTIVATION,
CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete,     
CASE WHEN (anciennete_client_mag BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete_mag,     
CASE WHEN (anciennete_client_web BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete_web         
FROM tickets a
JOIN info_clt b ON a.CODE_CLIENT=b.MASTER_CUSTOMER_ID  
LEFT JOIN base_nps c ON a.CODE_CLIENT=c.CODE_CLIENT ; 

SELECT * FROM BASE_INFOCLT_TEST_SML_EXON WHERE note_nps_moy IS NOT null;

/****** information sur les diffrents KPI'S Client ****/

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_N AS
SELECT *
FROM ( 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
(SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, 
 '00_GLOBAL'  AS typo_clt, 
 '00_GLOBAL'  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION        
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
 UNION                
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres' END AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '24_ENSEIGNE' AS typo_clt,  
CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres' END AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres' END AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION        
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXON
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
 );

-- Calcul des autres indicateurs 
CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_EXON AS
SELECT a.*, 
    CASE WHEN nb_clt>0 THEN ROUND(CA/nb_clt,1) END AS CA_PAR_CLIENT,
    CASE WHEN nb_clt>0 THEN ROUND(nb_ticket/nb_clt,1) END AS FREQ_PAR_CLIENT,
	CASE WHEN nb_ticket>0 THEN ROUND(CA/nb_ticket,1) END AS PM,
	CASE WHEN nb_ticket>0 THEN ROUND(qte_achete/nb_ticket,1) END AS IDV,
	CASE WHEN qte_achete>0 THEN ROUND(CA/qte_achete,1) END AS PVM,
 		CASE WHEN typo_clt = '00_GLOBAL' THEN nb_clt END AS nb_eff,
		FIRST_VALUE(nb_eff) OVER (PARTITION BY perimetre order BY nb_eff NULLS LAST) AS FLAG_nb_eff,
		CASE WHEN modalite NOT IN ('Anciennete Moy Clt','Anciennete Moy Mag Clt','Anciennete Moy web Clt') THEN round(nb_clt/FLAG_nb_eff, 4) END  AS poids_clt,
		$dtdeb_EXON AS dtdeb_EXON, $dtfin_EXON AS dtfin_EXON
FROM DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_N a 
ORDER BY 1,2,3,4 ; 

SELECT * FROM KPI_INFOCLT_TEST_SML_EXON ORDER BY 1,2,3,4; 



-- Pour l'année N-1

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_TEST_SML_EXONm1 AS
WITH delai_achat AS ( SELECT *, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC) AS lag_date_ticket,
  	DATEDIFF(month, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC),date_ticket) as DELAI_DERNIER_ACHAT
  	FROM (
	SELECT DISTINCT
		vd.CODE_CLIENT,
		vd.date_ticket 
		from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
where vd.ID_ORG_ENSEIGNE IN (1,3) AND date_ticket<=$dtfin_EXONm1
)),
ACTIVITE_CLT AS (
  SELECT * , 
		CASE 
		WHEN DELAI_DERNIER_ACHAT <=12 THEN 'ACTIF 12MR' 
		WHEN DELAI_DERNIER_ACHAT >12 THEN 'REACTIVATION APRES 12MR' 
		WHEN DELAI_DERNIER_ACHAT IS NULL THEN 'NOUVEAU'
	ELSE 'ND' 
	END AS ACTIVITE_CLT
	FROM delai_achat),
adress_clt AS ( SELECT DISTINCT code_client, id_type_contact, valeur_contact,  
num_voie, type_voie, libelle_voie, code_postal, ville, code_pays, qualite_adresse
FROM DATA_MESH_PROD_CLIENT.HUB.DMD_CLI_Contact),
base_nps as (select code_client,             
    avg(valeur_reponse) as note_nps_moy
    --, table_nps.LIBELLE_ENQUETE, table_nps.LIBELLE_QUESTION, table_nps.CODE_QUESTION,    o
from DATA_MESH_PROD_CLIENT.HUB.DMF_CLI_NPS
WHERE libelle_question like '%ecommand%'
AND  DATEH_CREATION_REPONSE_NPS BETWEEN DATE($dtdeb_EXONm1) AND DATE($dtfin_EXONm1)
group by 1),
info_clt AS (
SELECT a.*,
nom, prenom, id_magasin, id_magasin_courant, date_naissance,age, id_titre, titre, b.GENDER AS genre_clt, date_premier_achat, type_client, code_type_client,
date_dernier_achat, date_premier_achat_mag, date_dernier_achat_mag, date_premier_achat_ecom, date_dernier_achat_ecom, canal_entree,  code_pays AS pays_clt,
date_recrutement, code_postal, est_optin_sms_com, est_optin_email_com, est_valeur_optin_courrier_fid, est_valeur_optout_courrier_com, est_optin_sms_fid, 
est_optin_email_fid, ecommerce, wallet, est_valide_telephone, est_valide_email, source_recrutement
FROM DATA_MESH_PROD_CLIENT.WORK.RFM_DATAVIZ_HISTORIQUE a
JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE b ON a.MASTER_CUSTOMER_ID = b.CODE_CLIENT  
WHERE MONTH(DATE_PARTITION)=MONTH(DATE($dtfin_EXONm1))  AND YEAR(DATE_PARTITION)=YEAR(DATE($dtfin_EXONm1)) AND b.date_suppression_client IS NULL ),
produit as (
    select distinct ref.ID_REFERENCE, ref.ID_FAMILLE_ACHAT, fam.LIB_FAMILLE_ACHAT,
        G.LIB_GROUPE_FAMILLE
    from DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_REFERENCE ref
    join DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_FAMILLE_ACHAT fam 
        on ref.ID_FAMILLE_ACHAT = fam.ID_FAMILLE_ACHAT
    INNER JOIN DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_GROUPE_FAMILLE_ACHAT G
        ON G.ID_GROUPE_FAMILLE = fam.id_groupe_famille
    where ref.est_version_courante = 1 and ref.id_marque = 'JUL'),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
mag.type_emplacement, mag.code_magasin AS code_mag, mag.lib_magasin, mag.lib_statut, mag.id_concept, mag.lib_enseigne, mag.code_pays,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE, 
act.ACTIVITE_CLT
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
LEFT join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
LEFT JOIN ACTIVITE_CLT act ON vd.CODE_CLIENT=act.CODE_CLIENT AND vd.date_ticket=act.date_ticket
where vd.date_ticket BETWEEN DATE($dtdeb_EXONm1) AND DATE($dtfin_EXONm1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  )  
SELECT a.*, b.*, c.note_nps_moy 
,datediff(MONTH ,date_premier_achat,$dtfin_EXONm1) AS ANCIENNETE_CLIENT
,datediff(MONTH ,date_premier_achat_mag,$dtfin_EXONm1) AS ANCIENNETE_CLIENT_MAG
,datediff(MONTH ,date_premier_achat_Ecom,$dtfin_EXONm1) AS ANCIENNETE_CLIENT_WEB
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin_EXONm1),2) AS AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
,code_postal AS CODEPOSTAL
	 , CASE WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('75','77','78','91','92','93','94','95')             then  '01FRA_01_Ile de France' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('02', '59', '60', '62', '80')                                      then  '01FRA_02_Hauts-de-France'
              WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('18', '28', '36', '37', '41', '45' )       						then  '01FRA_03_Centre-Val de Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('14', '27', '50', '61', '76')                                      then  '01FRA_04_Normandie'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('44', '49', '53', '72', '85')                                      then  '01FRA_05_Pays de la Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('22','29','35','56')       						then  '01FRA_06_Bretagne'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87')                                      then  '01FRA_07_Nouvelle-Aquitaine'	
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('08', '10', '51', '52', '54', '55', '57', '67', '68', '88')					then  '01FRA_08_Grand Est'
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('01','03','07','15','26','38','42','43','63','69','73','74' ) then  '01FRA_09_Auvergne-Rh ne-Alpes' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('21', '25', '39','58','70','71', '89', '90' ) then  '01FRA_10_Bourgogne-Franche-Comt ' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82' )                                      then  '01FRA_11_Occitanie' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('04', '05', '06', '13', '83', '84')                                      then  '01FRA_12_Provence-Alpes-C te d Azur' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('20')											then  '01FRA_13_Corse' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 3) in ('971','972','973','974','975','976','986','987','988') then  '01FRA_14_Outre-mer' 
			WHEN pays_clt='FRA' AND code_postal = '98000' 	then  '01FRA_15_Monaco'
			WHEN pays_clt='BEL'  	then  '02BELGIQUE'
			ELSE '01FRA_99_AUTRES/NC' END AS REGION -- a completer avec les informations de la BEL
,CASE 
            WHEN LIB_FAMILLE_ACHAT = 'Bermuda' THEN 'Bermuda'
             WHEN LIB_FAMILLE_ACHAT = 'Pantalon Denim' THEN 'Pantalon Denim'
            WHEN LIB_FAMILLE_ACHAT = 'Underwear' THEN 'Underwear'
            ELSE LIB_GROUPE_FAMILLE
          END AS LIB_GROUPE_FAMILLE_V2,           
CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
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
  ELSE '11_INA24' END AS SEGMENT_RFM,     
CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM,
Max(CASE WHEN ACTIVITE_CLT='NOUVEAU' THEN 1 ELSE 0 END) over (partition by a.CODE_Client) as TOP_RECRUE,
Max(CASE WHEN ACTIVITE_CLT='REACTIVATION APRES 12MR' THEN 1 ELSE 0 END) over (partition by a.CODE_Client) as TOP_REACTIVATION,
CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete,     
CASE WHEN (anciennete_client_mag BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client_mag IS NOT NULL AND anciennete_client_mag > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete_mag,     
CASE WHEN (anciennete_client_web BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client_web IS NOT NULL AND anciennete_client_web > 60 THEN 'f: + de 60 mois'
     else 'z: Non def' END  AS Tr_anciennete_web         
FROM tickets a
JOIN info_clt b ON a.CODE_CLIENT=b.MASTER_CUSTOMER_ID  
LEFT JOIN base_nps c ON a.CODE_CLIENT=c.CODE_CLIENT ; 

SELECT * FROM BASE_INFOCLT_TEST_SML_EXONm1;

/****** information sur les diffrents KPI'S Client ****/


CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_Nm1 AS
SELECT *
FROM ( 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '00_GLOBAL' as VIZ, '00_GLOBAL' AS perimetre, '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
(SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '01_SEXE' as VIZ, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS perimetre, '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, 
 '00_GLOBAL'  AS typo_clt, 
 '00_GLOBAL'  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '02_OMNICANALITE' as VIZ, CASE WHEN ID_segment_omni IS NULL OR LIB_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION        
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '03_CAT_SEGMENT_RFM' as VIZ, CAT_SEGMENT_RFM AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '04_SEGMENT_RFM' as VIZ, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
 UNION                
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '05_ACTIVITE_CLT' as VIZ, CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION         
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres' END AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '24_ENSEIGNE' AS typo_clt,  
CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres' END AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '06_ENSEIGNE' as VIZ, CASE WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'  WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres' END AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION        
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
  UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENDER='H' THEN '01-Hommes'
 WHEN GENDER='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
  UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '04_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)            
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '05_ACTIVITE_CLT' AS typo_clt, 
 CASE WHEN TOP_RECRUE=1 THEN '01-NOUVEAU' 
      WHEN TOP_REACTIVATION=1 THEN '02-REACTIVATION' ELSE 'ACTIF 12MR' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '06_BAIGNOIRE' AS typo_clt, 
 CASE WHEN ID_BAIGNOIRE IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE) END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '07_OMNICANALITE' AS typo_clt, 
 CASE WHEN ID_segment_omni IS NULL THEN '9-NC/NR' ELSE CONCAT(ID_segment_omni,'_',LIB_segment_omni) END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '08_CANAL_ACHAT' AS typo_clt, 
 CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '09_TYPE_ACHAT' AS typo_clt, 
Libelle_type_ligne AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '10_TYPO_MAGASIN' AS typo_clt,  type_emplacement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '11_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '12_ANCIENNETE CLIENT MOY' AS typo_clt,  'Anciennete Moy Clt' AS modalite, 
    ROUND (AVG (anciennete_client),1) AS nb_clt
    ,ROUND (AVG (anciennete_client),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client),1) AS CA
	,ROUND (AVG (anciennete_client),1) AS qte_achete
    ,ROUND (AVG (anciennete_client),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '13_ANCIENNETE CLIENT MAG' AS typo_clt,  Tr_anciennete_mag AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '14_ANCIENNETE CLIENT MAG MOY' AS typo_clt,  'Anciennete Moy Mag Clt' AS modalite, 
    ROUND (AVG (anciennete_client_mag),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_mag),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_mag),1) AS CA
	,ROUND (AVG (anciennete_client_mag),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_mag),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '15_ANCIENNETE CLIENT WEB' AS typo_clt,  Tr_anciennete_web AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '16_ANCIENNETE CLIENT WEB MOY' AS typo_clt,  'Anciennete Moy web Clt' AS modalite, 
    ROUND (AVG (anciennete_client_web),1) AS nb_clt
    ,ROUND (AVG (anciennete_client_web),1) AS nb_ticket
    ,ROUND (AVG (anciennete_client_web),1) AS CA
	,ROUND (AVG (anciennete_client_web),1) AS qte_achete
    ,ROUND (AVG (anciennete_client_web),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '17A_AGE' AS typo_clt,  CLASSE_AGE AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '17B_AGE MOY' AS typo_clt,  'Age Moy' AS modalite, 
    ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_clt
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS nb_ticket
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS CA
	,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS qte_achete
    ,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)        
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '18_OPTIN_SMS_COM' AS typo_clt,  CASE WHEN est_optin_sms_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '19_OPTIN_EMAIL_COM' AS typo_clt,  CASE WHEN est_optin_email_com=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '20_OPTIN_SMS_FID' AS typo_clt,  CASE WHEN est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '21_OPTIN_EMAIL_FID' AS typo_clt,  CASE WHEN est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '22_SOURCE_RECRUTEMENT' AS typo_clt, source_recrutement AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4) 
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '23_REGION' AS typo_clt, region AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '24_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, '25_GROUPE_FAMILLE' AS typo_clt,  
 CASE WHEN LIB_GROUPE_FAMILLE_V2 IS NULL THEN 'Z-NC/NR' ELSE LIB_GROUPE_FAMILLE_V2 END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, 
 '26_SCORE NPS CLIENT' AS typo_clt,  CASE WHEN note_nps_moy IS NOT NULL AND note_nps_moy >=9 THEN '01-PROMOTEURS' 
     WHEN note_nps_moy IS NOT NULL AND note_nps_moy <7 THEN '02-DETRACTEURS'
     WHEN note_nps_moy IS NULL THEN '09-Pas de Note'
             ELSE '03-PASSIFS'
        END AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
UNION 
 (SELECT '07_MAGASINS' as VIZ, CONCAT(CODE_MAG,'_',LIB_MAGASIN) AS perimetre, 
 '27_SCORE NPS CLIENT MOY' AS typo_clt,  'NPS CLIENT Moy Clt' AS modalite, 
    ROUND (AVG (note_nps_moy),1) AS nb_clt
    ,ROUND (AVG (note_nps_moy),1) AS nb_ticket
    ,ROUND (AVG (note_nps_moy),1) AS CA
	,ROUND (AVG (note_nps_moy),1) AS qte_achete
    ,ROUND (AVG (note_nps_moy),1) AS Marge	
FROM BASE_INFOCLT_TEST_SML_EXONm1
    GROUP BY 1,2,3,4
        ORDER BY 1,2,3,4)
 );

-- Calcul des autres indicateurs 
CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_EXONm1 AS
SELECT a.* , 
    CASE WHEN nb_clt>0 THEN ROUND(CA/nb_clt,1) END AS CA_PAR_CLIENT,
    CASE WHEN nb_clt>0 THEN ROUND(nb_ticket/nb_clt,1) END AS FREQ_PAR_CLIENT,
	CASE WHEN nb_ticket>0 THEN ROUND(CA/nb_ticket,1) END AS PM,
	CASE WHEN nb_ticket>0 THEN ROUND(qte_achete/nb_ticket,1) END AS IDV,
	CASE WHEN qte_achete>0 THEN ROUND(CA/qte_achete,1) END AS PVM,
 		CASE WHEN typo_clt = '00_GLOBAL' THEN nb_clt END AS nb_eff,
		FIRST_VALUE(nb_eff) OVER (PARTITION BY perimetre order BY nb_eff NULLS LAST) AS FLAG_nb_eff,
		CASE WHEN modalite NOT IN ('Anciennete Moy Clt','Anciennete Moy Mag Clt','Anciennete Moy web Clt') AND FLAG_nb_eff>0 THEN round(nb_clt/FLAG_nb_eff, 4) END  AS poids_clt,
		$dtdeb_EXONm1 AS dtdeb_EXONm1, $dtfin_EXONm1 AS dtfin_EXONm1
FROM DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_Nm1 a 
ORDER BY 1,2,3,4;

  --- Combinaison des deux années

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_JULES AS
WITH tab0 AS (SELECT * FROM (
SELECT DISTINCT VIZ, Perimetre, Typo_clt, modalite FROM DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_EXON 
UNION
SELECT DISTINCT VIZ, Perimetre, Typo_clt, modalite FROM DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_EXONm1))
SELECT a.VIZ, a.Perimetre, a.Typo_clt, a.modalite 
,b.nb_clt AS nb_clt_EXON 
,b.nb_ticket AS nb_ticket_EXON
,b.CA AS CA_EXON
,b.Qte_achete AS Qte_achete_EXON
,b.marge AS marge_EXON
,b.Ca_par_client AS Ca_par_client_EXON
,b.freq_par_client AS freq_par_client_EXON
,b.PM AS PM_EXON
,b.IDV AS IDV_EXON
,b.PVM AS PVM_EXON
,b.Poids_Clt AS Poids_Clt_EXON
,c.nb_clt AS nb_clt_EXONm1 
,c.nb_ticket AS nb_ticket_EXONm1
,c.CA AS CA_EXONm1
,c.Qte_achete AS Qte_achete_EXONm1
,c.marge AS marge_EXONm1
,c.Ca_par_client AS Ca_par_client_EXONm1
,c.freq_par_client AS freq_par_client_EXONm1
,c.PM AS PM_EXONm1
,c.IDV AS IDV_EXONm1
,c.PVM AS PVM_EXONm1
,c.Poids_Clt AS Poids_Clt_EXONm1
FROM tab0 a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_EXON b ON (a.VIZ=b.VIZ AND  a.Perimetre=b.Perimetre AND  a.Typo_clt=b.Typo_clt AND  a.modalite=b.modalite) 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_SML_EXONm1 c ON (a.VIZ=c.VIZ AND  a.Perimetre=c.Perimetre AND  a.Typo_clt=c.Typo_clt AND  a.modalite=c.modalite) 
ORDER BY 1,2,3,4; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_JULES ORDER BY 1,2,3,4;  

--- Export des informations 

-- EXPORT ----
      /* cr�er un dossier volant dossier_export_data_*/
--CREATE OR REPLACE TEMPORARY STAGE dossier_export_data_;
/* dedans, je vais mettre la requete entre FROM et FILE_FORMAT */
--COPY INTO @dossier_export_data_/KPI_INFOCLT_TEST_JULES.csv FROM 
--(SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.KPI_INFOCLT_TEST_JULES ORDER BY 1,2,3,4)      
--FILE_FORMAT = ( 
 --TYPE='CSV' 
 --COMPRESSION=NONE /*GZIP */ /* compression ou non */
 --FIELD_DELIMITER=',' /* dlm*/
 --ESCAPE=NONE 
 --ESCAPE_UNENCLOSED_FIELD=NONE 
 --date_format='AUTO' 
 --time_format='AUTO' 
 --timestamp_format='AUTO'
 --binary_format='UTF-8' 
 --field_optionally_enclosed_by='"' 
 --null_if='' 
 --EMPTY_FIELD_AS_NULL = FALSE 
--)  
--overwrite=TRUE /* écrire par dessus */ 
--single=TRUE /* fichier seul ou parralelisation*/ 
--max_file_size=5368709120 /* taille max en Octet du fichier */
--header=TRUE /* noms e col */;
/* recuperer le nom du fichier / vérifier que c ok */
--ls @dossier_export_data_;
--GET @dossier_export_data_/KPI_INFOCLT_TEST_JULES.csv  /* dans ton dossier volant */
--file://C:\Users\msaka\OneDrive - HAPPYCHIC\Bureau\Extract_Result_Sql



/******** information par deciles
 * 
 * 
 */
        
 SELECT * from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE;

