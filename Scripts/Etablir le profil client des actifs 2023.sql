-- Etablir le profil client des actifs 2023

     -- etape 1 Parametre Date de Debut et fin et périmetre client 

-- SET dtdeb ='2023-01-01'; 
SET dtfin = to_date('2024-05-01')-1; 
SET dtdeb = to_date(dateadd('year', -1, $dtfin))+1 ;
-- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb , $dtfin, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_TEST_SML AS
WITH delai_achat AS ( SELECT *, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC) AS lag_date_ticket,
  	DATEDIFF(month, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC),date_ticket) as DELAI_DERNIER_ACHAT
  	FROM (
	SELECT DISTINCT
		vd.CODE_CLIENT,
		vd.date_ticket 
		from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
where vd.ID_ORG_ENSEIGNE IN (1,3))),
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
AND  DATEH_CREATION_REPONSE_NPS BETWEEN DATE($dtdeb) AND DATE($dtfin)
group by 1),
info_clt AS (
SELECT a.*,
nom, prenom, id_magasin, id_magasin_courant, date_naissance,age, id_titre, titre, b.GENDER AS genre_clt, date_premier_achat, type_client, code_type_client,
date_dernier_achat, date_premier_achat_mag, date_dernier_achat_mag, date_premier_achat_ecom, date_dernier_achat_ecom, canal_entree, 
date_recrutement, code_postal, est_optin_sms_com, est_optin_email_com, est_valeur_optin_courrier_fid, est_valeur_optout_courrier_com, est_optin_sms_fid, 
est_optin_email_fid, ecommerce, wallet, est_valide_telephone, est_valide_email, source_recrutement
FROM DATA_MESH_PROD_CLIENT.WORK.RFM_DATAVIZ_HISTORIQUE a
JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE b ON a.MASTER_CUSTOMER_ID = b.CODE_CLIENT  
WHERE MONTH(DATE_PARTITION)=MONTH(DATE($dtfin))  AND YEAR(DATE_PARTITION)=YEAR(DATE($dtfin)) AND b.date_suppression_client IS NULL ),
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
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  )  
SELECT a.*, b.*, c.note_nps_moy, 
ROUND(DATEDIFF(YEAR,date_naissance,$dtfin),2) AS AGE_C
,CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) AS CLASSE_AGE
,code_postal AS CODEPOSTAL
	 , CASE WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('75','77','78','91','92','93','94','95')             then  'FRA_01_Ile de France' 
		      WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('02', '59', '60', '62', '80')                                      then  'FRA_02_Hauts-de-France'
              WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('18', '28', '36', '37', '41', '45' )       						then  'FRA_03_Centre-Val de Loire'
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('14', '27', '50', '61', '76')                                      then  'FRA_04_Normandie'
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('44', '49', '53', '72', '85')                                      then  'FRA_05_Pays de la Loire'
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('22','29','35','56')       						then  'FRA_06_Bretagne'
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87')                                      then  'FRA_07_Nouvelle-Aquitaine'	
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('08', '10', '51', '52', '54', '55', '57', '67', '68', '88')					then  'FRA_08_Grand Est'
		      WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('01','03','07','15','26','38','42','43','63','69','73','74' ) then  'FRA_09_Auvergne-Rh ne-Alpes' 
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('21', '25', '39','58','70','71', '89', '90' ) then  'FRA_10_Bourgogne-Franche-Comt ' 
		      WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82' )                                      then  'FRA_11_Occitanie' 
		      WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('04', '05', '06', '13', '83', '84')                                      then  'FRA_12_Provence-Alpes-C te d Azur' 
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 2) in ('20')											then  'FRA_13_Corse' 
			  WHEN code_pays='FRA' AND SUBSTRING(code_postal, 1, 3) in ('971','972','973','974','975','976','986','987','988') then  'FRA_14_Outre-mer' 
			WHEN code_pays='FRA' AND code_postal = '98000' 	then  'FRA_15_Monaco' 
			ELSE 'FRA_99_AUTRES/NC' END AS REGION -- a completer avec les informations de la BEL
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
Max(CASE WHEN ACTIVITE_CLT='REACTIVATION APRES 12MR' THEN 1 ELSE 0 END) over (partition by a.CODE_Client) as TOP_REACTIVATION
FROM tickets a
JOIN info_clt b ON a.CODE_CLIENT=b.MASTER_CUSTOMER_ID  
LEFT JOIN base_nps c ON a.CODE_CLIENT=c.CODE_CLIENT ; 



SELECT * FROM BASE_INFOCLT_TEST_SML;	

SELECT DISTINCT LIB_SEGMENT_OMNI
FROM BASE_INFOCLT_TEST_SML


/****** information sur les diffrents KPI'S Client ****/
SELECT * FROM ( 
 (SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(distinct id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(MONTANT_TTC)/COUNT(DISTINCT CODE_CLIENT) AS CA_PAR_CLIENT_glb
    ,COUNT(DISTINCT id_ticket)/COUNT(DISTINCT CODE_CLIENT) AS FREQ_PAR_CLIENT_glb
	,SUM(MONTANT_TTC)/COUNT(DISTINCT id_ticket) AS PM_glb
	,SUM(QUANTITE_LIGNE)/COUNT(DISTINCT id_ticket) AS IDV_glb
	,SUM(MONTANT_TTC)/SUM(QUANTITE_LIGNE) AS PVM_glb
	
    ,COUNT( DISTINCT CASE WHEN GENDER='H' THEN CODE_CLIENT end) AS nb_clt_Hom
    ,COUNT(distinct  CASE WHEN GENDER='H' THEN id_ticket end) AS nb_ticket_Hom
    ,SUM( CASE WHEN GENDER='H' THEN MONTANT_TTC end) AS CA_Hom
	,SUM( CASE WHEN GENDER='H' THEN QUANTITE_LIGNE end) AS qte_achete_Hom
    ,SUM( CASE WHEN GENDER='H' THEN MONTANT_MARGE_SORTIE end) AS Marge_Hom
    ,SUM( CASE WHEN GENDER='H' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN CODE_CLIENT end) AS CA_PAR_CLIENT_Hom
    ,COUNT(DISTINCT  CASE WHEN GENDER='H' THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_Hom
	,SUM( CASE WHEN GENDER='H' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN id_ticket end) AS PM_Hom
	,SUM( CASE WHEN GENDER='H' THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN id_ticket end) AS IDV_Hom
	,SUM(CASE WHEN GENDER='H' THEN MONTANT_TTC end)/SUM(CASE WHEN GENDER='H' THEN QUANTITE_LIGNE end) AS PVM_Hom
	
	
    ,COUNT( DISTINCT CASE WHEN GENDER='F' THEN CODE_CLIENT end) AS nb_clt_Hom
    ,COUNT(distinct  CASE WHEN GENDER='F' THEN id_ticket end) AS nb_ticket_Hom
    ,SUM( CASE WHEN GENDER='F' THEN MONTANT_TTC end) AS CA_Hom
	,SUM( CASE WHEN GENDER='F' THEN QUANTITE_LIGNE end) AS qte_achete_Hom
    ,SUM( CASE WHEN GENDER='F' THEN MONTANT_MARGE_SORTIE end) AS Marge_Hom
    ,SUM( CASE WHEN GENDER='F' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN CODE_CLIENT end) AS CA_PAR_CLIENT_Hom
    ,COUNT(DISTINCT  CASE WHEN GENDER='F' THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_Hom
	,SUM( CASE WHEN GENDER='F' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN id_ticket end) AS PM_Hom
	,SUM( CASE WHEN GENDER='F' THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN GENDER='H' THEN id_ticket end) AS IDV_Hom
	,SUM(CASE WHEN GENDER='F' THEN MONTANT_TTC end)/SUM(CASE WHEN GENDER='H' THEN QUANTITE_LIGNE end) AS PVM_Hom	
	
    ,COUNT( DISTINCT CASE WHEN GENDER='F' THEN CODE_CLIENT end) AS nb_clt_Fem
    ,COUNT(distinct  CASE WHEN GENDER='F' THEN id_ticket end) AS nb_ticket_Fem
    ,SUM( CASE WHEN GENDER='F' THEN MONTANT_TTC end) AS CA_Fem
	,SUM( CASE WHEN GENDER='F' THEN QUANTITE_LIGNE end) AS qte_achete_Fem
    ,SUM( CASE WHEN GENDER='F' THEN MONTANT_MARGE_SORTIE end) AS Marge_Fem
    ,SUM( CASE WHEN GENDER='F' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN GENDER='F' THEN CODE_CLIENT end) AS CA_PAR_CLIENT_Fem
    ,COUNT(DISTINCT  CASE WHEN GENDER='F' THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN GENDER='F' THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_Fem
	,SUM( CASE WHEN GENDER='F' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN GENDER='F' THEN id_ticket end) AS PM_Fem
	,SUM( CASE WHEN GENDER='F' THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN GENDER='F' THEN id_ticket end) AS IDV_Fem
	,SUM(CASE WHEN GENDER='F' THEN MONTANT_TTC end)/SUM(CASE WHEN GENDER='F' THEN QUANTITE_LIGNE end) AS PVM_Fem
	
	
    ,COUNT( DISTINCT CASE WHEN TOP_RECRUE=1 THEN CODE_CLIENT end) AS nb_clt_RECRUE
    ,COUNT(distinct  CASE WHEN TOP_RECRUE=1 THEN id_ticket end) AS nb_ticket_RECRUE
    ,SUM( CASE WHEN TOP_RECRUE=1 THEN MONTANT_TTC end) AS CA_RECRUE
	,SUM( CASE WHEN TOP_RECRUE=1 THEN QUANTITE_LIGNE end) AS qte_achete_RECRUE
    ,SUM( CASE WHEN TOP_RECRUE=1 THEN MONTANT_MARGE_SORTIE end) AS Marge_RECRUE
    ,SUM( CASE WHEN TOP_RECRUE=1 THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN TOP_RECRUE=1 THEN CODE_CLIENT end) AS CA_PAR_CLIENT_RECRUE
    ,COUNT(DISTINCT  CASE WHEN TOP_RECRUE=1 THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN TOP_RECRUE=1 THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_RECRUE
	,SUM( CASE WHEN TOP_RECRUE=1 THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN TOP_RECRUE=1 THEN id_ticket end) AS PM_RECRUE
	,SUM( CASE WHEN TOP_RECRUE=1 THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN TOP_RECRUE=1 THEN id_ticket end) AS IDV_RECRUE
	,SUM(CASE WHEN TOP_RECRUE=1 THEN MONTANT_TTC end)/SUM(CASE WHEN TOP_RECRUE=1 THEN QUANTITE_LIGNE end) AS PVM_RECRUE
	
	
    ,COUNT( DISTINCT CASE WHEN TOP_REACTIVATION=1 THEN CODE_CLIENT end) AS nb_clt_REACTIVATION
    ,COUNT(distinct  CASE WHEN TOP_REACTIVATION=1 THEN id_ticket end) AS nb_ticket_REACTIVATION
    ,SUM( CASE WHEN TOP_REACTIVATION=1 THEN MONTANT_TTC end) AS CA_REACTIVATION
	,SUM( CASE WHEN TOP_REACTIVATION=1 THEN QUANTITE_LIGNE end) AS qte_achete_REACTIVATION
    ,SUM( CASE WHEN TOP_REACTIVATION=1 THEN MONTANT_MARGE_SORTIE end) AS Marge_REACTIVATION
    ,SUM( CASE WHEN TOP_REACTIVATION=1 THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN TOP_REACTIVATION=1 THEN CODE_CLIENT end) AS CA_PAR_CLIENT_REACTIVATION
    ,COUNT(DISTINCT  CASE WHEN TOP_REACTIVATION=1 THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN TOP_REACTIVATION=1 THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_REACTIVATION
	,SUM( CASE WHEN TOP_REACTIVATION=1 THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN TOP_REACTIVATION=1 THEN id_ticket end) AS PM_REACTIVATION
	,SUM( CASE WHEN TOP_REACTIVATION=1 THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN TOP_REACTIVATION=1 THEN id_ticket end) AS IDV_REACTIVATION
	,SUM(CASE WHEN TOP_REACTIVATION=1 THEN MONTANT_TTC end)/SUM(CASE WHEN TOP_REACTIVATION=1 THEN QUANTITE_LIGNE end) AS PVM_REACTIVATION
	
	
    ,COUNT( DISTINCT CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN CODE_CLIENT end) AS nb_clt_OMNI
    ,COUNT(distinct  CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN id_ticket end) AS nb_ticket_OMNI
    ,SUM( CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN MONTANT_TTC end) AS CA_OMNI
	,SUM( CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN QUANTITE_LIGNE end) AS qte_achete_OMNI
    ,SUM( CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN MONTANT_MARGE_SORTIE end) AS Marge_OMNI
    ,SUM( CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN CODE_CLIENT end) AS CA_PAR_CLIENT_OMNI
    ,COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_OMNI
	,SUM( CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN id_ticket end) AS PM_OMNI
	,SUM( CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN id_ticket end) AS IDV_OMNI
	,SUM(CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN MONTANT_TTC end)/SUM(CASE WHEN LIB_SEGMENT_OMNI='OMNI' THEN QUANTITE_LIGNE end) AS PVM_OMNI	
	
    ,COUNT( DISTINCT CASE WHEN LIB_SEGMENT_MAG='MAG' THEN CODE_CLIENT end) AS nb_clt_MAG
    ,COUNT(distinct  CASE WHEN LIB_SEGMENT_MAG='MAG' THEN id_ticket end) AS nb_ticket_MAG
    ,SUM( CASE WHEN LIB_SEGMENT_MAG='MAG' THEN MONTANT_TTC end) AS CA_MAG
	,SUM( CASE WHEN LIB_SEGMENT_MAG='MAG' THEN QUANTITE_LIGNE end) AS qte_achete_MAG
    ,SUM( CASE WHEN LIB_SEGMENT_MAG='MAG' THEN MONTANT_MARGE_SORTIE end) AS Marge_MAG
    ,SUM( CASE WHEN LIB_SEGMENT_MAG='MAG' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_MAG='MAG' THEN CODE_CLIENT end) AS CA_PAR_CLIENT_MAG
    ,COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_MAG='MAG' THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_MAG='MAG' THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_MAG
	,SUM( CASE WHEN LIB_SEGMENT_MAG='MAG' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_MAG='MAG' THEN id_ticket end) AS PM_MAG
	,SUM( CASE WHEN LIB_SEGMENT_MAG='MAG' THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_MAG='MAG' THEN id_ticket end) AS IDV_MAG
	,SUM(CASE WHEN LIB_SEGMENT_MAG='MAG' THEN MONTANT_TTC end)/SUM(CASE WHEN LIB_SEGMENT_MAG='MAG' THEN QUANTITE_LIGNE end) AS PVM_MAG
	
	
    ,COUNT( DISTINCT CASE WHEN LIB_SEGMENT_WEB='WEB' THEN CODE_CLIENT end) AS nb_clt_WEB
    ,COUNT(distinct  CASE WHEN LIB_SEGMENT_WEB='WEB' THEN id_ticket end) AS nb_ticket_WEB
    ,SUM( CASE WHEN LIB_SEGMENT_WEB='WEB' THEN MONTANT_TTC end) AS CA_WEB
	,SUM( CASE WHEN LIB_SEGMENT_WEB='WEB' THEN QUANTITE_LIGNE end) AS qte_achete_WEB
    ,SUM( CASE WHEN LIB_SEGMENT_WEB='WEB' THEN MONTANT_MARGE_SORTIE end) AS Marge_WEB
    ,SUM( CASE WHEN LIB_SEGMENT_WEB='WEB' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_WEB='WEB' THEN CODE_CLIENT end) AS CA_PAR_CLIENT_WEB
    ,COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_WEB='WEB' THEN id_ticket end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_WEB='WEB' THEN CODE_CLIENT end) AS FREQ_PAR_CLIENT_WEB
	,SUM( CASE WHEN LIB_SEGMENT_WEB='WEB' THEN MONTANT_TTC end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_WEB='WEB' THEN id_ticket end) AS PM_WEB
	,SUM( CASE WHEN LIB_SEGMENT_WEB='WEB' THEN QUANTITE_LIGNE end)/COUNT(DISTINCT  CASE WHEN LIB_SEGMENT_WEB='WEB' THEN id_ticket end) AS IDV_WEB
	,SUM(CASE WHEN LIB_SEGMENT_WEB='WEB' THEN MONTANT_TTC end)/SUM(CASE WHEN LIB_SEGMENT_WEB='WEB' THEN QUANTITE_LIGNE end) AS PVM_WEB	
	
	
	
	
	
	
	
	
	
	
	
	
FROM BASE_INFOCLT_TEST_SML
    GROUP BY 1,2
        ORDER BY 1,2)
        
        
        
        
        
        
        
UNION 
 (SELECT '01_GENRE' AS typo_clt, GENDER AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    FROM BASE_INFOCLT_TEST_SML
    GROUP BY 1,2
        ORDER BY 1,2)
UNION 
 (SELECT '02_RFM' AS typo_clt, CONCAT(ID_macro_segment,'_',lib_macro_segment) AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    FROM BASE_INFOCLT_TEST_SML
    GROUP BY 1,2
        ORDER BY 1,2)
UNION 
 (SELECT '03_BAIGNOIRE' AS typo_clt, CONCAT(ID_BAIGNOIRE,'_',LIB_BAIGNOIRE)  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    FROM BASE_INFOCLT_TEST_SML
    GROUP BY 1,2
        ORDER BY 1,2)
UNION 
 (SELECT '04_OMNICANALITE' AS typo_clt, CONCAT(ID_segment_omni,'_',LIB_sebment_omni)  AS modalite, 
    COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    FROM BASE_INFOCLT_TEST_SML
    GROUP BY 1,2
        ORDER BY 1,2)
        )ORDER BY 1,2














info_clt AS (
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.RFM_DATAVIZ_HISTORIQUE 
WHERE MONTH(DATE_PARTITION)=MONTH(DATE($dtfin))  AND YEAR(DATE_PARTITION)=YEAR(DATE($dtfin))  
--AND MASTER_CUSTOMER_ID ='313710012284'
ORDER BY 1)






; 








SELECT DISTINCT libelle_type_ticket , FLAG_TYPE_TICKET
FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE ;  

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE;  

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE WHERE libelle_type_ligne= 'Retour'; 

SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN ;  

SELECT * FROM DATA_MESH_PROD_CLIENT.HUB.DMD_CLIENT ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.RFM_SEGMENTATION; 

SELECT * FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM WHERE CODE_CLIENT ='313710012284' ORDER BY 1;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.RFM_SEGMENTATION WHERE CODE_CLIENT ='313710012284' ORDER BY 2; 

SELECT * FROM DATA_MESH_PROD_CLIENT.HUB.DMD_CLI_CARTE  ; 

adress_clt ( SELECT DISTINCT code_client, id_type_contact, valeur_contact,  
num_voie, type_voie, libelle_voie, code_postal, ville, code_pays, qualite_adresse
FROM DATA_MESH_PROD_CLIENT.HUB.DMD_CLI_Contact) ;  


SELECT * FROM DATA_MESH_PROD_CLIENT.HUB.DMH_CLIENT_SEGMENT_PROMO WHERE CODE_CLIENT ='313710012284' ORDER BY 1; 

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.RFM_DATAVIZ_HISTORIQUE WHERE MASTER_CUSTOMER_ID ='313710012284' ORDER BY 1; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE;  




SELECT Code_client, nom, prenom, id_magasin, id_magasin_courant, date_naissance,age, id_titre, titre, GENDER, date_premier_achat, type_client, code_type_client,
date_dernier_achat, date_premier_achat_mag, date_dernier_achat_mag, date_premier_achat_ecom, date_dernier_achat_ecom, canal_entree, 
date_recrutement, code_postal, est_optin_sms_com, est_optin_email_com, est_valeur_optin_courrier_fid, est_valeur_optout_courrier_com, est_optin_sms_fid, 
est_optin_email_fid, ecommerce, wallet, est_valide_telephone, est_valide_email, source_recrutement
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE
WHERE date_suppression_client IS NULL ;



-- set date_execution = date('2022-03-01');
-- select $date_execution, dateadd(year, -1, $date_execution);



SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN ;


-- Selection des clients actifs 








actif_0_36_mois_rfm as ( 
SELECT 
  $dtdeb as date_partition, 
	rfm.code_client as code_client_rfm,
	rfm.lib_macro_segment, 
    rfm.id_macro_segment,
    -- formatage des segments de RFM (sans espaces et correctement numérotés)
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
     ELSE '11_INA24' END AS SEGMENT_RFM_INIT,
    rfm.date_debut, 
    rfm.date_fin
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM  rfm
where date_suppression_client is NULL 
  and DATE_DEBUT <= $dtdeb AND (DATE_FIN > $dtdeb OR DATE_FIN IS NULL)
),

tickets_0_12m as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, date_ticket, 
vd.MONTANT_TTC AS MONTANT_TTC_orig
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_artile, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket
from 
DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
join produit on vd.CODE_REFERENCE = produit.ID_REFERENCE
join DATA_MESH_PROD_CLIENT.HUB.DMD_CLIENT c  on vd.CODE_CLIENT = c.code_client
where 1 = 1
  and date_suppression_client is null -------- voir si on enlève les clients supprimés vu les données de RFM
  and dateadd(year,-3,$dtdeb) <= date(vd.DATEH_TICKET) and $dtdeb > date(vd.DATEH_TICKET)
  and vd.FLAG_TYPE_TICKET = 0 -- conserve les tickets valides uniquement
  -- Périmètre Jules Brice France
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  -- and (mag.type_emplacement = $EMPLACEMENT1 or mag.type_emplacement = $EMPLACEMENT2 or mag.type_emplacement = $EMPLACEMENT3 or mag.type_emplacement = $EMPLACEMENT4 or mag.type_emplacement = $EMPLACEMENT5 or mag.type_emplacement = $EMPLACEMENT6)
  and c.ID_TITRE not in (0,4)
  and vd.CODE_CLIENT not like '%HPC%'
),



