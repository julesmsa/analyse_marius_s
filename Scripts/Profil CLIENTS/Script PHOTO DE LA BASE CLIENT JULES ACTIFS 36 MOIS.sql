-- PHOTO DE LA BASE CLIENT JULES ACTIFS 36 MOIS

SET dtdeb_EXON = Date('2021-05-01');
SET dtfin_EXON = DAte('2024-04-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_EXON, $dtfin_EXON;

SET dtdeb_EXONm1 = to_date(dateadd('year', -1, $dtdeb_EXON));
SET dtfin_EXONm1 = to_date(dateadd('year', -1, $dtfin_EXON));

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb_EXON, $dtfin_EXON, $dtdeb_EXONm1, $dtfin_EXONm1, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_client_test_CA_Sml AS
WITH
per_d AS (
Select vd.CODE_CLIENT,
MIN(date_ticket) AS min_date_ticket,
MIN(CASE WHEN type_emplacement IN ('EC','MP') THEN date_ticket END ) AS min_date_ticket_web,
MIN(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN date_ticket END ) AS min_date_ticket_mag,
MAX(date_ticket) AS max_date_ticket, 
MAX(CASE WHEN type_emplacement IN ('EC','MP') THEN date_ticket END ) AS max_date_ticket_web,
MAX(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN date_ticket END ) AS max_date_ticket_mag,
SUM(MONTANT_TTC) AS CAclt
FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
LEFT join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  GROUP BY 1
  ),
adress_clt AS ( SELECT DISTINCT code_client, id_type_contact, valeur_contact,  
num_voie, type_voie, libelle_voie, code_postal, ville, code_pays, qualite_adresse
FROM DATA_MESH_PROD_CLIENT.HUB.DMD_CLI_Contact),
info_clt AS (
SELECT a.*,
nom, prenom, id_magasin, id_magasin_courant, date_naissance,age, id_titre, titre, b.GENDER AS genre_clt, date_premier_achat, type_client, code_type_client,
date_dernier_achat, date_premier_achat_mag, date_dernier_achat_mag, date_premier_achat_ecom, date_dernier_achat_ecom, canal_entree, code_pays AS pays_clt,
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
vd.MONTANT_TTC,vd.PRIX_INIT_VENTE,  vd.PRIX_unitaire_base,
vd.PRIX_INIT_VENTE*vd.QUANTITE_LIGNE AS montant_init,
vd.PRIX_unitaire*vd.QUANTITE_LIGNE AS montant_unitaire,
vd.PRIX_unitaire_base*vd.QUANTITE_LIGNE AS montant_unitaire_base,
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
  ), 
  stat_ticket AS ( 
  SELECT Code_client 
    ,COUNT(distinct id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC) AS CA_clt
    ,SUM(montant_init) AS S_mtn_init
    ,SUM(montant_unitaire) AS S_mtn_unitaire
    ,SUM(montant_unitaire_base) AS S_mtn_unitaire_base
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge	
    ,SUM(montant_remise) AS S_Mnt_remise	
    FROM tickets
    GROUP BY 1
    HAVING CA_clt>=1 
    )
SELECT a.*, b.*, 



,datediff(MONTH ,COALESCE (date_premier_achat,c.min_date_ticket) ,$dtfin_EXON) AS ANCIENNETE_CLIENT
,datediff(MONTH ,COALESCE (date_premier_achat_mag,c.min_date_ticket_mag),$dtfin_EXON) AS ANCIENNETE_CLIENT_MAG
,datediff(MONTH ,COALESCE (date_premier_achat_Ecom,c.min_date_ticket_web),$dtfin_EXON) AS ANCIENNETE_CLIENT_WEB

,datediff(MONTH ,date_dernier_achat,$dtfin_EXON) AS tr_dernier_achat_CLIENT
,datediff(MONTH ,date_dernier_achat_mag,$dtfin_EXON) AS tr_dernier_achat_CLIENT_MAG
,datediff(MONTH ,date_dernier_achat_Ecom,$dtfin_EXON) AS tr_dernier_achat_CLIENT_WEB



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
			ELSE '01FRA_99_AUTRES/NC' END AS REGION, -- a completer avec les informations de la BEL			         
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
FROM stat_ticket a
JOIN info_clt b ON a.CODE_CLIENT=b.MASTER_CUSTOMER_ID 
LEFT JOIN per_d c ON a.CODE_CLIENT=c.CODE_CLIENT ;


