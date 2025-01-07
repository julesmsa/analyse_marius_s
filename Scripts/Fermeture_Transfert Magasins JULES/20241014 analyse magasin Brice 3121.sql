-- Analyse Fermeture Mag , Jeremy 

/* Communication 
 * 
	Contexte : 
o	Fermeture du magasin Brice 3121 sur le site de Lyon Part Dieu (dernier jour de vente le 9 novembre 24), avec maintien du magasin Jules 113 (XL prévue en septembre 25)

	Objectif : 
o	Obtenir les données sur les clients de Brice qui sont également acheteurs chez Jules et plus en général, ne pas perdre les clients Brice.

	Informations demandées :
o	La part de ces clients
o	Typologie des clients concernés
o	Détails sur les familles de produits achetés chez Brice (ainsi que tailles, coupes, etc.)
o	Précision sur le statut "promophile" de ces clients

*/


/*** Périmetre d'études 12 dernier mois à fin septembre ***/ 


SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- Date de fin ticket pour avoir des données stables 
SET dtfin = Date('2024-12-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dtdeb = Date('2023-10-01');
SET idmag_etude = 3121 ; 
SET idmag_cible = 113 ; 
SELECT $dtfin,  $dtdeb, $idmag_etude, $idmag_cible ;

--- Table d'identification des clients Brice sur la période d'étude 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.IDCLT AS
WITH tab0 AS (SELECT DISTINCT CODE_CLIENT AS Id_Client,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
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
FROM DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN DATE($dtdeb) AND (DATE($dtfin)-1) 
AND  vd.ID_MAGASIN = $idmag_etude -- listes des familles a analyser on peut analyse un ou plusieurs familles  
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0' ),
tab1 AS (SELECT Id_Client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
FROM tab0 
GROUP BY 1
HAVING nb_ticket>0 AND CA_clt>0
)
SELECT DISTINCT Id_Client,  1 AS top_brice
FROM tab1; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.IDCLT;
    
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
    WHERE code_pays IN  ($PAYS1 ,$PAYS2) AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL ),
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
FROM DHB_PROD.DNR.DN_VENTE vd
INNER JOIN DATA_MESH_PROD_CLIENT.WORK.IDCLT id ON vd.CODE_CLIENT=id.ID_CLIENT
where vd.date_ticket BETWEEN Date($dtdeb) AND (DATE($dtfin)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')  
SELECT a.*, 
b.*,  ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI
,datediff(MONTH ,date_recrutement,Date($dtfin)) AS ANCIENNETE_CLIENT
,CASE WHEN Date(date_recrutement) BETWEEN DATE($dtdeb) AND (DATE($dtfin)-1) THEN '02-Nouveaux' ELSE '01-Anciens' END AS Type_client
,ROUND(DATEDIFF(YEAR,date_naissance,Date($dtfin)),2) AS AGE_C
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
     else 'a: [0-12] mois'  END  AS Tr_anciennete, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2,
CASE WHEN nb_lign=1 THEN anciennete_client END AS anc_client
FROM tickets a
INNER JOIN info_clt b ON a.CODE_CLIENT=b.idclt
INNER JOIN DATA_MESH_PROD_CLIENT.WORK.IDCLT id ON a.CODE_CLIENT=id.ID_CLIENT
LEFT JOIN segrfm c ON a.code_client=c.code_client
LEFT JOIN segomni e ON a.code_client=e.code_client ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS ; 

 -- Statistique globale 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_CLTMAG_B AS
SELECT * FROM 
(
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '01_GENRE' AS typo_clt, CASE 	
 WHEN GENRE='H' THEN '01-Hommes'
 WHEN GENRE='F' THEN '02-Femmes'
 ELSE '01-Hommes'  END AS modalite
 ,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite 
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '03_CAT_SEGMENT_RFM' AS typo_clt, CASE WHEN CAT_SEGMENT_RFM='04_Inactifs' THEN '09_Non_Segmentes' ELSE CAT_SEGMENT_RFM END AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '04_SEGMENT_RFM' AS typo_clt, CASE WHEN SEGMENT_RFM IN ('10_INA12','11_INA24','12_NOSEG') THEN '99_NOSEG' ELSE SEGMENT_RFM end AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '05_OMNICANALITE' AS typo_clt, 
 CASE WHEN lib_segment_omni ='MAG' then '01-MAG' 
      When lib_segment_omni ='WEB' then '03-OMNI'
      When lib_segment_omni ='OMNI' then '03-OMNI' ELSE '01-MAG' end as modalite -- ON garde 2 Statu mag ou omni 
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '06_CANAL_ACHAT' AS typo_clt, PERIMETRE AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '07A_ANCIENNETE CLIENT' AS typo_clt, Tr_anciennete AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION 
SELECT '07B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite
,AVG(anc_client) AS nbclt_glb
,AVG(CASE WHEN Qte_pos>0 THEN anc_client end) AS nbticket_glb
,AVG(CASE WHEN annul_ticket=0 THEN anc_client end) AS CA_glb
,AVG(CASE WHEN annul_ticket=0 THEN anc_client end) AS qte_glb
,AVG(CASE WHEN annul_ticket=0 THEN anc_client end) AS Marge_glb
,AVG(CASE WHEN annul_ticket=0 THEN anc_client end) AS remise_glb
,AVG(CASE WHEN ID_MAGASIN=$idmag_etude THEN anc_client END) AS nbclt_BRICE
,AVG(CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN anc_client end) AS nbticket_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN anc_client end) AS CA_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN anc_client end) AS qte_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN anc_client end) AS Marge_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN anc_client end) AS remise_BRICE
,AVG(CASE WHEN ID_MAGASIN=$idmag_cible THEN anc_client END) AS nbclt_JULES
,AVG(CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN anc_client end) AS nbticket_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN anc_client end) AS CA_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN anc_client end) AS qte_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN anc_client end) AS Marge_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN anc_client end) AS remise_JULES
,AVG( CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN anc_client END) AS nbclt_OTHERS
,AVG( CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN anc_client end ) AS nbticket_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN anc_client end) AS CA_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN anc_client end) AS qte_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN anc_client end) AS Marge_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN anc_client end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '08A_AGE' AS typo_clt,  
CASE WHEN CLASSE_AGE IN ('75-79','80-84','85-89','90-94') THEN '75-94' ELSE CLASSE_AGE END AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '08B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite
,AVG(AGE_C2) AS nbclt_glb
,AVG(CASE WHEN Qte_pos>0 THEN AGE_C2 end) AS nbticket_glb
,AVG(CASE WHEN annul_ticket=0 THEN AGE_C2 end) AS CA_glb
,AVG(CASE WHEN annul_ticket=0 THEN AGE_C2 end) AS qte_glb
,AVG(CASE WHEN annul_ticket=0 THEN AGE_C2 end) AS Marge_glb
,AVG(CASE WHEN annul_ticket=0 THEN AGE_C2 end) AS remise_glb
,AVG(CASE WHEN ID_MAGASIN=$idmag_etude THEN AGE_C2 END) AS nbclt_BRICE
,AVG(CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN AGE_C2 end) AS nbticket_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN AGE_C2 end) AS CA_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN AGE_C2 end) AS qte_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN AGE_C2 end) AS Marge_BRICE
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN AGE_C2 end) AS remise_BRICE
,AVG(CASE WHEN ID_MAGASIN=$idmag_cible THEN AGE_C2 END) AS nbclt_JULES
,AVG(CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN AGE_C2 end) AS nbticket_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN AGE_C2 end) AS CA_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN AGE_C2 end) AS qte_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN AGE_C2 end) AS Marge_JULES
,AVG(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN AGE_C2 end) AS remise_JULES
,AVG( CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN AGE_C2 END) AS nbclt_OTHERS
,AVG( CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN AGE_C2 end ) AS nbticket_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN AGE_C2 end) AS CA_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN AGE_C2 end) AS qte_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN AGE_C2 end) AS Marge_OTHERS
,AVG(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN AGE_C2 end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '09_OPTIN_SMS' AS typo_clt,  CASE WHEN est_optin_sms_com=1 or est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '10_OPTIN_EMAIL' AS typo_clt,  CASE WHEN est_optin_email_com=1 or est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '22_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'  END  AS modalite
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2
UNION
SELECT '23_FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_etude THEN Code_Client END) AS nbclt_BRICE
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_etude THEN id_ticket end ) AS nbticket_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_TTC end) AS CA_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN QUANTITE_LIGNE end) AS qte_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN MONTANT_MARGE_SORTIE end) AS Marge_BRICE
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_etude THEN montant_remise end) AS remise_BRICE
,Count(DISTINCT CASE WHEN ID_MAGASIN=$idmag_cible THEN Code_Client END) AS nbclt_JULES
,Count(DISTINCT CASE WHEN Qte_pos>0 AND ID_MAGASIN=$idmag_cible THEN id_ticket end) AS nbticket_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_TTC end) AS CA_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN QUANTITE_LIGNE end) AS qte_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN MONTANT_MARGE_SORTIE end) AS Marge_JULES
,SUM(CASE WHEN annul_ticket=0 AND ID_MAGASIN=$idmag_cible THEN montant_remise end) AS remise_JULES
,Count(DISTINCT CASE WHEN PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN Code_Client END) AS nbclt_OTHERS
,Count(DISTINCT CASE WHEN Qte_pos>0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN id_ticket end ) AS nbticket_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_TTC end) AS CA_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN QUANTITE_LIGNE end) AS qte_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN MONTANT_MARGE_SORTIE end) AS Marge_OTHERS
,SUM(CASE WHEN annul_ticket=0 AND PERIMETRE = 'MAG' AND ID_MAGASIN NOT IN ($idmag_cible,$idmag_etude) THEN montant_remise end) AS remise_OTHERS
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2 
 ) ORDER BY 1,2 ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_CLTMAG_B ORDER BY 1,2 ;  

/*

--- Info par magasins 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS ;

SELECT Mag_achat AS typo_clt,  
 Lib_magasin AS modalite 
,Count(DISTINCT Code_Client) AS nbclt_glb
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nbticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1,2 
ORDER BY 1,2 ;

-- Information sur Nombre de Magasins par client Hors périmetre


With tag0 as (SELECT Code_Client
,Count(DISTINCT CASE WHEN  perimetre='MAG' THEN Mag_achat END ) AS nbmag
,Count(DISTINCT CASE WHEN  perimetre='WEB' THEN Mag_achat END ) AS nbweb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS
GROUP BY 1),
tag1 as ( Select *, 
Case when nbmag=1 then '01-Mag Unique' 
     when nbmag>1 then '02-Mag Multi' else 'Mag0' end as tr_nbmag
From tag0)
Select tr_nbmag, nbweb
,Count(DISTINCT Code_Client) AS nbclt
FRom tag1
Group by 1,2
Order by 1,2 ; 


