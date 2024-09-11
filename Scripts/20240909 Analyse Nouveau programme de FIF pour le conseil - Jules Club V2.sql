-- Analyse Nouveau programme de FIF pour le conseil 
  -- date de lancement du programme de FID 

/*  Analyse 1 
VISION GLOBALE DU PROGRAMME DE FIDELITE :

Période d’analyse : depuis le 24/04 jusqu’au 31/08 en N comparé à N-1
France Belgique magasins + web
Evolution par mois + un global

 Split Clients Club / Clients Club + / Non Club

Pour ces 3 segments : 
CA / Quantités / Marge €
GENEROSITE = Taux de remise moyen
IV/PM 
CAPACITE RECRUTEMENT ?
Taux de recrutement 
TAUX NOURRITURE ?

***/ 

/**** Test des informations pour la table a supp **
SELECT Code_client, DATE_DERNIER_ACHAT, nombre_points_fidelite, DATE_RECRUTEMENT, ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT 
FROM  DHB_PROD.DNR.DN_CLIENT
WHERE CODE_CLIENT ='000111009774'; 

SELECT DATE_RECRUTEMENT  FROM DHB_PROD.DNR.DN_CLIENT; 

SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, nombre_points_fidelite, DATE_RECRUTEMENT
FROM DHB_PROD.DNR.DN_CLIENT
WHERE FLAG_ACTIF=1 AND CODE_CLIENT ='000111009774'; 


SELECT * FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM WHERE CODE_CLIENT ='000111009774' ORDER BY DATE_DEBUT ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.RFM_SEGMENTATION WHERE CODE_CLIENT ='000111009774' ORDER BY DATE_DEBUT ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.RFM_SEGMENTATION WHERE LIB_MACRO_SEGMENT = 'VIP';
*/

SET dtfin_jclub='2024-08-31';
SET dtdeb_jclub='2024-04-24';

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.info_jclub AS 
WITH tab_rfm AS (SELECT DISTINCT Code_client, ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM WHERE EST_COURANT = 1), 
tab_omni AS (SELECT DISTINCT Code_client, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI WHERE EST_COURANT = 1), 
tab_pts AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, nombre_points_fidelite, DATE_RECRUTEMENT
FROM DHB_PROD.DNR.DN_CLIENT
WHERE FLAG_ACTIF=1 ),
tickets as (
Select   vd.CODE_CLIENT AS id_clt,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat,
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, vd.MONTANT_REMISE_OPE_COMM, 
vd.montant_remise +  vd.MONTANT_REMISE_OPE_COMM AS remise_total, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
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
where vd.date_ticket BETWEEN $dtdeb_jclub AND $dtfin_jclub
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0'
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  )
 SELECT a.*, b.ID_MACRO_SEGMENT, b.LIB_MACRO_SEGMENT , c.LIB_SEGMENT_OMNI, 
 e.*,
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
CASE WHEN LIB_SEGMENT_OMNI='WEB' THEN '02-WEB'
     WHEN LIB_SEGMENT_OMNI='OMNI' THEN '03-OMNI'
     ELSE '01-MAG' END AS SEGMENT_OMNI,  
CASE WHEN nombre_points_fidelite BETWEEN 0 AND 99 THEN '0_99pts' 
     WHEN nombre_points_fidelite BETWEEN 100 AND 199 THEN '100_199pts'
    WHEN nombre_points_fidelite BETWEEN 200 AND 299 THEN '200_299pts'
 WHEN nombre_points_fidelite >=300 THEN '300pts & +'
   ELSE '99_Null' END AS cat_points,
CASE WHEN nombre_points_fidelite BETWEEN 0 AND 299 THEN '02-JClub'
      WHEN nombre_points_fidelite >=300 THEN '01-JClub_prem'
   ELSE '99_Null' END AS cat_Jclub, 
   CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN '02-Nouveaux' ELSE '01-Anciens' END AS typo_clt
FROM tab_pts a 
LEFT JOIN tab_rfm b ON a.code_client=b.code_client
LEFT JOIN tab_omni c ON a.code_client=c.code_client
LEFT JOIN tickets e ON a.code_client=e.id_clt; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.info_jclub; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_jclub AS 
SELECT * FROM (
(SELECT '00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN Id_clt end ) AS nb_clt_actif,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN remise_total end) AS Remise_clt,
Count(DISTINCT CASE WHEN cat_Jclub='01-JClub_prem' THEN Code_client END) AS nb_clt_Jprem, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN Id_clt end ) AS nb_clt_actif_Jprem,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN id_ticket end ) AS nb_ticket_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_TTC end) AS CA_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN QUANTITE_LIGNE end) AS QTE_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN remise_total end) AS Remise_clt_Jprem,
Count(DISTINCT CASE WHEN cat_Jclub='02-JClub' THEN Code_client END) AS nb_clt_J, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN Id_clt end ) AS nb_clt_actif_J,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN id_ticket end ) AS nb_ticket_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_TTC end) AS CA_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN QUANTITE_LIGNE end) AS QTE_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN remise_total end) AS Remise_clt_J,
Count(DISTINCT CASE WHEN cat_points='0_99pts' THEN Code_client END) AS nb_clt_000pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN Id_clt end ) AS nb_clt_actif_000pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN id_ticket end ) AS nb_ticket_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_TTC end) AS CA_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN QUANTITE_LIGNE end) AS QTE_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN remise_total end) AS Remise_clt_000pts,
Count(DISTINCT CASE WHEN cat_points='100_199pts' THEN Code_client END) AS nb_clt_100pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN Id_clt end ) AS nb_clt_actif_100pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN id_ticket end ) AS nb_ticket_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_TTC end) AS CA_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN QUANTITE_LIGNE end) AS QTE_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN remise_total end) AS Remise_clt_100pts,
Count(DISTINCT CASE WHEN cat_points='200_299pts' THEN Code_client END) AS nb_clt_200pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN Id_clt end ) AS nb_clt_actif_200pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN id_ticket end ) AS nb_ticket_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_TTC end) AS CA_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN QUANTITE_LIGNE end) AS QTE_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN remise_total end) AS Remise_clt_200pts,
Count(DISTINCT CASE WHEN cat_points='300pts & +' THEN Code_client END) AS nb_clt_300pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN Id_clt end ) AS nb_clt_actif_300pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN id_ticket end ) AS nb_ticket_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_TTC end) AS CA_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN QUANTITE_LIGNE end) AS QTE_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN remise_total end) AS Remise_clt_300pts
FROM DATA_MESH_PROD_CLIENT.WORK.info_jclub
GROUP BY 1,2)
UNION 
(SELECT '01-Typo_client' AS Typo, typo_clt AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN Id_clt end ) AS nb_clt_actif,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN remise_total end) AS Remise_clt,
Count(DISTINCT CASE WHEN cat_Jclub='01-JClub_prem' THEN Code_client END) AS nb_clt_Jprem, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN Id_clt end ) AS nb_clt_actif_Jprem,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN id_ticket end ) AS nb_ticket_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_TTC end) AS CA_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN QUANTITE_LIGNE end) AS QTE_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN remise_total end) AS Remise_clt_Jprem,
Count(DISTINCT CASE WHEN cat_Jclub='02-JClub' THEN Code_client END) AS nb_clt_J, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN Id_clt end ) AS nb_clt_actif_J,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN id_ticket end ) AS nb_ticket_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_TTC end) AS CA_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN QUANTITE_LIGNE end) AS QTE_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN remise_total end) AS Remise_clt_J,
Count(DISTINCT CASE WHEN cat_points='0_99pts' THEN Code_client END) AS nb_clt_000pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN Id_clt end ) AS nb_clt_actif_000pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN id_ticket end ) AS nb_ticket_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_TTC end) AS CA_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN QUANTITE_LIGNE end) AS QTE_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN remise_total end) AS Remise_clt_000pts,
Count(DISTINCT CASE WHEN cat_points='100_199pts' THEN Code_client END) AS nb_clt_100pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN Id_clt end ) AS nb_clt_actif_100pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN id_ticket end ) AS nb_ticket_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_TTC end) AS CA_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN QUANTITE_LIGNE end) AS QTE_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN remise_total end) AS Remise_clt_100pts,
Count(DISTINCT CASE WHEN cat_points='200_299pts' THEN Code_client END) AS nb_clt_200pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN Id_clt end ) AS nb_clt_actif_200pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN id_ticket end ) AS nb_ticket_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_TTC end) AS CA_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN QUANTITE_LIGNE end) AS QTE_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN remise_total end) AS Remise_clt_200pts,
Count(DISTINCT CASE WHEN cat_points='300pts & +' THEN Code_client END) AS nb_clt_300pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN Id_clt end ) AS nb_clt_actif_300pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN id_ticket end ) AS nb_ticket_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_TTC end) AS CA_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN QUANTITE_LIGNE end) AS QTE_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN remise_total end) AS Remise_clt_300pts
FROM DATA_MESH_PROD_CLIENT.WORK.info_jclub
GROUP BY 1,2)
UNION 
(SELECT '02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN Id_clt end ) AS nb_clt_actif,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN remise_total end) AS Remise_clt,
Count(DISTINCT CASE WHEN cat_Jclub='01-JClub_prem' THEN Code_client END) AS nb_clt_Jprem, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN Id_clt end ) AS nb_clt_actif_Jprem,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN id_ticket end ) AS nb_ticket_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_TTC end) AS CA_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN QUANTITE_LIGNE end) AS QTE_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN remise_total end) AS Remise_clt_Jprem,
Count(DISTINCT CASE WHEN cat_Jclub='02-JClub' THEN Code_client END) AS nb_clt_J, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN Id_clt end ) AS nb_clt_actif_J,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN id_ticket end ) AS nb_ticket_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_TTC end) AS CA_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN QUANTITE_LIGNE end) AS QTE_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN remise_total end) AS Remise_clt_J,
Count(DISTINCT CASE WHEN cat_points='0_99pts' THEN Code_client END) AS nb_clt_000pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN Id_clt end ) AS nb_clt_actif_000pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN id_ticket end ) AS nb_ticket_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_TTC end) AS CA_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN QUANTITE_LIGNE end) AS QTE_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN remise_total end) AS Remise_clt_000pts,
Count(DISTINCT CASE WHEN cat_points='100_199pts' THEN Code_client END) AS nb_clt_100pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN Id_clt end ) AS nb_clt_actif_100pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN id_ticket end ) AS nb_ticket_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_TTC end) AS CA_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN QUANTITE_LIGNE end) AS QTE_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN remise_total end) AS Remise_clt_100pts,
Count(DISTINCT CASE WHEN cat_points='200_299pts' THEN Code_client END) AS nb_clt_200pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN Id_clt end ) AS nb_clt_actif_200pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN id_ticket end ) AS nb_ticket_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_TTC end) AS CA_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN QUANTITE_LIGNE end) AS QTE_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN remise_total end) AS Remise_clt_200pts,
Count(DISTINCT CASE WHEN cat_points='300pts & +' THEN Code_client END) AS nb_clt_300pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN Id_clt end ) AS nb_clt_actif_300pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN id_ticket end ) AS nb_ticket_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_TTC end) AS CA_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN QUANTITE_LIGNE end) AS QTE_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN remise_total end) AS Remise_clt_300pts
FROM DATA_MESH_PROD_CLIENT.WORK.info_jclub
GROUP BY 1,2)
UNION 
(SELECT '03-Segment RFM' AS Typo, SEGMENT_OMNI AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN Id_clt end ) AS nb_clt_actif,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN remise_total end) AS Remise_clt,
Count(DISTINCT CASE WHEN cat_Jclub='01-JClub_prem' THEN Code_client END) AS nb_clt_Jprem, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN Id_clt end ) AS nb_clt_actif_Jprem,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN id_ticket end ) AS nb_ticket_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_TTC end) AS CA_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN QUANTITE_LIGNE end) AS QTE_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN remise_total end) AS Remise_clt_Jprem,
Count(DISTINCT CASE WHEN cat_Jclub='02-JClub' THEN Code_client END) AS nb_clt_J, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN Id_clt end ) AS nb_clt_actif_J,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN id_ticket end ) AS nb_ticket_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_TTC end) AS CA_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN QUANTITE_LIGNE end) AS QTE_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN remise_total end) AS Remise_clt_J,
Count(DISTINCT CASE WHEN cat_points='0_99pts' THEN Code_client END) AS nb_clt_000pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN Id_clt end ) AS nb_clt_actif_000pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN id_ticket end ) AS nb_ticket_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_TTC end) AS CA_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN QUANTITE_LIGNE end) AS QTE_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN remise_total end) AS Remise_clt_000pts,
Count(DISTINCT CASE WHEN cat_points='100_199pts' THEN Code_client END) AS nb_clt_100pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN Id_clt end ) AS nb_clt_actif_100pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN id_ticket end ) AS nb_ticket_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_TTC end) AS CA_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN QUANTITE_LIGNE end) AS QTE_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN remise_total end) AS Remise_clt_100pts,
Count(DISTINCT CASE WHEN cat_points='200_299pts' THEN Code_client END) AS nb_clt_200pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN Id_clt end ) AS nb_clt_actif_200pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN id_ticket end ) AS nb_ticket_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_TTC end) AS CA_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN QUANTITE_LIGNE end) AS QTE_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN remise_total end) AS Remise_clt_200pts,
Count(DISTINCT CASE WHEN cat_points='300pts & +' THEN Code_client END) AS nb_clt_300pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN Id_clt end ) AS nb_clt_actif_300pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN id_ticket end ) AS nb_ticket_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_TTC end) AS CA_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN QUANTITE_LIGNE end) AS QTE_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN remise_total end) AS Remise_clt_300pts
FROM DATA_MESH_PROD_CLIENT.WORK.info_jclub
GROUP BY 1,2)
UNION 
(SELECT '04-Famille Achat' AS Typo, UPPER(lib_famille_achat) AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN Id_clt end ) AS nb_clt_actif,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' THEN remise_total end) AS Remise_clt,
Count(DISTINCT CASE WHEN cat_Jclub='01-JClub_prem' THEN Code_client END) AS nb_clt_Jprem, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN Id_clt end ) AS nb_clt_actif_Jprem,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN id_ticket end ) AS nb_ticket_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_TTC end) AS CA_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN QUANTITE_LIGNE end) AS QTE_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_Jprem,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='01-JClub_prem' THEN remise_total end) AS Remise_clt_Jprem,
Count(DISTINCT CASE WHEN cat_Jclub='02-JClub' THEN Code_client END) AS nb_clt_J, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN Id_clt end ) AS nb_clt_actif_J,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN id_ticket end ) AS nb_ticket_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_TTC end) AS CA_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN QUANTITE_LIGNE end) AS QTE_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_J,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_Jclub='02-JClub' THEN remise_total end) AS Remise_clt_J,
Count(DISTINCT CASE WHEN cat_points='0_99pts' THEN Code_client END) AS nb_clt_000pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN Id_clt end ) AS nb_clt_actif_000pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN id_ticket end ) AS nb_ticket_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_TTC end) AS CA_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN QUANTITE_LIGNE end) AS QTE_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_000pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='0_99pts' THEN remise_total end) AS Remise_clt_000pts,
Count(DISTINCT CASE WHEN cat_points='100_199pts' THEN Code_client END) AS nb_clt_100pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN Id_clt end ) AS nb_clt_actif_100pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN id_ticket end ) AS nb_ticket_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_TTC end) AS CA_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN QUANTITE_LIGNE end) AS QTE_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_100pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='100_199pts' THEN remise_total end) AS Remise_clt_100pts,
Count(DISTINCT CASE WHEN cat_points='200_299pts' THEN Code_client END) AS nb_clt_200pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN Id_clt end ) AS nb_clt_actif_200pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN id_ticket end ) AS nb_ticket_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_TTC end) AS CA_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN QUANTITE_LIGNE end) AS QTE_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_200pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='200_299pts' THEN remise_total end) AS Remise_clt_200pts,
Count(DISTINCT CASE WHEN cat_points='300pts & +' THEN Code_client END) AS nb_clt_300pts, 
Count(DISTINCT CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN Id_clt end ) AS nb_clt_actif_300pts,
Count(DISTINCT CASE WHEN Qte_pos>0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN id_ticket end ) AS nb_ticket_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_TTC end) AS CA_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN QUANTITE_LIGNE end) AS QTE_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN MONTANT_MARGE_SORTIE end) AS Marge_clt_300pts,
SUM(CASE WHEN annul_ticket=0 AND Id_clt IS NOT NULL AND Id_clt !='0' AND cat_points='300pts & +' THEN remise_total end) AS Remise_clt_300pts
FROM DATA_MESH_PROD_CLIENT.WORK.info_jclub
GROUP BY 1,2)
) ORDER BY 1,2;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_jclub ORDER BY 1,2 ;