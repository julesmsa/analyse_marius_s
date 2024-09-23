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




-- Revoir les informations en fonction 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.infotick_jclub AS 
WITH tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat,
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
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
    ELSE 0 END AS annul_ticket, 
CASE WHEN vd.code_client IS NOT NULL AND vd.code_client !='0' THEN 1 ELSE 0 END AS clt_ident 
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN $dtdeb_jclub AND $dtfin_jclub
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  ),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin_jclub)
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin_jclub) 
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        gender, 
        date_recrutement, nombre_points_fidelite, 
    DATEDIFF(MONTH, date_recrutement, $dtfin_jclub) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtdeb_jclub) AND DATE($dtfin_jclub)  THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin_jclub)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_client IS NOT NULL AND code_client !='0'),
t_club AS (SELECT distinct code_client , valeur
FROM DHB_PROD.HUB.D_CLI_INDICATEUR
where id_indic = 191 AND code_client IS NOT NULL AND code_client !='0'
and (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2))
SELECT a.*, b.* , 
c.ID_MACRO_SEGMENT , c.LIB_MACRO_SEGMENT,
e.LIB_SEGMENT_OMNI , f.valeur, 
CASE 
WHEN valeur=1 THEN '01-JClub Prem'  
WHEN valeur=0 THEN '02-JClub' ELSE '99-NR/NC' END 
AS statut_club,
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
   CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN '02-Nouveaux' ELSE '01-Anciens' END AS typo_clt,
    CASE 
        WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
        ELSE 'z: Non def' 
    END AS Tr_anciennete, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,    
    CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2,
        CASE WHEN nb_lign=1 THEN anciennete_client END AS anciennete_client_V2
FROM tickets a 
LEFT JOIN info_clt b ON a.code_client=b.idclt
LEFT JOIN segrfm c ON a.code_client=c.code_client
LEFT JOIN segomni e ON a.code_client=e.code_client 
LEFT JOIN t_club f ON a.code_client=f.code_client; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub WHERE code_client='336190003473'; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub AS 
SELECT * FROM (
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'00-Global' AS Lib_Statut_client,
'00-Global' AS Statut_client,
'00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'00-Global' AS Lib_Statut_client,
'00-Global' AS Statut_client,
'01-Typo_client' AS Typo, typo_clt AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'00-Global' AS Lib_Statut_client,
'00-Global' AS Statut_client,
'02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'00-Global' AS Lib_Statut_client,
'00-Global' AS Statut_client,
'03-Segment OMNI' AS Typo, SEGMENT_OMNI AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'00-Global' AS Lib_Statut_client,
'00-Global' AS Statut_client,
'04-Famille Achat' AS Typo, UPPER(lib_famille_achat) AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'01-Statut_Club' AS Lib_Statut_client,
statut_club AS Statut_client,
'00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'01-Statut_Club' AS Lib_Statut_client,
statut_club AS Statut_client,
'01-Typo_client' AS Typo, typo_clt AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'01-Statut_Club' AS Lib_Statut_client,
statut_club AS Statut_client,
'02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'01-Statut_Club' AS Lib_Statut_client,
statut_club AS Statut_client,
'03-Segment OMNI' AS Typo, SEGMENT_OMNI AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'01-Statut_Club' AS Lib_Statut_client,
statut_club AS Statut_client,
'04-Famille Achat' AS Typo, UPPER(lib_famille_achat) AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'02-Points FId' AS Lib_Statut_client,
cat_points AS Statut_client,
'00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'02-Points FId' AS Lib_Statut_client,
cat_points AS Statut_client,
'01-Typo_client' AS Typo, typo_clt AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'02-Points FId' AS Lib_Statut_client,
cat_points AS Statut_client,
'02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'02-Points FId' AS Lib_Statut_client,
cat_points AS Statut_client,
'03-Segment OMNI' AS Typo, SEGMENT_OMNI AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5
UNION 
SELECT CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
'02-Points FId' AS Lib_Statut_client,
cat_points AS Statut_client,
'04-Famille Achat' AS Typo, UPPER(lib_famille_achat) AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_clt,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QTE_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Remise_mkt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb,
ROUND (AVG (CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END),1) AS age_moy,
ROUND (AVG (anciennete_client_V2),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb_jclub AND $dtfin_jclub THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub
GROUP BY 1,2,3,4,5)
ORDER BY 1,2,3,4,5 ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub ORDER BY 1,2,3,4,5;