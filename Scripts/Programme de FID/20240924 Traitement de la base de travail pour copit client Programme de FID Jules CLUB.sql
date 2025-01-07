-- Traitement de la base de travail pour copit client Programme de FID Jules CLUB

SET dtfin_jclub = '2024-12-01';  -- Date de Calcul de la population Jules Club avec Activité sur les 12 derniers mois 
-- Date à partir du 
SET dtdeb_jclub = to_date(dateadd('year', -1, $dtfin_jclub));

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL'

SELECT $dtdeb_jclub, $dtfin_jclub, $ENSEIGNE1, $ENSEIGNE2, $PAYS1, $PAYS2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.infotick_jclub AS 
WITH tickets as (
Select   vd.CODE_CLIENT,
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
where vd.date_ticket BETWEEN Date($dtdeb_jclub) AND (DATE($dtfin_jclub)-1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  ),
  entite AS (SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com, lib_enseigne
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL'),
pts_fid_clt AS (SELECT DISTINCT CODE_CLIENT, nombre_points_fidelite AS nb_pts_fidelite_a_date
FROM DHB_PROD.HUB.D_CLI_HISTO_CLIENT 
WHERE DATE_DEBUT <= DATE($dtfin_jclub)
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0' ),
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
        genre, 
        date_recrutement, 
        -- nombre_points_fidelite, 
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
t_club AS (
SELECT distinct code_client , valeur
FROM DHB_PROD.HUB.D_CLI_INDICATEUR
where id_indic = 191 AND code_client IS NOT NULL AND code_client !='0'
and (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
)
SELECT a.*, b.* , 
c.ID_MACRO_SEGMENT , c.LIB_MACRO_SEGMENT,
e.LIB_SEGMENT_OMNI , f.valeur, nb_pts_fidelite_a_date,
--CASE 
--WHEN valeur=1 THEN '01-JClub Prem'  
--WHEN valeur=0 THEN '02-JClub' ELSE '99-NR/NC' END 
--AS statut_club,
CASE WHEN nb_pts_fidelite_a_date IS NOT NULL AND nb_pts_fidelite_a_date BETWEEN 0 AND 299 THEN '02-JClub' 
  WHEN nb_pts_fidelite_a_date IS NOT NULL AND nb_pts_fidelite_a_date>=300 THEN '01-JClub Prem'  ELSE '99-NR/NC' END AS statut_club,
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
CASE WHEN nb_pts_fidelite_a_date BETWEEN 0 AND 99 THEN '0_99pts' 
     WHEN nb_pts_fidelite_a_date BETWEEN 100 AND 199 THEN '100_199pts'
    WHEN nb_pts_fidelite_a_date BETWEEN 200 AND 299 THEN '200_299pts'
 WHEN nb_pts_fidelite_a_date >=300 THEN '300pts & +'
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
        CASE WHEN nb_lign=1 THEN anciennete_client END AS anciennete_client_V2,
        h.Id_entite, h.Code_entite, h.Lib_entite, h.id_region_com, h.lib_region_com, h.lib_grande_region_com, h.lib_enseigne
FROM tickets a 
LEFT JOIN info_clt b ON a.code_client=b.idclt
LEFT JOIN segrfm c ON a.code_client=c.code_client
LEFT JOIN segomni e ON a.code_client=e.code_client 
LEFT JOIN t_club f ON a.code_client=f.code_client
LEFT JOIN pts_fid_clt g ON a.code_client=g.code_client
LEFT JOIN entite h ON a.idmag_achat=h.id_entite 
; 

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub WHERE code_client='336190003473';


-- SELECT DISTINCT CODE_PAYS  FROM DATA_MESH_PROD_CLIENT.WORK.infotick_jclub ;



/*
 * 00-Global 
 * 01-pays 
 * 02-enseigne
 * 03 grande region 
 * 04 region 
 * 05 magasin
 */


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_Gbl AS 
SELECT * FROM (
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '00-GLOBAL' as Descrip, '00-GLOBAL' as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7)
ORDER BY 1,2,3,4,5,6,7 ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_enseigne AS 
SELECT * FROM (
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '01-ENSEIGNE' as Descrip, lib_enseigne as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7)
ORDER BY 1,2,3,4,5,6,7 ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GRANDE_REGION AS 
SELECT * FROM (
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '02-GRANDE REGION' as Descrip, lib_grande_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7)
ORDER BY 1,2,3,4,5,6,7 ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_REGION AS 
SELECT * FROM (
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '03-REGION' as Descrip, lib_region_com as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7)
ORDER BY 1,2,3,4,5,6,7 ;




CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_PAYS AS 
SELECT * FROM (
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '04-PAYS' as Descrip, CODE_PAYS as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7)
ORDER BY 1,2,3,4,5,6,7 ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_magasins AS 
SELECT * FROM (
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7
UNION 
SELECT '05-MAGASIN' as Descrip, mag_achat as  type_mag, CASE WHEN clt_ident=1 THEN '01-Client Identifié' ELSE '02-Client NC' END AS t_client,
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
GROUP BY 1,2,3,4,5,6,7)
ORDER BY 1,2,3,4,5,6,7 ;



CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_M AS 
WITH tab0 AS (SELECT * FROM (
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_Gbl
union
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_enseigne
union  
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GRANDE_REGION 
union 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_REGION 
union
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_magasins))
SELECT a.* 
,CASE WHEN nb_client IS NOT NULL AND nb_client>0 THEN Round(CA_clt/nb_client,4) END AS CA_par_clt_glb
,CASE WHEN nb_client IS NOT NULL AND nb_client>0 THEN Round(nb_ticket/nb_client,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 THEN Round(CA_clt/nb_ticket,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 THEN Round(QTE_clt/nb_ticket,4) END AS idv_clt_glb        
,CASE WHEN QTE_clt IS NOT NULL AND QTE_clt>0 THEN Round(CA_clt/QTE_clt,4) END AS pvm_clt_glb      
,CASE WHEN CA_clt IS NOT NULL AND CA_clt>0 THEN Round(Marge_clt/CA_clt,4) END AS txmarge_clt_glb   
,CASE WHEN CA_clt IS NOT NULL AND CA_clt>0 THEN Round(Remise_glb/(CA_clt + Remise_glb),4) END AS txremise_clt_glb
,DATE($dtfin_jclub) AS Date_Calcul
FROM tab0 a
ORDER BY 1,2,3,4,5,6,7 ;



SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_M LIMIT 10 ; 


-- Changement des noms de variable pour la table N

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_DECEMBRE24 AS 
WITH oldtab AS (
SELECT DESCRIP,	TYPE_MAG,	T_CLIENT,	LIB_STATUT_CLIENT,	STATUT_CLIENT,	TYPO,	MODALITE,
NB_CLIENT  as  NB_CLIENT_Nm1,
NB_TICKET  as  NB_TICKET_Nm1,
CA_CLT  as  CA_CLT_Nm1,
QTE_CLT  as  QTE_CLT_Nm1,
MARGE_CLT  as  MARGE_CLT_Nm1,
REMISE_MKT  as  REMISE_MKT_Nm1,
REMISE_GLB  as  REMISE_GLB_Nm1,
AGE_MOY  as  AGE_MOY_Nm1,
ANCIENNETE_MOY  as  ANCIENNETE_MOY_Nm1,
NB_NEWCLIENT  as  NB_NEWCLIENT_Nm1,
CA_PAR_CLT_GLB  as  CA_PAR_CLT_GLB_Nm1,
FREQ_CLT_GLB  as  FREQ_CLT_GLB_Nm1,
PANIER_CLT_GLB  as  PANIER_CLT_GLB_Nm1,
IDV_CLT_GLB  as  IDV_CLT_GLB_Nm1,
PVM_CLT_GLB  as  PVM_CLT_GLB_Nm1,
TXMARGE_CLT_GLB  as  TXMARGE_CLT_GLB_Nm1,
TXREMISE_CLT_GLB  as  TXREMISE_CLT_GLB_Nm1,
DATE('2023-09-01')  as  DATE_CALCUL_Nm1
FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_OLD)
SELECT a.* , 
NB_CLIENT_Nm1,
NB_TICKET_Nm1,
CA_CLT_Nm1,
QTE_CLT_Nm1,
MARGE_CLT_Nm1,
REMISE_MKT_Nm1,
REMISE_GLB_Nm1,
AGE_MOY_Nm1,
ANCIENNETE_MOY_Nm1,
NB_NEWCLIENT_Nm1,
CA_PAR_CLT_GLB_Nm1,
FREQ_CLT_GLB_Nm1,
PANIER_CLT_GLB_Nm1,
IDV_CLT_GLB_Nm1,
PVM_CLT_GLB_Nm1,
TXMARGE_CLT_GLB_Nm1,
TXREMISE_CLT_GLB_Nm1,
DATE_CALCUL_Nm1
FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_M a
LEFT JOIN oldtab b ON a.Descrip=b.Descrip  AND a.type_mag=b.type_mag AND  a.t_client=b.t_client AND  a.Lib_Statut_client=b.Lib_Statut_client AND  a.Statut_client=b.Statut_client AND  a.Typo=b.Typo AND  a.modalite=b.modalite ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_DECEMBRE24 ORDER BY 1,2,3,4,5,6,7 ; 


/* 
 * 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GBLDFT AS 
SELECT * FROM (  
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_MARS24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_AVRIL24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_MAI24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_JUIN24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_JUILLET24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_AOUT24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_SEPTEMBRE24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_OCTOBRE24 
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_NOVEMBRE24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_DECEMBRE24 
)
ORDER BY 1,2,3,4,5,6,7 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GBLDFT ORDER BY 1,2,3,4,5,6,7 ; 

-- UPDATE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GBLDFT SET DATE_CALCUL_NM1 = DATE('2024-02-01');  -- correction de la date de calcul de nm1 ; 

SELECT DISTINCT DATE_CALCUL FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GBLDFT ORDER BY 1,2,3,4,5,6,7 ;

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_FINAL AS 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_GBLDFT ORDER BY 1,2,3,4,5,6,7 ;


/*
 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_MARS24 ORDER BY 1,2,3,4,5,6,7 ;   
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_AVRIL24 ORDER BY 1,2,3,4,5,6,7 ; 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_MAI24 ORDER BY 1,2,3,4,5,6,7 ; 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_JUIN24 ORDER BY 1,2,3,4,5,6,7 ; 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_JUILLET24 ORDER BY 1,2,3,4,5,6,7 ; 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_AOUT24 ORDER BY 1,2,3,4,5,6,7 ;
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_SEPTEMBRE24 ORDER BY 1,2,3,4,5,6,7 ; 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_OCTOBRE24 ORDER BY 1,2,3,4,5,6,7 ; 
 
CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_FINAL AS 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_FINAL_V2 ORDER BY 1,2,3,4,5,6,7 ;


SELECT DISTINCT Descrip,type_mag, t_client, Lib_Statut_client, Statut_client, Typo, modalite, nb_client
FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_FINAL
WHERE Typo='00-Global' 
ORDER BY 1,2,3,4,5,6,7 ; 

*/

 /*
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Statclt_jclub_FINAL 
WHERE DATE_CALCUL ='2024-10-01'ORDER BY 1,2,3,4,5,6,7 ; 
MODALITE = '00-Global' AND LIB_STATUT_CLIENT = '00-Global' AND TYPE_MAG = '00-GLOBAL'
AND 







