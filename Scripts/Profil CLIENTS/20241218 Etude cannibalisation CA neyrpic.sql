--- Étude cannibalisation CA neyrpic

SET dtdeb = Date('2023-10-01');
SET dtfin = Date('2024-12-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dt_init = Date('2024-10-02');

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET LIB_ENSEIGNE1 = 'JULES'; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET LIB_ENSEIGNE2 = 'BRICE';
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL'


/****** Message 
L'idée c'est d'analyser le taux de client du mag  758 - GRENOBLE ST MARTIN CC NEYRPIC qui avait effectué leur dernier achat dans un des mags suivants :
204 - GRENOBLE CV R. DE LA POSTE
123 - GRENOBLE CC GD PLACE
3125 - GRENOBLE CC GD PLACE
350 - GRENOBLE ÉCHIROLLES CC LECLERC

***/ 


/**** information Magasins **/

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.info_mag AS 
SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com,
type_emplacement, lib_statut, id_concept, lib_enseigne, code_pays, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale  
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND CODE_PAYS IN ($PAYS1, $PAYS2) 
AND LIB_ENSEIGNE IN ($LIB_ENSEIGNE1, $LIB_ENSEIGNE2) AND Id_entite IN (758,204,123,3125,350) ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.info_mag ; 

SET dt_init = Date('2024-10-02');

/**** Information Base tickets  */

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag AS 
WITH 
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
tickets as (
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
CASE WHEN vd.code_client IS NOT NULL AND vd.code_client !='0' THEN 1 ELSE 0 END AS clt_ident,
CASE WHEN date_ticket BETWEEN Date('2024-10-02') AND DATE('2024-12-01') AND idmag_achat=758 THEN 1 ELSE 0 END AS top_mag758,
CASE WHEN date_ticket BETWEEN Date('2023-10-01') AND DATE('2024-10-01') AND idmag_achat=204 THEN 1 ELSE 0 END AS top_mag204,
CASE WHEN date_ticket BETWEEN Date('2023-10-01') AND DATE('2024-10-01') AND idmag_achat=123 THEN 1 ELSE 0 END AS top_mag123,
CASE WHEN date_ticket BETWEEN Date('2023-10-01') AND DATE('2024-10-01') AND idmag_achat=3125 THEN 1 ELSE 0 END AS top_mag3125,
CASE WHEN date_ticket BETWEEN Date('2023-10-01') AND DATE('2024-10-01') AND idmag_achat=350 THEN 1 ELSE 0 END AS top_mag350
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb) AND (DATE($dtfin)-1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)
  AND vd.ID_MAGASIN IN (758,204,123,3125,350)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' ),
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN Date($dt_init) AND (DATE($dtfin)-1) THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin - 1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE CODE_CLIENT IN ( SELECT DISTINCT CODE_CLIENT FROM tickets) ),
ticket_v AS (SELECT * , 
MAX(top_mag758) OVER (PARTITION BY CODE_CLIENT) AS clt_mag758,
MAX(top_mag204) OVER (PARTITION BY CODE_CLIENT) AS clt_mag204,
MAX(top_mag123) OVER (PARTITION BY CODE_CLIENT) AS clt_mag123,
MAX(top_mag3125) OVER (PARTITION BY CODE_CLIENT) AS clt_mag3125,
MAX(top_mag350) OVER (PARTITION BY CODE_CLIENT) AS clt_mag350
FROM tickets a)
SELECT a.*,
    CASE 
        WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
        ELSE 'z: Non def' 
    END AS Tr_anciennete , 
ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI, 
CASE WHEN g.id_macro_segment = '01' THEN '01_VIP' 
     WHEN g.id_macro_segment = '02' THEN '02_TBC'
     WHEN g.id_macro_segment = '03' THEN '03_BC'
     WHEN g.id_macro_segment = '04' THEN '04_MOY'
     WHEN g.id_macro_segment = '05' THEN '05_TAP'
     WHEN g.id_macro_segment = '06' THEN '06_TIEDE'
     WHEN g.id_macro_segment = '07' THEN '07_TPURG'
     WHEN g.id_macro_segment = '09' THEN '08_NCV'
     WHEN g.id_macro_segment = '08' THEN '09_NAC'
  ELSE '12_NOSEG' END AS SEGMENT_RFM ,
  CASE WHEN f.LIB_SEGMENT_OMNI='OMNI' THEN '03-OMNI'
       WHEN f.LIB_SEGMENT_OMNI='MAG' THEN '01-MAG'
       WHEN f.LIB_SEGMENT_OMNI='WEB' THEN '02-WEB'
       ELSE '09-NR/NC' END AS SEGMENT_OMNI, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign, c.*,
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2   
FROM ticket_v a
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt 
LEFT JOIN segrfm g ON a.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON a.CODE_CLIENT=f.CODE_CLIENT ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag; 




/*** informations global par client ***/ 


-- Analyse profil du magasin 758

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_mag758 AS 
SELECT * FROM (
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '01_GENRE' AS typo_clt, CASE 	
 WHEN GENRE='F' THEN '02-Femmes'
 ELSE '01-Hommes'  END AS modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '03_SEGMENT_RFM' AS typo_clt,  SEGMENT_RFM AS modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '04_OMNICANALITE' AS typo_clt, SEGMENT_OMNI as modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '05A_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION 
SELECT '05B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite,
AVG(CASE WHEN anciennete_client IS NOT NULL THEN anciennete_client END) AS nb_clt,
AVG(CASE WHEN anciennete_client IS NOT NULL THEN anciennete_client END) AS nb_ticket,
AVG(CASE WHEN anciennete_client IS NOT NULL THEN anciennete_client END) AS Mtn_CA,
AVG(CASE WHEN anciennete_client IS NOT NULL THEN anciennete_client END) AS Qte_clt,
AVG(CASE WHEN anciennete_client IS NOT NULL THEN anciennete_client END) AS Marge_clt,
AVG(CASE WHEN anciennete_client IS NOT NULL THEN anciennete_client END) AS Rem_clt,
AVG(CASE WHEN anciennete_client IS NOT NULL AND clt_mag204=1 THEN anciennete_client END ) AS nbclt_mag204,
AVG(CASE WHEN anciennete_client IS NOT NULL AND clt_mag123=1 THEN anciennete_client END ) AS nbclt_mag123,
AVG(CASE WHEN anciennete_client IS NOT NULL AND clt_mag3125=1 THEN anciennete_client END ) AS nbclt_mag3125,
AVG(CASE WHEN anciennete_client IS NOT NULL AND clt_mag350=1 THEN anciennete_client END ) AS nbclt_mag350    
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '06A_AGE' AS typo_clt,  CASE WHEN CLASSE_AGE IN ('80-84','85-89','90-94','95-99') THEN '80 et +' ELSE CLASSE_AGE END AS modalite,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt,
COUNT(DISTINCT CASE WHEN clt_mag204=1 THEN Code_client END ) AS nbclt_mag204,
COUNT(DISTINCT CASE WHEN clt_mag123=1 THEN Code_client END ) AS nbclt_mag123,
COUNT(DISTINCT CASE WHEN clt_mag3125=1 THEN Code_client END ) AS nbclt_mag3125,
COUNT(DISTINCT CASE WHEN clt_mag350=1 THEN Code_client END ) AS nbclt_mag350
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
    GROUP BY 1,2
UNION
SELECT '06B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_clt,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_ticket,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Mtn_CA,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Qte_clt,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Marge_clt,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Rem_clt,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 AND clt_mag204=1 THEN AGE_C2 END ) AS nbclt_mag204,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 AND clt_mag123=1 THEN AGE_C2 END ) AS nbclt_mag123,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 AND clt_mag3125=1 THEN AGE_C2 END ) AS nbclt_mag3125,
AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 AND clt_mag350=1 THEN AGE_C2 END ) AS nbclt_mag350    
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
GROUP BY 1,2)
ORDER BY 1,2;    
    
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_mag758 ORDER BY 1,2;



    
    
-- Statistique du magasin 758 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_mag758_kpi AS 
SELECT * FROM (
Select  '0758 - GRENOBLE ST MARTIN CC NEYRPIC' AS typo, COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758
GROUP BY 1 
UNION 
Select  '0204 - GRENOBLE CV R. DE LA POSTE' AS typo, COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758 AND clt_mag204=1
GROUP BY 1 
UNION 
Select  '0123 - GRENOBLE CC GD PLACE' AS typo, COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758 AND clt_mag123=1
GROUP BY 1
UNION 
Select  '3125 - GRENOBLE CC GD PLACE' AS typo, COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758 AND clt_mag3125=1
GROUP BY 1
UNION 
Select  '0350 - GRENOBLE ÉCHIROLLES CC LECLERC' AS typo, COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt
FROM DATA_MESH_PROD_CLIENT.WORK.info_ticketsmag
WHERE date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1) AND idmag_achat=758 AND clt_mag350=1
GROUP BY 1)
ORDER BY 1; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_mag758_kpi ORDER BY 1;


-- Analyse du Magasin 758 - GRENOBLE ST MARTIN CC NEYRPIC 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.info_Mtick758g AS 
WITH Clt_M758 as (
Select DISTINCT  vd.CODE_CLIENT
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)
  AND vd.ID_MAGASIN=758 
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' )
Select vd.CODE_CLIENT,
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
    ELSE 0 END AS annul_ticket
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dt_init) AND (DATE($dtfin)-1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)
  AND VD.CODE_CLIENT IN (SELECT DISTINCT CODE_CLIENT FROM Clt_M758)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0';
  
 
--- Statistique 
 
 Select idmag_achat, nom_mag,
COUNT(DISTINCT Code_client) AS nb_clt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_CA,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_clt,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_clt,
SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Rem_clt
FROM DATA_MESH_PROD_CLIENT.WORK.info_Mtick758g
GROUP BY 1,2 
ORDER BY nb_clt DESC ; 




