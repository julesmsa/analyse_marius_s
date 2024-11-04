/***** Mise à jour des informations sur l'étude du transfert avec un nouveau fichier de magasins **/

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET LIB_ENSEIGNE1 = 'JULES'; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET LIB_ENSEIGNE2 = 'BRICE';
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- Date de fin ticket pour avoir des données stables 
SET dtfin = DAte('2024-09-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE(); 
SELECT $dtfin; -- Date à la quelle ON analyse les performances des magasins

-- Travailler surles tables du magasins 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.STATUT_MAGASIN_FERMES; -- Nouvelles TABLES avec les TYPES de Fermeture 

SELECT * FROM DHB_PROD.DNR.DN_ENTITE;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG AS
WITH Tabl0 AS (SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com,
type_emplacement, lib_statut, id_concept, lib_enseigne, code_pays, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale  
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND CODE_PAYS IN ($PAYS1, $PAYS2) 
AND LIB_ENSEIGNE IN ($LIB_ENSEIGNE1, $LIB_ENSEIGNE2) )
SELECT a.*, b.*, 
CASE WHEN date_fermeture_public IS NULL OR DATE(date_fermeture_public) > DATE($dtfin) THEN DATE($dtfin) 
ELSE date_fermeture_public END AS date_fermeture_etude
FROM DATA_MESH_PROD_RETAIL.WORK.STATUT_MAGASIN_FERMES a
INNER JOIN Tabl0 b ON a.NUM_MAG=b.Id_entite
WHERE Type_Ferm != 'ID_ENTITE';

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG ORDER BY 1; 

-- Analyse des statistiques des ventes sur les 12 derniers mois avant la date de fermeture etude ou réel 

--- Historique des ventes sur les 18 derniers mois après la fermeture 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth AS
WITH type_mag AS ( SELECT DISTINCT Num_Mag, Code_entite, Lib_entite, type_ferm, lib_region_com, lib_grande_region_com,
lib_statut, lib_enseigne, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal AS code_postal_Mag, surface_commerciale, date_fermeture_etude
FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG ),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin) 
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, vd.MONTANT_TTC,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
CODE_EVT_PROMO, MONTANT_SOLDE,
CODE_AM, CODE_OPE_COMM,
MONTANT_REMISE_OPE_COMM,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE, 
COUNT(DISTINCT id_ticket) Over (partition by vd.CODE_CLIENT) as NB_tick_clt,
ROW_NUMBER() OVER (PARTITION BY VD.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_brute,     
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                    from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
      THEN 1         
    ELSE 0 END AS annul_ticket,
g.ID_MACRO_SEGMENT , g.LIB_MACRO_SEGMENT,
e.LIB_SEGMENT_OMNI , 
CASE WHEN g.id_macro_segment = '01' THEN '01_VIP' 
     WHEN g.id_macro_segment = '02' THEN '02_TBC'
     WHEN g.id_macro_segment = '03' THEN '03_BC'
     WHEN g.id_macro_segment = '04' THEN '04_MOY'
     WHEN g.id_macro_segment = '05' THEN '05_TAP'
     WHEN g.id_macro_segment = '06' THEN '06_TIEDE'
     WHEN g.id_macro_segment = '07' THEN '07_TPURG'
     WHEN g.id_macro_segment = '09' THEN '08_NCV'
     WHEN g.id_macro_segment = '08' THEN '09_NAC'
     WHEN g.id_macro_segment = '10' THEN '10_INA12'
     WHEN g.id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM, 
CASE 
	WHEN LIB_SEGMENT_OMNI IS NULL OR LIB_SEGMENT_OMNI='' THEN 'NOSEGMENT' 
	WHEN LIB_SEGMENT_OMNI='WEB' THEN 'OMNI'
	ELSE LIB_SEGMENT_OMNI END AS SEGMENT_OMNI,
c.Code_entite, c.Lib_entite, c.type_ferm, c.lib_region_com, c.lib_grande_region_com,
c.lib_statut, c.lib_enseigne, c.gpe_collectionning,
c.date_ouverture_public, c.date_fermeture_public, c.code_postal_Mag, c.surface_commerciale, c.date_fermeture_etude,
MAX(vd.date_ticket) Over (partition by vd.ID_MAGASIN) as max_dte_ticket_mag,
Min(vd.date_ticket) Over (partition by vd.ID_MAGASIN) as min_dte_ticket_mag
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN type_mag c on vd.ID_MAGASIN = c.NUM_MAG
LEFT JOIN segrfm g ON vd.code_client = g.code_client
LEFT JOIN segomni e ON vd.code_client=e.code_client 
where vd.date_ticket BETWEEN dateadd('month', -18, c.date_fermeture_etude) AND DATE(c.date_fermeture_etude) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND vd.date_ticket <= $dtfin
  ) 
SELECT *, datediff(MONTH ,min_dte_ticket_mag,max_dte_ticket_mag) AS periode_etud
FROM tickets
WHERE date_fermeture_etude IS NOT NULL AND DATE(date_fermeture_etude)<=$dtfin AND PERIMETRE='MAG';


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth;

/* Nous allons selectionner pour chaque client le dernier effet à prendre en compte ***/
/*** On tient compte de la derniere fermeturedu client  id client = 000110001619*/

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mths AS
WITH tab0  AS (SELECT DISTINCT CODE_CLIENT, idmag_achat, date_fermeture_etude,date_ouverture_public, 
MAX(date_fermeture_etude) OVER (PARTITION BY CODE_CLIENT) AS max_dateferm,
MAX(date_ouverture_public) OVER (PARTITION BY CODE_CLIENT) AS max_dateouv
FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth
WHERE CODE_CLIENT IS NOT NULL AND CODE_CLIENT != '0'), 
tab1 AS (SELECT * FROM Tab0 WHERE date_fermeture_etude=max_dateferm AND date_ouverture_public=max_dateouv)
SELECT a.* 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth a
INNER JOIN tab1 b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.idmag_achat=b.idmag_achat
ORDER BY 1, 2;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mths;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth AS
WITH stat0 AS (
SELECT idorgens_achat, idmag_achat , Code_client 
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END ) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN remise_totale END) AS Mnt_remise_clt
FROM tab_mag_histo_18mths 
WHERE code_client IS NOT NULL AND code_client !='0'
GROUP BY 1,2,3),
info_mag AS (SELECT DISTINCT idorgens_achat, idmag_achat, MAG_ACHAT,  
TYPE_EMPLACEMENT,LIB_ENTITE,TYPE_FERM, LIB_REGION_COM, LIB_GRANDE_REGION_COM, LIB_ENSEIGNE,  
GPE_COLLECTIONNING,DATE_OUVERTURE_PUBLIC,DATE_FERMETURE_PUBLIC, DATE_FERMETURE_ETUDE,
SURFACE_COMMERCIALE,
min_dte_ticket_mag,max_dte_ticket_mag,periode_etud, Code_client, SEGMENT_RFM, SEGMENT_OMNI 
FROM tab_mag_histo_18mths )
SELECT a.*, nb_ticket_clt, CA_clt, qte_achete_clt, Marge_clt, Mnt_remise_clt
FROM info_mag a
INNER JOIN stat0 b ON a.idorgens_achat=b.idorgens_achat AND a.idmag_achat=b.idmag_achat AND a.Code_client=b.Code_client ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth ; 

 -- Information sur la typologie des clients 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_act_histo AS
WITH mag_histo AS ( SELECT idmag_achat AS idmag_histo
,MIN(date_ticket)  AS min_date_ticket_histo
,MAX(date_ticket)  AS max_date_ticket_histo
FROM tab_mag_histo_18mths
GROUP BY 1 ),
clt_mag AS (SELECT DISTINCT code_client, idmag_achat AS idmag_histo FROM tab_mag_histo_18mths 
WHERE code_client IS NOT NULL AND code_client !='0' ),
clt_mag_histo AS (
SELECT a.*, b.code_client AS idclt_histo
FROM mag_histo a
INNER JOIN clt_mag b ON a.idmag_histo=b.idmag_histo ),
tickets as (
Select  vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE_achat,
vd.CODE_TICKET, vd.date_ticket, 
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.MONTANT_TTC,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,     
CASE 
    WHEN PERIMETRE_achat = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                   from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
        THEN 1         
    ELSE 0 END AS annul_ticket, 
c.idmag_histo AS idmag_achat_ref, min_date_ticket_histo, max_date_ticket_histo
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN clt_mag_histo c on vd.CODE_CLIENT = c.idclt_histo
where vd.date_ticket BETWEEN min_date_ticket_histo AND max_date_ticket_histo -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (VD.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND vd.date_ticket <= $dtfin ) 
  SELECT *
FROM tickets; 
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_act_histo AS
SELECT CODE_CLIENT, idmag_achat_ref 
,COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN idmag_achat END)  as NB_centre 
,COUNT(DISTINCT CASE WHEN PERIMETRE_achat='MAG' AND Qte_pos>0 THEN idmag_achat end)  as NB_centre_mag 
,COUNT(DISTINCT CASE WHEN PERIMETRE_achat='WEB' AND Qte_pos>0 THEN idmag_achat END )  as NB_centre_web 
,Count(DISTINCT id_ticket) AS nb_tick_glb_hist
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_glb_hist
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_glb_hist
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_glb_hist	
,Count(DISTINCT CASE WHEN PERIMETRE_achat='WEB' AND Qte_pos>0 THEN id_ticket end) AS nb_tick_web_hist
,SUM(CASE WHEN PERIMETRE_achat='WEB' AND annul_ticket=0 THEN MONTANT_TTC END ) AS CA_web_hist
,SUM(CASE WHEN PERIMETRE_achat='WEB' AND annul_ticket=0 THEN QUANTITE_LIGNE END ) AS qte_web_hist
,SUM(CASE WHEN PERIMETRE_achat='WEB' AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_web_hist
FROM DATA_MESH_PROD_CLIENT.WORK.tab_act_histo
GROUP BY 1,2; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_act_histo AS
SELECT DISTINCT *, CASE 
WHEN NB_centre_mag=1 THEN '01-Mono MAG' 
WHEN NB_centre_mag=2 THEN '02- 2 MAG' 
WHEN NB_centre_mag>=3 THEN '03- 3 MAG et +' 
END AS Mag_client, 
CASE WHEN NB_centre_mag>0 THEN 1 ELSE 0 END AS top_mag_client,
CASE WHEN NB_centre_web>0 THEN 1 ELSE 0 END AS top_web_client
FROM DATA_MESH_PROD_CLIENT.WORK.stat_act_histo; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_act_histo ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 AS
SELECT a.*,NB_centre, NB_centre_mag, NB_centre_web, nb_tick_glb_hist, CA_glb_hist, qte_glb_hist, Marge_glb_hist,
nb_tick_web_hist, CA_web_hist, qte_web_hist, Marge_web_hist, b.mag_client, b.top_mag_client, b.top_web_client
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.stat_act_histo b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_achat_ref ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 LIMIT 10; 

-- Statistiques par magasin 
-- Conctrcution de l'onglet TAB_STAT_HISTO_MAG 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_histo_18mth AS
WITH 
stat0 AS (
SELECT idorgens_achat, idmag_achat 
,MAX(max_dte_ticket_mag) AS max_date_ticket 
,MAX(min_dte_ticket_mag)  AS min_date_ticket
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glb
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2),
info_mag AS (SELECT DISTINCT idorgens_achat, idmag_achat,  
TYPE_EMPLACEMENT,LIB_ENTITE,TYPE_FERM,LIB_ENSEIGNE,
GPE_COLLECTIONNING,DATE_OUVERTURE_PUBLIC,DATE_FERMETURE_PUBLIC,SURFACE_COMMERCIALE, periode_etud 
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
WHERE periode_etud>=15)
SELECT a.* , b.min_date_ticket, b.max_date_ticket, b.nb_clt_glb, b.nb_ticket_glb, 
b.CA_glb, b.qte_achete_glb, b.Marge_glb, b.Mnt_remise_glb
FROM info_mag a
INNER JOIN stat0 b ON a.idorgens_achat=b.idorgens_achat AND a.idmag_achat=b.idmag_achat; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_mag_histo_18mth ORDER BY 1,2; 


-- Synthèse STAT sur les magasins fermes 
-- il s'agit d'une table avec les statistiques global pour les magasins ayant femrés 
-- il faut trappler qu'un client peut appartenir a plusieurs magasins ayant fermés , on regarde la situation du client sur les 18 mois avant fermeture du magasin 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth AS
SELECT * FROM (
(SELECT '00_GLOBAL' as typo, '00_GLOBAL' AS modalite,
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION 
(SELECT '01_TYPE_MAGASIN' as typo, type_ferm AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '02_TYPE_EMPLACEMENT' as typo, TYPE_EMPLACEMENT AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_etude)) AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '04_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '05_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '06_TYPO CLIENT' as typo, MAG_CLIENT AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
ORDER BY 1,2);

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth ORDER BY 1,2;











