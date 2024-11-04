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

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG AS
WITH Tabl0 AS (SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com,
type_emplacement, lib_statut, id_concept, lib_enseigne, code_pays, gpe_collectionning, CODE_enseigne,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale  
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND CODE_PAYS IN ($PAYS1, $PAYS2) 
AND LIB_ENSEIGNE IN ($LIB_ENSEIGNE1, $LIB_ENSEIGNE2) )
SELECT a.*, b.*, 
CASE WHEN date_fermeture_public IS NULL OR DATE(date_fermeture_public) > DATE($dtfin) THEN DATE($dtfin) 
ELSE date_fermeture_public END AS date_fermeture_etude,
dateadd('month', -18, date_fermeture_etude) AS Datedeb_obs
FROM DATA_MESH_PROD_RETAIL.WORK.STATUT_MAGASIN_FERMES a
INNER JOIN Tabl0 b ON a.NUM_MAG=b.Id_entite
WHERE Type_Ferm != 'ID_ENTITE';

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG ORDER BY 1; 

-- Analyse des statistiques des ventes sur les 12 derniers mois avant la date de fermeture etude ou réel 

--- Historique des ventes sur les 18 derniers mois après la fermeture

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth AS
WITH type_mag AS ( SELECT DISTINCT Num_Mag, Code_entite, Lib_entite, type_ferm, lib_region_com, lib_grande_region_com,
lib_statut, lib_enseigne, gpe_collectionning,type_emplacement, code_pays AS code_pays_mag,
date_ouverture_public, date_fermeture_public, code_postal AS code_postal_Mag, surface_commerciale, date_fermeture_etude, Datedeb_obs
FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG ), -- Selection des informations des magasins a analyser 
ent_ticket AS (SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, CODE_CLIENT, 
CONCAT(ent.id_org_enseigne,'-',ent.id_magasin,'-',ent.code_caisse,'-',ent.code_date_ticket,'-',ent.code_ticket) AS id_ticket 
FROM data_mesh_prod_retail.work.vente_entete_historique ent
INNER JOIN DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG mag ON ent.id_org_enseigne=mag.CODE_enseigne AND ent.ID_magasin=mag.id_entite
WHERE ID_ORG_ENSEIGNE IN ($ENSEIGNE1, $ENSEIGNE2)  AND DATE(dateh_ticket) <= $dtfin  
AND DATE(dateh_ticket) BETWEEN DATE(Datedeb_obs) AND DATE(date_fermeture_etude) ),
tab_xts AS (SELECT DISTINCT SKU, LIBELLE_FAMILLE_ACHAT AS  lib_famille_achat 
FROM DHB_PROD.DNR.DN_PRODUIT WHERE id_marque='JUL' AND FLAG_REFERENCE_COULEUR_COURANT=1),
tickets_lign as (
Select 
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, DATE(vd.dateh_ticket) AS date_ticket, 
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, vd.MONTANT_TTC,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
-- vd.libelle_type_ticket, 
--vd.id_ticket, 
CONCAT(vd.id_org_enseigne,'-',vd.id_magasin,'-',vd.code_caisse,'-',vd.code_date_ticket,'-',vd.code_ticket) as id_ticket,
CODE_EVT_PROMO, MONTANT_SOLDE,
CODE_AM, CODE_OPE_COMM,
MONTANT_REMISE_OPE_COMM,
c.type_emplacement,
CASE WHEN c.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN c.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE, 
SUM(CASE 
    WHEN xts.lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
SUM(CASE 
    WHEN xts.lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN vd.montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_brute,     
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    --WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                    from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
      THEN 1         
    ELSE 0 END AS annul_ticket,
c.Code_entite, c.Lib_entite, c.type_ferm, c.lib_region_com, c.lib_grande_region_com,
c.lib_statut, c.lib_enseigne, c.gpe_collectionning, c.code_pays_mag,
c.date_ouverture_public, c.date_fermeture_public, c.code_postal_Mag, c.surface_commerciale, c.date_fermeture_etude,
FROM data_mesh_prod_retail.work.vente_ligne_historique vd
LEFT JOIN tab_xts xts ON vd.CODE_SKU=xts.SKU
INNER JOIN type_mag c on vd.ID_MAGASIN = c.NUM_MAG
where vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and code_pays_mag IN ($PAYS1 , $PAYS2) AND date_ticket <= $dtfin AND DATE(dateh_ticket) BETWEEN DATE(Datedeb_obs) AND DATE(date_fermeture_etude) ), 
tickets AS ( SELECT a.CODE_CLIENT, b.*
, ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne, b.ID_TICKET)) AS nb_lign
, COUNT(DISTINCT b.id_ticket) Over (partition by CODE_CLIENT) as NB_tick_clt
  FROM tickets_lign b
  INNER JOIN ent_ticket a ON a.id_ticket=b.id_ticket 
  WHERE lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')), 
base AS (SELECT tj.*,
MAX(date_ticket) Over (partition by idmag_achat) as max_dte_ticket_mag,
Min(date_ticket) Over (partition by idmag_achat) as min_dte_ticket_mag,
datediff(MONTH ,min_dte_ticket_mag,max_dte_ticket_mag) AS periode_etud
FROM tickets tj
WHERE date_fermeture_etude IS NOT NULL AND DATE(date_fermeture_etude)<=$dtfin AND PERIMETRE='MAG')
SELECT * FROM base WHERE periode_etud>=16;

-- on tient compte unqiuement des magasins disposant d'au moins 16 mois d'historique 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth;

SELECT periode_etud, COUNT(DISTINCT idmag_achat) AS nb_mag
FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth
GROUP BY 1
ORDER BY 1;


/* Nous allons selectionner pour chaque client le dernier effet à prendre en compte ***/
/*** On tient compte de la derniere fermeturedu client  id client = 000110001619*/

-- Pour chaque client , nous allons essayer de voir s'il a effectuer d'autres achat sur la meme période dans d'autre magasisn 

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
min_dte_ticket_mag,max_dte_ticket_mag,periode_etud, Code_client
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
tbMAG AS ( SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com,
type_emplacement, lib_statut, id_concept, CODE_enseigne, lib_enseigne, code_pays, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale  
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND CODE_PAYS IN ($PAYS1, $PAYS2) 
AND LIB_ENSEIGNE IN ($LIB_ENSEIGNE1, $LIB_ENSEIGNE2) ),
tab_xts AS ( SELECT DISTINCT SKU, LIBELLE_FAMILLE_ACHAT AS  lib_famille_achat 
FROM DHB_PROD.DNR.DN_PRODUIT WHERE id_marque='JUL' AND FLAG_REFERENCE_COULEUR_COURANT=1 ),
ent_ticket AS ( SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, CODE_CLIENT, 
CONCAT(ent.id_org_enseigne,'-',ent.id_magasin,'-',ent.code_caisse,'-',ent.code_date_ticket,'-',ent.code_ticket) AS id_ticket_ent ,
DATE(dateh_ticket) AS Date_ticket_ent , clt.*
FROM data_mesh_prod_retail.work.vente_entete_historique ent
INNER JOIN clt_mag_histo clt ON clt.idclt_histo=ent.CODE_CLIENT
WHERE ID_ORG_ENSEIGNE IN ($ENSEIGNE1, $ENSEIGNE2) AND DATE(dateh_ticket) <= $dtfin AND DATE(dateh_ticket) BETWEEN min_date_ticket_histo AND max_date_ticket_histo),
tickets_lign as (
Select
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, mag.type_emplacement,
CASE WHEN mag.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN mag.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE_achat,
vd.CODE_TICKET, DATE(vd.dateh_ticket) AS date_ticket, 
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.code_ligne,
vd.MONTANT_TTC,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
xts.lib_famille_achat,
CONCAT(vd.id_org_enseigne,'-',vd.id_magasin,'-',vd.code_caisse,'-',vd.code_date_ticket,'-',vd.code_ticket) as id_ticket,
SUM( CASE 
    WHEN xts.lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,     
CASE 
    WHEN PERIMETRE_achat = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    -- WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                   from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
        THEN 1         
    ELSE 0 END AS annul_ticket
FROM data_mesh_prod_retail.work.vente_ligne_historique vd
INNER JOIN tab_xts xts ON vd.CODE_SKU=xts.SKU
INNER JOIN tbMAG MAG ON MAG.CODE_enseigne=vd.ID_ORG_ENSEIGNE AND mag.Id_entite=vd.ID_MAGASIN AND MAG.code_pays IN ( $PAYS1 , $PAYS2 )
where vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 ,$ENSEIGNE2) AND date_ticket <= $dtfin ) ,
tickets AS ( SELECT a.idmag_histo AS idmag_achat_ref, min_date_ticket_histo, max_date_ticket_histo, a.idclt_histo,  b.*
, ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne, b.ID_TICKET)) AS nb_lign
, COUNT(DISTINCT b.id_ticket) Over (partition by CODE_CLIENT) as NB_tick_clt
  FROM tickets_lign b
  INNER JOIN ent_ticket a ON a.id_ticket_ent=b.id_ticket 
  WHERE lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
AND b.date_ticket BETWEEN min_date_ticket_histo AND max_date_ticket_histo ) 
SELECT * FROM tickets
where date_ticket BETWEEN min_date_ticket_histo AND max_date_ticket_histo ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_act_histo ; 

-- ON analyse les ventes sur les 18 mois avant la date de fermeture
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_act_histo AS
SELECT idclt_histo AS CODE_CLIENT, idmag_achat_ref 
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
WHEN NB_centre_mag>=2 THEN '02- 2 MAG et +' 
END AS Mag_client, 
CASE WHEN NB_centre_mag>0 THEN 1 ELSE 0 END AS top_mag_client,
CASE WHEN NB_centre_web>0 THEN 1 ELSE 0 END AS top_web_client, 
CASE WHEN NB_centre_mag>0 AND NB_centre_web>0 THEN '01-Client OMNI' ELSE '02-Client MAG' END AS top_Omni
FROM DATA_MESH_PROD_CLIENT.WORK.stat_act_histo; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_act_histo ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 AS
SELECT a.*,NB_centre, NB_centre_mag, NB_centre_web, nb_tick_glb_hist, CA_glb_hist, qte_glb_hist, Marge_glb_hist,
nb_tick_web_hist, CA_web_hist, qte_web_hist, Marge_web_hist, b.mag_client, b.top_mag_client, b.top_web_client, b.top_Omni
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
GPE_COLLECTIONNING,DATE_OUVERTURE_PUBLIC,DATE_FERMETURE_ETUDE, SURFACE_COMMERCIALE, periode_etud 
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2)
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
ORDER BY 1,2);

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth ORDER BY 1,2;

--- Information du reachat Client après fermeture Magasin 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext AS
WITH infoclt AS ( SELECT DISTINCT IDORGENS_ACHAT AS ens_ferm, IDMAG_ACHAT AS idmag_ferm, TYPE_EMPLACEMENT, LIB_entite,
TYPE_FERM, LIB_ENSEIGNE, GPE_COLLECTIONNING, DATE_OUVERTURE_PUBLIC, DATE_FERMETURE_ETUDE,
SURFACE_COMMERCIALE, PERIODE_ETUD, CODE_CLIENT AS id_client, MAG_CLIENT,
dateadd('month', +18, date_fermeture_etude) AS DTE_PERIODE_NEXT
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 ),
tickets as (
SELECT DISTINCT CODE_CLIENT,
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
vd.TYPE_EMPLACEMENT AS TYPE_EMPLACEMENT_ACHAT,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE_ACHAT,     
CASE 
    WHEN PERIMETRE_ACHAT = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                    from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
      THEN 1         
    ELSE 0 END AS annul_ticket
FROM DHB_PROD.DNR.DN_VENTE vd
INNER  JOIN infoclt mag ON vd.code_client = mag.id_client
where vd.date_ticket BETWEEN dateadd('day', +1, mag.date_fermeture_etude) AND dateadd('month', +18, mag.date_fermeture_etude) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND vd.date_ticket <= $dtfin) 
  SELECT DISTINCT mag.*, tic.*  
,CASE WHEN tic.date_ticket<=date_fermeture_etude THEN NULL ELSE datediff(MONTH ,date_fermeture_etude,tic.date_ticket) END AS delai_reachat
FROM infoclt mag
LEFT JOIN tickets tic ON mag.id_client=tic.CODE_CLIENT AND date_ticket BETWEEN dateadd('day', +1, date_fermeture_etude) AND DTE_PERIODE_NEXT; 
  
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext ORDER BY 1; 


--  Statistique Global des magasins sur 18 mois après fermeture

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_gbl_vte18mth_glb_v1 AS
 SELECT * FROM (
 (SELECT '00-Global' AS typo , '00-Global' AS modalite 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2)
  UNION 
(SELECT '01_TYPE_MAGASIN' as typo, type_ferm AS modalite 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2)
  UNION 
(SELECT '02_TYPE_EMPLACEMENT' as typo, TYPE_EMPLACEMENT AS modalite 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2)
    UNION 
(SELECT '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_etude)) AS modalite  
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2)
ORDER BY 1,2);

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_gbl_vte18mth_glb_v1 ORDER BY 1,2 ; 


-- Infomation Stat Global par Typo magasins 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_gbl_vte18mth_glb_mtyp  AS
 SELECT * FROM (
 (SELECT MAG_CLIENT, '00-Global' AS typo , '00-Global' AS modalite 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2,3)
  UNION 
(SELECT MAG_CLIENT, '01_TYPE_MAGASIN' as typo, type_ferm AS modalite 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2,3)
  UNION 
(SELECT MAG_CLIENT, '02_TYPE_EMPLACEMENT' as typo, TYPE_EMPLACEMENT AS modalite 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2,3)
    UNION 
(SELECT MAG_CLIENT, '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_etude)) AS modalite  
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2,3)
  ORDER BY 1,2,3); 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_gbl_vte18mth_glb_mtyp ORDER BY 1,2,3 ; 
 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext; 


 -- Depense sur la  période de fermeture des clients actifs 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth_actif AS
WITH tclt AS (SELECT DISTINCT ens_ferm, idmag_ferm, code_client FROM tab_ticket_18mthnext WHERE code_client IS NOT NULL AND code_client !='0') 
SELECT * FROM (
(SELECT '00_GLOBAL' as typo, '00_GLOBAL' AS modalite,
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_etude)),1) AS duree_moy_act
,Count(DISTINCT a.code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN a.code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 a
INNER JOIN tclt b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_ferm AND a.idorgens_achat=b.ens_ferm
GROUP BY 1,2)
UNION 
(SELECT '01_TYPE_MAGASIN' as typo, type_ferm AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT a.code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN a.code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 a
INNER JOIN tclt b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_ferm AND a.idorgens_achat=b.ens_ferm
GROUP BY 1,2)
UNION  
(SELECT '02_TYPE_EMPLACEMENT' as typo, TYPE_EMPLACEMENT AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT a.code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN a.code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 a
INNER JOIN tclt b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_ferm AND a.idorgens_achat=b.ens_ferm
GROUP BY 1,2)
UNION  
(SELECT '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_etude)) AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT a.code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
,SUM( nb_tick_glb_hist) AS nb_ticket_sh
,SUM(CA_glb_hist) AS CA_sh
,SUM(qte_glb_hist) AS qte_achete_sh
,SUM(Marge_glb_hist) AS Marge_sh
,Count(DISTINCT CASE WHEN NB_centre_web > 0 THEN a.code_client END ) AS nb_clt_sw
,SUM( CASE WHEN NB_centre_web > 0 THEN nb_tick_web_hist END ) AS nb_ticket_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN CA_web_hist END ) AS CA_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN qte_web_hist END ) AS qte_achete_sw
,SUM(CASE WHEN NB_centre_web > 0 THEN Marge_web_hist END ) AS Marge_sw
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 a
INNER JOIN tclt b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_ferm AND a.idorgens_achat=b.ens_ferm
GROUP BY 1,2)
ORDER BY 1,2);

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth_actif ORDER BY 1,2;

SELECT * FROM tab_ticket_18mthnext; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_react_client_mag AS
WITH tab1 AS (
SELECT ens_ferm, idmag_ferm, type_emplacement, lib_magasin, type_ferm, date_fermeture_etude
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt  
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
GROUP BY ens_ferm, idmag_ferm, type_emplacement, lib_magasin, type_ferm, date_fermeture_etude),
tclt AS (SELECT DISTINCT ens_ferm, idmag_ferm, code_client FROM tab_ticket_18mthnext WHERE code_client IS NOT NULL AND code_client !='0'), 
tab_histo AS (
SELECT DISTINCT  a.IDORGENS_ACHAT AS ens_ferm, a.IDMAG_ACHAT AS idmag_ferm
,Count(DISTINCT a.code_client) AS nb_clt_histo
,SUM( nb_ticket_clt) AS nb_ticket_histo
,SUM(CA_clt) AS CA_histo
,SUM(qte_achete_clt) AS qte_achete_histo
,SUM(Marge_clt) AS Marge_histo	
,SUM(Mnt_remise_clt) AS Mnt_remise_histo
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 a
INNER JOIN tclt b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_ferm AND a.idorgens_achat=b.ens_ferm
GROUP BY 1,2
ORDER BY 1,2)
SELECT a.*, b.nb_clt_histo, nb_ticket_histo, CA_histo, qte_achete_histo, Marge_histo, Mnt_remise_histo
FROM tab1 a 
LEFT JOIN tab_histo b ON a.ens_ferm=b.ens_ferm AND a.idmag_ferm=b.idmag_ferm ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_react_client_mag ORDER BY 1,2 ;

-- Activation par typo magasin

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_react_client_mag_typo AS
WITH tab1 AS (
SELECT ens_ferm, idmag_ferm, type_emplacement, lib_magasin, type_ferm, date_fermeture_etude, MAG_CLIENT
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt  
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
GROUP BY ens_ferm, idmag_ferm, type_emplacement, lib_magasin, type_ferm, date_fermeture_etude, MAG_CLIENT),
tclt AS (SELECT DISTINCT ens_ferm, idmag_ferm, code_client FROM tab_ticket_18mthnext WHERE code_client IS NOT NULL AND code_client !='0'), 
tab_histo AS (
SELECT DISTINCT  a.IDORGENS_ACHAT AS ens_ferm, a.IDMAG_ACHAT AS idmag_ferm, a.MAG_CLIENT
,Count(DISTINCT a.code_client) AS nb_clt_histo
,SUM( nb_ticket_clt) AS nb_ticket_histo
,SUM(CA_clt) AS CA_histo
,SUM(qte_achete_clt) AS qte_achete_histo
,SUM(Marge_clt) AS Marge_histo	
,SUM(Mnt_remise_clt) AS Mnt_remise_histo
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 a
INNER JOIN tclt b ON a.code_client=b.code_client AND a.idmag_achat=b.idmag_ferm AND a.idorgens_achat=b.ens_ferm
GROUP BY 1,2,3
ORDER BY 1,2,3)
SELECT a.*, b.nb_clt_histo, nb_ticket_histo, CA_histo, qte_achete_histo, Marge_histo, Mnt_remise_histo
FROM tab1 a 
LEFT JOIN tab_histo b ON a.ens_ferm=b.ens_ferm AND a.idmag_ferm=b.idmag_ferm AND a.mag_client=b.mag_client;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_react_client_mag_typo ORDER BY 1,2 ;

/*** Statistiques Magasin par magasin ***/ 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statmag_18mthnext AS
SELECT ens_ferm, idmag_ferm, idorgens_achat, idmag_achat,lib_magasin, PERIMETRE_achat   
,min(Date_ticket) AS min_dtHA_ticket_clt
,max(Date_ticket) AS max_dtHA_ticket_clt
,Count(DISTINCT code_client) AS nbclt_mag
,Count(DISTINCT id_ticket) AS nb_ticket_mag
,SUM(MONTANT_TTC ) AS CA_mag
,SUM(QUANTITE_LIGNE ) AS qte_achete_clt
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_clt	
,SUM(montant_remise ) AS Mnt_remise_clt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext
GROUP BY ens_ferm, idmag_ferm, idorgens_achat, idmag_achat,lib_magasin, PERIMETRE_achat ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.statmag_18mthnext ORDER BY 1,2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_distance AS
WITH tab0 AS ( 
SELECT  DISTINCT ID_ORG_ENSEIGNE AS id_enseigne_a, 
ID_MAGASIN AS id_magasin_a, 
type_emplacement AS type_emplacement_a,
code_magasin AS code_magasin_a, 
lib_magasin AS lib_magasin_a, 
latitude AS latitude_a, 
longitude AS longitude_a
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND (code_pays = $PAYS1 or code_pays = $PAYS2) AND type_emplacement IN ('PAC','CC', 'CV','CCV')),
tab1 AS ( 
SELECT  DISTINCT ID_ORG_ENSEIGNE AS id_enseigne_b, 
ID_MAGASIN AS id_magasin_b, 
type_emplacement AS type_emplacement_b,
code_magasin AS code_magasin_b, 
lib_magasin AS lib_magasin_b, 
latitude AS latitude_b, 
longitude AS longitude_b
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN 
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND (code_pays = $PAYS1 or code_pays = $PAYS2) AND type_emplacement IN ('PAC','CC', 'CV','CCV') )
SELECT DISTINCT a.*,b.*
,concat(id_enseigne_a,'_',id_magasin_a,'_',id_enseigne_b,'_',id_magasin_b) AS id_ref_mag
,(ST_DISTANCE(ST_MAKEPOINT(longitude_a, latitude_a), ST_MAKEPOINT(longitude_b, latitude_b))) / 1000  AS distanc_mag
FROM tab0 a, tab1 b;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_distance ;

/***** Statistiques des ventes par mag mag ***/

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v AS
WITH tab0 AS (SELECT DISTINCT id_ref_mag,distanc_mag 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_distance),
tab1 AS (SELECT *, 
concat(ens_ferm,'_',idmag_ferm,'_',IDORGENS_ACHAT,'_',IDMAG_ACHAT) AS id_ref_mag_achat
FROM DATA_MESH_PROD_CLIENT.WORK.statmag_18mthnext)
SELECT a.*, ROUND(b.distanc_mag) AS distanc_mag
FROM tab1 a 
LEFT JOIN tab0 b ON a.id_ref_mag_achat=b.id_ref_mag;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v ORDER BY 1,2 , nbclt_mag desc ; 

-- rajout des elements sur le nombre de client potentiel et le nombre de client activés au global 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_react_client_mag ORDER BY 1,2 ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v2 AS
WITH react AS (SELECT DISTINCT ens_ferm, idmag_ferm,type_emplacement, lib_entite AS lib_magasin_ferm, type_ferm, date_fermeture_public, nb_client_potentiel, nbclt_actif FROM DATA_MESH_PROD_CLIENT.WORK.Stat_react_client_mag ORDER BY 1,2 )
SELECT a.*, b.type_emplacement, b.lib_magasin_ferm, b.type_ferm, b.date_fermeture_public, b.nb_client_potentiel, b.nbclt_actif
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v a 
LEFT JOIN react b ON a.ens_ferm=b.ens_ferm AND a.idmag_ferm=b.idmag_ferm ; 

-- ranger les information et déduire le top 10 pour chaque Magasins 

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v2 ; 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v3 AS
 WITH tab0 AS (SELECT DISTINCT 
 ens_ferm, idmag_ferm, lib_magasin_ferm,
type_ferm, date_fermeture_public, idorgens_achat, idmag_achat, lib_magasin, perimetre_achat, distanc_mag, 
nb_client_potentiel, nbclt_actif, nbclt_mag, nb_ticket_mag, ca_mag, 
qte_achete_clt, marge_clt, mnt_remise_clt
FROM tabclt_ticket_18mthnext_v2  ),
tab1 AS (SELECT *, 
rank() over(partition by idmag_ferm order by nbclt_mag DESC) as rang_mag,
ROW_NUMBER() over(partition by idmag_ferm order by nbclt_mag DESC) as lign_mag
 FROM tab0)
SELECT DISTINCT  ens_ferm, idmag_ferm, lib_magasin_ferm,
type_ferm, date_fermeture_public, idorgens_achat, idmag_achat, lib_magasin_achat, perimetre_achat, distanc_mag, 
nb_client_potentiel, nbclt_actif, nbclt_mag, nb_ticket_mag, ca_mag, 
qte_achete_clt, marge_clt, mnt_remise_clt, lign_mag 
FROM tab1 WHERE lign_mag<=10;

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v3 order BY idmag_ferm , nbclt_mag DESC;

--- Analyse de la distance avec le magasin de reachat

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext_v2 WHERE IDMAG_FERM =19;

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_gbl_mag_distanc AS
 WITH tab0 AS (SELECT DISTINCT id_ref_mag,distanc_mag 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_distance),
tab1 AS (SELECT *, 
concat(ens_ferm,'_',idmag_ferm,'_',IDORGENS_ACHAT,'_',IDMAG_ACHAT) AS id_ref_mag_achat
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext), 
tab2 AS (SELECT a.*, ROUND(b.distanc_mag) AS distanc_mag
FROM tab1 a 
LEFT JOIN tab0 b ON a.id_ref_mag_achat=b.id_ref_mag)
SELECT '02-Distance / Type ferm' AS niv0 , type_ferm AS niv1 , 
CASE WHEN perimetre_achat='WEB' THEN 'a-Achat WEB'
	WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag BETWEEN 0 AND 3 THEN 'b: 00-03Km'
     WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag BETWEEN 4 AND 6 THEN 'c: 04-06Km'
     WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag BETWEEN 7 AND 9 THEN 'd: 07-09Km'
     WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag BETWEEN 10 AND 12 THEN 'e: 10-12Km'
     WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag BETWEEN 13 AND 15 THEN 'f: 13-15Km'
     WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag BETWEEN 16 AND 20 THEN 'g: 16-20Km'
     WHEN perimetre_achat='MAG' AND distanc_mag IS NOT NULL AND distanc_mag >20 THEN 'h: 20Km et + '
     ELSE 'z: Non définie'
     END AS niv2 
,min(Date_ticket) AS min_dtHA_ticket_clt
,max(Date_ticket) AS max_dtHA_ticket_clt
,Count(DISTINCT code_client) AS nbclt_mag
,Count(DISTINCT id_ticket) AS nb_ticket_mag
,SUM(MONTANT_TTC ) AS CA_mag
,SUM(QUANTITE_LIGNE ) AS qte_achete_clt
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_clt	
,SUM(montant_remise ) AS Mnt_remise_clt  
FROM tab2
  GROUP BY 1,2,3; 
  
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_gbl_mag_distanc ORDER BY 1,2,3;

-- Statistiques global de reachat client 
SELECT MAG_CLIENT, type_ferm 
  ,COUNT(DISTINCT IDMAG_FERM) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth   
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN id_ticket END) AS nb_ticket_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_TTC END) AS CA_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_clt_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='MAG' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN code_client END ) AS nbclt_actif_web
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN id_ticket END) AS nb_ticket_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_TTC END) AS CA_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_clt_web
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND PERIMETRE_achat='WEB' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt_web
  FROM tab_ticket_18mthnext
  GROUP BY 1,2,3)

  
  
  








