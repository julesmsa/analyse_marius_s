/***** Mise à jour des informations en faisant le distinguo , Mono Mag plusieurs Mag **/


SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- Recupération des données sur l'historique de 18 mois avant fermeture du mag 
-- il s'agit des magasins fermés depuis 2021 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND lib_statut='Fermé' AND YEAR (date_fermeture_public)>=2021),
type_mag AS ( SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_ferm 
FROM DATA_MESH_PROD_CLIENT.WORK.STATUT_MAGASIN_FERMES
WHERE type_ferm NOT IN ('Bascule Brice Solo','Jules Plage')),
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5),
produit as (
    select distinct ref.ID_REFERENCE, ref.ID_FAMILLE_ACHAT, fam.LIB_FAMILLE_ACHAT,
        G.LIB_GROUPE_FAMILLE
    from DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_REFERENCE ref
    join DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_FAMILLE_ACHAT fam 
        on ref.ID_FAMILLE_ACHAT = fam.ID_FAMILLE_ACHAT
    INNER JOIN DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_GROUPE_FAMILLE_ACHAT G
        ON G.ID_GROUPE_FAMILLE = fam.id_groupe_famille
    where ref.est_version_courante = 1 and ref.id_marque = 'JUL'),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
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
COUNT(DISTINCT id_ticket) Over (partition by vd.CODE_CLIENT) as NB_tick_clt,
ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI,DATE_PARTITION ,
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
mag.*, c.type_ferm,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE,
MAX(vd.date_ticket) Over (partition by vd.ID_MAGASIN) as max_dte_ticket_mag,
Min(vd.date_ticket) Over (partition by vd.ID_MAGASIN) as min_dte_ticket_mag
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
INNER JOIN type_mag c on vd.ID_ORG_ENSEIGNE = c.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = c.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
LEFT JOIN tabtg g ON vd.CODE_CLIENT=g.code_client AND DATE_FROM_PARTS(YEAR(date_fermeture_public) , MONTH(date_fermeture_public), 1)=g.DATE_PARTITION
where vd.date_ticket BETWEEN dateadd('month', -18, date_fermeture_public) AND DATE(date_fermeture_public) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  ) 
SELECT *,datediff(MONTH ,min_dte_ticket_mag,max_dte_ticket_mag) AS periode_etud  FROM tickets;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth ; 


-- nous ramenons toutes les informations niveau Client 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth AS
WITH stat0 AS (
SELECT idorgens_achat, idmag_achat , Code_client 
,Count(DISTINCT id_ticket) AS nb_ticket_clt
,SUM(MONTANT_TTC ) AS CA_clt
,SUM(QUANTITE_LIGNE ) AS qte_achete_clt
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_clt	
,SUM(montant_remise ) AS Mnt_remise_clt
FROM tab_mag_histo_18mth 
WHERE code_client IS NOT NULL AND code_client !='0' AND periode_etud>=15 
GROUP BY 1,2,3),
info_mag AS (SELECT DISTINCT idorgens_achat, idmag_achat,  
TYPE_EMPLACEMENT,LIB_MAGASIN,TYPE_FERM,ID_CONCEPT,LIB_ENSEIGNE,
GPE_COLLECTIONNING,DATE_OUVERTURE_PUBLIC,DATE_FERMETURE_PUBLIC,SURFACE_COMMERCIALE,
min_dte_ticket_mag,max_dte_ticket_mag,periode_etud, Code_client, SEGMENT_RFM, SEGMENT_OMNI 
FROM tab_mag_histo_18mth
WHERE periode_etud>=15)
SELECT a.*, nb_ticket_clt, CA_clt, qte_achete_clt, Marge_clt, Mnt_remise_clt
FROM info_mag a
INNER JOIN stat0 b ON a.idorgens_achat=b.idorgens_achat AND a.idmag_achat=b.idmag_achat AND a.Code_client=b.Code_client ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth ; 

 -- Information sur la typologie des clients 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_act_histo AS
WITH mag_histo AS ( SELECT idmag_achat 
,MIN(date_ticket)  AS min_date_ticket_histo
,MAX(date_ticket)  AS max_date_ticket_histo
FROM tab_mag_histo_18mth
WHERE periode_etud>=15
GROUP BY 1 ),
clt_mag AS (SELECT DISTINCT code_client, idmag_achat FROM tab_mag_histo_18mth 
WHERE code_client IS NOT NULL AND code_client !='0' ),
clt_mag_histo AS (
SELECT a.*, b.code_client 
FROM mag_histo a
INNER JOIN clt_mag b ON a.idmag_achat=b.idmag_achat ),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
c.idmag_achat AS idmag_achat_ref, min_date_ticket_histo, max_date_ticket_histo
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN AND type_emplacement IN ('PAC','CC', 'CV','CCV')
INNER JOIN clt_mag_histo c on vd.CODE_CLIENT = c.CODE_CLIENT
where vd.date_ticket BETWEEN min_date_ticket_histo AND max_date_ticket_histo -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) )
  SELECT *
FROM tickets; 
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_act_histo AS
SELECT CODE_CLIENT, idmag_achat_ref, 
COUNT(DISTINCT idmag_achat)  as NB_mag 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_act_histo
GROUP BY 1,2; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_act_histo AS
SELECT DISTINCT CODE_CLIENT, idmag_achat_ref, CASE 
WHEN NB_mag=1 THEN '01-Mono MAG' 
WHEN NB_mag=2 THEN '02- 2 MAG' 
WHEN NB_mag>=3 THEN '03- 3 MAG et +' 
END AS Mag_client
FROM DATA_MESH_PROD_CLIENT.WORK.stat_act_histo; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_act_histo ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 AS
SELECT a.*,b.mag_client
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
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2),
info_mag AS (SELECT DISTINCT idorgens_achat, idmag_achat,  
TYPE_EMPLACEMENT,LIB_MAGASIN,TYPE_FERM,ID_CONCEPT,LIB_ENSEIGNE,
GPE_COLLECTIONNING,DATE_OUVERTURE_PUBLIC,DATE_FERMETURE_PUBLIC,SURFACE_COMMERCIALE, periode_etud 
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
WHERE periode_etud>=15)
SELECT a.* , b.min_date_ticket, b.max_date_ticket, b.nb_clt_glb, b.nb_ticket_glb, 
b.CA_glb, b.qte_achete_glb, b.Marge_glb, b.Mnt_remise_glb
FROM info_mag a
INNER JOIN stat0 b ON a.idorgens_achat=b.idorgens_achat AND a.idmag_achat=b.idmag_achat; 

-- Synthèse STAT sur les magasins fermes 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth AS
SELECT * FROM (
(SELECT '00_GLOBAL' as typo, '00_GLOBAL' AS modalite,
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION 
(SELECT '01_TYPE_MAGASIN' as typo, type_ferm AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '02_TYPE_EMPLACEMENT' as typo, TYPE_EMPLACEMENT AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_public)) AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '04_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '05_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
UNION  
(SELECT '06_TYPO CLIENT' as typo, MAG_CLIENT AS modalite, 
COUNT(DISTINCT idmag_achat) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT code_client) AS nb_clt_glb
,SUM( nb_ticket_clt) AS nb_ticket_glb
,SUM(CA_clt) AS CA_glb
,SUM(qte_achete_clt) AS qte_achete_glb
,SUM(Marge_clt) AS Marge_glb	
,SUM(Mnt_remise_clt) AS Mnt_remise_glbt
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2
GROUP BY 1,2)
ORDER BY 1,2);

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2;

SELECT DISTINCT IDORGENS_ACHAT AS ens_ferm, IDMAG_ACHAT AS idmag_ferm, Code_client, Segment_rfm, Segment_omni, mag_client
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth ORDER BY 1,2;

--- Information du reachat Client après fermeture Magasin 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext AS
WITH infoclt AS ( SELECT DISTINCT IDORGENS_ACHAT AS ens_ferm, IDMAG_ACHAT AS idmag_ferm, TYPE_EMPLACEMENT, LIB_MAGASIN,
TYPE_FERM, ID_CONCEPT, LIB_ENSEIGNE, GPE_COLLECTIONNING, DATE_OUVERTURE_PUBLIC, DATE_FERMETURE_PUBLIC,
SURFACE_COMMERCIALE, PERIODE_ETUD, CODE_CLIENT AS id_client, MAG_CLIENT, SEGMENT_OMNI , SEGMENT_RFM, 
dateadd('month', +18, date_fermeture_public) AS DTE_PERIODE_NEXT
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2 ),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin AS lib_magasin_achat, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE_achat
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) and (code_pays = $PAYS1 or code_pays = $PAYS2) ),
tickets as (
Select DISTINCT vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, lib_magasin_achat, vd.CODE_MAGASIN AS mag_achat, PERIMETRE_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
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
vd.id_ticket
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
INNER  JOIN infoclt mag ON vd.code_client = mag.id_client
inner join Magasin jul  on vd.ID_ORG_ENSEIGNE = jul.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = jul.ID_MAGASIN
where vd.date_ticket BETWEEN dateadd('day', +1, mag.date_fermeture_public) AND dateadd('month', +18, mag.date_fermeture_public) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)) 
  SELECT DISTINCT mag.*, tic.*  
,CASE WHEN tic.date_ticket<=date_fermeture_public THEN NULL ELSE datediff(MONTH ,date_fermeture_public,tic.date_ticket) END AS delai_reachat
FROM infoclt mag
LEFT JOIN tickets tic ON mag.id_client=tic.CODE_CLIENT AND date_ticket BETWEEN dateadd('day', +1, date_fermeture_public) AND DTE_PERIODE_NEXT; 
  
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext ORDER BY 1; 

-- creation de la table client next 18 mois 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2;


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
(SELECT '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_public)) AS modalite  
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
(SELECT '04_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite
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
(SELECT '05_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite
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
(SELECT '06_TYPO CLIENT' as typo, MAG_CLIENT AS modalite
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
(SELECT MAG_CLIENT, '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_public)) AS modalite  
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
(SELECT MAG_CLIENT, '04_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite
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
(SELECT MAG_CLIENT, '05_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite
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

 
 
 
 
 
 
 
 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext AS
WITH infoclt AS (SELECT DISTINCT IDORGENS_ACHAT AS ens_ferm, IDMAG_ACHAT AS idmag_ferm, 
type_emplacement, lib_magasin AS limag_ferm, lib_enseigne AS libens_ferm, type_ferm, gpe_collectionning, 
Surface_commerciale, Code_client AS id_client, Segment_rfm, Segment_omni, mag_client,
nb_ticket_clt AS nb_ticket_clt_hist, Ca_clt AS Ca_clt_hist, qte_achete_clt AS qte_clt_hist, marge_clt AS marge_clt_hist, Mnt_remise_clt AS Mnt_remise_clt_hist
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_mag_histo_18mth_V2), 
statg AS (
SELECT ens_ferm, idmag_ferm, idorgens_achat, idmag_achat,lib_magasin_achat, PERIMETRE_achat, id_client, code_client  
,min(Date_ticket) AS min_dtHA_ticket_clt
,max(Date_ticket) AS max_dtHA_ticket_clt
,Count(DISTINCT id_ticket) AS nb_ticket_clt
,SUM(MONTANT_TTC ) AS CA_clt
,SUM(QUANTITE_LIGNE ) AS qte_achete_clt
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_clt	
,SUM(montant_remise ) AS Mnt_remise_clt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext
GROUP BY ens_ferm, idmag_ferm, idorgens_achat, idmag_achat,lib_magasin_achat, PERIMETRE_achat, id_client, code_client  )
SELECT a.*, 
idmag_achat,lib_magasin_achat, PERIMETRE_achat, code_client, min_dtHA_ticket_clt, max_dtHA_ticket_clt, nb_ticket_clt, 
CA_clt, qte_achete_clt, Marge_clt, Mnt_remise_clt
FROM infoclt  a
LEFT JOIN statg b ON a.ens_ferm=b.ens_ferm AND a.idmag_ferm= b.idmag_ferm AND a.id_client=b.id_client ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_ticket_18mthnext; 

-- ajout des informatins clients sur l'historique d'achat 

/*** Statistiques Magasin par magasin ***/ 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statmag_18mthnext AS
SELECT ens_ferm, idmag_ferm, idorgens_achat, idmag_achat,lib_magasin_achat, PERIMETRE_achat   
,min(Date_ticket) AS min_dtHA_ticket_clt
,max(Date_ticket) AS max_dtHA_ticket_clt
,Count(DISTINCT code_client) AS nbclt_mag
,Count(DISTINCT id_ticket) AS nb_ticket_mag
,SUM(MONTANT_TTC ) AS CA_mag
,SUM(QUANTITE_LIGNE ) AS qte_achete_clt
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_clt	
,SUM(montant_remise ) AS Mnt_remise_clt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_18mthnext
GROUP BY ens_ferm, idmag_ferm, idorgens_achat, idmag_achat,lib_magasin_achat, PERIMETRE_achat ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.statmag_18mthnext;

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
FROM tab_mag_distance),
tab1 AS (SELECT *, 
concat(ens_ferm,'_',idmag_ferm,'_',IDORGENS_ACHAT,'_',IDMAG_ACHAT) AS id_ref_mag_achat
FROM tabclt_ticket_18mthnext)
SELECT a.*, ROUND(b.distanc_mag) AS distanc_mag
FROM tab1 a 
LEFT JOIN tab0 b ON a.id_ref_mag_achat=b.id_ref_mag;






-- SELECT * FROM tab_mag_vte_18mth_v; 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb AS
SELECT ID_ORG_ENSEIGNE AS ens_ferm, ID_MAGASIN AS idmag_ferm, type_emplacement, lib_magasin, type_ferm , date_fermeture_public
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 18 THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3,4,5,6
  ORDER BY 1,2,3,4,5,6;
 
 -- SELECT * FROM stat_mag_vte18mth_glb ORDER BY 1,2,3,4,5,6;
 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_clt_histo_18mth; 

-- Nous allons rajouter l'historique d'achat des clients qui se sont reactivés 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_clt_histo_18mth

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_histo_clt_actif AS
WITH tgb AS (SELECT DISTINCT id_org_enseigne, id_magasin,code_client 
FROM tab_mag_vte_18mth_v  
WHERE code_client IS NOT NULL AND code_client !='0' AND delai_reachat BETWEEN 0 AND 18)
SELECT a.idorgens_achat AS idens_histo, a.idmag_achat AS idmag_histo 
  ,Count(DISTINCT a.code_client) AS nbclt_actif_h
  ,SUM(a.nb_ticket_clt) AS nb_ticket_clt_h
  ,SUM(a.CA_clt ) AS CA_clt_h
  ,SUM(a.qte_achete_clt ) AS qte_achete_clt_h
  ,SUM(a.Marge_clt ) AS Marge_clt_h
FROM DATA_MESH_PROD_CLIENT.WORK.stat_clt_histo_18mth a
INNER JOIN tgb b ON a.idorgens_achat=b.id_org_enseigne AND a.idmag_achat=b.id_magasin AND a.code_client=b.code_client
GROUP BY 1,2
ORDER BY 1,2; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_histo_clt_actif ORDER BY 1,2; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_compl AS
SELECT a.*,b.* 
FROM stat_mag_vte18mth_glb a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.stat_histo_clt_actif b ON a.ens_ferm = b.idens_histo AND a.idmag_ferm = b.idmag_histo
ORDER BY 1,2,3,4,5,6;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_compl ;

 --- tableau des statistiques pour toper les mag ou le client s'est rendu !!!
 
  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag AS
  SELECT a.ID_ORG_ENSEIGNE AS ens_ferm, a.ID_MAGASIN AS idmag_ferm, a.lib_magasin AS lib_magasin_ferm, type_ferm, a.date_fermeture_public, a.idorgens_achat AS enseigne_actif , a.mag_achat , mg.lib_magasin AS lib_magasin_actif
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN distanc_mag END ) AS distanc_Max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth_mag
  FROM tab_mag_vte_18mth_v a
  INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mg on a.idorgens_achat = mg.ID_ORG_ENSEIGNE and a.mag_achat = mg.ID_MAGASIN
  GROUP BY 1,2,3,4,5,6,7,8
  HAVING nbclt_actif_mag>0
  ORDER BY 1,2,3,4,5,6,7,8;
  
  -- SELECT * FROM stat_mag_vte18mth_glb_mag ORDER BY 1,2, nbclt_actif_mag DESC ;
 
 -- nous allons integrer les informations sur le nombre de cliebt potentiel 
 
  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_V2 AS
 WITH tab0 AS (SELECT DISTINCT a.ens_ferm, a.idmag_ferm, a.lib_magasin_ferm, a.type_ferm, a.date_fermeture_public, a.enseigne_actif , a.mag_achat , a.lib_magasin_actif,distanc_Max, a.periode_etud_max, a.delai_reachat_max,
 b.nb_client_potentiel, b.nbclt_actif, a.nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag, 
 a.nbclt_actif_03mth_mag, a.nbclt_actif_06mth_mag, a.nbclt_actif_09mth_mag, a.nbclt_actif_12mth_mag, a.nbclt_actif_15mth_mag
FROM stat_mag_vte18mth_glb_mag a
LEFT JOIN stat_mag_vte18mth_glb b ON a.ens_ferm=b.ens_ferm AND  a.idmag_ferm=b.idmag_ferm ),
tab1 AS (SELECT *, 
rank() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as rang_mag,
ROW_NUMBER() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as lign_mag
 FROM tab0)
SELECT DISTINCT ens_ferm, idmag_ferm, lib_magasin_ferm, type_ferm, date_fermeture_public, enseigne_actif , mag_achat , lib_magasin_actif,distanc_Max, periode_etud_max, delai_reachat_max,
 nb_client_potentiel, nbclt_actif, nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag,
 nbclt_actif_03mth_mag, nbclt_actif_06mth_mag, nbclt_actif_09mth_mag, nbclt_actif_12mth_mag, nbclt_actif_15mth_mag, lign_mag 
FROM tab1 WHERE lign_mag<=10; 

-- SELECT * FROM stat_mag_vte18mth_glb_mag_V2 ORDER BY ens_ferm, idmag_ferm, lign_mag ;

-- Zoom client par modul 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_modul AS
SELECT ID_ORG_ENSEIGNE AS ens_ferm, ID_MAGASIN AS idmag_ferm, type_emplacement, lib_magasin, type_ferm , date_fermeture_public, mag_client
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN delai_reachat END) AS delai_reachat_max
  ,AVG(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud AND distanc_mag BETWEEN 0 AND 50 THEN distanc_mag END ) AS distanc_moy
  ,MEDIAN(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud AND distanc_mag BETWEEN 0 AND 50 THEN distanc_mag END ) AS distanc_median
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 18 THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3,4,5,6,7
  ORDER BY 1,2,3,4,5,6,7;
 
 
 -- SELECT * FROM stat_mag_vte18mth_glb_modul ORDER BY 1,2,3,4,5,6,7; 
 

  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_mod1 AS
  SELECT a.ID_ORG_ENSEIGNE AS ens_ferm, a.ID_MAGASIN AS idmag_ferm, a.lib_magasin AS lib_magasin_ferm, type_ferm, a.date_fermeture_public, a.idorgens_achat AS enseigne_actif , a.mag_achat , mg.lib_magasin AS lib_magasin_actif
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN distanc_mag END ) AS distanc_Max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth_mag
  FROM tab_mag_vte_18mth_v a
  INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mg on a.idorgens_achat = mg.ID_ORG_ENSEIGNE and a.mag_achat = mg.ID_MAGASIN
  WHERE Mag_client='01-Mono MAG'
  GROUP BY 1,2,3,4,5,6,7,8
  HAVING nbclt_actif_mag>0
  ORDER BY 1,2,3,4,5,6,7,8;
  
  
  -- SELECT * FROM stat_mag_vte18mth_glb_mag_mod1 ORDER BY 1,2, nbclt_actif_mag DESC ;
 
  -- SELECT * FROM stat_mag_vte18mth_glb_modul ORDER BY 1,2,3,4,5,6,7; 
 
 
  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_V2_mod1 AS
 WITH tab0 AS (SELECT DISTINCT a.ens_ferm, a.idmag_ferm, a.lib_magasin_ferm, a.type_ferm, a.date_fermeture_public, a.enseigne_actif , a.mag_achat , a.lib_magasin_actif,distanc_Max, a.periode_etud_max, a.delai_reachat_max,
 b.nb_client_potentiel, b.nbclt_actif, a.nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag, 
 a.nbclt_actif_03mth_mag, a.nbclt_actif_06mth_mag, a.nbclt_actif_09mth_mag, a.nbclt_actif_12mth_mag, a.nbclt_actif_15mth_mag
FROM stat_mag_vte18mth_glb_mag_mod1 a
LEFT JOIN stat_mag_vte18mth_glb_modul b ON a.ens_ferm=b.ens_ferm AND  a.idmag_ferm=b.idmag_ferm AND Mag_client='01-Mono MAG'),
tab1 AS (SELECT *, 
rank() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as rang_mag,
ROW_NUMBER() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as lign_mag
 FROM tab0)
SELECT DISTINCT ens_ferm, idmag_ferm, lib_magasin_ferm, type_ferm, date_fermeture_public, enseigne_actif , mag_achat , lib_magasin_actif,distanc_Max, periode_etud_max, delai_reachat_max,
 nb_client_potentiel, nbclt_actif, nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag,
 nbclt_actif_03mth_mag, nbclt_actif_06mth_mag, nbclt_actif_09mth_mag, nbclt_actif_12mth_mag, nbclt_actif_15mth_mag, lign_mag 
FROM tab1 WHERE lign_mag<=5; 

-- SELECT * FROM stat_mag_vte18mth_glb_mag_V2_mod1 ORDER BY ens_ferm, idmag_ferm, lign_mag ;

--- Sur les Clients avec 2 magasins

  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_mod2 AS
  SELECT a.ID_ORG_ENSEIGNE AS ens_ferm, a.ID_MAGASIN AS idmag_ferm, a.lib_magasin AS lib_magasin_ferm, type_ferm, a.date_fermeture_public, a.idorgens_achat AS enseigne_actif , a.mag_achat , mg.lib_magasin AS lib_magasin_actif
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN distanc_mag END ) AS distanc_Max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth_mag
  FROM tab_mag_vte_18mth_v a
  INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mg on a.idorgens_achat = mg.ID_ORG_ENSEIGNE and a.mag_achat = mg.ID_MAGASIN
  WHERE Mag_client='02- 2 MAG'
  GROUP BY 1,2,3,4,5,6,7,8
  HAVING nbclt_actif_mag>0
  ORDER BY 1,2,3,4,5,6,7,8;
  
  SELECT * FROM stat_mag_vte18mth_glb_mag_mod2 ORDER BY 1,2, nbclt_actif_mag DESC ;
 
  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_V2_mod2 AS
 WITH tab0 AS (SELECT DISTINCT a.ens_ferm, a.idmag_ferm, a.lib_magasin_ferm, a.type_ferm, a.date_fermeture_public, a.enseigne_actif , a.mag_achat , a.lib_magasin_actif,distanc_Max, a.periode_etud_max, a.delai_reachat_max,
 b.nb_client_potentiel, b.nbclt_actif, a.nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag, 
 a.nbclt_actif_03mth_mag, a.nbclt_actif_06mth_mag, a.nbclt_actif_09mth_mag, a.nbclt_actif_12mth_mag, a.nbclt_actif_15mth_mag
FROM stat_mag_vte18mth_glb_mag_mod2 a
LEFT JOIN stat_mag_vte18mth_glb_modul b ON a.ens_ferm=b.ens_ferm AND  a.idmag_ferm=b.idmag_ferm AND Mag_client='02- 2 MAG'),
tab1 AS (SELECT *, 
rank() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as rang_mag,
ROW_NUMBER() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as lign_mag
 FROM tab0)
SELECT DISTINCT ens_ferm, idmag_ferm, lib_magasin_ferm, type_ferm, date_fermeture_public, enseigne_actif , mag_achat , lib_magasin_actif,distanc_Max, periode_etud_max, delai_reachat_max,
 nb_client_potentiel, nbclt_actif, nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag,
 nbclt_actif_03mth_mag, nbclt_actif_06mth_mag, nbclt_actif_09mth_mag, nbclt_actif_12mth_mag, nbclt_actif_15mth_mag, lign_mag 
FROM tab1 WHERE lign_mag<=5;

--- Pour les clints 3 magasins et plus 

  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_mod3 AS
  SELECT a.ID_ORG_ENSEIGNE AS ens_ferm, a.ID_MAGASIN AS idmag_ferm, a.lib_magasin AS lib_magasin_ferm, type_ferm, a.date_fermeture_public, a.idorgens_achat AS enseigne_actif , a.mag_achat , mg.lib_magasin AS lib_magasin_actif
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN distanc_mag END ) AS distanc_Max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN periode_etud END) AS periode_etud_max
  ,MAX(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN delai_reachat END) AS delai_reachat_max
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_mag
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth_mag
  FROM tab_mag_vte_18mth_v a
  INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mg on a.idorgens_achat = mg.ID_ORG_ENSEIGNE and a.mag_achat = mg.ID_MAGASIN
  WHERE Mag_client='03- 3 MAG et +'
  GROUP BY 1,2,3,4,5,6,7,8
  HAVING nbclt_actif_mag>0
  ORDER BY 1,2,3,4,5,6,7,8;
  
  --SELECT * FROM stat_mag_vte18mth_glb_mag_mod3 ORDER BY 1,2, nbclt_actif_mag DESC ;
 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_V2_mod3 AS
 WITH tab0 AS (SELECT DISTINCT a.ens_ferm, a.idmag_ferm, a.lib_magasin_ferm, a.type_ferm, a.date_fermeture_public, a.enseigne_actif , a.mag_achat , a.lib_magasin_actif,distanc_Max, a.periode_etud_max, a.delai_reachat_max,
 b.nb_client_potentiel, b.nbclt_actif, a.nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag, 
 a.nbclt_actif_03mth_mag, a.nbclt_actif_06mth_mag, a.nbclt_actif_09mth_mag, a.nbclt_actif_12mth_mag, a.nbclt_actif_15mth_mag
FROM stat_mag_vte18mth_glb_mag_mod3 a
LEFT JOIN stat_mag_vte18mth_glb_modul b ON a.ens_ferm=b.ens_ferm AND  a.idmag_ferm=b.idmag_ferm AND Mag_client='03- 3 MAG et +' ),
tab1 AS (SELECT *, 
rank() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as rang_mag,
ROW_NUMBER() over(partition by idmag_ferm order by nbclt_actif_mag DESC) as lign_mag
 FROM tab0)
SELECT DISTINCT ens_ferm, idmag_ferm, lib_magasin_ferm, type_ferm, date_fermeture_public, enseigne_actif , mag_achat , lib_magasin_actif,distanc_Max, periode_etud_max, delai_reachat_max,
 nb_client_potentiel, nbclt_actif, nbclt_actif_mag, nb_ticket_mag, CA_mag, qte_achete_mag, Marge_mag,
 nbclt_actif_03mth_mag, nbclt_actif_06mth_mag, nbclt_actif_09mth_mag, nbclt_actif_12mth_mag, nbclt_actif_15mth_mag, lign_mag 
FROM tab1 WHERE lign_mag<=5;

SELECT * FROM stat_mag_vte18mth_glb_mag_V2_mod3 ORDER BY ens_ferm, idmag_ferm, lign_mag ; 
SELECT * FROM stat_mag_vte18mth_glb_mag_V2_mod2 ORDER BY ens_ferm, idmag_ferm, lign_mag ; 
SELECT * FROM stat_mag_vte18mth_glb_mag_V2_mod1 ORDER BY ens_ferm, idmag_ferm, lign_mag ;


-- synthèse
 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_gbl_vte18mth_glb_v1 AS
 SELECT * FROM (
 (SELECT '00-Global' AS niv1 , '00-Global' AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth  
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2)
  UNION 
 (SELECT '01-type_ferm' AS niv1 , type_ferm AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2)
  UNION 
 (SELECT '02-type_emplacement' AS niv1 , type_emplacement AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2)
  UNION 
 (SELECT '03-ANNEE_FERMETURE' AS niv1 ,  CONCAT('AN_',YEAR(date_fermeture_public))  AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2)
  );
 
 
 SELECT * FROM stat_gbl_vte18mth_glb_v1 ORDER BY 1,2; 

SELECT * FROM tab_mag_vte_18mth_v; 
/***** analyse Reactivation Client par Typologie ***/ 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statgbl_vte18mth_modul AS
 SELECT * FROM (
 (SELECT mag_client, '00-Global' AS niv1 , '00-Global' AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth  
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
  UNION 
 (SELECT mag_client, '01-type_ferm' AS niv1 , type_ferm AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
  UNION 
 (SELECT mag_client, '02-type_emplacement' AS niv1 , type_emplacement AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
  UNION 
 (SELECT mag_client, '03-ANNEE_FERMETURE' AS niv1 ,  CONCAT('AN_',YEAR(date_fermeture_public))  AS niv2 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
  );

SELECT * FROM statgbl_vte18mth_modul ORDER BY 1,2,3 ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statgbl_vte18mth_zoom AS
SELECT mag_client, type_ferm, type_emplacement , CONCAT('AN_',YEAR(date_fermeture_public))  AS anne_ferm 
  ,COUNT(DISTINCT ID_MAGASIN) AS nb_mag_ferm 
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth  
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3,4 ; 

SELECT * FROM statgbl_vte18mth_zoom ORDER BY 1,2,3 ; 




 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_gbl_vte18mth_glb_v2 AS
 SELECT * FROM (
 (SELECT '00-Global' AS niv0 , '00-Global' AS niv1 , '00-Global' AS niv2 
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth  
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
  UNION
 (SELECT '01-Distance' AS niv0 , '01-Distance' AS niv1 , CASE WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 0 AND 3 THEN '00-03Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 4 AND 6 THEN '04-06Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 7 AND 9 THEN '07-09Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 10 AND 12 THEN '10-12Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 13 AND 15 THEN '13-15Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 16 AND 20 THEN '16-20Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 21 AND 30 THEN '21-30Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 31 AND 40 THEN '31-40Km'     
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 41 AND 50 THEN '41-50Km'          
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 51 AND 60 THEN '51-60Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag >60 THEN '60Km et + ' END AS niv2 
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth  
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
    UNION
 (SELECT '02-Distance / Type ferm' AS niv0 , type_ferm AS niv1 , CASE WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 0 AND 3 THEN '00-03Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 4 AND 6 THEN '04-06Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 7 AND 9 THEN '07-09Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 10 AND 12 THEN '10-12Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 13 AND 15 THEN '13-15Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 16 AND 20 THEN '16-20Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 21 AND 30 THEN '21-30Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 31 AND 40 THEN '31-40Km'     
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 41 AND 50 THEN '41-50Km'          
     WHEN distanc_mag IS NOT NULL AND distanc_mag BETWEEN 51 AND 60 THEN '51-60Km'
     WHEN distanc_mag IS NOT NULL AND distanc_mag >60 THEN '60Km et + ' END AS niv2 
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN code_client END ) AS nbclt_actif
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN id_ticket END) AS nb_ticket_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_TTC END) AS CA_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN QUANTITE_LIGNE END) AS qte_achete_clt
  ,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat<=periode_etud THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth  
  FROM tab_mag_vte_18mth_v
  GROUP BY 1,2,3)
  ); 
  
SELECT * FROM stat_gbl_vte18mth_glb_v2 ORDER BY 1,2,3; 


/**** Mise à jour à Faire sur le projet avec Finalisation 
 * Pour les clients reactivés, Analyser les indicateurs de performance avant fermeture et comparer au performances après fermeture , 
 * cela permettra de projeter un CA des clients qui sont reactivés 
 * Parmi les reactivés , qui sont ceux qui ont acheté uniquement sur le Web ! 
 * ***/ 


, 