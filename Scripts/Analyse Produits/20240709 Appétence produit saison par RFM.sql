/*** Demande d'analyse Client 
 * Appétence produit/saison par RFM
 * En tant que responsable de l'animation commerciale, je souhaite pouvoir connaître l'appétence des clients (par segment RFM)
 * aux produits/famille/saison. Cela me permettra d'affiner ma construction des offres commerciales.
***/ 

/**** analyse sur N versus N-1 ***/

SET dtdeb = Date('2024-01-01');
SET dtfin = DAte('2024-06-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb, $dtfin;

SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb));
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.infoticket_N AS 
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT WHERE id_niveau=5 AND DATE_PARTITION=DATE_FROM_PARTS(YEAR($dtfin) , MONTH($dtfin), 1)),
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
vd.CODE_SKU, vd.Code_RCT,
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
mag.*,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE,
CASE 
            WHEN pdt.LIB_FAMILLE_ACHAT = 'Bermuda' THEN 'Bermuda'
             WHEN pdt.LIB_FAMILLE_ACHAT = 'Pantalon Denim' THEN 'Pantalon Denim'
            WHEN pdt.LIB_FAMILLE_ACHAT = 'Underwear' THEN 'Underwear'
            ELSE pdt.LIB_GROUPE_FAMILLE
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM
from DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
LEFT join tabtg seg on vd.CODE_CLIENT = seg.CODE_CLIENT
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0'
  ) ,
tab0 AS (SELECT DISTINCT SKU, LIBELLE_GROUPE_FAMILLE_ACHAT , PRIX_VENTE_INITIAL 
FROM DHB_PROD.DNR.DN_PRODUIT 
WHERE FLAG_REFERENCE_COULEUR_COURANT =1 AND PRIX_VENTE_INITIAL IS NOT NULL),
E_PRIX AS (SELECT *, NTILE(4) OVER(PARTITION BY LIBELLE_GROUPE_FAMILLE_ACHAT ORDER BY PRIX_VENTE_INITIAL) AS QUARTILE_PRIX 
FROM tab0 )
SELECT a.* , b.PRIX_VENTE_INITIAL, b.QUARTILE_PRIX 
FROM tickets a 
LEFT JOIN E_PRIX b ON a.Code_SKU=b.SKU; 

-- Statistique general par famille
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statinfo_N AS 
SELECT * FROM (
SELECT '00-GLOBAL' AS Grp, '00-GLOBAL' AS typo, '00-GLOBAL' AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '01-GROUPE_FAMILLE' AS Grp, LIB_GROUPE_FAMILLE_V2 AS typo, SEGMENT_RFM AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '02-FAMILLE_ACHAT' AS Grp, LIB_FAMILLE_ACHAT AS typo, SEGMENT_RFM AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3
 UNION 
SELECT '00-GLOBAL' AS Grp, '00-GLOBAL' AS typo, SEGMENT_RFM AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '01-GROUPE_FAMILLE' AS Grp, LIB_GROUPE_FAMILLE_V2 AS typo, '00-GLOBAL' AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '02-FAMILLE_ACHAT' AS Grp, LIB_FAMILLE_ACHAT AS typo, '00-GLOBAL' AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3
)
ORDER BY 1,2,3; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_N ORDER BY 1,2,3; 

--- Tableau Pivot avec les informations client 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statclient_N AS 
WITH tab0 AS (select DISTINCT Grp, typo, modalite , nb_clt
FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_N)
select * 
FROM tab0
pivot (SUM(nb_clt) for modalite in ('00-GLOBAL','01_VIP', '02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12', '11_INA24','12_NOSEG')) ORDER BY 1,2 ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.statclient_N  ORDER BY 1,2 ;


/**** Information sur l'année N-1 ***/ 


SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb));
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb_Nm1 , $dtfin_Nm1 ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1 AS 
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT WHERE id_niveau=5 AND DATE_PARTITION=DATE_FROM_PARTS(YEAR($dtfin_Nm1) , MONTH($dtfin_Nm1), 1)),
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
vd.CODE_SKU, vd.Code_RCT,
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
mag.*,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE,
CASE 
            WHEN pdt.LIB_FAMILLE_ACHAT = 'Bermuda' THEN 'Bermuda'
             WHEN pdt.LIB_FAMILLE_ACHAT = 'Pantalon Denim' THEN 'Pantalon Denim'
            WHEN pdt.LIB_FAMILLE_ACHAT = 'Underwear' THEN 'Underwear'
            ELSE pdt.LIB_GROUPE_FAMILLE
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM
from DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE  vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
LEFT join tabtg seg on vd.CODE_CLIENT = seg.CODE_CLIENT
where vd.date_ticket BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0'
  ) ,
tab0 AS (SELECT DISTINCT SKU, LIBELLE_GROUPE_FAMILLE_ACHAT , PRIX_VENTE_INITIAL 
FROM DHB_PROD.DNR.DN_PRODUIT 
WHERE FLAG_REFERENCE_COULEUR_COURANT =1 AND PRIX_VENTE_INITIAL IS NOT NULL),
E_PRIX AS (SELECT *, NTILE(4) OVER(PARTITION BY LIBELLE_GROUPE_FAMILLE_ACHAT ORDER BY PRIX_VENTE_INITIAL) AS QUARTILE_PRIX 
FROM tab0 )
SELECT a.* , b.PRIX_VENTE_INITIAL, b.QUARTILE_PRIX 
FROM tickets a 
LEFT JOIN E_PRIX b ON a.Code_SKU=b.SKU; 

-- Statistique general par famille
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statinfo_Nm1 AS 
SELECT * FROM (
SELECT '00-GLOBAL' AS Grp, '00-GLOBAL' AS typo, '00-GLOBAL' AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '01-GROUPE_FAMILLE' AS Grp, LIB_GROUPE_FAMILLE_V2 AS typo, SEGMENT_RFM AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '02-FAMILLE_ACHAT' AS Grp, LIB_FAMILLE_ACHAT AS typo, SEGMENT_RFM AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3
 UNION 
SELECT '00-GLOBAL' AS Grp, '00-GLOBAL' AS typo, SEGMENT_RFM AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '01-GROUPE_FAMILLE' AS Grp, LIB_GROUPE_FAMILLE_V2 AS typo, '00-GLOBAL' AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3 
UNION 
SELECT '02-FAMILLE_ACHAT' AS Grp, LIB_FAMILLE_ACHAT AS typo, '00-GLOBAL' AS modalite 
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3
)
ORDER BY 1,2,3; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_Nm1 ORDER BY 1,2,3; 

--- Tableau Pivot avec les informations client 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.statclient_Nm1 AS 
WITH tab0 AS (select DISTINCT Grp, typo, modalite , nb_clt
FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_Nm1)
select * 
FROM tab0
pivot (SUM(nb_clt) for modalite in ('00-GLOBAL','01_VIP', '02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12', '11_INA24','12_NOSEG')) ORDER BY 1,2 ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.statclient_Nm1  ORDER BY 1,2 ;

SELECT DISTINCT * FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_Nm1 ORDER BY 1,2 ;

SELECT DISTINCT TYPO, modalite, nb_clt AS nbclt_segxts FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_Nm1 WHERE GRP='01-GROUPE_FAMILLE' ORDER BY 1,2 ;
/***** 
 * Information quartile de prix 
 */

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_prix_N AS 
WITH tab0 AS 
( SELECT Segment_RFM, LIB_GROUPE_FAMILLE_V2, quartile_prix
,MIN(PRIX_VENTE_INITIAL) AS min_prix_base
,Max(PRIX_VENTE_INITIAL) AS max_prix_base
,Count(DISTINCT Code_SKU) AS nb_Produits
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_N
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3), 
tgb AS ( SELECT DISTINCT TYPO, modalite, nb_clt AS nbclt_segxts FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_N WHERE GRP='01-GROUPE_FAMILLE')
SELECT a.*, b.nbclt_segxts
FROM tab0 a 
LEFT JOIN tgb b ON a.Segment_RFM=b.modalite AND a.LIB_GROUPE_FAMILLE_V2=b.typo
ORDER BY 1,2,3;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_prix_N ORDER BY 1,2,3;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_prix_Nm1 AS 
WITH tab0 AS 
( SELECT Segment_RFM, LIB_GROUPE_FAMILLE_V2, quartile_prix
,MIN(PRIX_VENTE_INITIAL) AS min_prix_base
,Max(PRIX_VENTE_INITIAL) AS max_prix_base
,Count(DISTINCT Code_SKU) AS nb_Produits
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT id_ticket) AS nb_ticket
,SUM(MONTANT_TTC ) AS CA_glb
,SUM(QUANTITE_LIGNE ) AS qte_glb
,SUM(MONTANT_MARGE_SORTIE ) AS Marge_glbt	
FROM DATA_MESH_PROD_CLIENT.WORK.infoticket_Nm1
WHERE LIB_GROUPE_FAMILLE_V2 NOT IN ('','Services','Indeterminé')
GROUP BY 1,2,3), 
tgb AS ( SELECT DISTINCT TYPO, modalite, nb_clt AS nbclt_segxts FROM DATA_MESH_PROD_CLIENT.WORK.statinfo_Nm1 WHERE GRP='01-GROUPE_FAMILLE')
SELECT a.*, b.nbclt_segxts
FROM tab0 a 
LEFT JOIN tgb b ON a.Segment_RFM=b.modalite AND a.LIB_GROUPE_FAMILLE_V2=b.typo
ORDER BY 1,2,3; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_prix_Nm1 ORDER BY 1,2,3 ;  
