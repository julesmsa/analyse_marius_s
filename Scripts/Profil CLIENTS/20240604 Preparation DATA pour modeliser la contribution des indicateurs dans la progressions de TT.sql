--- Preparation DATA pour modeliser la contribution des indicateurs dans la progressions de TT 

-- Analyse sur magasin comparable avec données de ventes comparable 

-- Période d'étude Analyse des TT et ventes sur la S01 à S22

-- S01 à S22 2023 ==> Du 01 janvier 2023 au 02 juin 2023 
-- S01 à S22 2024 ==> Du 01 janvier 2024 au 04 juin 2024 

SET dtdeb = Date('2024-01-01');
SET dtfin = DAte('2024-06-04'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();

SET dtdeb_m1= Date('2023-01-01');
SET dtfin_m1= Date('2023-06-02');

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 


SELECT DISTINCT lib_statut FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.BASE_c_trafic AS 
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND (code_pays = $PAYS1 or code_pays = $PAYS2) AND lib_statut='Ouvert'),
trafic_mag as ( SELECT 
 a.ID_ORG_ENSEIGNE
,a.ID_MAGASIN
,DATE(DATEH_TRAFIC) AS DATE_TRAFIC, 
SUM(NOMBRE_ENTREE) AS SUM_ENTREE,
SUM(NOMBRE_SORTIES) AS SUM_SORTIES,
SUM(NOMBRE_PASSAGE) AS SUM_PASSAGE
FROM DATA_MESH_PROD_RETAIL.HUB.DMF_TRAFIC_MAGASIN a
INNER JOIN Magasin mag  on a.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and a.ID_MAGASIN = mag.ID_MAGASIN AND type_emplacement IN ('PAC','CC', 'CV','CCV')
WHERE (DATE(DATEH_TRAFIC) BETWEEN $dtdeb AND $dtfin OR DATE(DATEH_TRAFIC) BETWEEN $dtdeb_m1 AND $dtfin_m1) and (a.ID_ORG_ENSEIGNE = $ENSEIGNE1 or a.ID_ORG_ENSEIGNE = $ENSEIGNE2) and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
GROUP BY 1,2,3 )
SELECT *, 
FROM trafic_mag; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.BASE_c_trafic; 


--- Construction des ventes par semaine 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.BASE_c_trafic AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND (code_pays = $PAYS1 or code_pays = $PAYS2) AND lib_statut='Ouvert'),
tickets as (
Select vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.date_ticket 
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
INNER JOIN Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where (DATE(date_ticket) BETWEEN $dtdeb AND $dtfin OR DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  GROUP BY 1,2,3)

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE ; 

tab_am AS (SELECT DISTINCT ID_ENSEIGNE, ID_ACTION_MARKETING, LIBELLE_ACTION_MARKETING, CATEGORIE_ACTION_MARKETING  FROM DATA_MESH_PROD_RETAIL.HUB.DMD_ACTION_MARKETING)





-- 