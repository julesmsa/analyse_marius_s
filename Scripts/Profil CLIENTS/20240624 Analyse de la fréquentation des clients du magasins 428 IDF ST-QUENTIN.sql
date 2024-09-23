/**** Analyse Client 
Nous aurions besoin de savoir si nos clients du magasin 428 consomment dans d’autres de nos magasins aux alentours ***/ 

SET dtdeb = Date('2023-06-01');
SET dtfin = DAte('2024-05-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

select $dtdeb, $dtfin;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_client_Mag428 AS 
WITH top_clt_428 as (
Select  DISTINCT CODE_CLIENT AS id_client
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2) AND vd.ID_MAGASIN=428 
  AND code_client IS NOT NULL AND code_client !='0'
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2))
SELECT * FROM ( 
SELECT 0 AS ID_ORG_ENSEIGNE , 0 AS ID_MAGASIN, '00- Global' AS LIB_MAGASIN  
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE a
inner join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on a.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and a.ID_MAGASIN = mag.ID_MAGASIN
INNER JOIN top_clt_428 c ON a.code_client=c.id_client
where a.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (a.ID_ORG_ENSEIGNE = $ENSEIGNE1 or a.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  AND a.code_client IS NOT NULL AND a.code_client !='0' and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  GROUP BY 1,2,3
UNION 
SELECT a.ID_ORG_ENSEIGNE , a.ID_MAGASIN, mag.LIB_MAGASIN  
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE a
inner join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on a.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and a.ID_MAGASIN = mag.ID_MAGASIN
INNER JOIN top_clt_428 c ON a.code_client=c.id_client
where a.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (a.ID_ORG_ENSEIGNE = $ENSEIGNE1 or a.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  AND a.code_client IS NOT NULL AND a.code_client !='0' and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
    GROUP BY 1,2,3)
ORDER BY 1,2,3; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_client_Mag428 ORDER BY nb_clt DESC ; 

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
FROM tab0 a, tab1 b ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_client_Mag428_V2 AS 
WITH trfb0 AS ( 
SELECT DISTINCT id_magasin_A, id_magasin_b, id_ref_mag,Distanc_mag
FROM DATA_MESH_PROD_CLIENT.WORK.tab_mag_distance 
WHERE id_magasin_A = 428)
SELECT a.*, b.distanc_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.stat_client_Mag428 a 
LEFT JOIN trfb0 b ON a.Id_magasin=b.id_magasin_b ; 
;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_client_Mag428_V2 ORDER BY nb_clt DESC ; 


