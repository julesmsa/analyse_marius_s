--- Analyse impact plan Co Sur le recrutement Client 

SET dtdeb_EXON = Date('2024-01-01');
SET dtfin_EXON = DAte('2024-05-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_EXON, $dtfin_EXON;

SET dtdeb_EXONm1 = to_date(dateadd('year', -1, $dtdeb_EXON));
SET dtfin_EXONm1 = to_date(dateadd('year', -1, $dtfin_EXON)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_EXON AS
WITH tick_clt as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
mag.type_emplacement, mag.code_magasin AS code_mag, mag.lib_magasin, mag.lib_statut, mag.id_concept, mag.lib_enseigne, mag.code_pays
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where vd.date_ticket BETWEEN DATE($dtdeb_EXON) AND DATE($dtfin_EXON) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) )  
  SELECT a.*, b.Date_recrutement, b.date_naissance, gender 
  ,datediff(MONTH ,Date_recrutement,$dtfin_EXON) AS ANCIENNETE_CLIENT
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin_EXON),2) AS AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 90 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
, CASE WHEN DATE(Date_recrutement) BETWEEN DATE($dtdeb_EXON) AND DATE($dtfin_EXON) THEN '02-NOUVEAUX' ELSE '01-ANCIENS' END AS TYP_CLIENT
  FROM tick_clt a 
  INNER JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE b ON a.CODE_CLIENT=b.CODE_CLIENT 
 WHERE a.code_client IS NOT NULL AND a.code_client !='0'; 
 
 
 SELECT DISTINCT CLASSE_AGE  FROM DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_EXON ORDER BY 1; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_EXON AS
SELECT * , ROUND(nb_newclt_EXON/nb_clt_EXON,4) AS tx_newclt
FROM (
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXON,
Count(DISTINCT id_ticket) AS nb_ticket_EXON,
SUM(MONTANT_TTC) AS CA_EXON,
SUM(QUANTITE_LIGNE) AS qte_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXON,
FROM BASE_INFOCLT_EXON 
GROUP BY 1,2
UNION 
SELECT '01_MOIS' AS typo_clt, CASE WHEN MONTH(date_ticket)<10 THEN CONCAT('MOIS_M0',MONTH(date_ticket)) ELSE CONCAT('MOIS_M',MONTH(date_ticket)) END AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXON,
Count(DISTINCT id_ticket) AS nb_ticket_EXON,
SUM(MONTANT_TTC) AS CA_EXON,
SUM(QUANTITE_LIGNE) AS qte_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXON,
FROM BASE_INFOCLT_EXON 
GROUP BY 1,2
UNION 
SELECT '01_SEMAINE' AS typo_clt, CASE WHEN WEEK(date_ticket)<10 THEN CONCAT('WEEK_S0',WEEK(date_ticket)) ELSE CONCAT('WEEK_S',WEEK(date_ticket)) END AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXON,
Count(DISTINCT id_ticket) AS nb_ticket_EXON,
SUM(MONTANT_TTC) AS CA_EXON,
SUM(QUANTITE_LIGNE) AS qte_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXON,
FROM BASE_INFOCLT_EXON 
GROUP BY 1,2
UNION 
SELECT '01_jOUR' AS typo_clt, CONCAT('J_',DATE(date_ticket)) AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXON,
Count(DISTINCT id_ticket) AS nb_ticket_EXON,
SUM(MONTANT_TTC) AS CA_EXON,
SUM(QUANTITE_LIGNE) AS qte_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXON,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXON,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXON,
FROM BASE_INFOCLT_EXON 
GROUP BY 1,2)
ORDER BY 1,2; 

 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_EXON ORDER BY 1,2;

-- sur N-1 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_EXONm1 AS
WITH tick_clt as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
mag.type_emplacement, mag.code_magasin AS code_mag, mag.lib_magasin, mag.lib_statut, mag.id_concept, mag.lib_enseigne, mag.code_pays
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where vd.date_ticket BETWEEN DATE($dtdeb_EXONm1) AND DATE($dtfin_EXONm1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) )  
  SELECT a.*, b.Date_recrutement, b.date_naissance, gender 
  ,datediff(MONTH ,Date_recrutement,$dtfin_EXONm1) AS ANCIENNETE_CLIENT
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin_EXONm1),2) AS AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 90 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
, CASE WHEN DATE(Date_recrutement) BETWEEN DATE($dtdeb_EXONm1) AND DATE($dtfin_EXONm1) THEN '02-NOUVEAUX' ELSE '01-ANCIENS' END AS TYP_CLIENT
  FROM tick_clt a 
  INNER JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE b ON a.CODE_CLIENT=b.CODE_CLIENT 
 WHERE a.code_client IS NOT NULL AND a.code_client !='0'; 
 
 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_INFOCLT_EXONm1 ; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_EXONm1 AS
SELECT * , ROUND(nb_newclt_EXONm1/nb_clt_EXONm1,4) AS tx_newclt
FROM (
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXONm1,
Count(DISTINCT id_ticket) AS nb_ticket_EXONm1,
SUM(MONTANT_TTC) AS CA_EXONm1,
SUM(QUANTITE_LIGNE) AS qte_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXONm1,
FROM BASE_INFOCLT_EXONm1 
GROUP BY 1,2
UNION 
SELECT '01_MOIS' AS typo_clt, CASE WHEN MONTH(date_ticket)<10 THEN CONCAT('MOIS_M0',MONTH(date_ticket)) ELSE CONCAT('MOIS_M',MONTH(date_ticket)) END AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXONm1,
Count(DISTINCT id_ticket) AS nb_ticket_EXONm1,
SUM(MONTANT_TTC) AS CA_EXONm1,
SUM(QUANTITE_LIGNE) AS qte_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXONm1,
FROM BASE_INFOCLT_EXONm1 
GROUP BY 1,2
UNION 
SELECT '01_SEMAINE' AS typo_clt, CASE WHEN WEEK(date_ticket)<10 THEN CONCAT('WEEK_S0',WEEK(date_ticket)) ELSE CONCAT('WEEK_S',WEEK(date_ticket)) END AS modalite,
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXONm1,
Count(DISTINCT id_ticket) AS nb_ticket_EXONm1,
SUM(MONTANT_TTC) AS CA_EXONm1,
SUM(QUANTITE_LIGNE) AS qte_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXONm1,
FROM BASE_INFOCLT_EXONm1 
GROUP BY 1,2
UNION 
SELECT '01_jOUR' AS typo_clt, CONCAT('J_',DATE(date_ticket)) AS modalite, 
MIN(date_ticket) AS min_date_ticket, 
MAX(date_ticket)  AS max_date_ticket,
Count(DISTINCT code_client) AS nb_clt_EXONm1,
Count(DISTINCT id_ticket) AS nb_ticket_EXONm1,
SUM(MONTANT_TTC) AS CA_EXONm1,
SUM(QUANTITE_LIGNE) AS qte_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client END ) AS nb_newclt_EXONm1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN id_ticket END) AS nb_newticket_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN MONTANT_TTC END) AS CAnew_EXONm1,
SUM(CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN QUANTITE_LIGNE END) AS qtenew_EXONm1,
FROM BASE_INFOCLT_EXONm1 
GROUP BY 1,2)
ORDER BY 1,2; 

 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_EXONm1 ORDER BY 1,2; 


 --- Jointure des informations 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT AS
 SELECT a.* ,
nb_clt_EXONm1, nb_ticket_EXONm1, CA_EXONm1, qte_EXONm1, nb_newclt_EXONm1, nb_newticket_EXONm1, CAnew_EXONm1, qtenew_EXONm1
FROM DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_EXON a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_EXONm1 b ON a.typo_clt=b.typo_clt AND a.modalite=b.modalite
ORDER BY 1,2; 
 
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT ORDER BY 1,2; 




-- Information sur l'age des client au global 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_AGE_EXON AS
WITH tab0 AS (SELECT DISTINCT Code_Client, TYP_CLIENT, AGE_C, CLASSE_AGE FROM BASE_INFOCLT_EXON), 
tab1 AS (
SELECT * FROM (
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite,
Count(DISTINCT code_client) AS nb_clt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt
FROM tab0
GROUP BY 1,2
UNION 
SELECT '01_CLASSE_AGE' AS typo_clt, CLASSE_AGE AS modalite,
Count(DISTINCT code_client) AS nb_clt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt
FROM tab0
GROUP BY 1,2))
SELECT *, 
ROW_NUMBER() over(partition by typo_clt order by nb_clt DESC) as lign_typ
FROM tab1
ORDER BY nb_clt DESC ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_AGE_m1 AS
WITH tab0 AS (SELECT DISTINCT Code_Client, TYP_CLIENT, AGE_C, CLASSE_AGE FROM BASE_INFOCLT_EXONm1), 
tab1 AS (
SELECT * FROM (
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite,
Count(DISTINCT code_client) AS nb_clt_m1,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt_m1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt_m1,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt_m1
FROM tab0
GROUP BY 1,2
UNION 
SELECT '01_CLASSE_AGE' AS typo_clt, CLASSE_AGE AS modalite,
Count(DISTINCT code_client) AS nb_clt_m1,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt_m1,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt_m1,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt_m1
FROM tab0
GROUP BY 1,2))
SELECT *, 
ROW_NUMBER() over(partition by typo_clt order by nb_clt_m1 DESC) as lign_typ
FROM tab1
ORDER BY nb_clt_m1 DESC ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stall_ICLT_AGE AS
SELECT a.typo_clt, a.modalite, nb_clt, age_clt, nb_newclt, age_newclt, 
nb_clt_m1, age_clt_m1, nb_newclt_m1, age_newclt_m1
FROM DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_AGE_EXON a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_AGE_m1 b ON a.typo_clt=b.typo_clt AND a.modalite=b.modalite
ORDER BY 1,2;

SELECT * FROM Stall_ICLT_AGE ORDER BY 1,2;



-- Information sur l'age des client au semaine 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_AGE_EXON_sem AS
WITH tab0 AS (SELECT DISTINCT Code_Client, TYP_CLIENT, CASE WHEN WEEK(date_ticket)<10 THEN CONCAT('WEEK_S0',WEEK(date_ticket)) ELSE CONCAT('WEEK_S',WEEK(date_ticket)) END AS semaine, 
AGE_C, CLASSE_AGE FROM BASE_INFOCLT_EXON), 
tab1 AS (
SELECT * FROM (
SELECT '00_semaine' AS typo_clt, semaine AS modalite,
Count(DISTINCT code_client) AS nb_clt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt
FROM tab0
GROUP BY 1,2
UNION 
SELECT semaine AS typo_clt, CLASSE_AGE AS modalite,
Count(DISTINCT code_client) AS nb_clt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt
FROM tab0
GROUP BY 1,2))
SELECT *, 
ROW_NUMBER() over(partition by typo_clt order by nb_clt DESC) as lign_typ,
ROW_NUMBER() over(partition by typo_clt order by nb_newclt DESC) as lign_typ_new
FROM tab1
ORDER BY nb_clt DESC ; 

SELECT * FROM STAT_ICLT_AGE_EXON_sem ORDER BY 1,2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_ICLT_AGE_EXONm1_sem AS
WITH tab0 AS (SELECT DISTINCT Code_Client, TYP_CLIENT, CASE WHEN WEEK(date_ticket)<10 THEN CONCAT('WEEK_S0',WEEK(date_ticket)) ELSE CONCAT('WEEK_S',WEEK(date_ticket)) END AS semaine, 
AGE_C, CLASSE_AGE FROM BASE_INFOCLT_EXONm1), 
tab1 AS (
SELECT * FROM (
SELECT '00_semaine' AS typo_clt, semaine AS modalite,
Count(DISTINCT code_client) AS nb_clt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt
FROM tab0
GROUP BY 1,2
UNION 
SELECT semaine AS typo_clt, CLASSE_AGE AS modalite,
Count(DISTINCT code_client) AS nb_clt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C end),1) AS age_clt,
Count(DISTINCT CASE WHEN TYP_CLIENT='02-NOUVEAUX' THEN code_client end) AS nb_newclt,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 AND TYP_CLIENT='02-NOUVEAUX' THEN AGE_C end),1) AS age_newclt
FROM tab0
GROUP BY 1,2))
SELECT *, 
ROW_NUMBER() over(partition by typo_clt order by nb_clt DESC) as lign_typ,
ROW_NUMBER() over(partition by typo_clt order by nb_newclt DESC) as lign_typ_new
FROM tab1
ORDER BY nb_clt DESC ; 

SELECT * FROM STAT_ICLT_AGE_EXONm1_sem ORDER BY 1,2; 


--- ACTION direct des campagnes marketing 

SELECT * FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT WHERE ID_NIVEAU = 2 ;

SELECT DISTINCT CATEGORIE, SOUS_CATEGORIE FROM DATA_MESH_PROD_CLIENT.SHARED.T_OBT_CLIENT WHERE ID_NIVEAU = 2 AND year(DATE_ENVOI)=2024; 










