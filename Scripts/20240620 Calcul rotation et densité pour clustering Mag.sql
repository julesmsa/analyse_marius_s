/***** construction de la TABLE pour le JAM *****/ 

-- Analyse sur la période de 12 mois allant du 01 juin 2023 au 31 Mai 2024

  /*** Analyse du fichier Stock par semaine ****/ 
/*
SELECT 
S.ID_ENTITE AS ID_MAGASIN,
S.DATE AS DATE_PHOTO_STOCK,
MAX(T.CODE_ANNEE_SEMAINE) AS ID_SEMAINE_ANNEE_STOCK,
GREATEST(SUM(S.QTE),0) AS QTE_EN_STOCK --- Si pas de ligne cela veut dire que le stock = 0
FROM DATA_MESH_PROD_SUPPLY.HUB.DMF_STK_PHOTO_JOUR S
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_TPS_JOUR T --- TABLE CONTENANT UN CALENDRIER
ON T.DATE_DATE=S.DATE
AND T.NUM_JOUR_SEMAINE = 1 --- Le lundi de la semaine
AND T.NUM_ANNEE = 2023
WHERE S.ETAT_STOCK = 'ERP_DISPO' --- Etat du stock des magasins physiques 
GROUP BY ID_MAGASIN, DATE_PHOTO_STOCK
ORDER BY ID_MAGASIN, DATE_PHOTO_STOCK ; 
*/
-- Integration des informations pour le calcul de la rotation et du stock 

SET dtdeb = Date('2023-06-01');
SET dtfin = DAte('2024-05-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

select $dtdeb, $dtfin;

-- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.LISTMAG_CLUSTERV0; 

SELECT   * FROM   DATA_MESH_PROD_RETAIL.WORK.LISTMAG_CLUSTERV0MAG ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_stock_vente AS 
WITH tabmag AS (SELECT   DISTINCT Mag,
CAST( REPLACE (surface_vente,',','.') AS FLOAT ) AS surface_mag
FROM  DATA_MESH_PROD_RETAIL.WORK.LISTMAG_CLUSTERV0MAG),
LISTE_MAGASINS AS (
    SELECT DISTINCT ID_MAGASIN AS num_mag,
        TO_CHAR(ID_MAGASIN) AS ID_MAGASIN,
        COALESCE (a.SURFACE_COMMERCIALE,b.surface_mag) AS SURFACE_COMMERCIALE       
    FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN a 
    INNER JOIN tabmag b ON a.ID_MAGASIN=b.mag
    WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) 
    AND type_emplacement IN ('PAC','CC', 'CV','CCV')
    AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
-- Détermine la quantité en stock par sku par magasin en debut de semaine
QTE_STOCK_START_WEEK AS (
SELECT 
S.ID_ENTITE AS ID_MAGASIN,
S.DATE AS DATE_PHOTO_STOCK,
MAX(T.CODE_ANNEE_SEMAINE) AS ID_SEMAINE_ANNEE_STOCK,
GREATEST(SUM(S.QTE),0) AS QTE_EN_STOCK --- Si pas de ligne cela veut dire que le stock = 0
FROM DATA_MESH_PROD_SUPPLY.HUB.DMF_STK_PHOTO_JOUR S
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_TPS_JOUR T --- TABLE CONTENANT UN CALENDRIER
ON T.DATE_DATE=S.DATE
AND T.NUM_JOUR_SEMAINE = 1 --- Le lundi de la semaine
WHERE S.ETAT_STOCK = 'ERP_DISPO' --- Etat du stock des magasins physiques 
    AND S.DATE BETWEEN DATE($dtdeb) AND DATE($dtfin)
GROUP BY ID_MAGASIN, DATE_PHOTO_STOCK
ORDER BY ID_MAGASIN, DATE_PHOTO_STOCK ),
-- Déterminer les quantités de sku vendues par magasin et par semaine
QTE_VENDUE_IN_WEEK AS (
    SELECT 
        TO_CHAR(L.ID_MAGASIN) AS ID_MAGASIN,
        --TO_DOUBLE(CONCAT(WEEKISO(L.date_ticket), YEAROFWEEKISO(L.date_ticket))) AS ID_SEMAINE_ANNEE_VENTE,
        YEAROFWEEKISO(L.date_ticket)*100+WEEKISO(L.date_ticket) AS ID_SEMAINE_ANNEE_VENTE,        
        SUM(QUANTITE_LIGNE) AS QTE_VENDUE, 
        SUM(montant_ttc) AS CA_MAG
    FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE L
    LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on L.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and L.ID_MAGASIN = mag.ID_MAGASIN
    where L.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (L.ID_ORG_ENSEIGNE = $ENSEIGNE1 or L.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
    GROUP BY L.ID_MAGASIN, ID_SEMAINE_ANNEE_VENTE
    ORDER BY L.ID_MAGASIN, ID_SEMAINE_ANNEE_VENTE)
SELECT  c.*,
    a.ID_SEMAINE_ANNEE_VENTE AS ID_SEMAINE_ANNEE,
    a.QTE_VENDUE, a.CA_MAG,
    QTE_EN_STOCK,
    CASE WHEN QTE_VENDUE>0 THEN QTE_EN_STOCK/QTE_VENDUE END  AS ROTATION_SEM,
    CASE WHEN SURFACE_COMMERCIALE>0 THEN QTE_EN_STOCK/SURFACE_COMMERCIALE END  AS DENSITE_SEM
FROM QTE_VENDUE_IN_WEEK a
LEFT JOIN QTE_STOCK_START_WEEK b ON a.ID_SEMAINE_ANNEE_VENTE = b.ID_SEMAINE_ANNEE_STOCK AND a.ID_MAGASIN = B.ID_MAGASIN
INNER JOIN LISTE_MAGASINS c ON A.ID_MAGASIN = C.ID_MAGASIN;
   
   
 -- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_stock_vente WHERE QTE_EN_STOCK>0

-- Calcul de la densite moyenne par magasin  

 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_stock_vente ORDER BY 1,4; 
 

-- statistiques des variables 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_tabstock_vente AS 
SELECT * FROM (
SELECT 00 AS num_mag, '00-Global' AS id_magasin, 
COUNT(DISTINCT id_semaine_annee) AS nb_sem,
AVG(surface_commerciale) AS Surface_moy, 
Sum(QTE_VENDUE) AS QTE_VENDUE_GBL,
Sum(CA_MAG) AS CA_GBL,
AVG(QTE_VENDUE) AS QTE_VENDUE_MOY,
MIN(QTE_EN_STOCK) AS QTE_STOCK_MIN,
AVG(QTE_EN_STOCK) AS QTE_STOCK_MOY,
MAX(QTE_EN_STOCK) AS QTE_STOCK_MAX,
MIN(ROTATION_SEM) AS ROTATION_MIN,
AVG(ROTATION_SEM) AS ROTATION_MOY,
MAX(ROTATION_SEM) AS ROTATION_MAX,
MIN(DENSITE_SEM) AS DENSITE_MIN,
AVG(DENSITE_SEM) AS DENSITE_MOY,
MAX(DENSITE_SEM) AS DENSITE_MAX,
FROM DATA_MESH_PROD_RETAIL.WORK.stat_stock_vente 
GROUP BY 1,2
UNION
SELECT num_mag, id_magasin, 
COUNT(DISTINCT id_semaine_annee) AS nb_sem,
AVG(surface_commerciale) AS Surface_moy, 
Sum(QTE_VENDUE) AS QTE_VENDUE_GBL,
Sum(CA_MAG) AS CA_GBL,
AVG(QTE_VENDUE) AS QTE_VENDUE_MOY,
MIN(QTE_EN_STOCK) AS QTE_STOCK_MIN,
AVG(QTE_EN_STOCK) AS QTE_STOCK_MOY,
MAX(QTE_EN_STOCK) AS QTE_STOCK_MAX,
MIN(ROTATION_SEM) AS ROTATION_MIN,
AVG(ROTATION_SEM) AS ROTATION_MOY,
MAX(ROTATION_SEM) AS ROTATION_MAX,
MIN(DENSITE_SEM) AS DENSITE_MIN,
AVG(DENSITE_SEM) AS DENSITE_MOY,
MAX(DENSITE_SEM) AS DENSITE_MAX,
FROM DATA_MESH_PROD_RETAIL.WORK.stat_stock_vente 
GROUP BY 1,2)
ORDER BY 1,2; 


 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_tabstock_vente ORDER BY 1,2; 
 
-- probleme mag sans surface de vente calculé 
/*
SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_MAGASIN IN (515,
516,525,527,529,557,734,736,740,742,745,761,762,763,764,765,766,767,768,770,
771,772,773,774,779,781,782,783,784,787,788,789,790,791,792,793,794,795,796,797,
798,811,812,813,814,816,817,818,820,821,822,823,824,825,826,827,828,831,832,833,
834,837,838,839,842,843,844,845,846,847,848,850,851,852,853,854,855,856,857,858,859,
860,861,862,863,864,865,866,867,869,870,1163,1400,1401,1402,1403)
*/
-- integration des données de Remi onglet du mois de Mai 2024 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.List_Magclust AS 
WITH tab0 AS (SELECT   DISTINCT Mag, Lib_Magasin, CSP, Prochaine_Region, Ancienne_Region,	
Enseigne_info,	Cluster_GCO, Cluster_GCO_Formel, Cluster_GCO_Chaussures, Situation,	
Type_Empl, Empl_detail, Mode_Exploit, Date_OUV, Date_derniere_reno,	
Concept, Cat_magasin, 
CAST( REPLACE (surface_vente,',','.') AS FLOAT ) AS surface_mag, 
CAST( REPLACE (Amplitude_Hebdo,',','.') AS FLOAT ) AS Ampl_Hebdo,
CAST( REPLACE (Obj_CA_2024,',','.') AS FLOAT ) AS Obj_CA_2024_mag, 
CAST( REPLACE (CA_Forecast,',','.') AS FLOAT ) AS CA_Forecast_mag 
FROM  DATA_MESH_PROD_RETAIL.WORK.LISTMAG_CLUSTERV0MAG), 
tab1 AS (
SELECT a.*, b.num_mag, b.nb_sem, b.Surface_moy, b.QTE_VENDUE_GBL, b.CA_GBL, b.QTE_VENDUE_MOY, b.QTE_STOCK_MOY, b.ROTATION_MOY, b.DENSITE_MOY 
FROM tab0 a 
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.stat_tabstock_vente b ON a.Mag=b.num_mag)
SELECT *, 
LN(QTE_VENDUE_GBL) AS Ln_QTE_VENDUE_GBL , LN(CA_GBL) AS LN_CA_GBL, 
LN(CA_Forecast_mag) AS LN_CA_Forecast_mag,
LN(QTE_VENDUE_MOY) AS LN_QTE_VENDUE_MOY, 
LN(QTE_STOCK_MOY) AS LN_QTE_STOCK_MOY, LN(ROTATION_MOY) AS LN_ROTATION_MOY, LN(DENSITE_MOY) AS LN_DENSITE_MOY
FROM tab1;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.List_Magclust_V2 AS 
SELECT DISTINCT a.*
FROM DATA_MESH_PROD_RETAIL.WORK.List_Magclust a
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.mag=b.ID_MAGASIN  AND type_emplacement IN ('PAC','CC', 'CV','CCV') 
AND lib_statut='Ouvert' AND date_fermeture_public IS NULL AND (code_pays = $PAYS1 or code_pays = $PAYS2)
WHERE type_empl !='plage'
ORDER BY 1; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.List_Magclust_V2 ORDER BY 1; 

--  Le clustering a été fait sous python 

SELECT   * FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024 LIMIT 10;

 --- Calcul des statistiques par Cluster pour la creation de 5 classes 
 
SELECT *, 
CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust5 AS 
SELECT CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag, 
AVG(Surface_MOY_Y) AS Surface_Moy, 
AVG(rotation_moy) AS Rotation_Sem, 
AVG(Densite_moy) AS Densite_Sem, 
AVG(CA_FORECAST_MAG) AS Ca_forecast_Moy
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1
ORDER BY 1;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust5; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V2_Clust5 AS
SELECT * FROM (
(SELECT '01-Enseigne' AS grp, Enseigne_info AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '02-Cluster_GCO' AS grp, Cluster_GCO AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '03-Situation' AS grp, Situation AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '04-TYPE EMPL' AS grp, TYPE_EMPL AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '05-Mode EXploitation' AS grp, Mode_Exploit AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '06-Concept' AS grp, CASE WHEN Concept='In Progress' THEN '01-In Progress' ELSE '02-Autres' END AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '07-Cat Magasin' AS grp, Cat_Magasin AS typo,  CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.FICHIER_CLUSTERMAG2024
GROUP BY 1,2,3)); 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V3_Clust5 AS
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V2_Clust5
pivot (SUM(nb_mag) for Class_cluster in ( 'CLASS_0', 'CLASS_1', 'CLASS_2', 'CLASS_3', 'CLASS_4')) ORDER BY 1,2 ; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V3_Clust5; 




-- Realisation des memes informations avec 3 Clusters beaucoup plus equitables 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.F_CLUSTERMAG2024_3CLUS ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust3 AS 
SELECT CONCAT('CLASS_', LABELS_KMEANS) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag, 
AVG(Surface_MOY_Y) AS Surface_Moy, 
AVG(rotation_moy) AS Rotation_Sem, 
AVG(Densite_moy) AS Densite_Sem, 
AVG(CA_FORECAST_MAG) AS Ca_forecast_Moy
FROM DATA_MESH_PROD_RETAIL.WORK.F_CLUSTERMAG2024_3CLUS
GROUP BY 1
ORDER BY 1 ; 

-- Clustering V0 

DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN.LIBELLE_CATEGORIE_PERFORMANCE


-- Information à la date du 20240624


CREATE OR REPLACE TABLE DATA_MESH_PROD_RETAIL.WORK.F_CLUSTER_JULES_V2024_CLUST5_FORECAST_V2 AS 
SELECT * FROM DATA_MESH_PROD_SUPPLY.WORK.F_CLUSTER_JULES_V2024_CLUST5_FORECAST_V2



CREATE OR REPLACE TABLE DATA_MESH_PROD_RETAIL.WORK.F_Cluster_Jules_V2024_Clust5_CA_GBL_V2 AS 
SELECT * FROM DATA_MESH_PROD_SUPPLY.WORK.F_Cluster_Jules_V2024_Clust5_CA_GBL_V2

 
 
 /*** Regroupement des informations ***/ 
 CREATE OR REPLACE TABLE DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024 AS 
 SELECT 
a.MAG,	
a.LIB_MAGASIN,	
a.CSP,	
a.PROCHAINE_REGION,
 a.ANCIENNE_REGION,	
 a.ENSEIGNE_INFO,	
 a.CLUSTER_GCO,	
 a.CLUSTER_GCO_FORMEL,	
 a.CLUSTER_GCO_CHAUSSURES,
 a.SITUATION,	
 a.TYPE_EMPL,	
 a.EMPL_DETAIL,	
 a.MODE_EXPLOIT	,
 a.DATE_OUV	,
 a.DATE_DERNIERE_RENO,	
 a.CONCEPT	,
 a.CAT_MAGASIN	,
 a.SURFACE_MAG	,
 a.AMPL_HEBDO,	
 a.OBJ_CA_2024_MAG	,
 a.CA_FORECAST_MAG,
 a.SURFACE_MOY_x AS  SURFACE_MOY,
 a.QTE_VENDUE_GBL,
 a.CA_GBL,
 a.QTE_VENDUE_MOY,
 a.QTE_STOCK_MOY,
 a.ROTATION_MOY,
 a.DENSITE_MOY,
a.labels_kmeans  AS libkmeans_CAforecast,
b.labels_kmeans  AS libkmeans_CA12mth
FROM DATA_MESH_PROD_RETAIL.WORK.F_CLUSTER_JULES_V2024_CLUST5_FORECAST_V2 a 
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.F_Cluster_Jules_V2024_Clust5_CA_GBL_V2 b ON a.MAG=b.MAG ; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024; 
 
 
 
--- Statistique avec le CA forecast 

--- Calcul des statistiques par Cluster pour la creation de 5 classes 
 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust5_forecast AS 
SELECT CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag, 
AVG(SURFACE_MOY) AS Surface, 
AVG(rotation_moy) AS Rotation_Sem, 
AVG(Densite_moy) AS Densite_Sem, 
AVG(CA_FORECAST_MAG) AS Ca_forecast_Moy,
AVG(OBJ_CA_2024_MAG) AS Ca_2024_MAG_Moy
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1
ORDER BY 1;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust5_forecast; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V2_Clust5_forecast AS
SELECT * FROM (
(SELECT '01-Enseigne' AS grp, Enseigne_info AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '02-Cluster_GCO' AS grp, Cluster_GCO AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '03-Situation' AS grp, Situation AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '04-TYPE EMPL' AS grp, TYPE_EMPL AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '05-Mode EXploitation' AS grp, Mode_Exploit AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '06-Concept' AS grp, CASE WHEN Concept='In Progress' THEN '01-In Progress' ELSE '02-Autres' END AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '07-Cat Magasin' AS grp, Cat_Magasin AS typo,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)); 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V3_Clust5_forecast AS
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V2_Clust5_forecast
pivot (SUM(nb_mag) for Class_cluster in ( 'CLASS_0', 'CLASS_1', 'CLASS_2', 'CLASS_3', 'CLASS_4')) ORDER BY 1,2 ; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V3_Clust5_forecast ORDER BY 1,2 ;   


 -- Statistiques avec LE Clustering pour CA Normal 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust5_ca12mth AS 
SELECT CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag, 
AVG(SURFACE_MOY) AS Surface, 
AVG(rotation_moy) AS Rotation_Sem, 
AVG(Densite_moy) AS Densite_Sem, 
AVG(CA_FORECAST_MAG) AS Ca_forecast_Moy,
AVG(OBJ_CA_2024_MAG) AS Ca_2024_MAG_Moy
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1
ORDER BY 1;


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V1_Clust5_ca12mth; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V2_Clust5_ca12mth AS
SELECT * FROM (
(SELECT '01-Enseigne' AS grp, Enseigne_info AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '02-Cluster_GCO' AS grp, Cluster_GCO AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '03-Situation' AS grp, Situation AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '04-TYPE EMPL' AS grp, TYPE_EMPL AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '05-Mode EXploitation' AS grp, Mode_Exploit AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '06-Concept' AS grp, CASE WHEN Concept='In Progress' THEN '01-In Progress' ELSE '02-Autres' END AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)
UNION 
(SELECT '07-Cat Magasin' AS grp, Cat_Magasin AS typo,  CONCAT('CLASS_', libkmeans_CA12mth) AS Class_cluster, 
count(DISTINCT Mag) AS nb_mag 
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024
GROUP BY 1,2,3)); 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_V3_Clust5_ca12mth AS
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V2_Clust5_ca12mth
pivot (SUM(nb_mag) for Class_cluster in ( 'CLASS_0', 'CLASS_1', 'CLASS_2', 'CLASS_3', 'CLASS_4')) ORDER BY 1,2 ; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_V3_Clust5_ca12mth ORDER BY 1,2 ; 


/***** Migration des clusters Mag ****/ 


SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, LIBELLE_CATEGORIE_PERFORMANCE FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
    WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) 
    AND type_emplacement IN ('PAC','CC', 'CV','CCV')
    AND (code_pays = $PAYS1 or code_pays = $PAYS2)

 CREATE OR REPLACE TABLE DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024_V2 AS 
 SELECT a.*,  CONCAT('CLASS_', libkmeans_CAforecast) AS Class_cluster, 
b.LIBELLE_CATEGORIE_PERFORMANCE
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024 a 
LEFT JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.MAG=b.ID_MAGASIN ;  

SELECT * FROM 
(SELECT LIBELLE_CATEGORIE_PERFORMANCE, Class_cluster, 
Count(DISTINCT Mag) AS nb_mag
FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024_V2
GROUP BY 1,2)
pivot (SUM(nb_mag) for Class_cluster in ( 'CLASS_0', 'CLASS_1', 'CLASS_2', 'CLASS_3', 'CLASS_4')) ORDER BY 1,2 ; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Fichier_NewCluster_MAG2024_V2 ORDER BY 1,2 ;  



