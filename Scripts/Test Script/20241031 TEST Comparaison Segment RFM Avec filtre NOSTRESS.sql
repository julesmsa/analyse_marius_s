-- Comparaison Segment RFM



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.TEST_SEGMENT_RFM AS
WITH Tab0 AS (SELECT DISTINCT CODE_CLIENT AS ID_CLIENT, ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_NOSTRESS, LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_NOSTRESS,
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM_NOSTRESS 
FROM DATA_MESH_PROD_CLIENT.WORK.SEGMENTATION_RFM_OMNI_PARTITIONNEE_NOSTRESS
WHERE  ID_ORG_ENSEIGNE IN (1,3) AND CODE_PAYS IN ('FRA', 'BEL')),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT,
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
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE('2024-10-01')
AND (DATE_FIN > DATE('2024-10-01') OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0')
SELECT a.*, b.* , 
COALESCE(a.CODE_CLIENT,b.ID_CLIENT) AS IDCLT
FROM segrfm a
FULL JOIN Tab0 b ON a.CODE_CLIENT=b.ID_CLIENT; 


SELECT SEGMENT_RFM, SEGMENT_RFM_NOSTRESS, 
COUNT(DISTINCT IDCLT) AS NBCLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.TEST_SEGMENT_RFM
GROUP BY 1,2 ; 




