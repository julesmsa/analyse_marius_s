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




SELECT CODE_CLIENT, 
        GENRE, 
        DATE_RECRUTEMENT, DATE_PREMIER_ACHAT, DATE_DERNIER_ACHAT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI       
        FROM DHB_PROD.DNR.DN_CLIENT WHERE CODE_Client='006210037155';
       
SELECT CODE_CLIENT, 
        GENRE, 
        DATE_RECRUTEMENT, DATE_PREMIER_ACHAT, DATE_DERNIER_ACHAT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI       
        FROM DHB_PROD.DNR.DN_CLIENT 
       WHERE LIB_MACRO_SEGMENT LIKE '%INACTIFS%' AND YEAR(DATE_DERNIER_ACHAT) = 2024; 
      
SELECT LIB_MACRO_SEGMENT, COUNT( DISTINCT CODE_CLIENT) AS nb_Client      
        FROM DHB_PROD.DNR.DN_CLIENT 
       WHERE  YEAR(DATE_DERNIER_ACHAT) = 2024
      GROUP BY 1 ;
     
SELECT *
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE CODE_Client='006210037155' ORDER BY DATE_DEBUT 



DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0' AND CODE_Client='006210037155'

---
SELECT *
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE CODE_Client='006210037155' ORDER BY DATE_DEBUT

SELECT *
FROM DATA_MESH_PROD_CLIENT.WORK.SEGMENTATION_RFM_OMNI_PARTITIONNEE_NOSTRESS
WHERE CODE_Client='006210037155'

SELECT *
FROM DATA_MESH_PROD_CLIENT.WORK.RFM_SEGMENTATION
WHERE CODE_Client='006210037155' ORDER BY 1 DESC

SELECT DISTINCT CODE_Client, DATE_TICKET 
FROM DHB_PROD.DNR.DN_VENTE vd
WHERE CODE_Client='006210037155' ORDER BY DATE_TICKET ;
---

-- Test avec quelques magasins 

WITH tab0 AS (SELECT DISTINCT CODE_CLIENT,
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
TYPE_EMPLACEMENT,
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
FROM DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN DATE('2020-05-20') AND DATE('2021-11-20') 
AND vd.ID_MAGASIN = 82
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0' )
  SELECT 
  COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN Qte_pos>0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN Qte_pos>0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN Qte_pos>0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN Qte_pos>0 THEN montant_remise end) AS Mnt_remise_glb
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
  FROM tab0
  
  
WITH tab0 AS (SELECT DISTINCT CODE_CLIENT,
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
TYPE_EMPLACEMENT,
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
from DATA_MESH_PROD_RETAIL.WORK.DN_VENTE vd
where vd.date_ticket BETWEEN DATE('2020-05-20') AND DATE('2021-11-20') 
AND vd.ID_MAGASIN = 82
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0' )
  SELECT 
  COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0  THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
  FROM tab0
  
  
  
  -- Info club 
  
SELECT * FROM DHB_PROD.HUB.D_CLI_HISTO_CLIENT 
WHERE 

SELECT * FROM  DHB_PROD.HUB.d_cli_histo_indicateur;


DATE_DEBUT <= DATE($dtfin_jclub)
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'
  
SELECT DISTINCT CODE_CLIENT, nombre_points_fidelite AS nb_pts_fidelite_a_date
FROM DHB_PROD.HUB.D_CLI_HISTO_CLIENT 
WHERE DATE_DEBUT <= DATE($dtfin_jclub)
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'



select distinct code_client, id_indic, valeur from dhb_prod.hub.d_cli_histo_indicateur where id_indic in (191, 198)
and id_org_enseigne in (1, 3)
and code_client = '035790002291' limit 1000;


WITH tab_a AS (SELECT DISTINCT CODE_CLIENT, id_indic AS id_indic_a, libelle_indicateur AS libelle_indicateur_a, valeur AS valeur_a , DATE_DEBUT, DATE_FIN
FROM dhb_prod.hub.d_cli_histo_indicateur 
WHERE DATE_DEBUT <= DATE($dtfin_jclub)
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'
AND id_indic IN (191) ),
tab_b AS (SELECT DISTINCT CODE_CLIENT, id_indic AS id_indic_b, libelle_indicateur AS libelle_indicateur_b, valeur AS valeur_b
FROM dhb_prod.hub.d_cli_histo_indicateur 
WHERE DATE_DEBUT <= DATE($dtfin_jclub)
AND (DATE_FIN > DATE($dtfin_jclub) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'
AND id_indic IN (198) )
SELECT a.CODE_CLIENT, id_indic_a, libelle_indicateur_a, valeur_a , id_indic_b, libelle_indicateur_b, valeur_b, DATE_DEBUT, DATE_FIN
FROM tab_a a 
INNER JOIN tab_b b ON a.CODE_CLIENT=b.CODE_CLIENT ; 



