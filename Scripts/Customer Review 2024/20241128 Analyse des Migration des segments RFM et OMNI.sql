--- Migration des segments RFM et OMNI 

SET dtdeb = DAte('2024-01-01'); -- to_date(dateadd('year', -1, $dtfin)); 
SET dtfin = DAte('2024-11-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 


-- Segment RFM

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_RFM AS
WITH segrfm_deb AS ( SELECT DISTINCT a.CODE_CLIENT, 
a.ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_DEB, 
a.LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_DEB,
CASE WHEN a.ID_MACRO_SEGMENT = '01' THEN '01_VIP' 
     WHEN a.ID_MACRO_SEGMENT = '02' THEN '02_TBC'
     WHEN a.ID_MACRO_SEGMENT = '03' THEN '03_BC'
     WHEN a.ID_MACRO_SEGMENT = '04' THEN '04_MOY'
     WHEN a.ID_MACRO_SEGMENT = '05' THEN '05_TAP'
     WHEN a.ID_MACRO_SEGMENT = '06' THEN '06_TIEDE'
     WHEN a.ID_MACRO_SEGMENT = '07' THEN '07_TPURG'
     WHEN a.ID_MACRO_SEGMENT = '09' THEN '08_NCV'
     WHEN a.ID_MACRO_SEGMENT = '08' THEN '09_NAC'
     WHEN a.ID_MACRO_SEGMENT = '10' THEN '10_INA12'
     WHEN a.ID_MACRO_SEGMENT = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM_DEB 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM a
INNER JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT AND ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND b.code_client IS NOT NULL AND b.code_client !='0' AND date_suppression_client IS NULL
WHERE DATE_DEBUT <= DATE($dtdeb)
AND (DATE_FIN > DATE($dtdeb) OR DATE_FIN IS NULL) AND a.code_client IS NOT NULL AND a.code_client !='0' AND a.ID_MACRO_SEGMENT IS NOT NULL),
segrfm_fin AS (SELECT DISTINCT a.CODE_CLIENT, 
a.ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_FIN, 
a.LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_FIN
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM a
INNER JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT AND ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND b.code_client IS NOT NULL AND b.code_client !='0' AND date_suppression_client IS NULL
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND a.code_client IS NOT NULL AND a.code_client !='0')
SELECT COALESCE(a.CODE_CLIENT, b.CODE_CLIENT) AS CODE_CLIENT, 
a.ID_MACRO_SEGMENT_DEB, a.LIB_MACRO_SEGMENT_DEB, a.SEGMENT_RFM_DEB,
b.ID_MACRO_SEGMENT_FIN, b.LIB_MACRO_SEGMENT_FIN, 
CASE WHEN b.ID_MACRO_SEGMENT_FIN = '01' THEN '01_VIP' 
     WHEN b.ID_MACRO_SEGMENT_FIN = '02' THEN '02_TBC'
     WHEN b.ID_MACRO_SEGMENT_FIN = '03' THEN '03_BC'
     WHEN b.ID_MACRO_SEGMENT_FIN = '04' THEN '04_MOY'
     WHEN b.ID_MACRO_SEGMENT_FIN = '05' THEN '05_TAP'
     WHEN b.ID_MACRO_SEGMENT_FIN = '06' THEN '06_TIEDE'
     WHEN b.ID_MACRO_SEGMENT_FIN = '07' THEN '07_TPURG'
     WHEN b.ID_MACRO_SEGMENT_FIN = '09' THEN '08_NCV'
     WHEN b.ID_MACRO_SEGMENT_FIN = '08' THEN '09_NAC'
     WHEN b.ID_MACRO_SEGMENT_FIN = '10' THEN '10_INA12'
     WHEN b.ID_MACRO_SEGMENT_FIN = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM_FIN
FROM segrfm_deb a
LEFT JOIN segrfm_fin b ON a.CODE_CLIENT=b.CODE_CLIENT;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_RFM ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_tickets AS
WITH tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM,vd.montant_remise,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS M_remise,
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
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb) AND (DATE($dtfin)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0') 
SELECT CODE_CLIENT 
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise
FROM tickets
GROUP BY 1 ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets_RFM AS
SELECT a.* ,b.nb_ticket ,b.CA ,b.Qte ,b.Marge ,b.Mnt_remise
FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_RFM a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_tickets b ON a.CODE_CLIENT=b.CODE_CLIENT ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets_RFM; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM  AS
WITH tabhn AS ( SELECT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, 
Count(DISTINCT CODE_CLIENT) AS nb_clt_glb,
Count(DISTINCT CASE WHEN nb_ticket>0 AND nb_ticket IS NOT NULL THEN CODE_CLIENT END) AS nb_clt_actif,
SUM(nb_ticket) AS nb_ticket_glb,
SUM(CA) AS CA_glb,
SUM(qte) AS qte_glb,
SUM(Marge) AS Marge_glb,
SUM(Mnt_remise) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets_RFM
GROUP BY 1,2)
SELECT a.* 
,CASE WHEN nb_clt_actif IS NOT NULL AND nb_clt_actif>0 THEN Round(CA_glb/nb_clt_actif,4) END AS CA_par_clt_glb
,CASE WHEN nb_clt_actif IS NOT NULL AND nb_clt_actif>0 THEN Round(nb_ticket_glb/nb_clt_actif,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(CA_glb/nb_ticket_glb,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(qte_glb/nb_ticket_glb,4) END AS idv_clt_glb        
,CASE WHEN qte_glb IS NOT NULL AND qte_glb>0 THEN Round(CA_glb/qte_glb,4) END AS pvm_clt_glb      
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Marge_glb/CA_glb,4) END AS txmarge_clt_glb   
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Mnt_remise_glb/(CA_glb + Mnt_remise_glb),4) END AS txremise_clt_glb
FROM tabhn a
ORDER BY 1,2 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Clt_glb_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, nb_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '01-Client Global' AS typo, *  FROM tagbd
pivot (SUM(nb_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Clt_act_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, nb_clt_actif FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '02-Client Actif' AS typo, *  FROM tagbd
pivot (SUM(nb_clt_actif) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.CA_par_clt_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, CA_par_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '03-CA/Clt' AS typo, *  FROM tagbd
pivot (SUM(CA_par_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.freq_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, freq_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '04-Fréquence' AS typo, *  FROM tagbd
pivot (SUM(freq_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.panier_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, panier_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '05-Panier Moy' AS typo, *  FROM tagbd
pivot (SUM(panier_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Vision_glb_RFM  AS
SELECT * FROM 
( SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Clt_glb_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Clt_act_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CA_par_clt_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.freq_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.panier_RFM )
ORDER BY 1,2 ; 


Select * FROM DATA_MESH_PROD_CLIENT.WORK.Vision_glb_RFM ORDER BY 1,2 ; 


SELECT DISTINCT DATE_DEBUT  FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI ; 

SELECT CODE_CLIENT, LIB_SEGMENT_OMNI, LIB_SEGMENT_OMNI_NM1  FROM DHB_PROD.DNR.DN_CLIENT ; 

SELECT  LIB_SEGMENT_OMNI_NM1, LIB_SEGMENT_OMNI, Count(DISTINCT CODE_CLIENT) AS nbclt
 FROM DHB_PROD.DNR.DN_CLIENT
 WHERE ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL
GROUP BY 1,2
ORDER BY 1,2 ;


SELECT  LIB_MACRO_SEGMENT , LIB_MACRO_SEGMENT_NM1 , Count(DISTINCT CODE_CLIENT) AS nbclt
 FROM DHB_PROD.DNR.DN_CLIENT
 WHERE ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL
             -- AND LIB_MACRO_SEGMENT_NM1 IS NOT NULL 
GROUP BY 1,2
ORDER BY 1,2 ; 









-- Vision OMNI

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_omni AS
WITH segomni_deb AS ( SELECT DISTINCT a.CODE_CLIENT, 
a.LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_DEB,
  CASE WHEN a.LIB_SEGMENT_OMNI='OMNI' THEN '03-OMNI'
       WHEN a.LIB_SEGMENT_OMNI='MAG' THEN '01-MAG'
       WHEN a.LIB_SEGMENT_OMNI='WEB' THEN '02-WEB'
       ELSE '09-NR/NC' END AS SEGMENT_OMNI_DEB
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI a
INNER JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT AND ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND b.code_client IS NOT NULL AND b.code_client !='0' AND date_suppression_client IS NULL
WHERE DATE_DEBUT <= DATE($dtdeb)
AND (DATE_FIN > DATE($dtdeb) OR DATE_FIN IS NULL) AND a.code_client IS NOT NULL AND a.code_client !='0' AND a.LIB_SEGMENT_OMNI IS NOT NULL ),
segomni_fin AS (SELECT DISTINCT a.CODE_CLIENT, a.LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_FIN
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI a
INNER JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT AND ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND b.code_client IS NOT NULL AND b.code_client !='0' AND date_suppression_client IS NULL
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND a.code_client IS NOT NULL AND a.code_client !='0')
SELECT a.CODE_CLIENT,  a.LIB_SEGMENT_OMNI_DEB, a.SEGMENT_OMNI_DEB,
b.LIB_SEGMENT_OMNI_FIN, 
  CASE WHEN LIB_SEGMENT_OMNI_FIN='OMNI' THEN '03-OMNI'
       WHEN LIB_SEGMENT_OMNI_FIN='MAG' THEN '01-MAG'
       WHEN LIB_SEGMENT_OMNI_FIN='WEB' THEN '02-WEB'
       ELSE '09-NR/NC' END AS SEGMENT_OMNI_FIN      
FROM segomni_deb a
LEFT JOIN segomni_fin b ON a.CODE_CLIENT=b.CODE_CLIENT;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_omni ; 

SELECT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN,
count(DISTINCT CODE_CLIENT) AS nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_omni 
GROUP BY 1,2
ORDER BY 1,2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_tickets AS
WITH tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS montant_remise,
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
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb) AND (DATE($dtfin)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0') 
SELECT CODE_CLIENT 
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
,SUM(CASE WHEN Qte_pos>0 THEN MONTANT_TTC end) AS CA
,SUM(CASE WHEN Qte_pos>0 THEN QUANTITE_LIGNE end) AS qte
,SUM(CASE WHEN Qte_pos>0 THEN MONTANT_MARGE_SORTIE end) AS Marge
,SUM(CASE WHEN Qte_pos>0 THEN montant_remise end) AS Mnt_remise
FROM tickets
GROUP BY 1 ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets_omni AS
SELECT a.* ,b.nb_ticket ,b.CA ,b.Qte ,b.Marge ,b.Mnt_remise
FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_segment_omni a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_tickets b ON a.CODE_CLIENT=b.CODE_CLIENT ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets_omni; 

---- Vision Segment RFM 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM  AS
WITH tabhn AS ( SELECT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, 
Count(DISTINCT CODE_CLIENT) AS nb_clt_glb,
Count(DISTINCT CASE WHEN nb_ticket>0 AND nb_ticket IS NOT NULL THEN CODE_CLIENT END) AS nb_clt_actif,
SUM(nb_ticket) AS nb_ticket_glb,
SUM(CA) AS CA_glb,
SUM(qte) AS qte_glb,
SUM(Marge) AS Marge_glb,
SUM(Mnt_remise) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets_omni
GROUP BY 1,2)
SELECT a.* 
,CASE WHEN nb_clt_actif IS NOT NULL AND nb_clt_actif>0 THEN Round(CA_glb/nb_clt_actif,4) END AS CA_par_clt_glb
,CASE WHEN nb_clt_actif IS NOT NULL AND nb_clt_actif>0 THEN Round(nb_ticket_glb/nb_clt_actif,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(CA_glb/nb_ticket_glb,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(qte_glb/nb_ticket_glb,4) END AS idv_clt_glb        
,CASE WHEN qte_glb IS NOT NULL AND qte_glb>0 THEN Round(CA_glb/qte_glb,4) END AS pvm_clt_glb      
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Marge_glb/CA_glb,4) END AS txmarge_clt_glb   
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Mnt_remise_glb/(CA_glb + Mnt_remise_glb),4) END AS txremise_clt_glb
FROM tabhn a
ORDER BY 1,2 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Clt_glb_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, nb_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '01-Client Global' AS typo, *  FROM tagbd
pivot (SUM(nb_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Clt_act_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, nb_clt_actif FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '02-Client Actif' AS typo, *  FROM tagbd
pivot (SUM(nb_clt_actif) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.CA_par_clt_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, CA_par_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '03-CA/Clt' AS typo, *  FROM tagbd
pivot (SUM(CA_par_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.freq_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, freq_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '04-Fréquence' AS typo, *  FROM tagbd
pivot (SUM(freq_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.panier_RFM  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_RFM_DEB, SEGMENT_RFM_FIN, panier_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_RFM)
SELECT '05-Panier Moy' AS typo, *  FROM tagbd
pivot (SUM(panier_clt_glb) for SEGMENT_RFM_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Vision_glb_RFM  AS
SELECT * FROM 
( SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Clt_glb_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Clt_act_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CA_par_clt_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.freq_RFM
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.panier_RFM )
ORDER BY 1,2 ; 


Select * FROM DATA_MESH_PROD_CLIENT.WORK.Vision_glb_RFM ORDER BY 1,2 ; 

---- Vision Segment OMNI


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI  AS
WITH tabhn AS ( SELECT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, 
Count(DISTINCT CODE_CLIENT) AS nb_clt_glb,
Count(DISTINCT CASE WHEN nb_ticket>0 AND nb_ticket IS NOT NULL THEN CODE_CLIENT END) AS nb_clt_actif,
SUM(nb_ticket) AS nb_ticket_glb,
SUM(CA) AS CA_glb,
SUM(qte) AS qte_glb,
SUM(Marge) AS Marge_glb,
SUM(Mnt_remise) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_tickets
GROUP BY 1,2)
SELECT a.* 
,CASE WHEN nb_clt_actif IS NOT NULL AND nb_clt_actif>0 THEN Round(CA_glb/nb_clt_actif,4) END AS CA_par_clt_glb
,CASE WHEN nb_clt_actif IS NOT NULL AND nb_clt_actif>0 THEN Round(nb_ticket_glb/nb_clt_actif,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(CA_glb/nb_ticket_glb,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket_glb IS NOT NULL AND nb_ticket_glb>0 THEN Round(qte_glb/nb_ticket_glb,4) END AS idv_clt_glb        
,CASE WHEN qte_glb IS NOT NULL AND qte_glb>0 THEN Round(CA_glb/qte_glb,4) END AS pvm_clt_glb      
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Marge_glb/CA_glb,4) END AS txmarge_clt_glb   
,CASE WHEN CA_glb IS NOT NULL AND CA_glb>0 THEN Round(Mnt_remise_glb/(CA_glb + Mnt_remise_glb),4) END AS txremise_clt_glb
FROM tabhn a
ORDER BY 1,2 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Clt_glb_OMNI  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, nb_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI)
SELECT '01-Client Global' AS typo, *  FROM tagbd
pivot (SUM(nb_clt_glb) for SEGMENT_OMNI_FIN in ('01-MAG','02-WEB','03-OMNI','99_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Clt_act_OMNI  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, nb_clt_actif FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI)
SELECT '02-Client Actif' AS typo, *  FROM tagbd
pivot (SUM(nb_clt_actif) for SEGMENT_OMNI_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.CA_par_clt_OMNI  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, CA_par_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI)
SELECT '03-CA/Clt' AS typo, *  FROM tagbd
pivot (SUM(CA_par_clt_glb) for SEGMENT_OMNI_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.freq_OMNI  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, freq_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI)
SELECT '04-Fréquence' AS typo, *  FROM tagbd
pivot (SUM(freq_clt_glb) for SEGMENT_OMNI_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.panier_OMNI  AS
WITH tagbd AS ( SELECT DISTINCT SEGMENT_OMNI_DEB, SEGMENT_OMNI_FIN, panier_clt_glb FROM DATA_MESH_PROD_CLIENT.WORK.tab_Clt_OMNI)
SELECT '05-Panier Moy' AS typo, *  FROM tagbd
pivot (SUM(panier_clt_glb) for SEGMENT_OMNI_FIN in ('01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG'));


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Vision_glb_OMNI  AS
SELECT * FROM 
( SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Clt_glb_OMNI
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Clt_act_OMNI
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CA_par_clt_OMNI
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.freq_OMNI
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.panier_OMNI )
ORDER BY 1,2 ;

Select * FROM DATA_MESH_PROD_CLIENT.WORK.Vision_glb_OMNI ORDER BY 1,2 ; 


