--- Analyse des indicateurs de performances des clients Coupon FID 

-- Analyse Opération Marketing Nouveau programme de FID

SET dtdeb='2024-04-24';
SET dtfin='2024-10-31';
SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb)); 
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb, $dtfin, $dtdeb_Nm1, $dtfin_Nm1,  $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

-- SELECT * FROM DHB_PROD.DNR.DN_COUPON ; 

/* On prend les coupons créer poiur les clients FR/BE ***/ 

-- SELECT * FROM DHB_PROD.DNR.DN_ENTITE ;
-- CODE_CLient = '360000352833' and CODE_COUPON='108250003588979' and CODE_AM='108250' and CODE_MAGASIN='851'


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_mag AS 
SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com,
type_emplacement, lib_statut, id_concept, lib_enseigne, code_pays, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND CODE_PAYS IN ($PAYS1, $PAYS2) 
AND LIB_ENSEIGNE IN ('JULES', 'BRICE') ; 

--SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_mag ;

--SELECT * FROM DHB_PROD.DNR.DN_COUPON WHERE CODE_AM='101623' and id_ticket IS NOT NULL;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0 AS 
SELECT DISTINCT  a.CODE_CLIENT, a.CODE_COUPON, a.CODE_AM, a.CODE_MAGASIN, a.DATE_DEBUT_VALIDITE, a.DATE_FIN_VALIDITE, a.DATE_FIN_TOLERE, a.VALEUR,
CASE WHEN code_am IN ('108250') AND DATE(DATE_FIN_TOLERE) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN 1 
WHEN code_am IN ('101623','301906','130146','126861','326910','130147','130148') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN 1
ELSE 0 END AS Top_period, 
CASE WHEN code_am IN ('108250') AND ID_TICKET IS NOT NULL AND DATE(DATE_TICKET) < DATE($dtdeb) THEN 0 ELSE 1 END AS Top_cpd
FROM DHB_PROD.DNR.DN_COUPON a
INNER JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT AND b.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND b.code_pays IN  ($PAYS1 ,$PAYS2) 
             AND b.code_client IS NOT NULL AND b.code_client !='0' AND date_suppression_client IS NULL
WHERE Top_period=1 AND Top_cpd=1;

-- identifier le nombre de coupon par client et code AM sur la période 

-- information sur les coupons 

  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N AS
 WITH tab0  AS (SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_achat_AM ,
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket >= DATE($dtdeb)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0') 
SELECT * FROM (
SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,Canal_achat_AM ,
CODE_COUPON1 AS CODE_COUPON,
MTREMISE_COUPON1 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON1 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON1 IS NOT NULL  
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,Canal_achat_AM ,
CODE_COUPON2 AS CODE_COUPON,
MTREMISE_COUPON2 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON2 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON2 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,Canal_achat_AM ,
CODE_COUPON3 AS CODE_COUPON,
MTREMISE_COUPON3 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON3 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON3 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,Canal_achat_AM ,
CODE_COUPON4 AS CODE_COUPON,
MTREMISE_COUPON4 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON4 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON4 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,Canal_achat_AM ,
CODE_COUPON5 AS CODE_COUPON,
MTREMISE_COUPON5 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON5 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON5 IS NOT NULL 
) 
WHERE CODEACTIONMARKETING_COUPON IN ('101623','301906','130146','126861','326910','130147','130148','108250');

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N ;
 
-- jointure avec le code coupon 
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon AS  
SELECT a.* , id_ticket, date_ticket, MTREMISE_COUPON,Canal_achat_AM 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N b ON a.CODE_COUPON=b.CODE_COUPON AND a.CODE_CLIENT=b.CODE_CLIENT AND a.CODE_AM=b.CODEACTIONMARKETING_COUPON;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon ;
--- les coupon renseigné sont insuffisant 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise AS 
WITH tab1 AS (SELECT id_org_enseigne, Id_magasin, CONCAT(id_org_enseigne,'-',id_magasin,'-',code_caisse,'-',code_date_ticket,'-',code_ticket) as id_ticket_lgt, NUMERO_OPERATION 
,SUM(Montant_remise) AS remise_AM
FROM dhb_prod.hub.f_vte_remise_detaillee
WHERE DATE(dateh_ticket)>= DATE($dtdeb) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
GROUP BY 1,2,3,4)
SELECT DISTINCT  vd.CODE_CLIENT, a.id_ticket_lgt, a.NUMERO_OPERATION, a.remise_AM, vd.type_emplacement, date_ticket,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_remise_AM 
FROM tab1 a
LEFT JOIN DHB_PROD.DNR.DN_VENTE vd ON a.id_ticket_lgt=vd.id_ticket and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND CODE_CLIENT IN (SELECT CODE_CLIENT FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0);
 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise_web AS 
 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_remise
 WHERE Canal_remise_AM='WEB';
 
SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_remise_web ;
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl AS  
SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon
WHERE id_ticket IS NULL;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl AS  
SELECT a.*, id_ticket_lgt AS id_ticket, b.date_ticket, b.remise_AM AS MTREMISE_COUPON, b.Canal_remise_AM AS Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_remise_web b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.CODE_AM=b.NUMERO_OPERATION AND b.date_ticket BETWEEN DATE(DATE_DEBUT_VALIDITE) AND DATE(DATE_FIN_TOLERE);


-- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl WHERE CODE_CLIENT = '022220002067';   -- '000110001411' --  ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_g AS  
SELECT DISTINCT  code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd, 
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM
FROM 
( SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon WHERE id_ticket IS NOT NULL
UNION 
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl) ; 

Select * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_g LIMIT 10;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_AM AS  
WITH
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtdeb) AND DATE($dtfin)  THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT 
    WHERE CODE_CLIENT IN (SELECT CODE_CLIENT FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_g)),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) )
SELECT DISTINCT 
a.CODE_CLIENT,
a.CODE_COUPON,
a.CODE_AM,
a.CODE_MAGASIN as CODEMAG_coupon,
a.DATE_DEBUT_VALIDITE,
a.DATE_FIN_VALIDITE,
a.DATE_FIN_TOLERE,
a.VALEUR,
a.ID_TICKET,
a.DATE_TICKET,
a.MTREMISE_COUPON,
a.CANAL_ACHAT_AM,
h.type_emplacement as type_empl_coupon, 
h.lib_entite as libmag_coupon, 
CASE WHEN h.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN h.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_Crea_coupon,
CASE 
	WHEN a.code_am IN ('101623','301906') THEN '01-ANNIVERSAIRE'
	WHEN a.code_am IN ('130146')          THEN '02-CHEQUE_FID'
   WHEN a.code_am IN ('108250')          THEN '04-J_PRIVILEGE '
	WHEN a.code_am IN ('126861','326910','130147','130148') THEN '03-BIENVENUE'
END AS lIB_CODE_AM,
date_naissance, genre,
        date_recrutement, ANCIENNETE_CLIENT, Type_client, AGE_C, CLASSE_AGE,
    CASE 
        WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
        ELSE 'z: Non def' 
    END AS Tr_anciennete, 
g.ID_MACRO_SEGMENT , g.LIB_MACRO_SEGMENT, f.LIB_SEGMENT_OMNI, 
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM ,
  CASE WHEN f.LIB_SEGMENT_OMNI='OMNI' THEN '03-OMNI'
       WHEN f.LIB_SEGMENT_OMNI='MAG' THEN '01-MAG'
       WHEN f.LIB_SEGMENT_OMNI='WEB' THEN '02-WEB'
       ELSE '09-NR/NC' END AS SEGMENT_OMNI 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_g a
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt 
LEFT JOIN segrfm g ON a.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON a.CODE_CLIENT=f.CODE_CLIENT 
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_mag h ON a.CODE_MAGASIN=h.Id_entite ; 

-- Information sur les tickets des clients

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_ticket AS 
WITH 
tab1 AS ( SELECT DISTINCT ID_TICKET FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_AM
WHERE ID_TICKET IS NOT NULL ),
tickets as (
Select DISTINCT  vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM, vd.montant_remise,
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
where vd.id_ticket IN ( SELECT DISTINCT ID_TICKET FROM tab1 )
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0'), 
agre_ticket AS ( SELECT CODE_CLIENT , id_ticket ,date_ticket, PERIMETRE
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_tck
,SUM(CASE WHEN annul_ticket=0  THEN QUANTITE_LIGNE end) AS qte_tck
,SUM(CASE WHEN annul_ticket=0  THEN MONTANT_MARGE_SORTIE end) AS Marge_tck
,SUM(CASE WHEN annul_ticket=0  THEN montant_remise end) AS Mnt_remise_mkt_tck
,SUM(CASE WHEN annul_ticket=0  THEN M_remise end) AS Mnt_remise_glb_tck
FROM tickets
GROUP BY 1,2,3,4)  
SELECT b.*
FROM tab1 a 
INNER JOIN agre_ticket b ON a.ID_TICKET=b.id_ticket ; 

-- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_ticket WHERE id_ticket='1-326-2-20241008-24282042' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket AS 
SELECT a.*, b.id_ticket AS id_ticket_acht, b.date_ticket AS date_ticket_acht, b.PERIMETRE, b.CA_tck, b.qte_tck, b.Marge_tck, b.Mnt_remise_mkt_tck, b.Mnt_remise_glb_tck,
 CASE WHEN b.date_ticket IS NOT NULL THEN  DATEDIFF(DAY, date_debut_validite, b.date_ticket) ELSE NULL END AS Delai_used
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_ticket b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.ID_TICKET=b.id_ticket ; 

--SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket WHERE ID_TICKET IS NOT NULL ;











--- Information base ticket sur la période d'analyse 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb AS 
WITH tag_am AS ( SELECT DISTINCT CODE_CLIENT , 
 MAX(CASE WHEN CODE_AM ='101623' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM101623, 
 MAX(CASE WHEN CODE_AM ='130146' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130146,
 MAX(CASE WHEN CODE_AM ='130147' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130147,
 MAX(CASE WHEN CODE_AM ='130148' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130148,
 MAX(CASE WHEN CODE_AM ='108250' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM108250
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket 
WHERE ID_TICKET IS NOT NULL )
Select DISTINCT  vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM, vd.montant_remise,
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
    ELSE 0 END AS annul_ticket, 
    top_AM101623, top_AM130146, top_AM130147, top_AM130148, top_AM108250
from DHB_PROD.DNR.DN_VENTE vd
LEFT JOIN tag_am tg ON vd.CODE_CLIENT=tg.CODE_CLIENT
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' ; 
 
 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb AS
SELECT * FROM 
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 GROUP BY 1,2 
 UNION 
 SELECT '01-ANNIVERSAIRE' AS typo_clt, '101623' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 WHERE top_AM101623=1
 GROUP BY 1,2 
 UNION 
 SELECT '02-CHEQUE_FID' AS typo_clt, '130146' AS modalite
     ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 WHERE top_AM130146=1 
 GROUP BY 1,2 
  UNION 
 SELECT '03-BIENVENUE' AS typo_clt, '130147' AS modalite
     ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM130147=1
 GROUP BY 1,2 
  UNION 
 SELECT '03-BIENVENUE' AS typo_clt, '130148' AS modalite
     ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM130148=1
 GROUP BY 1,2 
  UNION 
 SELECT '04-J_PRIVILEGE' AS typo_clt, '108250' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM108250=1
 GROUP BY 1,2 )
 ORDER BY 1,2 ; 


Select * FROM DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb  ORDER BY 1,2 ; 







-- Statistique des familles de Produits 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS AS 
WITH tag_am AS ( SELECT DISTINCT ID_TICKET , 
 MAX(CASE WHEN CODE_AM ='101623' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM101623, 
 MAX(CASE WHEN CODE_AM ='130146' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130146,
 MAX(CASE WHEN CODE_AM ='130147' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130147,
 MAX(CASE WHEN CODE_AM ='130148' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130148,
 MAX(CASE WHEN CODE_AM ='108250' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM108250
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket 
WHERE ID_TICKET IS NOT NULL )
Select DISTINCT  vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM, vd.montant_remise,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS M_remise,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
TYPE_EMPLACEMENT,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN QUANTITE_LIGNE END ) OVER (PARTITION BY vd.id_ticket) AS Qte_pos,
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
    ELSE 0 END AS annul_ticket, 
    top_AM101623, top_AM130146, top_AM130147, top_AM130148, top_AM108250
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN tag_am tg ON vd.ID_TICKET=tg.ID_TICKET
where vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' ; 
 
 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.Stat_ticket_XTS AS 
SELECT * FROM
(SELECT '01-ANNIVERSAIRE' AS lIB_CODE_AM, '101623' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM101623=1
GROUP BY 1,2,3,4 
UNION
SELECT '01-ANNIVERSAIRE' AS lIB_CODE_AM, '101623' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM101623=1
GROUP BY 1,2,3,4
UNION
SELECT '02-CHEQUE_FID' AS lIB_CODE_AM, '130146' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130146=1
GROUP BY 1,2,3,4 
UNION
SELECT '02-CHEQUE_FID' AS lIB_CODE_AM, '130146' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130146=1
GROUP BY 1,2,3,4
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130147' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130147=1
GROUP BY 1,2,3,4 
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130147' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130147=1
GROUP BY 1,2,3,4 
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130148' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130148=1
GROUP BY 1,2,3,4 
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130148' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130148=1
GROUP BY 1,2,3,4 
UNION
SELECT '04-J_PRIVILEGE' AS lIB_CODE_AM, '108250' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM108250=1
GROUP BY 1,2,3,4 
UNION
SELECT '04-J_PRIVILEGE' AS lIB_CODE_AM, '108250' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
   ,SUM(CASE WHEN annul_ticket=0 THEN M_remise end) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM108250=1
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4;


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_ticket_XTS ORDER BY 1,2,3,4;
