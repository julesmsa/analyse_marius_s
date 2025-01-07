-- Analyse Opération Marketing Nouveau programme de FID

SET dtdeb='2024-04-24';
SET dtfin='2024-11-30';
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
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin)
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

--- les coupon renseigné sont insuffisant ils manquent les tickets Web 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise AS 
WITH tab1 AS (SELECT id_org_enseigne, Id_magasin, CONCAT(id_org_enseigne,'-',id_magasin,'-',code_caisse,'-',code_date_ticket,'-',code_ticket) as id_ticket_lgt, NUMERO_OPERATION 
,SUM(Montant_remise) AS remise_AM
FROM dhb_prod.hub.f_vte_remise_detaillee
WHERE DATE(dateh_ticket) BETWEEN DATE($dtdeb) AND DATE($dtfin) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
GROUP BY 1,2,3,4)
SELECT DISTINCT  vd.CODE_CLIENT, a.id_ticket_lgt, a.NUMERO_OPERATION, a.remise_AM, vd.type_emplacement, date_ticket,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_remise_AM 
FROM tab1 a
LEFT JOIN DHB_PROD.DNR.DN_VENTE vd ON a.id_ticket_lgt=vd.id_ticket and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND CODE_CLIENT IN (SELECT CODE_CLIENT FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0);
 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl AS  
SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon
WHERE id_ticket IS NULL;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise_web AS 
SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_remise 
WHERE Canal_remise_AM='WEB';

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl AS
SELECT a.*, id_ticket_lgt AS id_ticket, b.date_ticket, b.remise_AM AS MTREMISE_COUPON, b.Canal_remise_AM AS Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_remise_web b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.CODE_AM=b.NUMERO_OPERATION AND b.date_ticket BETWEEN DATE(DATE_DEBUT_VALIDITE) AND DATE(DATE_FIN_TOLERE);

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_v AS
WITH tab0 AS (SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl WHERE id_ticket IS NOT NULL), 
tab1 AS ( SELECT *,
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT,code_am,id_ticket ORDER BY code_am,id_ticket ) AS nb_lign
FROM tab0),
tab2 AS (SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM FROM tab1 WHERE nb_lign=1),
tab3 AS (SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl WHERE id_ticket IS NULL)
SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM 
FROM 
(SELECT * FROM tab2
UNION 
SELECT * FROM tab3
); 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_v ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2 AS  
WITH tab0 AS (SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon
WHERE id_ticket IS NOT NULL),
tab1 AS (SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_v )
SELECT * FROM 
(SELECT * FROM tab0 
UNION 
SELECT * FROM tab1) ; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2 ;

-- enrichissement des données clients 


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
    WHERE CODE_CLIENT IN (SELECT CODE_CLIENT FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2)),
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
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2 a
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
,SUM(MONTANT_TTC) AS CA_tck
,SUM(QUANTITE_LIGNE ) AS qte_tck
,SUM(MONTANT_MARGE_SORTIE) AS Marge_tck
,SUM(montant_remise) AS Mnt_remise_mkt_tck
,SUM(M_remise) AS Mnt_remise_glb_tck
FROM tickets
GROUP BY 1,2,3,4)  
SELECT b.*
FROM tab1 a 
INNER JOIN agre_ticket b ON a.ID_TICKET=b.id_ticket ; 

-- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_ticket WHERE id_ticket='1-326-2-20241008-24282042' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket AS 
SELECT a.*, b.id_ticket AS id_ticket_acht, b.date_ticket AS date_ticket_acht, b.PERIMETRE, b.CA_tck, b.qte_tck, b.Marge_tck, b.Mnt_remise_mkt_tck, b.Mnt_remise_glb_tck,
 CASE WHEN b.date_ticket IS NOT NULL THEN  DATEDIFF(DAY, date_debut_validite, b.date_ticket) ELSE NULL END AS Delai_used
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_AM a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_ticket b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.ID_TICKET=b.id_ticket ; 


--  SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket where code_am IN ('130146') WHERE ID_TICKET IS NOT NULL


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_N AS
SELECT * FROM (
SELECT lIB_CODE_AM, CODE_AM,
'00-Global' AS Typo, '00-Global' AS modalite
,Min(DATE($dtdeb)) AS date_deb_op
,Max(DATE($dtfin)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'01-Typo_client' AS Typo, Type_client AS modalite
,Min(DATE($dtdeb)) AS date_deb_op
,Max(DATE($dtfin)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite
,Min(DATE($dtdeb)) AS date_deb_op
,Max(DATE($dtfin)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'03-Segment OMNI' AS Typo, SEGMENT_OMNI AS modalite
,Min(DATE($dtdeb)) AS date_deb_op
,Max(DATE($dtfin)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'04-SEXE' AS Typo, CASE WHEN genre='F' THEN '02-Femmes' ELSE '01-Hommes' END AS modalite
,Min(DATE($dtdeb)) AS date_deb_op
,Max(DATE($dtfin)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket
GROUP BY 1,2,3,4 )
ORDER BY 1,2,3,4;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_N ; 

-- Calcul des Statistiques global 

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_N AS
SELECT a.* 
,CASE WHEN nb_coupon_emis IS NOT NULL AND nb_coupon_emis>0 THEN Round(nb_coupon_used/nb_coupon_emis,4) END AS Tx_Coupon_Used
,CASE WHEN nb_cp_emis_fin_val IS NOT NULL AND nb_cp_emis_fin_val>0 THEN Round(nb_cp_used_fin_val/nb_cp_emis_fin_val,4) END AS Tx_Cp_Used_fin_val
,CASE WHEN nb_clt_used IS NOT NULL AND nb_clt_used>0 THEN Round(CA_Global/nb_clt_used,4) END AS CA_par_clt_glb
,CASE WHEN nb_clt_used IS NOT NULL AND nb_clt_used>0 THEN Round(nb_ticket_used/nb_clt_used,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket_used IS NOT NULL AND nb_ticket_used>0 THEN Round(CA_Global/nb_ticket_used,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket_used IS NOT NULL AND nb_ticket_used>0 THEN Round(Qte_totale/nb_ticket_used,4) END AS idv_clt_glb        
,CASE WHEN Qte_totale IS NOT NULL AND Qte_totale>0 THEN Round(CA_Global/Qte_totale,4) END AS pvm_clt_glb      
,CASE WHEN CA_Global IS NOT NULL AND CA_Global>0 THEN Round(marge_totale/CA_Global,4) END AS txmarge_totale_glb   
,CASE WHEN CA_Global IS NOT NULL AND CA_Global>0 THEN Round(remise_totale/(CA_Global + remise_totale),4) END AS txremise_glb
,CASE WHEN CA_Global IS NOT NULL AND CA_Global>0 THEN Round(remise_coupon/(CA_Global + remise_coupon),4) END AS txremise_coupon
,CASE WHEN nb_ticket_used IS NOT NULL AND nb_ticket_used>0 THEN Round(remise_coupon/nb_ticket_used,4) END AS rem_moyen_coupon
,DATE($dtfin) AS DATE_CALCUL
FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_N a
ORDER BY 1,2,3,4;



-- Information au N-1 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0_Nm1 AS 
SELECT DISTINCT  a.CODE_CLIENT, a.CODE_COUPON, a.CODE_AM, a.CODE_MAGASIN, a.DATE_DEBUT_VALIDITE, a.DATE_FIN_VALIDITE, a.DATE_FIN_TOLERE, a.VALEUR,
CASE WHEN code_am IN ('108250') AND DATE(DATE_FIN_TOLERE) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN 1 
WHEN code_am IN ('101623','301906','130146','126861','326910','130147','130148') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN 1
ELSE 0 END AS Top_period, 
CASE WHEN code_am IN ('108250') AND ID_TICKET IS NOT NULL AND DATE(DATE_TICKET) < DATE($dtdeb_Nm1) THEN 0 ELSE 1 END AS Top_cpd
FROM DHB_PROD.DNR.DN_COUPON a
INNER JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT AND b.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND b.code_pays IN  ($PAYS1 ,$PAYS2) 
             AND b.code_client IS NOT NULL AND b.code_client !='0' AND date_suppression_client IS NULL
WHERE Top_period=1 AND Top_cpd=1;

-- identifier le nombre de coupon par client et code AM sur la période 

-- information sur les coupons 

  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_Nm1 AS
 WITH tab0  AS (SELECT DISTINCT CODE_CLIENT, id_ticket, date_ticket,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_achat_AM ,
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket between DATE($dtdeb_Nm1) and DATE($dtfin_Nm1)
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

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_Nm1 ;
 
-- jointure avec le code coupon 
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0_Nm1; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Nm1 AS  
SELECT a.* , id_ticket, date_ticket, MTREMISE_COUPON,Canal_achat_AM 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0_Nm1 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_Nm1 b ON a.CODE_COUPON=b.CODE_COUPON AND a.CODE_CLIENT=b.CODE_CLIENT AND a.CODE_AM=b.CODEACTIONMARKETING_COUPON;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Nm1 ;

--- les coupon renseigné sont insuffisant ils manquent les tickets Web 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise_Nm1 AS 
WITH tab1 AS (SELECT id_org_enseigne, Id_magasin, CONCAT(id_org_enseigne,'-',id_magasin,'-',code_caisse,'-',code_date_ticket,'-',code_ticket) as id_ticket_lgt, NUMERO_OPERATION 
,SUM(Montant_remise) AS remise_AM
FROM dhb_prod.hub.f_vte_remise_detaillee
WHERE DATE(dateh_ticket) between DATE($dtdeb_Nm1) and DATE($dtfin_Nm1) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
GROUP BY 1,2,3,4)
SELECT DISTINCT  vd.CODE_CLIENT, a.id_ticket_lgt, a.NUMERO_OPERATION, a.remise_AM, vd.type_emplacement, date_ticket,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_remise_AM 
FROM tab1 a
LEFT JOIN DHB_PROD.DNR.DN_VENTE vd ON a.id_ticket_lgt=vd.id_ticket and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND CODE_CLIENT IN (SELECT CODE_CLIENT FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V0_Nm1);
 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl_Nm1 AS  
SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Nm1
WHERE id_ticket IS NULL;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl_Nm1 ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise_web_Nm1 AS 
SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_remise_Nm1 
WHERE Canal_remise_AM='WEB';

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_Nm1 AS
SELECT a.*, id_ticket_lgt AS id_ticket, b.date_ticket, b.remise_AM AS MTREMISE_COUPON, b.Canal_remise_AM AS Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_mpl_Nm1 a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_remise_web_Nm1 b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.CODE_AM=b.NUMERO_OPERATION AND b.date_ticket BETWEEN DATE(DATE_DEBUT_VALIDITE) AND DATE(DATE_FIN_TOLERE);

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_v_Nm1 AS
WITH tab0 AS (SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_Nm1 WHERE id_ticket IS NOT NULL), 
tab1 AS ( SELECT *,
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT,code_am,id_ticket ORDER BY code_am,id_ticket ) AS nb_lign
FROM tab0),
tab2 AS (SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM FROM tab1 WHERE nb_lign=1),
tab3 AS (SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_Nm1 WHERE id_ticket IS NULL)
SELECT DISTINCT code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM 
FROM 
(SELECT * FROM tab2
UNION 
SELECT * FROM tab3
); 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_v_Nm1 ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2_Nm1 AS  
WITH tab0 AS (SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Nm1
WHERE id_ticket IS NOT NULL),
tab1 AS (SELECT Distinct code_client, code_coupon, code_am, code_magasin, date_debut_validite, date_fin_validite, date_fin_tolere, valeur, top_period, top_cpd,
id_ticket, date_ticket, MTREMISE_COUPON, Canal_achat_AM 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_Gbl_v_Nm1 )
SELECT * FROM 
(SELECT * FROM tab0 
UNION 
SELECT * FROM tab1) ; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2_Nm1 ;

-- enrichissement des données clients 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_AM_Nm1 AS  
WITH
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin_Nm1) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1)  THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin_Nm1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT 
    WHERE CODE_CLIENT IN (SELECT CODE_CLIENT FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2_Nm1)),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) )
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
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_V2_Nm1 a
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt 
LEFT JOIN segrfm g ON a.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON a.CODE_CLIENT=f.CODE_CLIENT 
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_mag h ON a.CODE_MAGASIN=h.Id_entite ; 

-- Information sur les tickets des clients

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_ticket_Nm1 AS 
WITH 
tab1 AS ( SELECT DISTINCT ID_TICKET FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_AM_Nm1
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
,SUM(MONTANT_TTC) AS CA_tck
,SUM(QUANTITE_LIGNE ) AS qte_tck
,SUM(MONTANT_MARGE_SORTIE) AS Marge_tck
,SUM(montant_remise) AS Mnt_remise_mkt_tck
,SUM(M_remise) AS Mnt_remise_glb_tck
FROM tickets
GROUP BY 1,2,3,4)  
SELECT b.*
FROM tab1 a 
INNER JOIN agre_ticket b ON a.ID_TICKET=b.id_ticket ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1 AS 
SELECT a.*, b.id_ticket AS id_ticket_acht, b.date_ticket AS date_ticket_acht, b.PERIMETRE, b.CA_tck, b.qte_tck, b.Marge_tck, b.Mnt_remise_mkt_tck, b.Mnt_remise_glb_tck,
 CASE WHEN b.date_ticket IS NOT NULL THEN  DATEDIFF(DAY, date_debut_validite, b.date_ticket) ELSE NULL END AS Delai_used
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_AM_Nm1 a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_ticket_Nm1 b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.ID_TICKET=b.id_ticket ; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1 ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_Nm1 AS
SELECT * FROM (
SELECT lIB_CODE_AM, CODE_AM,
'00-Global' AS Typo, '00-Global' AS modalite
,Min(DATE($dtdeb_Nm1)) AS date_deb_op
,Max(DATE($dtfin_Nm1)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin_Nm1) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'01-Typo_client' AS Typo, Type_client AS modalite
,Min(DATE($dtdeb_Nm1)) AS date_deb_op
,Max(DATE($dtfin_Nm1)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin_Nm1) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite
,Min(DATE($dtdeb_Nm1)) AS date_deb_op
,Max(DATE($dtfin_Nm1)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin_Nm1) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'03-Segment OMNI' AS Typo, SEGMENT_OMNI AS modalite
,Min(DATE($dtdeb_Nm1)) AS date_deb_op
,Max(DATE($dtfin_Nm1)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin_Nm1) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'04-SEXE' AS Typo, CASE WHEN genre='F' THEN '02-Femmes' ELSE '01-Hommes' END AS modalite
,Min(DATE($dtdeb_Nm1)) AS date_deb_op
,Max(DATE($dtfin_Nm1)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) <= DATE($dtfin_Nm1) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN code_coupon END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) THEN code_coupon END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon END) AS nb_coupon_used_Web
,ROUND (AVG (delai_used),1) AS Delai_used_moy
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN CODE_CLIENT end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN CODE_CLIENT end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN CODE_CLIENT end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN id_ticket IS NOT NULL THEN MTREMISE_COUPON End) AS remise_coupon
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Mnt_remise_glb_tck End) AS remise_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN CA_tck End) AS CA_Global
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Qte_tck End) AS Qte_totale
,SUM(CASE WHEN id_ticket IS NOT NULL THEN Marge_tck End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN id_ticket IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN id_ticket end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1
GROUP BY 1,2,3,4 )
ORDER BY 1,2,3,4;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_Nm1 ; 

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_Nm1 AS
SELECT a.* 
,CASE WHEN nb_coupon_emis IS NOT NULL AND nb_coupon_emis>0 THEN Round(nb_coupon_used/nb_coupon_emis,4) END AS Tx_Coupon_Used
,CASE WHEN nb_cp_emis_fin_val IS NOT NULL AND nb_cp_emis_fin_val>0 THEN Round(nb_cp_used_fin_val/nb_cp_emis_fin_val,4) END AS Tx_Cp_Used_fin_val
,CASE WHEN nb_clt_used IS NOT NULL AND nb_clt_used>0 THEN Round(CA_Global/nb_clt_used,4) END AS CA_par_clt_glb
,CASE WHEN nb_clt_used IS NOT NULL AND nb_clt_used>0 THEN Round(nb_ticket_used/nb_clt_used,4) END AS freq_clt_glb   
,CASE WHEN nb_ticket_used IS NOT NULL AND nb_ticket_used>0 THEN Round(CA_Global/nb_ticket_used,4) END AS panier_clt_glb    
,CASE WHEN nb_ticket_used IS NOT NULL AND nb_ticket_used>0 THEN Round(Qte_totale/nb_ticket_used,4) END AS idv_clt_glb        
,CASE WHEN Qte_totale IS NOT NULL AND Qte_totale>0 THEN Round(CA_Global/Qte_totale,4) END AS pvm_clt_glb      
,CASE WHEN CA_Global IS NOT NULL AND CA_Global>0 THEN Round(marge_totale/CA_Global,4) END AS txmarge_totale_glb   
,CASE WHEN CA_Global IS NOT NULL AND CA_Global>0 THEN Round(remise_totale/(CA_Global + remise_totale),4) END AS txremise_glb
,CASE WHEN CA_Global IS NOT NULL AND CA_Global>0 THEN Round(remise_coupon/(CA_Global + remise_coupon),4) END AS txremise_coupon
,CASE WHEN nb_ticket_used IS NOT NULL AND nb_ticket_used>0 THEN Round(remise_coupon/nb_ticket_used,4) END AS rem_moyen_coupon
,DATE($dtfin_Nm1) AS DATE_CALCUL
FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_Nm1 a
ORDER BY 1,2,3,4 ;

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.schema_base AS
WITH Taba AS (SELECT DISTINCT LIB_CODE_AM,CODE_AM
FROM 
(SELECT DISTINCT LIB_CODE_AM,CODE_AM FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_N
UNION 
SELECT DISTINCT LIB_CODE_AM,CODE_AM FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_Nm1
) ),
Tabb AS (SELECT DISTINCT TYPO, MODALITE
FROM 
(SELECT DISTINCT TYPO, MODALITE FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_N
UNION 
SELECT DISTINCT TYPO, MODALITE FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_Nm1
) )
SELECT Distinct a.*, b.*
FROM Taba a,Tabb b
ORDER BY 1,2,3,4 ;

-- Regroupement des information, 

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_FID_COUPON AS
SELECT sb.*, 
a.DATE_DEB_OP,
a.DATE_FIN_OP,
a.NB_COUPON_EMIS,
a.NB_CP_EMIS_FIN_VAL,
a.NB_COUPON_MAG,
a.NB_COUPON_WEB,
a.NB_COUPON_USED,
a.NB_CP_USED_FIN_VAL,
a.NB_COUPON_USED_MAG,
a.NB_COUPON_USED_WEB,
a.DELAI_USED_MOY,
a.NB_CLT_COUPON,
a.AGE_MOY,
a.ANCIENNETE_MOY,
a.NB_CLT_USED,
a.NB_CLT_USED_MAG,
a.NB_CLT_USED_WEB,
a.NB_TICKET_USED,
a.NB_TICKET_USED_MAG,
a.NB_TICKET_USED_WEB,
a.REMISE_COUPON,
a.REMISE_TOTALE,
a.CA_GLOBAL,
a.QTE_TOTALE,
a.MARGE_TOTALE,
a.NB_NEWCLT_COUPON,
a.NB_NEWCLT_USED,
a.NB_NEWCLT_USED_MAG,
a.NB_NEWCLT_USED_WEB,
a.TX_COUPON_USED,
a.TX_CP_USED_FIN_VAL,
a.CA_PAR_CLT_GLB,
a.FREQ_CLT_GLB,
a.PANIER_CLT_GLB,
a.IDV_CLT_GLB,
a.PVM_CLT_GLB,
a.TXMARGE_TOTALE_GLB,
a.TXREMISE_GLB,
a.TXREMISE_COUPON,
a.REM_MOYEN_COUPON,
a.DATE_CALCUL,
b.DATE_DEB_OP As DATE_DEB_OP_Nm1,
b.DATE_FIN_OP As DATE_FIN_OP_Nm1,
b.NB_COUPON_EMIS As NB_COUPON_EMIS_Nm1,
b.NB_CP_EMIS_FIN_VAL As NB_CP_EMIS_FIN_VAL_Nm1,
b.NB_COUPON_MAG As NB_COUPON_MAG_Nm1,
b.NB_COUPON_WEB As NB_COUPON_WEB_Nm1,
b.NB_COUPON_USED As NB_COUPON_USED_Nm1,
b.NB_CP_USED_FIN_VAL As NB_CP_USED_FIN_VAL_Nm1,
b.NB_COUPON_USED_MAG As NB_COUPON_USED_MAG_Nm1,
b.NB_COUPON_USED_WEB As NB_COUPON_USED_WEB_Nm1,
b.DELAI_USED_MOY As DELAI_USED_MOY_Nm1,
b.NB_CLT_COUPON As NB_CLT_COUPON_Nm1,
b.AGE_MOY As AGE_MOY_Nm1,
b.ANCIENNETE_MOY As ANCIENNETE_MOY_Nm1,
b.NB_CLT_USED As NB_CLT_USED_Nm1,
b.NB_CLT_USED_MAG As NB_CLT_USED_MAG_Nm1,
b.NB_CLT_USED_WEB As NB_CLT_USED_WEB_Nm1,
b.NB_TICKET_USED As NB_TICKET_USED_Nm1,
b.NB_TICKET_USED_MAG As NB_TICKET_USED_MAG_Nm1,
b.NB_TICKET_USED_WEB As NB_TICKET_USED_WEB_Nm1,
b.REMISE_COUPON As REMISE_COUPON_Nm1,
b.REMISE_TOTALE As REMISE_TOTALE_Nm1,
b.CA_GLOBAL As CA_GLOBAL_Nm1,
b.QTE_TOTALE As QTE_TOTALE_Nm1,
b.MARGE_TOTALE As MARGE_TOTALE_Nm1,
b.NB_NEWCLT_COUPON As NB_NEWCLT_COUPON_Nm1,
b.NB_NEWCLT_USED As NB_NEWCLT_USED_Nm1,
b.NB_NEWCLT_USED_MAG As NB_NEWCLT_USED_MAG_Nm1,
b.NB_NEWCLT_USED_WEB As NB_NEWCLT_USED_WEB_Nm1,
b.TX_COUPON_USED As TX_COUPON_USED_Nm1,
b.TX_CP_USED_FIN_VAL As TX_CP_USED_FIN_VAL_Nm1,
b.CA_PAR_CLT_GLB As CA_PAR_CLT_GLB_Nm1,
b.FREQ_CLT_GLB As FREQ_CLT_GLB_Nm1,
b.PANIER_CLT_GLB As PANIER_CLT_GLB_Nm1,
b.IDV_CLT_GLB As IDV_CLT_GLB_Nm1,
b.PVM_CLT_GLB As PVM_CLT_GLB_Nm1,
b.TXMARGE_TOTALE_GLB As TXMARGE_TOTALE_GLB_Nm1,
b.TXREMISE_GLB As TXREMISE_GLB_Nm1,
b.TXREMISE_COUPON As TXREMISE_COUPON_Nm1,
b.REM_MOYEN_COUPON As REM_MOYEN_COUPON_Nm1,
b.DATE_CALCUL As DATE_CALCUL_Nm1
FROM DATA_MESH_PROD_CLIENT.WORK.schema_base sb
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_N a ON sb.LIB_CODE_AM=a.LIB_CODE_AM AND sb.CODE_AM=a.CODE_AM AND sb.TYPO=a.TYPO AND sb.MODALITE=a.MODALITE
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.stat_globale_C_Nm1 b ON sb.LIB_CODE_AM=b.LIB_CODE_AM AND sb.CODE_AM=b.CODE_AM AND sb.TYPO=b.TYPO AND sb.MODALITE=b.MODALITE
ORDER BY 1,2,3,4;

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_MAI24 as
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FID_COUPON 
ORDER BY 1,2,3,4; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_MAI24 ORDER BY 1,2,3,4;


/*** Enregistrement des tables ***/

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_OCTOBRE24 ORDER BY 1,2,3,4;
-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_SEPTEMBRE24 ORDER BY 1,2,3,4;
-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_JUIN24 ORDER BY 1,2,3,4;
-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_JUILLET24 ORDER BY 1,2,3,4;
-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_AOUT24 ORDER BY 1,2,3,4;
-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_MAI24 ORDER BY 1,2,3,4;
-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_NOVEMBRE24 ORDER BY 1,2,3,4;

/*
CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_COUPON_FID_JULES2024 AS
SELECT *, DATE(DATE_CALCUL + 1) as DATE_OBSERVATION  FROM 
( 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_MAI24
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_JUIN24
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_JUILLET24
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_AOUT24
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_SEPTEMBRE24
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_OCTOBRE24
UNION
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_NOVEMBRE24
)
ORDER BY DATE_CALCUL,1,2,3,4; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_COUPON_FID_JULES2024 ORDER BY DATE_CALCUL,1,2,3,4; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_FIN_OCTOBRE24 ORDER BY DATE_CALCUL,1,2,3,4; 
*/

-- Zoom sur les Clients et global magasin suite analyse 

--- Information base ticket sur la période d'analyse 

/*

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb AS 
WITH tag_am AS ( SELECT DISTINCT CODE_CLIENT , 
 MAX(CASE WHEN CODE_AM ='101623' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM101623, 
 MAX(CASE WHEN CODE_AM ='130146' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130146,
 MAX(CASE WHEN CODE_AM ='130147' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130147,
 MAX(CASE WHEN CODE_AM ='130148' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130148,
 MAX(CASE WHEN CODE_AM ='108250' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM108250
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket 
WHERE ID_TICKET IS NOT NULL ),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) )
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
    top_AM101623, top_AM130146, top_AM130147, top_AM130148, top_AM108250, 
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
from DHB_PROD.DNR.DN_VENTE vd
LEFT JOIN tag_am tg ON vd.CODE_CLIENT=tg.CODE_CLIENT
LEFT JOIN segrfm g ON vd.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON vd.CODE_CLIENT=f.CODE_CLIENT 
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' ; 
 
 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb_Famxts AS
SELECT * FROM 
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 GROUP BY 1,2
 UNION
SELECT '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
     ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 GROUP BY 1,2)
 ORDER BY 1,2 ;
 
Select * FROM DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb_Famxts ORDER BY 1,2 ;   
 
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb AS
SELECT * FROM 
(SELECT '00_GLOBAL' AS gp_clt, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 GROUP BY 1,2,3 
 UNION 
 SELECT '00_GLOBAL' AS gp_clt, '00_GLOBAL' AS typo_clt, SEGMENT_RFM AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 GROUP BY 1,2,3
  UNION
 SELECT '01-ANNIVERSAIRE' AS gp_clt, 'CODE_AM101623' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 WHERE top_AM101623=1
 GROUP BY 1,2,3  
  UNION
 SELECT '01-ANNIVERSAIRE' AS gp_clt, 'CODE_AM101623' AS typo_clt, SEGMENT_RFM AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 WHERE top_AM101623=1
 GROUP BY 1,2,3 
  UNION 
 SELECT '02-CHEQUE_FID' AS gp_clt, 'CODE_AM130146' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 WHERE top_AM130146=1 
 GROUP BY 1,2,3 
 UNION 
 SELECT '02-CHEQUE_FID' AS gp_clt, 'CODE_AM130146' AS typo_clt, SEGMENT_RFM AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb 
 WHERE top_AM130146=1 
 GROUP BY 1,2,3
  UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130147' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM130147=1
 GROUP BY 1,2,3 
   UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130147' AS typo_clt, SEGMENT_RFM AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM130147=1
 GROUP BY 1,2,3
  UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130148' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM130148=1
 GROUP BY 1,2,3
   UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130148' AS typo_clt, SEGMENT_RFM AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM130148=1
 GROUP BY 1,2,3
   UNION 
 SELECT '04-J_PRIVILEGE' AS gp_clt, 'CODE_AM108250' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM108250=1
 GROUP BY 1,2,3
  UNION 
 SELECT '04-J_PRIVILEGE' AS gp_clt, 'CODE_AM108250' AS typo_clt, SEGMENT_RFM AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb  
 WHERE top_AM108250=1
 GROUP BY 1,2,3 )
 ORDER BY 1,2,3 ; 


Select * FROM DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb  ORDER BY 1,2,3 ; 


--- Information famille de produits 


-- Statistique des familles de Produits 

/*
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS AS 
WITH tag_am AS ( SELECT DISTINCT ID_TICKET , 
 MAX(CASE WHEN CODE_AM ='101623' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY ID_TICKET) AS  top_AM101623, 
 MAX(CASE WHEN CODE_AM ='130146' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY ID_TICKET) AS  top_AM130146,
 MAX(CASE WHEN CODE_AM ='130147' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY ID_TICKET) AS  top_AM130147,
 MAX(CASE WHEN CODE_AM ='130148' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY ID_TICKET) AS  top_AM130148,
 MAX(CASE WHEN CODE_AM ='108250' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY ID_TICKET) AS  top_AM108250
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
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM101623=1
GROUP BY 1,2,3,4 
UNION
SELECT '01-ANNIVERSAIRE' AS lIB_CODE_AM, '101623' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM101623=1
GROUP BY 1,2,3,4
UNION
SELECT '02-CHEQUE_FID' AS lIB_CODE_AM, '130146' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS  
 WHERE top_AM130146=1
GROUP BY 1,2,3,4 
UNION
SELECT '02-CHEQUE_FID' AS lIB_CODE_AM, '130146' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130146=1
GROUP BY 1,2,3,4
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130147' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130147=1
GROUP BY 1,2,3,4 
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130147' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130147=1
GROUP BY 1,2,3,4 
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130148' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130148=1
GROUP BY 1,2,3,4 
UNION
SELECT '03-BIENVENUE' AS lIB_CODE_AM, '130148' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM130148=1
GROUP BY 1,2,3,4 
UNION
SELECT '04-J_PRIVILEGE' AS lIB_CODE_AM, '108250' AS CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM108250=1
GROUP BY 1,2,3,4 
UNION
SELECT '04-J_PRIVILEGE' AS lIB_CODE_AM, '108250' AS CODE_AM, '01-FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_XTS 
 WHERE top_AM108250=1
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4;


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.Stat_ticket_XTS ORDER BY 1,2,3,4;

*/

--- information sur N-1 
/*
Select * FROM FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 AS 
WITH tag_am AS ( SELECT DISTINCT CODE_CLIENT , 
 MAX(CASE WHEN CODE_AM ='101623' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM101623, 
 MAX(CASE WHEN CODE_AM ='130146' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130146,
 MAX(CASE WHEN CODE_AM ='130147' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130147,
 MAX(CASE WHEN CODE_AM ='130148' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM130148,
 MAX(CASE WHEN CODE_AM ='108250' AND ID_TICKET IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY CODE_CLIENT) AS  top_AM108250
FROM DATA_MESH_PROD_RETAIL.WORK.tab_coupon_ticket_Nm1 
WHERE ID_TICKET IS NOT NULL ),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) )
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
    top_AM101623, top_AM130146, top_AM130147, top_AM130148, top_AM108250, 
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
from DHB_PROD.DNR.DN_VENTE vd
LEFT JOIN tag_am tg ON vd.CODE_CLIENT=tg.CODE_CLIENT
LEFT JOIN segrfm g ON vd.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON vd.CODE_CLIENT=f.CODE_CLIENT 
where vd.date_ticket BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) 
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' ; 
 
 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb_Nm1 AS
SELECT * FROM 
(SELECT '00_GLOBAL' AS gp_clt, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 
 GROUP BY 1,2,3 
 UNION 
 SELECT '00_GLOBAL' AS gp_clt, '00_GLOBAL' AS typo_clt, SEGMENT_RFM AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 
 GROUP BY 1,2,3
  UNION
 SELECT '01-ANNIVERSAIRE' AS gp_clt, 'CODE_AM101623' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 
 WHERE top_AM101623=1
 GROUP BY 1,2,3  
  UNION
 SELECT '01-ANNIVERSAIRE' AS gp_clt, 'CODE_AM101623' AS typo_clt, SEGMENT_RFM AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 
 WHERE top_AM101623=1
 GROUP BY 1,2,3 
  UNION 
 SELECT '02-CHEQUE_FID' AS gp_clt, 'CODE_AM130146' AS typo_clt, '00_GLOBAL' AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 
 WHERE top_AM130146=1 
 GROUP BY 1,2,3 
 UNION 
 SELECT '02-CHEQUE_FID' AS gp_clt, 'CODE_AM130146' AS typo_clt, SEGMENT_RFM AS modalite 
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1 
 WHERE top_AM130146=1 
 GROUP BY 1,2,3
  UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130147' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1  
 WHERE top_AM130147=1
 GROUP BY 1,2,3 
   UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130147' AS typo_clt, SEGMENT_RFM AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1  
 WHERE top_AM130147=1
 GROUP BY 1,2,3
  UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130148' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1  
 WHERE top_AM130148=1
 GROUP BY 1,2,3
   UNION 
 SELECT '03-BIENVENUE' AS gp_clt, 'CODE_AM130148' AS typo_clt, SEGMENT_RFM AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1  
 WHERE top_AM130148=1
 GROUP BY 1,2,3
   UNION 
 SELECT '04-J_PRIVILEGE' AS gp_clt, 'CODE_AM108250' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1  
 WHERE top_AM108250=1
 GROUP BY 1,2,3
  UNION 
 SELECT '04-J_PRIVILEGE' AS gp_clt, 'CODE_AM108250' AS typo_clt, SEGMENT_RFM AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt
    ,COUNT( DISTINCT id_ticket) AS nb_ticket
    ,SUM(MONTANT_TTC ) AS CA
	,SUM(QUANTITE_LIGNE) AS qte_achete
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge
    ,SUM(montant_remise) AS Mnt_remise_mkt
   ,SUM(M_remise) AS Mnt_remise_glb
 FROM DATA_MESH_PROD_RETAIL.WORK.base_ticket_glb_Nm1  
 WHERE top_AM108250=1
 GROUP BY 1,2,3 )
 ORDER BY 1,2,3 ; 


Select * FROM DATA_MESH_PROD_RETAIL.WORK.stat_ticket_glb_Nm1  ORDER BY 1,2,3 ; 


