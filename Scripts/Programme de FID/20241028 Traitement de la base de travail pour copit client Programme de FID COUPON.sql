-- Analyse Nouveau programme de FID pour le conseil 
  -- date de lancement du programme de FID 

/**** Analyse Cheque FID 
('130146')          THEN '02-CHEQUE_FID'

Période d’analyse : depuis le 24/04 jusqu’au 31/08
France Belgique magasins + web
Evolution par mois + un global


Nombre de chèques émis / utilisés / taux d’utilisation 
Nombre moyen de chèques / client
Délai moyen d'utilisation du chèque après émission
Valeur moyenne du chèque 
CA / % dans le CA total
Taux de remise / Taux de marge sortie
IV / PM
Fréquence
Typologie de clients 
Clients omnicanaux

Comparer ces mêmes chiffres à tous les autres tickets sur la période

****/ 

SET dtdeb='2024-04-24';
SET dtfin='2024-05-31';
SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb)); 
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb, $dtfin, $dtdeb_Nm1, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

--- Information sur l'année N

        
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N AS 
WITH info_clt AS (
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
    FROM DHB_PROD.DNR.DN_CLIENT),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) )
Select distinct a.code_coupon, a.date_debut_validite, a.date_fin_validite, a.code_magasin as codemag_coupon, a.code_client, 
a.valeur, a.code_am, a.code_status, a.description_longue, a.description_courte, type_emplacement as type_empl_coupon, lib_magasin as libmag_coupon, 
DATE_FROM_PARTS(YEAR(date_debut_validite) , MONTH(date_debut_validite)-1, 1) AS date_niv_part,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_Crea_coupon,
CASE 
	WHEN code_am IN ('101623','301906') THEN '01-ANNIVERSAIRE'
	WHEN code_am IN ('130146')          THEN '02-CHEQUE_FID'
   WHEN code_am IN ('108250')          THEN '04-J_PRIVILEGE '
	WHEN code_am IN ('126861','326910','130147','130148') THEN '03-BIENVENUE'
END AS lIB_CODE_AM, 
CASE WHEN code_am IN ('108250') AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN 1 
WHEN code_am IN ('101623','301906','130146','126861','326910','130147','130148') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN 1
ELSE 0 END AS Top_period,
idclt, date_naissance, genre,
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
From DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE a
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.code_magasin=b.ID_MAGASIN 
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt 
LEFT JOIN segrfm g ON a.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON a.CODE_CLIENT=f.CODE_CLIENT 
where Top_period=1;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N;

--- identification des coupon utilisé sur les tickets 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N AS
Select vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, vd.MONTANT_REMISE_OPE_COMM, 
vd.montant_remise +  vd.MONTANT_REMISE_OPE_COMM AS remise_total, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
CODE_AM, 
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
ROW_NUMBER() OVER (PARTITION BY VD.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,     
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
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N ; 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N AS
 WITH tab0  AS (SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N ) 
SELECT * FROM (
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON1 AS CODE_COUPON,
MTREMISE_COUPON1 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON1 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON1 IS NOT NULL  
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON2 AS CODE_COUPON,
MTREMISE_COUPON2 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON2 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON2 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON3 AS CODE_COUPON,
MTREMISE_COUPON3 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON3 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON3 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON4 AS CODE_COUPON,
MTREMISE_COUPON4 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON4 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON4 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON5 AS CODE_COUPON,
MTREMISE_COUPON5 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON5 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON5 IS NOT NULL 
) 
WHERE CODEACTIONMARKETING_COUPON IN ('101623','301906','130146','126861','326910','130147','130148','108250');

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_coupon_detail AS
WITH tab1 AS (SELECT CONCAT(id_org_enseigne,'-',id_magasin,'-',code_caisse,'-',code_date_ticket,'-',code_ticket) as id_ticket_ligt2, NUMERO_OPERATION 
,SUM(Montant_remise) AS remise_VBNUM2
FROM dhb_prod.hub.f_vte_remise_detaillee
WHERE DATE(dateh_ticket) BETWEEN DATE($dtdeb) AND DATE($dtfin) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
GROUP BY 1,2),
tab0  AS (SELECT DISTINCT CODE_CLIENT, id_ticket
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N )
SELECT a.CODE_CLIENT, b.*
FROM tab0 a
INNER JOIN tab1 b ON a.id_ticket = b.id_ticket_ligt2
ORDER BY 1,2;

--SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_coupon_detail ; 

--SELECT DISTINCT NUMERO_OPERATION FROM DATA_MESH_PROD_CLIENT.WORK.tab_coupon_detail

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N_V2 AS
SELECT DISTINCT  CODE_CLIENT, id_ticket, CODE_COUPON, MTREMISE_COUPON, CODEACTIONMARKETING_COUPON 
FROM (
SELECT * FROM tab_list_coupon_N 
UNION 
(SELECT CODE_CLIENT, 
id_ticket_ligt2 AS id_ticket,
0 AS Code_coupon, 
remise_VBNUM2 AS MTREMISE_COUPON , 
NUMERO_OPERATION AS CODEACTIONMARKETING_COUPON 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_coupon_detail
WHERE 
CONCAT( CODE_CLIENT, id_ticket_ligt2, CAST (NUMERO_OPERATION AS VARCHAR(10)) ) NOT IN (SELECT DISTINCT CONCAT( CODE_CLIENT, id_ticket, CAST (CODEACTIONMARKETING_COUPON AS VARCHAR(10)) ) AS unik FROM tab_list_coupon_N)
));

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N_V2 ;  

-- information de chaque ticket de caisse 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_statticket_N AS
SELECT CODE_CLIENT,id_ticket, Qte_pos,PERIMETRE,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN remise_total END) AS Rem_Gbl,
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4; 


-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_statticket_N; 
-- recuperer toutes les informations ticket pour chaque code coupon 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_stat_V AS
SELECT a.*,  b.id_ticket AS id_ticket_lign, Qte_pos,PERIMETRE, b.Mtn_Gbl, b.Qte_Gbl, b.Marge_Gbl, b.Rem_Gbl
FROM tab_list_coupon_N_V2 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_statticket_N b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.id_ticket=b.id_ticket ; 

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_stat_V ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_cvf AS
WITH tab0 AS (SELECT DISTINCT Code_client , code_coupon, CODEACTIONMARKETING_COUPON
FROM DATA_MESH_PROD_CLIENT.WORK.tab_stat_V
WHERE code_coupon=0) 
SELECT a.*, b.code_coupon AS cp_cde
FROM tab0 a
INNER JOIN DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N b ON a.Code_client=b.Code_client AND a.CODEACTIONMARKETING_COUPON=b.CODE_AM ;

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_cvf WHERE code_client='037410022657'; 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_stat_V2 AS
SELECT a.*, cp_cde , 
CASE WHEN a.code_coupon=0 THEN cp_cde ELSE a.code_coupon END AS code_coupon_used
FROM DATA_MESH_PROD_CLIENT.WORK.tab_stat_V a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_cvf b ON a.Code_client=b.Code_client AND a.CODEACTIONMARKETING_COUPON=b.CODEACTIONMARKETING_COUPON ;

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_stat_V2 WHERE code_client='020410023328';

--- jointure des informations d'utilisation ticket et coupon 

-- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N WHERE code_client='020410023328'; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon AS
SELECT a.*, 
 b.CODe_Client AS idclt_tk, b.id_ticket, b.code_coupon_used, b.mtremise_coupon, b.CODEACTIONMARKETING_COUPON, Qte_pos,PERIMETRE, b.Mtn_Gbl, b.Qte_Gbl, b.Marge_Gbl, b.Rem_Gbl
 FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N a
 LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_stat_V2  b ON a.CODE_COUPON=b.code_coupon_used AND a.code_AM=b.CODEACTIONMARKETING_COUPON;

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon ;

-- WHERE code_am = 130148;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_N AS
SELECT * FROM (
SELECT lIB_CODE_AM, CODE_AM,
'00-Global' AS Typo, '00-Global' AS modalite
,Min(DATE(date_debut_validite)) AS date_deb_op
,Max(DATE(date_fin_validite)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN code_coupon_used END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon_used END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon_used END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon_used END) AS nb_coupon_used_Web
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN idclt_tk end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN idclt_tk end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN idclt_tk end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Mtremise_coupon End) AS remise_coupon
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN rem_gbl End) AS remise_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN mtn_gbl End) AS CA_Global
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Qte_Gbl End) AS Qte_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Marge_Gbl End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'01-Typo_client' AS Typo, Type_client AS modalite
,Min(DATE(date_debut_validite)) AS date_deb_op
,Max(DATE(date_fin_validite)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN code_coupon_used END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon_used END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon_used END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon_used END) AS nb_coupon_used_Web
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN idclt_tk end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN idclt_tk end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN idclt_tk end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Mtremise_coupon End) AS remise_coupon
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN rem_gbl End) AS remise_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN mtn_gbl End) AS CA_Global
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Qte_Gbl End) AS Qte_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Marge_Gbl End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'02-Segment RFM' AS Typo, SEGMENT_RFM AS modalite
,Min(DATE(date_debut_validite)) AS date_deb_op
,Max(DATE(date_fin_validite)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN code_coupon_used END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon_used END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon_used END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon_used END) AS nb_coupon_used_Web
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN idclt_tk end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN idclt_tk end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN idclt_tk end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Mtremise_coupon End) AS remise_coupon
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN rem_gbl End) AS remise_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN mtn_gbl End) AS CA_Global
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Qte_Gbl End) AS Qte_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Marge_Gbl End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'03-Segment OMNI' AS Typo, SEGMENT_OMNI AS modalite
,Min(DATE(date_debut_validite)) AS date_deb_op
,Max(DATE(date_fin_validite)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN code_coupon_used END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon_used END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon_used END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon_used END) AS nb_coupon_used_Web
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN idclt_tk end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN idclt_tk end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN idclt_tk end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Mtremise_coupon End) AS remise_coupon
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN rem_gbl End) AS remise_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN mtn_gbl End) AS CA_Global
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Qte_Gbl End) AS Qte_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Marge_Gbl End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon
GROUP BY 1,2,3,4
UNION
SELECT lIB_CODE_AM, CODE_AM,
'04-SEXE' AS Typo, CASE WHEN genre='F' THEN '02-Femmes' ELSE '01-Hommes' END AS modalite
,Min(DATE(date_debut_validite)) AS date_deb_op
,Max(DATE(date_fin_validite)) AS date_fin_op
,Count(Distinct Code_coupon) AS nb_coupon_emis
,count(DISTINCT CASE WHEN DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN Code_coupon end) AS nb_cp_emis_fin_val
,Count(Distinct CASE WHEN canal_crea_coupon = 'MAG' THEN Code_coupon END ) AS nb_coupon_Mag
,Count(Distinct CASE WHEN canal_crea_coupon = 'WEB' THEN Code_coupon END ) AS nb_coupon_Web
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN code_coupon_used END) AS nb_coupon_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND DATE(date_fin_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN code_coupon_used END) AS nb_cp_used_fin_val
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN code_coupon_used END) AS nb_coupon_used_Mag
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN code_coupon_used END) AS nb_coupon_used_Web
,Count(DISTINCT code_client) AS nb_clt_coupon
,ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy
,ROUND (AVG (anciennete_client),1) AS anciennete_moy
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL THEN idclt_tk end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' THEN idclt_tk end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' THEN idclt_tk end ) AS nb_clt_used_WEB 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Qte_pos>0 THEN id_ticket end ) AS nb_ticket_used_WEB
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Mtremise_coupon End) AS remise_coupon
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN rem_gbl End) AS remise_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN mtn_gbl End) AS CA_Global
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Qte_Gbl End) AS Qte_totale
,SUM(CASE WHEN code_coupon_used IS NOT NULL THEN Marge_Gbl End) AS marge_totale
,Count(DISTINCT CASE WHEN Type_client='02-Nouveaux' THEN code_client END ) AS nb_newclt_coupon
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'MAG' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_MAG 
,Count(DISTINCT CASE WHEN code_coupon_used IS NOT NULL AND PERIMETRE = 'WEB' AND Type_client='02-Nouveaux' THEN idclt_tk end ) AS nb_newclt_used_WEB 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon
GROUP BY 1,2,3,4);

-- Calcul des Statistiques global 

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_JUIN24 AS
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
,DATE($dtfin)+1 AS DATE_CALCUL
FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_coupon_N a
ORDER BY 1,2,3,4 ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_JUIN24 ;


/**
 * SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_SEPTEMBRE24 ;
 * 
 * SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_OCTOBRE24 ;
 * SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_AOUT24
 * SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_JUILLET24
 * SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_JUIN24 ;
 */

--- REGROUPEMENT DE SINFORMATIONS DANS UNE TABLE EXPLOITABLE 


/*
 
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_globale_fid_GBLDFT AS 
SELECT * FROM (  
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_SEPTEMBRE24 
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_OCTOBRE24 
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_AOUT24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_JUILLET24
 UNION
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_JUIN24 
) 
ORDER BY 1,2,3,4 ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_fid_GBLDFT ORDER BY DATE_CALCUL,1,2,3,4 ;  

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_COUPONFID_FINAL AS 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_globale_fid_GBLDFT ORDER BY DATE_CALCUL,1,2,3,4; 

*/

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_COUPONFID_FINAL  ORDER BY DATE_CALCUL,1,2,3,4 ;

/*
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon_V2 AS
 SELECT a.*,b.*
 FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon a
 LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_coupon_detail b ON a.id_ticket=b.id_ticket_ligt2 AND a.code_AM=b.NUMERO_OPERATION ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_globale_coupon_V2 WHERE NUMERO_OPERATION=130148; 
*/


