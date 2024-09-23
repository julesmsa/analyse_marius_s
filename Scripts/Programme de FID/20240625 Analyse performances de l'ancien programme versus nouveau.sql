-- Nouvelle Demande FId à Traiter 
-- créer une  table avec l'ensemble des coupons
--- Information sur l'année N 

SET dtdeb_N = Date('2024-04-24'); --- prendre le 24 Avril 2024
SET dtfin_N = DAte('2024-06-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_N, $dtfin_N;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N AS 
WITH 
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_PART , LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_PART, LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_PART   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5)
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
g.ID_MACRO_SEGMENT_PART, g.LIB_MACRO_SEGMENT_PART, LIB_SEGMENT_OMNI_PART,
CASE WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '01' THEN '01_VIP' 
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '02' THEN '02_TBC'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '03' THEN '03_BC'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '04' THEN '04_MOY'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '05' THEN '05_TAP'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '06' THEN '06_TIEDE'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '07' THEN '07_TPURG'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '09' THEN '08_NCV'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '08' THEN '09_NAC'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '10' THEN '10_INA12'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM , 
DATE_RECRUTEMENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT
From DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE a
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.code_magasin=b.ID_MAGASIN 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE c ON a.CODE_CLIENT = c.CODE_CLIENT
LEFT JOIN tabtg g ON a.CODE_CLIENT=g.code_client AND DATE_FROM_PARTS(YEAR(date_debut_validite) , MONTH(date_debut_validite), 1)=g.DATE_PARTITION 
where code_am IN ('101623','301906','130146','126861','326910','130147','130148','108250') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb_N) AND DATE($dtfin_N);

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_tick_N AS 
WITH tab0 AS (SELECT t1.id_org_enseigne AS id_enseigne, t1.id_magasin AS id_mag, t1.code_magasin, t1.code_ticket, t1.code_ligne, t1.type_ligne, t1.dateh_ticket, t1.code_date_ticket, t1.code_caisse, t1.code_reference,  
t1.quantite_ligne, t1.prix_unitaire, t1.montant_ttc, t1.code_AM, t1.montant_remise, t1.MONTANT_MARGE_SORTIE,
CONCAT(t1.id_org_enseigne,'-',t1.id_magasin,'-',t1.code_caisse,'-',t1.code_date_ticket,'-',t1.code_ticket) as id_ticket_lig,
CONCAT(t2.id_org_enseigne,'-',t2.id_magasin,'-',t2.code_caisse,'-',t2.code_date_ticket,'-',t2.code_ticket) as id_ticket_ligt2,
t2.MONTANT AS MONTANT_xts , t2.montant_remise AS remise_xts, t2.NUMERO_OPERATION , Date(t2.dateh_ticket) AS Date_ticket
FROM dhb_prod.hub.F_VTE_TICKET_LIGNE_V2 t1
LEFT JOIN dhb_prod.hub.f_vte_remise_detaillee t2 on t1.id_magasin=t2.id_magasin and t1.code_ligne=t2.code_ligne and t1.code_ticket=t2.code_ticket 
AND id_ticket_lig=id_ticket_ligt2
WHERE DATE(t1.dateh_ticket) BETWEEN DATE($dtdeb_N) AND DATE($dtfin_N) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
AND id_ticket_lig=id_ticket_ligt2) 
SELECT * FROM tab0; 

SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_tick_N;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N AS 
WITH 
tab_vte as (Select code_client,id_org_enseigne, id_magasin, id_ticket, date_ticket as date_ticket_vte 
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
,SUM(CASE WHEN id_ticket IS NOT null THEN MONTANT_MARGE_SORTIE End) AS S_marge_ttc
FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE
where DATE(date_ticket) BETWEEN DATE($dtdeb_N) AND DATE($dtfin_N) and code_client is not null and code_client !='0'
group by 1,2,3,4,5),
tab0 AS (
SELECT id_enseigne, id_mag, id_ticket_lig, NUMERO_OPERATION, Date_ticket, CAST (NUMERO_OPERATION AS VARCHAR) AS code_op_am,
SUM(remise_xts) AS S_remise_xts
FROM DATA_MESH_PROD_RETAIL.WORK.tab_tick_N
GROUP BY 1,2,3,4,5)
SELECT a.* ,type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_mag_achat, 
tv.code_client AS id_client, S_montant_remise, S_montant_ttc,S_marge_ttc 
FROM tab0 a
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c.id_org_enseigne and a.id_mag=c.ID_MAGASIN 
LEFT JOIN tab_vte tv ON a.id_enseigne=tv.id_org_enseigne and a.id_mag=c.ID_MAGASIN AND a.id_ticket_lig=tv.id_ticket AND a.Date_ticket=tv.date_ticket_vte ;

/*
SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N 
WHERE CODE_OP_AM ='130146' 
AND ID_TICKET_LIG ='1-1029-1-20240505-14126019';

SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_tick_N WHERE ID_TICKET_LIG ='1-452-1-20240606-14158006';
 
SELECT CODE_OP_AM, AVG(S_REMISE_XTS) AS moy_remise
FROM  DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N 
GROUP BY 1;
*/

SELECT Code_op_am, 
Count(DISTINCT ID_CLIENT) AS nbclt
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N
GROUP BY 1
ORDER BY 1; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N AS 
Select a.* , b.*
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N b ON a.code_client=b.id_client and a.code_am=b.code_op_am ; 








SELECT *FROM  DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon_N AS 
SELECT * FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N
GROUP BY 1,2,3,4)
UNION ALL
(SELECT '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N
GROUP BY 1,2,3,4)
UNION ALL
(SELECT '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI_PART AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon_N ORDER BY 1,2,3,4; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fin_catcoupon_N AS 
SELECT * FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, lIB_CODE_AM
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N
GROUP BY 1,2,3)
UNION ALL
(SELECT '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite, lIB_CODE_AM
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N
GROUP BY 1,2,3)
UNION ALL
(SELECT '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI_PART AS modalite, lIB_CODE_AM 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N
GROUP BY 1,2,3)
ORDER BY 1,2,3);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_catcoupon_N ORDER BY 1,2,3,4;



/***** Information sur l'année N-1  */

SET dtdeb_Nm1 = Date('2023-04-24');
SET dtfin_Nm1 = DAte('2023-06-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_Nm1, $dtfin_Nm1;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_Nm1 AS 
WITH 
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_PART , LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_PART, LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_PART   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5)
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
g.ID_MACRO_SEGMENT_PART, g.LIB_MACRO_SEGMENT_PART,
CASE WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '01' THEN '01_VIP' 
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '02' THEN '02_TBC'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '03' THEN '03_BC'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '04' THEN '04_MOY'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '05' THEN '05_TAP'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '06' THEN '06_TIEDE'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '07' THEN '07_TPURG'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '09' THEN '08_NCV'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '08' THEN '09_NAC'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '10' THEN '10_INA12'
     WHEN COALESCE(ID_MACRO_SEGMENT_PART,id_macro_segment) = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM , 
DATE_RECRUTEMENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI_PART
From DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE a
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.code_magasin=b.ID_MAGASIN 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE c ON a.CODE_CLIENT = c.CODE_CLIENT
LEFT JOIN tabtg g ON a.CODE_CLIENT=g.code_client AND DATE_FROM_PARTS(YEAR(date_debut_validite) , MONTH(date_debut_validite), 1)=g.DATE_PARTITION 
where code_am IN ('101623','301906','130146','126861','326910','130147','130148','108250') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1);

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_tick_Nm1 AS 
WITH tab0 AS (SELECT t1.id_org_enseigne AS id_enseigne, t1.id_magasin AS id_mag, t1.code_magasin, t1.code_ticket, t1.code_ligne, t1.type_ligne, t1.dateh_ticket, t1.code_date_ticket, t1.code_caisse, t1.code_reference,  
t1.quantite_ligne, t1.prix_unitaire, t1.montant_ttc, t1.code_AM, t1.montant_remise,
CONCAT(t1.id_org_enseigne,'-',t1.id_magasin,'-',t1.code_caisse,'-',t1.code_date_ticket,'-',t1.code_ticket) as id_ticket_lig,
CONCAT(t2.id_org_enseigne,'-',t2.id_magasin,'-',t2.code_caisse,'-',t2.code_date_ticket,'-',t2.code_ticket) as id_ticket_ligt2,
t2.MONTANT AS MONTANT_xts , t2.montant_remise AS remise_xts, t2.NUMERO_OPERATION , Date(t2.dateh_ticket) AS Date_ticket
FROM dhb_prod.hub.F_VTE_TICKET_LIGNE_V2 t1
LEFT JOIN dhb_prod.hub.f_vte_remise_detaillee t2 on t1.id_magasin=t2.id_magasin and t1.code_ligne=t2.code_ligne and t1.code_ticket=t2.code_ticket AND id_ticket_lig=id_ticket_ligt2
WHERE DATE(t1.dateh_ticket) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
AND id_ticket_lig=id_ticket_ligt2) 
SELECT * FROM tab0; 

SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_tick_Nm1;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_Nm1 AS 
WITH 
tab_vte as (Select code_client,id_org_enseigne, id_magasin, id_ticket, date_ticket as date_ticket_vte 
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
,SUM(CASE WHEN id_ticket IS NOT null THEN MONTANT_MARGE_SORTIE End) AS S_marge_ttc
FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE
where DATE(date_ticket) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) and code_client is not null and code_client !='0'
group by 1,2,3,4,5),
tab0 AS (
SELECT id_enseigne, id_mag, id_ticket_lig, NUMERO_OPERATION, Date_ticket, CAST (NUMERO_OPERATION AS VARCHAR) AS code_op_am,
SUM(remise_xts) AS S_remise_xts
FROM DATA_MESH_PROD_RETAIL.WORK.tab_tick_Nm1
GROUP BY 1,2,3,4,5)
SELECT a.* ,type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_mag_achat, 
tv.code_client AS id_client, S_montant_remise, S_montant_ttc,S_marge_ttc
FROM tab0 a
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c.id_org_enseigne and a.id_mag=c.ID_MAGASIN 
LEFT JOIN tab_vte tv ON a.id_enseigne=tv.id_org_enseigne and a.id_mag=c.ID_MAGASIN AND a.id_ticket_lig=tv.id_ticket AND a.Date_ticket=tv.date_ticket_vte ;

SELECT *FROM  DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_Nm1 ;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_Nm1; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1 AS 
Select a.* , b.*
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_Nm1 a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_Nm1 b ON a.code_client=b.id_client and a.code_am=b.code_op_am ; 

SELECT *FROM  DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon_Nm1 AS 
SELECT * FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1
GROUP BY 1,2,3,4)
UNION ALL
(SELECT '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1
GROUP BY 1,2,3,4)
UNION ALL
(SELECT '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI_PART AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon_Nm1 ORDER BY 1,2,3,4; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fin_catcoupon_Nm1 AS 
SELECT * FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, lIB_CODE_AM
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1
GROUP BY 1,2,3)
UNION ALL
(SELECT '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite, lIB_CODE_AM
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1
GROUP BY 1,2,3)
UNION ALL
(SELECT '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI_PART AS modalite, lIB_CODE_AM 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL then code_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then code_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then code_client end ) AS nb_clt_used_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_Nm1
GROUP BY 1,2,3)
ORDER BY 1,2,3);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_catcoupon_Nm1 ORDER BY 1,2,3,4;

/**** Extraction des tables ***/ 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_catcoupon_Nm1 ORDER BY 1,2,3,4;
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon_Nm1 ORDER BY 1,2,3,4;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_catcoupon_N ORDER BY 1,2,3,4;
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon_N ORDER BY 1,2,3,4;

/**** Requetes a sauv
 * 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_OMNI AS 
WITH tagr AS (SELECT DISTINCT id_client , date_ticket , DATE_FROM_PARTS(YEAR(date_ticket) , MONTH(date_ticket)-1, 1) AS date_nivm1
, DATE_FROM_PARTS(YEAR(date_ticket) , MONTH(date_ticket), 1) AS date_niv
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N ),
tabnm AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_M1   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5),
tabn AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5)
SELECT DISTINCT a.* , b.DATE_PARTITION AS DATE_PARTITION_M1, b.LIB_SEGMENT_OMNI_M1, c.DATE_PARTITION, c.LIB_SEGMENT_OMNI 
FROM tagr a 
LEFT JOIN tabnm b ON a.id_client=b.code_client AND a.date_nivm1=b.DATE_PARTITION 
LEFT JOIN tabn c ON a.id_client=c.code_client AND a.date_niv=c.DATE_PARTITION ; 


***/

--- Information sur utilisation journée privilège ----- Depuis 2024

-- Analyse Journée Privilègpour l'année 2023 

SET dtdeb_JP = Date('2024-01-01'); --- prendre le 24 Avril 2024
SET dtfin_JP = DAte('2024-06-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_JP, $dtfin_JP;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_tick_JP AS 
WITH tab0 AS (SELECT t1.id_org_enseigne AS id_enseigne, t1.id_magasin AS id_mag, t1.code_magasin, t1.code_ticket, t1.code_ligne, t1.type_ligne, t1.dateh_ticket, t1.code_date_ticket, t1.code_caisse, t1.code_reference,  
t1.quantite_ligne, t1.prix_unitaire, t1.montant_ttc, t1.code_AM, t1.montant_remise,
CONCAT(t1.id_org_enseigne,'-',t1.id_magasin,'-',t1.code_caisse,'-',t1.code_date_ticket,'-',t1.code_ticket) as id_ticket_lig,
CONCAT(t2.id_org_enseigne,'-',t2.id_magasin,'-',t2.code_caisse,'-',t2.code_date_ticket,'-',t2.code_ticket) as id_ticket_ligt2,
t2.MONTANT AS MONTANT_xts , t2.montant_remise AS remise_xts, t2.NUMERO_OPERATION , Date(t2.dateh_ticket) AS Date_ticket
FROM dhb_prod.hub.F_VTE_TICKET_LIGNE_V2 t1
LEFT JOIN dhb_prod.hub.f_vte_remise_detaillee t2 on t1.id_magasin=t2.id_magasin and t1.code_ligne=t2.code_ligne and t1.code_ticket=t2.code_ticket AND id_ticket_lig=id_ticket_ligt2
WHERE DATE(t1.dateh_ticket) BETWEEN DATE($dtdeb_jp) AND DATE($dtfin_jp) AND NUMERO_OPERATION IN ('108250'))
SELECT * FROM tab0; 

SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_tick_JP;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_JP AS 
WITH 
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5 AND DATE(DATE_PARTITION) = DATE_FROM_PARTS(YEAR($dtfin_jp) , MONTH($dtfin_jp), 1) ),
tab_vte as (Select code_client,id_org_enseigne, id_magasin, id_ticket, date_ticket as date_ticket_vte
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
,SUM(CASE WHEN id_ticket IS NOT null THEN QUANTITE_LIGNE End) AS S_QUANTITE_LIGNE
,SUM(CASE WHEN id_ticket IS NOT null THEN MONTANT_MARGE_SORTIE End) AS S_marge_ttc
FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE
where DATE(date_ticket) BETWEEN DATE($dtdeb_jp) AND DATE($dtfin_jp) and code_client is not null and code_client !='0'
group by 1,2,3,4,5),
tab0 AS (
SELECT id_enseigne, id_mag, id_ticket_lig, NUMERO_OPERATION, Date_ticket, CAST (NUMERO_OPERATION AS VARCHAR) AS code_op_am,
SUM(remise_xts) AS S_remise_xts
FROM DATA_MESH_PROD_RETAIL.WORK.tab_tick_JP
GROUP BY 1,2,3,4,5)
SELECT a.* ,type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_mag_achat, 
tv.code_client, S_montant_remise, S_montant_ttc, S_marge_ttc, S_QUANTITE_LIGNE, g.LIB_SEGMENT_OMNI , g.ID_MACRO_SEGMENT, clt.DATE_RECRUTEMENT,
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
FROM tab0 a
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c.id_org_enseigne and a.id_mag=c.ID_MAGASIN 
LEFT JOIN tab_vte tv ON a.id_enseigne=tv.id_org_enseigne and a.id_mag=c.ID_MAGASIN AND a.id_ticket_lig=tv.id_ticket AND a.Date_ticket=tv.date_ticket_vte
LEFT JOIN tabtg g ON tv.CODE_CLIENT=g.code_client
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE clt ON tv.CODE_CLIENT = clt.CODE_CLIENT;

-- SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_tick_JP WHERE date_ticket='2022-03-05';

--- Statistqiues de Ventes des tickets

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_JP_Nm1 AS 
SELECT * FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
,Min(DATE(date_ticket)) AS date_deb
,Max(DATE(date_ticket)) AS date_fin
,Count(DISTINCT CODE_CLIENT) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then CODE_CLIENT end ) AS nb_clt_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then CODE_CLIENT end ) AS nb_clt_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_QUANTITE_LIGNE End) AS SUM_QUANTITE
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_JP
GROUP BY 1,2)
UNION 
(SELECT '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite
,Min(DATE(date_ticket)) AS date_deb
,Max(DATE(date_ticket)) AS date_fin
,Count(DISTINCT CODE_CLIENT) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then CODE_CLIENT end ) AS nb_clt_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then CODE_CLIENT end ) AS nb_clt_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_QUANTITE_LIGNE End) AS SUM_QUANTITE
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_JP
GROUP BY 1,2)
UNION
(SELECT '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI AS modalite
,Min(DATE(date_ticket)) AS date_deb
,Max(DATE(date_ticket)) AS date_fin
,Count(DISTINCT CODE_CLIENT) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then CODE_CLIENT end ) AS nb_clt_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then CODE_CLIENT end ) AS nb_clt_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_QUANTITE_LIGNE End) AS SUM_QUANTITE
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_JP
GROUP BY 1,2)
UNION 
(SELECT '04_Mois' AS typo_clt, 
CASE WHEN MONTH(date_ticket)<10 THEN Concat(YEAR(date_ticket),'_Mois_0',MONTH(date_ticket)) ELSE Concat(YEAR(date_ticket),'_Mois_',MONTH(date_ticket)) END AS modalite
,Min(DATE(date_ticket)) AS date_deb
,Max(DATE(date_ticket)) AS date_fin
,Count(DISTINCT CODE_CLIENT) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='MAG' then CODE_CLIENT end ) AS nb_clt_MAG 
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL and canal_mag_achat='WEB' then CODE_CLIENT end ) AS nb_clt_WEB
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_QUANTITE_LIGNE End) AS SUM_QUANTITE
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_JP
GROUP BY 1,2)
ORDER BY 1,2);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_JP_Nm1 ORDER BY 1,2; 


---- Nombre de clients par Palier 

SET dtfin_frt = DAte('2024-06-30'); 

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE ; 

WITH tab0 AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, STATUT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT,LIB_SEGMENT_OMNI, nombre_points_fidelite
,datediff(YEAR,DATE_DERNIER_ACHAT,$dtfin_frt) AS ACTIF_CLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE DATE_DERNIER_ACHAT IS NOT NULL AND datediff(MONTH,DATE_DERNIER_ACHAT,$dtfin_frt)<=36)
SELECT * FROM (
(SELECT '00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 0 AND 99 THEN Code_client end) AS nb_client_0_99pts, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 100 AND 199 THEN Code_client end) AS nb_client_100_199pts, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 200 AND 299 THEN Code_client end) AS nb_client_2000_299pts,
Count(DISTINCT CASE WHEN nombre_points_fidelite >=300 THEN Code_client end) AS nb_client_300pts
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '01-SEGMENT OMNI' AS Typo, LIB_SEGMENT_OMNI AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 0 AND 99 THEN Code_client end) AS nb_client_0_99pts, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 100 AND 199 THEN Code_client end) AS nb_client_100_199pts, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 200 AND 299 THEN Code_client end) AS nb_client_2000_299pts,
Count(DISTINCT CASE WHEN nombre_points_fidelite >=300 THEN Code_client end) AS nb_client_300pts
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '02-SEGMENT RFM' AS Typo, CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
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
  ELSE '12_NOSEG' END AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 0 AND 99 THEN Code_client end) AS nb_client_0_99pts, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 100 AND 199 THEN Code_client end) AS nb_client_100_199pts, 
Count(DISTINCT CASE WHEN nombre_points_fidelite BETWEEN 200 AND 299 THEN Code_client end) AS nb_client_2000_299pts,
Count(DISTINCT CASE WHEN nombre_points_fidelite >=300 THEN Code_client end) AS nb_client_300pts
FROM tab0
GROUP BY 1,2)
) 
ORDER BY 1,2; 

--- Verification coupon biennue par client 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N 
WHERE code_client IN ('160100054029' , '060012172619') AND code_am IN ('130147', '130148') 
ORDER BY CODE_CLIENT ; 


SELECT Code_am, Description_courte, 
COUNT(DISTINCT Code_coupon) AS nb_coupon, 
count(DISTINCT code_client) AS nb_clt
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N 
GROUP BY 1,2
ORDER BY 1,2 ; 

-- Code 
SELECT code_client, COUNT(DISTINCT Code_coupon) AS nb_coupon , 
COUNT(DISTINCT code_am) AS nb_code_am 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N
WHERE code_am IN ('130147', '130148')
GROUP BY 1 
HAVING nb_coupon >1
ORDER BY nb_coupon DESC ;





SELECT code_client, COUNT(DISTINCT Code_coupon) AS nb_coupon , 
COUNT(DISTINCT code_am) AS nb_code_am 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N
WHERE code_am IN ('130146')
GROUP BY 1 
HAVING nb_coupon >1
ORDER BY nb_coupon DESC 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N WHERE code_am IN ('130146') AND code_client='006511803341';


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon_N WHERE code_client='006511803341'
; 
