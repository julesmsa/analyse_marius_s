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


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N ; 

--- Information ticket Client 

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
recrut AS (SELECT DISTINCT Code_client, DATE_RECRUTEMENT 
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE ),
tab_vte as (Select code_client,id_org_enseigne, id_magasin, id_ticket, date_ticket as date_ticket_vte 
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
,SUM(CASE WHEN id_ticket IS NOT null THEN QUANTITE_LIGNE End) AS S_QUANTITE_LIGNE
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
tv.code_client AS id_client, S_montant_remise, S_montant_ttc,S_marge_ttc,S_QUANTITE_LIGNE, DATE_RECRUTEMENT  
FROM tab0 a
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c.id_org_enseigne and a.id_mag=c.ID_MAGASIN 
LEFT JOIN tab_vte tv ON a.id_enseigne=tv.id_org_enseigne and a.id_mag=c.ID_MAGASIN AND a.id_ticket_lig=tv.id_ticket AND a.Date_ticket=tv.date_ticket_vte 
LEFT JOIN recrut clt ON tv.CODE_CLIENT = clt.CODE_CLIENT;

--- Stat des ventes et autre 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stab_clt_N AS 
WITH 
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5 AND DATE(DATE_PARTITION) = DATE_FROM_PARTS(YEAR($dtfin_N) , MONTH($dtfin_N), 1) ),
stat_coupons AS (SELECT CODE_CLIENT,code_am, lIB_CODE_AM
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N
GROUP BY 1,2,3),
Stat_vtes AS (SELECT id_client,code_op_am
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_QUANTITE_LIGNE End) AS SUM_QUANTITE_LIGNE
,MAX( CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then 1 ELSE 0 end ) AS newclt
,MAX( CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then 1 ELSE 0  end ) AS newclt_web
,MAX( CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then 1 ELSE 0  end ) AS newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_N
GROUP BY 1,2)
SELECT a.*, b.*, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI ,
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
FROM stat_coupons a 
LEFT JOIN Stat_vtes b ON a.CODE_CLIENT=b.id_client AND a.code_am=b.code_op_am 
LEFT JOIN tabtg g ON a.CODE_CLIENT=g.code_client; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_N

-- Statistique global

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fich_catcoupon_N AS 
SELECT * FROM (
SELECT lIB_CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
,Min(DATE(date_deb)) AS date_deb_op
,Max(DATE(date_fin)) AS date_fin_op
,Sum(nb_coupon) AS S_coupon
,Sum(Nb_coupon_MAG) AS S_coupon_Mag
,Sum(Nb_coupon_WEB) AS S_coupon_Web
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS nb_coupon_used
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_coupon_used_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN nb_ticket IS NOT NULL then id_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then id_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then id_client end ) AS nb_clt_used_WEB 
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS S_ticket
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_ticket_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_ticket_WEB
,SUM(CASE WHEN nb_ticket IS NOT null THEN sum_remise_xts End) AS remise_xts
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_mnt_ttc End) AS CA_ttc
,SUM(CASE WHEN nb_ticket IS NOT null THEN SUM_QUANTITE_LIGNE End) AS QUANTITE
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_marge_ttc End) AS marge_ttc
,SUM(CASE WHEN nb_ticket IS NOT NULL AND newclt>0 then newclt end ) AS nb_newclt
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_mag>0 then newclt_mag end ) AS nb_newclt_MAG
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_web>0 then newclt_web end ) AS nb_newclt_web
FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_N
GROUP BY 1,2,3
UNION 
SELECT lIB_CODE_AM, '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI AS modalite 
,Min(DATE(date_deb)) AS date_deb_op
,Max(DATE(date_fin)) AS date_fin_op
,Sum(nb_coupon) AS S_coupon
,Sum(Nb_coupon_MAG) AS S_coupon_Mag
,Sum(Nb_coupon_WEB) AS S_coupon_Web
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS nb_coupon_used
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_coupon_used_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN nb_ticket IS NOT NULL then id_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then id_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then id_client end ) AS nb_clt_used_WEB 
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS S_ticket
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_ticket_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_ticket_WEB
,SUM(CASE WHEN nb_ticket IS NOT null THEN sum_remise_xts End) AS remise_xts
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_mnt_ttc End) AS CA_ttc
,SUM(CASE WHEN nb_ticket IS NOT null THEN SUM_QUANTITE_LIGNE End) AS QUANTITE
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_marge_ttc End) AS marge_ttc
,SUM(CASE WHEN nb_ticket IS NOT NULL AND newclt>0 then newclt end ) AS nb_newclt
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_mag>0 then newclt_mag end ) AS nb_newclt_MAG
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_web>0 then newclt_web end ) AS nb_newclt_web
FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_N
GROUP BY 1,2,3
UNION 
SELECT lIB_CODE_AM, '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite 
,Min(DATE(date_deb)) AS date_deb_op
,Max(DATE(date_fin)) AS date_fin_op
,Sum(nb_coupon) AS S_coupon
,Sum(Nb_coupon_MAG) AS S_coupon_Mag
,Sum(Nb_coupon_WEB) AS S_coupon_Web
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS nb_coupon_used
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_coupon_used_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN nb_ticket IS NOT NULL then id_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then id_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then id_client end ) AS nb_clt_used_WEB 
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS S_ticket
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_ticket_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_ticket_WEB
,SUM(CASE WHEN nb_ticket IS NOT null THEN sum_remise_xts End) AS remise_xts
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_mnt_ttc End) AS CA_ttc
,SUM(CASE WHEN nb_ticket IS NOT null THEN SUM_QUANTITE_LIGNE End) AS QUANTITE
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_marge_ttc End) AS marge_ttc
,SUM(CASE WHEN nb_ticket IS NOT NULL AND newclt>0 then newclt end ) AS nb_newclt
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_mag>0 then newclt_mag end ) AS nb_newclt_MAG
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_web>0 then newclt_web end ) AS nb_newclt_web
FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_N
GROUP BY 1,2,3)
ORDER BY 1,2,3;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fich_catcoupon_N ORDER BY 1,2,3;


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
where code_am IN ('101623','301906','130146','126861','326910','130147','130148','108250') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1);


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_Nm1 ; 

--- Information ticket Client 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_tick_Nm1 AS 
WITH tab0 AS (SELECT t1.id_org_enseigne AS id_enseigne, t1.id_magasin AS id_mag, t1.code_magasin, t1.code_ticket, t1.code_ligne, t1.type_ligne, t1.dateh_ticket, t1.code_date_ticket, t1.code_caisse, t1.code_reference,  
t1.quantite_ligne, t1.prix_unitaire, t1.montant_ttc, t1.code_AM, t1.montant_remise, t1.MONTANT_MARGE_SORTIE,
CONCAT(t1.id_org_enseigne,'-',t1.id_magasin,'-',t1.code_caisse,'-',t1.code_date_ticket,'-',t1.code_ticket) as id_ticket_lig,
CONCAT(t2.id_org_enseigne,'-',t2.id_magasin,'-',t2.code_caisse,'-',t2.code_date_ticket,'-',t2.code_ticket) as id_ticket_ligt2,
t2.MONTANT AS MONTANT_xts , t2.montant_remise AS remise_xts, t2.NUMERO_OPERATION , Date(t2.dateh_ticket) AS Date_ticket
FROM dhb_prod.hub.F_VTE_TICKET_LIGNE_V2 t1
LEFT JOIN dhb_prod.hub.f_vte_remise_detaillee t2 on t1.id_magasin=t2.id_magasin and t1.code_ligne=t2.code_ligne and t1.code_ticket=t2.code_ticket 
AND id_ticket_lig=id_ticket_ligt2
WHERE DATE(t1.dateh_ticket) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin_Nm1) AND NUMERO_OPERATION IN ('101623','301906','130146','126861','326910','130147','130148','108250')
AND id_ticket_lig=id_ticket_ligt2) 
SELECT * FROM tab0; 

SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_tick_Nm1;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_Nm1 AS 
WITH 
recrut AS (SELECT DISTINCT Code_client, DATE_RECRUTEMENT 
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE ),
tab_vte as (Select code_client,id_org_enseigne, id_magasin, id_ticket, date_ticket as date_ticket_vte 
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
,SUM(CASE WHEN id_ticket IS NOT null THEN QUANTITE_LIGNE End) AS S_QUANTITE_LIGNE
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
tv.code_client AS id_client, S_montant_remise, S_montant_ttc,S_marge_ttc,S_QUANTITE_LIGNE, DATE_RECRUTEMENT  
FROM tab0 a
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c.id_org_enseigne and a.id_mag=c.ID_MAGASIN 
LEFT JOIN tab_vte tv ON a.id_enseigne=tv.id_org_enseigne and a.id_mag=c.ID_MAGASIN AND a.id_ticket_lig=tv.id_ticket AND a.Date_ticket=tv.date_ticket_vte 
LEFT JOIN recrut clt ON tv.CODE_CLIENT = clt.CODE_CLIENT;

--- Stat des ventes et autre 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stab_clt_Nm1 AS 
WITH 
tabtg AS (SELECT DISTINCT CODE_CLIENT , DATE_PARTITION , ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI   
FROM DATA_MESH_PROD_client.SHARED.T_OBT_CLIENT 
WHERE id_niveau=5 AND DATE(DATE_PARTITION) = DATE_FROM_PARTS(YEAR($dtfin_Nm1) , MONTH($dtfin_Nm1), 1) ),
stat_coupons AS (SELECT CODE_CLIENT,code_am, lIB_CODE_AM
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB 
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_Nm1
GROUP BY 1,2,3),
Stat_vtes AS (SELECT id_client,code_op_am
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='MAG' then id_ticket_lig end ) AS nb_ticket_MAG 
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT NULL and canal_mag_achat='WEB' then id_ticket_lig end ) AS nb_ticket_WEB
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_marge_ttc End) AS SUM_marge_ttc
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_QUANTITE_LIGNE End) AS SUM_QUANTITE_LIGNE
,MAX( CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then 1 ELSE 0 end ) AS newclt
,MAX( CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then 1 ELSE 0  end ) AS newclt_web
,MAX( CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then 1 ELSE 0  end ) AS newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick_Nm1
GROUP BY 1,2)
SELECT a.*, b.*, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI ,
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
FROM stat_coupons a 
LEFT JOIN Stat_vtes b ON a.CODE_CLIENT=b.id_client AND a.code_am=b.code_op_am 
LEFT JOIN tabtg g ON a.CODE_CLIENT=g.code_client; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_Nm1;

-- Statistique global

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fich_catcoupon_Nm1 AS 
SELECT * FROM (
SELECT lIB_CODE_AM, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
,Min(DATE(date_deb)) AS date_deb_op
,Max(DATE(date_fin)) AS date_fin_op
,Sum(nb_coupon) AS S_coupon
,Sum(Nb_coupon_MAG) AS S_coupon_Mag
,Sum(Nb_coupon_WEB) AS S_coupon_Web
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS nb_coupon_used
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_coupon_used_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN nb_ticket IS NOT NULL then id_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then id_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then id_client end ) AS nb_clt_used_WEB 
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS S_ticket
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_ticket_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_ticket_WEB
,SUM(CASE WHEN nb_ticket IS NOT null THEN sum_remise_xts End) AS remise_xts
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_mnt_ttc End) AS CA_ttc
,SUM(CASE WHEN nb_ticket IS NOT null THEN SUM_QUANTITE_LIGNE End) AS QUANTITE
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_marge_ttc End) AS marge_ttc
,SUM(CASE WHEN nb_ticket IS NOT NULL AND newclt>0 then newclt end ) AS nb_newclt
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_mag>0 then newclt_mag end ) AS nb_newclt_MAG
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_web>0 then newclt_web end ) AS nb_newclt_web
FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_Nm1
GROUP BY 1,2,3
UNION 
SELECT lIB_CODE_AM, '01_SEGMENT OMNI' AS typo_clt, LIB_SEGMENT_OMNI AS modalite 
,Min(DATE(date_deb)) AS date_deb_op
,Max(DATE(date_fin)) AS date_fin_op
,Sum(nb_coupon) AS S_coupon
,Sum(Nb_coupon_MAG) AS S_coupon_Mag
,Sum(Nb_coupon_WEB) AS S_coupon_Web
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS nb_coupon_used
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_coupon_used_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN nb_ticket IS NOT NULL then id_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then id_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then id_client end ) AS nb_clt_used_WEB 
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS S_ticket
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_ticket_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_ticket_WEB
,SUM(CASE WHEN nb_ticket IS NOT null THEN sum_remise_xts End) AS remise_xts
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_mnt_ttc End) AS CA_ttc
,SUM(CASE WHEN nb_ticket IS NOT null THEN SUM_QUANTITE_LIGNE End) AS QUANTITE
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_marge_ttc End) AS marge_ttc
,SUM(CASE WHEN nb_ticket IS NOT NULL AND newclt>0 then newclt end ) AS nb_newclt
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_mag>0 then newclt_mag end ) AS nb_newclt_MAG
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_web>0 then newclt_web end ) AS nb_newclt_web
FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_Nm1
GROUP BY 1,2,3
UNION 
SELECT lIB_CODE_AM, '02_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite 
,Min(DATE(date_deb)) AS date_deb_op
,Max(DATE(date_fin)) AS date_fin_op
,Sum(nb_coupon) AS S_coupon
,Sum(Nb_coupon_MAG) AS S_coupon_Mag
,Sum(Nb_coupon_WEB) AS S_coupon_Web
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS nb_coupon_used
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_coupon_used_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN nb_ticket IS NOT NULL then id_client end ) AS nb_clt_used
,Count(DISTINCT CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then id_client end ) AS nb_clt_used_MAG 
,Count(DISTINCT CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then id_client end ) AS nb_clt_used_WEB 
,SUM(CASE WHEN nb_ticket IS NOT NULL AND nb_ticket>0 then nb_ticket end ) AS S_ticket
,SUM(CASE WHEN nb_ticket_mag IS NOT NULL AND nb_ticket_mag>0 then nb_ticket_mag end ) AS nb_ticket_MAG
,SUM(CASE WHEN nb_ticket_web IS NOT NULL AND nb_ticket_web>0 then nb_ticket_web end ) AS nb_ticket_WEB
,SUM(CASE WHEN nb_ticket IS NOT null THEN sum_remise_xts End) AS remise_xts
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_mnt_ttc End) AS CA_ttc
,SUM(CASE WHEN nb_ticket IS NOT null THEN SUM_QUANTITE_LIGNE End) AS QUANTITE
,SUM(CASE WHEN nb_ticket IS NOT null THEN Sum_marge_ttc End) AS marge_ttc
,SUM(CASE WHEN nb_ticket IS NOT NULL AND newclt>0 then newclt end ) AS nb_newclt
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_mag>0 then newclt_mag end ) AS nb_newclt_MAG
,SUM( CASE WHEN nb_ticket IS NOT NULL AND newclt_web>0 then newclt_web end ) AS nb_newclt_web
FROM DATA_MESH_PROD_RETAIL.WORK.stab_clt_Nm1
GROUP BY 1,2,3)
ORDER BY 1,2,3;

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fich_catcoupon_Nm1 ORDER BY 1,2,3;


--- Palier de Points 
SELECT FLAG_ACTIF, FLAG_ACTIF_NM1 , FLAG_ACTIF_NM2  FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE ; 



WITH tab0 AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, STATUT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT,LIB_SEGMENT_OMNI, nombre_points_fidelite
--,datediff(YEAR,DATE_DERNIER_ACHAT,$dtfin_frt) AS ACTIF_CLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE 
FLAG_ACTIF=1
OR FLAG_ACTIF_NM1=1
OR FLAG_ACTIF_NM2=1
-- DATE_DERNIER_ACHAT IS NOT NULL AND datediff(MONTH,DATE_DERNIER_ACHAT,$dtfin_frt)<=36
)
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


--- Information sur les jules Club et autres sur 12 24 36 Mois

WITH tab0 AS (SELECT DISTINCT Code_client, DATE_DERNIER_ACHAT, STATUT , ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT,LIB_SEGMENT_OMNI, nombre_points_fidelite
--,datediff(YEAR,DATE_DERNIER_ACHAT,$dtfin_frt) AS ACTIF_CLIENT
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE 
FLAG_ACTIF=1
--OR FLAG_ACTIF_NM1=1
--OR FLAG_ACTIF_NM2=1
-- DATE_DERNIER_ACHAT IS NOT NULL AND datediff(MONTH,DATE_DERNIER_ACHAT,$dtfin_frt)<=36
)
SELECT * FROM (
(SELECT '00-Global' AS Typo, '00-Global' AS modalite,
Count(DISTINCT Code_client) AS nb_client, 
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '01-SEGMENT RFM' AS Typo, CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
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
Count(DISTINCT CASE WHEN Statut='0' THEN Code_client end) AS nb_client_JClub, 
Count(DISTINCT CASE WHEN Statut='1' THEN Code_client end) AS nb_client_JClub_prem
FROM tab0
GROUP BY 1,2)) 
ORDER BY 1,2; 










