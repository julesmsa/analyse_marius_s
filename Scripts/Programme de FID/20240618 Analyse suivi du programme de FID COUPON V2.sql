
 -- Nouvelles vision avec données en provenance de BO 

Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)

SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN WHERE  ID_ORG_ENSEIGNE AND ID_MAGASIN=393


-- créer une  table avec l'ensemble des coupons 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon AS 
Select distinct a.code_coupon, a.date_debut_validite, a.date_fin_validite, a.code_magasin as codemag_coupon, a.code_client, a.valeur, a.code_am, a.code_status, a.description_longue, a.description_courte, type_emplacement as type_empl_coupon, lib_magasin as libmag_coupon, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_Crea_coupon,
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM , DATE_RECRUTEMENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT
From DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE a
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.code_magasin=b.ID_MAGASIN 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE c ON a.CODE_CLIENT = c.CODE_CLIENT
where code_am IN ('101623','301906','130146','130147','130148','130145') AND YEAR(date_debut_validite)>=2024 and DATE(date_debut_validite)<'2024-06-01';



SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon ; 

/*
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON t1.id_org_enseigne=c.id_org_enseigne and t1.id_magasin=c.ID_MAGASIN 
,type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_mag_achat
*/

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_tick AS 
WITH tab0 AS (SELECT t1.id_org_enseigne AS id_enseigne, t1.id_magasin AS id_mag, t1.code_magasin, t1.code_ticket, t1.code_ligne, t1.type_ligne, t1.dateh_ticket, t1.code_date_ticket, t1.code_caisse, t1.code_reference,  
t1.quantite_ligne, t1.prix_unitaire, t1.montant_ttc, t1.code_AM, t1.montant_remise,
CONCAT(t1.id_org_enseigne,'-',t1.id_magasin,'-',t1.code_caisse,'-',t1.code_date_ticket,'-',t1.code_ticket) as id_ticket_lig,
t2.MONTANT AS MONTANT_xts , t2.montant_remise AS remise_xts, t2.NUMERO_OPERATION , Date(t2.dateh_ticket) AS Date_ticket
FROM dhb_prod.hub.F_VTE_TICKET_LIGNE_V2 t1
LEFT JOIN dhb_prod.hub.f_vte_remise_detaillee t2 on t1.id_magasin=t2.id_magasin and t1.code_ligne=t2.code_ligne and t1.code_ticket=t2.code_ticket
WHERE YEAR(t1.dateh_ticket)>=2024 and DATE(t1.dateh_ticket)<'2024-06-01' AND NUMERO_OPERATION IN ('101623','301906','130146','130147','130148','130145')) 
SELECT * FROM tab0 ; 

SELECT * FROM  DATA_MESH_PROD_RETAIL.WORK.tab_tick;



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick AS 
WITH 
tab_vte as (Select code_client,id_org_enseigne, id_magasin, id_ticket, date_ticket as date_ticket_vte 
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE
where YEAR(date_ticket)>=2024 and DATE(date_ticket)<'2024-06-01' and code_client is not null and code_client !='0'
group by 1,2,3,4,5),
tab0 AS (
SELECT id_enseigne, id_mag, id_ticket_lig, NUMERO_OPERATION, Date_ticket, CAST (NUMERO_OPERATION AS VARCHAR) AS code_op_am,
SUM(remise_xts) AS S_remise_xts
FROM DATA_MESH_PROD_RETAIL.WORK.tab_tick
GROUP BY 1,2,3,4,5)
SELECT a.* ,type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_mag_achat, 
tv.code_client AS id_client, S_montant_remise, S_montant_ttc
FROM tab0 a
LEFT join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c.id_org_enseigne and a.id_mag=c.ID_MAGASIN 
LEFT JOIN tab_vte tv ON a.id_enseigne=tv.id_org_enseigne and a.id_mag=c.ID_MAGASIN AND a.id_ticket_lig=tv.id_ticket AND a.Date_ticket=tv.date_ticket_vte ; 


SELECT *FROM  DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick ;
SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon AS 
Select a.* , b.*
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.stat_tab_tick b ON a.code_client=b.id_client and a.code_am=b.code_op_am ; 

SELECT *FROM  DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon ;



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon AS 
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
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon
GROUP BY 1,2,3,4)
UNION ALL
(SELECT '01_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite, code_am , DESCRIPTION_COURTE 
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
,Count(DISTINCT CASE WHEN id_ticket_lig IS NOT null THEN id_ticket_lig End) AS Nb_ticket
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN s_remise_xts End) AS SUM_s_remise_xts
,SUM(CASE WHEN id_ticket_lig IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket IS NOT NULL AND DATE_RECRUTEMENT=date_ticket AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon ORDER BY 1,2,3,4; 


----------------------------------- fin mise à jour des données --------------------------------------------------------------------; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_used_coupon AS 
WITH tab_vte as (Select code_client,id_org_enseigne, id_magasin, code_caisse,code_ticket,
code_date_ticket, id_ticket, date_ticket as date_ticket_vte 
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_remise End) AS S_montant_remise
,SUM(CASE WHEN id_ticket IS NOT null THEN montant_ttc End) AS S_montant_ttc
FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE
where YEAR(date_ticket)>=2024 and DATE(date_ticket)<'2024-06-01' and code_client is not null and code_client !='0'
group by 1,2,3,4,5,6,7,8),
mnt_rem as ( Select Codemagasin, codecaisse, numticket, numoperation,DATE(jourdevente) as date_jourdevente,
SUM(montant) as mnt_produit, 
SUM(montantremise) as mnt_remise
FROM dhb_prod.acq.stl_remise_detaillees
where numoperation IN ('101623','301906','130146','130147','130148','130145')
AND YEAR(jourdevente)>=2024 and DATE(jourdevente)<'2024-06-01'
group by 1,2,3,4,5 ),
id_ticket_BO as (Select distinct id_enseigne, id_magasin,num_caisse,num_ticket, num_client,date_ticket as date_ticket_BO, CONCAT(id_enseigne,'-',id_magasin,'-',num_caisse,'-',date_ticket,'-',num_ticket) as id_ticket_snow,
num_am_caisse, lib_am_caisse_1, lib_am_caisse_2, cat_am_caisse, num_coupon_vente
FROM DATA_MESH_PROD_CLIENT.WORK.TEMP_COUPON_AM_FID 
where num_am_caisse IN ('101623','301906','130146','130147','130148','130145') ),
tab0 as (
Select a.*, b.date_ticket_vte, b.S_montant_remise,b.S_montant_ttc,type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_mag_achat
From id_ticket_BO a
Left join tab_vte b ON a.num_client=b.code_client and a.id_ticket_snow=b.id_ticket  
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c. id_org_enseigne and a.id_magasin=c.ID_MAGASIN) 
Select a.*, 
f.numoperation, mnt_produit, mnt_remise 
FROM tab0 a 
LEFT JOIN mnt_rem f ON a.id_magasin=f.Codemagasin and a.num_caisse=f.codecaisse   and a.num_ticket=f.numticket and a.num_am_caisse=f.numoperation and a.date_ticket_vte=f.date_jourdevente;


Select * From DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon;

Select * FROM DATA_MESH_PROD_RETAIL.WORK.tab_used_coupon;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon AS 
Select a.* , b.*
FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_used_coupon b ON a.code_client=b.Num_client and a.code_am=b.num_am_caisse ; 

Select * FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon;



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon AS 
SELECT * FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL then code_client end ) AS nb_clt_used 
,Count(DISTINCT CASE WHEN id_ticket_snow IS NOT null THEN id_ticket_snow End) AS Nb_ticket
,SUM(CASE WHEN id_ticket_snow IS NOT null THEN mnt_remise End) AS SUM_mnt_remise
,SUM(CASE WHEN id_ticket_snow IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL AND DATE_RECRUTEMENT=date_ticket_vte then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL AND DATE_RECRUTEMENT=date_ticket_vte AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL AND DATE_RECRUTEMENT=date_ticket_vte AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon
GROUP BY 1,2,3,4)
UNION ALL
(SELECT '01_SEGMENT RFM' AS typo_clt, SEGMENT_RFM AS modalite, code_am , DESCRIPTION_COURTE 
,Min(DATE(date_debut_validite)) AS date_deb
,Max(DATE(date_fin_validite)) AS date_fin
,Count(DISTINCT Code_coupon) AS nb_coupon 
,Count(DISTINCT CASE WHEN canal_crea_coupon='MAG' THEN Code_coupon End) AS Nb_coupon_MAG
,Count(DISTINCT CASE WHEN canal_crea_coupon='WEB' THEN Code_coupon End) AS Nb_coupon_WEB
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL then Code_coupon end ) AS nb_coupon_used
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL and canal_mag_achat='MAG' then Code_coupon end ) AS nb_coupon_used_MAG 
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL and canal_mag_achat='WEB' then Code_coupon end ) AS nb_coupon_used_WEB
,Count(DISTINCT code_client) AS nb_clt
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL then code_client end ) AS nb_clt_used 
,Count(DISTINCT CASE WHEN id_ticket_snow IS NOT null THEN id_ticket_snow End) AS Nb_ticket
,SUM(CASE WHEN id_ticket_snow IS NOT null THEN mnt_remise End) AS SUM_mnt_remise
,SUM(CASE WHEN id_ticket_snow IS NOT null THEN S_montant_ttc End) AS SUM_mnt_ttc
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL AND DATE_RECRUTEMENT=date_ticket_vte then code_client end ) AS nb_newclt
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL AND DATE_RECRUTEMENT=date_ticket_vte AND canal_mag_achat='WEB' then code_client end ) AS nb_newclt_web
,Count(DISTINCT CASE WHEN date_ticket_vte IS NOT NULL AND DATE_RECRUTEMENT=date_ticket_vte AND canal_mag_achat='MAG' then code_client end ) AS nb_newclt_mag
FROM DATA_MESH_PROD_RETAIL.WORK.tab_fin_coupon
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4);



SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_fin_coupon ORDER BY 1,2,3,4; 




mnt_remise as ( Select Codemagasin, codecaisse, numticket, numoperation,DATE(jourdevente) as date_ticket,
SUM(montant) as mnt_produit, 
SUM(montantremise) as mnt_remise
FROM dhb_prod.acq.stl_remise_detaillees
where numoperation IN ('101623','301906','130146','130147','130148','130145')
AND YEAR(jourdevente)>=2024 and DATE(jourdevente)<'2024-06-01'
group by 1,2,3,4,5 )



with id_ticket_BO as (Select distinct a.id_enseigne, a.id_magasin,num_caisse,num_ticket, num_client,date_ticket as date_ticket_BO, CONCAT(id_enseigne,'-',a.id_magasin,'-',num_caisse,'-',date_ticket,'-',num_ticket) as id_ticket_snow,
num_am_caisse, lib_am_caisse_1, lib_am_caisse_2, cat_am_caisse, num_coupon_vente,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal
FROM DATA_MESH_PROD_CLIENT.WORK.TEMP_COUPON_AM_FID a
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN c ON a.id_enseigne=c. id_org_enseigne and a.id_magasin=c.ID_MAGASIN
where num_am_caisse IN ('101623','301906','130146','130147','130148','130145') )
Select Canal,num_am_caisse, Count(Distinct num_coupon_vente) as nbvte
From id_ticket_BO
group by 1,2
order by 1,2 ; 




remise_v2 as ( Select distinct codemagasin, DATE(jourdevente) as dateticket , codeclient, codecaisse, 
canetrealise, mtremise, codesaisie, codeactionmarketing, 

where type_ligne=6 )

select distinct typeligne from dhb_prod.acq.stl_historique_caisses_v2


select codemagasin, codecaisse, numticket,DATE(jourdevente) as dateticket
Sum(montan) as s_mnt, Sum(montantremise) as s_mntremise
from dhb_prod.acq.stl_remise_detaillees 
where numoperation IN ('101623','301906','130146','130147','130148','130145') 
and 






select * from dhb_prod.acq.stl_remise_detaillees 

select * from dhb_prod.acq.stl_remise_detaillees where numticket='14104095' and codemagasin = 13

select * from dhb_prod.acq.stl_historique_caisses_v2 where numticket='14104095' and codemagasin = 13  


select * from dhb_prod.acq.stl_remise_detaillees where numticket='14119057' and codemagasin = 15




SELECT *, CONCAT(id_enseigne,'-',id_magasin,'-',num_caisse,'-',date_ticket,'-',num_ticket) as id_ticket_snow
FROM DATA_MESH_PROD_CLIENT.WORK.TEMP_COUPON_AM_FID ;


Select distinct id_enseigne, id_magasin, date_ticket, CONCAT(id_enseigne,'-',id_magasin,'-',num_caisse,'-',date_ticket,'-',num_ticket) as id_ticket_snow,
num_am_caisse, lib_am_caisse_1, lib_am_caisse_2, cat_am_caisse, num_coupon_vente
FROM DATA_MESH_PROD_CLIENT.WORK.TEMP_COUPON_AM_FID ;

  SELECT * FROM DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE 
  WHERE CODE_AM IN ('130147','130148'); 
 
  SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE 
  WHERE CODE_AM IN ('130147','130148');  

   SELECT * FROM DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE  
    WHERE CODE_AM IN ('130147','130148') AND CODE_STATUS='U' ; 


select * from dhb_prod.hub.F_VTE_TICKET_LIGNE_V2 t1
inner join dhb_prod.hub.f_vte_remise_detaillee t2 on t1.id_magasin=t2.id_magasin and t1.code_ligne=t2.code_ligne
and t1.code_ticket=t2.code_ticket---set dateh_maj=current_timestamp
where t2.code_ticket=4703104---24034010
and t2.id_magasin=600;




  