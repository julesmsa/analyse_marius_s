/**** Analyse campagne Marketing 15€ des 80€ d'achat  ***/

-- Parametre des dates 

SET dtdeb = DAte('2024-11-01'); -- to_date(dateadd('year', -1, $dtfin)); 
SET dtfin = DAte('2024-11-03'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Cible_campagne AS
WITH 
broadlogs as (
select Distinct 
    m.CODE_CLIENT, 
    initcap(m.STATUS) as STATUS,
    m.CODE_MESSAGE, -- id du log
    m.DATEH_EVENEMENT,
    date(m.DATEH_EVENEMENT) as DATE_EVENEMENT, 
    m.CODE_ACTIVATION, -- id de la diffusion
    case when UPPER(m.CODE_ACTIVATION) like '%\_EMAIL\_%' then 'EMAIL' 
        when UPPER(m.CODE_ACTIVATION) like '%\_SMS\_%' then 'SMS'
        when m.ADRESSE like '%@%' then 'EMAIL'
      end as CANAL,
    rank() over (partition by m.CODE_ACTIVATION, m.CODE_CLIENT order by m.DATEH_EVENEMENT desc) as RANG 
from DHB_PROD.HUB.F_CLI_COM_HISTO_MESSAGE_JUL m
where code_appli_source = 'IGO'
AND	 lower(m.code_activation) not like '%test%' and lower(m.code_activation) not like 'bat%' 
and CODE_ACTIVATION IN ('FR_EMAIL_PRODUIT_GPIECES_011124', 'BEFR_EMAIL_PRODUIT_GPIECES011124', 'BENL_EMAIL_PRODUIT_GPIECES011124' )
and DATE(DATEH_EVENEMENT)= DATE($dtdeb)
and initcap(m.PURPOSE) = 'Marketing' AND initcap(STATUS)='Delivered'
),
info_clt AS (
    SELECT DISTINCT Code_client AS idclt    
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_pays IN  ($PAYS1 ,$PAYS2) AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL )
SELECT DISTINCT  CODE_CLIENT AS CODE_CLIENT_CRM, STATUS, CODE_MESSAGE, DATE_EVENEMENT, CODE_ACTIVATION, CANAL, RANG
from broadlogs a 
INNER JOIN info_clt b ON a.Code_client=b.idclt; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_Cible_campagne;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_action_campagne AS
WITH tracking AS 
( SELECT t.CODE_CLIENT,
		t.CODE_MESSAGE as PK_DELIVERY_TRACKING_LOGS, -- id de tracking
		case when url like '%desabonnement%' or url like '%unsubscribe%' then 3 -- desabonnement
            when LOWER(t.TYPE) = 'open' then 2                                  -- ouverture
            when LOWER(t.TYPE) = 'click' then 1                                 -- clic
            --when LOWER(t.TYPE) = 'Page miroir' then 6                           -- le cas n'est plus identifié aujourd'hui. Conservé pour mémoire
            end as CODE_ACTION,
        case when url like '%desabonnement%' or url like '%unsubscribe%' then 'desabonnement'
            else LOWER(t.TYPE)
            end as ACTION_t,
		t.CODE_ACTIVATION,
		date(t.DATEH_EVENEMENT) as DATE_EVENEMENT,
		t.DATEH_EVENEMENT           
from	DHB_PROD.HUB.F_CLI_COM_HISTO_TRACKING_JUL  t
where	CODE_APPLI_SOURCE = 'IGO' and CODE_ACTIVATION LIKE '%GPIECES_011124%' )
SELECT CODE_CLIENT 
,MIN(DATE_EVENEMENT) AS DATE_EVENT_ACTION
,MAX( CASE WHEN ACTION_t='open' THEN 1 ELSE 0 END ) AS top_open
,MAX( CASE WHEN ACTION_t='click' THEN 1 ELSE 0 END ) AS top_click
,MAX( CASE WHEN ACTION_t='desabonnement' THEN 1 ELSE 0 END ) AS top_desabo
FROM tracking
GROUP BY 1 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_action_campagne;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabglb_campagne AS
SELECT a.*, DATE_EVENT_ACTION, top_open, top_click, top_desabo
FROM DATA_MESH_PROD_CLIENT.WORK.tab_Cible_campagne a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_action_campagne b ON a.CODE_CLIENT_CRM = b.CODE_CLIENT ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tabglb_campagne;

/**** Statistique de ventes et autres email ***/ 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_campagne AS
SELECT CANAL, CODE_ACTIVATION, 
Count(DISTINCT CODE_CLIENT_CRM) AS Nb_client,
Count(DISTINCT CASE WHEN top_open=1 THEN CODE_CLIENT_CRM END ) AS Nb_client_open,
Count(DISTINCT CASE WHEN top_click=1 THEN CODE_CLIENT_CRM END ) AS Nb_client_click,
Count(DISTINCT CASE WHEN top_desabo=1 THEN CODE_CLIENT_CRM END ) AS Nb_client_desabo
FROM DATA_MESH_PROD_CLIENT.WORK.tabglb_campagne
GROUP BY 1,2
ORDER BY 1,2 ; 

SELECT * FROM DHB_PROD.HUB.D_ACTION_MARKETING
order by DATEH_CREATION_AM  DESC


SELECT * 
from DHB_PROD.DNR.DN_VENTE vd
WHERE CODE_AM = 150426 

SELECT count(DISTINCT ) FROM DHB_PROD.HUB.F_VTE_REMISE_DETAILLEE 
WHERE NUMERO_OPERATION = 150426 


/*** Analyse des tickets sur les deux premiers jour  */

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Campagne AS
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
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN MONTANT_TTC END ) OVER (PARTITION BY id_ticket) AS Mntttc_ticket,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN vd.montant_remise END ) OVER (PARTITION BY id_ticket) AS Mntremise_ticket,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN vd.MONTANT_REMISE_OPE_COMM END ) OVER (PARTITION BY id_ticket) AS REM_OPECOMM_ticket,
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
where vd.date_ticket BETWEEN Date($dtdeb) AND DATE($dtfin)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0'),
  rem_am AS (SELECT DISTINCT  CONCAT(id_org_enseigne,'-',id_magasin,'-',code_caisse,'-',code_date_ticket,'-',code_ticket)  as cde_id_ticket,
  1 AS tag_op
FROM dhb_prod.hub.f_vte_remise_detaillee
where NUMERO_OPERATION in ( 150423, 150426))
SELECT a.*, b.* 
FROM tickets a 
LEFT JOIN rem_am b ON a.id_ticket=b.cde_id_ticket;  

--SELECT tag_op, count(DISTINCT code_client) AS nbclt, count(DISTINCT id_ticket) AS nbidticket, 
---FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Campagne
--WHERE Qte_pos>0
--GROUP BY 1

-- 42593 clients 

-- SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Campagne WHERE code_client='019010010166' AND id_ticket='1-471-1-20241102-14307004';

/**** Regroupement de toute les informations ***/

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne AS
WITH segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin) 
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt    
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_pays IN  ($PAYS1 ,$PAYS2) AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL ),
ghclt AS (SELECT a.*, b.* , 
COALESCE(CODE_CLIENT_CRM,CODE_CLIENT) AS ID_CLIENT,
CASE WHEN CODE_CLIENT_CRM IS NOT NULL AND CODE_ACTIVATION IS NOT NULL THEN '01-Client_Ciblés' ELSE '02-Client_NonCiblés' END AS typecrm_clt
FROM DATA_MESH_PROD_CLIENT.WORK.tabglb_campagne a
FULL JOIN DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_Campagne b ON a.CODE_CLIENT_CRM=b.CODE_CLIENT)
SELECT a.*, 
b.*,  ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI
,datediff(MONTH ,date_recrutement,$dtfin) AS ANCIENNETE_CLIENT
,CASE WHEN Date(date_recrutement) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN '02-Nouveaux' ELSE '01-Anciens' END AS Type_client
,ROUND(DATEDIFF(YEAR,date_naissance,$dtfin),2) AS AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
	 , CASE WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('75','77','78','91','92','93','94','95')             then  '01_Ile de France' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('02', '59', '60', '62', '80')                                      then  '02_Hauts-de-France'
              WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('18', '28', '36', '37', '41', '45' )       						then  '03_Centre-Val de Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('14', '27', '50', '61', '76')                                      then  '04_Normandie'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('44', '49', '53', '72', '85')                                      then  '05_Pays de la Loire'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('22','29','35','56')       						then  '06_Bretagne'
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87')                                      then  '07_Nouvelle-Aquitaine'	
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('08', '10', '51', '52', '54', '55', '57', '67', '68', '88')					then  '08_Grand Est'
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('01','03','07','15','26','38','42','43','63','69','73','74' ) then  '09_Auvergne-Rhone-Alpes' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('21', '25', '39','58','70','71', '89', '90' ) then  '10_Bourgogne-Franche-Comte' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82' )                                      then  '11_Occitanie' 
		      WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('04', '05', '06', '13', '83', '84')                                      then  '12_Provence-Alpes-Cote-d-Azur' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 2) in ('20')											then  '13_Corse' 
			  WHEN pays_clt='FRA' AND SUBSTRING(code_postal, 1, 3) in ('971','972','973','974','975','976','986','987','988') then  '14_Outre-mer' 
			WHEN pays_clt='FRA' AND code_postal = '98000' 	then  '15_Monaco'
			WHEN pays_clt='BEL'  	then  '20_BELGIQUE'
			ELSE '99_AUTRES/NC' END AS REGION -- a completer avec les informations de la BEL     
,CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
     WHEN id_macro_segment = '02' THEN '02_TBC'
     WHEN id_macro_segment = '03' THEN '03_BC'
     WHEN id_macro_segment = '04' THEN '04_MOY'
     WHEN id_macro_segment = '05' THEN '05_TAP'
     WHEN id_macro_segment = '06' THEN '06_TIEDE'
     WHEN id_macro_segment = '07' THEN '07_TPURG'
     WHEN id_macro_segment = '09' THEN '08_NCV'
     WHEN id_macro_segment = '08' THEN '09_NAC'
     -- WHEN id_macro_segment = '10' THEN '10_INA12'
     -- WHEN id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM  -- On estime que tous les clients actifs ont effectué au moins un achat et sont donc segmentés    
,CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     -- WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' -- On estime que tous les clients actifs ont effectué au moins un achat et sont donc segmentés 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM
,CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'a: [0-12] mois'  END  AS Tr_anciennete, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2,
CASE WHEN nb_lign=1 THEN anciennete_client END AS anc_clt_C2,
CASE WHEN cde_id_ticket IS NOT NULL AND tag_op=1 THEN '01-ticket_80euros' ELSE '02-ticket_others' END AS typo_ticket
--CASE WHEN Mntttc_ticket + Mntremise_ticket >=80 THEN '01-ticket_80euros' ELSE '02-ticket_others' END AS typo_ticket
FROM ghclt a
INNER JOIN info_clt b ON a.ID_CLIENT=b.idclt
LEFT JOIN segrfm c ON a.ID_CLIENT=c.code_client
LEFT JOIN segomni e ON a.ID_CLIENT=e.code_client;  

--SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
--WHERE (Mntttc_ticket + Mntremise_ticket) >=80 AND tag_op IS NULL ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_campagne AS
SELECT * FROM (
SELECT typecrm_clt, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENRE='H' THEN '01-Hommes'
 WHEN GENRE='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite 
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '04_SEGMENT_RFM' AS typo_clt,  SEGMENT_RFM AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '05_OMNICANALITE' AS typo_clt, 
 CASE WHEN lib_segment_omni ='MAG' then '01-MAG' 
      When lib_segment_omni ='WEB' then '02-WEB'
      When lib_segment_omni ='OMNI' then '03-OMNI' ELSE '99-NON RENSEIGNE' end as modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '06_CANAL_ACHAT' AS typo_clt, PERIMETRE AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '07A_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION 
SELECT typecrm_clt, '07B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite
,AVG(anc_clt_C2) AS Nb_client
,AVG( CASE WHEN top_open=1 THEN anc_clt_C2 END ) AS Nb_client_open
,AVG( CASE WHEN top_click=1 THEN anc_clt_C2 END ) AS Nb_client_click
,AVG( CASE WHEN top_desabo=1 THEN anc_clt_C2 END ) AS Nb_client_desabo
,AVG(  CASE WHEN Qte_pos>0 THEN anc_clt_C2 end) AS nb_clt_actif
,AVG(  CASE WHEN Qte_pos>0 THEN anc_clt_C2 end) AS nb_ticket_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN anc_clt_C2 end) AS CA_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN anc_clt_C2 end) AS qte_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN anc_clt_C2 end) AS Marge_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN anc_clt_C2 end) AS Mntrem_actif
,AVG( CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then anc_clt_C2 end) AS nb_newclt
,AVG(  CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN anc_clt_C2 end) AS nb_clt_80e
,AVG(  CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN anc_clt_C2 end) AS nb_ticket_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN anc_clt_C2 end) AS CA_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN anc_clt_C2 end) AS qte_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN anc_clt_C2 end) AS Marge_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN anc_clt_C2 end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '08A_AGE' AS typo_clt,  CASE WHEN CLASSE_AGE IN ('80-84','85-89','90-94','95-99') THEN '80 et +' ELSE CLASSE_AGE END AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '08B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite
,AVG(AGE_C2) AS Nb_client
,AVG( CASE WHEN top_open=1 THEN AGE_C2 END ) AS Nb_client_open
,AVG( CASE WHEN top_click=1 THEN AGE_C2 END ) AS Nb_client_click
,AVG( CASE WHEN top_desabo=1 THEN AGE_C2 END ) AS Nb_client_desabo
,AVG(  CASE WHEN Qte_pos>0 THEN AGE_C2 end) AS nb_clt_actif
,AVG(  CASE WHEN Qte_pos>0 THEN AGE_C2 end) AS nb_ticket_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN AGE_C2 end) AS CA_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN AGE_C2 end) AS qte_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN AGE_C2 end) AS Marge_actif
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN AGE_C2 end) AS Mntrem_actif
,AVG( CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then AGE_C2 end) AS nb_newclt
,AVG(  CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN AGE_C2 end) AS nb_clt_80e
,AVG(  CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN AGE_C2 end) AS nb_ticket_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN AGE_C2 end) AS CA_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN AGE_C2 end) AS qte_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN AGE_C2 end) AS Marge_80e
,AVG(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN AGE_C2 end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '09_OPTIN_SMS' AS typo_clt,  CASE WHEN est_optin_sms_com=1 or est_optin_sms_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '10_OPTIN_EMAIL' AS typo_clt,  CASE WHEN est_optin_email_com=1 or est_optin_email_fid=1 THEN '01_OUI' ELSE '02_NON' END AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '22_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '23_FAMILLE_PRODUITS' AS typo_clt,  
 CASE WHEN LIB_FAMILLE_ACHAT IS NULL OR LIB_FAMILLE_ACHAT='Marketing' THEN 'Z-NC/NR' ELSE LIB_FAMILLE_ACHAT END AS modalite 
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '24_REGION' AS typo_clt,  REGION AS modalite 
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
UNION
SELECT typecrm_clt, '25_PAYS' AS typo_clt,  pays_clt AS modalite 
,COUNT(DISTINCT ID_CLIENT) AS Nb_client
,COUNT(DISTINCT CASE WHEN top_open=1 THEN ID_CLIENT END ) AS Nb_client_open
,COUNT(DISTINCT CASE WHEN top_click=1 THEN ID_CLIENT END ) AS Nb_client_click
,COUNT(DISTINCT CASE WHEN top_desabo=1 THEN ID_CLIENT END ) AS Nb_client_desabo
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT end) AS nb_clt_actif
,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_TTC end) AS CA_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_actif
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 THEN montant_remise end) AS Mntrem_actif
,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) and Qte_pos>0 then CODE_CLIENT end) AS nb_newclt
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN CODE_CLIENT end) AS nb_clt_80e
,COUNT( DISTINCT CASE WHEN Qte_pos>0 AND typo_ticket='01-ticket_80euros' THEN id_ticket end) AS nb_ticket_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_TTC end) AS CA_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN QUANTITE_LIGNE end) AS qte_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN MONTANT_MARGE_SORTIE end) AS Marge_80e
,SUM(CASE WHEN Qte_pos>0 AND annul_ticket=0 AND typo_ticket='01-ticket_80euros' THEN montant_remise end) AS Mntrem_80e
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_GLOBALE_Campagne
GROUP BY 1,2,3
)
ORDER BY 1,2,3 ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_campagne ORDER BY 1,2,3 ; 


/*
 * 
-- ticket du client pour le 02 novembre 
Select * FROM DHB_PROD.DNR.DN_VENTE
where id_ticket='1-109-1-20241102-14307133'

-- le client est ciblé 
select Distinct 
    m.CODE_CLIENT, 
    initcap(m.STATUS) as STATUS,
    m.CODE_MESSAGE, -- id du log
    m.DATEH_EVENEMENT,
    date(m.DATEH_EVENEMENT) as DATE_EVENEMENT, 
    m.CODE_ACTIVATION, -- id de la diffusion
    case when UPPER(m.CODE_ACTIVATION) like '%\_EMAIL\_%' then 'EMAIL' 
        when UPPER(m.CODE_ACTIVATION) like '%\_SMS\_%' then 'SMS'
        when m.ADRESSE like '%@%' then 'EMAIL'
      end as CANAL,
    rank() over (partition by m.CODE_ACTIVATION, m.CODE_CLIENT order by m.DATEH_EVENEMENT desc) as RANG 
from DHB_PROD.HUB.F_CLI_COM_HISTO_MESSAGE_JUL m
where code_appli_source = 'IGO'
AND	 lower(m.code_activation) not like '%test%' and lower(m.code_activation) not like 'bat%' 
and CODE_ACTIVATION IN ('FR_EMAIL_PRODUIT_GPIECES_011124', 'BEFR_EMAIL_PRODUIT_GPIECES011124', 'BENL_EMAIL_PRODUIT_GPIECES011124' )
and code_client='048310007662' 



SELECT * FROM DHB_PROD.HUB.D_ACTION_MARKETING
order by DATEH_CREATION_AM  desc

CODE_AM = 150426 

Select Date_ticket, count(distinct id_ticket) as nb_ticket  
from DHB_PROD.DNR.DN_VENTE vd
where CODE_AM = 150423
group by 1

codeactionmarketing_coupon1=150423 -- 150426


SELECT NUMERO_OPERATION, 
Min(date(dateh_ticket)) as min_date, 
Max(date(dateh_ticket)) as max_date, 
count( distinct CONCAT(id_org_enseigne,'-',id_magasin,'-',code_caisse,'-',code_date_ticket,'-',code_ticket) ) as nb_id_ticket, 
SUM(Montant_remise) AS remise_VBNUM2
FROM dhb_prod.hub.f_vte_remise_detaillee
where NUMERO_OPERATION in ( 150423, 150426)
group by 1
