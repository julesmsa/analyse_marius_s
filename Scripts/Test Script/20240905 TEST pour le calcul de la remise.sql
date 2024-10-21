-- TEST pour le calcul de la remise 

WITH tickets AS ( 
Select vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,montant_remise_ope_comm,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
CODE_AM,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_brute,     
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    --WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
     --               from DHB_PROD.DNR.DN_PRODUIT
      --              where ID_TYPE_ARTICLE<>1
     --               and id_marque='JUL')
      --  THEN 1         
    ELSE 0 END AS annul_ticket 
from DHB_PROD.DNR.DN_VENTE vd
where YEAR(vd.date_ticket)>=2021  
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  --AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
)
SELECT YEAR(date_ticket) AS  Annee_achat, 
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT END ) AS nb_client_Gbl,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Rem_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise_ope_comm END) AS Rem_ope_comm_Gbl
FROM tickets
WHERE id_ticket='1-363-1-20230930-13273032'
GROUP BY 1
ORDER BY 1 DESC ; 

SELECT * FROM DHB_PROD.DNR.DN_VENTE WHERE prix_init_vente IS NULL AND code_marq='JUL'


SELECT * FROM DHB_PROD.DNR.DN_VENTE 
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (code_pays = $PAYS1 or code_pays = $PAYS2) 
  --AND MONTANT_REMISE >0 AND MONTANT_REMISE_OPE_COMM >0
  AND id_ticket='1-363-1-20230930-13273032'


SELECT * FROM DHB_PROD.DNR.DN_PRODUIT WHERE SKU='3745943'




--- Test sur le recrutement, 

SELECT (YEAR (date_recrutement)*100+ MONTH(date_recrutement)) AS date_recrut,
Count(DISTINCT CODE_Client) AS Nb_client
FROM DHB_PROD.DNR.DN_CLIENT
GROUP BY 1
ORDER BY 1 DESC; 


SELECT YEAR (date_recrutement) AS date_recrut,
Count(DISTINCT CODE_Client) AS Nb_client
FROM DHB_PROD.DNR.DN_CLIENT
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (code_pays = $PAYS1 or code_pays = $PAYS2) AND (CODE_CLIENT IS NOT NULL AND CODE_CLIENT !='0')
GROUP BY 1
ORDER BY 1 DESC; 

SELECT YEAR (date_recrutement) AS date_recrut,
Count(DISTINCT CODE_Client) AS Nb_client
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (code_pays = $PAYS1 or code_pays = $PAYS2) AND (CODE_CLIENT IS NOT NULL AND CODE_CLIENT !='0')
GROUP BY 1
ORDER BY 1 DESC; 



-- Test pour le calcul des indicateurs NO Stress 

-- 30 Septembre au 06 octobre 2024 pour nouvelles mise à jour 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.TAB_TEST_NOSTRESS LIMIT 10;
 
SELECT DISTINCT TRANSACTION_TYPE  FROM DATA_MESH_PROD_CLIENT.WORK.TAB_TEST_NOSTRESS



SELECT * FROM DHB_PROD.DNR.DN_ENTITE ;

SET dtdeb = Date('2024-09-30');
SET dtfin = DAte('2024-10-06');

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA'
SET PAYS2 = 'BEL'; --code_pays = 'BEL'


WITH   Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne, code_pays, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale,  
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE IN  ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN ($PAYS1, $PAYS2) )
SELECT TRANSACTION_TYPE,
count(DISTINCT Store_id) AS nb_mag,
Count(Distinct id) AS nbticket, 
SUM(quantity) AS qte,
Sum(price_eur) AS sum_price,
Sum(valpr_eur) AS sum_valpr,
Sum(marge_eur) AS sum_marge
FROM DATA_MESH_PROD_CLIENT.WORK.TAB_TEST_NOSTRESS a 
INNER JOIN Magasin h ON a.STORE_ID=h.ID_MAGASIN 
GROUP BY 1
; 


 
-- Test sur KPI's CLient Versus NoStress 
-- Test sur 1 mois dhistorique Ventes 
    
SET dtdeb = Date('2024-09-30');
SET dtfin = DAte('2024-10-06');

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA'
SET PAYS2 = 'BEL'; --code_pays = 'BEL'

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml AS
Select  vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise,  MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,   
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
1 AS top_lign,    
CASE WHEN Qte_pos>0 THEN 1 ELSE 0 END AS top_Qte_pos,
CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN 1 ELSE 0 END AS exclu_famill,
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                    from DHB_PROD.DNR.DN_PRODUIT
                    where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
        THEN 1  ELSE 0 END AS annul_ticket        
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1, $ENSEIGNE2)
  and vd.code_pays IN ($PAYS1 , $PAYS2) 
  -- AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  --AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_TICKETS_Sml AS
SELECT * FROM 
(SELECT '01-SANS FILTRE' AS typo
    ,COUNT(DISTINCT id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC_eur) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_mkt 
    ,SUM(remise_totale) AS Mnt_remise_glb 
FROM  DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml
GROUP BY 1
UNION
SELECT '02-NO STRESS' AS typo
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_mkt
,SUM(CASE WHEN annul_ticket=0 THEN remise_totale end) AS Remise_glb
FROM  DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml
GROUP BY 1)
ORDER BY 1,2 ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_TICKETS_Sml ORDER BY 1,2 ;  

 /*
  
SELECT top_lign, top_Qte_pos, exclu_famill, annul_ticket
    ,COUNT( DISTINCT CASE WHEN CODE_CLIENT IS NOT NULL AND CODE_CLIENT !='0' THEN  CODE_CLIENT END) AS nb_clt_distinct
    ,COUNT(DISTINCT id_ticket) AS nb_ticket_glb
    ,SUM(MONTANT_TTC_eur) AS CA_glb
	,SUM(QUANTITE_LIGNE) AS qte_achete_glb
    ,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb 
FROM  DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;
    
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_Sml ORDER BY 1,2,3,4 ;

*/ 


-- Test requete matrice client 

---- autres CA : CA non identifié
-- tickets sans client

/*** Liste des parametres ***/
set dtdeb = date('2024-01-01'); -- début de période de 12 mois qui sert de repère pour réaliser la projection (ici n-1). 
set dtfin = date('2024-09-01'); -- fin de période de 12 mois qui sert de repère pour réaliser la projection
-- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
set ENSEIGNE1 = 1; -- ID_ORG_ENSEIGNE = 1 ou 666
set ENSEIGNE2 = 3; -- ID_ORG_ENSEIGNE = 3 ou 666
set PAYS1 = 'FRA'; --code_pays = 'FRA' ... ou 'lutin'
set PAYS2 = 'BEL'; --code_pays = 'BEL' ... ou 'lutin'
set EMPLACEMENT1 = 'EC'; --type_emplacement = 'EC' ... ou 'lutin'
set EMPLACEMENT2 = 'MP'; --type_emplacement = 'MP' ... ou 'lutin'
set EMPLACEMENT3 = 'PAC'; --type_emplacement = 'PAC' ... ou 'lutin'
set EMPLACEMENT4 = 'CC'; --type_emplacement = 'CC' ... ou 'lutin'
set EMPLACEMENT5 = 'CV'; --type_emplacement = 'CV' ... ou 'lutin'
set EMPLACEMENT6 = 'CCV'; --type_emplacement = 'CCV' ... ou 'lutin'
select $dtdeb , $dtfin, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2, $EMPLACEMENT1, $EMPLACEMENT2, $EMPLACEMENT3, $EMPLACEMENT4, $EMPLACEMENT5, $EMPLACEMENT6;

-- Données de tickets de la période de référence
---- On prend tous les tickets
---- Pas de notion d'actif ici


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.matrice_TICKETS_Sml AS
WITH segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtdeb)
AND (DATE_FIN > DATE($dtdeb) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
base_tickets as (
Select --vd.*
  case when (vd.CODE_CLIENT is null or vd.code_client = '0') then '02-non identifié' 
  WHEN vd.CODE_CLIENT is NOT null AND  vd.code_client != '0' AND (id_type_client IS NOT NULL AND c.id_type_client=1) then '01-Clt identifié' 
  else '03-Clt non segmenté' end as type_ticket,
  vd.CODE_CLIENT,
  vd.id_ticket,
  date(vd.DATEH_TICKET) as DATE_TICKET, -- exclusion des cartes cadeau et microdons (microdon = 6, carte cadeau = 5) ?
  vd.QUANTITE_LIGNE,
  vd.MONTANT_TTC,
  vd.MONTANT_MARGE_SORTIE,
  vd.ID_ORG_ENSEIGNE,
  vd.code_pays,
  vd.type_emplacement,
  vd.ID_MAGASIN,
  c.date_recrutement,c.ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_actuel,
      CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtdeb) AND DATE($dtfin)  THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM,
  SUM(CASE 
        WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') ------- remplacer par vd.code_type_article not in (5,6) or vd.code_type_article is null ?
        THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
  CASE 
        WHEN type_emplacement IN ('EC','MP') AND libelle_type_ligne ='Retour' THEN 1   ------------------------------------------- retour web non pris en compte ?
        WHEN EST_MDON_CKDO=True THEN 1 
        WHEN CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                        from DHB_PROD.DNR.DN_PRODUIT
                        where ID_TYPE_ARTICLE<>1
                        and id_marque='JUL')
        THEN 1  ELSE 0 END AS annul_ticket        

from DHB_PROD.DNR.DN_VENTE vd 
--join DHB_PROD.DNR.DN_ENTITE mag on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT JOIN segrfm g ON vd.code_client=g.code_client
left join DHB_PROD.DNR.DN_CLIENT c
  on vd.CODE_CLIENT = c.code_client
where 1 = 1
  and (date_suppression_client is null or date_suppression_client > $dtfin) ----------------------------------------------- à valider
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  -- Période d'analyse = de la date de segmentation éventuelle à la date de fin
  and $dtdeb <= date(vd.DATEH_TICKET) and $dtfin > date(vd.DATEH_TICKET)
  -- Périmètre magasin au choix
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  and (vd.type_emplacement = $EMPLACEMENT1 or vd.type_emplacement = $EMPLACEMENT2 or vd.type_emplacement = $EMPLACEMENT3 or vd.type_emplacement = $EMPLACEMENT4 or vd.type_emplacement = $EMPLACEMENT5 or vd.type_emplacement = $EMPLACEMENT6)  
        -- mise au périmètre client inconnu
  -- and (vd.CODE_CLIENT is null or vd.code_client = '0') -------------------------------------------------------------------- spécifique à customer forecast
  )
  SELECT * FROM base_tickets ;  
  
  
  SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.matrice_TICKETS_Sml
  WHERE type_ticket='01-Clt identifié' AND  SEGMENT_RFM = '12_NOSEG' AND Type_client='01-Anciens'; 
 
 
 '008820003875'
 
 SELECT *
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE code_client='008820003875'


SELECT * FROM DHB_PROD.DNR.DN_VENTE
 WHERE code_client='008820003875' ; 


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_ENTETE_HISTORIQUE 
 WHERE code_client='008820003875';

SELECT * FROM DHB_PROD.DNR.DN_CLIENT WHERE code_client='008820003875';
 
SELECT type_ticket, SEGMENT_RFM 
,Count(DISTINCT CODE_CLIENT) AS nbclt 
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS nb_ticket_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
,Count(DISTINCT CASE WHEN DATE_RECRUTEMENT BETWEEN $dtdeb AND $dtfin THEN Code_client END) AS nb_newclient 
FROM DATA_MESH_PROD_CLIENT.WORK.matrice_TICKETS_Sml
GROUP BY 1,2 
ORDER BY 1,2 ; 








-- Segmentation RFM des clients Actifs 36 Mois au 1er janvier 

segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtdeb)
AND (DATE_FIN > DATE($dtdeb) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),





---

--- Analyse histo des ventes 

SELECT Min(DATE(DATEH_TICKET)) AS date_min, Max(DATE(DATEH_TICKET)) AS date_max
FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_ENTETE_HISTORIQUE 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_ENTETE_HISTORIQUE ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.test_ACT_CLT_Sml AS
WITH tab0 AS ( SELECT DISTINCT CODE_CLIENT, DATE(DATEH_TICKET) AS date_ticket   
FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_ENTETE_HISTORIQUE  
where ID_ORG_ENSEIGNE IN (1,3) AND code_client IS NOT NULL AND code_client !='0'  ),
delai_achat AS ( SELECT *, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC) AS lag_date_ticket,
  	DATEDIFF(month, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC),date_ticket) as DELAI_DERNIER_ACHAT
  	FROM tab0),
ACTIVITE_CLT AS (
  SELECT * , 
		CASE 
		WHEN DELAI_DERNIER_ACHAT <=12 THEN 'ACTIF 12MR' 
		WHEN DELAI_DERNIER_ACHAT >12 THEN 'REACTIVATION APRES 12MR' 
		WHEN DELAI_DERNIER_ACHAT IS NULL THEN 'NOUVEAU'
	ELSE 'ND' 
	END AS ACTIVITE_CLT
	FROM delai_achat)
SELECT a.* , b.date_recrutement, 
DATE(date_premier_achat) AS dt_premier_achat, Date(date_dernier_achat) AS dt_dernier_achat
FROM ACTIVITE_CLT a
LEFT JOIN DHB_PROD.DNR.DN_CLIENT b ON a.code_client=b.code_client; 


SELECT * FROM DHB_PROD.DNR.DN_CLIENT; 
--WHERE code_client='008820003875'  

WITH tab0 AS (SELECT *, CASE WHEN ACTIVITE_CLT IN ('NOUVEAU','REACTIVATION APRES 12MR') THEN date_ticket END AS date_new
FROM DATA_MESH_PROD_CLIENT.WORK.test_ACT_CLT_Sml 
ORDER BY 1,2) 
SELECT *, 
  MAX(date_new) OVER (PARTITION BY code_client) AS Date_recrut_V0
FROM tab0 
ORDER BY 1,2


WITH delai_achat AS ( SELECT *, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC) AS lag_date_ticket,
  	DATEDIFF(month, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC),date_ticket) as DELAI_DERNIER_ACHAT
  	FROM (
	SELECT DISTINCT
		vd.CODE_CLIENT,
		vd.date_ticket 
		from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
where vd.ID_ORG_ENSEIGNE IN (1,3))),
ACTIVITE_CLT AS (
  SELECT * , 
		CASE 
		WHEN DELAI_DERNIER_ACHAT <=12 THEN 'ACTIF 12MR' 
		WHEN DELAI_DERNIER_ACHAT >12 THEN 'REACTIVATION APRES 12MR' 
		WHEN DELAI_DERNIER_ACHAT IS NULL THEN 'NOUVEAU'
	ELSE 'ND' 
	END AS ACTIVITE_CLT
	FROM delai_achat),
	
	
	





