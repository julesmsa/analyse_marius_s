--- Mise en place tableau des deciles Clients Jules

  -- SCRIPT POUR L'ANNEE EN COURS 

SET dtfin = DAte('2024-09-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 
SET dtfin_Nm2 = to_date(dateadd('year', -2, $dtfin)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtfin, $dtfin_Nm1, $dtfin_Nm2, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

/*** Liste des codes AM à catégoriser 
SELECT * FROM   DATA_MESH_PROD_RETAIL.WORK.LISTE_CODE_AM_JULES where CODE_AM='108250'
LIMIT
  10;
***/ 

-- traitement des CODE_AM

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise_CODEAM AS 
WITH tab_rem AS (SELECT CONCAT(t2.id_org_enseigne,'-',t2.id_magasin,'-',t2.code_caisse,'-',t2.code_date_ticket,'-',t2.code_ticket) as id_ticket_lign,
CAST (NUMERO_OPERATION AS VARCHAR(10))  AS CODE_AM_V,
SUM(MONTANT_REMISE) AS M_Remise
FROM dhb_prod.hub.f_vte_remise_detaillee t2
WHERE DATE(dateh_ticket) BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) AND NUMERO_OPERATION IS NOT NULL 
GROUP BY 1,2),
tab0 AS (SELECT a.*, b.CODE_AM, b.lib_operation, b.type_remise , 
CASE WHEN type_remise ='Plan co' THEN 'Plan_co' ELSE type_remise END AS type_remiseV2
FROM tab_rem a 
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.LISTE_CODE_AM_JULES b ON a.CODE_AM_V=b.CODE_AM
ORDER BY 1,2), 
tab1 AS (SELECT id_ticket_lign, type_remiseV2,
SUM(M_Remise) AS Mnt_Remise_CODEAM
FROM tab0
GROUP BY 1,2)
select * FROM tab1
pivot (SUM(Mnt_Remise_CODEAM) for type_remiseV2 in ('CLUB','Plan_co','Desto','Autre'));


-- Definir la notion de Nouveau Client 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ACTIVITE_CLT AS
WITH tgb0 AS (
	SELECT DISTINCT
		vd.CODE_CLIENT,
		vd.date_ticket,
		SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos_v0
		from DHB_PROD.DNR.DN_VENTE vd
where vd.ID_ORG_ENSEIGNE IN (1,3) ),
delai_achat AS ( SELECT DISTINCT
		CODE_CLIENT,
		date_ticket, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC) AS lag_date_ticket,
  	DATEDIFF(month, lag(date_ticket) over (PARTITION BY CODE_CLIENT ORDER BY date_ticket ASC),date_ticket) as DELAI_DERNIER_ACHAT
  	FROM tgb0 WHERE  Qte_pos_v0>0 ),
ACTIVITE_CLT AS (
  SELECT * , 
		CASE 
		WHEN DELAI_DERNIER_ACHAT <=12 THEN 'ACTIF 12MR' 
		WHEN DELAI_DERNIER_ACHAT >12 THEN 'REACTIVATION APRES 12MR' 
		WHEN DELAI_DERNIER_ACHAT IS NULL THEN 'NOUVEAU'
	ELSE 'ND' 
	END AS ACTIVITE_CLT
	FROM delai_achat
	WHERE date_ticket BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) )
SELECT *, 
MAX(CASE WHEN ACTIVITE_CLT IN ('REACTIVATION APRES 12MR', 'NOUVEAU') AND (CODE_CLIENT IS NOT NULL AND CODE_CLIENT !='0')
    THEN 1 ELSE 0 END ) OVER (PARTITION BY CODE_CLIENT) AS Top_Newclt
FROM ACTIVITE_CLT ; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ACTIVITE_CLT 
WHERE code_client ;  

SELECT Top_Newclt,
count(DISTINCT CODE_CLIENT) AS nbclt 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ACTIVITE_CLT
GROUP BY 1
ORDER BY 1; 





CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ticket AS
Select vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, 
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
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_brute,     
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
LEFT JOIN ACTIVITE_CLT act ON vd.CODE_CLIENT=act.CODE_CLIENT AND vd.date_ticket=act.date_ticket
where vd.date_ticket BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') ;
 
 
  -- AND id_ticket='1-855-2-20240615-24167006' exemple de ticket avec 3 code AM ;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt AS
WITH tabc AS (
SELECT DISTINCT CODE_CLIENT, id_ticket , b.*
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_remise_CODEAM b ON a.id_ticket=b.id_ticket_lign)
SELECT CODE_CLIENT , 
SUM("'CLUB'") AS rem_Club,
SUM("'Plan_co'") AS rem_Plan_co,
SUM("'Desto'") AS rem_Desto,
SUM("'Autre'") AS rem_Autre
FROM tabc
GROUP BY 1 ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_infoclt AS
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        gender, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin) AS ANCIENNETE_CLIENT,
    --CASE 
        --WHEN DATE(date_recrutement) BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) THEN '02-Nouveaux' 
        --ELSE '01-Anciens' 
    -- END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin - 1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE CODE_CLIENT IN ( SELECT DISTINCT CODE_CLIENT FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket)
),
Stat_clt AS ( SELECT CODE_CLIENT,Top_Newclt,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Rem_Gbl,
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket
GROUP BY 1,2
HAVING Mtn_Gbl>=1
ORDER BY 1 ), 
tab0 AS (SELECT *,ROW_NUMBER() OVER (order by Mtn_Gbl) AS row_num 
FROM Stat_clt ),
tab1 AS (SELECT * , MAX(row_num) over() AS max_nb_clt 
FROM tab0 ), 
tab2 AS (SELECT *, FLOOR(row_num/(max_nb_clt/10)) AS decile 
FROM tab1)
SELECT a.* , rem_Club, rem_Plan_co, rem_Desto, rem_Autre, c.*,
    CASE 
        WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
        ELSE 'z: Non def' 
    END AS Tr_anciennete
FROM tab2 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt b ON a.CODE_CLIENT=b.CODE_CLIENT 
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt ; 

SELECT DISTINCT decile FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt ORDER BY 1; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_N AS
SELECT '00-GLOBAL' AS TR_Decile,
Count(DISTINCT CODE_CLIENT) AS Nbclt, 
SUM(nb_ticket_Gbl) AS Nb_Ticket,
SUM(Mtn_Gbl) AS CA_clt,
SUM(Qte_Gbl) AS Quantite,
SUM(Marge_Gbl) AS Marge,
SUM(Rem_Gbl) AS Mnt_Remise,
SUM(rem_Club) AS Mnt_Remise_Club,
SUM(rem_Plan_co) AS Mnt_Remise_Plan_co,
SUM(rem_Desto) AS Mnt_Remise_Desto,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy,
ROUND (AVG (anciennete_client),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN Top_Newclt=1 THEN CODE_CLIENT END ) AS Nb_newclt 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1 
UNION 
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile,
Count(DISTINCT CODE_CLIENT) AS Nbclt, 
SUM(nb_ticket_Gbl) AS Nb_Ticket,
SUM(Mtn_Gbl) AS CA_clt,
SUM(Qte_Gbl) AS Quantite,
SUM(Marge_Gbl) AS Marge,
SUM(Rem_Gbl) AS Mnt_Remise,
SUM(rem_Club) AS Mnt_Remise_Club,
SUM(rem_Plan_co) AS Mnt_Remise_Plan_co,
SUM(rem_Desto) AS Mnt_Remise_Desto,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy,
ROUND (AVG (anciennete_client),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN Top_Newclt=1  THEN CODE_CLIENT END ) AS Nb_newclt 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1 
ORDER BY 1;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_N ORDER BY 1; 


SELECT * FROM DHB_PROD.DNR.DN_CLIENT; 




