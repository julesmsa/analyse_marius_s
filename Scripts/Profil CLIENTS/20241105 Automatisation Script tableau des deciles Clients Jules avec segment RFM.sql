--- Mise en place tableau des deciles Clients Jules 

-- Analyse Versus N-1 

  -- SCRIPT POUR L'ANNEE EN COURS 

SET dtfin = DAte('2024-11-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 
SET dtfin_Nm2 = to_date(dateadd('year', -2, $dtfin)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtfin, $dtfin_Nm1, $dtfin_Nm2, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

/*** Liste des codes AM à catégoriser ***/

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

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_remise_CODEAM ; 


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
CODE_EVT_PROMO, MONTANT_SOLDE,
CODE_AM, CODE_OPE_COMM,
MONTANT_REMISE_OPE_COMM,
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5,
type_emplacement,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
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
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                   from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
        THEN 1         
    ELSE 0 END AS annul_ticket,     
SUM(CASE 
    WHEN annul_ticket= 0 THEN montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_nette   
from DHB_PROD.DNR.DN_VENTE vd
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
SELECT CODE_CLIENT, 
SUM("'CLUB'") AS rem_Club,
SUM("'Plan_co'") AS rem_Plan_co,
SUM("'Desto'") AS rem_Desto,
SUM("'Autre'") AS rem_Autre
FROM tabc
GROUP BY 1 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_infoclt AS
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin - 1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE CODE_CLIENT IN ( SELECT DISTINCT CODE_CLIENT FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket)
),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) ),
Stat_clt AS ( SELECT CODE_CLIENT,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Rem_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_REMISE_OPE_COMM END) AS Rem_OPE_COMM,
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket
GROUP BY 1
HAVING Mtn_Gbl>=1 AND nb_ticket_Gbl>0 -- ON prend uniqument les clients avec un ticket et montant positif 
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
    END AS Tr_anciennete , 
ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI, 
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
FROM tab2 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt b ON a.CODE_CLIENT=b.CODE_CLIENT 
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt 
LEFT JOIN segrfm g ON a.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON a.CODE_CLIENT=f.CODE_CLIENT ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt ; 

SELECT DISTINCT decile FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt ORDER BY 1; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Dec AS
SELECT '00-GLOBAL' AS TR_Decile,
Count(DISTINCT CODE_CLIENT) AS Nbclt, 
SUM(nb_ticket_Gbl) AS Nb_Ticket,
SUM(Mtn_Gbl) AS CA_clt,
SUM(Qte_Gbl) AS Quantite,
SUM(Marge_Gbl) AS Marge,
SUM(Rem_Gbl) AS Mnt_Remise_clt,
SUM(rem_Club) AS Mnt_Remise_Club,
SUM(rem_Plan_co) AS Mnt_Remise_Plan_co,
SUM(rem_Desto) AS Mnt_Remise_Desto,
SUM(Rem_OPE_COMM) AS Mnt_Remise_Markdown,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy,
ROUND (AVG (anciennete_client),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN Type_client='02-Nouveaux'  THEN CODE_CLIENT END ) AS Nb_newclt 
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
SUM(Rem_OPE_COMM) AS Mnt_Remise_Markdown,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy,
ROUND (AVG (anciennete_client),1) AS anciennete_moy,
Count(DISTINCT CASE WHEN Type_client='02-Nouveaux'  THEN CODE_CLIENT END ) AS Nb_newclt 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1 
ORDER BY 1;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Dec ORDER BY 1; 

--- Rajouter les informations sur la segmentation 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Seg AS
WITH asd AS (SELECT DISTINCT TR_Decile, modalite, Nbclt  FROM (
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile, 
'00-GLOBAL' as typo, '00-GLOBAL' AS modalite, 
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1, 2, 3 
UNION 
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile, 
'01_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite, 
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1, 2, 3 
UNION 
SELECT '00-GLOBAL' AS TR_Decile, 
'01_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite, 
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1, 2, 3 
UNION 
SELECT '00-GLOBAL' AS TR_Decile, 
'02_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite,  
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1, 2, 3
UNION 
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile, 
'02_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite,  
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt
GROUP BY 1, 2, 3)
ORDER BY 1,2,3)
SELECT * FROM asd
pivot (SUM(Nbclt) for modalite in ('00-GLOBAL','01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG','01-MAG','02-WEB','03-OMNI'));

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Seg ORDER BY 1; 

/*** concatenation des informations pour les Déciles de 2 tables concernant les Déciles  */

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl AS 
SELECT a.*,
-- "'00-GLOBAL'",
"'01_VIP'" AS RFM_VIP,
"'02_TBC'" AS RFM_TBC,
"'03_BC'" AS RFM_BC,
"'04_MOY'" AS RFM_MOY,
"'05_TAP'" AS RFM_TAP,
"'06_TIEDE'" AS RFM_TIEDE,
"'07_TPURG'" AS RFM_TPURG,
"'08_NCV'" AS RFM_NCV,
"'09_NAC'" AS RFM_NAC,
"'10_INA12'" AS RFM_INA12,
"'11_INA24'" AS RFM_INA24,
"'12_NOSEG'" AS RFM_NOSEG,
"'01-MAG'" AS CANAL_MAG,
"'02-WEB'" AS CANAL_WEB,
"'03-OMNI'" AS CANAL_OMNI
FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Dec a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Seg b ON a.TR_DECILE=b.TR_DECILE 
ORDER BY 1;  

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl ORDER BY 1;  



-- EXPORT ----
      /* cr�er un dossier volant dossier_export_data_*/

--CREATE OR REPLACE TEMPORARY STAGE dossier_export_data_;
/* dedans, je vais mettre la requete entre FROM et FILE_FORMAT */
--COPY INTO @dossier_export_data_/info_decile.csv FROM 
--(SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl ORDER BY 1)      
--FILE_FORMAT = ( 
 --TYPE='CSV' 
 --COMPRESSION=NONE /*GZIP */ /* compression ou non */
 --FIELD_DELIMITER=',' /* dlm*/
 --ESCAPE=NONE 
-- ESCAPE_UNENCLOSED_FIELD=NONE 
 --date_format='AUTO' 
-- time_format='AUTO' 
 --timestamp_format='AUTO'
 --binary_format='UTF-8' 
-- field_optionally_enclosed_by='"' 
-- null_if='' 
-- EMPTY_FIELD_AS_NULL = FALSE 
--)  
--overwrite=TRUE /* �crire par dessus */ 
--single=TRUE /* fichier seul ou parralelisation*/ 
--max_file_size=5368709120 /* taille max en Octet du fichier */
--header=TRUE /* noms e col */;
/* recuperer le nom du fichier / v�rifier que c ok */
--ls @dossier_export_data_;
--GET @dossier_export_data_/info_decile.csv  /* dans ton dossier volant */
 --       file://C:\Users\msaka\OneDrive - HAPPYCHIC\Bureau\Extract_Result_Sql



---- Calcul des informations sur N-1 

SELECT $dtfin, $dtfin_Nm1, $dtfin_Nm2, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_remise_CODEAM_Nm1 AS 
WITH tab_rem AS (SELECT CONCAT(t2.id_org_enseigne,'-',t2.id_magasin,'-',t2.code_caisse,'-',t2.code_date_ticket,'-',t2.code_ticket) as id_ticket_lign,
CAST (NUMERO_OPERATION AS VARCHAR(10))  AS CODE_AM_V,
SUM(MONTANT_REMISE) AS M_Remise
FROM dhb_prod.hub.f_vte_remise_detaillee t2
WHERE DATE(dateh_ticket) BETWEEN DATE($dtfin_Nm2) AND DATE($dtfin_Nm1 - 1) AND NUMERO_OPERATION IS NOT NULL 
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

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_remise_CODEAM_Nm1 ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ticket_Nm1 AS
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
CODE_EVT_PROMO, MONTANT_SOLDE,
CODE_AM, CODE_OPE_COMM,
MONTANT_REMISE_OPE_COMM,
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5,
type_emplacement,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
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
    WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
                   from DHB_PROD.DNR.DN_PRODUIT
                   where ID_TYPE_ARTICLE<>1
                    and id_marque='JUL')
        THEN 1         
    ELSE 0 END AS annul_ticket,     
SUM(CASE 
    WHEN annul_ticket= 0 THEN montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_nette   
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN DATE($dtfin_Nm2) AND DATE($dtfin_Nm1 - 1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') ;
  
 
 -- AND id_ticket='1-855-2-20240615-24167006' exemple de ticket avec 3 code AM ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_Nm1 ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt_Nm1 AS
WITH tabc AS (
SELECT DISTINCT CODE_CLIENT, id_ticket , b.*
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_Nm1 a
LEFT JOIN DATA_MESH_PROD_RETAIL.WORK.tab_remise_CODEAM_Nm1 b ON a.id_ticket=b.id_ticket_lign)
SELECT CODE_CLIENT, 
SUM("'CLUB'") AS rem_Club,
SUM("'Plan_co'") AS rem_Plan_co,
SUM("'Desto'") AS rem_Desto,
SUM("'Autre'") AS rem_Autre
FROM tabc
GROUP BY 1 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt_Nm1; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1 AS
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin_Nm1) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtfin_Nm2) AND DATE($dtfin_Nm1 - 1) THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin_Nm1 - 1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE CODE_CLIENT IN ( SELECT DISTINCT CODE_CLIENT FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_Nm1)
),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) ),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) ),
Stat_clt AS ( SELECT CODE_CLIENT,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Rem_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_REMISE_OPE_COMM END) AS Rem_OPE_COMM,
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_Nm1
GROUP BY 1
HAVING Mtn_Gbl>=1 AND nb_ticket_Gbl>0 -- ON prend uniqument les clients avec un ticket et montant positif 
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
    END AS Tr_anciennete , 
ID_MACRO_SEGMENT, LIB_MACRO_SEGMENT, LIB_SEGMENT_OMNI, 
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
FROM tab2 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.tab_rem_Clt_Nm1 b ON a.CODE_CLIENT=b.CODE_CLIENT 
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt 
LEFT JOIN segrfm g ON a.CODE_CLIENT=g.CODE_CLIENT 
LEFT JOIN segomni f ON a.CODE_CLIENT=f.CODE_CLIENT ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1 ; 

SELECT DISTINCT decile FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1 ORDER BY 1; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Dec_Nm1 AS
SELECT '00-GLOBAL' AS TR_Decile,
Count(DISTINCT CODE_CLIENT) AS Nbclt_Nm1, 
SUM(nb_ticket_Gbl) AS Nb_Ticket_Nm1,
SUM(Mtn_Gbl) AS CA_clt_Nm1,
SUM(Qte_Gbl) AS Quantite_Nm1,
SUM(Marge_Gbl) AS Marge_Nm1,
SUM(Rem_Gbl) AS Mnt_Remise_clt_Nm1,
SUM(rem_Club) AS Mnt_Remise_Club_Nm1,
SUM(rem_Plan_co) AS Mnt_Remise_Plan_co_Nm1,
SUM(rem_Desto) AS Mnt_Remise_Desto_Nm1,
SUM(Rem_OPE_COMM) AS Mnt_Remise_Markdown_Nm1,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy_Nm1,
ROUND (AVG (anciennete_client),1) AS anciennete_moy_Nm1,
Count(DISTINCT CASE WHEN Type_client='02-Nouveaux'  THEN CODE_CLIENT END ) AS Nb_newclt_Nm1 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1 
UNION 
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile,
Count(DISTINCT CODE_CLIENT) AS Nbclt_Nm1, 
SUM(nb_ticket_Gbl) AS Nb_Ticket_Nm1,
SUM(Mtn_Gbl) AS CA_clt_Nm1,
SUM(Qte_Gbl) AS Quantite_Nm1,
SUM(Marge_Gbl) AS Marge_Nm1,
SUM(Rem_Gbl) AS Mnt_Remise_clt_Nm1,
SUM(rem_Club) AS Mnt_Remise_Club_Nm1,
SUM(rem_Plan_co) AS Mnt_Remise_Plan_co_Nm1,
SUM(rem_Desto) AS Mnt_Remise_Desto_Nm1,
SUM(Rem_OPE_COMM) AS Mnt_Remise_Markdown_Nm1,
ROUND (AVG (CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END),1) AS age_moy_Nm1,
ROUND (AVG (anciennete_client),1) AS anciennete_moy_Nm1,
Count(DISTINCT CASE WHEN Type_client='02-Nouveaux'  THEN CODE_CLIENT END ) AS Nb_newclt_Nm1 
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1 
ORDER BY 1;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Dec_Nm1 ORDER BY 1; 

--- Rajouter les informations sur la segmentation 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Seg_Nm1 AS
WITH asd AS (SELECT DISTINCT TR_Decile, modalite, Nbclt  FROM (
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile, 
'00-GLOBAL' as typo, '00-GLOBAL' AS modalite, 
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1, 2, 3 
UNION 
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile, 
'01_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite, 
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1, 2, 3 
UNION 
SELECT '00-GLOBAL' AS TR_Decile, 
'01_SEGMENT RFM' as typo, SEGMENT_RFM AS modalite, 
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1, 2, 3 
UNION 
SELECT '00-GLOBAL' AS TR_Decile, 
'02_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite,  
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1, 2, 3
UNION 
SELECT CASE WHEN decile<9 THEN CONCAT('DECILE 0',decile+1) ELSE 'DECILE 10' END AS TR_Decile, 
'02_SEGMENT OMNI' as typo, SEGMENT_OMNI AS modalite,  
Count(DISTINCT CODE_CLIENT) AS Nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.tab_infoclt_Nm1
GROUP BY 1, 2, 3)
ORDER BY 1,2,3)
SELECT * FROM asd
pivot (SUM(Nbclt) for modalite in ('00-GLOBAL','01_VIP','02_TBC','03_BC','04_MOY','05_TAP','06_TIEDE','07_TPURG','08_NCV','09_NAC','10_INA12','11_INA24','12_NOSEG','01-MAG','02-WEB','03-OMNI'));

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Seg_Nm1 ORDER BY 1; 

/*** concatenation des informations pour les Déciles de 2 tables concernant les Déciles  */

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl_Nm1 AS 
SELECT a.*,
-- "'00-GLOBAL'",
"'01_VIP'" AS RFM_VIP_Nm1,
"'02_TBC'" AS RFM_TBC_Nm1,
"'03_BC'" AS RFM_BC_Nm1,
"'04_MOY'" AS RFM_MOY_Nm1,
"'05_TAP'" AS RFM_TAP_Nm1,
"'06_TIEDE'" AS RFM_TIEDE_Nm1,
"'07_TPURG'" AS RFM_TPURG_Nm1,
"'08_NCV'" AS RFM_NCV_Nm1,
"'09_NAC'" AS RFM_NAC_Nm1,
"'10_INA12'" AS RFM_INA12_Nm1,
"'11_INA24'" AS RFM_INA24_Nm1,
"'12_NOSEG'" AS RFM_NOSEG_Nm1,
"'01-MAG'" AS CANAL_MAG_Nm1,
"'02-WEB'" AS CANAL_WEB_Nm1,
"'03-OMNI'" AS CANAL_OMNI_Nm1
FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Dec_Nm1 a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_Seg_Nm1 b ON a.TR_DECILE=b.TR_DECILE 
ORDER BY 1;  

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl_Nm1 ORDER BY 1;  

-- Regroupement de toutes les informations necessaires 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_DECILE AS 
SELECT 
DATE($dtfin) AS DATE_CALCUL,
a.*, 
NBCLT_NM1,
NB_TICKET_NM1,
CA_CLT_NM1,
QUANTITE_NM1,
MARGE_NM1,
MNT_REMISE_CLT_NM1,
MNT_REMISE_CLUB_NM1,
MNT_REMISE_PLAN_CO_NM1,
MNT_REMISE_DESTO_NM1,
MNT_REMISE_MARKDOWN_NM1,
AGE_MOY_NM1,
ANCIENNETE_MOY_NM1,
NB_NEWCLT_NM1,
RFM_VIP_NM1,
RFM_TBC_NM1,
RFM_BC_NM1,
RFM_MOY_NM1,
RFM_TAP_NM1,
RFM_TIEDE_NM1,
RFM_TPURG_NM1,
RFM_NCV_NM1,
RFM_NAC_NM1,
RFM_INA12_NM1,
RFM_INA24_NM1,
RFM_NOSEG_NM1,
CANAL_MAG_NM1,
CANAL_WEB_NM1,
CANAL_OMNI_NM1,
FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl a
LEFT JOIN  DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_DecGbl_Nm1 b ON a.TR_Decile=b.TR_Decile 
ORDER BY 1,2;  


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_DECILE ORDER BY 1,2; 

-- Dater les partitions

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_NOVEMBRE24 AS 
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_DECILE 
ORDER BY 1,2; 


/*  
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_OCTOBRE24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_SEPTEMBRE24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_AOUT24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_JUILLET24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_JUIN24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_MAI24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_AVRIL24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_MARS24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_FEVRIER24 ORDER BY 1,2;
 * Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_JANVIER24 ORDER BY 1,2;
 */



---- Regroupement des informations par Mois 

/*

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_FINAL AS 
SELECT * FROM (
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_NOVEMBRE24
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_OCTOBRE24
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_SEPTEMBRE24
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_AOUT24 
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_JUILLET24 
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_JUIN24
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_MAI24 
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_AVRIL24 
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_MARS24 
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_FEVRIER24 
 UNION
 Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_JANVIER24 
)
ORDER BY 1,2 ; 

Select * FROM DATA_MESH_PROD_CLIENT.WORK.STAT_GLOBAL_DECILE_FINAL ORDER BY 1,2;











