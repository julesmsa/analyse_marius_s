-- Analyse clients Feel Good 


SET dtdeb = DAte('2024-10-01'); 
SET dtfin = DAte('2024-12-31'); 

SET dtfin_Nm1 = DAte('2023-12-31'); 


SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET LIB_ENSEIGNE1 = 'JULES'; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET LIB_ENSEIGNE2 = 'BRICE';
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

/*** Creation de la table permettant d'identifier les clients pour le profil ***/   

-- On tient compte de la segmentation des clients au debut de la période des soldes
 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG AS    
WITH i_mag AS (SELECT DISTINCT Id_entite, Code_entite, Lib_entite, 
CASE 
	WHEN Id_entite IN (747) THEN Date('2024-10-31')
   WHEN Id_entite IN (236, 3553) THEN Date('2023-10-31')	
	WHEN Id_entite IN (1166) THEN Date('2024-11-21')
	WHEN Id_entite IN ( 1040) THEN Date('2023-11-21')	
	WHEN Id_entite IN (778) THEN Date('2024-12-05')
	WHEN Id_entite IN (327, 3137) THEN Date('2023-12-05')	
	WHEN Id_entite IN (785) THEN Date('2024-12-13')
	WHEN Id_entite IN ( 167) THEN Date('2023-12-13')	
	ELSE NULL END AS datedeb_etud, 
CASE 
	WHEN Id_entite IN (747) THEN Date('2024-12-31')
   WHEN Id_entite IN (236, 3553) THEN Date('2023-12-31')	
	WHEN Id_entite IN (1166) THEN Date('2024-12-31')
	WHEN Id_entite IN ( 1040) THEN Date('2023-12-31')	
	WHEN Id_entite IN (778) THEN Date('2024-12-31')
	WHEN Id_entite IN (327, 3137) THEN Date('2023-12-31')	
	WHEN Id_entite IN (785) THEN Date('2024-12-31')
	WHEN Id_entite IN ( 167) THEN Date('2023-12-31')	
	ELSE NULL END AS datefin_etud 
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND CODE_PAYS IN ($PAYS1, $PAYS2) 
AND LIB_ENSEIGNE IN ($LIB_ENSEIGNE1, $LIB_ENSEIGNE2) 
AND ID_ENTITE IN (747, 236, 3553,1166, 1040,778, 327, 3137,785, 167))
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
    ELSE 0 END AS annul_ticket, datedeb_etud, datefin_etud
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN i_mag mag ON vd.ID_MAGASIN=mag.Id_entite 
where vd.date_ticket BETWEEN Date(datedeb_etud) AND DATE(datefin_etud)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0' ; 
 
 SELECT ID_MAGASIN, Count(DISTINCT CODE_CLIENT) AS nbclt
FROM  DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG
GROUP BY 1
ORDER BY 1; 

 
--- Rajouter les informations clients Sur N-1 et N 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG2 AS  
WITH 
info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt    
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_client IS NOT NULL AND code_client !='0' AND (date_suppression_client is null or date_suppression_client > $dtfin) ),  
segrfm_N AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_N, LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_N
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'), 
segomni_N AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_N
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin) 
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),  
segrfm_Nm1 AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT AS ID_MACRO_SEGMENT_Nm1, LIB_MACRO_SEGMENT AS LIB_MACRO_SEGMENT_Nm1
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin_Nm1)
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),  
segomni_Nm1 AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI AS LIB_SEGMENT_OMNI_Nm1
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin_Nm1) 
AND (DATE_FIN > DATE($dtfin_Nm1) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0')
SELECT a.*, 
b.*,  ID_MACRO_SEGMENT_N , LIB_MACRO_SEGMENT_N, LIB_SEGMENT_OMNI_N ,  
ID_MACRO_SEGMENT_Nm1 , LIB_MACRO_SEGMENT_Nm1, LIB_SEGMENT_OMNI_Nm1 
,CASE 
WHEN YEAR(datefin_etud)=2024 THEN datediff(MONTH ,date_recrutement,$dtfin) 
WHEN YEAR(datefin_etud)=2023 THEN datediff(MONTH ,date_recrutement,$dtfin_Nm1) 
ELSE NULL END AS ANCIENNETE_CLIENT
,CASE WHEN Date(date_recrutement) BETWEEN Date(datedeb_etud) AND DATE(datefin_etud) THEN '02-Nouveaux' ELSE '01-Anciens' END AS Type_client
, CASE 
WHEN YEAR(datefin_etud)=2024 THEN ROUND(DATEDIFF(YEAR,date_naissance,$dtfin),2) 
WHEN YEAR(datefin_etud)=2023 THEN ROUND(DATEDIFF(YEAR,date_naissance,$dtfin_Nm1),2)  
ELSE NULL END  AGE_C
, CASE WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) ELSE '99-NR/NC' END AS CLASSE_AGE
,CASE 
WHEN YEAR(datefin_etud)=2024 THEN ID_MACRO_SEGMENT_N
WHEN YEAR(datefin_etud)=2023 THEN ID_MACRO_SEGMENT_Nm1
ELSE NULL END AS ID_MACRO_SEGMENT
,CASE WHEN id_macro_segment = '01' THEN '01_VIP' 
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
  ELSE '12_NOSEG' END AS SEGMENT_RFM  -- On estime que tous les clients actifs ont effectué au moins un achat et sont donc segmentés 
,CASE WHEN id_macro_segment IN ('01', '02', '03') THEN '01_Haut_de_Fichier' 
     WHEN id_macro_segment IN ('04', '09') THEN '02_Ventre_Mou' 
     WHEN id_macro_segment IN ('05', '06', '07','08') THEN '03_Bas_de_Fichier' 
     WHEN id_macro_segment IN ('10', '11') THEN '04_Inactifs' -- On estime que tous les clients actifs ont effectué au moins un achat et sont donc segmentés 
     ELSE '09_Non_Segmentes' END AS CAT_SEGMENT_RFM     
,CASE 
WHEN YEAR(datefin_etud)=2024 THEN LIB_SEGMENT_OMNI_N
WHEN YEAR(datefin_etud)=2023 THEN LIB_SEGMENT_OMNI_Nm1
ELSE NULL END AS LIB_SEGMENT_OMNI     
  ,CASE WHEN lib_segment_omni ='MAG' then '01-MAG' 
      When lib_segment_omni ='WEB' then '02-WEB'
      When lib_segment_omni ='OMNI' then '03-OMNI' ELSE '99-NON RENSEIGNE' end as SEGMENT_OMNI     
,CASE WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
     WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
     else 'a: [0-12] mois'  END  AS Tr_anciennete, 
ROW_NUMBER() OVER (PARTITION BY a.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
CASE WHEN nb_lign=1 THEN AGE_C END AS AGE_C2,
	CASE 
	WHEN id_magasin IN (747, 236, 3553)  THEN '01-MAG_CRETEIL'
	WHEN id_magasin IN (1166, 1040) THEN '02-MAG_LIEGE'
	WHEN id_magasin IN (778, 327, 3137)  THEN '03-MAG_ENGLOS'
	WHEN id_magasin IN (785, 167) THEN '04-MAG_PARIS'
	ELSE '99-AUTRES' END AS Compar_etud
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG a
INNER JOIN info_clt b ON a.CODE_CLIENT=b.idclt
LEFT JOIN segrfm_N c ON a.code_client=c.code_client
LEFT JOIN segomni_N e ON a.code_client=e.code_client
LEFT JOIN segrfm_Nm1 f ON a.code_client=f.code_client
LEFT JOIN segomni_Nm1 g ON a.code_client=g.code_client ;    
  
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG ; 


-- information Sur le NPS par CLient 
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_NOTE_NPS AS
with 
liste_actifs as (
SELECT DISTINCT CODE_CLIENT, ID_magasin, datedeb_etud, datefin_etud
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG2 ), 
base AS (SELECT * FROM DATA_MESH_PROD_CLIENT.HUB.DMF_CLI_NPS WHERE id_magasin !='829-old'),
base_nps AS (SELECT * , CAST(ID_magasin AS NUMERIC) AS num_MAG
FROM base
WHERE libelle_question like '%ecommand%' AND CAST(ID_magasin AS NUMERIC) IN (747, 236, 3553,1166, 1040,778, 327, 3137,785, 167) ), 
base_nps2 AS (SELECT a.*  
FROM base_nps a 
INNER JOIN liste_actifs b ON a.CODE_CLIENT=b.CODE_CLIENT AND a.num_MAG=b.ID_magasin AND DATE(DATEH_CREATION_REPONSE_NPS) BETWEEN Date(datedeb_etud) AND date(datefin_etud)
),
note_nps AS ( SELECT CODE_Client, num_MAG, 
    avg(valeur_reponse) as note_nps_moy
FROM base_nps2
GROUP BY 1,2)
select *,
    case when note_nps_moy >= 0 and note_nps_moy <= 6 then 'Détracteur'
         when note_nps_moy <= 8 then 'Neutre'
        when note_nps_moy <= 10 then 'Promoteur' end as cat_clt_nps,
from note_nps ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_NOTE_NPS ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2 AS
SELECT a.*, note_nps_moy, cat_clt_nps   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG2 a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.BASE_NOTE_NPS b ON a.CODE_CLIENT=B.CODE_CLIENT AND a.ID_MAGASIN=b.num_MAG; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2 ;


(count(distinct case when categorie_client_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when categorie_client_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when categorie_client_nps is not null then CODE_CLIENT end) as nps
from classification_client





/**** Mise en place des statitistiques */


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.KPICLT_MAG_PERF AS
SELECT *
FROM ( 
SELECT Compar_etud, ID_magasin, nom_mag, '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '01_GENRE' AS typo_clt, CASE 	
 WHEN GENRE='H' THEN '01-Hommes'
 WHEN GENRE='F' THEN '02-Femmes'
 ELSE '03-Autres/NC'  END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '02_TYPE_CLIENT' AS typo_clt, TYPE_CLIENT AS modalite 
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '03_CAT_SEGMENT_RFM' AS typo_clt,  CAT_SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '04_SEGMENT_RFM' AS typo_clt,  SEGMENT_RFM AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '05_OMNICANALITE' AS typo_clt, 
 CASE WHEN lib_segment_omni ='MAG' then '01-MAG' 
      When lib_segment_omni ='WEB' then '02-WEB'
      When lib_segment_omni ='OMNI' then '03-OMNI' ELSE '99-NON RENSEIGNE' end as modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '06_CANAL_ACHAT' AS typo_clt, PERIMETRE AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '07A_ANCIENNETE CLIENT' AS typo_clt,  Tr_anciennete AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION 
SELECT Compar_etud, ID_magasin, nom_mag, '07B_ANCIENNETE Moyenne' AS typo_clt,  'ANCIENNETE Moyenne' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,AVG(anciennete_client) AS nb_clt_glb
    ,AVG(anciennete_client) AS nb_ticket_glb
    ,AVG(anciennete_client) AS CA_glb
	,AVG(anciennete_client) AS qte_achete_glb
    ,AVG(anciennete_client) AS Marge_glb
    ,AVG(anciennete_client) AS Mnt_remise_glb     
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '08A_AGE' AS typo_clt, CASE WHEN CLASSE_AGE IN ('80-84','85-89','90-94','95-99') THEN '80 et +' ELSE CLASSE_AGE END AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb 
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '08B_AGE MOYEN' AS typo_clt,  'AGE Moyen' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_clt_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS nb_ticket_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS CA_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS qte_achete_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Marge_glb
    ,AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C2 END) AS Mnt_remise_glb   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '09A_NPS CATEGORIE' AS typo_clt,  cat_clt_nps AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket
    ,COUNT(DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT( DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE end) AS Marge_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN montant_remise end) AS Mnt_remise_glb  
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5
UNION
SELECT Compar_etud, ID_magasin, nom_mag, '09B_NOTE NPS' AS typo_clt,  'NOTE NPS' AS modalite
    ,MIN(DATE(date_ticket)) AS Min_date_ticket
    ,MAX(DATE(date_ticket)) AS Max_date_ticket  
    ,round(((count(distinct case when cat_clt_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when cat_clt_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when cat_clt_nps is not null then CODE_CLIENT end))*100) AS nb_clt_glb
    ,round(((count(distinct case when cat_clt_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when cat_clt_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when cat_clt_nps is not null then CODE_CLIENT end))*100) AS nb_ticket_glb
    ,round(((count(distinct case when cat_clt_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when cat_clt_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when cat_clt_nps is not null then CODE_CLIENT end))*100) AS CA_glb
    ,round(((count(distinct case when cat_clt_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when cat_clt_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when cat_clt_nps is not null then CODE_CLIENT end))*100) AS qte_achete_glb
    ,round(((count(distinct case when cat_clt_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when cat_clt_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when cat_clt_nps is not null then CODE_CLIENT end))*100) AS Marge_glb
    ,round(((count(distinct case when cat_clt_nps = 'Promoteur' then CODE_CLIENT end) - count(distinct case when cat_clt_nps = 'Détracteur' then CODE_CLIENT end)) / count(distinct case when cat_clt_nps is not null then CODE_CLIENT end))*100) AS Mnt_remise_glb   
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_MAG_V2
    GROUP BY 1,2,3,4,5)
ORDER BY 1,2,3,4,5 ;    


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.KPICLT_MAG_PERF ORDER BY 1,2,3,4,5 ;

