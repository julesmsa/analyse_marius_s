/**** Calcul des KPI4S Clients sur N et N-1 *** 
 * 
 */

SET dtdeb = Date('2021-09-01');
SET dtfin = DAte('2024-08-31');

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA'
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.B_TICKETS AS
WITH info_clt AS (
    SELECT DISTINCT 
        Code_client AS idclient, 
        id_titre, 
        date_naissance, 
        age, 
        gender, 
        est_valide_telephone, 
        est_optin_sms_com, 
        est_optin_sms_fid, 
        est_optin_email_com, 
        est_optin_email_fid, 
        code_postal, 
        code_pays AS pays_clt, 
        date_recrutement
    FROM  DHB_PROD.DNR.DN_CLIENT
    WHERE (code_pays = $PAYS1 OR code_pays = $PAYS2) 
         AND (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
date_recrutement,
CASE WHEN DATE(date_recrutement) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN '02-Nouveaux' 
        ELSE '01-Anciens' END AS Type_client,    
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
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
        THEN 1  ELSE 0 END AS annul_ticket        
from DHB_PROD.DNR.DN_VENTE vd
LEFT JOIN info_clt b ON vd.CODE_CLIENT = b.idclient
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') )
  SELECT * FROM (
SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC_eur END ) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2 
 UNION  
 SELECT '01_PERIMETRE' AS typo_clt, PERIMETRE AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC_eur END ) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
    GROUP BY 1,2 
UNION
SELECT '2_ENSEIGNE' AS typo_clt,  CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite
    ,COUNT( DISTINCT CODE_CLIENT) AS nb_clt_glb
    ,COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC_eur END ) AS CA_glb
	,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS qte_achete_glb
    ,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_glb
    ,SUM(montant_remise) AS Mnt_remise_glb
    ,COUNT(DISTINCT CASE WHEN Date(date_recrutement)=Date(date_ticket) then CODE_CLIENT end) AS nb_newclt
    ,AVG(CASE WHEN ref_top_achat=1 AND AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen_ref   
    ,AVG(CASE WHEN AGE_C2 BETWEEN 15 AND 99 THEN AGE_C2 END) AS age_moyen
FROM DATA_MESH_PROD_CLIENT.WORK.BASE_TICKETS_V2
WHERE PERIMETRE = 'MAG'
    GROUP BY 1,2) 
    
    
    
    
    
    

