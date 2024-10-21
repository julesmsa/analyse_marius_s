/*** Périmetre d'études 12 dernier mois à fin septembre ***/ 


SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- Date de fin ticket pour avoir des données stables 
SET dtfin = DAte('2024-10-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dtdeb = to_date(dateadd('year', -1, $dtfin)) ;
SELECT $dtfin,  $dtdeb ;


/*** SELECTION DES CLIENTS 0 SEGMENTER AU 01 OCTOBRE 2024 ***/

CREATE OR REPLACE TABLE DATA_MESH_PROD_CLIENT.WORK.CLT_ACTIF_12MOIS AS
WITH tab0 AS (SELECT DISTINCT CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS remise_totale,
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
    ELSE 0 END AS annul_ticket
FROM DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN DATE($dtdeb) AND (DATE($dtfin)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2) AND vd.code_client IS NOT NULL AND vd.code_client !='0' 
    AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
    ),
tab1 AS (SELECT CODE_CLIENT 
,MIN(CASE WHEN Qte_pos>0 THEN DATE(date_ticket) END) AS Min_date_ticket_12MOIS
,MAX(CASE WHEN Qte_pos>0 THEN DATE(date_ticket) END) AS Max_date_ticket_12MOIS
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket end ) AS FREQUENCE_12MOIS 
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC end) AS MONTANT_TTC_12MOIS
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE end) AS QUANTITE_12MOIS
FROM tab0 
GROUP BY 1 
HAVING MONTANT_TTC_12MOIS>0 AND FREQUENCE_12MOIS>0 )
SELECT a.* 
,DATE($dtfin) AS Date_Partition
,b.date_naissance 
,b.genre 
,b.date_recrutement 
,DATE(b.DATE_PREMIER_ACHAT) AS DATE_PREMIER_ACHAT
,DATEDIFF(DAY, DATE(Max_date_ticket_12MOIS), DATE($dtfin)) AS RECENSE
FROM tab1 a 
LEFT JOIN DHB_PROD.DNR.DN_CLIENT b ON a.CODE_CLIENT=b.CODE_CLIENT ;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLT_ACTIF_12MOIS ; 


/*

SELECT count(DISTINCT CODE_Client) AS nbclt
FROM DATA_MESH_PROD_CLIENT.WORK.CLT_ACTIF_12MOIS
GROUP BY 1 ; 

SELECT * FROM DHB_PROD.DNR.DN_CLIENT; 

info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt    
    FROM DHB_PROD.DNR.DN_CLIENT
    WHERE code_pays IN  ($PAYS1 ,$PAYS2) AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL ),

