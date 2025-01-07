--- Gestion Actifs 3 Ans de clients 

SET dtfin = DAte('2024-12-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 
SET dtfin_Nm2 = to_date(dateadd('year', -2, $dtfin)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtfin, $dtfin_Nm1, $dtfin_Nm2, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_clt_N AS
Select DISTINCT vd.CODE_CLIENT,
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
vd.montant_remise + vd.MONTANT_REMISE_OPE_COMM AS GBL_remise,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
ROW_NUMBER() OVER (PARTITION BY vd.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
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
        date_naissance, 
        genre, 
        date_recrutement,
        CASE WHEN genre='F' THEN '02-FEMMES' ELSE '01-HOMMES' END AS type_genre,
    DATEDIFF(MONTH, date_recrutement, $dtfin) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
CASE WHEN DATE(date_recrutement) = DATE(vd.date_ticket) AND type_emplacement IN ('EC','MP')  THEN '01-Nouveaux_WEB'   
ELSE '02-Autres_Client' 
    END AS T_client_Web,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin - 1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE,
    CASE WHEN CLASSE_AGE IN ('70-74', '75-79', '80-84' , '85-89', '90-94', '95-99' ) THEN '75 ans et +' ELSE CLASSE_AGE END AS TR_AGE
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN DHB_PROD.DNR.DN_CLIENT clt ON vd.CODE_CLIENT=clt.CODE_CLIENT
where vd.date_ticket BETWEEN DATE($dtfin_Nm1) AND DATE($dtfin - 1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')  ;
 
 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_N AS
 SELECT * FROM (
  SELECT '00-Global' AS Descrip, '00-Global' AS typo, '00-Global' AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N
  GROUP BY 1,2,3
  UNION 
 SELECT '00-Global' AS Descrip, '00-Global' AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N
  GROUP BY 1,2,3
  UNION 
 SELECT '01-Type_client' AS Descrip, Type_client AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N
  GROUP BY 1,2,3
  UNION   
  SELECT '02-Canal_Achat' AS Descrip, PERIMETRE AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N
  GROUP BY 1,2,3 
  UNION
  SELECT '03-Recrut_WEB' AS Descrip, T_client_Web AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N
  WHERE T_client_Web='01-Nouveaux_WEB'
  GROUP BY 1,2,3)
  ORDER BY 1,2,3;  

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_N  ORDER BY 1,2,3;




--- information sur Nm1

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1 AS
Select DISTINCT vd.CODE_CLIENT,
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
vd.montant_remise + vd.MONTANT_REMISE_OPE_COMM AS GBL_remise,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
ROW_NUMBER() OVER (PARTITION BY vd.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
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
        date_naissance, 
        genre, 
        date_recrutement,
        CASE WHEN genre='F' THEN '02-FEMMES' ELSE '01-HOMMES' END AS type_genre,
    DATEDIFF(MONTH, date_recrutement, $dtfin_Nm1) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) BETWEEN DATE($dtfin_Nm2) AND DATE($dtfin_Nm1 - 1) THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
CASE WHEN DATE(date_recrutement) = DATE(vd.date_ticket) AND type_emplacement IN ('EC','MP')  THEN '01-Nouveaux_WEB'   
ELSE '02-Autres_Client' 
    END AS T_client_Web,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin_Nm1 - 1)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE,
    CASE WHEN CLASSE_AGE IN ('70-74', '75-79', '80-84' , '85-89', '90-94', '95-99' ) THEN '75 ans et +' ELSE CLASSE_AGE END AS TR_AGE
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN DHB_PROD.DNR.DN_CLIENT clt ON vd.CODE_CLIENT=clt.CODE_CLIENT
where vd.date_ticket BETWEEN DATE($dtfin_Nm2) AND DATE($dtfin_Nm1 - 1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')  ;
 
 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_Nm1 AS
 SELECT * FROM (
  SELECT '00-Global' AS Descrip, '00-Global' AS typo, '00-Global' AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1
  GROUP BY 1,2,3
  UNION 
 SELECT '00-Global' AS Descrip, '00-Global' AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1
  GROUP BY 1,2,3
  UNION 
 SELECT '01-Type_client' AS Descrip, Type_client AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1
  GROUP BY 1,2,3
  UNION   
  SELECT '02-Canal_Achat' AS Descrip, PERIMETRE AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1
  GROUP BY 1,2,3 
  UNION
  SELECT '03-Recrut_WEB' AS Descrip, T_client_Web AS typo, TR_AGE AS modalite,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1
  WHERE T_client_Web='01-Nouveaux_WEB'
  GROUP BY 1,2,3)
  ORDER BY 1,2,3;  

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_Nm1  ORDER BY 1,2,3; 


 -- jOINTURE DES INFORMATIONS 

 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_Gbl AS
SELECT a.*, 
b.nb_Client_Global AS nb_Client_Global_Nm1, b.nb_Client_Hom AS nb_Client_Hom_Nm1, b.nb_Client_Fem AS nb_Client_Fem_Nm1
FROM DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_N a
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_Nm1 b ON a.Descrip=b.Descrip AND a.typo=b.typo AND a.modalite=b.modalite ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_tab_clt_Gbl ORDER BY 1,2,3; 



-- Calcul de L'age Moyen des Hommes et Femmes N et N-1 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N ; 

WITH tab_Nm1 AS (SELECT DISTINCT CODE_CLIENT, GENRE, age_C FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_Nm1 )
SELECT GENRE,  AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END) AS age_moyen
FROM tab_Nm1
GROUP BY 1
ORDER BY 1 ; 


WITH tab_N AS (SELECT DISTINCT CODE_CLIENT, GENRE, age_C FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_N )
SELECT GENRE,  AVG(CASE WHEN AGE_C BETWEEN 15 AND 99 THEN AGE_C END) AS age_moyen
FROM tab_N
GROUP BY 1
ORDER BY 1 ; 






CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_age_client_36mois AS 
WITH info_clt AS (
    SELECT DISTINCT Code_client, 
        date_naissance, 
        genre, 
        date_recrutement,
        CASE WHEN genre='F' THEN '02-FEMMES' ELSE '01-HOMMES' END AS type_genre,
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
    WHERE ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) AND code_pays IN  ($PAYS1 ,$PAYS2) 
             AND code_client IS NOT NULL AND code_client !='0' AND date_suppression_client IS NULL AND est_actif_36_mois=1 )
  SELECT CASE WHEN CLASSE_AGE IN ('75-79', '80-84' , '85-89', '90-94', '95-99' ) THEN '75 ans et +' ELSE CLASSE_AGE END AS TR_AGE,
  Count(DISTINCT CODE_CLIENT) AS nb_Client_Global,
  Count(DISTINCT CASE WHEN type_genre='01-HOMMES' THEN CODE_CLIENT END ) AS nb_Client_Hom,
  Count(DISTINCT CASE WHEN type_genre='02-FEMMES' THEN CODE_CLIENT END ) AS nb_Client_Fem
  FROM info_clt
  GROUP BY 1
  ORDER BY 1 ;
 
 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_age_client_36mois  ORDER BY 1 ; 
  
  
