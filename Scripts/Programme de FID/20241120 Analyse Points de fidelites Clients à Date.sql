-- Analyse Points de fidelites Clients à Date 


SET dtfin='2024-12-31';
SET dtdeb='2024-01-01';

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL'


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.infoclt_pts AS 
WITH tickets as (
Select DISTINCT vd.CODE_CLIENT
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb) AND DATE($dtfin)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0'), -- Actif sur les 12 derniers mois
  info_clt AS (
    SELECT DISTINCT a.Code_client, code_pays, ID_ORG_ENSEIGNE,
        date_naissance, 
        genre, 
        date_recrutement      
    FROM DHB_PROD.DNR.DN_CLIENT a
    INNER JOIN tickets b ON a.CODE_CLIENT=b.CODE_CLIENT
    WHERE code_pays IN  ($PAYS1 ,$PAYS2) AND a.code_client IS NOT NULL 
    AND a.code_client !='0' AND date_suppression_client IS NULL
    AND ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) 
     ),
    pts_fid_clt AS (SELECT DISTINCT CODE_CLIENT, nombre_points_fidelite
FROM DHB_PROD.HUB.D_CLI_HISTO_CLIENT 
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) 
 AND ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) 
 AND nombre_points_fidelite IS NOT NULL AND nombre_points_fidelite>0
AND code_client IS NOT NULL AND code_client !='0' ),
t_club AS (SELECT distinct code_client , valeur
FROM dhb_prod.hub.d_cli_histo_indicateur where id_indic=191
AND DATE_DEBUT <= DATE($dtfin) 
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'
and ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2) )
SELECT a.*, b.valeur , c.nombre_points_fidelite,
CASE WHEN nombre_points_fidelite BETWEEN 0 AND 99 THEN '0_99pts' 
     WHEN nombre_points_fidelite BETWEEN 100 AND 199 THEN '100_199pts'
    WHEN nombre_points_fidelite BETWEEN 200 AND 299 THEN '200_299pts'
 WHEN nombre_points_fidelite >=300 THEN '300pts & +'
   ELSE '99_Null' END AS cat_points,
MOD(nombre_points_fidelite,100) AS point_restant, 
100-point_restant AS declen_cheq
FROM info_clt a
INNER JOIN t_club b ON a.code_client=b.code_client
INNER JOIN pts_fid_clt c ON a.code_client=c.code_client 
WHERE nombre_points_fidelite IS NOT NULL AND nombre_points_fidelite>0 ;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.infoclt_pts ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_pts AS 
SELECT 
CASE WHEN code_pays='FRA' THEN '01-FRANCE'
     WHEN code_pays='BEL' THEN '02-BELGIQUE' ELSE '03-Autres'
 END  AS PAYS,
CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS ENSEIGNE , cat_points, 
 CASE WHEN valeur=1 THEN '01-JClub Prem' ELSE '02-JClub' END AS Statut_Club, 
Count(DISTINCT Code_client) AS nb_clts,
SUM(nombre_points_fidelite) AS Sum_pts_Fid,
SUM(point_restant) AS Sum_pts_Fid_restants,
SUM(declen_cheq) AS declen_cheq
FROM DATA_MESH_PROD_CLIENT.WORK.infoclt_pts
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_infoclt_pts ORDER BY 1,2,3,4;

-- (5/100)*0,8 + (10/100)*0,2

/*** Export Fichier Code_client pays , point restant **/ 



