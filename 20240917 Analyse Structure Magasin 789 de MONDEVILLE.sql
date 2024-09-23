--- Analyse Structure Magasin de MONDEVILLE 

SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN 
WHERE LIB_MAGASIN LIKE '%MONDEVILLE%' ;


SET dtdeb = Date('2023-04-01');
SET dtfin = DAte('2024-08-31');

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA'
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- SELECT * FROM DHB_PROD.DNR.DN_CLIENT; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789 AS
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement      
    FROM DHB_PROD.DNR.DN_CLIENT),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND ID_MAGASIN IN (789)) ,
tickets_MAg as (
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
vd.id_ticket, vd.type_emplacement,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
mag.date_ouverture_public,vd.lib_magasin, 
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
date_naissance, 
genre, 
date_recrutement,
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
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT JOIN info_clt c ON vd.CODE_CLIENT=c.idclt 
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') )  
SELECT * 
FROM  tickets_MAg ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789 ; 

-- Statistiques kpi's Client  
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_mag789 AS
SELECT mag_achat, lib_magasin, '00-GLOBAL' AS top_mag789
,Count(DISTINCT CODE_CLIENT) AS Nbclt
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Mnt_remise_clt
,Count(DISTINCT CASE WHEN DATE(date_recrutement) BETWEEN DATE('2023-04-29') AND DATE('2024-04-28')  THEN CODE_CLIENT END ) AS Nb_newclt
FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789
where date_ticket BETWEEN DATE('2023-04-29') AND DATE('2024-04-28') 
GROUP BY 1,2,3
ORDER BY 1,2,3 ; 

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.Stat_mag789 ORDER BY 1,2,3 ; 

-- repartition des clients ayant 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.TICK_maghisto74_3197 AS
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement      
    FROM DHB_PROD.DNR.DN_CLIENT),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND ID_MAGASIN IN (74,3197)) ,
tickets_MAg as (
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
vd.id_ticket, vd.type_emplacement,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
mag.date_ouverture_public,vd.lib_magasin, 
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
date_naissance, 
genre, 
date_recrutement,
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
        THEN 1  ELSE 0 END AS annul_ticket,        
CASE WHEN CODE_CLIENT IN (SELECT DISTINCT CODE_CLIENT 
FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789
where date_ticket BETWEEN DATE('2023-04-29') AND DATE('2024-04-28') ) THEN '01-CLTMAG789' ELSE '02-AUTRES' END AS top_mag789
from DHB_PROD.DNR.DN_VENTE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT JOIN info_clt c ON vd.CODE_CLIENT=c.idclt 
where vd.date_ticket BETWEEN DATE('2022-04-29') AND DATE('2023-04-28')  
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') )  
SELECT * 
FROM  tickets_MAg ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.TICK_maghisto74_3197 ;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_mag74_3197 AS 
SELECT * FROM (
SELECT mag_achat, lib_magasin , '00-GLOBAL' AS top_mag789
,Count(DISTINCT CODE_CLIENT) AS Nbclt
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Mnt_remise_clt
,Count(DISTINCT CASE WHEN DATE(date_recrutement) BETWEEN DATE('2022-04-29') AND DATE('2023-04-28')  THEN CODE_CLIENT END ) AS Nb_newclt
FROM DATA_MESH_PROD_CLIENT.WORK.TICK_maghisto74_3197 
GROUP BY 1,2,3
UNION 
SELECT mag_achat, lib_magasin , top_mag789
,Count(DISTINCT CODE_CLIENT) AS Nbclt
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Mnt_remise_clt
,Count(DISTINCT CASE WHEN DATE(date_recrutement) BETWEEN DATE('2022-04-29') AND DATE('2023-04-28')  THEN CODE_CLIENT END ) AS Nb_newclt
FROM DATA_MESH_PROD_CLIENT.WORK.TICK_maghisto74_3197
GROUP BY 1,2,3 )
ORDER BY 1,2,3 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_mag74_3197 ORDER BY 1,2,3 ;  

-- Vision new mag 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789_V2 AS
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        genre, 
        date_recrutement      
    FROM DHB_PROD.DNR.DN_CLIENT),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND ID_MAGASIN IN (789)) ,
tickets_MAg as (
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
vd.id_ticket, vd.type_emplacement,
CASE WHEN vd.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN vd.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
mag.date_ouverture_public,vd.lib_magasin, 
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
date_naissance, 
genre, 
date_recrutement,
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
        THEN 1  ELSE 0 END AS annul_ticket,        
CASE WHEN CODE_CLIENT IN (SELECT DISTINCT CODE_CLIENT 
FROM DATA_MESH_PROD_CLIENT.WORK.TICK_maghisto74_3197
WHERE mag_achat=74 ) THEN '01-CLTMAG74' ELSE '02-AUTRES' END AS top_mag74,       
CASE WHEN CODE_CLIENT IN (SELECT DISTINCT CODE_CLIENT 
FROM DATA_MESH_PROD_CLIENT.WORK.TICK_maghisto74_3197
WHERE mag_achat=3197 ) THEN '01-CLTMAG3197' ELSE '02-AUTRES' END AS top_mag3197 
from DHB_PROD.DNR.DN_VENTE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT JOIN info_clt c ON vd.CODE_CLIENT=c.idclt 
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') )  
SELECT * 
FROM  tickets_MAg ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789_V2 ORDER BY 1,2,3 ;  

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.Stat_mag789_V2 AS 
SELECT * FROM (
SELECT mag_achat, lib_magasin , '00-GLOBAL' AS top_mag789
,Count(DISTINCT CODE_CLIENT) AS Nbclt
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Mnt_remise_clt
,Count(DISTINCT CASE WHEN DATE(date_recrutement) BETWEEN DATE('2023-04-29') AND DATE('2024-04-28')  THEN CODE_CLIENT END ) AS Nb_newclt
FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789_V2
where date_ticket BETWEEN DATE('2023-04-29') AND DATE('2024-04-28') 
GROUP BY 1,2,3
UNION 
SELECT mag_achat, lib_magasin , '01-CLTMAG74' AS top_mag789
,Count(DISTINCT CODE_CLIENT) AS Nbclt
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Mnt_remise_clt
,Count(DISTINCT CASE WHEN DATE(date_recrutement) BETWEEN DATE('2023-04-29') AND DATE('2024-04-28')  THEN CODE_CLIENT END ) AS Nb_newclt
FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789_V2
WHERE top_mag74='01-CLTMAG74' AND date_ticket BETWEEN DATE('2023-04-29') AND DATE('2024-04-28') 
GROUP BY 1,2,3 
UNION 
SELECT mag_achat, lib_magasin , '02-CLTMAG3197' AS top_mag789
,Count(DISTINCT CODE_CLIENT) AS Nbclt
,Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Mnt_remise_clt
,Count(DISTINCT CASE WHEN DATE(date_recrutement) BETWEEN DATE('2023-04-29') AND DATE('2024-04-28')  THEN CODE_CLIENT END ) AS Nb_newclt
FROM DATA_MESH_PROD_CLIENT.WORK.test_TICKETS_mag789_V2
WHERE top_mag3197='01-CLTMAG3197' AND date_ticket BETWEEN DATE('2023-04-29') AND DATE('2024-04-28') 
GROUP BY 1,2,3  
)
ORDER BY 1,2,3 ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.Stat_mag789_V2 ORDER BY 1,2,3 ; 

-----


WITH tabs AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE('2023-01-01')
AND (DATE_FIN > DATE('2023-04-01') OR DATE_FIN IS NULL) )
SELECT ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT ,
count(DISTINCT CODE_CLIENT) AS nbclient
FROM tabs
GROUP BY 1,2
ORDER BY 1,2; 


