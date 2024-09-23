-- Etude Trafic Magasin

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE; 

SELECT * FROM  DATA_MESH_PROD_RETAIL.HUB.DMF_TRAFIC_MAGASIN ;

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- Période d'étude 

SET dtdeb = Date('2024-01-01');
SET dtfin = DAte('2024-05-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();

SET dtdeb_m1 = to_date(dateadd('year', -1, $dtdeb));
SET dtfin_m1= to_date(dateadd('year', -1, $dtfin)); 

-- Nous allons Etudier 
--- DEFINIR le Nombre de visite par jour 


--- On analyse les magasins ayant ouvert sur les 3 dernieres années pas de fermeture

-- Informations global sur le TT 
 -- Si TT inf à 5% et + de 65 % , on a le nombre de magasins 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic AS 
WITH trafic_mag as ( SELECT 
 a.ID_ORG_ENSEIGNE
,a.ID_MAGASIN
,DATE(DATEH_TRAFIC) AS DATE_TRAFIC, 
SUM(NOMBRE_ENTREE) AS SUM_ENTREE,
SUM(NOMBRE_SORTIES) AS SUM_SORTIES,
SUM(NOMBRE_PASSAGE) AS SUM_PASSAGE
FROM DATA_MESH_PROD_RETAIL.HUB.DMF_TRAFIC_MAGASIN a
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on a.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and a.ID_MAGASIN = mag.ID_MAGASIN
WHERE (DATE(DATEH_TRAFIC) BETWEEN $dtdeb AND $dtfin OR DATE(DATEH_TRAFIC) BETWEEN $dtdeb_m1 AND $dtfin_m1) and (a.ID_ORG_ENSEIGNE = $ENSEIGNE1 or a.ID_ORG_ENSEIGNE = $ENSEIGNE2) and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
GROUP BY 1,2,3 ),
Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning, surface_commerciale,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND (code_pays = $PAYS1 or code_pays = $PAYS2) ),
tickets as (
Select vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.date_ticket 
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
where (DATE(date_ticket) BETWEEN $dtdeb AND $dtfin OR DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  GROUP BY 1,2,3)
SELECT a.*, b.SUM_ENTREE,b.SUM_SORTIES,b.SUM_PASSAGE,
type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning, surface_commerciale,PERIMETRE,
CASE WHEN SUM_PASSAGE IS NOT NULL AND SUM_PASSAGE>0 THEN ROUND(nb_ticket_glb/SUM_PASSAGE,4) END AS TT_Mag, 
CASE WHEN TT_mag<0.05 OR TT_Mag>0.65 THEN 1 ELSE 0 END AS top_anormal, 
CASE WHEN WEEK(date_ticket)<10 THEN CONCAT('WEEK_S0',WEEK(date_ticket)) ELSE CONCAT('WEEK_S',WEEK(date_ticket)) END AS Sem_vente,
YEAR(date_ticket) AS anne_ticket
FROM tickets a
INNER JOIN trafic_mag b ON a.ID_ORG_ENSEIGNE=b.ID_ORG_ENSEIGNE AND a.ID_MAGASIN=b.ID_MAGASIN AND  a.date_ticket=b.DATE_TRAFIC
INNER JOIN Magasin mag ON a.ID_ORG_ENSEIGNE=mag.ID_ORG_ENSEIGNE AND a.ID_MAGASIN=mag.ID_MAGASIN AND PERIMETRE='MAG'; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic;

-- Calcul du TT par semaine avec les magasins sans anomalie

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_V0_trafic AS 
SELECT * FROM 
(SELECT '00-Global' AS enseigne, '00-Global' AS semaine,
Min(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS min_date_2024,
Max(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS max_date_2024,
Min(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS min_date_2023,
Max(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS max_date_2023,
count(DISTINCT ID_MAGASIN ) AS nb_mag,
SUM(CASE WHEN anne_ticket=2024 THEN SUM_PASSAGE end) AS nb_PASSAGE2024, 
SUM(CASE WHEN anne_ticket=2024 THEN nb_ticket_glb end) AS nb_ticket2024,
CASE WHEN nb_PASSAGE2024 IS NOT NULL AND nb_PASSAGE2024>0 THEN ROUND(nb_ticket2024/nb_PASSAGE2024,4) END AS TT_Mag_2024, 
SUM(CASE WHEN anne_ticket=2023 THEN SUM_PASSAGE end) AS nb_PASSAGE2023, 
SUM(CASE WHEN anne_ticket=2023 THEN nb_ticket_glb end) AS nb_ticket2023, 
CASE WHEN nb_PASSAGE2023 IS NOT NULL AND nb_PASSAGE2023>0 THEN ROUND(nb_ticket2023/nb_PASSAGE2023,4) END AS TT_Mag_2023, 
ROUND(TT_Mag_2024-TT_Mag_2023,4)*100 AS ecart_pts_TT_Mag,
CASE WHEN TT_Mag_2023 IS NOT NULL AND TT_Mag_2023>0 THEN ROUND((TT_Mag_2024-TT_Mag_2023)/TT_Mag_2023,4) END AS Evol_TT_Mag
FROM DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic
WHERE top_anormal=0
GROUP BY 1,2
UNION
SELECT CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS enseigne, '00-Global' AS semaine,
Min(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS min_date_2024,
Max(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS max_date_2024,
Min(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS min_date_2023,
Max(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS max_date_2023,
count(DISTINCT ID_MAGASIN ) AS nb_mag,
SUM(CASE WHEN anne_ticket=2024 THEN SUM_PASSAGE end) AS nb_PASSAGE2024, 
SUM(CASE WHEN anne_ticket=2024 THEN nb_ticket_glb end) AS nb_ticket2024,
CASE WHEN nb_PASSAGE2024 IS NOT NULL AND nb_PASSAGE2024>0 THEN ROUND(nb_ticket2024/nb_PASSAGE2024,4) END AS TT_Mag_2024, 
SUM(CASE WHEN anne_ticket=2023 THEN SUM_PASSAGE end) AS nb_PASSAGE2023, 
SUM(CASE WHEN anne_ticket=2023 THEN nb_ticket_glb end) AS nb_ticket2023, 
CASE WHEN nb_PASSAGE2023 IS NOT NULL AND nb_PASSAGE2023>0 THEN ROUND(nb_ticket2023/nb_PASSAGE2023,4) END AS TT_Mag_2023, 
ROUND(TT_Mag_2024-TT_Mag_2023,4)*100 AS ecart_pts_TT_Mag,
CASE WHEN TT_Mag_2023 IS NOT NULL AND TT_Mag_2023>0 THEN ROUND((TT_Mag_2024-TT_Mag_2023)/TT_Mag_2023,4) END AS Evol_TT_Mag
FROM DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic
WHERE top_anormal=0
GROUP BY 1,2
UNION
SELECT '00-Semaine' AS enseigne, Sem_vente AS semaine,
Min(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS min_date_2024,
Max(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS max_date_2024,
Min(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS min_date_2023,
Max(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS max_date_2023, 
count(DISTINCT ID_MAGASIN ) AS nb_mag,
SUM(CASE WHEN anne_ticket=2024 THEN SUM_PASSAGE end) AS nb_PASSAGE2024, 
SUM(CASE WHEN anne_ticket=2024 THEN nb_ticket_glb end) AS nb_ticket2024,
CASE WHEN nb_PASSAGE2024 IS NOT NULL AND nb_PASSAGE2024>0 THEN ROUND(nb_ticket2024/nb_PASSAGE2024,4) END AS TT_Mag_2024, 
SUM(CASE WHEN anne_ticket=2023 THEN SUM_PASSAGE end) AS nb_PASSAGE2023, 
SUM(CASE WHEN anne_ticket=2023 THEN nb_ticket_glb end) AS nb_ticket2023, 
CASE WHEN nb_PASSAGE2023 IS NOT NULL AND nb_PASSAGE2023>0 THEN ROUND(nb_ticket2023/nb_PASSAGE2023,4) END AS TT_Mag_2023, 
ROUND(TT_Mag_2024-TT_Mag_2023,4)*100 AS ecart_pts_TT_Mag,
CASE WHEN TT_Mag_2023 IS NOT NULL AND TT_Mag_2023>0 THEN ROUND((TT_Mag_2024-TT_Mag_2023)/TT_Mag_2023,4) END AS Evol_TT_Mag
FROM DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic
WHERE top_anormal=0
GROUP BY 1,2
UNION
SELECT CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS enseigne, Sem_vente AS semaine,
Min(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS min_date_2024,
Max(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS max_date_2024,
Min(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS min_date_2023,
Max(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS max_date_2023, 
count(DISTINCT ID_MAGASIN ) AS nb_mag,
SUM(CASE WHEN anne_ticket=2024 THEN SUM_PASSAGE end) AS nb_PASSAGE2024, 
SUM(CASE WHEN anne_ticket=2024 THEN nb_ticket_glb end) AS nb_ticket2024,
CASE WHEN nb_PASSAGE2024 IS NOT NULL AND nb_PASSAGE2024>0 THEN ROUND(nb_ticket2024/nb_PASSAGE2024,4) END AS TT_Mag_2024, 
SUM(CASE WHEN anne_ticket=2023 THEN SUM_PASSAGE end) AS nb_PASSAGE2023, 
SUM(CASE WHEN anne_ticket=2023 THEN nb_ticket_glb end) AS nb_ticket2023, 
CASE WHEN nb_PASSAGE2023 IS NOT NULL AND nb_PASSAGE2023>0 THEN ROUND(nb_ticket2023/nb_PASSAGE2023,4) END AS TT_Mag_2023, 
ROUND(TT_Mag_2024-TT_Mag_2023,4)*100 AS ecart_pts_TT_Mag,
CASE WHEN TT_Mag_2023 IS NOT NULL AND TT_Mag_2023>0 THEN ROUND((TT_Mag_2024-TT_Mag_2023)/TT_Mag_2023,4) END AS Evol_TT_Mag
FROM DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic
WHERE top_anormal=0
GROUP BY 1,2)
ORDER BY 1,2; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_V0_trafic ORDER BY 1,2;  


---- Taux de TT des magasins sur la période d'étude 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.stat_V1_trafic_mag AS 
WITH tab0 AS (
SELECT CASE 
 	WHEN ID_ORG_ENSEIGNE = 1 THEN '01-JULES'
 	WHEN ID_ORG_ENSEIGNE = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS enseigne, ID_MAGASIN, LIB_MAGASIN,
Min(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS min_date_2024,
Max(CASE WHEN anne_ticket=2024 THEN date_ticket end) AS max_date_2024,
Min(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS min_date_2023,
Max(CASE WHEN anne_ticket=2023 THEN date_ticket end) AS max_date_2023,
count(DISTINCT ID_MAGASIN ) AS nb_mag,
SUM(CASE WHEN anne_ticket=2024 THEN SUM_PASSAGE end) AS nb_PASSAGE2024, 
SUM(CASE WHEN anne_ticket=2024 THEN nb_ticket_glb end) AS nb_ticket2024,
CASE WHEN nb_PASSAGE2024 IS NOT NULL AND nb_PASSAGE2024>0 THEN ROUND(nb_ticket2024/nb_PASSAGE2024,4) END AS TT_Mag_2024, 
SUM(CASE WHEN anne_ticket=2023 THEN SUM_PASSAGE end) AS nb_PASSAGE2023, 
SUM(CASE WHEN anne_ticket=2023 THEN nb_ticket_glb end) AS nb_ticket2023, 
CASE WHEN nb_PASSAGE2023 IS NOT NULL AND nb_PASSAGE2023>0 THEN ROUND(nb_ticket2023/nb_PASSAGE2023,4) END AS TT_Mag_2023, 
ROUND(TT_Mag_2024-TT_Mag_2023,4)*100 AS ecart_pts_TT_Mag,
CASE WHEN TT_Mag_2023 IS NOT NULL AND TT_Mag_2023>0 AND TT_Mag_2024 IS NOT NULL AND TT_Mag_2024>0 THEN ROUND((TT_Mag_2024-TT_Mag_2023)/TT_Mag_2023,4) END AS Evol_TT_Mag
FROM DATA_MESH_PROD_RETAIL.WORK.BASE_calcul_trafic
WHERE top_anormal=0
GROUP BY 1,2,3), 
tab1 AS (SELECT DISTINCT enseigne, ID_MAGASIN, Evol_TT_Mag FROM tab0 WHERE Evol_TT_Mag IS NOT NULL), 
tab2 AS (SELECT *, CASE WHEN Evol_TT_Mag IS NOT NULL then  NTILE(4) OVER(ORDER BY Evol_TT_Mag) END AS QUARTILE FROM tab1)
SELECT a.*, QUARTILE 
FROM tab0 a 
LEFT JOIN tab2 b ON a.enseigne=b.enseigne AND a.ID_MAGASIN=b.ID_MAGASIN
ORDER BY 1,2,3; 



SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.stat_V1_trafic_mag ORDER BY 1,2,3;  


SELECT QUARTILE, count(DISTINCT ID_MAGASIN ) AS nb_mag, 
MIN(Evol_TT_Mag) AS Min_Evol_TT_Mag,
MAX(Evol_TT_Mag) AS Max_Evol_TT_Mag,
MIN(TT_Mag_2024) AS Min_TT_Mag_2024,
MAX(TT_Mag_2024) AS Max_TT_Mag_2024,
MIN(TT_Mag_2023) AS Min_TT_Mag_2023,
MAX(TT_Mag_2023) AS Max_TT_Mag_2023
FROM DATA_MESH_PROD_RETAIL.WORK.stat_V1_trafic_mag 
GROUP BY 1 
ORDER BY 1 ; 



-- Analyse de la performance des magasins par quartiles 

SELECT DISTINCT code_evt_promo FROM DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE;

SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN ; 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag AS 
WITH
Inf_q AS ( SELECT DISTINCT  CASE 
 	WHEN ENSEIGNE ='01-JULES' THEN 1
 	WHEN ENSEIGNE = '02-BRICE' THEN 3 ELSE 0
 END  AS Ens, Id_magasin, Lib_magasin, TT_mag_2024, TT_mag_2023, Ecart_Pts_TT_mag, Evol_TT_Mag, Quartile FROM DATA_MESH_PROD_RETAIL.WORK.stat_V1_trafic_mag ),
Magasin AS (
SELECT DISTINCT a.ID_ORG_ENSEIGNE, a.ID_MAGASIN, a.type_emplacement,a.code_magasin, a.lib_magasin, a.lib_statut, a.id_concept, a.lib_enseigne, a.code_pays, a.gpe_collectionning,
a.date_ouverture_public, a.date_fermeture_public, a.code_postal, a.surface_commerciale, a.id_franchise, a.lib_franchise, a.id_magasin_cible, a.code_magasin_cible, a.date_bascule_cible, 
CASE WHEN a.type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN a.type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE, 
b.libelle_sous_nature, b.id_region, b.libelle_region, b.id_grande_region, b.libelle_grande_region
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN a 
LEFT JOIN DATA_MESH_PROD_RETAIL.SHARED.DMD_ORG_MAGASIN_JUL b ON a.ID_ORG_ENSEIGNE=b.ID_ORG_ENSEIGNE AND a.ID_MAGASIN=b.ID_MAGASIN),
code_am_jul AS (SELECT DISTINCT id_enseigne, id_action_marketing, libelle_action_marketing, observations, id_type_action_marketing,libelle_action_marketing_bo,
categorie_action_marketing 
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_ACTION_MARKETING ORDER BY 1,2), 
produit as (
    select distinct ref.ID_REFERENCE, ref.ID_FAMILLE_ACHAT, fam.LIB_FAMILLE_ACHAT,
        G.LIB_GROUPE_FAMILLE
    from DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_REFERENCE ref
    join DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_FAMILLE_ACHAT fam 
        on ref.ID_FAMILLE_ACHAT = fam.ID_FAMILLE_ACHAT
    INNER JOIN DATA_MESH_PROD_OFFRE.HUB.DMD_PRD_GROUPE_FAMILLE_ACHAT G
        ON G.ID_GROUPE_FAMILLE = fam.id_groupe_famille
    where ref.est_version_courante = 1 and ref.id_marque = 'JUL'),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.PRIX_INIT_VENTE,  vd.PRIX_unitaire_base,
vd.PRIX_INIT_VENTE*vd.QUANTITE_LIGNE AS montant_init,
vd.PRIX_unitaire*vd.QUANTITE_LIGNE AS montant_unitaire,
vd.PRIX_unitaire_base*vd.QUANTITE_LIGNE AS montant_unitaire_base,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, code_AM, Code_ope_comm, code_remise, code_evt_promo,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, am.*,
mag.*, TT_mag_2024, TT_mag_2023, Ecart_Pts_TT_mag, Evol_TT_Mag, Quartile,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
INNER JOIN Inf_q k ON vd.ID_ORG_ENSEIGNE = k.ENS and vd.ID_MAGASIN = k.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
LEFT JOIN code_am_jul am ON vd.ID_ORG_ENSEIGNE=am.ID_ENSEIGNE AND vd.code_AM=id_action_marketing
where (DATE(vd.date_ticket) BETWEEN $dtdeb AND $dtfin OR DATE(vd.date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1)
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  ) 
SELECT * FROM tickets; 

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag;

-- rajout d'info client 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2 AS 
SELECT a.*, date(DATE_PREMIER_ACHAT) AS Date_First
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE b ON a.CODE_CLIENT=b.CODE_CLIENT  AND b.code_client IS NOT NULL AND b.code_client !='0' ; 


 SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2 ; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.STAT_trafic_mag_v2 AS 
SELECT * FROM (
SELECT '00-Global' AS grp, '00-Global' AS Typo, '00-Global' AS modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
UNION 
SELECT '01-Quartile' AS grp, '01-Quartile' AS Typo, Concat('Quartile ',Quartile) AS modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
UNION 
SELECT '02-ENSEIGNE' AS grp, Concat('Quartile ',Quartile) AS Typo, CASE 
 	WHEN idorgens_achat = 1 THEN '01-JULES'
 	WHEN idorgens_achat = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
UNION 
SELECT '03-type_emplacement' AS grp, Concat('Quartile ',Quartile) AS Typo, type_emplacement AS modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
UNION 
SELECT '04-REGION' AS grp, Concat('Quartile ',Quartile) AS Typo, CONCAT(id_region,'_',libelle_region) AS modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
UNION 
SELECT '05-GRANDE REGION' AS grp, Concat('Quartile ',Quartile) AS Typo, CONCAT(id_grande_region,'_',libelle_grande_region) AS modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
UNION 
SELECT '06-GROUPE_FAMILLE' AS grp, Concat('Quartile ',Quartile) AS Typo, CASE 
            WHEN LIB_FAMILLE_ACHAT = 'Bermuda' THEN 'Bermuda'
             WHEN LIB_FAMILLE_ACHAT = 'Pantalon Denim' THEN 'Pantalon Denim'
            WHEN LIB_FAMILLE_ACHAT = 'Underwear' THEN 'Underwear'
            WHEN LIB_FAMILLE_ACHAT IS NULL THEN 'Z-NC/NR'
            ELSE LIB_GROUPE_FAMILLE
          END AS  modalite, 
Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN id_ticket END) AS NBTICK_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN MONTANT_TTC END) AS CA_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin THEN QUANTITE_LIGNE  END) AS qte_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2024
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb AND $dtfin AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb AND $dtfin THEN code_client END) AS nb_newclt_2024
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN id_ticket END) AS NBTICK_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN MONTANT_TTC END) AS CA_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN QUANTITE_LIGNE END) AS qte_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_AM>0 THEN id_ticket END) AS NBTICK_AM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND CODE_OPE_COMM>0 THEN id_ticket END) AS NBTICK_OPECOMM_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nbtick_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  MONTANT_TTC END) AS CA_clt_2023
,SUM(CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' THEN  QUANTITE_LIGNE END) AS qte_clt_2023
,Count(DISTINCT CASE WHEN DATE(date_ticket) BETWEEN $dtdeb_m1 AND $dtfin_m1 AND code_client IS NOT NULL AND code_client !='0' AND DATE(Date_First) BETWEEN $dtdeb_m1 AND $dtfin_m1 THEN code_client END) AS nb_newclt_2023
FROM DATA_MESH_PROD_RETAIL.WORK.ventes_trafic_mag_v2
GROUP BY 1,2,3
);

SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.STAT_trafic_mag_v2 ORDER BY 1,2,3; 



