-- Report Magasin Client 

/*** Liste des magasins concernés *****/

SET dtdeb_EXON = Date('2021-05-01');
SET dtfin_EXON = DAte('2024-04-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_EXON, $dtfin_EXON;

SET dtdeb_EXONm1 = to_date(dateadd('year', -1, $dtdeb_EXON));
SET dtfin_EXONm1 = to_date(dateadd('year', -1, $dtfin_EXON));

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb_EXON, $dtfin_EXON, $dtdeb_EXONm1, $dtfin_EXONm1, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND lib_statut='Fermé' AND YEAR (date_fermeture_public)>=2021),
type_mag AS ( SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_ferm FROM DATA_MESH_PROD_CLIENT.WORK.STATUT_MAG_FERME),
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
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
mag.*, c.type_ferm,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
INNER JOIN type_mag c on vd.ID_ORG_ENSEIGNE = c.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = c.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
where vd.date_ticket BETWEEN dateadd('month', -18, date_fermeture_public) AND DATE(date_fermeture_public) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  ) 
SELECT * FROM tickets ; 

SELECT * FROM tab_mag_histo_18mth; 
--- statistique des achats et autres des magasins 121 mois avant leur fermeture 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_histo_18mth AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) AND lib_statut='Fermé' AND YEAR (date_fermeture_public)>=2021),
type_mag AS ( SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_ferm FROM DATA_MESH_PROD_CLIENT.WORK.STATUT_MAG_FERME),
stat0 AS (
SELECT idorgens_achat, idmag_achat 
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab_mag_histo_18mth 
GROUP BY 1,2)
SELECT a.*, b.*, c.type_ferm
FROM stat0 a 
inner join Magasin b  on a.idorgens_achat = b.ID_ORG_ENSEIGNE and a.idmag_achat = b.ID_MAGASIN
INNER JOIN type_mag c on a.idorgens_achat = c.ID_ORG_ENSEIGNE and a.idmag_achat = c.ID_MAGASIN
ORDER BY 1,2; 


SELECT * FROM stat_mag_histo_18mth; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_GLB_histo_18mth AS
SELECT * FROM (
(SELECT '00_GLOBAL' as typo, '00_GLOBAL' AS modalite,
COUNT(DISTINCT id_magasin) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab_mag_histo_18mth
GROUP BY 1,2
ORDER BY 1,2)
UNION 
(SELECT '01_TYPE_MAGASIN' as typo, type_ferm AS modalite, 
COUNT(DISTINCT id_magasin) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab_mag_histo_18mth 
GROUP BY 1,2
ORDER BY 1,2)
UNION  
(SELECT '02_TYPE_EMPLACEMENT' as typo, TYPE_EMPLACEMENT AS modalite, 
COUNT(DISTINCT id_magasin) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab_mag_histo_18mth 
GROUP BY 1,2
ORDER BY 1,2)
UNION  
(SELECT '03_ANNEE_FERMETURE' as typo, CONCAT('AN_',YEAR(date_fermeture_public)) AS modalite, 
COUNT(DISTINCT id_magasin) AS nb_mag, 
ROUND (AVG (datediff(YEAR ,date_ouverture_public,date_fermeture_public)),1) AS duree_moy_act
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab_mag_histo_18mth 
GROUP BY 1,2
ORDER BY 1,2)
ORDER BY 1,2);
    
SELECT * FROM stat_GLB_histo_18mth ORDER BY 1,2;     


-- SELECT * FROM tab_mag_histo_18mth; 

--- Analyse des ventes sur les 

--  Stat suivant les catégorie des magasins

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_vte_18mth AS
WITH type_mag AS ( SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_ferm FROM DATA_MESH_PROD_CLIENT.WORK.STATUT_MAG_FERME),
clt_magasin AS (
SELECT DISTINCT a.ID_ORG_ENSEIGNE AS enseigne_ferm, a.ID_MAGASIN AS idmag_ferm, mg.lib_magasin, a.date_fermeture_public, code_client AS id_client,type_ferm
FROM tab_mag_histo_18mth a
INNER JOIN type_mag c on a.ID_ORG_ENSEIGNE = c.ID_ORG_ENSEIGNE and a.ID_MAGASIN = c.ID_MAGASIN
INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mg on a.ID_ORG_ENSEIGNE = mg.ID_ORG_ENSEIGNE and a.ID_MAGASIN = mg.ID_MAGASIN
WHERE code_client IS NOT NULL AND code_client !='0' ),
cltid AS (SELECT DISTINCT code_client AS id_client
FROM tab_mag_histo_18mth WHERE code_client IS NOT NULL AND code_client !='0'),
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
Select vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.PRIX_INIT_VENTE,  vd.PRIX_unitaire_base,
vd.PRIX_INIT_VENTE*vd.QUANTITE_LIGNE AS montant_init,
vd.PRIX_unitaire*vd.QUANTITE_LIGNE AS montant_unitaire,
vd.PRIX_unitaire_base*vd.QUANTITE_LIGNE AS montant_unitaire_base,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket  
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join cltid id  on vd.code_client = id.id_client
INNER  JOIN clt_magasin mag ON vd.code_client = mag.id_client
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
where vd.date_ticket BETWEEN dateadd('day', +1, mag.date_fermeture_public) AND dateadd('month', +18, mag.date_fermeture_public) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)) 
SELECT mag.*, tic.*  
,CASE WHEN date_ticket<=date_fermeture_public THEN NULL ELSE datediff(MONTH ,date_fermeture_public,date_ticket) END AS delai_reachat
FROM clt_magasin mag
LEFT JOIN tickets tic ON mag.id_client=tic.CODE_CLIENT;


SELECT * FROM tab_mag_vte_18mth WHERE code_client='072920000415' ORDER BY date_fermeture_public, DATE_TICKET ; -- Client ayant acheté dans des magasins ayant ferme ensuite 



 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb AS
    SELECT enseigne_ferm, idmag_ferm, lib_magasin, date_fermeture_public, type_ferm
  ,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 18 THEN code_client END ) AS nbclt_actif_18mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth
  FROM tab_mag_vte_18mth
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5;
 
 SELECT * FROM stat_mag_vte18mth_glb ORDER BY 1,2,3,4,5;

 
 --- tableau des statistiques pour toper les mag ou le client s'est rendu !!!
 
  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag AS
  SELECT a.enseigne_ferm, a.idmag_ferm, a.lib_magasin AS lib_magasin_ferm, type_ferm, a.date_fermeture_public, a.idorgens_achat AS enseigne_actif , a.mag_achat , mg.lib_magasin AS lib_magasin_actif
  --,Count(DISTINCT id_client ) AS nb_client_potentiel
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 18 THEN code_client END ) AS nbclt_actif_18mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 3 THEN code_client END ) AS nbclt_actif_03mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 6 THEN code_client END ) AS nbclt_actif_06mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 9 THEN code_client END ) AS nbclt_actif_09mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 12 THEN code_client END ) AS nbclt_actif_12mth_mag
  ,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' AND delai_reachat IS NOT NULL AND delai_reachat BETWEEN 0 AND 15 THEN code_client END ) AS nbclt_actif_15mth_mag
  FROM tab_mag_vte_18mth a
  INNER JOIN DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mg on a.idorgens_achat = mg.ID_ORG_ENSEIGNE and a.mag_achat = mg.ID_MAGASIN
  GROUP BY 1,2,3,4,5,6,7,8
  ORDER BY 1,2,3,4,5,6,7,8;
  
  
  SELECT * FROM stat_mag_vte18mth_glb_mag ORDER BY enseigne_ferm, idmag_ferm, nbclt_actif_18mth_mag DESC ; 
 
 -- nous allons integrer les informations sur le nombre de cliebt potentiel 
 
  CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_vte18mth_glb_mag_V2 AS
 WITH tab0 AS (SELECT DISTINCT a.enseigne_ferm, a.idmag_ferm, a.lib_magasin_ferm, a.type_ferm, a.date_fermeture_public, a.enseigne_actif , a.mag_achat , a.lib_magasin_actif,
 b.nb_client_potentiel, b.nbclt_actif_18mth, a.nbclt_actif_18mth_mag, 
 a.nbclt_actif_03mth_mag, a.nbclt_actif_06mth_mag, a.nbclt_actif_09mth_mag, a.nbclt_actif_12mth_mag, a.nbclt_actif_15mth_mag
FROM stat_mag_vte18mth_glb_mag a
LEFT JOIN stat_mag_vte18mth_glb b ON a.enseigne_ferm=b.enseigne_ferm AND  a.idmag_ferm=b.idmag_ferm ),
tab1 AS (SELECT *, 
rank() over(partition by idmag_ferm order by nbclt_actif_18mth_mag DESC) as rang_mag,
ROW_NUMBER() over(partition by idmag_ferm order by nbclt_actif_18mth_mag DESC) as lign_mag
 FROM tab0)
SELECT DISTINCT enseigne_ferm, idmag_ferm, lib_magasin_ferm, type_ferm, date_fermeture_public, enseigne_actif , mag_achat , lib_magasin_actif,
 nb_client_potentiel, nbclt_actif_18mth, nbclt_actif_18mth_mag, 
 nbclt_actif_03mth_mag, nbclt_actif_06mth_mag, nbclt_actif_09mth_mag, nbclt_actif_12mth_mag, nbclt_actif_15mth_mag, rang_mag 
FROM tab1 WHERE rang_mag<=10; 

SELECT * FROM stat_mag_vte18mth_glb_mag_V2 ORDER BY enseigne_ferm, idmag_ferm, rang_mag; 

/*
SELECT * FROM stat_mag_vte18mth_glb_mag_V2 
WHERE enseigne_ferm=1 and idmag_ferm=19
ORDER BY  lign_mag; 



SELECT * FROM stat_mag_vte18mth_glb_mag_V2 
WHERE enseigne_ferm=1 and idmag_ferm=19
ORDER BY  nbclt_actif_18mth_mag DESC ; 

rank() over(partition by a.enseigne_ferm, a.idmag_ferm order by nbclt_actif_18mth_mag DESC) as rang_mag



/*
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_typomag_test_sml AS
WITH tab0 AS ( SELECT *, datediff(YEAR ,date_ouverture_public, date_fermeture_public) AS dure_vie_mag
FROM tab_mag_test_sml)
SELECT * FROM (
(SELECT '00-Global' AS typo, '00-Global' AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '01-Enseigne' AS typo, CASE 
 	WHEN idorgens_achat = 1 THEN '01-JULES'
 	WHEN idorgens_achat = 3 THEN '02-BRICE' ELSE '03-Autres'
 END  AS  modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '02-Type emplacement' AS typo, type_emplacement AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '03-Type Concept' AS typo, id_concept AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '04-Pays' AS typo, code_pays AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '05-GPE Collectionning' AS typo, GPE_collectionning AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '06-Franchise' AS typo, CASE WHEN id_franchise IS NOT NULL THEN '01-Oui' ELSE '02-Non' END AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
UNION 
(SELECT '08-Perimetre' AS typo, perimetre AS modalite
,Count(DISTINCT idmag_achat) AS nb_mag_glb
,ROUND(AVG (dure_vie_mag),1) AS vie_mag_moy
,ROUND(AVG (surface_commerciale),1) AS surface_moy
,Count(DISTINCT id_ticket) AS nb_ticket_glb
,SUM(MONTANT_TTC) AS CA_glb
,SUM(QUANTITE_LIGNE) AS qte_achete_glb
,SUM(MONTANT_MARGE_SORTIE) AS Marge_glb	
,SUM(montant_remise) AS Mnt_remise_glb
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM tab0
GROUP BY 1,2)
)ORDER BY 1, 2; 


SELECT *, datediff(YEAR ,date_ouverture_public, date_fermeture_public) AS dure_vie_mag
FROM stat_mag_test_sml ORDER BY 1, 2; 


SELECT * FROM stat_typomag_test_sml ORDER BY 1, 2; 

SELECT DISTINCT CODE_CONCEPT, LIBELLE_CONCEPT , FLAG_DERNIER_CONCEPT  FROM DATA_MESH_PROD_RETAIL.HUB.DMD_ORG_CONCEPT ORDER BY 1;
