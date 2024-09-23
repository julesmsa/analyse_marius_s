-- Report Magasin Client 

/*** Liste des magasins concernés *****/

SELECT * FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN; 

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


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_test_sml AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND lib_statut='Fermé' AND YEAR (date_fermeture_public)>=2021),
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
mag.*,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
where vd.date_ticket BETWEEN dateadd('year', -1, date_fermeture_public) AND DATE(date_fermeture_public) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  ) 
SELECT * FROM tickets ; 


SELECT * FROM tab_mag_test_sml; 
--- statistique des achats et autres des magasins 121 mois avant leur fermeture 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_test_sml AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND lib_statut='Fermé' AND YEAR (date_fermeture_public)>=2021),
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
FROM tab_mag_test_sml a 
GROUP BY 1,2)
SELECT a.*, b.*
FROM stat0 a 
inner join Magasin b  on a.idorgens_achat = b.ID_ORG_ENSEIGNE and a.idmag_achat = b.ID_MAGASIN
ORDER BY 1,2; 


--  Stat suivant les catégorie des magasins 

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
