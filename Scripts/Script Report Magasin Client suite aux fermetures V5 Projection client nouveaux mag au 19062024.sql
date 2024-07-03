
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SET dtfin_etud = Date('2024-05-31');

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.t_histo_18mth_nm AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) 
AND ID_MAGASIN IN (5,6,15,16,46,52,54,63,71,97,106,119,130,133,137,141,147,
155,174,193,195,224,227,230,231,256,288,294,315,329,343,
350,351,368,377,405,428,430,435,450,453,455,460,461,462,
475,483,487,493,495,725,728,811,812,817,823,859,868,3352,
3517,3520,3575,3618,3622,3641,3643)
),
type_mag AS ( SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, 'PROJXXX' AS type_ferm 
FROM Magasin),
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
COUNT(DISTINCT id_ticket) Over (partition by vd.CODE_CLIENT) as NB_tick_clt,
mag.*,
pdt.ID_FAMILLE_ACHAT, pdt.LIB_FAMILLE_ACHAT, pdt.LIB_GROUPE_FAMILLE
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
inner join Magasin mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN
LEFT join produit pdt on vd.CODE_REFERENCE = pdt.ID_REFERENCE
where vd.date_ticket BETWEEN dateadd('month', -18, $dtfin_etud)+1 AND DATE($dtfin_etud) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2)
  ) 
SELECT * FROM tickets;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.t_histo_18mth_nm;


-- Definir 'sil s'agit du seul magasin frequenté au non sur les 18 derniers mois 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.mod_clt_nm AS
WITH top_clt as (
Select  DISTINCT CODE_CLIENT AS id_client
FROM DATA_MESH_PROD_CLIENT.WORK.t_histo_18mth_nm 
WHERE code_client IS NOT NULL AND code_client !='0'), 
vte_clt AS (
SELECT CODE_CLIENT,
Count(DISTINCT vd.ID_MAGASIN) AS nb_mag
from DATA_MESH_PROD_RETAIL.WORK.VENTE_DENORMALISE vd
INNER JOIN top_clt tc ON vd.CODE_CLIENT = tc.id_client
inner join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN mag  on vd.ID_ORG_ENSEIGNE = mag.ID_ORG_ENSEIGNE and vd.ID_MAGASIN = mag.ID_MAGASIN AND type_emplacement IN ('PAC','CC', 'CV','CCV') 
where vd.date_ticket BETWEEN dateadd('month', -18, $dtfin_etud) AND DATE($dtfin_etud) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND code_client IS NOT NULL AND code_client !='0'
  GROUP BY 1)
  SELECT *, CASE 
WHEN NB_mag=1 THEN '01-Mono MAG' 
WHEN NB_mag=2 THEN '02- 2 MAG' 
WHEN NB_mag>=3 THEN '03- 3 MAG et +' 
END AS Mag_client
  FROM vte_clt ; 
  
  
  SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.mod_clt_nm;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.t_histo_18mth_nm2 AS 
SELECT a.*, b.Mag_client 
FROM DATA_MESH_PROD_CLIENT.WORK.t_histo_18mth_nm a
INNER JOIN DATA_MESH_PROD_CLIENT.WORK.mod_clt_nm b ON a.CODE_CLIENT=b.CODE_CLIENT ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.t_histo_18mth_nm2;
 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_mag_histo_18mth AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2) ),
stat0 AS (
SELECT idorgens_achat, idmag_achat , '00-Global' AS typo
,MIN(date_ticket) AS min_date_ticket 
,MAX(date_ticket)  AS max_date_ticket
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM t_histo_18mth_nm2 
GROUP BY 1,2,3
UNION
SELECT idorgens_achat, idmag_achat , Mag_client AS typo
,MIN(date_ticket) AS min_date_ticket 
,MAX(date_ticket)  AS max_date_ticket
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt	
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN montant_remise END) AS Mnt_remise_clt
FROM t_histo_18mth_nm2 
GROUP BY 1,2,3)
SELECT a.*, b.*,
datediff(MONTH ,min_date_ticket,max_date_ticket) AS periode_etud
FROM stat0 a 
inner join Magasin b on a.idorgens_achat = b.ID_ORG_ENSEIGNE and a.idmag_achat = b.ID_MAGASIN
ORDER BY 1,2,3; 


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_mag_histo_18mth ORDER BY 1,2,3; 


 --- Statistiques des clients par magasins 
 -- Objectif
 WHEN NB_mag=1 THEN '01-Mono MAG' 
WHEN NB_mag=2 THEN '02- 2 MAG' 
WHEN NB_mag>=3 THEN '03- 3 MAG et +' 


SELECT * , 
CASE 
WHEN typo= '01-Mono MAG' THEN 0.259
WHEN typo= '02- 2 MAG'  THEN 0.499
WHEN typo= '03- 3 MAG et +' THEN  0.684
WHEN typo= '00-Global' THEN  0.391 END AS tx_reachat
, 
CASE 
WHEN typo= '01-Mono MAG' THEN 117
WHEN typo= '02- 2 MAG'  THEN 171.6
WHEN typo= '03- 3 MAG et +' THEN  279.9
WHEN typo= '00-Global' THEN  185.8 END AS tx_reachat



FROM DATA_MESH_PROD_CLIENT.WORK.stat_mag_histo_18mth ORDER BY 1,2,3;   
 
 