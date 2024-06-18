--- KPI Omnicanalite client 


SET dtdeb_EXON = Date('2023-01-01');
SET dtfin_EXON = DAte('2023-12-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_EXON, $dtfin_EXON;

SET dtdeb_EXONm1 = to_date(dateadd('year', -1, $dtdeb_EXON));
SET dtfin_EXONm1 = to_date(dateadd('year', -1, $dtfin_EXON)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Mag2023 AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
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
where vd.date_ticket BETWEEN DATE($dtdeb_EXON) AND DATE($dtfin_EXON) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND code_client IS NOT NULL AND code_client !='0'
  ) 
SELECT * 
,Max(CASE WHEN type_emplacement IN ('EC','MP') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_web
,Max(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_mag
FROM tickets ; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stattab_Mag2023 AS
SELECT 'ANNEE 2023' AS Periode,* FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 	
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2023
    GROUP BY 1,2)
UNION 
(SELECT '01_CANAL' AS typo_clt, 
CASE 
WHEN top_mag=1 AND top_web=0 THEN '01-Client ONLY MAG' 
WHEN top_mag=0 AND top_web=1 THEN '02-Client ONLY WEB' 
WHEN top_mag=1 AND top_web=1 THEN '03-Client OMNI'
ELSE '09-Autres' END AS modalite
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2023
    GROUP BY 1,2))
        ORDER BY 1,2 ;            
       
SELECT * FROM stattab_Mag2023 ORDER BY 1,2 ;  

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stattab_Mag2023_canal AS
SELECT 'ANNEE 2023' AS Periode,* FROM (
(SELECT PERIMETRE AS typo_clt, '00_GLOBAL' AS modalite 	
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2023
    GROUP BY 1,2)
UNION 
(SELECT PERIMETRE AS typo_clt, 
CASE 
WHEN top_mag=1 AND top_web=0 THEN '01-Client ONLY MAG' 
WHEN top_mag=0 AND top_web=1 THEN '02-Client ONLY WEB' 
WHEN top_mag=1 AND top_web=1 THEN '03-Client OMNI'
ELSE '09-Autres' END AS modalite
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2023
    GROUP BY 1,2))
        ORDER BY 1,2 ;  
       
SELECT * FROM stattab_Mag2023_canal ORDER BY 1,2 ;       
       
       
-- pour l'année 2022 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Mag2022 AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
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
where vd.date_ticket BETWEEN DATE($dtdeb_EXONm1) AND DATE($dtfin_EXONm1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND code_client IS NOT NULL AND code_client !='0'
  ) 
SELECT * 
,Max(CASE WHEN type_emplacement IN ('EC','MP') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_web
,Max(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_mag
FROM tickets ; 



CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stattab_Mag2022 AS
SELECT 'ANNEE 2022' AS Periode,* FROM (
(SELECT '00_GLOBAL' AS typo_clt, '00_GLOBAL' AS modalite 
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2022
    GROUP BY 1,2)
UNION 
(SELECT '01_CANAL' AS typo_clt, 
CASE 
WHEN top_mag=1 AND top_web=0 THEN '01-Client ONLY MAG' 
WHEN top_mag=0 AND top_web=1 THEN '02-Client ONLY WEB' 
WHEN top_mag=1 AND top_web=1 THEN '03-Client OMNI'
ELSE '09-Autres' END AS modalite
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2022
    GROUP BY 1,2))
        ORDER BY 1,2 ;            
       
SELECT * FROM stattab_Mag2022 ORDER BY 1,2 ; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stattab_Mag2022_canal AS
SELECT 'ANNEE 2022' AS Periode,* FROM (
(SELECT PERIMETRE AS typo_clt, '00_GLOBAL' AS modalite 	
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2022
    GROUP BY 1,2)
UNION 
(SELECT PERIMETRE AS typo_clt, 
CASE 
WHEN top_mag=1 AND top_web=0 THEN '01-Client ONLY MAG' 
WHEN top_mag=0 AND top_web=1 THEN '02-Client ONLY WEB' 
WHEN top_mag=1 AND top_web=1 THEN '03-Client OMNI'
ELSE '09-Autres' END AS modalite
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN code_client END) AS nb_clt
,Count(DISTINCT CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN id_ticket END) AS nb_ticket_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_TTC END) AS CA_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN QUANTITE_LIGNE END) AS qte_achete_clt
,SUM(CASE WHEN code_client IS NOT NULL AND code_client !='0' THEN MONTANT_MARGE_SORTIE END) AS Marge_clt
FROM tab_Mag2022
    GROUP BY 1,2))
        ORDER BY 1,2 ;  
       
SELECT * FROM stattab_Mag2022_canal ORDER BY 1,2 ;     


-- Analyse complementaire client Omni avec notion de fréquence d'achat associé 

-- par decile le poids de client omni , pur mag et pur web pour l'année 2022 et 2023 



SET dtdeb_EXON = Date('2023-01-01');
SET dtfin_EXON = DAte('2023-12-31'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb_EXON, $dtfin_EXON;

SET dtdeb_EXONm1 = to_date(dateadd('year', -1, $dtdeb_EXON));
SET dtfin_EXONm1 = to_date(dateadd('year', -1, $dtfin_EXON)); 

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Mag2023 AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
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
where vd.date_ticket BETWEEN DATE($dtdeb_EXON) AND DATE($dtfin_EXON) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND code_client IS NOT NULL AND code_client !='0'
  ), 
  tab0 AS ( 
SELECT * 
,Max(CASE WHEN type_emplacement IN ('EC','MP') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_web
,Max(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_mag 
,SUM(CASE WHEN type_emplacement IN ('EC','MP') THEN MONTANT_TTC ELSE 0 END) Over (partition by CODE_CLIENT) as top_web_ca
,SUM(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN MONTANT_TTC ELSE 0 END) Over (partition by CODE_CLIENT) as top_mag_ca 
FROM tickets)
SELECT * , 
CASE 
WHEN top_mag=1 AND top_mag_ca>0 AND top_web=0 AND top_web_ca=0 THEN '01-Client ONLY MAG' 
WHEN top_mag=0 AND top_mag_ca=0 AND top_web=1 AND top_web_ca>0 THEN '02-Client ONLY WEB' 
WHEN top_mag=1 AND top_web=1 AND top_mag_ca>0 AND top_web_ca >0 THEN '03-Client OMNI'
ELSE '09-Autres' END AS canal 
FROM tab0 ; 

  -- agregation des informations niveau client 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023 AS 
WITH tab0 AS (
SELECT code_client, canal 
,Count(DISTINCT id_ticket) AS nb_ticket_clt
,SUM(MONTANT_TTC) AS CA_clt
,SUM(QUANTITE_LIGNE) AS qte_achete_clt
,Count(DISTINCT CASE WHEN PERIMETRE='MAG' THEN id_ticket END) AS nb_ticket_mag
,SUM(CASE WHEN PERIMETRE='MAG' THEN MONTANT_TTC END) AS CA_mag
,SUM(CASE WHEN PERIMETRE='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_mag
,Count(DISTINCT CASE WHEN PERIMETRE='WEB' THEN id_ticket END) AS nb_ticket_web
,SUM(CASE WHEN PERIMETRE='WEB' THEN MONTANT_TTC END) AS CA_web
,SUM(CASE WHEN PERIMETRE='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_web
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_Mag2023 WHERE canal !='09-Autres'   
  GROUP BY 1,2 
 HAVING CA_clt>=1 ), -- ON prend que les client avec un CA positf 
tab1 AS (SELECT *, ROW_NUMBER() OVER (order by CA_Clt) AS row_num FROM tab0),
tab2 AS (SELECT *, MAX(row_num) over() AS max_nb_clt , 
FLOOR(row_num/(max_nb_clt/10)) + 1 AS decile FROM tab1)
SELECT * , CASE WHEN decile<10 THEN  CONCAT('DECILE 0',decile) ELSE CONCAT('DECILE ',decile) END AS tr_decile, 
CASE WHEN nb_ticket_clt=1 THEN '1 Ticket'
     WHEN nb_ticket_clt=2 THEN '2 Tickets'
     WHEN nb_ticket_clt>=3 THEN '3 Tickets et +' ELSE 'zzzz' END AS tr_nbticket
FROM tab2; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_Mag2023 AS 
SELECT * FROM (
SELECT '00-Global' AS  vision, '00-Global' AS tr_decile, '00-Global' AS canal, '00-Global' AS tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023 WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '01-TICKET' AS  vision, '00-Global' AS tr_decile, '00-Global' AS canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '02-CANAL' AS  vision, '00-Global' AS tr_decile, canal, '00-Global' AS tr_nbticket
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '03-DECILE' AS  vision, tr_decile, '00-Global' AS canal, '00-Global' AS tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '04-CANAL-TICKET' AS  vision, '00-Global' AS tr_decile, canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '05-DECILE-TICKET' AS  vision, tr_decile, '00-Global' AS canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '06-DECILE-CANAL-TICKET' AS  vision, tr_decile, canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023  WHERE decile<=10
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4;

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.stat_Mag2023 ORDER BY 1,2,3,4; 


-- Opéaration sur l'année 2022 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_Mag2022 AS
WITH Magasin AS (
SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, type_emplacement,code_magasin, lib_magasin, lib_statut, id_concept, lib_enseigne,code_pays,gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale, id_franchise, lib_franchise, id_magasin_cible, code_magasin_cible, date_bascule_cible, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE
FROM DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN
WHERE ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2 AND (code_pays = $PAYS1 or code_pays = $PAYS2)),
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
where vd.date_ticket BETWEEN DATE($dtdeb_EXONm1) AND DATE($dtfin_EXONm1) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (mag.code_pays = $PAYS1 or mag.code_pays = $PAYS2) AND code_client IS NOT NULL AND code_client !='0'
  ), 
  tab0 AS ( 
SELECT * 
,Max(CASE WHEN type_emplacement IN ('EC','MP') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_web
,Max(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 1 ELSE 0 END) Over (partition by CODE_CLIENT) as top_mag 
,SUM(CASE WHEN type_emplacement IN ('EC','MP') THEN MONTANT_TTC ELSE 0 END) Over (partition by CODE_CLIENT) as top_web_ca
,SUM(CASE WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN MONTANT_TTC ELSE 0 END) Over (partition by CODE_CLIENT) as top_mag_ca 
FROM tickets)
SELECT * , 
CASE 
WHEN top_mag=1 AND top_mag_ca>0 AND top_web=0 AND top_web_ca=0 THEN '01-Client ONLY MAG' 
WHEN top_mag=0 AND top_mag_ca=0 AND top_web=1 AND top_web_ca>0 THEN '02-Client ONLY WEB' 
WHEN top_mag=1 AND top_web=1 AND top_mag_ca>0 AND top_web_ca >0 THEN '03-Client OMNI'
ELSE '09-Autres' END AS canal 
FROM tab0 ; 

  -- agregation des informations niveau client 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022 AS 
WITH tab0 AS (
SELECT code_client, canal 
,Count(DISTINCT id_ticket) AS nb_ticket_clt
,SUM(MONTANT_TTC) AS CA_clt
,SUM(QUANTITE_LIGNE) AS qte_achete_clt
,Count(DISTINCT CASE WHEN PERIMETRE='MAG' THEN id_ticket END) AS nb_ticket_mag
,SUM(CASE WHEN PERIMETRE='MAG' THEN MONTANT_TTC END) AS CA_mag
,SUM(CASE WHEN PERIMETRE='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_mag
,Count(DISTINCT CASE WHEN PERIMETRE='WEB' THEN id_ticket END) AS nb_ticket_web
,SUM(CASE WHEN PERIMETRE='WEB' THEN MONTANT_TTC END) AS CA_web
,SUM(CASE WHEN PERIMETRE='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_web
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_Mag2022 WHERE canal !='09-Autres'   
  GROUP BY 1,2 
 HAVING CA_clt>=1 ), -- ON prend que les client avec un CA positf 
tab1 AS (SELECT *, ROW_NUMBER() OVER (order by CA_Clt) AS row_num FROM tab0),
tab2 AS (SELECT *, MAX(row_num) over() AS max_nb_clt , 
FLOOR(row_num/(max_nb_clt/10)) + 1 AS decile FROM tab1)
SELECT * , CASE WHEN decile<10 THEN  CONCAT('DECILE 0',decile) ELSE CONCAT('DECILE ',decile) END AS tr_decile, 
CASE WHEN nb_ticket_clt=1 THEN '1 Ticket'
     WHEN nb_ticket_clt=2 THEN '2 Tickets'
     WHEN nb_ticket_clt>=3 THEN '3 Tickets et +' ELSE 'zzzz' END AS tr_nbticket
FROM tab2; 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_Mag2022 AS 
SELECT * FROM (
SELECT '00-Global' AS  vision, '00-Global' AS tr_decile, '00-Global' AS canal, '00-Global' AS tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022 WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '01-TICKET' AS  vision, '00-Global' AS tr_decile, '00-Global' AS canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '02-CANAL' AS  vision, '00-Global' AS tr_decile, canal, '00-Global' AS tr_nbticket
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '03-DECILE' AS  vision, tr_decile, '00-Global' AS canal, '00-Global' AS tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '04-CANAL-TICKET' AS  vision, '00-Global' AS tr_decile, canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '05-DECILE-TICKET' AS  vision, tr_decile, '00-Global' AS canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4 
UNION 
SELECT '06-DECILE-CANAL-TICKET' AS  vision, tr_decile, canal, tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4)
ORDER BY 1,2,3,4;

SELECT * FROM  DATA_MESH_PROD_CLIENT.WORK.stat_Mag2022 ORDER BY 1,2,3,4; 


-- Notion de Decile et omnicanalite 

SELECT '05-DECILE-TICKET' AS  vision, tr_decile, canal, '00-Global' AS tr_nbticket 
,Count(DISTINCT code_client) AS nb_clt
,min(nb_ticket_clt) AS min_ticket_clt
,SUM(nb_ticket_clt) AS sum_ticket_clt
,max(nb_ticket_clt) AS max_ticket_clt
,min(CA_clt) AS min_CA_clt
,SUM(CA_clt) AS sum_CA_clt
,max(CA_clt) AS max_CA_clt
,Count(DISTINCT CASE WHEN nb_ticket_mag>0 THEN code_client END) AS nb_clt_mag
,min(nb_ticket_mag) AS min_ticket_mag
,SUM(nb_ticket_mag) AS sum_ticket_mag
,max(nb_ticket_mag) AS max_ticket_mag
,min(CA_mag) AS min_CA_mag
,SUM(CA_mag) AS sum_CA_mag
,max(CA_mag) AS max_CA_mag
,Count(DISTINCT CASE WHEN nb_ticket_web>0 THEN code_client END) AS nb_clt_web
,min(nb_ticket_web) AS min_ticket_web
,SUM(nb_ticket_web) AS sum_ticket_web
,max(nb_ticket_web) AS max_ticket_web
,min(CA_web) AS min_CA_web
,SUM(CA_web) AS sum_CA_web
,max(CA_web) AS max_CA_web
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022  WHERE decile<=10
GROUP BY 1,2,3,4 
order BY 1,2,3,4 


WITH 
tab0 AS (SELECT DISTINCT Code_client, canal AS canal22, nb_ticket_clt AS nb_ticket_clt22, CA_clt AS ca_clt22 
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2022), 
tab1 AS (SELECT DISTINCT Code_client, canal AS canal23, nb_ticket_clt AS nb_ticket_clt23, CA_clt AS ca_clt23 
FROM DATA_MESH_PROD_CLIENT.WORK.tabclt_Mag2023)
SELECT canal22, canal23 
,Count(DISTINCT a.code_client) AS nb_clt
,SUM(nb_ticket_clt22) AS sum_ticket_clt22
,SUM(nb_ticket_clt23) AS sum_ticket_clt23
,SUM(CA_clt22) AS sum_CA_clt22
,SUM(CA_clt23) AS sum_CA_clt23
FROM tab0 a
LEFT JOIN tab1 b ON a.code_client=b.code_client
GROUP BY 1,2
ORDER BY 1,2 ; 


SELECT code_client, canal 
,Count(DISTINCT id_ticket) AS nb_ticket_clt
,SUM(MONTANT_TTC) AS CA_clt
,SUM(QUANTITE_LIGNE) AS qte_achete_clt
,Count(DISTINCT CASE WHEN PERIMETRE='MAG' THEN id_ticket END) AS nb_ticket_mag
,SUM(CASE WHEN PERIMETRE='MAG' THEN MONTANT_TTC END) AS CA_mag
,SUM(CASE WHEN PERIMETRE='MAG' THEN QUANTITE_LIGNE END) AS qte_achete_mag
,Count(DISTINCT CASE WHEN PERIMETRE='WEB' THEN id_ticket END) AS nb_ticket_web
,SUM(CASE WHEN PERIMETRE='WEB' THEN MONTANT_TTC END) AS CA_web
,SUM(CASE WHEN PERIMETRE='WEB' THEN QUANTITE_LIGNE END) AS qte_achete_web
  FROM DATA_MESH_PROD_CLIENT.WORK.tab_Mag2022 WHERE canal ='09-Autres'   
  GROUP BY 1,2 





