--- Analyse Fermeture des magasins Jules 
    -- FOCUS Magasins fermeture seches

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

-- Date de fin ticket pour avoir des données stables 
SET dtfin = DAte('2024-09-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
SELECT $dtfin;


SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.STATUT_MAGASIN_FERMES ;

SELECT * FROM DHB_PROD.DNR.DN_ENTITE ;


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG AS
WITH Tabl0 AS (SELECT DISTINCT Id_entite, Code_entite, Lib_entite, id_region_com, lib_region_com, lib_grande_region_com,
type_emplacement, lib_statut, id_concept, lib_enseigne, code_pays, gpe_collectionning,
date_ouverture_public, date_fermeture_public, code_postal, surface_commerciale  
FROM DHB_PROD.DNR.DN_ENTITE 
WHERE id_marque='JUL' AND (lib_statut='Fermé' OR date_fermeture_public IS NOT NULL)
AND YEAR (date_fermeture_public)>=2021
ORDER BY date_fermeture_public DESC)
SELECT a.* , b.type_ferm ,
CASE 
WHEN type_ferm IS NULL AND Id_entite IN (403,725,160,351,137,294,373,3622,868,343,227,119,256,3552,3643,428,106,5 ) THEN 'Seche' 
WHEN type_ferm IS NULL AND Id_entite IN ( 188,3192,458,3190,19,3716,74,3197) THEN 'XL'
WHEN type_ferm IS NULL AND Id_entite IN ( 445,827,131,843,245,845,3231,423,1127,1400,3217,471,3709,3507,3535,3435) THEN '2en1' 
ELSE TYPE_ferm END AS type_ferm2 , 
FROM Tabl0 a 
LEFT JOIN DATA_MESH_PROD_CLIENT.WORK.STATUT_MAGASIN_FERMES b ON a.Id_entite=b.id_magasin ;

-- Selection des magasins avec fermetures definitive 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG_Seche AS
SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG
WHERE type_ferm2='Seche' ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG_Seche ;

--- Historique des ventes sur les 18 derniers mois après la fermeture 

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_mag_histo_18mth AS
WITH type_mag AS ( SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_INF0MAG
WHERE type_ferm2='Seche' ),
segrfm AS (SELECT DISTINCT CODE_CLIENT, ID_MACRO_SEGMENT , LIB_MACRO_SEGMENT 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_RFM
WHERE DATE_DEBUT <= DATE($dtfin)
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
segomni AS (SELECT DISTINCT CODE_CLIENT, LIB_SEGMENT_OMNI 
FROM DATA_MESH_PROD_CLIENT.SHARED.DMD_SEGMENT_OMNI 
WHERE DATE_DEBUT <= DATE($dtfin) 
AND (DATE_FIN > DATE($dtfin) OR DATE_FIN IS NULL) AND code_client IS NOT NULL AND code_client !='0'),
tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, vd.MONTANT_TTC,
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
CODE_EVT_PROMO, MONTANT_SOLDE,
CODE_AM, CODE_OPE_COMM,
MONTANT_REMISE_OPE_COMM,
COUNT(DISTINCT id_ticket) Over (partition by vd.CODE_CLIENT) as NB_tick_clt,
ROW_NUMBER() OVER (PARTITION BY VD.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
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
g.ID_MACRO_SEGMENT , g.LIB_MACRO_SEGMENT,
e.LIB_SEGMENT_OMNI , 
CASE WHEN g.id_macro_segment = '01' THEN '01_VIP' 
     WHEN g.id_macro_segment = '02' THEN '02_TBC'
     WHEN g.id_macro_segment = '03' THEN '03_BC'
     WHEN g.id_macro_segment = '04' THEN '04_MOY'
     WHEN g.id_macro_segment = '05' THEN '05_TAP'
     WHEN g.id_macro_segment = '06' THEN '06_TIEDE'
     WHEN g.id_macro_segment = '07' THEN '07_TPURG'
     WHEN g.id_macro_segment = '09' THEN '08_NCV'
     WHEN g.id_macro_segment = '08' THEN '09_NAC'
     WHEN g.id_macro_segment = '10' THEN '10_INA12'
     WHEN g.id_macro_segment = '11' THEN '11_INA24'
  ELSE '12_NOSEG' END AS SEGMENT_RFM, 
CASE 
	WHEN LIB_SEGMENT_OMNI IS NULL OR LIB_SEGMENT_OMNI='' THEN 'NOSEGMENT' 
	WHEN LIB_SEGMENT_OMNI='WEB' THEN 'OMNI'
	ELSE LIB_SEGMENT_OMNI END AS SEGMENT_OMNI,
mag.*, c.type_ferm, c.date_fermeture_public,
MAX(vd.date_ticket) Over (partition by vd.ID_MAGASIN) as max_dte_ticket_mag,
Min(vd.date_ticket) Over (partition by vd.ID_MAGASIN) as min_dte_ticket_mag
from DHB_PROD.DNR.DN_VENTE vd
INNER JOIN type_mag c on vd.ID_MAGASIN = c.ID_ENTITE
LEFT JOIN segrfm g ON vd.code_client = g.code_client
LEFT JOIN segomni e ON vd.code_client=e.code_client 
where vd.date_ticket BETWEEN dateadd('month', -18, c.date_fermeture_public) AND DATE(c.date_fermeture_public) -- ON analyse les ventes sur les 18 mois avant la date de fermeture 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND vd.date_ticket <= $dtfin
  ) 
SELECT *, datediff(MONTH ,min_dte_ticket_mag,max_dte_ticket_mag) AS periode_etud
FROM tickets
WHERE date_fermeture_public IS NOT NULL AND DATE(date_fermeture_public)<=$dtfin AND PERIMETRE='MAG' ;








