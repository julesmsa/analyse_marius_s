--- Magasin comparable

  -- SCRIPT POUR L'ANNEE EN COURS 
SET dtdeb = DAte('2024-01-01');
SET dtfin = DAte('2024-12-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();

SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb)); 
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 


SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb, $dtfin, $dtfin_Nm1, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_jour_mag AS
WITH tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN , vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS montant_remise,
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
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date($dtdeb_Nm1) AND (DATE($dtfin)-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  AND VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0') 
  SELECT ID_ORG_ENSEIGNE, ID_MAGASIN, CODE_MAGASIN, 
  Count(DISTINCT CASE WHEN Date(date_ticket) BETWEEN Date($dtdeb) AND (DATE($dtfin)-1) THEN date_ticket END) AS nbjour_ouvr_N,
   Count(DISTINCT CASE WHEN Date(date_ticket) BETWEEN Date($dtdeb) AND (DATE($dtfin)-1) THEN id_ticket END ) AS nb_ticket_N,
   Count(DISTINCT CASE WHEN Date(date_ticket) BETWEEN Date($dtdeb_Nm1) AND (DATE($dtfin_Nm1)-1) THEN date_ticket END) AS nbjour_ouvr_Nm1,
   Count(DISTINCT CASE WHEN Date(date_ticket) BETWEEN Date($dtdeb_Nm1) AND (DATE($dtfin_Nm1)-1) THEN id_ticket END ) AS nb_ticket_Nm1
   FROM tickets
   GROUP BY 1,2,3
   ORDER BY 1,2,3 ; 
  
  SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_jour_mag  ORDER BY 1,2,3 ; 
  
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_jour_mag_ok AS
  SELECT DISTINCT ID_ORG_ENSEIGNE, ID_MAGASIN, CODE_MAGASIN
 FROM DATA_MESH_PROD_CLIENT.WORK.tab_jour_mag 
WHERE nbjour_ouvr_N>=250 AND nbjour_ouvr_Nm1>=250
 ORDER BY 1,2,3 ; 

    
 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_jour_mag_ok  ORDER BY 1,2,3 ; 





  -- Retention Client à venir 

SET dtdeb = DAte('2021-12-01');
SET dtfin = DAte('2024-12-01'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();

SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb)); 
SET dtfin_Nm1 = to_date(dateadd('year', -1, $dtfin)); 


 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_clt_achat AS
WITH tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN, vd.CODE_MAGASIN , vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat, vd.lib_magasin, CONCAT( vd.CODE_MAGASIN,'_',lib_magasin) AS nom_mag,
vd.MONTANT_TTC, vd.code_pays,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, 
MONTANT_REMISE_OPE_COMM,
vd.montant_remise + MONTANT_REMISE_OPE_COMM AS montant_remise,
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
    ELSE 0 END AS annul_ticket,
CASE WHEN Date(date_ticket) BETWEEN Date('2021-12-01') AND (DATE('2022-12-01')-1) THEN 1 ELSE 0 END AS Annee1,
CASE WHEN Date(date_ticket) BETWEEN Date('2022-12-01') AND (DATE('2023-12-01')-1) THEN 1 ELSE 0 END AS Annee2,
CASE WHEN Date(date_ticket) BETWEEN Date('2023-12-01') AND (DATE('2024-12-01')-1) THEN 1 ELSE 0 END AS Annee3
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN Date('2021-12-01') AND (DATE('2024-12-01')-1)
  and vd.ID_ORG_ENSEIGNE IN ($ENSEIGNE1 , $ENSEIGNE2)
  and vd.code_pays IN  ($PAYS1 ,$PAYS2)
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
  AND vd.code_client IS NOT NULL AND vd.code_client !='0'
  )
  SELECT a.Code_Client
  ,MAX(Annee1) AS max_Annee1
  ,MAX(Annee2) AS max_Annee2
  ,MAX(Annee3) AS max_Annee3
  FROM tickets a
  INNER JOIN  DHB_PROD.DNR.DN_CLIENT b ON b.code_client IS NOT NULL AND b.code_client !='0' AND (date_suppression_client is null or date_suppression_client > $dtfin) 
  GROUP BY 1 ; 

 SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_achat ;
 
 SELECT max_Annee1,max_Annee2,max_Annee3,
 Count(DISTINCT Code_Client) AS nb_client
 FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_achat
 GROUP BY 1,2,3 
ORDER BY 1 DESC ,2 DESC ,3 DESC  ;


 SELECT max_Annee2,max_Annee3,
 Count(DISTINCT Code_Client) AS nb_client
 FROM DATA_MESH_PROD_CLIENT.WORK.tab_clt_achat
 GROUP BY 1,2
ORDER BY 1 DESC ,2 DESC ;
    