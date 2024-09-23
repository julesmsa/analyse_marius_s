-- TEST pour le calcul de la remise 

WITH tickets AS ( 
Select vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,montant_remise_ope_comm,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
CODE_AM,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
ROW_NUMBER() OVER (PARTITION BY CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN montant_remise END ) OVER (PARTITION BY id_ticket) AS Remise_brute,     
CASE 
    WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 
    WHEN EST_MDON_CKDO=True THEN 1 
    --WHEN REFCO IN (select distinct CONCAT(ID_REFERENCE,'_',ID_COULEUR) 
     --               from DHB_PROD.DNR.DN_PRODUIT
      --              where ID_TYPE_ARTICLE<>1
     --               and id_marque='JUL')
      --  THEN 1         
    ELSE 0 END AS annul_ticket 
from DHB_PROD.DNR.DN_VENTE vd
where YEAR(vd.date_ticket)>=2021  
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  --AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
)
SELECT YEAR(date_ticket) AS  Annee_achat, 
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN CODE_CLIENT END ) AS nb_client_Gbl,
COUNT(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise END) AS Rem_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN montant_remise_ope_comm END) AS Rem_ope_comm_Gbl
FROM tickets
WHERE id_ticket='1-363-1-20230930-13273032'
GROUP BY 1
ORDER BY 1 DESC ; 

SELECT * FROM DHB_PROD.DNR.DN_VENTE WHERE prix_init_vente IS NULL AND code_marq='JUL'


SELECT * FROM DHB_PROD.DNR.DN_VENTE 
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (code_pays = $PAYS1 or code_pays = $PAYS2) 
  --AND MONTANT_REMISE >0 AND MONTANT_REMISE_OPE_COMM >0
  AND id_ticket='1-363-1-20230930-13273032'


SELECT * FROM DHB_PROD.DNR.DN_PRODUIT WHERE SKU='3745943'




--- Test sur le recrutement, 

SELECT (YEAR (date_recrutement)*100+ MONTH(date_recrutement)) AS date_recrut,
Count(DISTINCT CODE_Client) AS Nb_client
FROM DHB_PROD.DNR.DN_CLIENT
GROUP BY 1
ORDER BY 1 DESC; 


SELECT YEAR (date_recrutement) AS date_recrut,
Count(DISTINCT CODE_Client) AS Nb_client
FROM DHB_PROD.DNR.DN_CLIENT
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (code_pays = $PAYS1 or code_pays = $PAYS2) AND (CODE_CLIENT IS NOT NULL AND CODE_CLIENT !='0')
GROUP BY 1
ORDER BY 1 DESC; 

SELECT YEAR (date_recrutement) AS date_recrut,
Count(DISTINCT CODE_Client) AS Nb_client
FROM DATA_MESH_PROD_CLIENT.WORK.CLIENT_DENORMALISEE 
WHERE (ID_ORG_ENSEIGNE = $ENSEIGNE1 or ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (code_pays = $PAYS1 or code_pays = $PAYS2) AND (CODE_CLIENT IS NOT NULL AND CODE_CLIENT !='0')
GROUP BY 1
ORDER BY 1 DESC; 