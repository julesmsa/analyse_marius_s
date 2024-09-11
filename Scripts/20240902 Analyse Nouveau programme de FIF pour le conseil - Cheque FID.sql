-- Analyse Nouveau programme de FID pour le conseil 
  -- date de lancement du programme de FID 

/**** Analyse Cheque FID 
('130146')          THEN '02-CHEQUE_FID'

Période d’analyse : depuis le 24/04 jusqu’au 31/08
France Belgique magasins + web
Evolution par mois + un global


Nombre de chèques émis / utilisés / taux d’utilisation 
Nombre moyen de chèques / client
Délai moyen d'utilisation du chèque après émission
Valeur moyenne du chèque 
CA / % dans le CA total
Taux de remise / Taux de marge sortie
IV / PM
Fréquence
Typologie de clients 
Clients omnicanaux

Comparer ces mêmes chiffres à tous les autres tickets sur la période

****/ 

SET dtdeb='2024-04-24';
SET dtfin='2024-08-31';
SET dtdeb_Nm1 = to_date(dateadd('year', -1, $dtdeb)); 
SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 

SELECT $dtdeb, $dtfin, $dtdeb_Nm1, $PAYS1, $PAYS2, $ENSEIGNE1, $ENSEIGNE2;

--- Information sur l'année N 
SELECT * FROM DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE;       
        
CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N AS 
WITH info_clt AS (
    SELECT DISTINCT Code_client AS idclt, 
        date_naissance, 
        gender, 
        date_recrutement,
    DATEDIFF(MONTH, date_recrutement, $dtfin) AS ANCIENNETE_CLIENT,
    CASE 
        WHEN DATE(date_recrutement) DATE($dtdeb) AND DATE($dtfin)  THEN '02-Nouveaux' 
        ELSE '01-Anciens' 
    END AS Type_client,
    ROUND(DATEDIFF(YEAR, DATE(date_naissance), DATE($dtfin)), 2) AS AGE_C,
    CASE 
        WHEN (FLOOR((AGE_C) / 5) * 5) BETWEEN 15 AND 99 THEN CONCAT(FLOOR((AGE_C) / 5) * 5, '-', FLOOR((AGE_C) / 5) * 5 + 4) 
        ELSE '99-NR/NC' 
    END AS CLASSE_AGE        
    FROM DHB_PROD.DNR.DN_CLIENT)
Select distinct a.code_coupon, a.date_debut_validite, a.date_fin_validite, a.code_magasin as codemag_coupon, a.code_client, 
a.valeur, a.code_am, a.code_status, a.description_longue, a.description_courte, type_emplacement as type_empl_coupon, lib_magasin as libmag_coupon, 
DATE_FROM_PARTS(YEAR(date_debut_validite) , MONTH(date_debut_validite)-1, 1) AS date_niv_part,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS Canal_Crea_coupon,
CASE 
	WHEN code_am IN ('101623','301906') THEN '01-ANNIVERSAIRE'
	WHEN code_am IN ('130146')          THEN '02-CHEQUE_FID'
   WHEN code_am IN ('108250')          THEN '04-J_PRIVILEGE '
	WHEN code_am IN ('126861','326910','130147','130148') THEN '03-BIENVENUE'
END AS lIB_CODE_AM, 
CASE WHEN code_am IN ('108250') AND DATE(date_debut_validite) BETWEEN DATE($dtdeb_Nm1) AND DATE($dtfin) THEN 1 
WHEN code_am IN ('101623','301906','130146','126861','326910','130147','130148') AND  DATE(date_debut_validite) BETWEEN DATE($dtdeb) AND DATE($dtfin) THEN 1
ELSE 0 END AS Top_period,
idclt, date_naissance, gender, 
        date_recrutement, ANCIENNETE_CLIENT, Type_client, AGE_C, CLASSE_AGE,
    CASE 
        WHEN (anciennete_client IS NULL) OR (anciennete_client BETWEEN 0 AND 12) THEN 'a: [0-12] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 13 AND 24 THEN 'b: ]12-24] mois' 
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 25 AND 36 THEN 'c: ]24-36] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 37 AND 48 THEN 'd: ]36-48] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client BETWEEN 49 AND 60 THEN 'e: ]48-60] mois'
        WHEN anciennete_client IS NOT NULL AND anciennete_client > 60 THEN 'f: + de 60 mois'
        ELSE 'z: Non def' 
    END AS Tr_anciennete
From DATA_MESH_PROD_client.SHARED.T_COUPON_DENORMALISEE a
left join DATA_MESH_PROD_RETAIL.HUB.DMD_MAGASIN b ON a.code_magasin=b.ID_MAGASIN 
LEFT JOIN info_clt c ON a.CODE_CLIENT=c.idclt ;
where Top_period=1;


SELECT * FROM DATA_MESH_PROD_RETAIL.WORK.tab_creat_coupon_N ; 

--- identification des coupon utilisé sur les tickets 


CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N AS
Select vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,vd.MONTANT_TTC_eur,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,'_',vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, vd.MONTANT_REMISE_OPE_COMM, 
vd.montant_remise +  vd.MONTANT_REMISE_OPE_COMM AS remise_total, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
CODE_AM, 
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5,
type_emplacement,
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE,
vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur,
ROW_NUMBER() OVER (PARTITION BY VD.CODE_CLIENT ORDER BY CONCAT(code_ligne,ID_TICKET)) AS nb_lign,
SUM(CASE 
    WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') 
    THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos,     
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
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) AND (VD.CODE_CLIENT IS NOT NULL AND VD.CODE_CLIENT !='0')
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') ;

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N ; 


 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_list_coupon_N AS
 WITH tab0  AS (SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON1, CODE_COUPON2, CODE_COUPON3, CODE_COUPON4, CODE_COUPON5, 
MTREMISE_COUPON1, MTREMISE_COUPON2, MTREMISE_COUPON3, MTREMISE_COUPON4, MTREMISE_COUPON5,
CODEACTIONMARKETING_COUPON1, CODEACTIONMARKETING_COUPON2, CODEACTIONMARKETING_COUPON3, CODEACTIONMARKETING_COUPON4, CODEACTIONMARKETING_COUPON5
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket_N ) 
SELECT * FROM (
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON1 AS CODE_COUPON,
MTREMISE_COUPON1 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON1 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON1 IS NOT NULL  
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON2 AS CODE_COUPON,
MTREMISE_COUPON2 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON2 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON2 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON3 AS CODE_COUPON,
MTREMISE_COUPON3 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON3 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON3 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON4 AS CODE_COUPON,
MTREMISE_COUPON4 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON4 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON4 IS NOT NULL 
UNION	
SELECT DISTINCT CODE_CLIENT, id_ticket, 
CODE_COUPON5 AS CODE_COUPON,
MTREMISE_COUPON5 AS MTREMISE_COUPON,
CODEACTIONMARKETING_COUPON5 AS CODEACTIONMARKETING_COUPON
FROM tab0
WHERE CODE_COUPON5 IS NOT NULL ) ; 

-- information de chaque ticket de caisse 
 CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_statticket_N AS
SELECT CODE_CLIENT,id_ticket,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC END) AS Mtn_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS Qte_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS Marge_Gbl,
SUM(CASE WHEN annul_ticket=0 THEN remise_total END) AS Rem_Gbl,
FROM DATA_MESH_PROD_CLIENT.WORK.tab_ticket
GROUP BY 1,2
ORDER BY 1,2; 















WITH tickets as (
Select   vd.CODE_CLIENT AS id_clt,
vd.ID_ORG_ENSEIGNE AS idorgens_achat, vd.ID_MAGASIN AS idmag_achat, vd.CODE_MAGASIN AS mag_achat, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.CODE_SKU, vd.Code_RCT,lib_famille_achat,
vd.MONTANT_TTC, vd.MONTANT_TTC_eur, 
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.code_marque,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket,
SUM(CASE WHEN lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','') THEN QUANTITE_LIGNE END ) OVER (PARTITION BY id_ticket) AS Qte_pos, 
CASE WHEN type_emplacement IN ('EC','MP') THEN 'WEB'
WHEN type_emplacement IN ('PAC','CC', 'CV','CCV') THEN 'MAG' END AS PERIMETRE, 
CASE WHEN PERIMETRE = 'WEB' AND libelle_type_ligne ='Retour' THEN 1 ELSE 0 END AS annul_ticket
from DHB_PROD.DNR.DN_VENTE vd
where vd.date_ticket BETWEEN $dtdeb AND $dtfin
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2) 
  --AND vd.code_client IS NOT NULL AND vd.code_client !='0'
  AND  lib_famille_achat NOT IN ('SERVICES', 'Marketing', 'Marketing Boy','Marketing Girl','SERVICES','Service','')
  ) SELECT 
Count(DISTINCT id_ticket) AS nb_ticket_1,
Count(DISTINCT CASE WHEN Qte_pos>0 THEN id_ticket END ) AS nb_ticket_2,
COUNT(DISTINCT  CASE WHEN annul_ticket=0 THEN id_ticket END ) AS Nb_ticket_3,
SUM(CASE WHEN annul_ticket=0 THEN QUANTITE_LIGNE END ) AS sum_qte,
SUM(CASE WHEN Qte_pos>0 THEN MONTANT_TTC_eur END ) AS CA_Qte_pos,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_TTC_eur END ) AS CA_ticket,
SUM(CASE WHEN Qte_pos>0 THEN MONTANT_MARGE_SORTIE END ) AS MARGE_Qte_pos,
SUM(CASE WHEN annul_ticket=0 THEN MONTANT_MARGE_SORTIE END ) AS MARGE_ticket
From tickets ;


SELECT * FROM DHB_PROD.DNR.DN_VENTE
WHERE id_ticket='1-363-1-20230930-13273032'


SELECT *
FROM DHB_PROD.HUB.D_OPERATION_COMMERCIALE_TARIF
WHERE CODE_OPE_COMM = '5082'
AND ID_MAGASIN = 363
AND SKU = 3745943
ORDER BY DATE_DEBUT DESC
;




