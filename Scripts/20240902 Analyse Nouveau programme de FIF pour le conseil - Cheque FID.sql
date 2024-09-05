-- Analyse Nouveau programme de FID pour le conseil 
  -- date de lancement du programme de FID 

/**** Analyse Cheque FID 

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

SET dtdeb='2024-08-01';
SET dtfin='2024-08-31';

SET ENSEIGNE1 = 1; -- renseigner ici les différentes enseignes et pays. Renseigner tous les paramètres, quitte à utiliser une valeur qui n'existe pas
SET ENSEIGNE2 = 3;
SET PAYS1 = 'FRA'; --code_pays = 'FRA' ... 
SET PAYS2 = 'BEL'; --code_pays = 'BEL' 


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




