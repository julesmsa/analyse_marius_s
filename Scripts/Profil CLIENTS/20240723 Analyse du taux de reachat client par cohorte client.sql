--- Analyse du taux de reachat client par cohorte 

SET dtdeb = Date('2021-01-01');
SET dtfin = DAte('2024-06-30'); -- to_date(dateadd('year', +1, $dtdeb_EXON))-1 ;  -- CURRENT_DATE();
select $dtdeb, $dtfin;

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.tab_tick_reachat AS
WITH tickets as (
Select   vd.CODE_CLIENT,
vd.ID_ORG_ENSEIGNE, vd.ID_MAGASIN AS mag_achat, vd.CODE_MAGASIN, vd.CODE_CAISSE, vd.CODE_DATE_TICKET, vd.CODE_TICKET, vd.date_ticket, 
vd.MONTANT_TTC,
vd.code_ligne, vd.type_ligne, vd.libelle_type_ligne, 
vd.code_type_article, vd.code_ligne_taille, vd.code_grille_taille, vd.code_coloris, vd.CODE_REFERENCE, vd.code_marque,
CONCAT(vd.CODE_REFERENCE,vd.CODE_COLORIS) AS REFCO,
vd.prix_unitaire, vd.montant_remise, 
vd.QUANTITE_LIGNE,
vd.MONTANT_MARGE_SORTIE,
vd.libelle_type_ticket, 
vd.id_ticket, 
vd.type_emplacement, vd.code_pays,
vd.LIB_FAMILLE_ACHAT, 
prix_unitaire_base_eur
from DATA_MESH_PROD_RETAIL.SHARED.T_VENTE_DENORMALISEE vd
where vd.date_ticket BETWEEN DATE($dtdeb) AND DATE($dtfin) 
  and (vd.ID_ORG_ENSEIGNE = $ENSEIGNE1 or vd.ID_ORG_ENSEIGNE = $ENSEIGNE2)
  and (vd.code_pays = $PAYS1 or vd.code_pays = $PAYS2)),
info_clt AS ( SELECT DISTINCT Code_client AS idclient, id_titre, date_naissance, age, gender, 
est_valide_telephone, est_optin_sms_com, est_optin_sms_fid, est_optin_email_com, 
est_optin_email_fid, code_postal, code_pays AS pays_clt, date_recrutement, id_macro_segment, lib_macro_segment, lib_segment_omni
FROM DATA_MESH_PROD_client.SHARED.T_CLIENT_DENORMALISEE WHERE (code_pays = $PAYS1 or code_pays = $PAYS2) AND date_suppression_client IS NULL AND date_recrutement >= DATE($dtdeb) )
SELECT a.*, b.*, 
datediff(MONTH,date_recrutement,date_ticket) AS delai_achat, 
CASE WHEN MONTH(date_recrutement) <10 THEN CONCAT(YEAR(date_recrutement),'_M0',MONTH(date_recrutement)) ELSE CONCAT(YEAR(date_recrutement),'_M',MONTH(date_recrutement)) END AS Mois_recrut,
MIN(delai_achat) OVER (PARTITION BY CODE_CLIENT) AS min_delai_achat
FROM tickets a
INNER JOIN info_clt b ON a.CODE_CLIENT=b.idclient 
WHERE date_ticket>=date_recrutement AND date_recrutement >= DATE($dtdeb) ; 

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.tab_tick_reachat WHERE code_client='107110003568' ORDER by date_ticket; 
/***** Statistique de réachat 

 */

CREATE OR REPLACE TEMPORARY TABLE DATA_MESH_PROD_CLIENT.WORK.stat_tick_reachat as
SELECT Mois_recrut, 
Count(DISTINCT Code_Client) AS nb_client,
Count(DISTINCT CASE WHEN delai_achat=0 THEN Code_Client END) AS nb_client_corr,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=3  THEN Code_Client END) AS nb_client_03Mois,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=6  THEN Code_Client END) AS nb_client_06Mois,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=9  THEN Code_Client END) AS nb_client_09Mois,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=12 THEN Code_Client END) AS nb_client_12Mois,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=15 THEN Code_Client END) AS nb_client_15Mois,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=18 THEN Code_Client END) AS nb_client_18Mois,
Count(DISTINCT CASE WHEN delai_achat>0 AND delai_achat<=24 THEN Code_Client END) AS nb_client_24Mois
FROM DATA_MESH_PROD_CLIENT.WORK.tab_tick_reachat
WHERE min_delai_achat=0
GROUP BY 1
ORDER BY 1 ; 
  

SELECT * FROM DATA_MESH_PROD_CLIENT.WORK.stat_tick_reachat ORDER BY 1 ; 

