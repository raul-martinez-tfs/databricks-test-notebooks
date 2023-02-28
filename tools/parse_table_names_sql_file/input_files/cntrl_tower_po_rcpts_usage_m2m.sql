-- /**************************************************************************
-- Artefact Name :- po_rcpt_usage control tower for Lighthouse 
-- Description :-   po_rcpt_usage control tower for Lighthouse
-- -------------------------------------------------------------------------------------------------------------------------------
-- Change Log
-- Version :        Date :                                Description                                     Changed By
-- -------------------------------------------------------------------------------------------------------------------------------
-- 0.0            	  04-01-2022                  			 First draft of sql file    			       Abhishek Ranjan
-- 1.0				  06-04-2022                             Adding rec_crt_ts,rec_updt_ts
--																Columns                                     Abhishek Rajan
-- **************************************************************************/

-- Databricks notebook source
(
SELECT 
  (trim(f_po_receipt.item_nbr)||'|'|| (case 
	when f_po_receipt.src_sys_cd like '%m2m%' and f_po_receipt.site_cd ='Default' then 'M2M-MBRG'
	when f_po_receipt.src_sys_cd like '%m2m%' and f_po_receipt.site_cd ='MEXICO' then 'M2M-MATS'
else  f_po_receipt.site_cd
end )) sku_site_cd
, CAST(f_po_receipt.recpt_date AS string) transaction_date
, f_po_receipt.item_nbr sku_number
, (case 
	when f_po_receipt.src_sys_cd like '%m2m%' and f_po_receipt.site_cd ='Default' then 'M2M-MBRG'
	when f_po_receipt.src_sys_cd like '%m2m%' and f_po_receipt.site_cd ='MEXICO' then 'M2M-MATS'
else  f_po_receipt.site_cd
end )  ops_site
, 'receipts' valuetype
, f_po_receipt.recpt_base_qty qty
, f_po_receipt.recpt_pmar_amt  pmar_amt
, f_po_receipt.src_sys_cd
,cast(current_timestamp as string) as rec_crt_ts
,cast(current_timestamp as string) as rec_updt_ts
FROM
  f_po_receipt,d_date dt
WHERE  f_po_receipt.recpt_date = dt_key
and  dt.fscl_yr_nbr > '2019'
and src_sys_cd = 'm2m_mbrg'
and f_po_receipt.item_nbr is not null
UNION ALL 
SELECT 
 (trim(f_purchase_order.item_nbr)||'|'||(case 
	when f_purchase_order.src_sys_cd like '%m2m%' and f_purchase_order.site_id ='Default' then 'M2M-MBRG'
	when f_purchase_order.src_sys_cd like '%m2m%' and f_purchase_order.site_id ='MEXICO' then 'M2M-MATS'
else  f_purchase_order.site_id
end ))  sku_site_cd
,  trim(f_purchase_order.plan_delvry_dt)  transaction_date
, f_purchase_order.item_nbr sku_number
, (case 
	when f_purchase_order.src_sys_cd like '%m2m%' and f_purchase_order.site_id ='Default' then 'M2M-MBRG'
	when f_purchase_order.src_sys_cd like '%m2m%' and f_purchase_order.site_id ='MEXICO' then 'M2M-MATS'
else  f_purchase_order.site_id
end ) ops_site
, 'purchase order' valuetype
, f_purchase_order.open_qty qty
, f_purchase_order.ext_prc_co_pmar_amt pmar_amt
, f_purchase_order.src_sys_cd
,cast(current_timestamp as string) as rec_crt_ts
,cast(current_timestamp as string) as rec_updt_ts
FROM
  f_purchase_order,d_date dt
WHERE  f_purchase_order.plan_delvry_dt = dt_key
and f_purchase_order.plan_delvry_dt > '20200101' 
and src_sys_cd ='m2m_mbrg'
and (f_purchase_order.item_status_cd = 'OPEN' or f_purchase_order.item_status_cd is null)
UNION ALL 
SELECT 
(trim(f_invntry_txn.item_nbr)||'|'|| 
(case 
	when f_invntry_txn.src_sys_cd like '%m2m%' and f_invntry_txn.plant_cd ='Default' then 'M2M-MBRG'
	when f_invntry_txn.src_sys_cd like '%m2m%' and f_invntry_txn.plant_cd ='MEXICO' then 'M2M-MATS'
else  f_invntry_txn.plant_cd
end ))
sku_site_cd
, CAST(f_invntry_txn.txn_date AS string) transaction_date
, f_invntry_txn.item_nbr sku_number
, (case 
	when f_invntry_txn.src_sys_cd like '%m2m%' and f_invntry_txn.plant_cd ='Default' then 'M2M-MBRG'
	when f_invntry_txn.src_sys_cd like '%m2m%' and f_invntry_txn.plant_cd ='MEXICO' then 'M2M-MATS'
else  f_invntry_txn.plant_cd
end ) ops_site
, 'usage' valuetype
, (f_invntry_txn.txn_qty*(-1)) qty
, (f_invntry_txn.txn_qty*(-1) * prod_cost) pmar_amt
, f_invntry_txn.src_sys_cd
,cast(current_timestamp as string) as rec_crt_ts
,cast(current_timestamp as string) as rec_updt_ts
FROM
  f_invntry_txn,d_date dt
WHERE  txn_date = dt_key
and  dt.fscl_yr_nbr > '2019'
and src_sys_cd = 'm2m_mbrg'
and ((f_invntry_txn.txn_type_cd in ('33','35','62')) or
(f_invntry_txn.txn_type_cd = '32' and f_invntry_txn.rsn_cd in ('8310','9050')) or
(f_invntry_txn.txn_type_cd = '32' and f_invntry_txn.gl_acct not in ('4792')) or
(f_invntry_txn.txn_type_cd = '63' and f_invntry_txn.rsn_cd in ('9050')))
)