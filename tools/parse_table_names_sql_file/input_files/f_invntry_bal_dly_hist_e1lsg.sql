/**************************************************************************
Artefact Name :- F_INVNTRY_BAL_DLY_HIST (SNAPSHOT)
Description :-  This table will hold the information of the on-hand_qty for E1LSG system
-------------------------------------------------------------------------------------------------------------------------------
Change Log
Version :        Date :                                Description                                     Changed By
-------------------------------------------------------------------------------------------------------------------------------
0.0            18-08-2022	                       First draft of sql file    			                Vijay Kelkar
1.1            14-09-2022                          UOM Issue : all the qty divide by 10,000             Vijay Kelkar  
1.2            19-10-2022						   Replace dims table with raw table                    Neha Chaturvedi	
1.3            08-11-2022						   Replace on_hand_qty logic                            Neha Chaturvedi	
1.4            09-11-2022						   Replace avail_qty logic                            Neha Chaturvedi
1.5            27-01-2022                          Remove Where Condition	                          Neha Chaturvedi
**************************************************************************/


(
select distinct
'e1lsg' as src_sys_cd,
trim(f4102.iblitm) as item_nbr,
cast(null as string) as item_desc,
'' as item_type,
'' as item_type_desc,
trim(f4102.ibmcu) as plant_cd,
cast(date_format(date_sub(current_timestamp,1),'yMMdd') as string) as capture_dt,
cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
cast(F41021.lilotn as string) as  lot_nbr,
'NA' as invntry_loc_cd,
cast(F41021.lilocn as string) as invntry_loc_name,
'NA' as strg_bin_cd,
cast(f0006.mcco as string) as  co_cd,
cast(f0010.CCNAME as string) as co_name,
case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end as co_curncy_cd,
'Finished Goods' as invntry_stk_type_cd,
'NA'  as valuation_cd,
cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string) as  recpt_dt,
-- ((f41021.LIPQOH/10000) - (f41021.LIHCOM/10000) - (f41021.LIPCOM/10000) - (f41021.LIFCOM/10000) - (f41021.LIFUN1/10000) - (f41021.LIQOWO/10000)) as avail_qty,
cast(0 as double) as avail_qty,
cast((CASE WHEN trim(f41021.LILOTS) in ('C','D','G','M','P','Q','q','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as on_hand_qty,
--(case when (TRIM(LIPBIN) = 'P' OR TRIM(LIPBIN) is NULL ) then 0 else (CASE WHEN TRIM(LILOTS) IS NULL THEN 0 ELSE (LIPQOH / 10000) end) end) as on_hold_qty,
cast(0 as double) as on_hold_qty, 
cast(0 as double) as transfer_qty,
cast(0 as double) as qa_inspn_qty,
cast(0 as double) as blocked_qty,
cast(0 as double) as rstrct_qty,
cast(null as double)  as unit_cost_co_amt,
cast(null as double) as unit_cost_lcur_amt,
cast(null as double) as unit_cost_co_pmar_amt,
coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity ,
case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' else trim(f0005.DRDL01) end as business_unit,
cast(trim(F0005_div.DRDL01) as string) as div_cd,
cast(f41021.LILOTS as string) as lot_stat_cd,
cast(F0005_LOT.DRDL01 as string) as lot_stat_nm,
cast(f41021.LIPBIN as string) as prim_loc_flg,
'NA' as flr_stk_cd,
'NA' as rejected_mat_flag,
cast(F0005_UOM.DRDL01 as string) as stk_uom_cd,
'NA' as  stk_uom_nm,
cast(f4102.IBSRP3 as string) as prod_family,
cast(F0005_PROD.DRDL01 as string) as prod_fam_typ,
'NA' as gl_account,
'NA' as src_crt_by,
'NA' as src_crt_ts,
cast(current_timestamp as string) as rec_crt_ts,
cast(current_timestamp as string) as rec_updt_ts 

from  F41021  
left outer join f4102_adt f4102 on trim(cast(cast(f4102.ibitm as integer) as string))= trim(cast(cast(F41021.LIITM as integer) as string))
and trim(F41021.LIMCU) = trim(f4102.ibmcu)
left outer join f4105
on  trim(cast(cast(F41021.LIITM as integer) as string)) = trim(cast(cast(f4105.COITM as integer) as string)) and trim(F41021.LIMCU) = trim(f4105.COMCU)
left outer join f0006 on f0006.mcmcu = f41021.limcu

left outer join f4101_adt f4101 on trim(f4101.IMITM) = trim(f41021.LIITM)

left outer join f0010 on f0010.CCCO= f0006.MCCO

left outer join d_date d_dt on date_format(if(date_format(current_timestamp,'%h')>='0' and date_format(current_timestamp,'%h')<='12',date_sub(current_date,1),current_date),'yMMdd') = cast (d_dt.dt_key as string)

left outer join (select curr_mnth.PMAR_RT as CO_PMAR_RT, curr_mnth.CURNCY_MTH_RT_KEY,curr_mnth.YR_MTH_NBR,curr_mnth.FROM_CURNCY_CD 
from d_curncy_mth_rt curr_mnth where TO_CURNCY_CD =  'USD') co_curr_mth  on co_curr_mth.YR_MTH_NBR = d_dt.fscl_yr_prd_nbr and
co_curr_mth.FROM_CURNCY_CD = COALESCE(f0010.CCCRCD,'USD')

left outer join  edp_lkup hfm on hfm.lkup_key_01 = f0006.MCCO and  hfm.LKUP_TYP_NM ='CO_TO_HFM' and hfm.lkup_key_02 = 'E1LSG'
left outer join f0005 F0005_LOT on trim(F0005_LOT.DRSY)= '41' and trim(F0005_LOT.DRRT)='L' and trim(F0005_LOT.DRKY)=trim(f41021.LILOTS)
left outer join f0005 F0005_UOM on trim(F0005_UOM.DRSY)= '00' and trim(F0005_UOM.DRRT)='UM' and trim(F0005_UOM.DRKY)=trim(f4101.IMUOM1)
left outer join f0005 F0005_PROD on trim(F0005_PROD.DRSY)= '41' and trim(F0005_PROD.DRRT)='S3'  and trim(F0005_PROD.DRKY)=trim(f4102.IBSRP3)

left outer join f0005 f0005 on trim(f0005.DRSY)= '41' and trim(f0005.DRRT)='S2' and trim(f0005.DRKY)=trim(f4102.IBSRP2)

left outer join f0005 F0005_div on trim(F0005_div.DRSY)= '41' and trim(F0005_div.DRRT)='S1' and trim(F0005_div.DRKY)=trim(f4102.IBSRP1)

)