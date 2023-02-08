# Databricks notebook source
df_d_date = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/d_date/")
df_d_date.createOrReplaceTempView("d_date")

df_d_curncy_mth_rt = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-uat/processed/d_curncy_mth_rt/")
df_d_curncy_mth_rt.createOrReplaceTempView("d_curncy_mth_rt")

df_edp_lkup = spark.read.format('parquet').load("s3://tfsdl-edp-common-dims-prod/processed/edp_lkup/")
df_edp_lkup.createOrReplaceTempView("edp_lkup")

df_f41112 = spark.read.format('delta').load("s3://tfsdl-e1lsg-test/processed/delta/proddta/f41112/")
df_f41112.createOrReplaceTempView("f41112")

df_f41021 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f41021/")
df_f41021.createOrReplaceTempView("f41021")

df_f4108_adt = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f4108_adt/")
df_f4108_adt.createOrReplaceTempView("f4108_adt")

df_f4102_adt = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f4102_adt/")
df_f4102_adt.createOrReplaceTempView("f4102_adt")

df_f0005 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/prodctl/f0005/")
df_f0005.createOrReplaceTempView("f0005")

df_f4105 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f4105/")
df_f4105.createOrReplaceTempView("f4105")

df_f0006 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f0006/")
df_f0006.createOrReplaceTempView("f0006")

df_f4101_adt = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f4101_adt/")
df_f4101_adt.createOrReplaceTempView("f4101_adt")

df_f0010 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f0010/")
df_f0010.createOrReplaceTempView("f0010")

f_invntry_bal_dly_hist = spark.read.format('delta').load("s3://tfsdl-edp-supplychain-prod/processed/f_invntry_bal_dly_hist/")
f_invntry_bal_dly_hist.createOrReplaceTempView("f_invntry_bal_dly_hist")

f43099 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f43099/")
f43099.createOrReplaceTempView("f43099")

f4311 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f4311/")
f4311.createOrReplaceTempView("f4311")

f43121 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f43121/")
f43121.createOrReplaceTempView("f43121")

f43092 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f43092/")
f43092.createOrReplaceTempView("f43092")

f43092 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f43092/")
f43092.createOrReplaceTempView("f43092")

f43121 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f43121/")
f43121.createOrReplaceTempView("f43121")

item_qty_on_hand = spark.read.format('delta').load("s3://tfsdl-cmd-navger-prod/processed/delta/dbo/item_qty_on_hand/")
item_qty_on_hand.createOrReplaceTempView("item_qty_on_hand")

location = spark.read.format('delta').load("s3://tfsdl-cmd-navger-prod/processed/delta/dbo/location/")
location.createOrReplaceTempView("location")

ItemWOPD = spark.read.format('delta').load("s3://tfsdl-cmd-navger-prod/processed/delta/dbo/itemwopd/")
ItemWOPD.createOrReplaceTempView("ItemWOPD")

Unit_of_Measure = spark.read.format('delta').load("s3://tfsdl-cmd-navger-prod/processed/delta/dbo/unit_of_measure/")
Unit_of_Measure.createOrReplaceTempView("Unit_of_Measure")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /**************************************************************************
# MAGIC Artefact Name :- F_INVNTRY_BAL_DLY_HIST (SNAPSHOT)
# MAGIC Description :-  This table will hold the information of the on-hand_qty for E1LSG system
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC Change Log
# MAGIC Version :        Date :                                Description                                     Changed By
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC 0.0            18-08-2022	                       First draft of sql file    			                Vijay Kelkar
# MAGIC 1.1            14-09-2022                          UOM Issue : all the qty divide by 10,000             Vijay Kelkar  
# MAGIC 1.2            19-10-2022						   Replace dims table with raw table                    Neha Chaturvedi	
# MAGIC 1.3            08-11-2022						   Replace on_hand_qty logic                            Neha Chaturvedi	
# MAGIC 1.4            09-11-2022						   Replace avail_qty logic                            Neha Chaturvedi
# MAGIC 1.5            27-01-2022                          Remove Where Condition	                          Neha Chaturvedi
# MAGIC 1.6            01-02-2022                          Replaced avail_qty logic                            Raul Martinez
# MAGIC                                                    Replaced on_hand_qty logic                          Raul Martinez
# MAGIC                                                    Replaced qa_inspn_qty logic                         Raul Martinez
# MAGIC                                                    Replaced blocked_qty logic                          Raul Martinez
# MAGIC                                                    Replaced item_nbr logic                             Raul Martinez
# MAGIC                                                    Replaced plant_cd logic                             Raul Martinez
# MAGIC                                                    Replaced lot_nbr logic                              Raul Martinez
# MAGIC                                                    Replaced invntry_loc_name logic                     Raul Martinez
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC with consignment_qty_records as (
# MAGIC   select 
# MAGIC     trim(b.PRLITM) as item_nbr,
# MAGIC     trim(a.pxmcu) as plant_cd,
# MAGIC     b.prlotn as lot_nbr,
# MAGIC     b.prlocn as invntry_loc_name,
# MAGIC     a.pxqtyo/10000 as consignment_qty 
# MAGIC   from f43092 a -- Need to convert the qty in to base unit of measure
# MAGIC   left outer join f43121 b
# MAGIC     on trim(cast(cast(a.pxdoco as integer) as string))= trim(cast(cast(b.PRDOCO as integer) as string))
# MAGIC       and a.pxdcto = b.prdcto
# MAGIC       and trim(cast(cast(a.pxlnid as integer) as string))= trim(cast(cast(b.prlnid as integer) as string))
# MAGIC       and trim(cast(cast(a.pxnlin as integer) as string))= trim(cast(cast(prnlin as integer) as string)) 
# MAGIC       and trim(a.pxmcu)=trim(b.prmcu)
# MAGIC   where a.pxnrou = 'CONS' 
# MAGIC     and a.pxoprc = 'CONS' 
# MAGIC     and a.pxupib = 'QTO1'
# MAGIC     and a.pxqtyo/10000 >0
# MAGIC     and trim(PRLITM) = '9101083'
# MAGIC     and trim(PRMCU) ='US01'
# MAGIC     and trim(PxMCU) ='US01'
# MAGIC     and trim(b.PRDCT) ='OV'
# MAGIC     and b.prmatc in('1','2')
# MAGIC ),
# MAGIC 
# MAGIC f_invntry_bal_dly_hist as (
# MAGIC   select distinct
# MAGIC       'e1lsg' as src_sys_cd,
# MAGIC       trim(f4102.iblitm) as item_nbr,
# MAGIC       cast(null as string) as item_desc,
# MAGIC       '' as item_type,
# MAGIC       '' as item_type_desc,
# MAGIC       trim(f4102.ibmcu) as plant_cd,
# MAGIC       cast(date_format(date_sub(current_timestamp,1),'yMMdd') as string) as capture_dt,
# MAGIC       cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC       cast(F41021.lilotn as string) as lot_nbr,
# MAGIC       'NA' as invntry_loc_cd,
# MAGIC       cast(F41021.lilocn as string) as invntry_loc_name,
# MAGIC       'NA' as strg_bin_cd,
# MAGIC       cast(f0006.mcco as string) as  co_cd,
# MAGIC       cast(f0010.CCNAME as string) as co_name,
# MAGIC       case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end as co_curncy_cd,
# MAGIC       'Finished Goods' as invntry_stk_type_cd,
# MAGIC       'NA'  as valuation_cd,
# MAGIC       cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string) as  recpt_dt,
# MAGIC       -- ((f41021.LIPQOH/10000) - (f41021.LIHCOM/10000) - (f41021.LIPCOM/10000) - (f41021.LIFCOM/10000) - (f41021.LIFUN1/10000) - (f41021.LIQOWO/10000)) as avail_qty,
# MAGIC       -- cast(0 as double) as avail_qty,
# MAGIC       cast((CASE WHEN trim(f41021.LILOTS) in ('#','C','D','G','M','P','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as avail_qty,
# MAGIC       -- cast((CASE WHEN trim(f41021.LILOTS) in ('C','D','G','M','P','Q','q','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as on_hand_qty,
# MAGIC       -- cast((CASE WHEN trim(f41021.LILOTS) in ('C','D','G','M','P','Q','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as on_hand_qty,
# MAGIC       'NA' as on_hand_qty,
# MAGIC       --(case when (TRIM(LIPBIN) = 'P' OR TRIM(LIPBIN) is NULL ) then 0 else (CASE WHEN TRIM(LILOTS) IS NULL THEN 0 ELSE (LIPQOH / 10000) end) end) as on_hold_qty,
# MAGIC       cast(0 as double) as on_hold_qty, 
# MAGIC       cast(0 as double) as transfer_qty,
# MAGIC       -- cast(0 as double) as qa_inspn_qty,
# MAGIC       cast((CASE WHEN trim(f41021.LILOTS) in ('Q') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as qa_inspn_qty,
# MAGIC       -- cast(0 as double) as blocked_qty,
# MAGIC       cast((CASE WHEN trim(f41021.LILOTS) in ('E','F','H','R','S','V','X') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as blocked_qty,
# MAGIC       cast(0 as double) as rstrct_qty,
# MAGIC       cast(null as double)  as unit_cost_co_amt,
# MAGIC       cast(null as double) as unit_cost_lcur_amt,
# MAGIC       cast(null as double) as unit_cost_co_pmar_amt,
# MAGIC       coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity ,
# MAGIC       case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' else trim(f0005.DRDL01) end as business_unit,
# MAGIC       cast(trim(F0005_div.DRDL01) as string) as div_cd,
# MAGIC       cast(f41021.LILOTS as string) as lot_stat_cd,
# MAGIC       cast(F0005_LOT.DRDL01 as string) as lot_stat_nm,
# MAGIC       cast(f41021.LIPBIN as string) as prim_loc_flg,
# MAGIC       'NA' as flr_stk_cd,
# MAGIC       'NA' as rejected_mat_flag,
# MAGIC       cast(F0005_UOM.DRDL01 as string) as stk_uom_cd,
# MAGIC       'NA' as  stk_uom_nm,
# MAGIC       cast(f4102.IBSRP3 as string) as prod_family,
# MAGIC       cast(F0005_PROD.DRDL01 as string) as prod_fam_typ,
# MAGIC       'NA' as gl_account,
# MAGIC       'NA' as src_crt_by,
# MAGIC       'NA' as src_crt_ts,
# MAGIC       cast(current_timestamp as string) as rec_crt_ts,
# MAGIC       cast(current_timestamp as string) as rec_updt_ts
# MAGIC   from F41021  
# MAGIC   left outer join f4102_adt f4102 
# MAGIC       on trim(cast(cast(f4102.ibitm as integer) as string))= trim(cast(cast(F41021.LIITM as integer) as string)) 
# MAGIC         and trim(F41021.LIMCU) = trim(f4102.ibmcu)
# MAGIC   left outer join f4105
# MAGIC       on  trim(cast(cast(F41021.LIITM as integer) as string)) = trim(cast(cast(f4105.COITM as integer) as string)) 
# MAGIC         and trim(F41021.LIMCU) = trim(f4105.COMCU)
# MAGIC   left outer join f0006 
# MAGIC       on f0006.mcmcu = f41021.limcu
# MAGIC   left outer join f4101_adt f4101 
# MAGIC       on trim(f4101.IMITM) = trim(f41021.LIITM)
# MAGIC   left outer join f0010 
# MAGIC       on f0010.CCCO= f0006.MCCO
# MAGIC   left outer join d_date d_dt 
# MAGIC       on date_format(if(date_format(current_timestamp,'%h')>='0' 
# MAGIC         and date_format(current_timestamp,'%h')<='12',date_sub(current_date,1),current_date),'yMMdd') = cast (d_dt.dt_key as string)
# MAGIC   left outer join (
# MAGIC           select 
# MAGIC               curr_mnth.PMAR_RT as CO_PMAR_RT, 
# MAGIC               curr_mnth.CURNCY_MTH_RT_KEY,
# MAGIC               curr_mnth.YR_MTH_NBR,
# MAGIC               curr_mnth.FROM_CURNCY_CD 
# MAGIC           from d_curncy_mth_rt curr_mnth 
# MAGIC           where TO_CURNCY_CD =  'USD'
# MAGIC           ) co_curr_mth  
# MAGIC       on co_curr_mth.YR_MTH_NBR = d_dt.fscl_yr_prd_nbr 
# MAGIC         and co_curr_mth.FROM_CURNCY_CD = COALESCE(f0010.CCCRCD,'USD')
# MAGIC   left outer join  edp_lkup hfm 
# MAGIC       on hfm.lkup_key_01 = f0006.MCCO 
# MAGIC         and  hfm.LKUP_TYP_NM ='CO_TO_HFM' 
# MAGIC         and hfm.lkup_key_02 = 'E1LSG'
# MAGIC   left outer join f0005 F0005_LOT 
# MAGIC       on trim(F0005_LOT.DRSY)= '41' 
# MAGIC         and trim(F0005_LOT.DRRT)='L' 
# MAGIC         and trim(F0005_LOT.DRKY)=trim(f41021.LILOTS)
# MAGIC   left outer join f0005 F0005_UOM 
# MAGIC       on trim(F0005_UOM.DRSY)= '00' 
# MAGIC         and trim(F0005_UOM.DRRT)='UM' 
# MAGIC         and trim(F0005_UOM.DRKY)=trim(f4101.IMUOM1)
# MAGIC   left outer join f0005 F0005_PROD 
# MAGIC       on trim(F0005_PROD.DRSY)= '41' 
# MAGIC         and trim(F0005_PROD.DRRT)='S3'  
# MAGIC         and trim(F0005_PROD.DRKY)=trim(f4102.IBSRP3)
# MAGIC   left outer join f0005 f0005 
# MAGIC       on trim(f0005.DRSY)= '41' 
# MAGIC         and trim(f0005.DRRT)='S2' 
# MAGIC         and trim(f0005.DRKY)=trim(f4102.IBSRP2)
# MAGIC   left outer join f0005 F0005_div 
# MAGIC       on trim(F0005_div.DRSY)= '41' 
# MAGIC         and trim(F0005_div.DRRT)='S1' 
# MAGIC         and trim(F0005_div.DRKY)=trim(f4102.IBSRP1)
# MAGIC   where trim(f4102.iblitm) = '9101083' and trim(f4102.ibmcu) = 'US01'
# MAGIC )
# MAGIC 
# MAGIC select 
# MAGIC   t1.*,
# MAGIC   t2.consignment_qty,
# MAGIC   case when t1.item_nbr is null then t2.item_nbr else t1.item_nbr end as item_nbr,
# MAGIC   case when t1.plant_cd is null then t2.plant_cd else t1.plant_cd end as plant_cd,
# MAGIC   case when t1.lot_nbr is null then t2.lot_nbr else t1.lot_nbr end as lot_nbr,
# MAGIC   case when t1.invntry_loc_name is null then t2.invntry_loc_name else t1.invntry_loc_name end as invntry_loc_name
# MAGIC from f_invntry_bal_dly_hist t1
# MAGIC full outer join consignment_qty_records t2
# MAGIC   on t1.item_nbr=t2.item_nbr
# MAGIC     and t1.plant_cd=t2.plant_cd
# MAGIC     and t1.lot_nbr=t2.lot_nbr
# MAGIC     and t1.invntry_loc_name=t2.invntry_loc_name
# MAGIC -- where t1.item_nbr = '9101083' and t1.plant_cd = 'US01'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /**************************************************************************
# MAGIC Artefact Name :- inventory Balance
# MAGIC Description :- f_inventory_balance fro nav_ger 
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC Change Log
# MAGIC Version :        Date :                                Description                                     Changed By
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC 0.0            04/13/2022                      First draft of sql file                       Abhishek Ranjan
# MAGIC 1.0            06/16/2022                      Repleced source table itemconsumption_sales         Bhaskar Reddy
# MAGIC                                                  with item_qty_on_hand    
# MAGIC 1.1            02/01/2022                        Replaced blocked_qty logic                          Raul Martinez
# MAGIC                                                  Replaced qa_inspn_qty logic                         Raul Martinez
# MAGIC                                                  Replaced avail_qty logic                            Raul Martinez
# MAGIC                                                  Added consignment_qty logic                         Raul Martinez
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC select   
# MAGIC   'nav_ger' as src_sys_cd,          
# MAGIC   cast(itmqtyOH.item_no_ as string) as item_nbr, 
# MAGIC   cast(ItemWOPD.Description as string) as item_desc, 
# MAGIC   'NA' as item_type, 
# MAGIC   'NA' as item_type_desc, 
# MAGIC   'CAGER' as plant_cd, 
# MAGIC   cast(date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd') as string) as capture_dt,      
# MAGIC   cast(0 as decimal(38,0)) as capture_yr_mth_nbr, 
# MAGIC   'NA' as lot_nbr, 
# MAGIC   cast(itmqtyOH.Location_Code as string) as invntry_loc_cd, 
# MAGIC   cast(location.Name as string) as invntry_loc_name, 
# MAGIC   'NA' as strg_bin_cd, 
# MAGIC   'NA' as co_cd, 
# MAGIC   'NA' as co_name, 
# MAGIC   'NA' as co_curncy_cd, 
# MAGIC   'NA' as invntry_stk_type_cd, 
# MAGIC   'NA' as valuation_cd, 
# MAGIC   'NA' as recpt_dt, 
# MAGIC --   cast(0 as double) as avail_qty, 
# MAGIC   case when trim(cast(itmqtyOH.Location_Code as string)) in ('HAUPT','GERMERING','LIN','MATRIUM','USED') then cast(itmqtyOH.qty_on_hand as double) else 0 end as avail_qty,
# MAGIC   cast(itmqtyOH.qty_on_hand as double) as on_hand_qty, 
# MAGIC   cast(0 as double) as on_hold_qty, 
# MAGIC   cast(0 as double) as transfer_qty, 
# MAGIC --   cast(0 as double) as qa_inspn_qty, 
# MAGIC   case when trim(cast(itmqtyOH.Location_Code as string)) in ('QS-WE','QS-RÜCK') then cast(itmqtyOH.qty_on_hand as double) else 0 end as qa_inspn_qty,
# MAGIC --   cast(0 as double) as blocked_qty, 
# MAGIC   case when trim(cast(itmqtyOH.Location_Code as string)) in ('FG-REP','MARKETING','SEED','MKT_DEMO','INT-REP') then cast(itmqtyOH.qty_on_hand as double) else 0 end as blocked_qty,
# MAGIC   cast(0 as double) as rstrct_qty, 
# MAGIC   cast(0 as double) as unit_cost_co_amt, 
# MAGIC   cast(0 as double) as unit_cost_lcur_amt, 
# MAGIC   cast(0 as double) as unit_cost_co_pmar_amt, 
# MAGIC   cast(0 as double) as co_curncy_mth_pmar_rt, 
# MAGIC   cast(0 as double) as consignment_qty, 
# MAGIC   'NA' as hfm_entity, 
# MAGIC   'NA' as business_unit, 
# MAGIC   'NA' as div_cd, 
# MAGIC   'NA' as lot_stat_cd, 
# MAGIC   'NA' as lot_stat_nm, 
# MAGIC   'NA' as prim_loc_flg, 
# MAGIC   'NA' as flr_stk_cd, 
# MAGIC   'NA' as rejected_mat_flag, 
# MAGIC -- ItemWOPD.Unit_of_Measure_Code as stk_uom_cd, 
# MAGIC   itmqtyOH.Unit_of_Measure_Code as stk_uom_cd,
# MAGIC   Unit_of_Measure.Description as stk_uom_nm, 
# MAGIC   'NA' as prod_family, 
# MAGIC   'NA' as prod_fam_typ, 
# MAGIC   'NA' as src_crt_by, 
# MAGIC   'NA' as src_crt_ts, 
# MAGIC   cast(current_timestamp() as string) as rec_crt_ts, 
# MAGIC   cast(current_timestamp() as string) as rec_updt_ts
# MAGIC FROM item_qty_on_hand itmqtyOH
# MAGIC left outer join ItemWOPD 
# MAGIC   on itmqtyOH.Item_No_=ItemWOPD.no_
# MAGIC left outer join location 
# MAGIC   on itmqtyOH.Location_Code=location.code
# MAGIC left outer join Unit_of_Measure 
# MAGIC   on itmqtyOH.Unit_of_Measure_Code=Unit_of_Measure.Code
# MAGIC ;

# COMMAND ----------


