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


f41003 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f41003/")
f41003.createOrReplaceTempView("f41003")


f41002 = spark.read.format('delta').load("s3://tfsdl-e1lsg-prod/processed/delta/proddta/f41002/")
f41002.createOrReplaceTempView("f41002")


# COMMAND ----------

mard = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/mard/")
mard.createOrReplaceTempView("mard")

mchb = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/mchb/")
mchb.createOrReplaceTempView("mchb")

mbew = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/mbew/")
mbew.createOrReplaceTempView("mbew")

t001w = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/t001w/")
t001w.createOrReplaceTempView("t001w")

marc = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/marc/")
marc.createOrReplaceTempView("marc")

t001l = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/t001l/")
t001l.createOrReplaceTempView("t001l")

t001 = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/t001/")
t001.createOrReplaceTempView("t001")

mara = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/mara/")
mara.createOrReplaceTempView("mara")

t006a = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/t006a/")
t006a.createOrReplaceTempView("t006a")

tvko = spark.read.format('delta').load("s3://tfsdl-lsg-d50-prod/processed/delta/sapr3/tvko/")
tvko.createOrReplaceTempView("tvko")



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
# MAGIC 1.6            09-02-2022                          Replaced avail_qty logic                            Raul Martinez
# MAGIC                                                    Replaced on_hand_qty logic                          Raul Martinez
# MAGIC                                                    Replaced qa_inspn_qty logic                         Raul Martinez
# MAGIC                                                    Replaced blocked_qty logic                          Raul Martinez
# MAGIC                                                    Replaced item_nbr logic                             Raul Martinez
# MAGIC                                                    Replaced plant_cd logic                             Raul Martinez
# MAGIC                                                    Replaced lot_nbr logic                              Raul Martinez
# MAGIC                                                    Replaced invntry_loc_name logic                     Raul Martinez
# MAGIC                                                    Added consignment_qty logic                         Raul Martinez
# MAGIC                                                    Added UOM to consignment_qty_records CTE            Vijay Kelkar
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC with consignment_qty_records as (
# MAGIC   select 
# MAGIC     match_UOM.item_nbr as item_nbr,
# MAGIC     match_UOM.plant_cd as plant_cd,
# MAGIC     match_UOM.lot_nbr as lot_nbr,
# MAGIC     match_UOM.invntry_loc_name as invntry_loc_name,
# MAGIC     cast(null as string) as  co_cd,
# MAGIC     sum((case when match_UOM.base_uom <> match_UOM.txn_uom then unmatch_uom.final_qty else match_UOM.original_qty end )) as consignment_qty
# MAGIC   from (
# MAGIC     select 
# MAGIC       cast(trim(f43121.PRLITM) as string) as item_nbr,
# MAGIC       cast(trim(f43092.pxmcu) as string) as plant_cd,
# MAGIC       cast(trim(f43121.pritm) as string) as item_id,
# MAGIC       cast(f43121.prlotn as string) as lot_nbr,
# MAGIC       cast(cast(f43121.PRDOC as integer) as string) as recpt_nbr,
# MAGIC       cast(cast(f43121.PRDOCO as integer) as string) as po_nbr ,
# MAGIC       cast(cast(f43121.PRLNID as integer) as string) as po_line_nbr,
# MAGIC       cast(cast(f43121.PRNLIN as integer) as string) as recpt_line_seq_nbr,
# MAGIC       cast(f43121.prlocn  as string) as invntry_loc_name,
# MAGIC       cast(f43121.PRUOM as string) as txn_uom,
# MAGIC       cast(f4101.IMUOM1 as string) as base_uom,
# MAGIC       f43092.pxqtyo/10000 as original_qty 
# MAGIC     from f43092   
# MAGIC     left outer join f43121 
# MAGIC       on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC         and f43092.pxdcto = f43121.prdcto
# MAGIC         and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC         and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC         and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC     LEFT OUTER JOIN  f4101_adt f4101 
# MAGIC       on trim(f4101.IMLITM)= trim(f43121.PRLITM) 
# MAGIC     where f43092.pxnrou = 'CONS' 
# MAGIC       and f43092.pxoprc = 'CONS' 
# MAGIC       and f43092.pxupib = 'QTO1'
# MAGIC       and f43092.pxqtyo/10000 >0
# MAGIC       and trim(f43121.PRDCT) ='OV'
# MAGIC       and f43121.prmatc in('1','2')
# MAGIC     ) match_UOM
# MAGIC     left outer join (
# MAGIC       select distinct  
# MAGIC         item_nbr,
# MAGIC         item_id,
# MAGIC         plant_cd,
# MAGIC         recpt_nbr,
# MAGIC         lot_nbr,
# MAGIC         po_nbr,
# MAGIC         invntry_loc_name,
# MAGIC         po_line_nbr,
# MAGIC         BASE_UOM,
# MAGIC         txn_uom,
# MAGIC         original_qty,
# MAGIC         --(case when trim(f41003.ucrum) is not null then (original_qty/f41003.UCCONV)/100000000 else nonstd_convertion_inv_qty end) as final_qty,
# MAGIC         case when f41003_from_uom IS NULL AND f41002_from_uom is NULL AND f41002_from_uom_inv is null then original_qty/(f41003.UCCONV/10000000) 
# MAGIC           else nonstd_convertion_inv_qty 
# MAGIC           end as final_qty,
# MAGIC         f41003_from_uom,
# MAGIC         f41003_to_uom,
# MAGIC         f41002_from_uom,
# MAGIC         f41002_to_uom,
# MAGIC         f41002_from_uom_inv,
# MAGIC         f41002_to_uom_inv,
# MAGIC         trim(f41003.ucum) as f41003_from_uom_inv,
# MAGIC         trim(f41003.ucrum) as f41003_to_uom_inv
# MAGIC       from (
# MAGIC         SELECT distinct   -- Part 3
# MAGIC           item_nbr,
# MAGIC           item_id,
# MAGIC           plant_cd,
# MAGIC           lot_nbr,
# MAGIC           invntry_loc_name,
# MAGIC           recpt_nbr,
# MAGIC           po_nbr,
# MAGIC           po_line_nbr,
# MAGIC           BASE_UOM,
# MAGIC           txn_uom,
# MAGIC           f41003_from_uom,
# MAGIC           f41003_to_uom,
# MAGIC           f41002_from_uom,
# MAGIC           f41002_to_uom,
# MAGIC           original_qty,
# MAGIC           --(case when trim(f41002.UMRUM) is not null then (original_qty/f41002.UMCONV)/10000000 else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC           (case when f41003_from_uom IS NULL AND f41002_from_uom is null then original_qty/(f41002.UMCONV/10000000) else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC           trim(f41002.UMRUM) AS f41002_from_uom_inv,
# MAGIC           trim(f41002.UMUM) AS f41002_to_uom_inv
# MAGIC         from (
# MAGIC           SELECT distinct   -- part 2
# MAGIC             item_nbr,
# MAGIC             item_id,
# MAGIC             plant_cd,
# MAGIC             recpt_nbr,
# MAGIC             lot_nbr,
# MAGIC             invntry_loc_name,
# MAGIC             po_nbr,
# MAGIC             po_line_nbr,
# MAGIC             BASE_UOM,
# MAGIC             txn_uom,
# MAGIC             f41003_from_uom,
# MAGIC             f41003_to_uom,
# MAGIC             original_qty,
# MAGIC             (case when f41003_from_uom is null  then (original_qty*f41002.UMCONV)/10000000 else std_convertion_qty end) as nonstd_convertion_qty,
# MAGIC             trim(f41002.UMUM) AS f41002_from_uom,
# MAGIC             trim(f41002.UMRUM) AS f41002_to_uom 
# MAGIC             from 
# MAGIC             (select distinct  -- part 1 
# MAGIC             item_nbr,
# MAGIC             item_id,
# MAGIC             plant_cd,
# MAGIC             lot_nbr,
# MAGIC             invntry_loc_name,
# MAGIC             recpt_nbr,
# MAGIC             po_nbr,
# MAGIC             po_line_nbr,
# MAGIC             BASE_UOM,
# MAGIC             txn_uom,
# MAGIC             original_qty,
# MAGIC             (case when trim(f41003.ucum) is not null then (original_qty*f41003.UCCONV)/10000000 else 0 end ) as  std_convertion_qty,
# MAGIC             trim(f41003.ucum) as f41003_from_uom,
# MAGIC             trim(f41003.ucrum) as f41003_to_uom
# MAGIC             from (
# MAGIC               select 
# MAGIC                 item_nbr,
# MAGIC                 plant_cd,
# MAGIC                 item_id,
# MAGIC                 lot_nbr,
# MAGIC                 recpt_nbr,
# MAGIC                 po_nbr,
# MAGIC                 po_line_nbr,
# MAGIC                 invntry_loc_name,
# MAGIC                 f4101.IMUOM1 as base_uom,
# MAGIC                 txn_uom,
# MAGIC                 original_qty
# MAGIC               from ( 
# MAGIC                 select 
# MAGIC                   cast(trim(f43121.PRLITM) as string) as item_nbr,
# MAGIC                   cast(trim(f43092.pxmcu) as string) as plant_cd,
# MAGIC                   cast(trim(f43121.pritm) as string) as item_id,
# MAGIC                   cast(f43121.prlotn as string) as lot_nbr,
# MAGIC                   cast(cast(f43121.PRDOC as integer) as string) as recpt_nbr,
# MAGIC                   cast(cast(f43121.PRDOCO as integer) as string) as po_nbr ,
# MAGIC                   cast(cast(f43121.PRLNID as integer) as string) as po_line_nbr,
# MAGIC                   cast(cast(f43121.PRNLIN as integer) as string) as recpt_line_seq_nbr,
# MAGIC                   cast(f43121.prlocn  as string) as invntry_loc_name,
# MAGIC                   cast(f43121.PRUOM as string) as txn_uom,
# MAGIC                   f43092.pxqtyo/10000 as original_qty 
# MAGIC                 from f43092   
# MAGIC                 left outer join f43121 
# MAGIC                   on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC                     and f43092.pxdcto = f43121.prdcto
# MAGIC                     and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC                     and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC                     and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC                 where f43092.pxnrou = 'CONS' 
# MAGIC                   and f43092.pxoprc = 'CONS' 
# MAGIC                   and f43092.pxupib = 'QTO1'
# MAGIC                   and f43092.pxqtyo/10000 >0
# MAGIC                   and trim(f43121.PRDCT) ='OV'
# MAGIC                   and f43121.prmatc in('1','2')
# MAGIC               ) f43092
# MAGIC               left outer join f4101_adt f4101
# MAGIC                 on trim(f4101.IMLITM)=trim(f43092.item_nbr)
# MAGIC               where f4101.IMUOM1 <> f43092.txn_uom
# MAGIC                 AND trim(f4101.IMLITM) IS NOT NULL
# MAGIC                 AND trim(f43092.plant_cd) in ('GB01','US01','US02','US03','US05','US15','US23','US24') 
# MAGIC             ) transuom_not_equal_baseuom   -- Base
# MAGIC             left outer join  f41003                               ---- Part 1
# MAGIC             on trim(f41003.ucum) = transuom_not_equal_baseuom.txn_uom
# MAGIC               and trim(f41003.ucrum) = transuom_not_equal_baseuom.BASE_UOM
# MAGIC           ) request_for_nonstd_convertion
# MAGIC           LEFT OUTER JOIN f41002                               --- Part 2 
# MAGIC             on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC               and txn_uom = trim(f41002.UMUM)
# MAGIC               and BASE_UOM = trim(f41002.UMRUM)
# MAGIC           --where trim(f41002.UMUM) IS  null 
# MAGIC         ) req_for_nonstd_conv_inv
# MAGIC         LEFT OUTER JOIN f41002                               -- Part 3
# MAGIC           on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC             and txn_uom = trim(f41002.UMRUM)
# MAGIC             and BASE_UOM = trim(f41002.UMUM)
# MAGIC         --where trim(f41002.UMRUM) IS null
# MAGIC       ) req_for_std_conv_inv
# MAGIC       left outer join  f41003  -- part 4 
# MAGIC         on trim(f41003.ucrum) = req_for_std_conv_inv.txn_uom
# MAGIC           and trim(f41003.ucum) = req_for_std_conv_inv.BASE_UOM
# MAGIC       --WHERE trim(f41003.ucrum) IS NULL
# MAGIC   ) unmatch_uom
# MAGIC     on match_UOM.item_nbr = unmatch_uom.item_nbr
# MAGIC   group by match_UOM.item_nbr,match_UOM.plant_cd,match_UOM.lot_nbr,match_UOM.invntry_loc_name
# MAGIC ),
# MAGIC 
# MAGIC f_invntry_bal_dly_hist as (
# MAGIC   select distinct
# MAGIC       'e1lsg' as src_sys_cd,
# MAGIC       cast(null as string) as item_desc,
# MAGIC --       trim(f4102.iblitm) as item_nbr,
# MAGIC       case when cqr.item_nbr is null then trim(f4102.iblitm) else cqr.item_nbr end as item_nbr,
# MAGIC --       trim(f4102.ibmcu) as plant_cd,
# MAGIC       case when cqr.plant_cd is null then trim(f4102.ibmcu) else cqr.plant_cd end as plant_cd,
# MAGIC --       cast(F41021.lilotn as string) as lot_nbr,
# MAGIC       case when cqr.lot_nbr is null then cast(F41021.lilotn as string) else cqr.lot_nbr end as lot_nbr,
# MAGIC --       cast(F41021.lilocn as string) as invntry_loc_name,
# MAGIC       case when cqr.invntry_loc_name is null then cast(F41021.lilocn as string) else cqr.invntry_loc_name end as invntry_loc_name,
# MAGIC --       cast(0 as double) as consignment_qty,
# MAGIC       case when cqr.consignment_qty is null then cast(0 as double) else cqr.consignment_qty end as consignment_qty,
# MAGIC       '' as item_type,
# MAGIC       '' as item_type_desc,
# MAGIC       cast(date_format(date_sub(current_timestamp,1),'yMMdd') as string) as capture_dt,
# MAGIC       cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC       'NA' as invntry_loc_cd,
# MAGIC       'NA' as strg_bin_cd,
# MAGIC       cast(f0006.mcco as string) as  co_cd,
# MAGIC       (case when cqr.co_cd is null then f0006.mcco else f0006.mcco end) as co_cd_test,
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
# MAGIC       case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' 
# MAGIC         when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' 
# MAGIC         else trim(f0005.DRDL01) 
# MAGIC         end as business_unit,
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
# MAGIC   full outer join consignment_qty_records cqr
# MAGIC       on cqr.item_nbr=trim(f4102.iblitm)
# MAGIC         and cqr.plant_cd=trim(f4102.ibmcu)
# MAGIC         and cqr.lot_nbr=cast(F41021.lilotn as string)
# MAGIC         and cqr.invntry_loc_name=cast(F41021.lilocn as string)
# MAGIC --   where (trim(f4102.iblitm) = '50122M03H25' or cqr.item_nbr = '50122M03H25') and (trim(f4102.ibmcu) = 'US02' or cqr.plant_cd='US02') -- test case 1: no match between tables, just append vals
# MAGIC --   where (trim(f4102.iblitm) = '4358293' or cqr.item_nbr = '4358293') and (trim(f4102.ibmcu) = 'SG05' or cqr.plant_cd='SG05') -- test case 2: all match between tables, just replace vals
# MAGIC )
# MAGIC 
# MAGIC select *
# MAGIC from f_invntry_bal_dly_hist
# MAGIC  where item_nbr = '50122M03H25' and plant_cd='US02' -- test case 1: no match between tables, just append vals
# MAGIC -- where item_nbr = '4358293' and plant_cd='SG05' -- test case 2: all match between tables, just replace vals
# MAGIC 
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
# MAGIC 1.1            02/09/2022                        Replaced blocked_qty logic                          Raul Martinez
# MAGIC                                                  Replaced qa_inspn_qty logic                         Raul Martinez
# MAGIC                                                  Replaced avail_qty logic                            Raul Martinez
# MAGIC                                                  Added consignment_qty logic                         Raul Martinez
# MAGIC                                                  Added prod_key column                               Raul Martinez
# MAGIC                                                  Added plant_key column                              Raul Martinez
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
# MAGIC   cast(current_timestamp() as string) as rec_updt_ts,
# MAGIC   'nav_ger' || '|' || itmqtyOH.Item_No_ as prod_key,
# MAGIC   'nav_ger' || '|' || 'CAGER' as plant_key
# MAGIC FROM item_qty_on_hand itmqtyOH
# MAGIC left outer join ItemWOPD 
# MAGIC   on itmqtyOH.Item_No_=ItemWOPD.no_
# MAGIC left outer join location 
# MAGIC   on itmqtyOH.Location_Code=location.code
# MAGIC left outer join Unit_of_Measure 
# MAGIC   on itmqtyOH.Unit_of_Measure_Code=Unit_of_Measure.Code
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Databricks notebook source
# MAGIC 
# MAGIC /**************************************************************************
# MAGIC Artefact Name :- inventory Balance pr1
# MAGIC Description :- f_inventory_balance_pr1 fact sql for cad 
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC Change Log
# MAGIC Version :        Date :                                Description                                     Changed By
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC --0.0            06-nov-2021	                     First draft of sql file    		       SMITA RAUT
# MAGIC --1.0            27-06-2022                        Added Consignment_qty coloumn                       Bhaskar Reddy
# MAGIC --2.0            28-06-2022                        Updated klabs in mard logic                         Bhaskar Reddy
# MAGIC --3.0	         01-09-2022		      Added union query to bring data from(MSKA+ MSKU+ MSLB)   Kanika Singh
# MAGIC --4.0	         04-10-2022			  Added unrstrct_avail_qty column		       Asawari Sorte
# MAGIC --5.0	         06-10-2022			  Added acct_asgn_grp column			       Asawari Sorte
# MAGIC --6.0	         12-10-2022			Added speclty_stk_type_nm, acct_asgn_grp_nm, added     Asawari Sorte
# MAGIC 						union query to bring data from mkol
# MAGIC --7.0		 11-11-2022			updated logic for invntry_bal_dly_hist_key,co_nm,      Asawari Sorte 
# MAGIC 						hfm_entity_key, hfm_entity_cd, prod_key,plant_key,
# MAGIC 						plant_nm, prod_plant_key, prod_cost_key			
# MAGIC --8.0		 18-11-2022			Updated logic for invntry_stk_type_nm, acct_asgn_grp,  Asawari Sorte
# MAGIC 						stk_uom_cd, stk_uom_nm, uom_code
# MAGIC --9.0		 09-12-2022 			Added prod_line_cd, demo_invntry_flg,                  Asawari Sorte
# MAGIC 						seed_invntry_flg,  svc_invntry_flg, unit_qty
# MAGIC --10.0		 27-01-2023			union query to bring data from mspr							Asawari Sorte
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC 
# MAGIC SELECT
# MAGIC src_sys_cd,
# MAGIC item_nbr,
# MAGIC item_desc,            
# MAGIC item_type,              
# MAGIC item_type_desc,          
# MAGIC plant_cd,
# MAGIC capture_dt,
# MAGIC capture_yr_mth_nbr, 
# MAGIC lot_nbr, 
# MAGIC invntry_loc_cd,
# MAGIC invntry_loc_name,        
# MAGIC strg_bin_cd,    
# MAGIC co_cd,
# MAGIC co_name, 
# MAGIC co_curncy_cd,            
# MAGIC invntry_stk_type_cd,     
# MAGIC valuation_cd,    
# MAGIC recpt_dt,
# MAGIC avail_qty, 
# MAGIC unrstrct_avail_qty,              
# MAGIC on_hand_qty,             
# MAGIC on_hold_qty,            
# MAGIC transfer_qty,            
# MAGIC qa_inspn_qty,           
# MAGIC blocked_qty,           
# MAGIC rstrct_qty,            
# MAGIC unit_cost_co_amt,
# MAGIC unit_cost_lcur_amt, 
# MAGIC unit_cost_co_pmar_amt,
# MAGIC co_curncy_mth_pmar_rt,
# MAGIC hfm_entity,  
# MAGIC business_unit,           
# MAGIC div_cd,  
# MAGIC lot_stat_cd,             
# MAGIC lot_stat_nm,            
# MAGIC prim_loc_flg,           
# MAGIC flr_stk_cd,          
# MAGIC rejected_mat_flag,     
# MAGIC stk_uom_cd,     
# MAGIC stk_uom_nm,            
# MAGIC prod_family,           
# MAGIC uom_code,
# MAGIC prft_cntr,          
# MAGIC prod_fam_typ,            
# MAGIC src_crt_by,     
# MAGIC src_crt_ts,            
# MAGIC rec_crt_ts,              
# MAGIC rec_updt_ts,             
# MAGIC gl_account,       
# MAGIC invntry_bal_dly_hist_key,    
# MAGIC co_nm,   
# MAGIC hfm_entity_key,       
# MAGIC hfm_entity_cd,    
# MAGIC prod_key,
# MAGIC plant_key,               
# MAGIC plant_nm,
# MAGIC prod_plant_key,       
# MAGIC prod_cost_key,        
# MAGIC prod_cost_hist_key,     
# MAGIC co_key,  
# MAGIC invntry_stk_type_nm, 
# MAGIC consnmt_qty,           
# MAGIC consnmt_qa_inspn_qty,   
# MAGIC consnmt_rstrct_qty, 
# MAGIC consnmt_blocked_qty,
# MAGIC sched_delvr_qty,        
# MAGIC consignment_qty,  
# MAGIC co_usd_curncy_mth_rt_key,       
# MAGIC afflt_dt_key,         
# MAGIC --expire_dt_key,          
# MAGIC dsna_dt_key,
# MAGIC dealer_dsna_dt_key,     
# MAGIC bu_cd,   
# MAGIC bu_nm,   
# MAGIC invntry_load_desc,
# MAGIC ship_to_cust_key,     
# MAGIC ship_to_cust_id,       
# MAGIC ship_to_cust_nm,       
# MAGIC sfty_stk_fill_flg,      
# MAGIC speclty_stk_type_cd,
# MAGIC speclty_stk_type_nm,
# MAGIC acct_asgn_grp,
# MAGIC acct_asgn_grp_nm,
# MAGIC updt_proc_ts,
# MAGIC src_updt_ts,
# MAGIC sched_delvr_dt,
# MAGIC prod_line_cd,  
# MAGIC demo_invntry_flg,  
# MAGIC seed_invntry_flg,  
# MAGIC svc_invntry_flg,  
# MAGIC unit_qty  
# MAGIC FROM
# MAGIC (
# MAGIC (select distinct
# MAGIC 'gbl' as src_sys_cd,
# MAGIC cast(mard.matnr as string) as item_nbr,
# MAGIC --cast(d_product.prod_desc as string) as item_desc,
# MAGIC cast(MAKT.MAKTX as string) as item_desc,
# MAGIC '' as item_type,
# MAGIC '' as item_type_desc,
# MAGIC cast(mard.werks as string) as plant_cd,
# MAGIC date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd') as capture_dt,
# MAGIC cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC cast(mchb.charg as string) as lot_nbr,
# MAGIC cast(mard.lgort as string) as invntry_loc_cd,
# MAGIC cast(t001l.lgobe as string) as invntry_loc_name,
# MAGIC mard.lgpbe as strg_bin_cd,
# MAGIC if (trim(t001w.vkorg) = '',t001w.ekorg,t001w.vkorg) as co_cd,
# MAGIC cast(d_company.co_nm as string) as co_name,
# MAGIC cast(d_company.co_curncy_cd as string) as co_curncy_cd,
# MAGIC (case
# MAGIC when mara.mtart='RAW' then 'Raw Material'
# MAGIC when mara.mtart='FP' then 'Finished Goods'
# MAGIC else 'Raw Material' 
# MAGIC end) as invntry_stk_type_cd,
# MAGIC cast(mbew.bklas as string) as valuation_cd,
# MAGIC cast(s032.letztzug as string) as recpt_dt,
# MAGIC round(if(t001l.diskz='1',0,if(mchb.charg is null,mard.labst,mchb.clabs)),2) as avail_qty,
# MAGIC round(if(mchb.charg is null,mard.labst,mchb.clabs),2) as unrstrct_avail_qty,
# MAGIC round(if(mchb.charg is null,coalesce(mard.umlme+mard.insme+mard.einme+mard.speme+mard.labst,0), coalesce(mchb.cumlm + mchb.cinsm + mchb.ceinm + mchb.cspem + mchb.clabs,0)),2) as on_hand_qty,
# MAGIC round(if(mchb.charg is null,coalesce(mard.umlme+mard.insme+mard.einme+mard.speme+mard.labst,0), coalesce(mchb.cumlm + mchb.cinsm + mchb.ceinm + mchb.cspem + mchb.clabs,0))- cast(if(t001l.diskz='1',0,if(mchb.charg is null,mard.labst,mchb.clabs)) as double),2) as on_hold_qty,
# MAGIC if(mchb.charg is null,coalesce(mard.umlme,0),coalesce(mchb.cumlm,0)) as transfer_qty,
# MAGIC if(mchb.charg is null , coalesce(mard.insme,0),coalesce(mchb.cinsm,0)) as qa_inspn_qty,
# MAGIC if(mchb.charg is null , coalesce(mard.speme,0),coalesce(mchb.cspem,0)) as blocked_qty,
# MAGIC if(mchb.charg is null ,coalesce(mard.einme,0),coalesce(mchb.ceinm,0)) as rstrct_qty,
# MAGIC case when trim(mbew.vprsv) = 'V' and d_company.co_curncy_cd in('KRW','JPY') then round(cast((mbew.verpr*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'V' then round(cast((mbew.verpr/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'S' and d_company.co_curncy_cd in('KRW','JPY') then round(cast((mbew.stprs*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'S' then round(cast((mbew.stprs/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC end as unit_cost_co_amt,
# MAGIC case when trim(mbew.vprsv) = 'V' and d_company.co_curncy_cd in('KRW','JPY') then round(cast((mbew.verpr*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'V' then round(cast((mbew.verpr/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'S' and d_company.co_curncy_cd in('KRW','JPY') then round(cast((mbew.stprs*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'S' then round(cast((mbew.stprs/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC end as unit_cost_lcur_amt, 
# MAGIC case when trim(mbew.vprsv) = 'V' and d_company.co_curncy_cd in('KRW','JPY') then round(cast((mbew.verpr*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * co_curr_mth.CO_PMAR_RT as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'V' then round(cast((mbew.verpr/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * co_curr_mth.CO_PMAR_RT as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'S' and d_company.co_curncy_cd in('KRW','JPY') then round(cast((mbew.stprs*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * co_curr_mth.CO_PMAR_RT as double),2)
# MAGIC 	 when trim(mbew.vprsv) = 'S' then round(cast((mbew.stprs/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * co_curr_mth.CO_PMAR_RT as double),2)
# MAGIC end as unit_cost_co_pmar_amt,
# MAGIC --round(cast(prod_cost.unit_cost_amt as double),2) as unit_cost_co_amt,
# MAGIC --round(cast(prod_cost.unit_cost_amt as double),2) as unit_cost_lcur_amt,
# MAGIC --round(cast(prod_cost.unit_cost_amt* co_curr_mth.CO_PMAR_RT as double) ,2) as unit_cost_co_pmar_amt,
# MAGIC round(cast(co_curr_mth.CO_PMAR_RT as double),4) as co_curncy_mth_pmar_rt,
# MAGIC coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity ,
# MAGIC coalesce(case when b_unit.LKUP_VAL_05='M&M' then 'PPA' else b_unit.LKUP_VAL_05 end,'NA') as business_unit ,
# MAGIC coalesce(zproftcntr_att.zdivision,'NA') as div_cd,
# MAGIC 'NA' as lot_stat_cd,
# MAGIC 'NA' as lot_stat_nm,
# MAGIC 'NA' as prim_loc_flg,
# MAGIC 'NA' as flr_stk_cd,
# MAGIC (case
# MAGIC when mard.werks = '0080' and mard.lgort in ('EFM1','SARA','SB05','SB27','SB38','SB55','SB70','SEN1','INCM') then 'Y'
# MAGIC when mard.werks = '2001' and mard.lgort in ('RM10','RM11') Then 'Y'
# MAGIC else 'N'
# MAGIC end) as rejected_mat_flag,
# MAGIC cast(mara.meins as string) as stk_uom_cd,
# MAGIC cast(t006a_meins.MSEHL as string) as stk_uom_nm,
# MAGIC coalesce(b_unit.lkup_val_04,'NA') as prod_family,
# MAGIC cast(mara.meins as string) as uom_code,
# MAGIC marc.prctr  as prft_cntr ,
# MAGIC coalesce(b_unit.lkup_val_02,'NA') as prod_fam_typ,
# MAGIC 'NA' as src_crt_by,
# MAGIC mard.ersda as src_crt_ts,
# MAGIC cast(current_timestamp as string) as rec_crt_ts,
# MAGIC cast(current_timestamp as string) as rec_updt_ts,
# MAGIC t0301.konts as gl_account,       
# MAGIC (date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd')) || '|' || 'gbl' || '|' || mard.matnr || '|' || mard.werks || '|' || mchb.charg || '|' || mard.lgort || '|' || mard.lgpbe as invntry_bal_dly_hist_key,    
# MAGIC cast(d_company.co_nm as string) as co_nm,   
# MAGIC 'gbl' || '|' || hfm.lkup_val_01 as hfm_entity_key,       
# MAGIC hfm.lkup_val_01 as hfm_entity_cd,    
# MAGIC 'gbl' || '|' || mard.matnr as prod_key,
# MAGIC 'gbl' || '|' || mard.werks as plant_key,             
# MAGIC t001w.name1 as plant_nm,
# MAGIC 'gbl' || '|' || mard.matnr || '|' ||mard.werks as prod_plant_key,       
# MAGIC 'gbl' || '|' || mbew.matnr || '|' || mbew.bwkey || '|' || mbew.bwtar as prod_cost_key,        
# MAGIC null as prod_cost_hist_key,     
# MAGIC 'gbl' || '|' ||t001w.vkorg as co_key,  
# MAGIC (case when mara.mtart='RAW' then 'Raw Material'
# MAGIC 	when mara.mtart='FP' then 'Finished Goods'
# MAGIC 	else 'Raw Material' 
# MAGIC 	end) as invntry_stk_type_nm , 
# MAGIC cast(mard.klabs as decimal(38,6)) as consnmt_qty,        
# MAGIC cast(mard.kinsm as decimal(38,6)) as consnmt_qa_inspn_qty ,   
# MAGIC cast(mard.keinm as decimal(38,6))  as consnmt_rstrct_qty , 
# MAGIC cast(mard.kspem as decimal(38,6)) as consnmt_blocked_qty,
# MAGIC cast(0 as decimal(38,6)) as sched_delvr_qty ,  
# MAGIC cast(mard.klabs as decimal(38,6)) as consignment_qty,
# MAGIC cast(co_curr_mth.curncy_mth_rt_key as string) as co_usd_curncy_mth_rt_key ,       
# MAGIC null as afflt_dt_key ,         
# MAGIC --cast(0 as decimal(38,0)) as expire_dt_key ,          
# MAGIC null as dsna_dt_key ,            
# MAGIC null as dealer_dsna_dt_key ,     
# MAGIC '' as bu_cd,   
# MAGIC '' as bu_nm,   
# MAGIC '' as invntry_load_desc ,      
# MAGIC '' as ship_to_cust_key,     
# MAGIC '' as ship_to_cust_id ,       
# MAGIC '' as ship_to_cust_nm,       
# MAGIC 'NA' as sfty_stk_fill_flg,      
# MAGIC '?' as speclty_stk_type_cd,
# MAGIC '?' as speclty_stk_type_nm,
# MAGIC case when mvke_cnt.assg_grp_cnt=1 then mvke.ktgrm else t001w.vtweg end as acct_asgn_grp, 
# MAGIC tvkmt.vtext as acct_asgn_grp_nm,
# MAGIC null as updt_proc_ts,
# MAGIC '' as src_updt_ts,
# MAGIC '' as sched_delvr_dt,
# MAGIC marc.prctr as prod_line_cd,  
# MAGIC case when mard.lgort in('1000','EXPM') then 'Y' else 'N' end as demo_invntry_flg,  
# MAGIC case when mard.lgort in('1060','EXPS') then 'Y' else 'N' end as seed_invntry_flg,  
# MAGIC case when 30=case when mvke_cnt.assg_grp_cnt=1 then mvke.ktgrm else t001w.vtweg end then 'Y' else 'N' end as svc_invntry_flg,  
# MAGIC cast(mbew.peinh as double) as unit_qty  
# MAGIC from 
# MAGIC (select * from mard  where (mard.labst+mard.umlme+mard.insme+mard.einme+mard.speme+mard.klabs)!=0) mard
# MAGIC left outer join (select * from mchb where  (mchb.cinsm + mchb.ceinm + mchb.cspem + mchb.cumlm + mchb.clabs) != 0) mchb on trim(mard.matnr)= trim(mchb.matnr) and mard.werks=mchb.werks and mard.lgort=mchb.lgort
# MAGIC left outer join mbew on trim(mard.matnr) = trim(mbew.matnr) and trim(mard.werks) = trim(mbew.bwkey)
# MAGIC left outer join (select distinct bklas, ktosl, konts, ktopl from t030 where ktosl='BSX') as t0301 on t0301.bklas=mbew.bklas
# MAGIC left outer join  t001w t001w on mard.werks = t001w.werks
# MAGIC left outer join marc on mard.werks=marc.werks and trim(mard.matnr) = trim(marc.matnr) and mard.mandt = marc.mandt
# MAGIC --left outer join d_product_plant d_prod_plnt on mard.werks=d_prod_plnt.plant_cd and trim(mard.matnr)= trim(d_prod_plnt.sku) and d_prod_plnt.src_sys_cd = 'gbl'
# MAGIC left outer join s032 on trim(mard.matnr)= trim(s032.matnr) and mard.werks=s032.werks and mard.lgort=s032.lgort
# MAGIC left outer join t001l on mard.werks= t001l.werks and mard.lgort= t001l.lgort 
# MAGIC left outer join d_company on (case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end) = d_company.co_cd and d_company.src_sys_cd ='gbl'
# MAGIC left outer join mara on trim(mara.matnr)= trim(mard.matnr) and mara.mtart<> 'NVAL'
# MAGIC left outer join zproftcntr_att on marc.prctr = zproftcntr_att.zrprctr and zproftcntr_att.mandt='100'
# MAGIC  
# MAGIC left outer join edp_lkup b_unit on marc.prctr= b_unit.lkup_key_01 and LKUP_TYP_NM = 'CAD_PROFIT_CENTER_BU_PL_LKUP'
# MAGIC left outer join (select MSEHL,msehi  from t006a  where t006a.mandt = '100' and t006a.SPRAS = 'E')t006a_meins on mara.meins= t006a_meins.msehi
# MAGIC left outer join (select distinct  mandt, matnr,vkorg, ktgrm from mvke)mvke on mard.matnr = mvke.matnr and mard.mandt = mvke.mandt and mard.werks = mvke.vkorg
# MAGIC left outer join (select mandt, matnr,vkorg,count(*) as assg_grp_cnt from mvke group by mandt, matnr,vkorg) mvke_cnt on mard.matnr = mvke_cnt.matnr and mard.mandt = mvke_cnt.mandt and mard.werks = mvke_cnt.vkorg 
# MAGIC left outer join tvkmt on tvkmt.spras='E' and tvkmt.ktgrm = case when mvke_cnt.assg_grp_cnt=1 then mvke.ktgrm else t001w.vtweg end  
# MAGIC 
# MAGIC left outer join  edp_lkup hfm on hfm.lkup_key_01 = d_company.co_cd and  hfm.LKUP_TYP_NM ='CO_TO_HFM' and hfm.lkup_key_02 = 'GBL'
# MAGIC --left outer join d_product on trim(mard.matnr)= trim(d_product.sku) and d_product.src_sys_cd = 'gbl' 
# MAGIC left outer join MAKT MAKT on mard.matnr= MAKT.MATNR and MAKT.SPRAS='E'
# MAGIC  
# MAGIC left outer join d_date d_dt on date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd') = cast (d_dt.dt_key as string)
# MAGIC  
# MAGIC left outer join (select curr_mnth.PMAR_RT as CO_PMAR_RT, curr_mnth.CURNCY_MTH_RT_KEY,curr_mnth.YR_MTH_NBR,curr_mnth.FROM_CURNCY_CD
# MAGIC from d_curncy_mth_rt curr_mnth where TO_CURNCY_CD =  'USD') co_curr_mth on co_curr_mth.YR_MTH_NBR = d_dt.fscl_yr_prd_nbr and co_curr_mth.FROM_CURNCY_CD = d_company.co_curncy_cd
# MAGIC ) 
# MAGIC UNION ALL
# MAGIC (SELECT distinct
# MAGIC 'gbl' as src_sys_cd,
# MAGIC consnmnt_tbls.matnr as item_nbr,
# MAGIC makt.maktx as item_desc,            
# MAGIC '' as item_type,              
# MAGIC '' as item_type_desc,          
# MAGIC consnmnt_tbls.werks as plant_cd,
# MAGIC date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd') as capture_dt,
# MAGIC cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr, 
# MAGIC consnmnt_tbls.charg as lot_nbr, 
# MAGIC consnmnt_tbls.lgort as invntry_loc_cd,
# MAGIC t001l.lgobe as invntry_loc_name,        
# MAGIC consnmnt_tbls.strg_bin as strg_bin_cd,    
# MAGIC (case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end) as co_cd,
# MAGIC t001.butxt as co_name, 
# MAGIC t001.waers as co_curncy_cd,            
# MAGIC case when ltrim(rtrim(mbew.bklas)) in ('FP1') then 'FG'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('RAW1') then 'RM'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('SEM1','SEM2') then 'WIP' else 'OT' end as invntry_stk_type_cd,     
# MAGIC cast(mbew.bklas as string) as valuation_cd,    
# MAGIC cast(s032.letztzug as string) as recpt_dt,
# MAGIC round(cast(0 as double),2) as avail_qty,
# MAGIC round(cast(0 as double),2) as unrstrct_avail_qty,               
# MAGIC round(cast(consnmnt_tbls.lblab+consnmnt_tbls.lbins+consnmnt_tbls.lbein+consnmnt_tbls.kaspe as double),2) as  on_hand_qty,             
# MAGIC round(cast(case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.kaspe else 0 end as double),2) as on_hold_qty,            
# MAGIC round(cast(0 as double),2) as transfer_qty,            
# MAGIC round(cast(case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.lbins else 0 end as double),2) as qa_inspn_qty,           
# MAGIC round(cast(case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.kaspe else 0 end as double),2) as blocked_qty,           
# MAGIC round(cast(case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.lbein else 0 end as double),2) as rstrct_qty, 
# MAGIC case when consnmnt_tbls.sobkz = 'E' then 
# MAGIC 	 case when trim(ebew.vprsv)='V' and t001.waers in('KRW','JPY') then round(cast((ebew.verpr*100/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) as double),2)          
# MAGIC 		  when trim(ebew.vprsv)='V' then round(cast((ebew.verpr/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) as double),2)
# MAGIC 		  when trim(ebew.vprsv)='S' and t001.waers in('KRW','JPY') then round(cast((ebew.stprs*100/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) as double),2)          
# MAGIC 		  when trim(ebew.vprsv)='S' then round(cast((ebew.stprs/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) as double),2) end
# MAGIC 	 when consnmnt_tbls.sobkz = 'Q' then 
# MAGIC 	 case when trim(qbew.vprsv)='V' and t001.waers in('KRW','JPY') then round(cast((qbew.verpr*100/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2)          
# MAGIC 		  when trim(qbew.vprsv)='V' then round(cast((qbew.verpr/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2)
# MAGIC 		  when trim(qbew.vprsv)='S' and t001.waers in('KRW','JPY') then round(cast((qbew.stprs*100/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2)          
# MAGIC 		  when trim(qbew.vprsv)='S' then round(cast((qbew.stprs/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2) end	  
# MAGIC else case when trim(mbew.vprsv) = 'V' and t001.waers in('KRW','JPY') then round(cast((mbew.verpr*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'V' then round(cast((mbew.verpr/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'S' and t001.waers in('KRW','JPY') then round(cast((mbew.stprs*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'S' then round(cast((mbew.stprs/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2) end
# MAGIC end as unit_cost_co_amt,
# MAGIC 
# MAGIC case when consnmnt_tbls.sobkz = 'E' then 
# MAGIC 	 case when trim(ebew.vprsv)='V'	and t001.waers in('KRW','JPY') then round(cast((ebew.verpr*100/case when ebew.peinh=0 then 1 else ebew.peinh end) as double),2)	
# MAGIC 		  when trim(ebew.vprsv)='V' then round(cast((ebew.verpr/case when ebew.peinh=0 then 1 else ebew.peinh end) as double),2)
# MAGIC 		  when trim(ebew.vprsv)='S'	and t001.waers in('KRW','JPY') then round(cast((ebew.stprs*100/case when ebew.peinh=0 then 1 else ebew.peinh end) as double),2)	
# MAGIC 		  when trim(ebew.vprsv)='S' then round(cast((ebew.stprs/case when ebew.peinh=0 then 1 else ebew.peinh end) as double),2) end
# MAGIC 	 when consnmnt_tbls.sobkz = 'Q' then 
# MAGIC 	 case when trim(qbew.vprsv)='V' and t001.waers in('KRW','JPY') then round(cast((qbew.verpr*100/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2)          
# MAGIC 		  when trim(qbew.vprsv)='V' then round(cast((qbew.verpr/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2)
# MAGIC 		  when trim(qbew.vprsv)='S' and t001.waers in('KRW','JPY') then round(cast((qbew.stprs*100/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2)          
# MAGIC 		  when trim(qbew.vprsv)='S' then round(cast((qbew.stprs/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) as double),2) end	  
# MAGIC else case when trim(mbew.vprsv) = 'V' and t001.waers in('KRW','JPY') then round(cast((mbew.verpr*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'V' then round(cast((mbew.verpr/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'S' and t001.waers in('KRW','JPY') then round(cast((mbew.stprs*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'S' then round(cast((mbew.stprs/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) as double),2)end
# MAGIC end as unit_cost_lcur_amt, 
# MAGIC 
# MAGIC case when consnmnt_tbls.sobkz = 'E' then 
# MAGIC      case when trim(ebew.vprsv)='V'	and t001.waers in('KRW','JPY') then round(cast((ebew.verpr*100/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)	
# MAGIC 		  when trim(ebew.vprsv)='V' then round(cast((ebew.verpr/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)
# MAGIC 		  when trim(ebew.vprsv)='S'	and t001.waers in('KRW','JPY') then round(cast((ebew.stprs*100/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)	
# MAGIC 		  when trim(ebew.vprsv)='S' then round(cast((ebew.stprs/(case when ebew.peinh is null or ebew.peinh=0 then 1 else ebew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2) end
# MAGIC 	 when consnmnt_tbls.sobkz = 'Q' then 
# MAGIC      case when trim(qbew.vprsv)='V'	and t001.waers in('KRW','JPY') then round(cast((qbew.verpr*100/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)	
# MAGIC 		  when trim(qbew.vprsv)='V' then round(cast((qbew.verpr/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)
# MAGIC 		  when trim(qbew.vprsv)='S'	and t001.waers in('KRW','JPY') then round(cast((qbew.stprs*100/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)	
# MAGIC 		  when trim(qbew.vprsv)='S' then round(cast((qbew.stprs/(case when qbew.peinh is null or qbew.peinh=0 then 1 else qbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2) end	  
# MAGIC else case when trim(mbew.vprsv) = 'V' and t001.waers in('KRW','JPY') then round(cast((mbew.verpr*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'V' then round(cast((mbew.verpr/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'S' and t001.waers in('KRW','JPY') then round(cast((mbew.stprs*100/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)
# MAGIC 		  when trim(mbew.vprsv) = 'S' then round(cast((mbew.stprs/(case when mbew.peinh is null OR mbew.peinh = 0 then 1 else mbew.peinh end)) * d_curncy_mth_rt.pmar_rt as double),2)end
# MAGIC end as unit_cost_co_pmar_amt,
# MAGIC round(cast(d_curncy_mth_rt.pmar_rt as double),4) as co_curncy_mth_pmar_rt,
# MAGIC hfm.lkup_val_01 as hfm_entity,  
# MAGIC coalesce(case when b_unit.LKUP_VAL_05='M&M' then 'PPA' else b_unit.LKUP_VAL_05 end,'NA') as business_unit,           
# MAGIC coalesce(zproftcntr_att.zdivision,'NA') as div_cd,  
# MAGIC '' as lot_stat_cd,             
# MAGIC '' as lot_stat_nm ,            
# MAGIC '' as prim_loc_flg ,           
# MAGIC '' as flr_stk_cd ,          
# MAGIC '' as rejected_mat_flag ,     
# MAGIC cast(mara.meins as string) as stk_uom_cd ,     
# MAGIC cast(t006a_meins.MSEHL as string) as stk_uom_nm  ,         
# MAGIC coalesce(b_unit.lkup_val_04,'NA') as prod_family,           
# MAGIC cast(mara.meins as string) as uom_code,   
# MAGIC marc.prctr  as prft_cntr ,          
# MAGIC coalesce(b_unit.lkup_val_02,'NA') as prod_fam_typ,           
# MAGIC 'NA' as src_crt_by ,     
# MAGIC 'NA' as src_crt_ts  ,            
# MAGIC cast(current_timestamp as string) as rec_crt_ts,              
# MAGIC cast(current_timestamp as string) as rec_updt_ts,             
# MAGIC t0301.konts as gl_account,       
# MAGIC (date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd')) || '|' || 'gbl' || '|' || consnmnt_tbls.matnr || '|' || consnmnt_tbls.werks || '|' || consnmnt_tbls.charg || '|' || consnmnt_tbls.lgort || '|' || consnmnt_tbls.strg_bin as invntry_bal_dly_hist_key ,
# MAGIC  t001.butxt as co_nm,   
# MAGIC 'gbl' || '|' || hfm.lkup_val_01 as hfm_entity_key,       
# MAGIC hfm.lkup_val_01 as hfm_entity_cd,    
# MAGIC 'gbl' || '|' || consnmnt_tbls.matnr as prod_key,
# MAGIC 'gbl' || '|' || consnmnt_tbls.werks as plant_key,             
# MAGIC t001w.name1 as plant_nm,
# MAGIC 'gbl' || '|' || consnmnt_tbls.matnr || '|' ||consnmnt_tbls.werks as prod_plant_key,       
# MAGIC 'gbl' || '|' || mbew.matnr || '|' || mbew.bwkey || '|' || mbew.bwtar as prod_cost_key,        
# MAGIC null as prod_cost_hist_key,     
# MAGIC 'gbl' || '|' ||(case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end) as co_key,  
# MAGIC case when (case when ltrim(rtrim(mbew.bklas)) in ('FP1') then 'FG'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('RAW1') then 'RM'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('SEM1','SEM2') then 'WIP' else 'OT' end) = 'FG' then 'Finished Good'
# MAGIC when (case when ltrim(rtrim(mbew.bklas)) in ('FP1') then 'FG'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('RAW1') then 'RM'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('SEM1','SEM2') then 'WIP' else 'OT' end) = 'RM' then 'Raw Material'
# MAGIC when (case when ltrim(rtrim(mbew.bklas)) in ('FP1') then 'FG'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('RAW1') then 'RM'
# MAGIC when ltrim(rtrim(mbew.bklas)) in ('SEM1','SEM2') then 'WIP' else 'OT' end) = 'WIP' then 'Work in Process' else 'Other' end as invntry_stk_type_nm , 
# MAGIC cast(consnmnt_tbls.consnmt_qty as decimal(38,6)) as consnmt_qty,           
# MAGIC cast(consnmnt_tbls.consnmt_qa_inspn_qty as decimal(38,6)) as consnmt_qa_inspn_qty ,   
# MAGIC cast(consnmnt_tbls.consnmt_rstrct_qty as decimal(38,6)) as consnmt_rstrct_qty ,   
# MAGIC cast(0 as decimal(38,6)) as consnmt_blocked_qty,
# MAGIC cast(0 as decimal(38,6)) as sched_delvr_qty ,    
# MAGIC cast(consnmnt_tbls.consignment_qty as decimal(38,6)) as consignment_qty, 
# MAGIC cast(d_curncy_mth_rt.curncy_mth_rt_key as string) as co_usd_curncy_mth_rt_key ,       
# MAGIC null as afflt_dt_key ,         
# MAGIC --cast(0 as decimal(38,0)) as expire_dt_key ,          
# MAGIC null as dsna_dt_key ,            
# MAGIC null as dealer_dsna_dt_key ,     
# MAGIC '' as bu_cd,   
# MAGIC '' as bu_nm,   
# MAGIC '' as invntry_load_desc ,      
# MAGIC 'gbl' || '|' || consnmnt_tbls.kunnr as ship_to_cust_key,     
# MAGIC consnmnt_tbls.kunnr as ship_to_cust_id ,       
# MAGIC kna1.name1 as ship_to_cust_nm,       
# MAGIC 'NA' as sfty_stk_fill_flg,      
# MAGIC consnmnt_tbls.sobkz as speclty_stk_type_cd,
# MAGIC (case when consnmnt_tbls.sobkz = 'W' or consnmnt_tbls.sobkz = 'V' then 'Stock with Customer' 
# MAGIC  when consnmnt_tbls.sobkz = 'O' or  consnmnt_tbls.sobkz = 'K' then 'Stock with Vendor' 
# MAGIC  when consnmnt_tbls.sobkz = 'E' then 'Sales Order Stock' end) as speclty_stk_type_nm,
# MAGIC case when mvke_cnt.assg_grp_cnt=1 then mvke.ktgrm else t001w.vtweg end as acct_asgn_grp, 
# MAGIC tvkmt.vtext as acct_asgn_grp_nm,
# MAGIC null as updt_proc_ts,
# MAGIC '' as src_updt_ts,
# MAGIC '' as sched_delvr_dt,
# MAGIC marc.prctr as prod_line_cd,  
# MAGIC cast(case when consnmnt_tbls.lgort in('1000','EXPM') then 'Y' else 'N' end as string) as demo_invntry_flg,  
# MAGIC cast(case when consnmnt_tbls.lgort in('1060','EXPS') then 'Y' else 'N' end as string) as seed_invntry_flg,  
# MAGIC case when 30=case when mvke_cnt.assg_grp_cnt=1 then mvke.ktgrm else t001w.vtweg end then 'Y' else 'N' end as svc_invntry_flg,
# MAGIC cast(mbew.peinh as double)as unit_qty 
# MAGIC FROM
# MAGIC (select mandt,matnr,werks,charg,'VEN' as lgort,
# MAGIC case when lblab is null then 0 else lblab end as lblab,
# MAGIC case when lbins is null then 0 else lbins end as lbins,
# MAGIC case when lbein is null then 0 else lbein end as lbein,
# MAGIC 0 as sspem, 'MSLB'||'_'||lifnr as strg_bin,null as vbeln, null as posnr,0 as kaspe, sobkz,null as kunnr,
# MAGIC case when lblab is null then 0 else lblab end as consignment_qty,
# MAGIC case when lblab is null then 0 else lblab end as consnmt_qty,
# MAGIC case when lbins is null then 0 else lbins end as consnmt_qa_inspn_qty ,   
# MAGIC case when lbein is null then 0 else lbein end as consnmt_rstrct_qty,
# MAGIC 0  as consnmt_blocked_qty,'MSLB' as src_nm
# MAGIC from mslb where (lblab+lbins+lbein) <>0
# MAGIC union all
# MAGIC select mandt, matnr,werks,charg,'CUS' as lgort,
# MAGIC case when kulab is null then 0 else kulab end as lblab,
# MAGIC case when kuins is null then 0 else kuins end as lbins,
# MAGIC case when kuein is null then 0 else kuein end as lbein,
# MAGIC 0 as sspem, 'CUS'||'_'||kunnr as strg_bin, null as vbeln, null as posnr,0 as kaspe, sobkz, kunnr,
# MAGIC case when kulab is null then 0 else kulab end as consignment_qty,
# MAGIC case when kulab is null then 0 else kulab end as consnmt_qty,            
# MAGIC case when kuins is null then 0 else kuins end as consnmt_qa_inspn_qty ,   
# MAGIC case when kuein is null then 0 else kuein end as consnmt_rstrct_qty,
# MAGIC 0  as consnmt_blocked_qty,'MSKU' as src_nm
# MAGIC from msku where (kulab+kuins+kuein) <> 0
# MAGIC  union all 
# MAGIC select mandt, matnr, werks, charg, lgort,
# MAGIC case when prlab is null then 0 else prlab end as lblab,
# MAGIC case when prins is null then 0 else prins end as lbins,
# MAGIC case when prein is null then 0 else prein end as lbein,
# MAGIC 0 as sspem, 'MSPR'||'_'||pspnr as strg_bin,pspnr as vbeln, null as posnr,0 as kaspe, sobkz, null as kunnr,
# MAGIC case when prlab is null then 0 else prlab end as consignment_qty,
# MAGIC case when prlab is null then 0 else prlab end as consnmt_qty,            
# MAGIC case when prins is null then 0 else prins end as consnmt_qa_inspn_qty ,   
# MAGIC case when prein is null then 0 else prein end as consnmt_rstrct_qty,
# MAGIC 0  as consnmt_blocked_qty,'MSPR' as src_nm
# MAGIC from mspr where (prlab + prins + prein) <> 0
# MAGIC union all
# MAGIC select mska.mandt as mandt, mska.matnr as matnr,mska.werks as werks, case when mska.werks= '0750' then mska.vbeln || '|' || mska.posnr else charg end as charg,lgort,
# MAGIC case when kalab is null then 0 else kalab end lblab,
# MAGIC case when kains is null then 0 else kains end as lbins,
# MAGIC case when kaein is null then 0 else kaein end as lbein,
# MAGIC 0 as sspem, 'MSKA'||'_'||mska.vbeln||'_'||mska.posnr as strg_bin, mska.vbeln as vbeln, mska.posnr as posnr, kaspe, mska.sobkz as sobkz, null as kunnr,
# MAGIC case when kalab is null then 0 else kalab end as consignment_qty,
# MAGIC case when kalab is null then 0 else kalab end as consnmt_qty,            
# MAGIC case when kains is null then 0 else kains end as consnmt_qa_inspn_qty,   
# MAGIC case when kaein is null then 0 else kaein end as consnmt_rstrct_qty,
# MAGIC 0  as consnmt_blocked_qty,'MSKA' as src_nm
# MAGIC from mska
# MAGIC 
# MAGIC inner join ebew on ebew.mandt=mska.mandt and ebew.matnr=mska.matnr and ebew.bwkey=mska.werks and ebew.vbeln=mska.vbeln and ebew.posnr=mska.posnr
# MAGIC where (mska.kalab + mska.kaspe + mska.kains + kaein) <> 0 ) consnmnt_tbls
# MAGIC left outer join mara on mara.matnr = consnmnt_tbls.matnr
# MAGIC left outer join (select * from makt where spras = 'E')makt on makt.matnr = consnmnt_tbls.matnr
# MAGIC left outer join kna1 on kna1.kunnr = consnmnt_tbls.kunnr
# MAGIC left outer join d_date d_dt on date_format(if(date_format(current_timestamp,'HH')>='0' and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd')  = cast (d_dt.dt_key as string)
# MAGIC left outer join t001l on consnmnt_tbls.werks= t001l.werks and consnmnt_tbls.lgort= t001l.lgort 
# MAGIC left outer join t001w  on t001w.werks = consnmnt_tbls.werks
# MAGIC left outer join t001 on t001.bukrs = (case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end) --t001w.vkorg
# MAGIC left outer join mbew on  ltrim(rtrim(mbew.matnr))= ltrim(rtrim(consnmnt_tbls.matnr)) and ltrim(rtrim(mbew.bwkey))=ltrim(rtrim(consnmnt_tbls.werks))
# MAGIC left outer join ebew on ebew.mandt = case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.mandt else null end and ebew.matnr=case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.matnr else null end 
# MAGIC 				and ebew.bwkey=case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.werks else null end and ebew.vbeln=case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.vbeln else null end
# MAGIC 				and ebew.posnr=case when consnmnt_tbls.src_nm='MSKA' then consnmnt_tbls.posnr end
# MAGIC left outer join qbew on qbew.mandt = case when consnmnt_tbls.src_nm='MSPR' then consnmnt_tbls.mandt else null end and qbew.matnr=case when consnmnt_tbls.src_nm='MSPR' then consnmnt_tbls.matnr else null end 
# MAGIC 				and qbew.bwkey=case when consnmnt_tbls.src_nm='MSPR' then consnmnt_tbls.werks else null end and qbew.pspnr=case when consnmnt_tbls.src_nm='MSPR' then consnmnt_tbls.vbeln else null end
# MAGIC left outer join (select distinct bklas, ktosl, konts, ktopl from t030 where ktosl='BSX') as t0301 on t0301.bklas=mbew.bklas
# MAGIC left outer join s032 on trim(consnmnt_tbls.matnr)= trim(s032.matnr) and consnmnt_tbls.werks=s032.werks and consnmnt_tbls.lgort=s032.lgort
# MAGIC left outer join marc on marc.matnr = consnmnt_tbls.matnr and marc.werks = consnmnt_tbls.werks  and marc.mandt = consnmnt_tbls.mandt
# MAGIC left outer join zproftcntr_att on marc.prctr = zproftcntr_att.zrprctr and zproftcntr_att.mandt='100'
# MAGIC left outer join (select pmar_rt, curncy_mth_rt_key,yr_mth_nbr,from_curncy_cd from d_curncy_mth_rt where to_curncy_cd =  'USD') d_curncy_mth_rt 
# MAGIC 									on d_curncy_mth_rt.yr_mth_nbr = d_dt.fscl_yr_prd_nbr and d_curncy_mth_rt.from_curncy_cd = t001.waers
# MAGIC left outer join (select distinct  mandt, matnr,vkorg, ktgrm from mvke)mvke 
# MAGIC 							on mvke.matnr = consnmnt_tbls.matnr and mvke.mandt = consnmnt_tbls.mandt and mvke.vkorg = (case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end)
# MAGIC left outer join (select mandt, matnr,vkorg,count(*) as assg_grp_cnt from mvke group by mandt, matnr,vkorg) mvke_cnt 
# MAGIC 							on mvke_cnt.matnr = consnmnt_tbls.matnr and mvke_cnt.mandt = consnmnt_tbls.mandt and mvke_cnt.vkorg = (case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end) 
# MAGIC left outer join tvkmt on tvkmt.spras='E' and tvkmt.ktgrm =  case when mvke_cnt.assg_grp_cnt=1 then mvke.ktgrm else t001w.vtweg end  
# MAGIC left outer join  edp_lkup hfm on hfm.lkup_key_01 = (case when trim(t001w.vkorg) = '' then t001w.ekorg else t001w.vkorg end) and  hfm.lkup_typ_nm ='CO_TO_HFM' and hfm.lkup_key_02 = 'GBL'
# MAGIC left outer join edp_lkup b_unit on marc.prctr= b_unit.lkup_key_01 and b_unit.lkup_typ_nm = 'CAD_PROFIT_CENTER_BU_PL_LKUP'
# MAGIC left outer join (select MSEHL,msehi  from t006a  where t006a.mandt = '100' and t006a.SPRAS = 'E')t006a_meins on mara.meins= t006a_meins.msehi
# MAGIC ) 
# MAGIC )final_union

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC -- Databricks notebook source
# MAGIC 
# MAGIC /**************************************************************************
# MAGIC Artefact Name :- inventory Balance saplsg
# MAGIC Description :- f_inventory_balance_saplsg fact sql for Lighthouse 
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC Change Log
# MAGIC Version :        Date :                                Description                                     Changed By
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC --0.0            02-aug-2022	                     First draft of sql file    			             Bhaskar Reddy
# MAGIC --0.1            04-Nov-2022	                     Replaced dims tables with
# MAGIC --                                                   source tables               			             Bhaskar Reddy
# MAGIC --0.2.           09-feb-2023                         added col prod_key                                  Raul Martinez
# MAGIC --               09-feb-2023                         added col plant_key                                 Raul Martinez
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC (select distinct
# MAGIC   'saplsg' as src_sys_cd,
# MAGIC   cast(trim(mard.matnr) as string) as item_nbr,
# MAGIC   cast('NA' as string) as item_desc,
# MAGIC   '' as item_type,
# MAGIC   '' as item_type_desc,
# MAGIC   cast(mard.werks as string) as plant_cd,
# MAGIC   cast(date_format(date_add( current_date,-1), 'yMMdd') as string) as capture_dt,
# MAGIC   cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC   cast(mchb.charg as string) as lot_nbr,
# MAGIC   cast(mard.lgort as string) as invntry_loc_cd,
# MAGIC   cast(t001l.lgobe as string) as invntry_loc_name,
# MAGIC   'NA' as strg_bin_cd,
# MAGIC   if (trim(t001w.vkorg) = '',t001w.ekorg,t001w.vkorg) as co_cd,
# MAGIC   cast(t001.butxt as string) as co_name,
# MAGIC   cast((case when t001.waers='RMB' then 'CNY' else t001.waers end) as string) as co_curncy_cd,
# MAGIC   (case
# MAGIC   when mara.mtart='RAW' then 'Raw Material'
# MAGIC   when mara.mtart='FP' then 'Finished Goods'
# MAGIC   else 'Raw Material' 
# MAGIC   end) as invntry_stk_type_cd,
# MAGIC   cast(mbew.bklas as string) as valuation_cd,
# MAGIC   --cast(s032.letztzug as string) as recpt_dt,
# MAGIC   cast(null as string) as recpt_dt,
# MAGIC   cast(round(if(t001l.diskz='1',0,if(mchb.charg is null,mard.labst,mchb.clabs)),2) as double) as avail_qty,
# MAGIC   cast(round(if(mchb.charg is null,coalesce(mard.umlme+mard.insme+mard.einme+mard.speme+mard.labst,0), coalesce(mchb.cumlm + mchb.cinsm + mchb.ceinm + mchb.cspem + mchb.clabs,0)),2) as double) as on_hand_qty,
# MAGIC   cast(round(if(mchb.charg is null,coalesce(mard.umlme+mard.insme+mard.einme+mard.speme+mard.labst,0), coalesce(mchb.cumlm + mchb.cinsm + mchb.ceinm + mchb.cspem + mchb.clabs,0))- cast(if(t001l.diskz='1',0,if(mchb.charg is null,mard.labst,mchb.clabs)) as double),2) as double)as on_hold_qty,
# MAGIC   cast(if(mchb.charg is null,coalesce(mard.umlme,0),coalesce(mchb.cumlm,0)) as double)as transfer_qty,
# MAGIC   cast(if(mchb.charg is null , coalesce(mard.insme,0),coalesce(mchb.cinsm,0)) as double)as qa_inspn_qty,
# MAGIC   cast(if(mchb.charg is null , coalesce(mard.speme,0),coalesce(mchb.cspem,0)) as double)as blocked_qty,
# MAGIC   cast(if(mchb.charg is null ,coalesce(mard.einme,0),coalesce(mchb.ceinm,0)) as double)as rstrct_qty,
# MAGIC   cast(round(cast( IF(mbew_a.VPRSV = 'V',(IF(IF(tvko.WAERS in ('KRW','JPY'),'Y','N')='Y',mbew_a.VERPR*100,mbew_a.VERPR))/(IF(mbew_a.PEINH IS NULL OR mbew_a.PEINH = 0,1,mbew_a.PEINH)),IF(IF(TVKO.WAERS in ('KRW','JPY'),'Y','N')='Y',mbew_a.STPRS*100,mbew_a.STPRS)/(IF(mbew_a.PEINH IS NULL OR mbew_a.PEINH = 0,1,mbew_a.PEINH)))  as double),2) as double)as unit_cost_co_amt,
# MAGIC   cast(round(cast( IF(mbew_a.VPRSV = 'V',(IF(IF(tvko.WAERS in ('KRW','JPY'),'Y','N')='Y',mbew_a.VERPR*100,mbew_a.VERPR))/(IF(mbew_a.PEINH IS NULL OR mbew_a.PEINH = 0,1,mbew_a.PEINH)),IF(IF(TVKO.WAERS in ('KRW','JPY'),'Y','N')='Y',mbew_a.STPRS*100,mbew_a.STPRS)/(IF(mbew_a.PEINH IS NULL OR mbew_a.PEINH = 0,1,mbew_a.PEINH)))  as double),2) as double)as unit_cost_lcur_amt,
# MAGIC   cast(round(cast( IF(mbew_a.VPRSV = 'V',(IF(IF(tvko.WAERS in ('KRW','JPY'),'Y','N')='Y',mbew_a.VERPR*100,mbew_a.VERPR))/(IF(mbew_a.PEINH IS NULL OR mbew_a.PEINH = 0,1,mbew_a.PEINH)),IF(IF(TVKO.WAERS in ('KRW','JPY'),'Y','N')='Y',mbew_a.STPRS*100,mbew_a.STPRS)/(IF(mbew_a.PEINH IS NULL OR mbew_a.PEINH = 0,1,mbew_a.PEINH)))* co_curr_mth.CO_PMAR_RT as double) ,2) as double)as unit_cost_co_pmar_amt,
# MAGIC   cast(round(cast(co_curr_mth.CO_PMAR_RT as double),4) as double)as co_curncy_mth_pmar_rt,
# MAGIC   coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity ,
# MAGIC   coalesce(case when b_unit.LKUP_VAL_05='M&M' then 'PPA' else b_unit.LKUP_VAL_05 end,'NA') as business_unit ,
# MAGIC   --coalesce(zproftcntr_att.zdivision,'NA') as div_cd,
# MAGIC   'NA' as div_cd,
# MAGIC   'NA' as lot_stat_cd,
# MAGIC   'NA' as lot_stat_nm,
# MAGIC   'NA' as prim_loc_flg,
# MAGIC   'NA' as flr_stk_cd,
# MAGIC   (case
# MAGIC   when mard.werks = '0080' and mard.lgort in ('EFM1','SARA','SB05','SB27','SB38','SB55','SB70','SEN1','INCM') then 'Y'
# MAGIC   when mard.werks = '2001' and mard.lgort in ('RM10','RM11') Then 'Y'
# MAGIC   else 'N'
# MAGIC   end) as rejected_mat_flag,
# MAGIC   cast(mara.meins as string) as stk_uom_cd,
# MAGIC   cast(t006a_meins.MSEHL as string) as stk_uom_nm,
# MAGIC   coalesce(b_unit.lkup_val_04,'NA') as prod_family,
# MAGIC   coalesce(b_unit.lkup_val_02,'NA') as prod_fam_typ,
# MAGIC   'NA' as src_crt_by,
# MAGIC   'NA' as src_crt_ts,
# MAGIC   cast(current_timestamp as string) as rec_crt_ts,
# MAGIC   cast(current_timestamp as string) as rec_updt_ts,
# MAGIC   cast(mard.klabs as decimal(38,6)) as Consignment_qty,
# MAGIC   'saplsg' || '|' || mard.matnr as prod_key,
# MAGIC   'saplsg' || '|' || mard.werks as plant_key
# MAGIC from (select * from mard  where (mard.labst+mard.umlme+mard.insme+mard.einme+mard.speme+mard.klabs)!=0) mard
# MAGIC left outer join (select * from mchb where  (mchb.cinsm + mchb.ceinm + mchb.cspem + mchb.cumlm + mchb.clabs) != 0) mchb 
# MAGIC   on trim(mard.matnr)= trim(mchb.matnr) 
# MAGIC     and mard.werks=mchb.werks 
# MAGIC     and mard.lgort=mchb.lgort
# MAGIC left outer join mbew 
# MAGIC   on trim(mard.matnr) = trim(mbew.matnr)
# MAGIC left outer join  t001w t001w 
# MAGIC   on mard.werks = t001w.werks
# MAGIC 
# MAGIC left outer join marc 
# MAGIC   on  trim(mard.matnr)= trim(marc.matnr) 
# MAGIC     and trim(marc.werks) = trim(mard.werks) 
# MAGIC     and trim(mard.mandt) = trim(marc.mandt)
# MAGIC 
# MAGIC -- left outer join s032 on trim(mard.matnr)= trim(s032.matnr) and mard.werks=s032.werks and mard.lgort=s032.lgort
# MAGIC left outer join t001l 
# MAGIC   on mard.werks= t001l.werks 
# MAGIC     and mard.lgort= t001l.lgort 
# MAGIC 
# MAGIC left outer join t001 
# MAGIC   on (case when trim(t001w.vkorg) = '' then t001w.ekorg  else t001w.vkorg end) = t001.bukrs
# MAGIC left outer join mara 
# MAGIC   on trim(mara.matnr)= trim(mard.matnr) 
# MAGIC     and mara.mtart<> 'NVAL'
# MAGIC --left outer join zproftcntr_att on d_prod_plnt.profit_center_cd = zproftcntr_att.zrprctr and zproftcntr_att.mandt='100'
# MAGIC 
# MAGIC left outer join edp_lkup b_unit 
# MAGIC   on marc.prctr = b_unit.lkup_key_01 
# MAGIC     and LKUP_TYP_NM = 'CAD_PROFIT_CENTER_BU_PL_LKUP'
# MAGIC 
# MAGIC left outer join (select MSEHL,msehi  from t006a  where t006a.mandt = '100' and t006a.SPRAS = 'E')t006a_meins 
# MAGIC   on mara.meins= t006a_meins.msehi
# MAGIC 
# MAGIC left outer join  edp_lkup hfm 
# MAGIC   on hfm.lkup_key_01 = cast(t001.bukrs as string) 
# MAGIC     and  hfm.LKUP_TYP_NM ='CO_TO_HFM' 
# MAGIC     and hfm.lkup_key_02 = 'SAPLSG'
# MAGIC 
# MAGIC 
# MAGIC left outer join d_date d_dt 
# MAGIC   on date_format(if(date_format(current_timestamp,'HH')>='0' 
# MAGIC     and date_format(current_timestamp,'HH')<='12',date_sub(current_date,1),current_date),'yMMdd') = cast (d_dt.dt_key as string)
# MAGIC 
# MAGIC left outer join (
# MAGIC     select curr_mnth.PMAR_RT as CO_PMAR_RT, curr_mnth.CURNCY_MTH_RT_KEY,curr_mnth.YR_MTH_NBR,curr_mnth.FROM_CURNCY_CD
# MAGIC     from d_curncy_mth_rt curr_mnth 
# MAGIC     where TO_CURNCY_CD =  'USD'
# MAGIC   ) co_curr_mth 
# MAGIC   on co_curr_mth.YR_MTH_NBR = d_dt.fscl_yr_prd_nbr and co_curr_mth.FROM_CURNCY_CD = t001.waers
# MAGIC 
# MAGIC left outer join  tvko 
# MAGIC   on tvko.mandt = mard.mandt 
# MAGIC     and tvko.vkorg = mard.werks
# MAGIC 
# MAGIC left outer join (select * from mbew where cast(((cast (mbew.lfgja as decimal(11,0)) * 100) + (cast (mbew.lfmon as decimal(11,0)))) as decimal(11,0) ) <=(select fscl_yr_prd_nbr from d_date where cast(dt_key as string)=(select if(date_format(current_timestamp,'H')>='0' and date_format(current_timestamp,'H')<='12',cast(date_format(date_sub(current_date,1),'yMMdd') as string) ,cast(date_format(current_date,'yMMdd') as string)))) and cast( 999912 as decimal(12,0))>=(select fscl_yr_prd_nbr from d_date where cast(dt_key as string)=(select if(date_format(current_timestamp,'H')>='0' and date_format(current_timestamp,'H')<='12',cast(date_format(date_sub(current_date,1),'yMMdd') as string),cast(date_format(current_date,'yMMdd') as string) )))) mbew_a 
# MAGIC   on trim(mbew_a.bwkey) = trim(mard.werks) and trim(mbew_a.matnr) = trim(mard.matnr) 
# MAGIC )
# MAGIC ;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


