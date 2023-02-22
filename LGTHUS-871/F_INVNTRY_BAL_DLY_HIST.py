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
# MAGIC 0.0            18-08-2022	                       First draft of sql file    			                            Vijay Kelkar
# MAGIC 1.1            14-09-2022                          UOM Issue : all the qty divide by 10,000                         Vijay Kelkar  
# MAGIC 1.2            19-10-2022						   Replace dims table with raw table                                Neha Chaturvedi	
# MAGIC 1.3            08-11-2022						   Replace on_hand_qty logic                                        Neha Chaturvedi	
# MAGIC 1.4            09-11-2022						   Replace avail_qty logic                                          Neha Chaturvedi
# MAGIC 1.5            27-01-2022                          Remove Where Condition	                                        Neha Chaturvedi
# MAGIC 1.6            09-02-2022                          Replaced avail_qty logic                                         Raul Martinez
# MAGIC                                                    Replaced on_hand_qty logic                                       Raul Martinez
# MAGIC                                                    Replaced qa_inspn_qty logic                                      Raul Martinez
# MAGIC                                                    Replaced blocked_qty logic                                       Raul Martinez
# MAGIC                                                    Replaced item_nbr logic                                          Raul Martinez
# MAGIC                                                    Replaced plant_cd logic                                          Raul Martinez
# MAGIC                                                    Replaced lot_nbr logic                                           Raul Martinez
# MAGIC                                                    Replaced invntry_loc_name logic                                  Raul Martinez
# MAGIC                                                    Added consignment_qty logic                                      Raul Martinez
# MAGIC                                                    Added UOM to consignment_qty_agg CTE                             Vijay Kelkar
# MAGIC                                                    Added prod_key                                                   Raul Martinez
# MAGIC                                                    Added plant_key                                                  Raul Martinez
# MAGIC                                                    Replaced capture_yr_mth_nbr logic                                Raul Martinez
# MAGIC                                                    Replaced co_cd logic                                             Raul Martinez
# MAGIC                                                    Replaced co_name logic                                           Raul Martinez
# MAGIC                                                    Replaced co_curncy_cd logic                                      Raul Martinez
# MAGIC                                                    Replaced recpt_dt logic                                          Raul Martinez
# MAGIC                                                    Replaced hfm_entity logic                                        Raul Martinez
# MAGIC                                                    Replaced business_unit logic                                     Raul Martinez
# MAGIC                                                    Replaced div_cd logic                                            Raul Martinez
# MAGIC                                                    Replaced lot_stat_cd logic                                       Raul Martinez
# MAGIC                                                    Replaced lot_stat_nm logic                                       Raul Martinez
# MAGIC                                                    Replaced prim_loc_flg logic                                      Raul Martinez
# MAGIC                                                    Replaced stk_uom_cd logic                                        Raul Martinez
# MAGIC                                                    Replaced prod_family logic                                       Raul Martinez
# MAGIC                                                    Replaced prod_fam_typ logic                                      Raul Martinez
# MAGIC                                                    Split code into 3 CTEs                                           Raul Martinez
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC with consignment_qty_agg as (
# MAGIC   select 
# MAGIC     match_UOM.item_nbr as item_nbr,
# MAGIC     match_UOM.plant_cd as plant_cd,
# MAGIC     match_UOM.lot_nbr as lot_nbr,
# MAGIC     match_UOM.invntry_loc_name as invntry_loc_name,
# MAGIC     sum((case when match_UOM.base_uom <> match_UOM.txn_uom then unmatch_uom.final_qty else match_UOM.original_qty end )) as consignment_qty,
# MAGIC     match_UOM.base_uom,
# MAGIC     match_UOM.IMSRP3,
# MAGIC     match_UOM.IMLOTS,
# MAGIC     match_UOM.IMSRP1,
# MAGIC     match_UOM.IMSRP2,
# MAGIC     match_UOM.PRLITM,
# MAGIC     match_UOM.PRMCU
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
# MAGIC       f43092.pxqtyo/10000 as original_qty,
# MAGIC       cast(f4101.IMSRP3 as string) as IMSRP3,
# MAGIC       cast(f4101.IMLOTS as string) as IMLOTS,
# MAGIC       cast(f4101.IMSRP1 as string) as IMSRP1,
# MAGIC       cast(f4101.IMSRP2 as string) as IMSRP2,
# MAGIC       cast(f43121.PRLITM as string) as PRLITM,
# MAGIC       cast(f43121.PRLITM as string) as PRMCU
# MAGIC     from f43092   
# MAGIC     left outer join f43121 
# MAGIC       on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC         and f43092.pxdcto = f43121.prdcto
# MAGIC         and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC         and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC         and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC     LEFT OUTER JOIN f4101_adt f4101 
# MAGIC       on trim(f4101.IMLITM)= trim(f43121.PRLITM) 
# MAGIC     where f43092.pxnrou = 'CONS' 
# MAGIC       and f43092.pxoprc = 'CONS' 
# MAGIC       and f43092.pxupib = 'QTO1'
# MAGIC       and f43092.pxqtyo/10000 >0
# MAGIC       and trim(f43121.PRDCT) ='OV'
# MAGIC       and f43121.prmatc in('1','2')
# MAGIC     ) match_UOM
# MAGIC   left outer join (
# MAGIC     select distinct  
# MAGIC       item_nbr,
# MAGIC       item_id,
# MAGIC       plant_cd,
# MAGIC       recpt_nbr,
# MAGIC       lot_nbr,
# MAGIC       po_nbr,
# MAGIC       invntry_loc_name,
# MAGIC       po_line_nbr,
# MAGIC       BASE_UOM,
# MAGIC       txn_uom,
# MAGIC       original_qty,
# MAGIC       --(case when trim(f41003.ucrum) is not null then (original_qty/f41003.UCCONV)/100000000 else nonstd_convertion_inv_qty end) as final_qty,
# MAGIC       case when f41003_from_uom IS NULL AND f41002_from_uom is NULL AND f41002_from_uom_inv is null then original_qty/(f41003.UCCONV/10000000) 
# MAGIC         else nonstd_convertion_inv_qty 
# MAGIC         end as final_qty,
# MAGIC       f41003_from_uom,
# MAGIC       f41003_to_uom,
# MAGIC       f41002_from_uom,
# MAGIC       f41002_to_uom,
# MAGIC       f41002_from_uom_inv,
# MAGIC       f41002_to_uom_inv,
# MAGIC       trim(f41003.ucum) as f41003_from_uom_inv,
# MAGIC       trim(f41003.ucrum) as f41003_to_uom_inv
# MAGIC     from (
# MAGIC       SELECT distinct   -- Part 3
# MAGIC         item_nbr,
# MAGIC         item_id,
# MAGIC         plant_cd,
# MAGIC         lot_nbr,
# MAGIC         invntry_loc_name,
# MAGIC         recpt_nbr,
# MAGIC         po_nbr,
# MAGIC         po_line_nbr,
# MAGIC         BASE_UOM,
# MAGIC         txn_uom,
# MAGIC         f41003_from_uom,
# MAGIC         f41003_to_uom,
# MAGIC         f41002_from_uom,
# MAGIC         f41002_to_uom,
# MAGIC         original_qty,
# MAGIC         --(case when trim(f41002.UMRUM) is not null then (original_qty/f41002.UMCONV)/10000000 else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC         (case when f41003_from_uom IS NULL AND f41002_from_uom is null then original_qty/(f41002.UMCONV/10000000) else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC         trim(f41002.UMRUM) AS f41002_from_uom_inv,
# MAGIC         trim(f41002.UMUM) AS f41002_to_uom_inv
# MAGIC       from (
# MAGIC         SELECT distinct   -- part 2
# MAGIC           item_nbr,
# MAGIC           item_id,
# MAGIC           plant_cd,
# MAGIC           recpt_nbr,
# MAGIC           lot_nbr,
# MAGIC           invntry_loc_name,
# MAGIC           po_nbr,
# MAGIC           po_line_nbr,
# MAGIC           BASE_UOM,
# MAGIC           txn_uom,
# MAGIC           f41003_from_uom,
# MAGIC           f41003_to_uom,
# MAGIC           original_qty,
# MAGIC           (case when f41003_from_uom is null  then (original_qty*f41002.UMCONV)/10000000 else std_convertion_qty end) as nonstd_convertion_qty,
# MAGIC           trim(f41002.UMUM) AS f41002_from_uom,
# MAGIC           trim(f41002.UMRUM) AS f41002_to_uom 
# MAGIC           from 
# MAGIC           (select distinct  -- part 1 
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
# MAGIC           original_qty,
# MAGIC           (case when trim(f41003.ucum) is not null then (original_qty*f41003.UCCONV)/10000000 else 0 end ) as  std_convertion_qty,
# MAGIC           trim(f41003.ucum) as f41003_from_uom,
# MAGIC           trim(f41003.ucrum) as f41003_to_uom
# MAGIC           from (
# MAGIC             select 
# MAGIC               item_nbr,
# MAGIC               plant_cd,
# MAGIC               item_id,
# MAGIC               lot_nbr,
# MAGIC               recpt_nbr,
# MAGIC               po_nbr,
# MAGIC               po_line_nbr,
# MAGIC               invntry_loc_name,
# MAGIC               f4101.IMUOM1 as base_uom,
# MAGIC               txn_uom,
# MAGIC               original_qty
# MAGIC             from ( 
# MAGIC               select 
# MAGIC                 cast(trim(f43121.PRLITM) as string) as item_nbr,
# MAGIC                 cast(trim(f43092.pxmcu) as string) as plant_cd,
# MAGIC                 cast(trim(f43121.pritm) as string) as item_id,
# MAGIC                 cast(f43121.prlotn as string) as lot_nbr,
# MAGIC                 cast(cast(f43121.PRDOC as integer) as string) as recpt_nbr,
# MAGIC                 cast(cast(f43121.PRDOCO as integer) as string) as po_nbr ,
# MAGIC                 cast(cast(f43121.PRLNID as integer) as string) as po_line_nbr,
# MAGIC                 cast(cast(f43121.PRNLIN as integer) as string) as recpt_line_seq_nbr,
# MAGIC                 cast(f43121.prlocn  as string) as invntry_loc_name,
# MAGIC                 cast(f43121.PRUOM as string) as txn_uom,
# MAGIC                 f43092.pxqtyo/10000 as original_qty 
# MAGIC               from f43092   
# MAGIC               left outer join f43121 
# MAGIC                 on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC                   and f43092.pxdcto = f43121.prdcto
# MAGIC                   and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC                   and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC                   and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC               where f43092.pxnrou = 'CONS' 
# MAGIC                 and f43092.pxoprc = 'CONS' 
# MAGIC                 and f43092.pxupib = 'QTO1'
# MAGIC                 and f43092.pxqtyo/10000 >0
# MAGIC                 and trim(f43121.PRDCT) ='OV'
# MAGIC                 and f43121.prmatc in('1','2')
# MAGIC             ) f43092
# MAGIC             left outer join f4101_adt f4101
# MAGIC               on trim(f4101.IMLITM)=trim(f43092.item_nbr)
# MAGIC             where f4101.IMUOM1 <> f43092.txn_uom
# MAGIC               AND trim(f4101.IMLITM) IS NOT NULL
# MAGIC               AND trim(f43092.plant_cd) in ('GB01','US01','US02','US03','US05','US15','US23','US24') 
# MAGIC           ) transuom_not_equal_baseuom   -- Base
# MAGIC           left outer join  f41003                               ---- Part 1
# MAGIC           on trim(f41003.ucum) = transuom_not_equal_baseuom.txn_uom
# MAGIC             and trim(f41003.ucrum) = transuom_not_equal_baseuom.BASE_UOM
# MAGIC         ) request_for_nonstd_convertion
# MAGIC         LEFT OUTER JOIN f41002                               --- Part 2 
# MAGIC           on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC             and txn_uom = trim(f41002.UMUM)
# MAGIC             and BASE_UOM = trim(f41002.UMRUM)
# MAGIC         --where trim(f41002.UMUM) IS  null 
# MAGIC       ) req_for_nonstd_conv_inv
# MAGIC       LEFT OUTER JOIN f41002                               -- Part 3
# MAGIC         on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC           and txn_uom = trim(f41002.UMRUM)
# MAGIC           and BASE_UOM = trim(f41002.UMUM)
# MAGIC       --where trim(f41002.UMRUM) IS null
# MAGIC     ) req_for_std_conv_inv
# MAGIC     left outer join f41003  -- part 4 
# MAGIC       on trim(f41003.ucrum) = req_for_std_conv_inv.txn_uom
# MAGIC         and trim(f41003.ucum) = req_for_std_conv_inv.BASE_UOM
# MAGIC     --WHERE trim(f41003.ucrum) IS NULL
# MAGIC   ) unmatch_uom
# MAGIC     on match_UOM.item_nbr = unmatch_uom.item_nbr
# MAGIC   group by match_UOM.item_nbr,match_UOM.plant_cd,match_UOM.lot_nbr,match_UOM.invntry_loc_name,match_UOM.base_uom,match_UOM.IMSRP3,match_UOM.IMLOTS,match_UOM.IMSRP1,match_UOM.IMSRP2,match_UOM.PRLITM,match_UOM.PRMCU
# MAGIC ),
# MAGIC 
# MAGIC consignment_qty_records as (
# MAGIC   select 
# MAGIC     cqa.*,
# MAGIC     cast(f0006.mcco as string) as co_cd,
# MAGIC     cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC     cast(f0010.ccname as string) as co_name,
# MAGIC     case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end as co_curncy_cd,
# MAGIC --     cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string) as recpt_dt,
# MAGIC     'NA' as recpt_dt,
# MAGIC     cast((CASE WHEN trim(cqa.IMLOTS) in ('#','C','D','G','M','P','W','','null') THEN (cqa.IMLOTS/10000) ELSE 0 end) as double) as avail_qty,
# MAGIC --     cast((CASE WHEN trim(cqa.IMLOTS) in ('Q') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as qa_inspn_qty,
# MAGIC     'NA' as qa_inspn_qty,
# MAGIC --     cast((CASE WHEN trim(cqa.IMLOTS) in ('E','F','H','R','S','V','X') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as blocked_qty,
# MAGIC     'NA' as blocked_qty,
# MAGIC     coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity,
# MAGIC     case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI'
# MAGIC       when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' 
# MAGIC       else trim(f0005.DRDL01) 
# MAGIC       end as business_unit,
# MAGIC     cast(trim(F0005_div.DRDL01) as string) as div_cd,
# MAGIC     cast(trim(cqa.IMLOTS) as string) as lot_stat_cd,
# MAGIC     cast(F0005_LOT.DRDL01 as string) as lot_stat_nm,
# MAGIC --     cast(f41021.LIPBIN as string) as prim_loc_flg,
# MAGIC     'NA' as prim_loc_flg,
# MAGIC     cast(F0005_UOM.DRDL01 as string) as stk_uom_cd,
# MAGIC     cast(cqa.IMSRP3 as string) as prod_family,
# MAGIC     cast(F0005_PROD.DRDL01 as string) as prod_fam_typ,
# MAGIC     'e1lsg' || '|' || trim(cqa.PRLITM) as prod_key,
# MAGIC     'e1lsg' || '|' || trim(cqa.PRMCU) as plant_key
# MAGIC 
# MAGIC   from consignment_qty_agg cqa
# MAGIC   left outer join d_date d_dt 
# MAGIC     on date_format(if(date_format(current_timestamp,'%h')>='0' 
# MAGIC       and date_format(current_timestamp,'%h')<='12',date_sub(current_date,1),current_date),'yMMdd') = cast (d_dt.dt_key as string)
# MAGIC   left outer join f0006 
# MAGIC     on trim(f0006.mcmcu) = cqa.plant_cd
# MAGIC   left outer join f0010 
# MAGIC     on f0010.ccco = f0006.mcco
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
# MAGIC --   left outer join f41021 
# MAGIC --     on trim(f0006.mcmcu) = trim(f41021.limcu)
# MAGIC --   left outer join f4102_adt f4102 
# MAGIC --     on trim(cast(cast(f4102.ibitm as integer) as string))= trim(cast(cast(F41021.LIITM as integer) as string)) 
# MAGIC --       and trim(F41021.LIMCU) = trim(f4102.ibmcu)
# MAGIC   left outer join edp_lkup hfm 
# MAGIC     on hfm.lkup_key_01 = f0006.MCCO 
# MAGIC       and hfm.LKUP_TYP_NM ='CO_TO_HFM' 
# MAGIC       and hfm.lkup_key_02 = 'E1LSG'
# MAGIC   left outer join f0005 f0005 
# MAGIC     on trim(f0005.DRSY)= '41' 
# MAGIC       and trim(f0005.DRRT)='S2' 
# MAGIC       and trim(f0005.DRKY)=trim(cqa.IMSRP2)
# MAGIC   left outer join f0005 F0005_div 
# MAGIC     on trim(F0005_div.DRSY)= '41' 
# MAGIC       and trim(F0005_div.DRRT)='S1' 
# MAGIC       and trim(F0005_div.DRKY)=trim(cqa.IMSRP1)
# MAGIC   left outer join f0005 F0005_LOT 
# MAGIC     on trim(F0005_LOT.DRSY)= '41' 
# MAGIC       and trim(F0005_LOT.DRRT)='L' 
# MAGIC       and trim(F0005_LOT.DRKY)=trim(cqa.IMLOTS)
# MAGIC   left outer join f0005 F0005_UOM 
# MAGIC     on trim(F0005_UOM.DRSY)= '00' 
# MAGIC       and trim(F0005_UOM.DRRT)='UM' 
# MAGIC       and trim(F0005_UOM.DRKY)=trim(cqa.base_uom)
# MAGIC   left outer join f0005 F0005_PROD 
# MAGIC     on trim(F0005_PROD.DRSY)= '41' 
# MAGIC       and trim(F0005_PROD.DRRT)='S3'  
# MAGIC       and trim(F0005_PROD.DRKY)=trim(cqa.IMSRP3)
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
# MAGIC --       cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC       case when cqr.capture_yr_mth_nbr is null then (cast(d_dt.fscl_yr_prd_nbr as decimal(38,0))) else cqr.capture_yr_mth_nbr end as capture_yr_mth_nbr,
# MAGIC       'NA' as invntry_loc_cd,
# MAGIC       'NA' as strg_bin_cd,
# MAGIC --       cast(f0006.mcco as string) as co_cd,
# MAGIC       case when cqr.co_cd is null then f0006.mcco else cqr.co_cd end as co_cd,
# MAGIC --       cast(f0010.CCNAME as string) as co_name,
# MAGIC       case when cqr.co_name is null then cast(f0010.CCNAME as string) else cqr.co_name end as co_name,
# MAGIC --       case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end as co_curncy_cd,
# MAGIC       case when cqr.co_curncy_cd is null then (case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end) else cqr.co_curncy_cd end as co_curncy_cd,
# MAGIC       'Finished Goods' as invntry_stk_type_cd,
# MAGIC       'NA'  as valuation_cd,
# MAGIC --       cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string) as  recpt_dt,
# MAGIC       case when cqr.recpt_dt is null then (cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string)) else cqr.recpt_dt end as recpt_dt,
# MAGIC       -- ((f41021.LIPQOH/10000) - (f41021.LIHCOM/10000) - (f41021.LIPCOM/10000) - (f41021.LIFCOM/10000) - (f41021.LIFUN1/10000) - (f41021.LIQOWO/10000)) as avail_qty,
# MAGIC       -- cast(0 as double) as avail_qty,
# MAGIC --       cast((CASE WHEN trim(f41021.LILOTS) in ('#','C','D','G','M','P','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as avail_qty,
# MAGIC       case when cqr.avail_qty is null 
# MAGIC         then (cast((CASE WHEN trim(f41021.LILOTS) in ('#','C','D','G','M','P','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double))
# MAGIC         else cqr.avail_qty
# MAGIC         end as avail_qty,
# MAGIC       -- cast((CASE WHEN trim(f41021.LILOTS) in ('C','D','G','M','P','Q','q','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as on_hand_qty,
# MAGIC       -- cast((CASE WHEN trim(f41021.LILOTS) in ('C','D','G','M','P','Q','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as on_hand_qty,
# MAGIC       'NA' as on_hand_qty,
# MAGIC       --(case when (TRIM(LIPBIN) = 'P' OR TRIM(LIPBIN) is NULL ) then 0 else (CASE WHEN TRIM(LILOTS) IS NULL THEN 0 ELSE (LIPQOH / 10000) end) end) as on_hold_qty,
# MAGIC       cast(0 as double) as on_hold_qty, 
# MAGIC       cast(0 as double) as transfer_qty,
# MAGIC       -- cast(0 as double) as qa_inspn_qty,
# MAGIC --       cast((CASE WHEN trim(f41021.LILOTS) in ('Q') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as qa_inspn_qty,
# MAGIC       case when cqr.qa_inspn_qty is null then (cast((CASE WHEN trim(f41021.LILOTS) in ('Q') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double)) else cqr.qa_inspn_qty end as qa_inspn_qty, 
# MAGIC       -- cast(0 as double) as blocked_qty,
# MAGIC --       cast((CASE WHEN trim(f41021.LILOTS) in ('E','F','H','R','S','V','X') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as blocked_qty,
# MAGIC       case when cqr.blocked_qty is null 
# MAGIC         then cast((CASE WHEN trim(f41021.LILOTS) in ('E','F','H','R','S','V','X') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) 
# MAGIC         else cqr.blocked_qty 
# MAGIC         end as blocked_qty, 
# MAGIC       cast(0 as double) as rstrct_qty,
# MAGIC       cast(null as double)  as unit_cost_co_amt,
# MAGIC       cast(null as double) as unit_cost_lcur_amt,
# MAGIC       cast(null as double) as unit_cost_co_pmar_amt,
# MAGIC       coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity,
# MAGIC --       case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' else trim(f0005.DRDL01) end as business_unit,
# MAGIC       case when cqr.business_unit is null 
# MAGIC         then (case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' else trim(f0005.DRDL01) end) 
# MAGIC         else cqr.business_unit 
# MAGIC         end as business_unit,
# MAGIC --       cast(trim(F0005_div.DRDL01) as string) as div_cd,
# MAGIC       case when cqr.div_cd is null then cast(trim(F0005_div.DRDL01) as string) else cqr.div_cd end as div_cd,
# MAGIC --       cast(f41021.LILOTS as string) as lot_stat_cd,
# MAGIC       case when cqr.lot_stat_cd is null then cast(f41021.LILOTS as string) else cqr.lot_stat_cd end as lot_stat_cd,
# MAGIC --       cast(F0005_LOT.DRDL01 as string) as lot_stat_nm,
# MAGIC       case when cqr.lot_stat_nm is null then cast(F0005_LOT.DRDL01 as string) else cqr.lot_stat_nm end as lot_stat_nm,
# MAGIC --       cast(f41021.LIPBIN as string) as prim_loc_flg,
# MAGIC       case when cqr.prim_loc_flg is null then (cast(f41021.LIPBIN as string)) else cqr.prim_loc_flg end as prim_loc_flg,
# MAGIC       'NA' as flr_stk_cd,
# MAGIC       'NA' as rejected_mat_flag,
# MAGIC --       cast(F0005_UOM.DRDL01 as string) as stk_uom_cd,
# MAGIC       case when cqr.stk_uom_cd is null then (cast(F0005_UOM.DRDL01 as string)) else cqr.stk_uom_cd end as stk_uom_cd,
# MAGIC       'NA' as  stk_uom_nm,
# MAGIC --       cast(f4102.IBSRP3 as string) as prod_family,
# MAGIC       case when cqr.prod_family is null then (cast(f4102.IBSRP3 as string)) else cqr.prod_family end as prod_family,
# MAGIC --       cast(F0005_PROD.DRDL01 as string) as prod_fam_typ,
# MAGIC       case when cqr.prod_fam_typ is null then (cast(F0005_PROD.DRDL01 as string)) else cqr.prod_fam_typ end as prod_fam_typ,
# MAGIC       'NA' as gl_account,
# MAGIC       'NA' as src_crt_by,
# MAGIC       'NA' as src_crt_ts,
# MAGIC       cast(current_timestamp as string) as rec_crt_ts,
# MAGIC       cast(current_timestamp as string) as rec_updt_ts,
# MAGIC --       'e1lsg' || '|' || trim(f4102.iblitm) as prod_key,
# MAGIC       case when cqr.prod_key is null then ('e1lsg' || '|' || trim(f4102.iblitm)) else cqr.prod_key end as prod_key,
# MAGIC --       'e1lsg' || '|' || trim(F41021.LIMCU) as plant_key,
# MAGIC       case when cqr.plant_key is null then ('e1lsg' || '|' || trim(F41021.LIMCU)) else cqr.plant_key end as plant_key
# MAGIC   
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
# MAGIC -- where item_nbr = '50122M03H25' and plant_cd='US02' -- test case 1: no match between tables, just append vals
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
# MAGIC 0.0            04/13/2022                      First draft of sql file                               Abhishek Ranjan
# MAGIC 1.0            06/16/2022                      Repleced source table itemconsumption_sales           Bhaskar Reddy
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
# MAGIC   'nav_ger' || '|' || trim(itmqtyOH.Item_No_) as prod_key,
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
# MAGIC   'saplsg' || '|' || trim(mard.matnr) as prod_key,
# MAGIC   'saplsg' || '|' || trim(mard.werks) as plant_key
# MAGIC   
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

# MAGIC %sql
# MAGIC 
# MAGIC /**************************************************************************
# MAGIC Artefact Name :- F_INVNTRY_BAL_DLY_HIST (SNAPSHOT)
# MAGIC Description :-  This table will hold the information of the on-hand_qty for E1LSG system
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC Change Log
# MAGIC Version :        Date :                                Description                                     Changed By
# MAGIC -------------------------------------------------------------------------------------------------------------------------------
# MAGIC 0.0            18-08-2022	                       First draft of sql file    			                            Vijay Kelkar
# MAGIC 1.1            14-09-2022                          UOM Issue : all the qty divide by 10,000                         Vijay Kelkar  
# MAGIC 1.2            19-10-2022						   Replace dims table with raw table                                Neha Chaturvedi	
# MAGIC 1.3            08-11-2022						   Replace on_hand_qty logic                                        Neha Chaturvedi	
# MAGIC 1.4            09-11-2022						   Replace avail_qty logic                                          Neha Chaturvedi
# MAGIC 1.5            27-01-2022                          Remove Where Condition	                                        Neha Chaturvedi
# MAGIC 1.6            09-02-2022                          Replaced avail_qty logic                                         Raul Martinez
# MAGIC                                                    Replaced on_hand_qty logic                                       Raul Martinez
# MAGIC                                                    Replaced qa_inspn_qty logic                                      Raul Martinez
# MAGIC                                                    Replaced blocked_qty logic                                       Raul Martinez
# MAGIC                                                    Replaced item_nbr logic                                          Raul Martinez
# MAGIC                                                    Replaced plant_cd logic                                          Raul Martinez
# MAGIC                                                    Replaced lot_nbr logic                                           Raul Martinez
# MAGIC                                                    Replaced invntry_loc_name logic                                  Raul Martinez
# MAGIC                                                    Added consignment_qty logic                                      Raul Martinez
# MAGIC                                                    Added UOM to consignment_qty_agg CTE                             Vijay Kelkar
# MAGIC                                                    Added prod_key                                                   Raul Martinez
# MAGIC                                                    Added plant_key                                                  Raul Martinez
# MAGIC                                                    Replaced capture_yr_mth_nbr logic                                Raul Martinez
# MAGIC                                                    Replaced co_cd logic                                             Raul Martinez
# MAGIC                                                    Replaced co_name logic                                           Raul Martinez
# MAGIC                                                    Replaced co_curncy_cd logic                                      Raul Martinez
# MAGIC                                                    Replaced recpt_dt logic                                          Raul Martinez
# MAGIC                                                    Replaced hfm_entity logic                                        Raul Martinez
# MAGIC                                                    Replaced business_unit logic                                     Raul Martinez
# MAGIC                                                    Replaced div_cd logic                                            Raul Martinez
# MAGIC                                                    Replaced lot_stat_cd logic                                       Raul Martinez
# MAGIC                                                    Replaced lot_stat_nm logic                                       Raul Martinez
# MAGIC                                                    Replaced prim_loc_flg logic                                      Raul Martinez
# MAGIC                                                    Replaced stk_uom_cd logic                                        Raul Martinez
# MAGIC                                                    Replaced prod_family logic                                       Raul Martinez
# MAGIC                                                    Replaced prod_fam_typ logic                                      Raul Martinez
# MAGIC                                                    Split code into 3 CTEs                                           Raul Martinez
# MAGIC **************************************************************************/
# MAGIC 
# MAGIC with consignment_qty_agg as (
# MAGIC   select 
# MAGIC     match_UOM.item_nbr as item_nbr,
# MAGIC     match_UOM.plant_cd as plant_cd,
# MAGIC     match_UOM.lot_nbr as lot_nbr,
# MAGIC     match_UOM.invntry_loc_name as invntry_loc_name,
# MAGIC     sum((case when match_UOM.base_uom <> match_UOM.txn_uom then unmatch_uom.final_qty else match_UOM.original_qty end )) as consignment_qty,
# MAGIC     match_UOM.base_uom,
# MAGIC     match_UOM.IMSRP3,
# MAGIC     match_UOM.IMLOTS,
# MAGIC     match_UOM.IMSRP1,
# MAGIC     match_UOM.IMSRP2,
# MAGIC     match_UOM.PRLITM,
# MAGIC     match_UOM.PRMCU
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
# MAGIC       f43092.pxqtyo/10000 as original_qty,
# MAGIC       cast(f4101.IMSRP3 as string) as IMSRP3,
# MAGIC       cast(f4101.IMLOTS as string) as IMLOTS,
# MAGIC       cast(f4101.IMSRP1 as string) as IMSRP1,
# MAGIC       cast(f4101.IMSRP2 as string) as IMSRP2,
# MAGIC       cast(f43121.PRLITM as string) as PRLITM,
# MAGIC       cast(f43121.PRLITM as string) as PRMCU
# MAGIC     from f43092   
# MAGIC     left outer join f43121 
# MAGIC       on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC         and f43092.pxdcto = f43121.prdcto
# MAGIC         and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC         and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC         and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC     LEFT OUTER JOIN f4101_adt f4101 
# MAGIC       on trim(f4101.IMLITM)= trim(f43121.PRLITM) 
# MAGIC     where f43092.pxnrou = 'CONS' 
# MAGIC       and f43092.pxoprc = 'CONS' 
# MAGIC       and f43092.pxupib = 'QTO1'
# MAGIC       and f43092.pxqtyo/10000 >0
# MAGIC       and trim(f43121.PRDCT) ='OV'
# MAGIC       and f43121.prmatc in('1','2')
# MAGIC     ) match_UOM
# MAGIC   left outer join (
# MAGIC     select distinct  
# MAGIC       item_nbr,
# MAGIC       item_id,
# MAGIC       plant_cd,
# MAGIC       recpt_nbr,
# MAGIC       lot_nbr,
# MAGIC       po_nbr,
# MAGIC       invntry_loc_name,
# MAGIC       po_line_nbr,
# MAGIC       BASE_UOM,
# MAGIC       txn_uom,
# MAGIC       original_qty,
# MAGIC       --(case when trim(f41003.ucrum) is not null then (original_qty/f41003.UCCONV)/100000000 else nonstd_convertion_inv_qty end) as final_qty,
# MAGIC       case when f41003_from_uom IS NULL AND f41002_from_uom is NULL AND f41002_from_uom_inv is null then original_qty/(f41003.UCCONV/10000000) 
# MAGIC         else nonstd_convertion_inv_qty 
# MAGIC         end as final_qty,
# MAGIC       f41003_from_uom,
# MAGIC       f41003_to_uom,
# MAGIC       f41002_from_uom,
# MAGIC       f41002_to_uom,
# MAGIC       f41002_from_uom_inv,
# MAGIC       f41002_to_uom_inv,
# MAGIC       trim(f41003.ucum) as f41003_from_uom_inv,
# MAGIC       trim(f41003.ucrum) as f41003_to_uom_inv
# MAGIC     from (
# MAGIC       SELECT distinct   -- Part 3
# MAGIC         item_nbr,
# MAGIC         item_id,
# MAGIC         plant_cd,
# MAGIC         lot_nbr,
# MAGIC         invntry_loc_name,
# MAGIC         recpt_nbr,
# MAGIC         po_nbr,
# MAGIC         po_line_nbr,
# MAGIC         BASE_UOM,
# MAGIC         txn_uom,
# MAGIC         f41003_from_uom,
# MAGIC         f41003_to_uom,
# MAGIC         f41002_from_uom,
# MAGIC         f41002_to_uom,
# MAGIC         original_qty,
# MAGIC         --(case when trim(f41002.UMRUM) is not null then (original_qty/f41002.UMCONV)/10000000 else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC         (case when f41003_from_uom IS NULL AND f41002_from_uom is null then original_qty/(f41002.UMCONV/10000000) else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC         trim(f41002.UMRUM) AS f41002_from_uom_inv,
# MAGIC         trim(f41002.UMUM) AS f41002_to_uom_inv
# MAGIC       from (
# MAGIC         SELECT distinct   -- part 2
# MAGIC           item_nbr,
# MAGIC           item_id,
# MAGIC           plant_cd,
# MAGIC           recpt_nbr,
# MAGIC           lot_nbr,
# MAGIC           invntry_loc_name,
# MAGIC           po_nbr,
# MAGIC           po_line_nbr,
# MAGIC           BASE_UOM,
# MAGIC           txn_uom,
# MAGIC           f41003_from_uom,
# MAGIC           f41003_to_uom,
# MAGIC           original_qty,
# MAGIC           (case when f41003_from_uom is null  then (original_qty*f41002.UMCONV)/10000000 else std_convertion_qty end) as nonstd_convertion_qty,
# MAGIC           trim(f41002.UMUM) AS f41002_from_uom,
# MAGIC           trim(f41002.UMRUM) AS f41002_to_uom 
# MAGIC           from 
# MAGIC           (select distinct  -- part 1 
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
# MAGIC           original_qty,
# MAGIC           (case when trim(f41003.ucum) is not null then (original_qty*f41003.UCCONV)/10000000 else 0 end ) as  std_convertion_qty,
# MAGIC           trim(f41003.ucum) as f41003_from_uom,
# MAGIC           trim(f41003.ucrum) as f41003_to_uom
# MAGIC           from (
# MAGIC             select 
# MAGIC               item_nbr,
# MAGIC               plant_cd,
# MAGIC               item_id,
# MAGIC               lot_nbr,
# MAGIC               recpt_nbr,
# MAGIC               po_nbr,
# MAGIC               po_line_nbr,
# MAGIC               invntry_loc_name,
# MAGIC               f4101.IMUOM1 as base_uom,
# MAGIC               txn_uom,
# MAGIC               original_qty
# MAGIC             from ( 
# MAGIC               select 
# MAGIC                 cast(trim(f43121.PRLITM) as string) as item_nbr,
# MAGIC                 cast(trim(f43092.pxmcu) as string) as plant_cd,
# MAGIC                 cast(trim(f43121.pritm) as string) as item_id,
# MAGIC                 cast(f43121.prlotn as string) as lot_nbr,
# MAGIC                 cast(cast(f43121.PRDOC as integer) as string) as recpt_nbr,
# MAGIC                 cast(cast(f43121.PRDOCO as integer) as string) as po_nbr ,
# MAGIC                 cast(cast(f43121.PRLNID as integer) as string) as po_line_nbr,
# MAGIC                 cast(cast(f43121.PRNLIN as integer) as string) as recpt_line_seq_nbr,
# MAGIC                 cast(f43121.prlocn  as string) as invntry_loc_name,
# MAGIC                 cast(f43121.PRUOM as string) as txn_uom,
# MAGIC                 f43092.pxqtyo/10000 as original_qty 
# MAGIC               from f43092   
# MAGIC               left outer join f43121 
# MAGIC                 on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC                   and f43092.pxdcto = f43121.prdcto
# MAGIC                   and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC                   and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC                   and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC               where f43092.pxnrou = 'CONS' 
# MAGIC                 and f43092.pxoprc = 'CONS' 
# MAGIC                 and f43092.pxupib = 'QTO1'
# MAGIC                 and f43092.pxqtyo/10000 >0
# MAGIC                 and trim(f43121.PRDCT) ='OV'
# MAGIC                 and f43121.prmatc in('1','2')
# MAGIC             ) f43092
# MAGIC             left outer join f4101_adt f4101
# MAGIC               on trim(f4101.IMLITM)=trim(f43092.item_nbr)
# MAGIC             where f4101.IMUOM1 <> f43092.txn_uom
# MAGIC               AND trim(f4101.IMLITM) IS NOT NULL
# MAGIC               AND trim(f43092.plant_cd) in ('GB01','US01','US02','US03','US05','US15','US23','US24') 
# MAGIC           ) transuom_not_equal_baseuom   -- Base
# MAGIC           left outer join  f41003                               ---- Part 1
# MAGIC           on trim(f41003.ucum) = transuom_not_equal_baseuom.txn_uom
# MAGIC             and trim(f41003.ucrum) = transuom_not_equal_baseuom.BASE_UOM
# MAGIC         ) request_for_nonstd_convertion
# MAGIC         LEFT OUTER JOIN f41002                               --- Part 2 
# MAGIC           on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC             and txn_uom = trim(f41002.UMUM)
# MAGIC             and BASE_UOM = trim(f41002.UMRUM)
# MAGIC         --where trim(f41002.UMUM) IS  null 
# MAGIC       ) req_for_nonstd_conv_inv
# MAGIC       LEFT OUTER JOIN f41002                               -- Part 3
# MAGIC         on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC           and txn_uom = trim(f41002.UMRUM)
# MAGIC           and BASE_UOM = trim(f41002.UMUM)
# MAGIC       --where trim(f41002.UMRUM) IS null
# MAGIC     ) req_for_std_conv_inv
# MAGIC     left outer join f41003  -- part 4 
# MAGIC       on trim(f41003.ucrum) = req_for_std_conv_inv.txn_uom
# MAGIC         and trim(f41003.ucum) = req_for_std_conv_inv.BASE_UOM
# MAGIC     --WHERE trim(f41003.ucrum) IS NULL
# MAGIC   ) unmatch_uom
# MAGIC     on match_UOM.item_nbr = unmatch_uom.item_nbr
# MAGIC   group by match_UOM.item_nbr,match_UOM.plant_cd,match_UOM.lot_nbr,match_UOM.invntry_loc_name,match_UOM.base_uom,match_UOM.IMSRP3,match_UOM.IMLOTS,match_UOM.IMSRP1,match_UOM.IMSRP2,match_UOM.PRLITM,match_UOM.PRMCU
# MAGIC ),
# MAGIC 
# MAGIC consignment_qty_records as (
# MAGIC   select 
# MAGIC     cqa.*,
# MAGIC     cast(f0006.mcco as string) as co_cd,
# MAGIC     cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC     cast(f0010.ccname as string) as co_name,
# MAGIC     case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end as co_curncy_cd,
# MAGIC     'NA' as recpt_dt,
# MAGIC     cast((CASE WHEN trim(cqa.IMLOTS) in ('#','C','D','G','M','P','W','','null') THEN (cqa.IMLOTS/10000) ELSE 0 end) as double) as avail_qty,
# MAGIC 	cast((CASE WHEN trim(cqa.LILOTS) in ('C','D','G','M','P','Q','q','W','','null') THEN (cqa.LILOTS/10000) ELSE 0 end) as double) as on_hand_qty,
# MAGIC     'NA' as qa_inspn_qty,
# MAGIC     'NA' as blocked_qty,
# MAGIC     coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity,
# MAGIC     case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI'
# MAGIC       when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' 
# MAGIC       else trim(f0005.DRDL01) 
# MAGIC       end as business_unit,
# MAGIC     cast(trim(F0005_div.DRDL01) as string) as div_cd,
# MAGIC     cast(trim(cqa.IMLOTS) as string) as lot_stat_cd,
# MAGIC     cast(F0005_LOT.DRDL01 as string) as lot_stat_nm,
# MAGIC     'NA' as prim_loc_flg,
# MAGIC     cast(F0005_UOM.DRDL01 as string) as stk_uom_cd,
# MAGIC     cast(cqa.IMSRP3 as string) as prod_family,
# MAGIC     cast(F0005_PROD.DRDL01 as string) as prod_fam_typ,
# MAGIC     'e1lsg' || '|' || trim(cqa.PRLITM) as prod_key,
# MAGIC     'e1lsg' || '|' || trim(cqa.PRMCU) as plant_key
# MAGIC 
# MAGIC   from consignment_qty_agg cqa
# MAGIC   left outer join d_date d_dt 
# MAGIC     on date_format(if(date_format(current_timestamp,'%h')>='0' 
# MAGIC       and date_format(current_timestamp,'%h')<='12',date_sub(current_date,1),current_date),'yMMdd') = cast (d_dt.dt_key as string)
# MAGIC   left outer join f0006 
# MAGIC     on trim(f0006.mcmcu) = cqa.plant_cd
# MAGIC   left outer join f0010 
# MAGIC     on f0010.ccco = f0006.mcco
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
# MAGIC   left outer join edp_lkup hfm 
# MAGIC     on hfm.lkup_key_01 = f0006.MCCO 
# MAGIC       and hfm.LKUP_TYP_NM ='CO_TO_HFM' 
# MAGIC       and hfm.lkup_key_02 = 'E1LSG'
# MAGIC   left outer join f0005 f0005 
# MAGIC     on trim(f0005.DRSY)= '41' 
# MAGIC       and trim(f0005.DRRT)='S2' 
# MAGIC       and trim(f0005.DRKY)=trim(cqa.IMSRP2)
# MAGIC   left outer join f0005 F0005_div 
# MAGIC     on trim(F0005_div.DRSY)= '41' 
# MAGIC       and trim(F0005_div.DRRT)='S1' 
# MAGIC       and trim(F0005_div.DRKY)=trim(cqa.IMSRP1)
# MAGIC   left outer join f0005 F0005_LOT 
# MAGIC     on trim(F0005_LOT.DRSY)= '41' 
# MAGIC       and trim(F0005_LOT.DRRT)='L' 
# MAGIC       and trim(F0005_LOT.DRKY)=trim(cqa.IMLOTS)
# MAGIC   left outer join f0005 F0005_UOM 
# MAGIC     on trim(F0005_UOM.DRSY)= '00' 
# MAGIC       and trim(F0005_UOM.DRRT)='UM' 
# MAGIC       and trim(F0005_UOM.DRKY)=trim(cqa.base_uom)
# MAGIC   left outer join f0005 F0005_PROD 
# MAGIC     on trim(F0005_PROD.DRSY)= '41' 
# MAGIC       and trim(F0005_PROD.DRRT)='S3'  
# MAGIC       and trim(F0005_PROD.DRKY)=trim(cqa.IMSRP3)
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
# MAGIC       cast(case when cqr.consignment_qty is null then 0 else cqr.consignment_qty end as decimal(38,6) ) as consignment_qty,
# MAGIC       '' as item_type,
# MAGIC       '' as item_type_desc,
# MAGIC       cast(date_format(date_sub(current_timestamp,1),'yMMdd') as string) as capture_dt,
# MAGIC --       cast(d_dt.fscl_yr_prd_nbr as decimal(38,0)) as capture_yr_mth_nbr,
# MAGIC       case when cqr.capture_yr_mth_nbr is null then (cast(d_dt.fscl_yr_prd_nbr as decimal(38,0))) else cqr.capture_yr_mth_nbr end as capture_yr_mth_nbr,
# MAGIC       'NA' as invntry_loc_cd,
# MAGIC       'NA' as strg_bin_cd,
# MAGIC --       cast(f0006.mcco as string) as co_cd,
# MAGIC       case when cqr.co_cd is null then f0006.mcco else cqr.co_cd end as co_cd,
# MAGIC --       cast(f0010.CCNAME as string) as co_name,
# MAGIC       case when cqr.co_name is null then cast(f0010.CCNAME as string) else cqr.co_name end as co_name,
# MAGIC --       case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end as co_curncy_cd,
# MAGIC       case when cqr.co_curncy_cd is null then (case when f0010.CCCRCD='RMB' then 'CNY' else f0010.CCCRCD end) else cqr.co_curncy_cd end as co_curncy_cd,
# MAGIC       'Finished Goods' as invntry_stk_type_cd,
# MAGIC       'NA'  as valuation_cd,
# MAGIC --       cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string) as  recpt_dt,
# MAGIC       case when cqr.recpt_dt is null then (cast(date_format(TO_DATE(cast(f41021.LILRCJ_dt as string),'yyyyMMdd'),'yMMdd') as string)) else cqr.recpt_dt end as recpt_dt,
# MAGIC       -- ((f41021.LIPQOH/10000) - (f41021.LIHCOM/10000) - (f41021.LIPCOM/10000) - (f41021.LIFCOM/10000) - (f41021.LIFUN1/10000) - (f41021.LIQOWO/10000)) as avail_qty,
# MAGIC       -- cast(0 as double) as avail_qty,
# MAGIC --       cast((CASE WHEN trim(f41021.LILOTS) in ('#','C','D','G','M','P','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as avail_qty,
# MAGIC       case when cqr.avail_qty is null 
# MAGIC         then (cast((CASE WHEN trim(f41021.LILOTS) in ('#','C','D','G','M','P','W','','null') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double))
# MAGIC         else cqr.avail_qty
# MAGIC         end as avail_qty,
# MAGIC 	  case when cqr.on_hand_qty is null 
# MAGIC         then (cast((CASE WHEN trim(f41021.LILOTS) in ('C','D','G','M','P','Q','q','W','','null') THEN (f41021.LILOTS/10000) ELSE 0 end) as double)) 
# MAGIC         else cqr.on_hand_qty
# MAGIC 		end as on_hand_qty,
# MAGIC       --cast(0 as double) as on_hand_qty,
# MAGIC       --(case when (TRIM(LIPBIN) = 'P' OR TRIM(LIPBIN) is NULL ) then 0 else (CASE WHEN TRIM(LILOTS) IS NULL THEN 0 ELSE (LIPQOH / 10000) end) end) as on_hold_qty,
# MAGIC       cast(0 as double) as on_hold_qty, 
# MAGIC       cast(0 as double) as transfer_qty,
# MAGIC       -- cast(0 as double) as qa_inspn_qty,
# MAGIC --       cast((CASE WHEN trim(f41021.LILOTS) in ('Q') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as qa_inspn_qty,
# MAGIC       cast(case when cqr.qa_inspn_qty is null then (cast((CASE WHEN trim(f41021.LILOTS) in ('Q') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double)) else cqr.qa_inspn_qty end  as double) as qa_inspn_qty, 
# MAGIC       -- cast(0 as double) as blocked_qty,
# MAGIC --       cast((CASE WHEN trim(f41021.LILOTS) in ('E','F','H','R','S','V','X') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) as blocked_qty,
# MAGIC       cast(case when cqr.blocked_qty is null 
# MAGIC         then cast((CASE WHEN trim(f41021.LILOTS) in ('E','F','H','R','S','V','X') THEN (f41021.LIPQOH/10000) ELSE 0 end) as double) 
# MAGIC         else cqr.blocked_qty 
# MAGIC         end as double) as blocked_qty, 
# MAGIC       cast(0 as double) as rstrct_qty,
# MAGIC       cast(null as double)  as unit_cost_co_amt,
# MAGIC       cast(null as double) as unit_cost_lcur_amt,
# MAGIC       cast(null as double) as unit_cost_co_pmar_amt,
# MAGIC       coalesce(hfm.LKUP_VAL_01,'NA') as hfm_entity,
# MAGIC --       case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' else trim(f0005.DRDL01) end as business_unit,
# MAGIC       case when cqr.business_unit is null 
# MAGIC         then (case when trim(f0005.DRDL01)='RMPEnvironmental' then 'FSI' when trim(f0005.DRDL01) in ('BulkEquipmentSvcs','MATERIALS & MINERALS','BulkEquipment','PackagingWI') then 'PPA' else trim(f0005.DRDL01) end) 
# MAGIC         else cqr.business_unit 
# MAGIC         end as business_unit,
# MAGIC --       cast(trim(F0005_div.DRDL01) as string) as div_cd,
# MAGIC       case when cqr.div_cd is null then cast(trim(F0005_div.DRDL01) as string) else cqr.div_cd end as div_cd,
# MAGIC --       cast(f41021.LILOTS as string) as lot_stat_cd,
# MAGIC       case when cqr.lot_stat_cd is null then cast(f41021.LILOTS as string) else cqr.lot_stat_cd end as lot_stat_cd,
# MAGIC --       cast(F0005_LOT.DRDL01 as string) as lot_stat_nm,
# MAGIC       case when cqr.lot_stat_nm is null then cast(F0005_LOT.DRDL01 as string) else cqr.lot_stat_nm end as lot_stat_nm,
# MAGIC --       cast(f41021.LIPBIN as string) as prim_loc_flg,
# MAGIC       case when cqr.prim_loc_flg is null then (cast(f41021.LIPBIN as string)) else cqr.prim_loc_flg end as prim_loc_flg,
# MAGIC       'NA' as flr_stk_cd,
# MAGIC       'NA' as rejected_mat_flag,
# MAGIC       case when cqr.stk_uom_cd is null then (cast(F0005_UOM.DRDL01 as string)) else cqr.stk_uom_cd end as stk_uom_cd,
# MAGIC       'NA' as  stk_uom_nm,
# MAGIC       case when cqr.prod_family is null then (cast(f4102.IBSRP3 as string)) else cqr.prod_family end as prod_family,
# MAGIC       case when cqr.prod_fam_typ is null then (cast(F0005_PROD.DRDL01 as string)) else cqr.prod_fam_typ end as prod_fam_typ,
# MAGIC       'NA' as gl_account,
# MAGIC       'NA' as src_crt_by,
# MAGIC       'NA' as src_crt_ts,
# MAGIC       cast(current_timestamp as string) as rec_crt_ts,
# MAGIC       cast(current_timestamp as string) as rec_updt_ts,
# MAGIC       case when cqr.prod_key is null then ('e1lsg' || '|' || trim(f4102.iblitm)) else cqr.prod_key end as prod_key,
# MAGIC       case when cqr.plant_key is null then ('e1lsg' || '|' || trim(F41021.LIMCU)) else cqr.plant_key end as plant_key
# MAGIC   
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
# MAGIC where item_nbr is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC with consignment_qty_agg as (
# MAGIC   select 
# MAGIC     match_UOM.item_nbr as item_nbr,
# MAGIC     match_UOM.plant_cd as plant_cd,
# MAGIC     match_UOM.lot_nbr as lot_nbr,
# MAGIC     match_UOM.invntry_loc_name as invntry_loc_name,
# MAGIC     sum((case when match_UOM.base_uom <> match_UOM.txn_uom then unmatch_uom.final_qty else match_UOM.original_qty end )) as consignment_qty,
# MAGIC     match_UOM.base_uom,
# MAGIC     match_UOM.IMSRP3,
# MAGIC     match_UOM.IMLOTS,
# MAGIC     match_UOM.IMSRP1,
# MAGIC     match_UOM.IMSRP2,
# MAGIC     match_UOM.PRLITM,
# MAGIC     match_UOM.PRMCU
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
# MAGIC       f43092.pxqtyo/10000 as original_qty,
# MAGIC       cast(f4101.IMSRP3 as string) as IMSRP3,
# MAGIC       cast(f4101.IMLOTS as string) as IMLOTS,
# MAGIC       cast(f4101.IMSRP1 as string) as IMSRP1,
# MAGIC       cast(f4101.IMSRP2 as string) as IMSRP2,
# MAGIC       cast(f43121.PRLITM as string) as PRLITM,
# MAGIC       cast(f43121.PRLITM as string) as PRMCU
# MAGIC     from f43092   
# MAGIC     left outer join f43121 
# MAGIC       on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC         and f43092.pxdcto = f43121.prdcto
# MAGIC         and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC         and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC         and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC     LEFT OUTER JOIN f4101_adt f4101 
# MAGIC       on trim(f4101.IMLITM)= trim(f43121.PRLITM) 
# MAGIC     where f43092.pxnrou = 'CONS' 
# MAGIC       and f43092.pxoprc = 'CONS' 
# MAGIC       and f43092.pxupib = 'QTO1'
# MAGIC       and f43092.pxqtyo/10000 >0
# MAGIC       and trim(f43121.PRDCT) ='OV'
# MAGIC       and f43121.prmatc in('1','2')
# MAGIC     ) match_UOM
# MAGIC   left outer join (
# MAGIC     select distinct  
# MAGIC       item_nbr,
# MAGIC       item_id,
# MAGIC       plant_cd,
# MAGIC       recpt_nbr,
# MAGIC       lot_nbr,
# MAGIC       po_nbr,
# MAGIC       invntry_loc_name,
# MAGIC       po_line_nbr,
# MAGIC       BASE_UOM,
# MAGIC       txn_uom,
# MAGIC       original_qty,
# MAGIC       --(case when trim(f41003.ucrum) is not null then (original_qty/f41003.UCCONV)/100000000 else nonstd_convertion_inv_qty end) as final_qty,
# MAGIC       case when f41003_from_uom IS NULL AND f41002_from_uom is NULL AND f41002_from_uom_inv is null then original_qty/(f41003.UCCONV/10000000) 
# MAGIC         else nonstd_convertion_inv_qty 
# MAGIC         end as final_qty,
# MAGIC       f41003_from_uom,
# MAGIC       f41003_to_uom,
# MAGIC       f41002_from_uom,
# MAGIC       f41002_to_uom,
# MAGIC       f41002_from_uom_inv,
# MAGIC       f41002_to_uom_inv,
# MAGIC       trim(f41003.ucum) as f41003_from_uom_inv,
# MAGIC       trim(f41003.ucrum) as f41003_to_uom_inv
# MAGIC     from (
# MAGIC       SELECT distinct   -- Part 3
# MAGIC         item_nbr,
# MAGIC         item_id,
# MAGIC         plant_cd,
# MAGIC         lot_nbr,
# MAGIC         invntry_loc_name,
# MAGIC         recpt_nbr,
# MAGIC         po_nbr,
# MAGIC         po_line_nbr,
# MAGIC         BASE_UOM,
# MAGIC         txn_uom,
# MAGIC         f41003_from_uom,
# MAGIC         f41003_to_uom,
# MAGIC         f41002_from_uom,
# MAGIC         f41002_to_uom,
# MAGIC         original_qty,
# MAGIC         --(case when trim(f41002.UMRUM) is not null then (original_qty/f41002.UMCONV)/10000000 else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC         (case when f41003_from_uom IS NULL AND f41002_from_uom is null then original_qty/(f41002.UMCONV/10000000) else nonstd_convertion_qty end ) as nonstd_convertion_inv_qty,
# MAGIC         trim(f41002.UMRUM) AS f41002_from_uom_inv,
# MAGIC         trim(f41002.UMUM) AS f41002_to_uom_inv
# MAGIC       from (
# MAGIC         SELECT distinct   -- part 2
# MAGIC           item_nbr,
# MAGIC           item_id,
# MAGIC           plant_cd,
# MAGIC           recpt_nbr,
# MAGIC           lot_nbr,
# MAGIC           invntry_loc_name,
# MAGIC           po_nbr,
# MAGIC           po_line_nbr,
# MAGIC           BASE_UOM,
# MAGIC           txn_uom,
# MAGIC           f41003_from_uom,
# MAGIC           f41003_to_uom,
# MAGIC           original_qty,
# MAGIC           (case when f41003_from_uom is null  then (original_qty*f41002.UMCONV)/10000000 else std_convertion_qty end) as nonstd_convertion_qty,
# MAGIC           trim(f41002.UMUM) AS f41002_from_uom,
# MAGIC           trim(f41002.UMRUM) AS f41002_to_uom 
# MAGIC           from 
# MAGIC           (select distinct  -- part 1 
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
# MAGIC           original_qty,
# MAGIC           (case when trim(f41003.ucum) is not null then (original_qty*f41003.UCCONV)/10000000 else 0 end ) as  std_convertion_qty,
# MAGIC           trim(f41003.ucum) as f41003_from_uom,
# MAGIC           trim(f41003.ucrum) as f41003_to_uom
# MAGIC           from (
# MAGIC             select 
# MAGIC               item_nbr,
# MAGIC               plant_cd,
# MAGIC               item_id,
# MAGIC               lot_nbr,
# MAGIC               recpt_nbr,
# MAGIC               po_nbr,
# MAGIC               po_line_nbr,
# MAGIC               invntry_loc_name,
# MAGIC               f4101.IMUOM1 as base_uom,
# MAGIC               txn_uom,
# MAGIC               original_qty
# MAGIC             from ( 
# MAGIC               select 
# MAGIC                 cast(trim(f43121.PRLITM) as string) as item_nbr,
# MAGIC                 cast(trim(f43092.pxmcu) as string) as plant_cd,
# MAGIC                 cast(trim(f43121.pritm) as string) as item_id,
# MAGIC                 cast(f43121.prlotn as string) as lot_nbr,
# MAGIC                 cast(cast(f43121.PRDOC as integer) as string) as recpt_nbr,
# MAGIC                 cast(cast(f43121.PRDOCO as integer) as string) as po_nbr ,
# MAGIC                 cast(cast(f43121.PRLNID as integer) as string) as po_line_nbr,
# MAGIC                 cast(cast(f43121.PRNLIN as integer) as string) as recpt_line_seq_nbr,
# MAGIC                 cast(f43121.prlocn  as string) as invntry_loc_name,
# MAGIC                 cast(f43121.PRUOM as string) as txn_uom,
# MAGIC                 f43092.pxqtyo/10000 as original_qty 
# MAGIC               from f43092   
# MAGIC               left outer join f43121 
# MAGIC                 on trim(cast(cast(f43092.pxdoco as integer) as string))= trim(cast(cast(f43121.PRDOCO as integer) as string))
# MAGIC                   and f43092.pxdcto = f43121.prdcto
# MAGIC                   and trim(cast(cast(f43092.pxlnid as integer) as string))= trim(cast(cast(f43121.prlnid as integer) as string))
# MAGIC                   and trim(cast(cast(f43092.pxnlin as integer) as string))= trim(cast(cast(f43121.prnlin as integer) as string)) 
# MAGIC                   and trim(f43092.pxmcu)=trim(f43121.prmcu)
# MAGIC               where f43092.pxnrou = 'CONS' 
# MAGIC                 and f43092.pxoprc = 'CONS' 
# MAGIC                 and f43092.pxupib = 'QTO1'
# MAGIC                 and f43092.pxqtyo/10000 >0
# MAGIC                 and trim(f43121.PRDCT) ='OV'
# MAGIC                 and f43121.prmatc in('1','2')
# MAGIC             ) f43092
# MAGIC             left outer join f4101_adt f4101
# MAGIC               on trim(f4101.IMLITM)=trim(f43092.item_nbr)
# MAGIC             where f4101.IMUOM1 <> f43092.txn_uom
# MAGIC               AND trim(f4101.IMLITM) IS NOT NULL
# MAGIC               AND trim(f43092.plant_cd) in ('GB01','US01','US02','US03','US05','US15','US23','US24') 
# MAGIC           ) transuom_not_equal_baseuom   -- Base
# MAGIC           left outer join  f41003                               ---- Part 1
# MAGIC           on trim(f41003.ucum) = transuom_not_equal_baseuom.txn_uom
# MAGIC             and trim(f41003.ucrum) = transuom_not_equal_baseuom.BASE_UOM
# MAGIC         ) request_for_nonstd_convertion
# MAGIC         LEFT OUTER JOIN f41002                               --- Part 2 
# MAGIC           on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC             and txn_uom = trim(f41002.UMUM)
# MAGIC             and BASE_UOM = trim(f41002.UMRUM)
# MAGIC         --where trim(f41002.UMUM) IS  null 
# MAGIC       ) req_for_nonstd_conv_inv
# MAGIC       LEFT OUTER JOIN f41002                               -- Part 3
# MAGIC         on item_id = trim(f41002.umitm) and plant_cd = trim(f41002.ummcu)
# MAGIC           and txn_uom = trim(f41002.UMRUM)
# MAGIC           and BASE_UOM = trim(f41002.UMUM)
# MAGIC       --where trim(f41002.UMRUM) IS null
# MAGIC     ) req_for_std_conv_inv
# MAGIC     left outer join f41003  -- part 4 
# MAGIC       on trim(f41003.ucrum) = req_for_std_conv_inv.txn_uom
# MAGIC         and trim(f41003.ucum) = req_for_std_conv_inv.BASE_UOM
# MAGIC     --WHERE trim(f41003.ucrum) IS NULL
# MAGIC   ) unmatch_uom
# MAGIC     on match_UOM.item_nbr = unmatch_uom.item_nbr
# MAGIC   group by match_UOM.item_nbr,match_UOM.plant_cd,match_UOM.lot_nbr,match_UOM.invntry_loc_name,match_UOM.base_uom,match_UOM.IMSRP3,match_UOM.IMLOTS,match_UOM.IMSRP1,match_UOM.IMSRP2,match_UOM.PRLITM,match_UOM.PRMCU
# MAGIC )
