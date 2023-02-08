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

d_product_plant = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/d_product_plant/")
d_product_plant.createOrReplaceTempView("d_product_plant")

d_product = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/d_product/")
d_product.createOrReplaceTempView("d_product")

d_tamr_prod_classification = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/d_tamr_prod_classification/")
d_tamr_prod_classification.createOrReplaceTempView("d_tamr_prod_classification")

d_product_supplier_xref = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/d_product_supplier_xref/")
d_product_supplier_xref.createOrReplaceTempView("d_product_supplier_xref")

ekko = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/ekko/") 
ekko.createOrReplaceTempView("ekko") 

t001 = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/t001/") 
t001.createOrReplaceTempView("t001")

eket = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/eket/") 
eket.createOrReplaceTempView("eket")

ekpo = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/ekpo/") 
ekpo.createOrReplaceTempView("ekpo")

marc = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/marc/") 
marc.createOrReplaceTempView("marc")

mbew = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/mbew/") 
mbew.createOrReplaceTempView("mbew")

mseg = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/mseg/") 
mseg.createOrReplaceTempView("mseg") 

mkpf = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/mkpf/") 
mkpf.createOrReplaceTempView("mkpf") 

d_curncy_mth_rt = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/d_curncy_mth_rt/")
d_curncy_mth_rt.createOrReplaceTempView("d_curncy_mth_rt")

edp_lkup = spark.read.format('parquet').load("s3://tfsdl-edp-common-dims-prod/processed/edp_lkup/")
edp_lkup.createOrReplaceTempView("edp_lkup")

mct_consumer_source_mapping = spark.read.format('parquet').load("s3://tfsdl-corp-pn-prod/master_account_table/consumer_source_mapping/")
mct_consumer_source_mapping.createOrReplaceTempView("mct_consumer_source_mapping")

mct_supplier_corporate_recognition_data = spark.read.format('parquet').load("s3://tfsdl-corp-pn-prod/master_account_table/supplier_corporate_recognition_data/")
mct_supplier_corporate_recognition_data.createOrReplaceTempView("mct_supplier_corporate_recognition_data")


# COMMAND ----------

lfa1 = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/lfa1/") 
lfa1.createOrReplaceTempView("lfa1") 

mara = spark.read.format('delta').load("s3://tfsdl-corp-pr1-prod/processed/delta/pr1/mara/") 
mara.createOrReplaceTempView("mara") 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- /**************************************************************************
# MAGIC -- Artefact Name :- sku site control tower for Lighthouse 
# MAGIC -- Description :-  PR1 sku_site_cntrl_twr
# MAGIC -- -------------------------------------------------------------------------------------------------------------------------------
# MAGIC -- Change Log
# MAGIC -- Version :        Date :                                Description                                     Changed By
# MAGIC -- -------------------------------------------------------------------------------------------------------------------------------
# MAGIC -- 1.0                25-04-2022                           f_cntrl_twe_sku_site_cd for 
# MAGIC --														    PR1/GBL                                           Vijay Kelkar
# MAGIC -- 1.1                 06-06-2022                         update logic of Component_type and Unit cost        Vijay Kelkar
# MAGIC -- 1.2                 08-06-2022                         added new column "native_component_type"            Vijay Kelkar
# MAGIC -- 1.3                 24-06-2022                         updated logic for unit_cost                         Bhaskar Reddy
# MAGIC -- 1.4                 30-06-2022                         updated logic for unit_cost                         Bhaskar Reddy
# MAGIC -- 1.5                 08-07-2022                         Updated logic for duns_parent_supplier_name         Vijay Kelkar
# MAGIC ---1.6                 19-07-2022                           Added 2 new column make_buy_cd,make_buy_nm        Vijay Kelkar
# MAGIC ---1.7                 20-07-2022                           Updated logic for Duns_supplier_name              Bhaskar Reddy
# MAGIC -- **************************************************************************/
# MAGIC 
# MAGIC -- Databricks notebook source
# MAGIC 
# MAGIC with f_cntrl_tower_sku_site as (
# MAGIC select distinct 
# MAGIC concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) as Sku_site_cd,
# MAGIC trim(dpp.sku) sku_number,
# MAGIC trim(dpp.plant_cd) as ops_site,
# MAGIC d_product.sku_nm as sku_name,
# MAGIC d_product.base_uom_cd as sku_uom,
# MAGIC d_tamr_prod_classification.cat_level_1 as category_l1,
# MAGIC d_tamr_prod_classification.cat_level_3 as category_l3,
# MAGIC d_tamr_prod_classification.cat_level_2 as category_l2,
# MAGIC first_value(mscrd.name) 
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following) as last_supplier_name ,
# MAGIC coalesce(first_value(mscrd.duns_name)
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following),first_value(mscrd.name)
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following)) as Duns_supplier_name,
# MAGIC first_value(mscrd.parent_duns_name) 							
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following) as duns_parent_supplier_name,
# MAGIC cast(case
# MAGIC        when mtl_t.week_count >= 42 then 'Super Runner'
# MAGIC        when mtl_t.week_count >=26 and mtl_t.week_count < 42 then 'Runner'
# MAGIC        when mtl_t.week_count >=12 and mtl_t.week_count < 26 then 'Repeater'
# MAGIC        when mtl_t.week_count < 12 then 'Stranger'
# MAGIC        else 'Stranger' end as  string) as component_type,
# MAGIC cast(marc.plifz as double)  as lead_time,
# MAGIC cast((case when dpp.plant_cd='1001' then mbew.stprs*coalesce(pmar.pmar_rt,1) 
# MAGIC            when dpp.plant_cd ='0070' then mbew.stprs/mbew.PEINH else mbew.stprs end )as float) as unit_cost,
# MAGIC cast(dpp.sfty_stk_qty as double ) as sfty_stk_qty,
# MAGIC marc.abcin as native_component_type,
# MAGIC dpp.mrp_replenishment_type as mrp_replenishment_type,
# MAGIC 'gbl' as src_sys_cd,
# MAGIC cast(current_timestamp as string) as rec_crt_ts,
# MAGIC cast(current_timestamp as string) as rec_updt_ts,
# MAGIC cast(dpp.make_buy_nm as string ) as make_buy_nm
# MAGIC 
# MAGIC from 
# MAGIC d_product_plant  dpp
# MAGIC left outer join  d_product ON (trim(dpp.sku) = trim(d_product.sku)) AND (d_product.src_sys_cd = 'gbl')
# MAGIC left outer join 
# MAGIC (select item_cd,cat_level_1,cat_level_3,cat_level_2,spend_usd, row_number() over(partition by item_cd order by spend_usd desc) as rnk from d_tamr_prod_classification)   d_tamr_prod_classification
# MAGIC on d_tamr_prod_classification.item_cd = dpp.sku and d_tamr_prod_classification.rnk ='1'
# MAGIC left outer join (select sku,name,account_name,duns_name,parent_duns_name,po_crt_dt from (     
# MAGIC select 
# MAGIC dpsx.sku as sku,
# MAGIC dpsx.vendor_name as name ,
# MAGIC supplier_corporate_recognition.account_name as account_name,
# MAGIC supplier_corporate_recognition.duns_name as duns_name ,
# MAGIC supplier_corporate_recognition.parent_duns_name as parent_duns_name,
# MAGIC dpsx.po_crt_dt as po_crt_dt
# MAGIC from  d_product_supplier_xref dpsx 
# MAGIC left join (
# MAGIC   select
# MAGIC       crd.account_key ,
# MAGIC       crd.account_name,
# MAGIC       crd.duns_name,
# MAGIC       crd.parent_duns_name
# MAGIC       from    mct_consumer_source_mapping as csm
# MAGIC       join   mct_supplier_corporate_recognition_data as crd
# MAGIC         on csm.original_source = crd.original_source
# MAGIC            and  csm.consumer = 'Bravo'
# MAGIC            and csm.source_system_id ='SAP-USWAL'
# MAGIC       ) supplier_corporate_recognition
# MAGIC            --on upper(supplier_corporate_recognition.account_name) = upper(dpsx.vendor_name )
# MAGIC on trim(regexp_replace(vendor_id,'^[0]*','')) = trim(supplier_corporate_recognition.account_key)
# MAGIC            and dpsx.src_sys_cd = 'gbl')  
# MAGIC            ) mscrd on  trim(dpp.sku) = trim(mscrd.sku)
# MAGIC left outer join (select * from marc where   marc.lvorm is not null ) marc on concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) = concat(trim(marc.matnr),'|',trim(marc.werks)) 
# MAGIC left outer join mbew mbew  on concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) = concat(trim(mbew.matnr),'|',trim(mbew.bwkey))
# MAGIC left outer join (select matnr, count(distinct extract(week from to_date(bldat,'yyyyMMdd'))) as week_count  from  mseg 
# MAGIC left outer join  mkpf  on mkpf.mandt = mseg.mandt and mkpf.mblnr = mseg.mblnr and mkpf.mjahr = mseg.mjahr
# MAGIC left outer join edp_lkup inv_harmnze on  mseg.bwart = inv_harmnze.lkup_key_02 and inv_harmnze.lkup_key_01='gbl' and lkup_val_02 = 'Consumption' 
# MAGIC where bldat >= date_format(add_months(current_date(),-12),'yMMdd')   
# MAGIC group by matnr)mtl_t on trim(dpp.sku) = mtl_t.matnr 
# MAGIC left outer join (select matnr,po_curncy_cd,co_curncy_cd,bedat,werks,row_num from (select matnr,po_curncy_cd,co_curncy_cd,bedat,werks, row_number()over (partition by matnr,werks  order by bedat desc ) row_num   from (select ekpo.matnr as matnr,if(ekko.waers='RMB','CNY',ekko.waers) as po_curncy_cd ,
# MAGIC if(t001.waers='RMB','CNY',t001.waers) as co_curncy_cd,max(eket.bedat) as bedat, ekpo.werks as werks from ekpo
# MAGIC left outer join ekko on ekko.ebeln = ekpo.ebeln 
# MAGIC left outer join t001 on ekko.bukrs=t001.bukrs and t001.mandt = '100'
# MAGIC left outer join  eket on ekpo.ebeln= eket.ebeln and ekpo.ebelp= eket.ebelp 
# MAGIC where ekpo.werks in ('1001','0010','0070') 
# MAGIC group by ekpo.matnr,if(ekko.waers='RMB','CNY',ekko.waers) ,
# MAGIC if(t001.waers='RMB','CNY',t001.waers)  , ekpo.werks
# MAGIC )) where row_num = '1'  
# MAGIC ) co_curr on trim(dpp.sku) = trim(co_curr.matnr) and  dpp.plant_cd =trim(co_curr.werks)
# MAGIC left outer join d_curncy_mth_rt pmar on
# MAGIC co_curr.co_curncy_cd = pmar.from_curncy_cd and
# MAGIC co_curr.po_curncy_cd = pmar.to_curncy_cd and 
# MAGIC pmar.to_curncy_cd = 'USD' and 
# MAGIC substr(co_curr.bedat,1,6) = pmar.yr_mth_nbr
# MAGIC where    dpp.plant_cd in ('0010' , '1001','0070') and trim(dpp.sku) is not null
# MAGIC )
# MAGIC 
# MAGIC select * 
# MAGIC from f_cntrl_tower_sku_site
# MAGIC -- where Duns_supplier_name is not null
# MAGIC where sku_site_cd='000-877302|0070'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- /**************************************************************************
# MAGIC -- Artefact Name :- sku site control tower for Lighthouse 
# MAGIC -- Description :-  PR1 sku_site_cntrl_twr
# MAGIC -- -------------------------------------------------------------------------------------------------------------------------------
# MAGIC -- Change Log
# MAGIC -- Version :        Date :                                Description                                     Changed By
# MAGIC -- -------------------------------------------------------------------------------------------------------------------------------
# MAGIC -- 1.0                25-04-2022                           f_cntrl_twe_sku_site_cd for 
# MAGIC --														    PR1/GBL                                           Vijay Kelkar
# MAGIC -- 1.1                 06-06-2022                         update logic of Component_type and Unit cost        Vijay Kelkar
# MAGIC -- 1.2                 08-06-2022                         added new column "native_component_type"            Vijay Kelkar
# MAGIC -- 1.3                 24-06-2022                         updated logic for unit_cost                         Bhaskar Reddy
# MAGIC -- 1.4                 30-06-2022                         updated logic for unit_cost                         Bhaskar Reddy
# MAGIC -- 1.5                 08-07-2022                         Updated logic for duns_parent_supplier_name         Vijay Kelkar
# MAGIC ---1.6                 19-07-2022                           Added 2 new column make_buy_cd,make_buy_nm        Vijay Kelkar
# MAGIC ---1.7                 20-07-2022                           Updated logic for Duns_supplier_name              Bhaskar Reddy
# MAGIC -- **************************************************************************/
# MAGIC 
# MAGIC -- Databricks notebook source
# MAGIC 
# MAGIC with f_cntrl_tower_sku_site as (
# MAGIC select distinct 
# MAGIC concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) as Sku_site_cd,
# MAGIC trim(dpp.sku) sku_number,
# MAGIC trim(dpp.plant_cd) as ops_site,
# MAGIC d_product.sku_nm as sku_name,
# MAGIC d_product.base_uom_cd as sku_uom,
# MAGIC d_tamr_prod_classification.cat_level_1 as category_l1,
# MAGIC d_tamr_prod_classification.cat_level_3 as category_l3,
# MAGIC d_tamr_prod_classification.cat_level_2 as category_l2,
# MAGIC first_value(mscrd.name) 
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following) as last_supplier_name ,
# MAGIC coalesce(first_value(mscrd.duns_name)
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following),
# MAGIC first_value(mscrd.name) over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following)) as Duns_supplier_name,
# MAGIC first_value(mscrd.parent_duns_name) 							
# MAGIC over(partition by concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) order by mscrd.po_crt_dt desc rows between unbounded preceding and unbounded following) as duns_parent_supplier_name,
# MAGIC cast(case
# MAGIC        when mtl_t.week_count >= 42 then 'Super Runner'
# MAGIC        when mtl_t.week_count >=26 and mtl_t.week_count < 42 then 'Runner'
# MAGIC        when mtl_t.week_count >=12 and mtl_t.week_count < 26 then 'Repeater'
# MAGIC        when mtl_t.week_count < 12 then 'Stranger'
# MAGIC        else 'Stranger' end as  string) as component_type,
# MAGIC cast(marc.plifz as double)  as lead_time,
# MAGIC cast((case when dpp.plant_cd='1001' then mbew.stprs*coalesce(pmar.pmar_rt,1) 
# MAGIC            when dpp.plant_cd ='0070' then mbew.stprs/mbew.PEINH else mbew.stprs end )as float) as unit_cost,
# MAGIC cast(dpp.sfty_stk_qty as double ) as sfty_stk_qty,
# MAGIC marc.abcin as native_component_type,
# MAGIC dpp.mrp_replenishment_type as mrp_replenishment_type,
# MAGIC 'gbl' as src_sys_cd,
# MAGIC cast(current_timestamp as string) as rec_crt_ts,
# MAGIC cast(current_timestamp as string) as rec_updt_ts,
# MAGIC cast(dpp.make_buy_nm as string ) as make_buy_nm
# MAGIC 
# MAGIC from 
# MAGIC d_product_plant  dpp
# MAGIC left outer join  d_product ON (trim(dpp.sku) = trim(d_product.sku)) AND (d_product.src_sys_cd = 'gbl')
# MAGIC left outer join 
# MAGIC (select item_cd,cat_level_1,cat_level_3,cat_level_2,spend_usd, row_number() over(partition by item_cd order by spend_usd desc) as rnk from d_tamr_prod_classification)   d_tamr_prod_classification
# MAGIC on d_tamr_prod_classification.item_cd = dpp.sku and d_tamr_prod_classification.rnk ='1'
# MAGIC left outer join (select sku,name,account_name,duns_name,parent_duns_name,po_crt_dt from (     
# MAGIC select 
# MAGIC dpsx.sku as sku,
# MAGIC dpsx.vendor_name as name ,
# MAGIC supplier_corporate_recognition.account_name as account_name,
# MAGIC supplier_corporate_recognition.duns_name as duns_name ,
# MAGIC supplier_corporate_recognition.parent_duns_name as parent_duns_name,
# MAGIC dpsx.po_crt_dt as po_crt_dt
# MAGIC from  d_product_supplier_xref dpsx 
# MAGIC left join (
# MAGIC   select
# MAGIC       crd.account_key ,
# MAGIC       crd.account_name,
# MAGIC       crd.duns_name,
# MAGIC       crd.parent_duns_name
# MAGIC       from    mct_consumer_source_mapping as csm
# MAGIC       join   mct_supplier_corporate_recognition_data as crd
# MAGIC         on csm.original_source = crd.original_source
# MAGIC            and  csm.consumer = 'Bravo'
# MAGIC            and csm.source_system_id ='SAP-USWAL'
# MAGIC       ) supplier_corporate_recognition
# MAGIC            --on upper(supplier_corporate_recognition.account_name) = upper(dpsx.vendor_name )
# MAGIC on trim(regexp_replace(vendor_id,'^[0]*','')) = trim(supplier_corporate_recognition.account_key)
# MAGIC            and dpsx.src_sys_cd = 'gbl')  
# MAGIC            ) mscrd on  trim(dpp.sku) = trim(mscrd.sku)
# MAGIC left outer join (select * from marc where   marc.lvorm is not null ) marc on concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) = concat(trim(marc.matnr),'|',trim(marc.werks)) 
# MAGIC left outer join mbew mbew  on concat(trim(dpp.sku),'|',trim(dpp.plant_cd)) = concat(trim(mbew.matnr),'|',trim(mbew.bwkey))
# MAGIC left outer join (select matnr, count(distinct extract(week from to_date(bldat,'yyyyMMdd'))) as week_count  from  mseg 
# MAGIC left outer join  mkpf  on mkpf.mandt = mseg.mandt and mkpf.mblnr = mseg.mblnr and mkpf.mjahr = mseg.mjahr
# MAGIC left outer join edp_lkup inv_harmnze on  mseg.bwart = inv_harmnze.lkup_key_02 and inv_harmnze.lkup_key_01='gbl' and lkup_val_02 = 'Consumption' 
# MAGIC where bldat >= date_format(add_months(current_date(),-12),'yMMdd')   
# MAGIC group by matnr)mtl_t on trim(dpp.sku) = mtl_t.matnr 
# MAGIC left outer join (select matnr,po_curncy_cd,co_curncy_cd,bedat,werks,row_num from (select matnr,po_curncy_cd,co_curncy_cd,bedat,werks, row_number()over (partition by matnr,werks  order by bedat desc ) row_num   from (select ekpo.matnr as matnr,if(ekko.waers='RMB','CNY',ekko.waers) as po_curncy_cd ,
# MAGIC if(t001.waers='RMB','CNY',t001.waers) as co_curncy_cd,max(eket.bedat) as bedat, ekpo.werks as werks from ekpo
# MAGIC left outer join ekko on ekko.ebeln = ekpo.ebeln 
# MAGIC left outer join t001 on ekko.bukrs=t001.bukrs and t001.mandt = '100'
# MAGIC left outer join  eket on ekpo.ebeln= eket.ebeln and ekpo.ebelp= eket.ebelp 
# MAGIC where ekpo.werks in ('1001','0010','0070') 
# MAGIC group by ekpo.matnr,if(ekko.waers='RMB','CNY',ekko.waers) ,
# MAGIC if(t001.waers='RMB','CNY',t001.waers)  , ekpo.werks
# MAGIC )) where row_num = '1'  
# MAGIC ) co_curr on trim(dpp.sku) = trim(co_curr.matnr) and  dpp.plant_cd =trim(co_curr.werks)
# MAGIC left outer join d_curncy_mth_rt pmar on
# MAGIC co_curr.co_curncy_cd = pmar.from_curncy_cd and
# MAGIC co_curr.po_curncy_cd = pmar.to_curncy_cd and 
# MAGIC pmar.to_curncy_cd = 'USD' and 
# MAGIC substr(co_curr.bedat,1,6) = pmar.yr_mth_nbr
# MAGIC where    dpp.plant_cd in ('0010' , '1001','0070') and trim(dpp.sku) is not null
# MAGIC )
# MAGIC 
# MAGIC select * 
# MAGIC from f_cntrl_tower_sku_site
# MAGIC where Duns_supplier_name is not null
# MAGIC -- where sku_site_cd='000-877302|0070'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from d_product_plant
# MAGIC where sku='000-877302' and plant_cd='0070'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from d_product_plant
# MAGIC where sku='00001-08020' and plant_cd='0010'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from d_product_supplier_xref
# MAGIC where vendor_name='FUTURE ACTIVE IND CORP'
# MAGIC limit 2
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from d_product_supplier_xref
# MAGIC where vendor_name is null
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from d_product_supplier_xref
# MAGIC where sku='000-877302'
# MAGIC limit 2
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from d_product_supplier_xref
# MAGIC where sku='00001-08020'
# MAGIC limit 2
# MAGIC ;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC with d_product_supplier_xref_gbl as (
# MAGIC select prod_suplr_key,
# MAGIC src_sys_cd,
# MAGIC item_id,
# MAGIC sku,
# MAGIC org_id,
# MAGIC vendor_id,
# MAGIC vendor_name,
# MAGIC transactions_enabled_flag,
# MAGIC po_crt_dt,
# MAGIC cast(current_timestamp as timestamp) as rec_crt_ts,
# MAGIC cast(current_timestamp as timestamp) as rec_updt_ts
# MAGIC from (select 
# MAGIC 'gbl|' || marc.matnr ||'|' || marc.werks as prod_suplr_key,
# MAGIC 'gbl' as src_sys_cd,
# MAGIC cast(marc.matnr as string) as item_id,
# MAGIC cast(marc.matnr as string) as sku,
# MAGIC cast(marc.werks as string) as org_id,
# MAGIC cast(temp_sup.lifnr as string) as vendor_id,
# MAGIC cast(temp_sup.name1 as string) as vendor_name,
# MAGIC cast(null as string) as transactions_enabled_flag,
# MAGIC temp_sup.bedat as po_crt_dt,
# MAGIC row_number() over (partition by 'gbl|' || marc.matnr ||'|' || marc.werks ,temp_sup.lifnr order by temp_sup.bedat desc) as rnk
# MAGIC 
# MAGIC from  (
# MAGIC select distinct ekpo.matnr as matnr ,
# MAGIC lfa1.name1 as name1,
# MAGIC lfa1.lifnr as lifnr,
# MAGIC max(ekko.bedat) as bedat  
# MAGIC from  ekko, lfa1, ekpo  
# MAGIC where 
# MAGIC  ekko.ebeln = ekpo.ebeln and  
# MAGIC  ekko.lifnr = lfa1.lifnr and lfa1.mandt ='100' and 
# MAGIC  ekpo.werks  in ('0010' , '1001','0070') 
# MAGIC group by 
# MAGIC ekpo.matnr,lfa1.name1  ,lfa1.lifnr
# MAGIC 
# MAGIC ) temp_sup  
# MAGIC left outer join marc
# MAGIC  on temp_sup.matnr = marc.matnr
# MAGIC left outer join mara on temp_sup.matnr = mara.matnr
# MAGIC where  marc.werks in ('1001','0010','0070')
# MAGIC )  where rnk = 1
# MAGIC )
# MAGIC 
# MAGIC select *
# MAGIC from d_product_supplier_xref_gbl
# MAGIC where sku='000-877302'
# MAGIC -- where vendor_name='FUTURE ACTIVE IND CORP'
# MAGIC ;
# MAGIC      

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


