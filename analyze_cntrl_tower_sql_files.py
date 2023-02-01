# Databricks notebook source
# MAGIC %md
# MAGIC # cntrl_tower_fcf_fcst_nav_ger.sql
# MAGIC 
# MAGIC s3://tfsdl-edp-supplychain-prod/workspace/cntrl_tower_tbls/cntrl_tower_fcf_fcst_nav_ger.sql

# COMMAND ----------

# -- /**************************************************************************
# -- Artefact Name :- fcf control tower for Lighthouse 
# -- Description :-   final Consensus forecat control tower for Lighthouse 
# -- -------------------------------------------------------------------------------------------------------------------------------
# -- Change Log
# -- Version :        Date :                                Description                                     Changed By
# -- -------------------------------------------------------------------------------------------------------------------------------
# -- 0.0            	  10-06-2022                  			 First draft of sql file    			       Vijay Kelkar
# -- 1.0				  29-06-2022             Replaced cal_yr_mth_nbr with fscl_yr_prd_nbr                  Bhaskar Reddy
# -- 2.0                23-09-2022            Added new condition fcf_yearqtrnbr is not null              Vijay Kelkar,Lekhna Potla
# -- **************************************************************************/
# -- Databricks notebook source

# select * from (
# SELECT 
# concat(trim(sup_dmd_table.item_nbr),'|CAGER' )  as fcf_sku_site_cd,
# cast(MthConv.fscl_yr_qtr_nbr as string) as fcf_yearqtrnbr,
# sum(sup_dmd_table.qty) as fcf_qty,
# 'nav_ger' as src_sys_cd,
# cast(current_timestamp as string) as rec_crt_ts,
# cast(current_timestamp as string) as rec_updt_ts,
# cast(MthConv.fscl_yr_prd_nbr as string) as fcf_yearmthnbr
# from
#  f_forecast sup_dmd_table
# left outer join (SELECT distinct fscl_yr_prd_nbr ,fscl_yr_qtr_nbr FROM  d_date where fscl_yr_nbr between date_format(add_months(current_date,-12),'yyyy') and date_format(add_months(current_date,24),'yyyy')) MthConv on sup_dmd_table.capture_yr_mth_nbr =MthConv.fscl_yr_prd_nbr
# where sup_dmd_table.forecast_type ='FCF'  and  sup_dmd_table.src_sys_cd in ('nav_ger')
# and  sup_dmd_table.capture_dt= (select max(sup_dmd_table.capture_dt) from  f_forecast sup_dmd_table where src_sys_cd = 'nav_ger' )
# group by 
# fcf_sku_site_cd,MthConv.fscl_yr_qtr_nbr,MthConv.fscl_yr_prd_nbr
# ) where fcf_yearqtrnbr is not null 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### tables used on query
# MAGIC - f_forecast -> s3://tfsdl-edp-supplychain-prod/processed/f_forecast/
# MAGIC - d_date -> s3://tfsdl-edp-common-dims-prod/processed/d_date/

# COMMAND ----------

spark.read.format('delta').load('s3://tfsdl-edp-supplychain-prod/processed/f_cntrl_tower_lh_aggr_tbl/').createOrReplaceTempView('f_cntrl_tower_lh_aggr_tbl')
spark.read.format('delta').load('s3://tfsdl-edp-supplychain-prod/processed/f_forecast/').createOrReplaceTempView('f_forecast')
spark.read.format('delta').load('s3://tfsdl-edp-common-dims-prod/processed/d_date/').createOrReplaceTempView('d_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from f_cntrl_tower_lh_aggr_tbl 
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from f_cntrl_tower_lh_aggr_tbl 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*)
# MAGIC from (
# MAGIC   select sku_site_cd, recpt_qty  
# MAGIC   from f_cntrl_tower_lh_aggr_tbl 
# MAGIC   group by sku_site_cd, recpt_qty 
# MAGIC )
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from f_forecast 
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from d_date 
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### test query

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from (
# MAGIC   select 
# MAGIC     concat(trim(sup_dmd_table.item_nbr),'|CAGER' ) as fcf_sku_site_cd,
# MAGIC     cast(MthConv.fscl_yr_qtr_nbr as string) as fcf_yearqtrnbr,
# MAGIC     sum(sup_dmd_table.qty) as fcf_qty,
# MAGIC     'nav_ger' as src_sys_cd,
# MAGIC     cast(current_timestamp as string) as rec_crt_ts,
# MAGIC     cast(current_timestamp as string) as rec_updt_ts,
# MAGIC     cast(MthConv.fscl_yr_prd_nbr as string) as fcf_yearmthnbr
# MAGIC   from f_forecast sup_dmd_table
# MAGIC   left outer join (
# MAGIC     select 
# MAGIC       distinct fscl_yr_prd_nbr,
# MAGIC       fscl_yr_qtr_nbr 
# MAGIC     from  d_date 
# MAGIC     where fscl_yr_nbr between date_format(add_months(current_date,-12),'yyyy') and date_format(add_months(current_date,24),'yyyy')
# MAGIC     ) MthConv 
# MAGIC   on sup_dmd_table.capture_yr_mth_nbr =MthConv.fscl_yr_prd_nbr
# MAGIC   where sup_dmd_table.forecast_type ='FCF'  
# MAGIC     and sup_dmd_table.src_sys_cd in ('nav_ger')
# MAGIC     and sup_dmd_table.capture_dt=(select max(sup_dmd_table.capture_dt) from f_forecast sup_dmd_table where src_sys_cd = 'nav_ger')
# MAGIC   group by fcf_sku_site_cd, MthConv.fscl_yr_qtr_nbr, MthConv.fscl_yr_prd_nbr
# MAGIC ) 
# MAGIC where fcf_yearqtrnbr is not null 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### test sub-queries

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select 
# MAGIC   distinct fscl_yr_prd_nbr,
# MAGIC   fscl_yr_qtr_nbr 
# MAGIC from  d_date 
# MAGIC where fscl_yr_nbr between date_format(add_months(current_date,-12),'yyyy') and date_format(add_months(current_date,24),'yyyy')

# COMMAND ----------


