# Databricks notebook source


# COMMAND ----------

# import tables as SQL views
f_purchase_order = spark.read.format('delta').load("s3://tfsdl-edp-supplychain-prod/processed/f_purchase_order/")
f_purchase_order.createOrReplaceTempView("f_purchase_order")

f_po_receipt = spark.read.format('delta').load("s3://tfsdl-edp-supplychain-prod/processed/f_po_receipt/")
f_po_receipt.createOrReplaceTempView("f_po_receipt")

f_invntry_bal_dly_hist = spark.read.format('delta').load("s3://tfsdl-edp-supplychain-prod/processed/f_invntry_bal_dly_hist/")
f_invntry_bal_dly_hist.createOrReplaceTempView("f_invntry_bal_dly_hist")

f_invntry_txn = spark.read.format('delta').load("s3://tfsdl-edp-supplychain-prod/processed/f_invntry_txn/")
f_invntry_txn.createOrReplaceTempView("f_invntry_txn")

f_forecast = spark.read.format('delta').load("s3://tfsdl-edp-supplychain-prod/processed/f_forecast/")
f_forecast.createOrReplaceTempView("f_forecast")

# COMMAND ----------

lighthouse_erps = [
  'gbl',
  'm2m_mbrg',
  'ebs_lgn',
  'nav_ger',	
  'e1lsg',
  'saplsg',
]

# COMMAND ----------

# MAGIC %sql
# MAGIC select * -- po_qty
# MAGIC from f_purchase_order 
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct src_sys_cd
# MAGIC from f_purchase_order 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * -- recpt_txn_qty, recpt_base_qty
# MAGIC from f_po_receipt 
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * -- on_hand_qty, capture_dt,
# MAGIC from f_invntry_bal_dly_hist 
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * -- txn_qty
# MAGIC from f_invntry_txn 
# MAGIC where src_sys_cd = 'ebs_lgn' and txn_period='201501'
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * -- qty, capture_dt
# MAGIC from f_forecast 
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE tfsdl_edp_common_dims_uat.tbl_edp_consumable_layer_qty_sum_2 (
# MAGIC   table_name string,
# MAGIC   column_name string,
# MAGIC   src_sys_cd string,
# MAGIC   project string,
# MAGIC   qty_sum string,
# MAGIC   report_date string,
# MAGIC   table_name_cntrl_tbl string,
# MAGIC   src_sys_cd_cntrl_tbl string
# MAGIC ) 
# MAGIC PARTITIONED BY (year string,month string,day string) 
# MAGIC STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
# MAGIC OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
# MAGIC LOCATION 's3://tfsdl-edp-common-dims-uat/processed/tbl_edp_consumable_layer_qty_sum/_symlink_format_manifest/'
# MAGIC TBLPROPERTIES ('transient_lastDdlTime'='1662652785')
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC GENERATE symlink_format_manifest FOR TABLE tfsdl_edp_common_dims_uat.tbl_edp_consumable_layer_qty_sum;

# COMMAND ----------

# MAGIC %sql
# MAGIC use tfsdl_edp_common_dims_uat
# MAGIC show TABLES
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM information_schema.columns
# MAGIC limit 10
# MAGIC -- WHERE  table_schema = 'rdspostgresql'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC -- from tfsdl_edp_common_dims_uat.tbl_edp_consumable_layer_qty_sum
# MAGIC from tfsdl_edp_common_dims_uat.d_bom
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

tbl_edp_consumable_layer_qty_sum = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-uat/processed/tbl_edp_consumable_layer_qty_sum/")
tbl_edp_consumable_layer_qty_sum.createOrReplaceTempView("tbl_edp_consumable_layer_qty_sum")

# COMMAND ----------

d_bom = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-uat/delta/d_bom/")
d_bom.createOrReplaceTempView("d_bom")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from d_bom
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from tbl_edp_consumable_layer_qty_sum
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------


