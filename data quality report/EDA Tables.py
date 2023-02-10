# Databricks notebook source
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
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * -- qty, capture_dt
# MAGIC from f_forecast 
# MAGIC limit 10;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


