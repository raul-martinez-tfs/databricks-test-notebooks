# Databricks notebook source
spark.read.format('delta').load('s3://tfsdl-edp-supplychain-prod/processed/f_cntrl_tower_lh_aggr_tbl/').createOrReplaceTempView('f_cntrl_tower_lh_aggr_tbl')
spark.read.format('delta').load('s3://tfsdl-edp-supplychain-prod/processed/f_cntrl_tower_sku_site/').createOrReplaceTempView('f_cntrl_tower_sku_site')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 
# MAGIC     a.*,
# MAGIC     b.rec_crt_ts,
# MAGIC     b.rec_updt_ts
# MAGIC from f_cntrl_tower_lh_aggr_tbl a
# MAGIC join f_cntrl_tower_sku_site b
# MAGIC on a.sku_site_cd=b.sku_site_cd
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- validate record count variance report
# MAGIC select 
# MAGIC     substr(b.rec_crt_ts, 1, 10),
# MAGIC     count(1) as cnt
# MAGIC from f_cntrl_tower_lh_aggr_tbl a
# MAGIC join f_cntrl_tower_sku_site b
# MAGIC on a.sku_site_cd=b.sku_site_cd
# MAGIC group by substr(b.rec_crt_ts, 1, 10)
# MAGIC ;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


