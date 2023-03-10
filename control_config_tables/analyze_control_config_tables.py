# Databricks notebook source
tbl_name_fp = [
  (
    'config_table_finance_1',
    's3://tfsdl-edp-common-dims-prod/delta/loadstats_table/',
    'delta',
  ),
  (
    'consummable_variance_config',
    'file:/Workspace/Repos/raul.martinez3@thermofisher.com/databricks-test-notebooks/control_config_tables/consummable_variance_config.csv',
    'csv',
  ),
  (
    'control_table',
    's3://tfsdl-edp-common-dims-prod/processed/control_table/',
    'delta',
  ),
  (
    'config_table',
    's3://tfsdl-edp-common-dims-prod/processed/config_table/',
    'delta',
  ),
  (
    'control_table_workspace',
    's3://tfsdl-edp-workspace-prod/processed/delta/control_table/',
    'delta',
  ),
  (
    'ccg_audit_control_table',
    's3://tfsds-ccg-prod/model-output-archive/ccg_audit_control_table/',
    'parquet',
  ),
]

for tbl, fp, fmt in tbl_name_fp:
  print(tbl)
  
  if fmt=='csv':
    df = spark.read.option('header',True).csv(fp)
  else:
    df = spark.read.format(fmt).load(fp)
  
  df.createOrReplaceTempView(tbl)


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from config_table_finance_1
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct project
# MAGIC from config_table_finance_1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from consummable_variance_config
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct table_name
# MAGIC from consummable_variance_config
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from control_table
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from config_table
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from control_table_workspace
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   distinct t1.table_name, 
# MAGIC   t2.table_name
# MAGIC from control_table t1
# MAGIC full outer join control_table_workspace t2
# MAGIC   on t1.table_name=t2.table_name
# MAGIC where t1.table_name is null 
# MAGIC   or t2.table_name is null 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   distinct t1.table_name, 
# MAGIC   t2.table_name
# MAGIC from control_table t1
# MAGIC full outer join control_table_workspace t2
# MAGIC   on t1.table_name=t2.table_name
# MAGIC where t2.table_name is null 
# MAGIC --   or t1.table_name is null 
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(distinct table_name) from control_table
# MAGIC UNION ALL
# MAGIC select count(distinct table_name) from control_table_workspace
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from ccg_audit_control_table
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct table_name)
# MAGIC from ccg_audit_control_table
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


