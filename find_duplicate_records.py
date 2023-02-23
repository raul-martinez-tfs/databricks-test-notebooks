# Databricks notebook source
# MAGIC %md 
# MAGIC #Read control table

# COMMAND ----------

# read control tables
control_table = spark.read.format('delta').load("s3://tfsdl-edp-common-dims-prod/processed/control_table/")

# control_table_filt = control_table.where("project == 'lighthouse'").toPandas()
control_table_filt = control_table.where("project == 'wf4s'").toPandas()

print(control_table_filt.shape)
control_table_filt.head()

# COMMAND ----------

print(control_table_filt.table_name.unique().size)
print(control_table_filt.table_name.unique())

# COMMAND ----------

# MAGIC %md
# MAGIC # Lighthouse kpi tables ckeck

# COMMAND ----------

# loop through lighthouse tables and count duplicated records
tables_list = [
#   'f_invntry_bal_dly_hist', 
#   'f_forecast', 
#   'f_po_receipt', 
#   'f_purchase_order',
#   'd_product_supplier_xref', 
#   'd_product_plant',
#   'd_supplier',
#   'f_po_delivery_schedule',
#   'd_product',
  'f_cntrl_tower_usage',
#   'd_product_cost',
#   'f_invntry_txn',
  'f_cntrl_tower_sku_site',
  'f_cntrl_tower_ioh',
#   'd_org_unit',
#   'd_company',
  'f_cntrl_tower_receipts',
  'f_cntrl_tower_erp_dmd', 
  'f_cntrl_tower_fes_fcst',
  'f_cntrl_tower_aopgrowth_fcst',
  'f_cntrl_tower_open_po',
  'f_cntrl_tower_fcf_fcst',
  'f_cntrl_tower_fg_fcst',
  'f_cntrl_tower_lh_aggr_tbl',
]
for t in tables_list:
  s3_bucket = 's3://tfsdl-edp-supplychain-prod/' if t.startswith('f_') else 's3://tfsdl-edp-common-dims-prod/'
  temp_df = spark.read.format('delta').load(s3_bucket+f'processed/{t}')
  print(f'tbl: {t}, s3_b: {s3_bucket}')
  
  no_records = temp_df.count()
  print('\t No. records: {:,}'.format(no_records))
  
  no_records_after_drop_duplicates = temp_df.dropDuplicates().count()
  print('\t No. records after dropping duplicates: {:,}'.format(no_records_after_drop_duplicates))
  
  if no_records>no_records_after_drop_duplicates:
    print(f'\t Number of duplicates found: {no_records-no_records_after_drop_duplicates}')
  else:
    print('\t No duplicates found')
  
  print('')

# COMMAND ----------

# MAGIC %md
# MAGIC # 4S kpi tables ckeck

# COMMAND ----------

tables_list = [
#   'f_supplier_invoice',
#   'f_purchase_order',
#   'd_gl_acct',
#   'd_supplier'
  'f_open_po',
  'f_net_source_saving_invoice_gbs',
#   'd_supplier_diversity',
  'f_net_source_saving_receipt_gbs',
#   'd_buyer', 
#   'd_product',
  'f_net_source_saving_invoice_jaggaer',
#   'd_company',
  'f_wac_gbs',
#   'f_po_receipt', 
#   'd_org_unit',
#   'd_product_plant',
]
for t in tables_list:
  s3_bucket = 's3://tfsdl-edp-supplychain-prod/' if t.startswith('f_') else 's3://tfsdl-edp-common-dims-prod/'
  temp_df = spark.read.format('delta').load(s3_bucket+f'processed/{t}')
  print(f'tbl: {t}, s3_b: {s3_bucket}')
  
  no_records = temp_df.count()
  print('\t No. records: {:,}'.format(no_records))
  
  no_records_after_drop_duplicates = temp_df.dropDuplicates().count()
  print('\t No. records after dropping duplicates: {:,}'.format(no_records_after_drop_duplicates))
  
  if no_records>no_records_after_drop_duplicates:
    print(f'\t Number of duplicates found: {no_records-no_records_after_drop_duplicates}')
  else:
    print('\t No duplicates found')
  
  print('')

# COMMAND ----------


