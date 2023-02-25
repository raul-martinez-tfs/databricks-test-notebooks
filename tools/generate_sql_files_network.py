# Databricks notebook source
# !pip install awswrangler
# !pip install plotly==5.13.0

# COMMAND ----------

import awswrangler as wr
import boto3
from collections import defaultdict
import plotly.graph_objects as go

# COMMAND ----------

def process_sql_file(bucket, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    string = ''
    for line in obj.get()['Body']._raw_stream:
        
        line = line.decode()
        line = line.rstrip()
        line = line.split('//')[0]
        line = line.split('--')[0]
        line = line.split('#')[0]
        line = line.replace('(', ' ( ')
        line = line.replace(')', ' ) ')
        string += ' ' + line

    # remove multi-line comments:
    while string.find('/*') > -1 and string.find('*/') > -1:
        l_multi_line = string.find('/*')
        r_multi_line = string.find('*/')
        string = string[:l_multi_line] + string[r_multi_line + 2:]

    # remove extra whitespaces and make list
    words = string.split()
    return words

def find_table_names(words):
    table_names = set()
    previous_word = ''

    for word in words:
        if previous_word.lower() == 'from' or previous_word.lower() == 'join':
            if word != '(':
                
                if ',' in word:
                    words_list = word.split(',')
                    for word in words_list:
                        table_names.add(word)
                
                table_names.add(word)
        previous_word = word
    return sorted(list(table_names))

def find_table_names_from_sql_file(bucket, key):
    words = process_sql_file(bucket, key)
    return sorted(find_table_names(words), key=lambda s: s.lower())

# COMMAND ----------

tables_list = [
  'f_invntry_bal_dly_hist', 
  'f_forecast', 
  'f_po_receipt', 
  'f_purchase_order',
  'd_product_supplier_xref', 
  'd_product_plant',
  'd_supplier',
  'f_po_delivery_schedule',
  'd_product',
  'd_product_cost',
  'f_invntry_txn',
  'd_org_unit',
  'd_company',
  'cntrl_tower_tbls',
]

# COMMAND ----------

main_dict = defaultdict(dict) # strucutre as follows --> {table_name: sql_file_name: list_of_input_tables}
for t in tables_list:
  print(t)
  home_bucket = 'tfsdl-edp-supplychain-prod' if (t.startswith('f_') or t.startswith('cntrl')) else 'tfsdl-edp-common-dims-prod'
  sql_files_list = wr.s3.list_objects(f's3://{home_bucket}/workspace/{t}/')
#   print(sql_files_list)
  
  for s in sql_files_list:
    bucket = s.split('/')[2]
    key = '/'.join(s.split('/')[3:]) 
    fn = s.split('/')[-1]
    
#     main_dict[t][fn] = find_table_names_from_sql_file(bucket, key)
    try:
      main_dict[t][fn] = find_table_names_from_sql_file(bucket, key)
    except:
      print(f'sql file not processed: {s}')
      pass

# COMMAND ----------

# clean cntrl_tower_tbls dict, they follow differen file path structure
cntrl_tower_tbls_dict = defaultdict(dict)
for k,v in main_dict['cntrl_tower_tbls'].items():
#   print(k)
  if 'receipts' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_receipts'][k] = v 
  elif ('erp_dmd' in k) or ('demand' in k):
    cntrl_tower_tbls_dict['f_cntrl_tower_erp_dmd'][k] = v 
  elif 'fes_fcst' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_fes_fcst'][k] = v 
  elif 'aopgrowth_fcst' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_aopgrowth_fcst'][k] = v 
  elif 'open_po' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_open_po'][k] = v 
  elif 'fcf_fcst' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_fcf_fcst'][k] = v 
  elif 'fg_fcst' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_fg_fcst'][k] = v 
  elif 'aggr' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_lh_aggr_tbl'][k] = v 
  elif 'sku_site' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_sku_site'][k] = v 
  elif 'usage' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_usage'][k] = v 
  elif 'ioh' in k:
    cntrl_tower_tbls_dict['f_cntrl_tower_ioh'][k] = v 

# COMMAND ----------

# show available tables in dictionary, strucutre as follows --> {table_name: sql_file_name: list_of_input_tables}
main_dict_cleaned = main_dict.copy()
del main_dict_cleaned['cntrl_tower_tbls']

main_dict_cleaned.update(cntrl_tower_tbls_dict)
print(main_dict_cleaned.keys()) 

# COMMAND ----------

def plot_sankey_for_target_tables(all_tables_dict, target_tables_list):  
  source_target_list = [] # tuple: (source_tables_list, target_table_name)
  for table_name, sql_file_dict in all_tables_dict.items():
    if (table_name in target_tables_list) and (table_name.strip()!='') and (table_name!=None):
      source_target_list.append((list(set(sum([v for k,v in sql_file_dict.items()],[]))), table_name))
  
  all_table_labels = list(set(sum([i[0] for i in source_target_list],[])+[i[1] for i in source_target_list]))
  print(f'Total number of Source and Target tables: {len(all_table_labels)}')

  source_target_list_tuples = []
  for i in source_target_list:
    for j in i[0]:
      if (j.strip()=='') or (j==None):
        continue
      source_target_list_tuples.append((j,i[1])) # ([source table list], target table name)
  print(f'Total number of Source and Target relationships: {len(source_target_list_tuples)}')

  source_target_sankey = []
  for i, j in source_target_list_tuples:
    source_target_sankey.append((all_table_labels.index(i), all_table_labels.index(j)))
  
  s_sankey = [i[0] for i in source_target_sankey]
  t_sankey = [i[1] for i in source_target_sankey]
  
  fig = go.Figure(go.Sankey(
    arrangement = "snap",
      node = {
          "label": all_table_labels,
  #         "x": [0.2, 0.1, 0.5, 0.7, 0.3, 0.5],
  #         "y": [0.7, 0.5, 0.2, 0.4, 0.2, 0.3],
          'pad':10,
      },  # 10 Pixels
      link = {
          "source": s_sankey,
          "target": t_sankey,
          "value": [0.5]*len(t_sankey),
      }))

  fig.update_layout(
  #     autosize=False,
      width=1200,
      height=2500,
  )

  fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # f_invntry_bal_dly_hist Target Table

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
    'f_invntry_bal_dly_hist', 
#     'f_forecast', 
#     'f_po_receipt', 
#     'f_purchase_order', 
#     'f_po_delivery_schedule', 
#     'f_invntry_txn', 
  #   'f_cntrl_tower_aopgrowth_fcst', 
  #   'f_cntrl_tower_ioh', 
  #   'f_cntrl_tower_erp_dmd', 
  #   'f_cntrl_tower_fcf_fcst', 
  #   'f_cntrl_tower_fes_fcst', 
  #   'f_cntrl_tower_fg_fcst', 
  #   'f_cntrl_tower_lh_aggr_tbl', 
  #   'f_cntrl_tower_open_po', 
  #   'f_cntrl_tower_usage', 
  #   'f_cntrl_tower_receipts', 
  #   'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # f_forecast Target Table

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
#     'f_invntry_bal_dly_hist', 
    'f_forecast', 
#     'f_po_receipt', 
#     'f_purchase_order', 
#     'f_po_delivery_schedule', 
#     'f_invntry_txn', 
  #   'f_cntrl_tower_aopgrowth_fcst', 
  #   'f_cntrl_tower_ioh', 
  #   'f_cntrl_tower_erp_dmd', 
  #   'f_cntrl_tower_fcf_fcst', 
  #   'f_cntrl_tower_fes_fcst', 
  #   'f_cntrl_tower_fg_fcst', 
  #   'f_cntrl_tower_lh_aggr_tbl', 
  #   'f_cntrl_tower_open_po', 
  #   'f_cntrl_tower_usage', 
  #   'f_cntrl_tower_receipts', 
  #   'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # f_po_receipt Target Table

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
#     'f_invntry_bal_dly_hist', 
#     'f_forecast', 
    'f_po_receipt', 
#     'f_purchase_order', 
#     'f_po_delivery_schedule', 
#     'f_invntry_txn', 
  #   'f_cntrl_tower_aopgrowth_fcst', 
  #   'f_cntrl_tower_ioh', 
  #   'f_cntrl_tower_erp_dmd', 
  #   'f_cntrl_tower_fcf_fcst', 
  #   'f_cntrl_tower_fes_fcst', 
  #   'f_cntrl_tower_fg_fcst', 
  #   'f_cntrl_tower_lh_aggr_tbl', 
  #   'f_cntrl_tower_open_po', 
  #   'f_cntrl_tower_usage', 
  #   'f_cntrl_tower_receipts', 
  #   'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # f_purchase_order Target Table

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
#     'f_invntry_bal_dly_hist', 
#     'f_forecast', 
#     'f_po_receipt', 
    'f_purchase_order', 
#     'f_po_delivery_schedule', 
#     'f_invntry_txn', 
  #   'f_cntrl_tower_aopgrowth_fcst', 
  #   'f_cntrl_tower_ioh', 
  #   'f_cntrl_tower_erp_dmd', 
  #   'f_cntrl_tower_fcf_fcst', 
  #   'f_cntrl_tower_fes_fcst', 
  #   'f_cntrl_tower_fg_fcst', 
  #   'f_cntrl_tower_lh_aggr_tbl', 
  #   'f_cntrl_tower_open_po', 
  #   'f_cntrl_tower_usage', 
  #   'f_cntrl_tower_receipts', 
  #   'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # f_po_delivery_schedule Target Table

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
#     'f_invntry_bal_dly_hist', 
#     'f_forecast', 
#     'f_po_receipt', 
#     'f_purchase_order', 
    'f_po_delivery_schedule', 
#     'f_invntry_txn', 
  #   'f_cntrl_tower_aopgrowth_fcst', 
  #   'f_cntrl_tower_ioh', 
  #   'f_cntrl_tower_erp_dmd', 
  #   'f_cntrl_tower_fcf_fcst', 
  #   'f_cntrl_tower_fes_fcst', 
  #   'f_cntrl_tower_fg_fcst', 
  #   'f_cntrl_tower_lh_aggr_tbl', 
  #   'f_cntrl_tower_open_po', 
  #   'f_cntrl_tower_usage', 
  #   'f_cntrl_tower_receipts', 
  #   'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # f_invntry_txn Target Tables

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
#     'f_invntry_bal_dly_hist', 
#     'f_forecast', 
#     'f_po_receipt', 
#     'f_purchase_order', 
#     'f_po_delivery_schedule', 
    'f_invntry_txn', 
  #   'f_cntrl_tower_aopgrowth_fcst', 
  #   'f_cntrl_tower_ioh', 
  #   'f_cntrl_tower_erp_dmd', 
  #   'f_cntrl_tower_fcf_fcst', 
  #   'f_cntrl_tower_fes_fcst', 
  #   'f_cntrl_tower_fg_fcst', 
  #   'f_cntrl_tower_lh_aggr_tbl', 
  #   'f_cntrl_tower_open_po', 
  #   'f_cntrl_tower_usage', 
  #   'f_cntrl_tower_receipts', 
  #   'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Control Tower Target Tables

# COMMAND ----------

plot_sankey_for_target_tables(
  main_dict_cleaned,
  [
#     'f_invntry_bal_dly_hist', 
#     'f_forecast', 
#     'f_po_receipt', 
#     'f_purchase_order', 
#     'f_po_delivery_schedule', 
#     'f_invntry_txn', 
    'f_cntrl_tower_aopgrowth_fcst', 
    'f_cntrl_tower_ioh', 
    'f_cntrl_tower_erp_dmd', 
    'f_cntrl_tower_fcf_fcst', 
    'f_cntrl_tower_fes_fcst', 
    'f_cntrl_tower_fg_fcst', 
    'f_cntrl_tower_lh_aggr_tbl', 
    'f_cntrl_tower_open_po', 
    'f_cntrl_tower_usage', 
    'f_cntrl_tower_receipts', 
    'f_cntrl_tower_sku_site',
  ]
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


