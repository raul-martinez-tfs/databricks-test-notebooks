# Databricks notebook source
# DBTITLE 1,To check the count of dims and facts in one file 
# MAGIC %md
# MAGIC Here we are first taking all the necessary dims and facts taking there count and then creating a single table and exposing it to athena 
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC you can enter three env uat prod and test to fetch data

# COMMAND ----------

dbutils.widgets.text(name = "environment", defaultValue = "", label = "01.Enter The Environment")
env = dbutils.widgets.get("environment")

dbutils.widgets.text(name = "project", defaultValue = "NA", label = "project")
project = dbutils.widgets.get("project")
# lighthouse,cad,aig3s

dbutils.widgets.text(name = "table_name", defaultValue = "NA", label = "table_name")
table_name = dbutils.widgets.get("table_name")
# f_purchase_order,f_po_receipt,f_invntry_bal_dly_hist,f_invntry_txn,f_forecast

dbutils.widgets.text(name = "src_sys_cd", defaultValue = "NA", label = "src_sys_cd")
src_sys_cd = dbutils.widgets.get("src_sys_cd")
# nav_ger,gbl,e1lsg,m2m,ebs_lgn,saplsg

dbutils.widgets.text(name = "generate_email", defaultValue = "N", label = "generate_email")
generate_email = dbutils.widgets.get("generate_email")

# COMMAND ----------

fields_data = [
  ('f_purchase_order', 'po_qty'), 
  ('f_po_receipt', 'recpt_txn_qty'), 
  ('f_po_receipt', 'recpt_base_qty'), 
  ('f_invntry_bal_dly_hist', 'on_hand_qty'), 
  ('f_invntry_txn', 'txn_qty'),
  ('f_forecast', 'qty'),
]

fields_cols = ["table_name", "column_name"]
fields_table = spark.createDataFrame(data=fields_data, schema=fields_cols).createOrReplaceTempView("fields_table")

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

# DBTITLE 1,necessary libraries
import boto3
from pyspark.sql.functions import *
import re
from pyspark.sql import Row
from datetime import date
from delta.tables import *
from pyspark.sql.types import *

# COMMAND ----------

list_table_name = table_name.split(",")
list_project = project.split(",")
list_src_sys_cd = src_sys_cd.split(",")
str_table_name =""
str_project = ""
str_src_sys_cd=""

for table in list_table_name:
  str_table_name = f"""{str_table_name}"{table}","""

str_table_name = str_table_name[:-1]

for project in list_project:
  str_project = f"""{str_project}"{project}","""
  
str_project = str_project[:-1]

for erp in list_src_sys_cd:
  str_src_sys_cd = f"""{str_src_sys_cd}"{erp}","""
  
str_src_sys_cd = str_src_sys_cd[:-1]

# print(table_name,project)
where_clause=""

# 3 conditions 
# only project 
if((project == "NA" or project == "") and (table_name == "NA" or table_name == "")  ):
    where_clause =" "

elif((project != "NA" or project != "") and (table_name == "NA" or table_name == "")  ):
  where_clause = f"where project in ({str_project}) "

# only table and Src_Sys_cd
elif((project == "NA" or project == "" ) and (table_name != "NA" or table_name != "" or src_sys_cd !="" or src_sys_cd !="NA" )):
  where_clause = f"where table_name in ({str_table_name}) and src_sys_cd in ({str_table_name}) "

# both present 
elif((project != "NA" or project != "") and (table_name != "NA" or table_name != "") and (src_sys_cd !="" or src_sys_cd !="NA")):
  where_clause = f"where project in ({str_project}) and table_name in ({str_table_name}) and src_sys_cd in ({str_src_sys_cd})"

else:
  where_clause =" "
print(where_clause)

print("**************  preparing where clause for Report email notification  **************")
if((project != "NA" or project != "") and (table_name != "NA" or table_name != "") and (src_sys_cd !="" or src_sys_cd !="NA")):
  where_clause_email_generation = f"where project in ({str_project}) and table_name in ({str_table_name}) and src_sys_cd_cntrl_tbl in ({str_src_sys_cd})"
else:
  where_clause_email_generation=""
print(where_clause_email_generation)  

# COMMAND ----------

df_control_table = (spark.sql(
  f''' 
    select 
      DISTINCT src_sys_cd,
      table_name,
      project,
      target_bucket,
      athena_db_name,
      write_format,
      -- write_mode,
      status_flag,
      partition_cols,
      load_group,
      -- load_type,
      is_active,
      extract_type 
    from (
      select 
        table_name,
        case when src_sys_cd='r12' then 'ebs_lgn' else src_sys_cd end as src_sys_cd,
        project,
        target_bucket,
        athena_db_name,
        write_format,
        -- write_mode,
        status_flag,
        partition_cols,
        load_group,
        -- load_type,
        is_active,
        extract_type from delta.`s3://tfsdl-edp-common-dims-prod/processed/control_table`
    ) 
    {where_clause} 
  '''))

df_control_table.createOrReplaceTempView("tbl_control_table")
df_control_table=spark.sql("select * from tbl_control_table where load_group<>-1")
df_control_table.createOrReplaceTempView("tbl_control_table")
print("control_table view got created ")

# COMMAND ----------

df_control_table_fields=spark.sql(
  """
    select a.*, b.column_name
    from tbl_control_table a
    join fields_table b
    on a.table_name=b.table_name
    ;
  """
)

df_control_table_fields.createOrReplaceTempView("tbl_control_table_fields")

# COMMAND ----------

# %sql
# select *
# from tbl_control_table_fields
# ;

# COMMAND ----------

control_table_list_fields = [row.asDict() for row in df_control_table_fields.collect()]

# if env=='uat':
#   control_table_list_fields = [{k:v.replace('-prod', '-uat') if k=='target_bucket' else v for k,v in i.items()} for i in df_control_table_fields]
  
print(len(control_table_list_fields))
print(control_table_list_fields[0])

# COMMAND ----------

# DBTITLE 1,UDFs 
def today_date_creation():
  return date.today().strftime("%Y/%m/%d").replace("/","")
  
def last_word_delete(string):
  spl_string = string.split()
  rm = spl_string[:-1]
  listToStr = ' '.join([str(elem) for elem in rm])
  return listToStr

def create_regex_formula(text,formula_number):
  if formula_number ==  1:
    return re.search("s3://(.*)/(processed.*)",text)
  if formula_number ==  2:
    text=text.split('/')
    return text[-2]
  
  
def folder_list_creation(bucket,prefix):
  result = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
  Path_of_folder_in_list = []
  count_of_folders=0
  
  for o in result.get('CommonPrefixes'):
      Path_of_folder_in_list.append( o.get('Prefix'))
      count_of_folders=count_of_folders+1
      
  print('count of folders : {}          for bucket : {}'.format(count_of_folders,bucket))
  if bucket == f"tfsdl-edp-common-dims-{env}":
    return filter_dims_and_facts_from_bucket(Path_of_folder_in_list)
  else:
    return Path_of_folder_in_list
  
  
def read_table_and_create_view(bucket,single_path,view_name,format):
    header = []
    if format == 'delta':
      df =  spark.read.format('delta').load(bucket+single_path)
    else:
      df =  spark.read.format('parquet').load(bucket+single_path)
    for field in df.schema.fields:
      header.append(field.name)   
    df.createOrReplaceTempView(view_name)
    print(f"created view for given {format} table name as {view_name}")
    if df.count() <= 0:
      return "zero_records"
    if "src_sys_cd" in header:
      print(" yes src_sys_cd exist")
      return True
    else:
      print("src_sys_cd does not exist")
      return False    
    
    
def generate_querry_according_to_view(
  view_name,Is_src_sys_cd,src_sys_cd_cntrl_tbl,table_name_cntrl_tbl,project_cntrl_tbl,search_like_src_sys_cd,field_name
):
   
  if Is_src_sys_cd == True:
    return  f"""
      select 
        '{view_name}' as table_name,
        '{field_name}' as column_name,
        '{project_cntrl_tbl}' as project_cntrl_tbl,
        src_sys_cd,
        '{src_sys_cd_cntrl_tbl}' as src_sys_cd_cntrl_tbl,
        sum({field_name}) as qty_sum,
        date_format(current_timestamp,'yMMdd')  as date,
        cast(date_format(current_timestamp,'y') as string) year ,
        cast(date_format(current_timestamp,'MM') as string) month, 
        cast(date_format(current_timestamp,'dd') as string) day,
        '{table_name_cntrl_tbl}' as table_name_cntrl_tbl 
      from {view_name}
      where src_sys_cd like '{search_like_src_sys_cd}'
      group by src_sys_cd
      ;
    """
  else:
    return  f"""
      select 
        '{view_name}' as table_name,
        '{field_name}' as column_name,
        '{project_cntrl_tbl}' as project_cntrl_tbl,
        'NA' as src_sys_cd ,
        '{src_sys_cd_cntrl_tbl}' as src_sys_cd_cntrl_tbl,
        sum({field_name}) as qty_sum,
        date_format(current_timestamp,'yMMdd')  as date,
        cast(date_format(current_timestamp,'y') as string) year ,
        cast(date_format(current_timestamp,'MM') as string) month, 
        cast(date_format(current_timestamp,'dd') as string) day,
        '{table_name_cntrl_tbl}' as table_name_cntrl_tbl 
      from {view_name}
      ;
    """
  
def filter_dims_and_facts_from_bucket(list_of_tables):
  new_list = []
  for single_table in list_of_tables:
    if 'd_' in single_table or 'f_' in single_table:
      new_list.append(single_table)
  return new_list


def read_and_generate_query(list_of_tables,bucket,src_sys_cd_cntrl_tbl,table_name_cntrl_tbl,project_cntrl_tbl,search_like_src_sys_cd,field_name):
  querry=''
  for single_path in list_of_tables:
    regex = create_regex_formula(single_path,2)
    view_name=regex
    try:
      Is_src_sys_cd = read_table_and_create_view(bucket,single_path,view_name,"delta") 
      querry = querry + generate_querry_according_to_view(view_name,Is_src_sys_cd,src_sys_cd_cntrl_tbl,table_name_cntrl_tbl,project_cntrl_tbl,search_like_src_sys_cd,field_name)+'\n union '

    except Exception as e:
      try:
        Is_src_sys_cd = read_table_and_create_view(bucket,single_path,view_name,"parquet") 
        querry = querry + generate_querry_according_to_view(view_name,Is_src_sys_cd,src_sys_cd_cntrl_tbl,table_name_cntrl_tbl,project_cntrl_tbl,search_like_src_sys_cd,field_name)+'\n union '

      except Exception as e:
        print(e)
        continue
      continue   
  return last_word_delete(querry)

# COMMAND ----------

TableFields = [
  StructField("table_name",StringType(),False),
  StructField("column_name",StringType(),False),
  StructField("project_cntrl_tbl",StringType(),False),# from cntrl_tbl
  StructField("src_sys_cd",StringType(),True),
  StructField("src_sys_cd_cntrl_tbl",StringType(),False),
  StructField("qty_sum",LongType(),False),
  StructField("date",StringType(),False),#change
  StructField("year",StringType(),False),
  StructField("month",StringType(),False),
  StructField("day",StringType(),False),
  StructField("table_name_cntrl_tbl",StringType(),False),
]

TableSchema = StructType(TableFields)

# COMMAND ----------

# DBTITLE 1,Variables Definition
latest_date =today_date_creation()
year = latest_date[:4]
month = latest_date[4:6]
day = latest_date[6:8]
s3_client = boto3.client('s3')
empty_dataframe = spark.sparkContext.emptyRDD()
df_incremental_data = spark.createDataFrame(empty_dataframe,TableSchema)
output_table = 'tbl_edp_consumable_layer_qty_sum'

# COMMAND ----------

# DBTITLE 1,Main fxn
for location in control_table_list_fields:
  bucket = location["target_bucket"]
  src_sys_cd_cntrl_tbl=location["src_sys_cd"]
  table_name_cntrl_tbl=location["table_name"]
  project_cntrl_tbl=location["project"]
  prefix = "processed/"
  table = location["table_name"]  
  field_name = location["column_name"]  
  list_of_tables=[]
  search_like_src_sys_cd="%"+src_sys_cd_cntrl_tbl+"%"
  print('search_like_src_sys_cd:', search_like_src_sys_cd)
  print('project_cntrl_tbl:', project_cntrl_tbl)
  print('src_sys_cd_cntrl_tbl:', src_sys_cd_cntrl_tbl)
  print('table_name_cntrl_tbl:', table_name_cntrl_tbl)
  print('field_name:', field_name)
  
  if table.lower() =="all":
    list_of_tables = folder_list_creation(bucket,prefix)
    print('list_of_tables:', list_of_tables)
    sql = read_and_generate_query(list_of_tables,bucket,src_sys_cd_cntrl_tbl,table_name_cntrl_tbl,project_cntrl_tbl,search_like_src_sys_cd,field_name)
  else:
    
    for single_table in table.split(','):
      list_of_tables.append(prefix + single_table+'/')
    print('list_of_tables:', list_of_tables)
    sql = read_and_generate_query(list_of_tables,bucket,src_sys_cd_cntrl_tbl,table_name_cntrl_tbl,project_cntrl_tbl,search_like_src_sys_cd,field_name)
  try:
    dataframe = spark.sql(sql)
  except Exception as e:
    print(e)
    continue
  df_incremental_data = df_incremental_data.union(dataframe)
  
  print('')

# COMMAND ----------

# DBTITLE 1,Incremental Data for Current Date
df_incremental_data.createOrReplaceTempView("tbl_incremental_data")

df_incremental_data_for_today=spark.sql(
  """
    select 
      distinct table_name,
      column_name,
      src_sys_cd,
      project_cntrl_tbl as project,
      qty_sum,
      date,
      table_name_cntrl_tbl,
      src_sys_cd_cntrl_tbl,
      year,
      month,
      day 
    from tbl_incremental_data
  """
)
df_incremental_data_for_today.createOrReplaceTempView("tbl_incremental_data_for_today")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from tbl_incremental_data_for_today 
# MAGIC limit 5;

# COMMAND ----------

# # WARNING - ONLY CLEAR TABLE FOR TESTING
# dbutils.fs.ls('s3://tfsdl-edp-common-dims-uat/processed/tbl_edp_consumable_layer_qty_sum')
# dbutils.fs.rm('s3://tfsdl-edp-common-dims-uat/processed/tbl_edp_consumable_layer_qty_sum', True)

# COMMAND ----------

# # WARNING - ONLY DURING SET-UP - create target delta table
# df=spark.sql('select * from tbl_incremental_data_for_today')
# p_list = ['year', 'month', 'day']
# df.write.partitionBy(p_list).format('delta').mode('append').save(f's3://tfsdl-edp-common-dims-uat/processed/{output_table}')

# COMMAND ----------

# DBTITLE 1,Target Data ( Historical Data )
load_path = f"s3://tfsdl-edp-common-dims-{env}/processed/{output_table}"
df_target_data =  spark.read.format('delta').load(load_path)
df_target_data.createOrReplaceTempView("tbl_target_data")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from tbl_target_data
# MAGIC limit 5
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC Merge into tbl_target_data as Destination
# MAGIC using tbl_incremental_data_for_today as Source
# MAGIC ON upper(ltrim(rtrim(Destination.table_name)))=upper(ltrim(rtrim(Source.table_name)))
# MAGIC   AND upper(ltrim(rtrim(Destination.column_name)))=upper(ltrim(rtrim(Source.column_name)))
# MAGIC   AND upper(ltrim(rtrim(Destination.project)))=upper(ltrim(rtrim(Source.project)))
# MAGIC   AND upper(ltrim(rtrim(Destination.src_sys_cd)))=upper(ltrim(rtrim(Source.src_sys_cd)))
# MAGIC   and Destination.date = Source.date
# MAGIC   and Destination.year = Source.year
# MAGIC   and Destination.month = Source.month
# MAGIC   and Destination.day = Source.day
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET Destination.qty_sum=Source.qty_sum
# MAGIC 
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     src_sys_cd,
# MAGIC     project,
# MAGIC     qty_sum,
# MAGIC     date,
# MAGIC     table_name_cntrl_tbl,
# MAGIC     src_sys_cd_cntrl_tbl,
# MAGIC     year,
# MAGIC     month,
# MAGIC     day
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     Source.table_name,
# MAGIC     Source.column_name,
# MAGIC     Source.src_sys_cd,
# MAGIC     Source.project,
# MAGIC     Source.qty_sum,
# MAGIC     Source.date,
# MAGIC     Source.table_name_cntrl_tbl,
# MAGIC     Source.src_sys_cd_cntrl_tbl,
# MAGIC     Source.year,
# MAGIC     Source.month,
# MAGIC     Source.day
# MAGIC   )

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false") 
vacuum_query= f"VACUUM delta.`{load_path}` RETAIN 0 HOURS"
spark.sql(vacuum_query).show(truncate=True)

# COMMAND ----------

# DBTITLE 1,athena load partition
import time
if env == 'test':
  athena_db_name = "tfsdl_edp_common_dims"
  AWS_REGION = "us-east-1"
  config = {'OutputLocation': "s3://aws-athena-query-results-096654394133-us-east-1/"}  
  ACCESS_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser-test_UserName')
  SECRET_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser-test_PassWord')
elif env == 'prod': 
  athena_db_name = "tfsdl_edp_common_dims"
  AWS_REGION = "us-east-1" 
  config = {"OutputLocation": "s3://aws-athena-query-results-903987810958-us-east-1/"}
  ACCESS_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser_UserName')
  SECRET_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser_PassWord')
elif env == 'uat': 
  athena_db_name = "tfsdl_edp_common_dims_uat"
  AWS_REGION = "us-east-1"
  config = {'OutputLocation': "s3://aws-athena-query-results-096654394133-us-east-1/"}
  ACCESS_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser-test_UserName')
  SECRET_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser-test_PassWord')
else:
  raise RuntimeError from None 

  
client = boto3.client('athena',region_name=AWS_REGION,
     aws_access_key_id=ACCESS_KEY,
     aws_secret_access_key=SECRET_KEY)

sql = f'MSCK REPAIR TABLE {athena_db_name}.{output_table} ;'
  
time.sleep(10)
  
context = {'Database': f'{athena_db_name}'}
response= client.start_query_execution(QueryString = sql, 
                               QueryExecutionContext = context,
                               ResultConfiguration = config)

print(response)

# COMMAND ----------

if generate_email == 'Y':
  load_path = f"s3://tfsdl-edp-common-dims-{env}/processed/{output_table}"
  #s3://tfsdl-edp-common-dims-prod/processed/{output_table}/
  source_df=spark.read.format("delta").load(load_path)
  source_df.createOrReplaceTempView("source_df")
  source_df_2 = spark.sql(
    f"""
      select * 
      from source_df 
      {where_clause_email_generation}
    """
  )
  source_df_2.createOrReplaceTempView("tbl_source_df_2")
  
  s2_df=spark.sql(
    """
      select 
        table_name,
        column_name,
        src_sys_cd,
        project,
        to_date(date, 'yyyyMMdd') as formatted_date,
        qty_sum
      from tbl_source_df_2
    """
  )
  #s2_df.printSchema()
  s2_df.createOrReplaceTempView("s2_df_tbl")

  filtered_df=spark.sql(
    """
      select * 
      from 
      s2_df_tbl 
      where formatted_date > current_date()-8
    """
  )
  filtered_df.createOrReplaceTempView("filtered_df_tbl")

  pivoted_df=filtered_df.groupBy("table_name","column_name","src_sys_cd","project").pivot("formatted_date").sum("qty_sum")
  
  #Do Validation Here for the combination of group by column and date there should be only one value 
  pivoted_df.createOrReplaceTempView("pivoted_df_tbl")

  N_days = 2
  moving_average_2_days_df=spark.sql(
    f"""
      select 
        table_name,
        column_name,
        src_sys_cd,
        project,
        formatted_date,
        qty_sum,
        row_number() over(partition by table_name,column_name,src_sys_cd,project order by formatted_date desc) as row_num,
        Avg(qty_sum) over(
          partition by table_name,column_name,src_sys_cd 
          order by formatted_date desc
          RANGE BETWEEN CURRENT ROW AND {N_days-1} FOLLOWING
          ) as Moving_Average 
      from filtered_df_tbl
    """
  )
  moving_average_2_days_df.createOrReplaceTempView("moving_average_2_days_df_tbl")
  
  formatted_moving_average_2_days_df=spark.sql(
    """
      select 
        table_name,
        column_name,
        src_sys_cd,
        project,
        formatted_date,
        qty_sum,
        row_num,
        -- floor(Moving_Average) as Moving_Average 
        Moving_Average
      from moving_average_2_days_df_tbl
    """
  )
  formatted_moving_average_2_days_df.createOrReplaceTempView("formatted_moving_average_2_days_df_tbl")

  count_average_df=spark.sql(
    f"""
      select 
        table_name,
        column_name,
        src_sys_cd,
        project,
        formatted_date,
        qty_sum,
        row_num,
        last_2_days_Average,
        (qty_sum-last_2_days_Average) as Variance_Quantity_Sum_for_Today,
        concat(format_number((((qty_sum-last_2_days_Average)/last_2_days_Average)*100),6),' %') as Variance_percentage_for_Today 
      from (
        select 
          table_name,
          column_name,
          src_sys_cd,
          project,
          formatted_date,
          qty_sum,
          row_num,
          lag(Moving_Average, 1) over(partition by table_name,column_name,src_sys_cd,project order by formatted_date asc) as last_2_days_Average  
        from formatted_moving_average_2_days_df_tbl 
        where row_num between {N_days-1} and {N_days+1}
        ) a 
      where a.row_num={N_days}-1
    """
  )
  count_average_df.createOrReplaceTempView("count_average_df_tbl")

# COMMAND ----------

# %sql
# select * from pivoted_df_tbl limit 10;

# COMMAND ----------

# %sql
# select * from formatted_moving_average_7_days_df_tbl limit 10;

# COMMAND ----------

if generate_email == 'Y':
  spark_df_final_dataset=spark.sql(
    """
      select 
        a.*,
        b.last_2_days_Average,
        b.Variance_Quantity_Sum_for_Today,
        b.Variance_percentage_for_Today
      from pivoted_df_tbl a 
      -- inner join count_average_df_tbl b 
      full outer join count_average_df_tbl b 
      ON a.table_name=b.table_name 
        and a.column_name=b.column_name
        and a.src_sys_cd=b.src_sys_cd 
      order by Variance_Quantity_Sum_for_Today desc
    """
  )

# COMMAND ----------

if generate_email == 'Y':
  display(spark_df_final_dataset)

# COMMAND ----------

import math

if generate_email == 'Y':
  import pandas as pd

  pandas_df = spark_df_final_dataset.toPandas()
  
  # round each date column value to zero decimals and truncate 
  cols = [i for i in pandas_df.columns if i.startswith('2')] + [
    'last_2_days_Average', 
    'Variance_Quantity_Sum_for_Today'
  ]
  for c in cols:
#     pandas_df[c] = pandas_df[c].round().apply(lambda x: "{:,}".format(math.trunc(x)))
    pandas_df[c] = pandas_df[c].round(3).apply(lambda x: "{:,}".format(x))
    
  pandas_df.drop(['project'], axis=1, inplace=True)
  
  # Option 1: Render HTML using Pandas Styler
  styler = pandas_df.style.set_table_styles([{
    'selector' : 'table,th,tr,td',
    'props' : [('border', '1px solid green')]
  }])
  html_table = styler.render()

  # Option 2: Render table using plain html from ppandas
  #html_table = pandas_df.to_html()

  # For plain text tabular formatting (but its not rendered properly in outlook)
  # from tabulate import tabulate
  # html_table = tabulate(pandas_df, headers = 'keys', tablefmt = 'psql')

  # print(html_table)

# COMMAND ----------

print(pandas_df.src_sys_cd.unique())
pandas_df.head()

# COMMAND ----------

if generate_email == 'Y':
  from datetime import datetime
  curr_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

  SUBJECT = f"Quantity Sum Variance report for {project} ({src_sys_cd}) on  ({curr_date} UTC)"

  BODY_HTML = f"""
  <html>
    <head>
    </head>
    <body>
      <h1>Quantity Sum Variance  report for {project} ({src_sys_cd}) on ({curr_date} UTC)</h1>
      <br>
      <p>Below is the list of tables for {project}({src_sys_cd}) : </p>
      <p  >{html_table}</p>
    </body>
  </html>
  """  

  # The email body for recipients with non-HTML email clients.
  BODY_TEXT = (f"Quantity Sum Variance report for {project} ({src_sys_cd}) on  ({curr_date} UTC)\r\n"
               f"Below is the list of tables for {project} ({src_sys_cd}) : \r\n"
               f"{html_table}."
              )

  # print(BODY_TEXT)

# COMMAND ----------

if generate_email == 'Y':
  import boto3
  from botocore.exceptions import ClientError

  def email_report():

    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    SENDER = "<uspgh.svc.databricks@thermofisher.com>"

    # Replace recipient@example.com with a "To" address. If your account 
    # is still in the sandbox, this address must be verified.
    RECIPIENT = [
#       "EDP-PlatformOps@thermofisher.onmicrosoft.com",
      "raju.gokaraju@thermofisher.com",
      "raul.martinez3@thermofisher.com",
      "lekhana.potla@thermofisher.com",
    ]
    

    #RECIPIENT = ["prashant.kumar@thermofisher.com","marouane.skandaji@thermofisher.com"]

  #   RECIPIENT = [["shivam.mishra@thermofisher.com","someshraju.suraparaju@thermofisher.com","prashant.kumar@thermofisher.com","lekhana.potla@thermofisher.com"]


    # Specify a configuration set. If you do not want to use a configuration
    # set, comment the following variable, and the 
    # ConfigurationSetName=CONFIGURATION_SET argument below.
    # CONFIGURATION_SET = "ConfigSet"

    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"

    # AWS Secrets
    ACCESS_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser_UserName')
    SECRET_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser_PassWord')

  #   ACCESS_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser-test_UserName')
  #   SECRET_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser-test_PassWord')

  #   ACCESS_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser_UserName')
  #   SECRET_KEY = dbutils.secrets.get('COMM_S3_BiGen_Scope','S3_BiGen_etluser_PassWord')



    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': 
                    RECIPIENT
                ,
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # If you are not using a configuration set, comment or delete the
            # following line
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


  email_report()

# COMMAND ----------


