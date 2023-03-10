# Databricks notebook source
# MAGIC %md
# MAGIC ## definitions

# COMMAND ----------

import plotly.express as px
import pandas as pd

# COMMAND ----------

s3_path = 's3://tfsdl-edp-supplychain-prod/processed'
table_name_list = [
  'f_purchase_order',
  'f_po_receipt',
  'f_invntry_bal_dly_hist',
  'f_invntry_txn',
  'f_forecast',
]
src_sys_cd_list = [
  'nav_ger',
  'gbl',
  'e1lsg',
  'm2m',
  'ebs_lgn',
  'saplsg',
]
tbl_qty_dt_tuple = [
  ('f_purchase_order', 'po_qty', 'po_crt_dt'), 
  ('f_po_receipt', 'recpt_txn_qty', 'recpt_date'), 
  ('f_po_receipt', 'recpt_base_qty', 'recpt_date'), 
  ('f_invntry_bal_dly_hist', 'on_hand_qty', 'capture_dt'), 
  ('f_invntry_txn', 'txn_qty', 'txn_date'),
  ('f_forecast', 'qty', 'capture_dt'),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## import tables as SQL views

# COMMAND ----------

for tbl in table_name_list:
  df = spark.read.format('delta').load(f"s3://tfsdl-edp-supplychain-prod/processed/{tbl}")
  df.createOrReplaceTempView(tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## data quality checks

# COMMAND ----------

# check source system list values
for tbl in table_name_list:
  print(tbl)
  q = """
    select distinct src_sys_cd 
    from TN 
    where lower(src_sys_cd) like '%SS%'
  """
  query = ''
  for idx, src_sys in enumerate(src_sys_cd_list):
    q_replaced = q.replace('TN',tbl).replace('SS',src_sys)
    if idx==0:
      query += q_replaced
    else:
      query += f" union all {q_replaced}"
  spark.sql(query).show()

# COMMAND ----------

# check for null values in quantity and datetime cols
for tbl, qty, dt in tbl_qty_dt_tuple:
  print(f'{tbl}, {qty}, {dt}')
  q = """
    (
      select 
        'SS' as src_sys_cd, 
        sum(case when (QTY is null) or (trim(QTY)='') then 1 else 0 end) as qty_null_cnt,
        sum(case when (DT is null) or (trim(DT)='') then 1 else 0 end) as dt_null_cnt,
        count(1) as total_no_records
      from TN 
      where lower(src_sys_cd) like '%SS%'
    )
  """
  query = ''
  for idx, src_sys in enumerate(src_sys_cd_list):
    q_replaced = q.replace('TN',tbl).replace('SS',src_sys).replace('QTY',qty).replace('DT',dt)
    if idx==0:
      query += q_replaced
    else:
      query += f" union all {q_replaced}"
  spark.sql(query).show()

# COMMAND ----------

# check datetime string format
#   Examples:
#     len=8, 'yyyymmdd'
#     len>8, 'yyyy-mm-dd'
#     len<8, 'yyyymm'
for tbl, qty, dt in tbl_qty_dt_tuple:
  print(f'{tbl}, {qty}, {dt}')
  q = """
    (
      select 
        'SS' as src_sys_cd, 
        sum(case when length(trim(DT))=8 then 1 else 0 end) as dt_len8_cnt,
        sum(case when length(trim(DT))>8 then 1 else 0 end) as dt_gt_len8_cnt,
        sum(case when length(trim(DT))<8 then 1 else 0 end) as dt_lt_len8_cnt,
        sum(case when (DT is null) or (trim(DT)='') then 1 else 0 end) as dt_null_cnt,
        count(1) as distinct_dt_cnt
      from (
        select distinct DT
        from TN
        where lower(src_sys_cd) like '%SS%'
      )
    )
  """
  query = ''
  for idx, src_sys in enumerate(src_sys_cd_list):
    q_replaced = q.replace('TN',tbl).replace('SS',src_sys).replace('QTY',qty).replace('DT',dt)
    if idx==0:
      query += q_replaced
    else:
      query += f" union all {q_replaced}"
  spark.sql(query).show()

# COMMAND ----------

# check quantity and datetime values range
for tbl, qty, dt in tbl_qty_dt_tuple:
  print(f'{tbl}, {qty}, {dt}')
  q = """
    (
      select 
        'SS' as src_sys_cd, 
        min(cast(QTY as double)) as min_qty,
        max(cast(QTY as double)) as max_qty,
        min(to_date(DT, 'yyyyMMdd')) as min_dt_len8,
        max(to_date(DT, 'yyyyMMdd')) as max_dt_len8,
        min(to_date(substring(DT,1,10), 'yyyy-MM-dd')) as min_dt_gt_len8,
        max(to_date(substring(DT,1,10), 'yyyy-MM-dd')) as max_dt_gt_len8
      from TN 
      where lower(src_sys_cd) like '%SS%'
    )
  """
  query = ''
  for idx, src_sys in enumerate(src_sys_cd_list):
    q_replaced = q.replace('TN',tbl).replace('SS',src_sys).replace('QTY',qty).replace('DT',dt)
    if idx==0:
      query += q_replaced
    else:
      query += f" union all {q_replaced}"
  spark.sql(query).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## analyze variance on day values

# COMMAND ----------

single_day_data = []
number_days_back = 60
moving_average_days = 5
for tbl, qty, dt in tbl_qty_dt_tuple:
  print(f'{tbl}, {qty}, {dt}')
  q = f"""
    (
      with tbl_1 as (
        select 
          QTY, 
          case when length(DT)>8 then replace(substring(DT,1,10),'-','') else DT end as DT
        from TN
        where lower(src_sys_cd) like '%SS%'
      ),
      tbl_2 as (
        select 
          DT,
          sum(QTY) as sum_qty
        from tbl_1
        where to_timestamp(DT,'yyyyMMdd')>=(current_timestamp - interval '{number_days_back}' day)
          and to_timestamp(DT,'yyyyMMdd')<=current_timestamp
        group by DT
      ),
      tbl_3 as (
        select 
          *,
          lag(sum_qty) over(order by DT) as prevday_sum_qty,
          avg(sum_qty) over(order by DT rows between {moving_average_days} preceding and current row) as movavg_sum_qty,
          row_number() over(order by DT) as rnk
        from tbl_2
      ),
      tbl_4 as (
        select 
          *,
          case when rnk>{moving_average_days} then movavg_sum_qty else null end as movavg_sum_qty_cleaned
        from tbl_3
      )
      select 
      --   *,
        DT,
        round(sum_qty,3) as sum_qty,
        round(prevday_sum_qty,3) as prevday_sum_qty,
        round(movavg_sum_qty_cleaned,3) as movavg_sum_qty_cleaned,
        'SS' as src_sys_cd, 
        round(100*((sum_qty-prevday_sum_qty)/prevday_sum_qty),3) as perc_variance_prevday,
        round(100*((sum_qty-movavg_sum_qty_cleaned)/movavg_sum_qty_cleaned),3) as perc_variance_movavg
      from tbl_4
      order by DT asc
    )
  """
  query = ''
  for idx, src_sys in enumerate(src_sys_cd_list):
    q_replaced = q.replace('TN',tbl).replace('SS',src_sys).replace('QTY',qty).replace('DT',dt)
    if idx==0:
      query += q_replaced
    else:
      query += f" union all {q_replaced}"
      
  single_day_data.append((tbl, qty, dt, spark.sql(query).toPandas()))

# COMMAND ----------

single_day_data[0][3].query("src_sys_cd == 'nav_ger'").sort_values(by='po_crt_dt')[:20]

# COMMAND ----------

for tbl, qty, dt, df in single_day_data:
  print('********** analyze variance on day values --> ', tbl, '**********')
  df[dt] =  pd.to_datetime(df[dt], format='%Y%m%d')
  
  px.line(
  df, 
  x=dt, 
  y='sum_qty', 
  color='src_sys_cd',
  title=f"{tbl}: raw quantities data for '{qty}'",
  markers=True,
  ).update_layout(height=280, margin=dict(r=5, l=5, t=30, b=0)).show()

  px.line(
    df, 
    x=dt, 
    y='perc_variance_prevday', 
    color='src_sys_cd',
    title=f"{tbl}: previous day variance data for '{qty}'",
    markers=True,
  ).update_layout(height=280, margin=dict(r=5, l=5, t=30, b=0)).show()

  px.line(
    df, 
    x=dt, 
    y='perc_variance_movavg', 
    color='src_sys_cd',
    title=f"{tbl}: moving average variance data for '{qty}'",
    markers=True,
  ).update_layout(height=280, margin=dict(r=5, l=5, t=30, b=0)).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## analyze variance on cummulative values

# COMMAND ----------

single_day_data = []
number_days_back = 60
moving_average_days = 5
for tbl, qty, dt in tbl_qty_dt_tuple:
  print(f'{tbl}, {qty}, {dt}')
  q = f"""
    (
      with tbl_1 as (
        select 
          QTY, 
          case when length(DT)>8 then replace(substring(DT,1,10),'-','') else DT end as DT
        from TN
        where lower(src_sys_cd) like '%SS%'
      ),
      tbl_2 as (
        select 
          DT,
          sum(QTY) as sum_qty
        from tbl_1
        group by DT
      ),
      tbl_3 as (
        select 
          DT,
          sum(sum_qty) over(order by to_timestamp(DT,'yyyyMMdd') asc) as cumsum_qty
        from tbl_2
        where DT is not null
      ),
      tbl_4 as (
        select 
          *,
          lag(cumsum_qty) over(order by DT) as prevday_cumsum_qty,
          avg(cumsum_qty) over(order by DT rows between {moving_average_days} preceding and current row) as movavg_cumsum_qty,
          row_number() over(order by DT) as rnk
        from tbl_3
        where to_timestamp(DT,'yyyyMMdd')>=(current_timestamp - interval '{number_days_back}' day)
          and to_timestamp(DT,'yyyyMMdd')<=current_timestamp
      ),
      tbl_5 as (
        select 
          *,
          case when rnk>{moving_average_days} then movavg_cumsum_qty else null end as movavg_cumsum_qty_cleaned
        from tbl_4
      )
      select 
      --   *,
        DT,
        round(cumsum_qty,3) as cumsum_qty,
        round(prevday_cumsum_qty,3) as prevday_cumsum_qty,
        round(movavg_cumsum_qty_cleaned,3) as movavg_cumsum_qty_cleaned,
        'SS' as src_sys_cd, 
        round(100*((cumsum_qty-prevday_cumsum_qty)/prevday_cumsum_qty),3) as perc_variance_prevday,
        round(100*((cumsum_qty-movavg_cumsum_qty_cleaned)/movavg_cumsum_qty_cleaned),3) as perc_variance_movavg
      from tbl_5
      order by DT asc
    )
  """
  query = ''
  for idx, src_sys in enumerate(src_sys_cd_list):
    q_replaced = q.replace('TN',tbl).replace('SS',src_sys).replace('QTY',qty).replace('DT',dt)
    if idx==0:
      query += q_replaced
    else:
      query += f" union all {q_replaced}"
      
  single_day_data.append((tbl, qty, dt, spark.sql(query).toPandas()))

# COMMAND ----------

single_day_data[0][3].query("src_sys_cd == 'nav_ger'").sort_values(by='po_crt_dt')[:20]

# COMMAND ----------

for tbl, qty, dt, df in single_day_data:
  print('********** analyze variance on cummulative values --> ', tbl, '**********')
  df[dt] =  pd.to_datetime(df[dt], format='%Y%m%d')
  
  px.line(
  df, 
  x=dt, 
  y='cumsum_qty', 
  color='src_sys_cd',
  title=f"{tbl}: raw quantities data for '{qty}'",
  markers=True,
  ).update_layout(height=280, margin=dict(r=5, l=5, t=30, b=0)).show()

  px.line(
    df, 
    x=dt, 
    y='perc_variance_prevday', 
    color='src_sys_cd',
    title=f"{tbl}: previous day variance data for '{qty}'",
    markers=True,
  ).update_layout(height=280, margin=dict(r=5, l=5, t=30, b=0)).show()

  px.line(
    df, 
    x=dt, 
    y='perc_variance_movavg', 
    color='src_sys_cd',
    title=f"{tbl}: moving average variance data for '{qty}'",
    markers=True,
  ).update_layout(height=280, margin=dict(r=5, l=5, t=30, b=0)).show()

# COMMAND ----------



# COMMAND ----------

# %sql
# select min(to_timestamp(po_crt_dt,'yyyyMMdd')), max(to_timestamp(po_crt_dt,'yyyyMMdd'))
# from f_purchase_order
# ;

# COMMAND ----------

# %sql
# select sum(po_qty)
# from f_purchase_order
# where to_timestamp(po_crt_dt,'yyyyMMdd')<=to_timestamp('20230307','yyyyMMdd')
#   and src_sys_cd='saplsg'
# ;

# COMMAND ----------



# COMMAND ----------


