from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.sql import Window
import json,datetime
import os,time
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame


spark = SparkSession.\
        builder.\
        appName("nds_ticker_training").\
        getOrCreate()
sc = spark.sparkContext
sqlctxt = SQLContext(sc)

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def percent_calculator(values,rank):
  import numpy as np
  values = [float(i) for i in values]
  return np.percentile(values,rank)
udf_percent_rank = f.udf(percent_calculator,FloatType())


def deviation_calculator(values):
  import numpy as np
  values = [float(i) for i in values]
  return np.std(values)
udf_deviation_rank = f.udf(deviation_calculator,FloatType())

def dec_time(timer):
  (h, m, s) = timer.split(':')
  result = int(h) * 3600 + int(m) * 60 + int(s)
  return round(result/float(86400),5)
udf_dec = f.udf(dec_time)

def get_time(decimal_time):
  sec = round(decimal_time * 86400)
  m, s = divmod(sec, 60)
  h, m = divmod(m, 60)
  d, h = divmod(h, 24)
  m = int(m)
  s = int(s)
  if m < 10:
    m = '0' + str(m)
  if s < 10:
    s = '0' + str(s)
  return str(int(h)) + ":" + str(m) + ":" + str(s)
udf_time = f.udf(get_time)

def max_before(values):
  values = [float(i) for i in values]
  values = [i for i in values if i <= 0.75347]
  if values != []:
    return max(values)
  else:
    return 0.75347

udf_max_before = f.udf(max_before)  

database = sys.argv[2]
database_m = sys.argv[1]

data = sqlctxt.sql("select * from " + str(database) + ".pxo_nds_navdel_1")
data = data.filter(data.as_of_date != 'NAV_DATE')
data = data.withColumn('Date_Identifier',f.lit(100))
data = data.select('Date_Identifier','ticker','as_of_date','event_time_numeric','FUND_ID')
data = data.withColumn('as_of_date', f.from_unixtime(f.unix_timestamp('as_of_date', 'MM/dd/yyy')).cast(DateType()))
max_date = str(datetime.date.today() - datetime.timedelta(days = 90))
data= data.filter(data.as_of_date >= max_date) 
data = data.filter(data.event_time_numeric <= 0.79167)
data = data.withColumn('ticker',f.trim(data.ticker))


client_info = sqlctxt.sql("select * from " + str(database) + ".PXO_NAVNOW_FUND_CLIENT_MAPPING_1")
client_info = client_info.select('client_name','fund_id','ticker')
client_info = client_info.withColumn('fund_id',f.trim(client_info.fund_id))
client_info = client_info.withColumn('ticker',f.trim(client_info.ticker))


data = data.join(client_info,on = ['fund_id','ticker'],how = 'inner')
data = data.dropna()

data = data.groupby('client_name','fund_id','ticker','as_of_date').agg(f.min('event_time_numeric').alias('event_time_numeric'))
data = data.dropna()


less_data = data.groupby('client_name','ticker','FUND_ID').agg(f.approxCountDistinct('as_of_date').alias('no_of_days'),\
                                                 f.max('event_time_numeric').alias('maxtime'),\
                                                 f.min('event_time_numeric').alias('fund_min_time'),\
                                                 f.collect_list("event_time_numeric").alias("group_time"))

less_data = less_data.filter(less_data.no_of_days <= 5)


data = data.groupby('client_name','ticker','FUND_ID').agg(f.approxCountDistinct('as_of_date').alias('no_of_days'),\
                                                 f.max('event_time_numeric').alias('maxtime'),\
                                                 f.min('event_time_numeric').alias('fund_min_time'),\
                                                 f.collect_list("event_time_numeric").alias("group_time"))

data = data.filter(data.no_of_days > 5)


data = data.withColumn('Quantile50',udf_percent_rank('group_time',f.lit(50)))
data = data.withColumn('Quantile05',udf_percent_rank('group_time',f.lit(5)))
data = data.withColumn('Quantile10',udf_percent_rank('group_time',f.lit(10)))
data = data.withColumn('Quantile15',udf_percent_rank('group_time',f.lit(15)))
data = data.withColumn('Quantile20',udf_percent_rank('group_time',f.lit(20)))
data = data.withColumn('Quantile60',udf_percent_rank('group_time',f.lit(60)))
data = data.withColumn('Quantile70',udf_percent_rank('group_time',f.lit(70)))
data = data.withColumn('Quantile80',udf_percent_rank('group_time',f.lit(80)))
data = data.withColumn('Quantile95',udf_percent_rank('group_time',f.lit(95)))
data = data.withColumn('Quantile99',udf_percent_rank('group_time',f.lit(99)))
data = data.withColumn('Quantile50',udf_percent_rank('group_time',f.lit(50)))


data = data.withColumn('maxtime_before1805',udf_max_before('group_time'))


temp_copy = data
temp_copy = temp_copy.drop('group_time')


temp_copy = temp_copy.withColumn('fund_min_time',udf_time('fund_min_time'))
temp_copy = temp_copy.withColumn('Quantile50',udf_time('Quantile50'))
temp_copy = temp_copy.withColumn('Quantile05',udf_time('Quantile05'))
temp_copy = temp_copy.withColumn('Quantile10',udf_time('Quantile10'))
temp_copy = temp_copy.withColumn('Quantile15',udf_time('Quantile15'))
temp_copy = temp_copy.withColumn('Quantile20',udf_time('Quantile20'))
temp_copy = temp_copy.withColumn('Quantile60',udf_time('Quantile60'))
temp_copy = temp_copy.withColumn('Quantile70',udf_time('Quantile70'))
temp_copy = temp_copy.withColumn('Quantile80',udf_time('Quantile80'))
temp_copy = temp_copy.withColumn('Quantile95',udf_time('Quantile95'))
temp_copy = temp_copy.withColumn('Quantile99',udf_time('Quantile99'))
temp_copy = temp_copy.withColumn('maxtime',udf_time('maxtime'))
temp_copy = temp_copy.withColumn('maxtime_before1805',udf_time('maxtime_before1805'))

temp_copy = temp_copy.withColumn('alert_time',temp_copy['maxtime_before1805'].cast(TimestampType()))
temp_copy = temp_copy.withColumn('alert_time',temp_copy.alert_time - f.expr("interval 5 minutes"))
temp_copy = temp_copy.withColumn('alert_time',f.date_format('alert_time','HH:mm:ss'))


from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
def unionAll(*dfs):
  return reduce(DataFrame.unionAll, dfs)


temp_copy = temp_copy.withColumn('sla',f.lit('1'))
df_master = temp_copy.filter(temp_copy.fund_min_time == 'status')


list1 = ['fund_min_time','Quantile99','Quantile95','Quantile80','Quantile20','Quantile05','fund_min_time']
list2 = ['Always Miss','Q99<SLA','Q95<SLA','Q80<SLA','Q20<SLA','Q5<SLA','Q1<SLA']
for c, s in zip(list1, list2):
    if s == 'Always Miss':
        temp_result = temp_copy.where(temp_copy[c] > '18:05:00')
        temp_result = temp_result.withColumn('sla',f.lit(s))
        temp_copy = temp_copy.where(temp_copy[c] <= '18:05:00')
    else:
        temp_result = temp_copy.where(temp_copy[c] < '18:05:00')
        temp_result = temp_result.withColumn('sla',f.lit(s))
        temp_copy = temp_copy.where(temp_copy[c] >= '18:05:00')
    df_master = unionAll(df_master,temp_result)
df_master = df_master.withColumnRenamed('fund_min_time','Quantile01')
df_master = df_master.withColumnRenamed('Quantile50','predicted_time')

def melt(df, id_vars, value_vars,var_name="Status", value_name="Time"):
    #Convert `DataFrame` from wide to long format.

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = f.array(*(
        f.struct(f.lit(c).alias(var_name), f.col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", f.explode(_vars_and_vals))

    cols = id_vars + [
            f.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

q1_sla = df_master.filter(df_master.sla == 'Q1<SLA')
q1_sla = q1_sla.select('client_name','ticker','fund_id','quantile01','maxtime_before1805','predicted_time','quantile80','quantile99')
q1_sla = q1_sla.withColumnRenamed('quantile01','Probably')
q1_sla = q1_sla.withColumn('maxtime_before1805',q1_sla.maxtime_before1805.cast(TimestampType()))
q1_sla = q1_sla.withColumn('maxtime_before1805',q1_sla.maxtime_before1805 + f.expr("interval 1 seconds"))
q1_sla = q1_sla.withColumn('maxtime_before1805',f.date_format('maxtime_before1805','HH:mm:ss'))
q1_sla = q1_sla.withColumnRenamed('maxtime_before1805','Probably Yes')
q1_sla = q1_sla.withColumn('Definitely Yes',f.lit('18:05:00'))
q1_sla = q1_sla.withColumnRenamed('predicted_time','Missed')
q1_sla = q1_sla.withColumnRenamed('quantile80','Missed1')
q1_sla = q1_sla.withColumnRenamed('quantile99','Missed2')
q1_sla = q1_sla.withColumn('last_time',f.lit('19:04:00'))



 
q1_sla = melt(q1_sla, id_vars=['client_name','ticker','fund_id'], value_vars=['Probably','Probably Yes','Definitely Yes','Missed','Missed1','Missed2','last_time'])



w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

q1_sla = q1_sla.withColumn('Status',f.rank().over(w))


q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','1','Probably'))
q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','2','Probably Yes'))
q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','3','Definitely Yes'))
q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','4','Missed'))
q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','5','Missed'))
q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','6','Missed'))
q1_sla = q1_sla.withColumn('Status',f.regexp_replace('Status','7','last_time'))




q5_sla = df_master.filter(df_master.sla == 'Q5<SLA')
q5_sla = q5_sla.withColumnRenamed('quantile01','Probably Not')
q5_sla = q5_sla.withColumnRenamed('quantile05','Probably')
q5_sla = q5_sla.withColumn('maxtime_before1805',q5_sla.maxtime_before1805.cast(TimestampType()))
q5_sla = q5_sla.withColumn('maxtime_before1805',q5_sla.maxtime_before1805 + f.expr("interval 1 seconds"))
q5_sla = q5_sla.withColumn('maxtime_before1805',f.date_format('maxtime_before1805','HH:mm:ss'))
q5_sla = q5_sla.withColumnRenamed('maxtime_before1805','Probably Yes')
q5_sla = q5_sla.withColumn('Definitely Yes',f.lit('18:05:00'))
q5_sla = q5_sla.withColumnRenamed('quantile15','Definitely Yes1')
q5_sla = q5_sla.withColumnRenamed('predicted_time','Missed1')
q5_sla = q5_sla.withColumnRenamed('quantile80','Missed2')
q5_sla = q5_sla.withColumnRenamed('quantile99','Missed3')
q5_sla = q5_sla.withColumn('last_time',f.lit('19:04:00'))




q5_sla = melt(q5_sla, id_vars=['client_name','ticker','fund_id'], value_vars=['Probably Not','Probably','Probably Yes','Definitely Yes','Definitely Yes1','Missed1','Missed2','Missed3','last_time'])
w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

q5_sla = q5_sla.withColumn('Status',f.rank().over(w))


q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','1','Probably Not'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','2','Probably'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','3','Probably Yes'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','4','Definitely Yes'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','5','Definitely Yes'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','6','Missed'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','7','Missed'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','8','Missed'))
q5_sla = q5_sla.withColumn('Status',f.regexp_replace('Status','9','last_time'))



q20_sla = df_master.filter(df_master.sla == 'Q20<SLA')
q20_sla = q20_sla.withColumnRenamed('quantile01','Probably Not')
q20_sla = q20_sla.withColumnRenamed('quantile10','Probably')
q20_sla = q20_sla.withColumnRenamed('quantile20','Probably Yes')
q20_sla = q20_sla.withColumn('maxtime_before1805',q20_sla.maxtime_before1805.cast(TimestampType()))
q20_sla = q20_sla.withColumn('maxtime_before1805',q20_sla.maxtime_before1805 + f.expr("interval 1 seconds"))
q20_sla = q20_sla.withColumn('maxtime_before1805',f.date_format('maxtime_before1805','HH:mm:ss'))
q20_sla = q20_sla.withColumnRenamed('maxtime_before1805','Probably Yes1')
q20_sla = q20_sla.withColumn('Definitely Yes',f.lit('18:05:00'))
q20_sla = q20_sla.withColumnRenamed('quantile80','Missed1')
q20_sla = q20_sla.withColumnRenamed('quantile99','Missed2')
q20_sla = q20_sla.withColumn('last_time',f.lit('19:04:00'))



q20_sla = melt(q20_sla, id_vars=['client_name','ticker','fund_id'], value_vars=['Probably Not','Probably','Probably Yes','Probably Yes1','Definitely Yes','Missed1','Missed2','last_time'])

w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 
q20_sla = q20_sla.withColumn('Status',f.rank().over(w))


q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','1','Probably Not'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','2','Probably'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','3','Probably Yes'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','4','Probably Yes'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','5','Definitely Yes'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','6','Missed'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','7','Missed'))
q20_sla = q20_sla.withColumn('Status',f.regexp_replace('Status','8','last_time'))




q80_sla = df_master.filter(df_master.sla == 'Q80<SLA')
q80_sla = q80_sla.withColumnRenamed('predicted_time','Definitely Not')
q80_sla = q80_sla.withColumnRenamed('quantile60','Probably Not')
q80_sla = q80_sla.withColumnRenamed('quantile80','Probably')
q80_sla = q80_sla.withColumn('maxtime_before1805',q80_sla.maxtime_before1805.cast(TimestampType()))
q80_sla = q80_sla.withColumn('maxtime_before1805',q80_sla.maxtime_before1805 + f.expr("interval 1 seconds"))
q80_sla = q80_sla.withColumn('maxtime_before1805',f.date_format('maxtime_before1805','HH:mm:ss'))
q80_sla = q80_sla.withColumnRenamed('maxtime_before1805','Probably Yes')
q80_sla = q80_sla.withColumn('Definitely Yes',f.lit('18:05:00'))
q80_sla = q80_sla.withColumnRenamed('quantile95','Missed1')
q80_sla = q80_sla.withColumnRenamed('quantile99','Missed2')
q80_sla = q80_sla.withColumn('last_time',f.lit('19:04:00'))



q80_sla = melt(q80_sla, id_vars=['client_name','ticker','fund_id'], value_vars=['Definitely Not','Probably Not','Probably','Definitely Yes','Probably Yes','Missed1','Missed2','last_time'])
w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

q80_sla = q80_sla.withColumn('Status',f.rank().over(w))


q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','1','Definitely Not'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','2','Probably Not'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','3','Probably'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','4','Probably Yes'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','5','Definitely Yes'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','6','Missed'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','7','Missed'))
q80_sla = q80_sla.withColumn('Status',f.regexp_replace('Status','8','last_time'))




q99_sla = df_master.filter(df_master.sla == 'Q99<SLA')
q99_sla = q99_sla.withColumnRenamed('predicted_time','Definitely Not')
q99_sla = q99_sla.withColumnRenamed('quantile80','Definitely Not1')
q99_sla = q99_sla.withColumnRenamed('quantile95','Definitely Not2')
q99_sla = q99_sla.withColumnRenamed('quantile99','Definitely Not3')
q99_sla = q99_sla.withColumn('maxtime_before1805',q99_sla.maxtime_before1805.cast(TimestampType()))
q99_sla = q99_sla.withColumn('maxtime_before1805',q99_sla.maxtime_before1805 + f.expr("interval 1 seconds"))
q99_sla = q99_sla.withColumn('maxtime_before1805',f.date_format('maxtime_before1805','HH:mm:ss'))
q99_sla = q99_sla.withColumnRenamed('maxtime_before1805','Probably')
q99_sla = q99_sla.withColumn('maxtime',q99_sla.maxtime.cast(TimestampType()))
q99_sla = q99_sla.withColumn('maxtime',q99_sla.maxtime + f.expr("interval 2 seconds"))
q99_sla = q99_sla.withColumn('maxtime',f.date_format('maxtime','HH:mm:ss'))
q99_sla = q99_sla.withColumnRenamed('maxtime','Definitely Yes')
q99_sla = q99_sla.withColumn('Definitely Yes1',f.lit('18:05:00'))
q99_sla = q99_sla.withColumn('last_time',f.lit('19:04:00'))



q99_sla = melt(q99_sla, id_vars=['client_name','ticker','fund_id'], value_vars=['Definitely Not','Definitely Not1','Definitely Not2','Definitely Not3','Probably','Definitely Yes','last_time','Definitely Yes1'])
w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

q99_sla = q99_sla.withColumn('Status',f.rank().over(w))


q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','1','Definitely Not'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','2','Definitely Not'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','3','Definitely Not'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','4','Definitely Not'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','5','Probably'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','6','Definitely Yes'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','7','Definitely Yes'))
q99_sla = q99_sla.withColumn('Status',f.regexp_replace('Status','8','last_time'))

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
q95_sla = df_master.filter(df_master.sla == 'Q95<SLA')
q95_sla = q95_sla.withColumnRenamed('predicted_time','Definitely Not')
q95_sla = q95_sla.withColumnRenamed('quantile80','Definitely Not1')
q95_sla = q95_sla.withColumnRenamed('quantile95','Probably Not')
q95_sla = q95_sla.withColumnRenamed('quantile99','Probably')
q95_sla = q95_sla.withColumn('alert_time',q95_sla.alert_time.cast(TimestampType()))
q95_sla = q95_sla.withColumn('alert_time',q95_sla.alert_time + f.expr("interval 1 seconds"))
q95_sla = q95_sla.withColumn('alert_time',f.date_format('alert_time','HH:mm:ss'))
q95_sla = q95_sla.withColumnRenamed('alert_time','Definitely Yes')
q95_sla = q95_sla.withColumn('Definitely Yes1',f.lit('18:05:00'))
q95_sla = q95_sla.withColumn('last_time',f.lit('19:04:00'))


        
q95_sla = melt(q95_sla, id_vars=['client_name','ticker','fund_id'], value_vars=['Definitely Not','Definitely Not1','Probably Not','Probably','Definitely Yes','last_time','Definitely Yes1'])
w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

q95_sla = q95_sla.withColumn('Status',f.rank().over(w))


q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','1','Definitely Not'))
q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','2','Definitely Not'))
q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','3','Probably Not'))
q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','4','Probably'))
q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','5','Definitely Yes'))
q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','6','Definitely Yes'))
q95_sla = q95_sla.withColumn('Status',f.regexp_replace('Status','7','last_time'))




always_miss = df_master.filter(df_master.sla == 'Always Miss')
always_miss = always_miss.withColumnRenamed('quantile01','Definitely Yes')
always_miss = always_miss.withColumnRenamed('predicted_time','Missed')
always_miss = always_miss.withColumnRenamed('quantile99','Missed1')
always_miss = always_miss.withColumn('last_time',f.lit('19:04:00'))


always_miss = melt(always_miss, id_vars=['client_name','ticker','fund_id'], value_vars=['Definitely Yes','Missed','Missed1','last_time'])
w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

always_miss = always_miss.withColumn('Status',f.rank().over(w))


always_miss = always_miss.withColumn('Status',f.regexp_replace('Status','1','Definitely Yes'))
always_miss = always_miss.withColumn('Status',f.regexp_replace('Status','2','Missed'))
always_miss = always_miss.withColumn('Status',f.regexp_replace('Status','3','Missed'))
always_miss = always_miss.withColumn('Status',f.regexp_replace('Status','4','last_time'))


less_data = less_data.withColumn('Quantile50',udf_percent_rank('group_time',f.lit(50)))
less_data = less_data.withColumn('Quantile50',udf_time('Quantile50'))
less_data = less_data.withColumn('Quantile50',f.when(less_data['Quantile50'] > '18:05:00','18:05:00').otherwise(less_data['Quantile50']))
less_data = less_data.withColumn('maxtime',udf_time('maxtime'))
less_data = less_data.withColumn('maxtime_before1805',udf_max_before('group_time'))
less_data = less_data.withColumn('maxtime_before1805',udf_time('maxtime_before1805'))
less_data = less_data.withColumn('alert_time',less_data['maxtime_before1805'].cast(TimestampType()))
less_data = less_data.withColumn('alert_time',less_data.alert_time - f.expr("interval 5 minutes"))
less_data = less_data.withColumn('alert_time',f.date_format('alert_time','HH:mm:ss'))
less_origin_data = less_data.withColumn('alert_time',f.date_format('alert_time','HH:mm:ss')).drop('group_time')
less_origin_data = less_origin_data.withColumn('noofmissdays',f.lit(0))



less_origin_data = less_origin_data.withColumn('Quantile01',f.lit(5))
less_origin_data = less_origin_data.withColumn('Quantile05',f.lit(5))
less_origin_data = less_origin_data.withColumn('Quantile10',f.lit(10))
less_origin_data = less_origin_data.withColumn('Quantile15',f.lit(15))
less_origin_data = less_origin_data.withColumn('Quantile20',f.lit(20))
less_origin_data = less_origin_data.withColumn('Quantile60',f.lit(60))
less_origin_data = less_origin_data.withColumn('Quantile70',f.lit(70))
less_origin_data = less_origin_data.withColumn('Quantile80',f.lit(80))
less_origin_data = less_origin_data.withColumn('Quantile95',f.lit(95))
less_origin_data = less_origin_data.withColumn('Quantile99',f.lit('18:05:00'))


less_data = less_data.withColumnRenamed('quantile50','Probably Not')
less_data = less_data.withColumn('maxtime',less_data.maxtime.cast(TimestampType()))
less_data = less_data.withColumn('maxtime',less_data.maxtime + f.expr('interval 1 seconds'))
less_data = less_data.withColumn('maxtime',f.date_format("maxtime",'HH:mm:ss'))
less_data = less_data.withColumnRenamed('maxtime','Probably')
less_data = less_data.withColumn('Definitely Yes',f.lit('18:05:00'))
less_data = less_data.withColumn('last_time',f.lit('19:04:00')).drop('group_time')



less_data = melt(less_data, id_vars=['client_name','ticker','fund_id'], value_vars=['Probably Not','Probably','Definitely Yes','last_time'])
w = Window.partitionBy('client_name','ticker','fund_id').orderBy('time') 

less_data = less_data.withColumn('Status',f.rank().over(w))

less_data = less_data.withColumn('Status',f.regexp_replace('Status','1','Probably Not'))
less_data = less_data.withColumn('Status',f.regexp_replace('Status','2','Probably'))
less_data = less_data.withColumn('Status',f.regexp_replace('Status','3','Definitely Yes'))
less_data = less_data.withColumn('Status',f.regexp_replace('Status','4','last_time'))


ticker_predictions = unionAll(q1_sla,q5_sla,q20_sla,q80_sla,q99_sla,q95_sla,always_miss,less_data)


data = ticker_predictions
origin_data = df_master
origin_data = origin_data.withColumn('maxtime_before1805',f.when(f.length(origin_data['maxtime_before1805']) > 2,origin_data['maxtime_before1805']).otherwise('18:05:00'))
origin_data = origin_data.withColumn('maxtime_before1805',origin_data.maxtime_before1805.cast(TimestampType()))
origin_data = origin_data.withColumn('alert_time',origin_data.maxtime_before1805 - f.expr("interval 5 minutes"))
origin_data = origin_data.withColumn('maxtime_before1805',f.date_format("maxtime_before1805",'HH:mm:ss'))
origin_data = origin_data.withColumn('alert_time',f.date_format("alert_time",'HH:mm:ss'))

origin_data = origin_data.withColumn('noofmissdays',f.lit(0))
sqlctxt.sql("truncate table " + str(database_m) +".pxo_navnow_ticker_prediction_quantiles1")

origin_data = origin_data.select("client_name",'ticker','fund_id','quantile01','quantile05','quantile10','quantile15','quantile20','predicted_time','quantile60','quantile70','quantile80','quantile95','quantile99','alert_time','no_of_days','noofmissdays','maxtime_before1805','maxtime')
less_origin_data = less_origin_data.select("client_name",'ticker','fund_id','quantile01','quantile05','quantile10','quantile15','quantile20','quantile50','quantile60','quantile70','quantile80','quantile95','quantile99','alert_time','no_of_days','noofmissdays','maxtime_before1805','maxtime')
origin_data = unionAll(origin_data,less_origin_data)

origin_data.select("client_name",'ticker','fund_id','quantile01','quantile05','quantile10',\
                   'quantile15','quantile20','predicted_time','quantile60','quantile70',\
                   'quantile80','quantile95','quantile99','alert_time','no_of_days',\
                   'noofmissdays','maxtime_before1805','maxtime').coalesce(6).write.mode("overwrite").insertInto(str(database_m) +".pxo_navnow_ticker_prediction_quantiles1")

origin_data = origin_data.withColumnRenamed('Client Name','client_name')
origin_data = origin_data.select('client_name','ticker','fund_id','Predicted_Time','maxtime_before1805','alert_time')


joined_data = data.join(origin_data,on = ['client_name','ticker','fund_id'],how = 'right')
joined_data = joined_data.withColumn("Actual_time",f.lit("NA"))

sqlctxt.sql("truncate table " + str(database_m) +".pxo_navnow_nds_ticker_prediction_1")
joined_data.select('ticker','fund_id','Client_Name','Status','Predicted_Time','maxtime_before1805','Time','Actual_time','alert_time').coalesce(6).write.insertInto(str(database_m) +".pxo_navnow_nds_ticker_prediction_1")

sc.stop()