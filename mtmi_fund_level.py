"""*********************************************************************************************
 Script Name  :      mtmi_fund_training.py
 
 Project      :      Nav Now
 
 Author       :      kiran reddy kancharla



pyspark job for mtmi fund level prediciotns.

The scipt will be called from the wrapper file.
***************************************************************************************************"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import datetime,time,sys
from pyspark.sql import Window
from pyspark.sql import DataFrame

spark = SparkSession.\
        builder.\
        appName("mtmi_funds_training").\
        getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlctxt = SQLContext(sc)


database_m = sys.argv[1]
database = sys.argv[2]


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def percent_calculator(values,rank):
  import numpy as np
  values = [float(i) for i in values]
  return np.percentile(values,rank)
udf_percent_rank = f.udf(percent_calculator,FloatType())

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

def dec_time(timer):
  (h, m, s) = timer.split(':')
  result = int(h) * 3600 + int(m) * 60 + int(s)
  return round(result/float(86400),5)
udf_dec = f.udf(dec_time)

def max_before(values):
  values = [float(i) for i in values]
  values = [i for i in values if i <= 0.75347]
  if values != []:
    return max(values)
  else:
    return 0.75347
udf_max_before = f.udf(max_before)  


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


data = sqlctxt.sql("select nav_date,event_time_numeric,FUND_ID \
                    from {}.pxo_myview_mtmi_1".format(database))
data = data.filter(data.nav_date != 'NAV_DATE')
data = data.withColumn('nav_date', f.from_unixtime(f.unix_timestamp('nav_date', 'MM/dd/yyy'))\
                                    .cast(DateType()))
max_date = str(datetime.date.today() - datetime.timedelta(days = 90))
data= data.filter(data.nav_date >= max_date) 
data = data.withColumn('FUND_ID',f.trim(data.FUND_ID))


client_info = sqlctxt.sql("select distinct ticker,fund_id,client_name \
                           from {}.PXO_NAVNOW_FUND_CLIENT_MAPPING_1".format(database))
client_info = client_info.withColumn('signal',f.when(client_info['ticker'] == 'BFDS_FUNDS' ,'BFDS').otherwise('NASDAQ'))
client_info = client_info.select('fund_id','client_name','signal').distinct()
w = Window.partitionBy('fund_id').orderBy()
client_info = client_info.withColumn('sig_count',f.count('fund_id').over(w))
client_info = client_info.withColumn('client_name',f.max('client_name').over(w))
client_info = client_info.withColumn('signal',f.when(client_info['sig_count'] == 2,'BOTH').otherwise(client_info['signal'])).drop('sig_count').distinct()
data = data.join(client_info,on = 'fund_id',how = 'left')
data = data.dropna()


data = data.groupby('client_name','fund_id','nav_date','signal').\
           agg(f.min('event_time_numeric').alias('event_time_numeric'))

data = data.groupby('client_name','FUND_ID','signal').agg(f.approxCountDistinct('nav_date').alias('no_of_days'),\
                                                          f.max('event_time_numeric').alias('maxtime'),\
                                                          f.min('event_time_numeric').alias('fund_min_time'),\
                                                          f.collect_list("event_time_numeric").alias("group_time"))

less_data = data.filter(data.no_of_days <= 5)
data = data.filter(data.no_of_days > 5)


data = data.withColumn('Quantile01',udf_percent_rank('group_time',f.lit(1)))
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
data = data.withColumn('maxtime_before1805',udf_max_before('group_time'))
data = data.drop('group_time')
data = data.withColumn('fund_min_time',udf_time('fund_min_time'))
data = data.withColumn('Quantile01',udf_time('Quantile01'))
data = data.withColumn('Quantile50',udf_time('Quantile50'))
data = data.withColumn('Quantile05',udf_time('Quantile05'))
data = data.withColumn('Quantile10',udf_time('Quantile10'))
data = data.withColumn('Quantile15',udf_time('Quantile15'))
data = data.withColumn('Quantile20',udf_time('Quantile20'))
data = data.withColumn('Quantile60',udf_time('Quantile60'))
data = data.withColumn('Quantile70',udf_time('Quantile70'))
data = data.withColumn('Quantile80',udf_time('Quantile80'))
data = data.withColumn('Quantile95',udf_time('Quantile95'))
data = data.withColumn('Quantile99',udf_time('Quantile99'))
data = data.withColumn('maxtime',udf_time('maxtime'))
data = data.withColumn('maxtime_before1805',udf_time('maxtime_before1805'))
data = data.withColumn('alert_time',data['maxtime_before1805'].cast(TimestampType()))
data = data.withColumn('alert_time',data.alert_time - f.expr("interval 5 minutes"))
data = data.withColumn('alert_time',f.date_format('alert_time','HH:mm:ss'))
data = data.withColumn('noofmissdays',f.lit(0))

less_data = less_data.withColumn('Quantile50',udf_percent_rank('group_time',f.lit(50)))
less_data = less_data.withColumn('Quantile50',udf_time('Quantile50'))
less_data = less_data.withColumn('Quantile50',f.when(less_data['Quantile50'] > '18:05:00','18:05:00').otherwise(less_data['Quantile50']))
less_data = less_data.withColumn('maxtime',udf_time('maxtime'))
less_data = less_data.withColumn('maxtime_before1805',udf_max_before('group_time'))
less_data = less_data.withColumn('maxtime_before1805',udf_time('maxtime_before1805'))
less_data = less_data.withColumn('alert_time',less_data['maxtime_before1805'].cast(TimestampType()))
less_data = less_data.withColumn('alert_time',less_data.alert_time - f.expr("interval 5 minutes"))
less_data = less_data.withColumn('alert_time',f.date_format('alert_time','HH:mm:ss')).drop('group_time')


less_origin_data = less_data.withColumn('noofmissdays',f.lit(0))
less_origin_data = less_origin_data.withColumn('Quantile01',f.lit(5))
less_origin_data = less_origin_data.withColumn('Quantile05',f.lit(5))
less_origin_data = less_origin_data.withColumn('Quantile10',f.lit(10))
less_origin_data = less_origin_data.withColumn('Quantile15',f.lit(15))
less_origin_data = less_origin_data.withColumn('Quantile20',f.lit(20))
less_origin_data = less_origin_data.withColumn('Quantile60',f.lit(60))
less_origin_data = less_origin_data.withColumn('Quantile70',f.lit('17:30:00'))
less_origin_data = less_origin_data.withColumn('Quantile80',f.lit(80))
less_origin_data = less_origin_data.withColumn('Quantile95',f.lit('17:30:00'))
less_origin_data = less_origin_data.withColumn('Quantile99',f.lit('17:30:00'))


less_origin_data = less_origin_data.withColumn('last_time',f.lit('19:04:00')).drop('group_time')

data = data.select("client_name","fund_id",'signal',"Quantile01","Quantile50","Quantile05","Quantile10",\
                   "Quantile15","Quantile20","Quantile60","Quantile70","Quantile80","Quantile95",\
                   "Quantile99","alert_time","no_of_days","noofmissdays","maxtime_before1805","maxtime")
less_data = less_origin_data.select("client_name","fund_id",'signal',"Quantile01","Quantile50",\
                                    "Quantile05","Quantile10","Quantile15","Quantile20","Quantile60",\
                                    "Quantile70","Quantile80","Quantile95","Quantile99","alert_time",\
                                    "no_of_days","noofmissdays","maxtime_before1805","maxtime")
data = unionAll(data,less_data)
origin_data = data.select('client_name','fund_id','signal','maxtime_before1805','Quantile50','alert_time')


mtmi_fund_level = data.withColumn('last_time',f.lit('19:04:00'))
mtmi_fund_level = mtmi_fund_level.withColumn('Quantile99',mtmi_fund_level.Quantile99.cast(TimestampType()))
mtmi_fund_level = mtmi_fund_level.withColumn('Quantile99',mtmi_fund_level.Quantile99 + f.expr("interval 1 seconds"))
mtmi_fund_level = mtmi_fund_level.withColumn('Quantile99',f.date_format('Quantile99','HH:mm:ss'))
mtmi_fund_level = mtmi_fund_level.withColumn('maxtime',mtmi_fund_level.maxtime.cast(TimestampType()))
mtmi_fund_level = mtmi_fund_level.withColumn('maxtime',mtmi_fund_level.maxtime + f.expr("interval 2 seconds"))
mtmi_fund_level = mtmi_fund_level.withColumn('maxtime',f.date_format('maxtime','HH:mm:ss'))
mtmi_fund_level = mtmi_fund_level.withColumn('noofmissdays',f.lit(0))


sqlctxt.sql("truncate table {}.pxo_navnow_mtmi_funds_predicted".format(database_m))
mtmi_fund_level.select("client_name","fund_id","Quantile01","Quantile50","Quantile05",\
                       "Quantile10","Quantile15","Quantile20","Quantile60","Quantile70",\
                       "Quantile80","Quantile95","Quantile99","alert_time","no_of_days",\
                       "noofmissdays","maxtime_before1805","maxtime",'signal').coalesce(1).write.mode("overwrite") \
                       .insertInto("{}.pxo_navnow_mtmi_funds_predicted".format(database_m))                    


mtmi_fund_level = melt(mtmi_fund_level, id_vars=['client_name','fund_id','signal'],\
                       value_vars=['Quantile50','Quantile70','Quantile95','maxtime',\
                                   'Quantile99','last_time'])
mtmi_fund_level = mtmi_fund_level.join(origin_data,on = ['client_name','fund_id','signal'],how = 'right')
mtmi_fund_level = mtmi_fund_level.withColumn('actual_time',f.lit("NA"))

sqlctxt.sql("truncate table {}.pxo_navnow_mtmi_funds_predicted_1".format(database_m))
mtmi_fund_level.select('client_name','fund_id','status','Time','Quantile50','maxtime_before1805','alert_time'\
                       ,'actual_time','signal').coalesce(1).write.insertInto("{}.pxo_navnow_mtmi_funds_predicted_1".format(database_m))
sc.stop()