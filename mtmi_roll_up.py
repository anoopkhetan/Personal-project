from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.sql import Window
import json,datetime
import threading
import json,datetime
from httplib import HTTPConnection
import urllib,os,time
import numpy as np


spark = SparkSession.\
        builder.\
        appName("mtmi_fund_rollup").\
        getOrCreate()
      
sc = spark.sparkContext
sqlctxt = SQLContext(sc)

database_m = sys.argv[1]

data = sqlctxt.sql("select client_name,fund_id,predicted_time,signal from {}.pxo_navnow_mtmi_funds_predicted".format(database_m))

data = data.withColumn('predicted_time',data.predicted_time.cast(TimestampType()))
data = data.withColumn('predicted_time',data.predicted_time.cast(TimestampType()))
data = data.withColumn('predicted_time',data.predicted_time - f.expr("interval 1 second"))
data = data.withColumn("predicted_time",(f.unix_timestamp(f.col("predicted_time")) - (f.unix_timestamp(f.col("predicted_time")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
data = data.withColumn('weekday',f.lit(1))
data = data.groupBy('client_name','weekday','predicted_time','signal',f.window("predicted_time", "1 minutes")).agg({'fund_id':'count'}).withColumnRenamed('count(fund_id)','Count')
data = data.drop('window').dropna()
data = data.withColumn('predicted_time',f.split("predicted_time"," ").getItem(1))  


data_quant99 = sqlctxt.sql("select client_name,fund_id,max_predictedtime,signal from {}.pxo_navnow_mtmi_funds_predicted".format(database_m))
data_quant99 = data_quant99.withColumn('max_predictedtime',data_quant99.max_predictedtime.cast(TimestampType()))
data_quant99 = data_quant99.withColumn('max_predictedtime',data_quant99.max_predictedtime - f.expr("interval 1 second"))
data_quant99 = data_quant99.withColumn("max_predictedtime",(f.unix_timestamp(f.col("max_predictedtime")) - (f.unix_timestamp(f.col("max_predictedtime")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
data_quant99 = data_quant99.withColumn('weekday',f.lit(1))
data_quant99 = data_quant99.groupBy('client_name','weekday','max_predictedtime','signal',f.window("max_predictedtime", "1 minutes")).agg({'fund_id':'count'}).withColumnRenamed('count(fund_id)','quant_Count')
data_quant99 = data_quant99.drop('window').dropna()
data_quant99 = data_quant99.withColumn('max_predictedtime',f.split("max_predictedtime"," ").getItem(1))  
data_quant99 = data_quant99.withColumnRenamed('max_predictedtime','predicted_time')


temp = data.groupBy('client_name','weekday','signal').max('weekday')
testing = temp.drop('max(weekday)')
temp = sc.parallelize([i for i in range(0,181)])
temp = temp.map(lambda x: (x, )).toDF(['bins'])

initial_time = '16:00:00'
temp = temp.withColumn('bin',f.lit(initial_time).cast(TimestampType()))

def bin_time(bins,bin):
  import pandas as pd
  import datetime
  return bin + datetime.timedelta(minutes = int(bins)) 
udf_bin_time = f.udf(bin_time,TimestampType())


new_df = temp.withColumn('bin',udf_bin_time(temp.bins,temp.bin)).drop('bins')
new_df = new_df.withColumn('predicted_time',f.split("bin"," ").getItem(1)).drop('bin') 

temp = testing.crossJoin(new_df)
data = data.join(temp, on= ['client_name','predicted_time','weekday','signal'],how= 'outer')
data = data.fillna(0)
data_quant99 = data_quant99.join(temp, on= ['client_name','predicted_time','weekday','signal'],how= 'outer')
data_quant99 = data_quant99.fillna(0)
data = data.join(data_quant99,on = ['client_name','weekday','predicted_time','signal'])



data = data.withColumn('predicted_time',f.regexp_replace('predicted_time','15:59:00','16:00:00'))

windowval = Window.partitionBy('client_name','signal').orderBy('predicted_time').rangeBetween(Window.unboundedPreceding, 0)
data = data.withColumn('predictd_cumsum', f.sum('count').over(windowval))
data = data.withColumn('predictd_cumsum', data.predictd_cumsum.cast(IntegerType()))
data = data.withColumn('quant_cumsum', f.sum('quant_count').over(windowval))
data = data.withColumn('quant_cumsum', data.quant_cumsum.cast(IntegerType()))


data = data.withColumnRenamed('predicted_time','bin')
data = data.withColumn('red_5',f.lit(0))
data = data.withColumn('red_10',f.lit(0))
data = data.withColumn('red_20',f.lit(0))
data = data.withColumn('weekday',f.lit(1))

sqlctxt.sql("truncate table {}.pxo_navnow_mtmi_predicted".format(database_m))
data.select('client_name','weekday','quant_cumsum','red_5','red_10','red_20','predictd_cumsum','bin','signal')\
    .coalesce(1).write.insertInto("{}.pxo_navnow_mtmi_predicted".format(database_m))
sc.stop()