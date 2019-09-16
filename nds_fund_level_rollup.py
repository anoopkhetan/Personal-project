from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
import pyspark.sql.functions as f
from pyspark.sql import Window
import json,datetime
import os,time
import numpy as np


spark = SparkSession.\
        builder.\
        appName("nds_fund_rollup").\
        getOrCreate()
      
sc = spark.sparkContext
sqlctxt = SQLContext(sc)

database = sys.argv[1]

data = sqlctxt.sql("select client_name,ticker,predicted_time from " + str(database) + ".pxo_navnow_ticker_prediction_quantiles1")
data = data.withColumn('predicted_time',data.predicted_time.cast(TimestampType()))
data = data.withColumn('predicted_time',data.predicted_time - f.expr("interval 1 second"))
data = data.withColumn("predicted_time",(f.unix_timestamp(f.col("predicted_time")) - (f.unix_timestamp(f.col("predicted_time")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
data = data.withColumn('weekday',f.lit(1))
data = data.groupBy('client_name','weekday','predicted_time',f.window("predicted_time", "1 minutes")).agg({'ticker':'count'}).withColumnRenamed('count(ticker)','Count')
data = data.drop('window').dropna()
data = data.withColumn('predicted_time',f.split("predicted_time"," ").getItem(1))  


data_quant99 = sqlctxt.sql("select client_name,ticker,quantile99 from " + str(database) + ".pxo_navnow_ticker_prediction_quantiles1")
data_quant99.count()
data_quant99 = data_quant99.withColumn('quantile99',data_quant99.quantile99.cast(TimestampType()))
data_quant99 = data_quant99.withColumn('quantile99',data_quant99.quantile99 - f.expr("interval 1 second"))
data_quant99 = data_quant99.withColumn("quantile99",(f.unix_timestamp(f.col("quantile99")) - (f.unix_timestamp(f.col("quantile99")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
data_quant99 = data_quant99.withColumn('weekday',f.lit(1))
data_quant99 = data_quant99.groupBy('client_name','weekday','quantile99',f.window("quantile99", "1 minutes")).agg({'ticker':'count'}).withColumnRenamed('count(ticker)','quant_Count')
data_quant99 = data_quant99.drop('window').dropna()
data_quant99 = data_quant99.withColumn('quantile99',f.split("quantile99"," ").getItem(1))  
data_quant99 = data_quant99.withColumnRenamed('quantile99','predicted_time')



temp = data.groupBy('client_name','weekday').max('weekday')
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
data = data.join(temp, on= ['client_name','predicted_time','weekday'],how= 'outer')
data = data.fillna(0)
data_quant99 = data_quant99.join(temp, on= ['client_name','predicted_time','weekday'],how= 'outer')
data_quant99 = data_quant99.fillna(0)
data = data.join(data_quant99,on = ['client_name','weekday','predicted_time'])

data = data.withColumn('predicted_time',f.regexp_replace('predicted_time','15:59:00','16:00:00'))

windowval = Window.partitionBy('client_name').orderBy('predicted_time').rangeBetween(Window.unboundedPreceding, 0)
data = data.withColumn('predictd_cumsum', f.sum('count').over(windowval))
data = data.withColumn('predictd_cumsum', data.predictd_cumsum.cast(IntegerType()))
data = data.withColumn('quant_cumsum', f.sum('quant_count').over(windowval))
data = data.withColumn('quant_cumsum', data.quant_cumsum.cast(IntegerType()))
data = data.withColumnRenamed('predicted_time','bin')
data = data.withColumn('red_5',f.lit(0))
data = data.withColumn('red_10',f.lit(0))
data = data.withColumn('red_20',f.lit(0))
data = data.withColumn('weekday',f.lit(1))


sqlctxt.sql("truncate table " +str(database) +".pxo_navnow_nds_predicted")
data.select('client_name','weekday','quant_cumsum','red_5','red_10','red_20','predictd_cumsum','bin')\
    .coalesce(1).write.insertInto(str(database) +".pxo_navnow_nds_predicted")
sc.stop()