"""*********************************************************************************************
 Script Name  :      predict_report.py
 
 Project      :      Nav Now
 
 Author       :      kiran reddy kancharla



python job for generating the miss report

The scipt will be called from the wrapper file. at the end of the day.
***************************************************************************************************"""

from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import json,datetime,time
from pyspark.sql import Window
import pandas as pd,time
import os,sys

spark = SparkSession.\
        builder.\
        appName("miss_report").\
        getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlctxt = SQLContext(sc)

historical_data = sqlctxt.sql("select * from " +str(sys.argv[1]) +".pxo_navnow_predict_report")
max_date = historical_data.select(f.max('nav_date')).collect()[0][0]


client_mapping = sqlctxt.sql("select trim(ticker) as ticker from" + str(sys.argv[2]) + ".PXO_NAVNOW_FUND_CLIENT_MAPPING_1")
tickers = client_mapping.select('ticker').collect()
tickers = [tickers[i][0] for i in range(len(tickers))]

if max_date is not None:
  nds_data = sqlctxt.sql("select nav_date,client_name,fund_id,ticker_id,actual_completion_time from " + str(sys.argv[1]) + ".pxo_navnow_nds_fundlevel_hist where actual_completion_time != 'NA'")
  nds_data = nds_data.filter(nds_data.nav_date > max_date)
  
else:
  nds_data = sqlctxt.sql("select nav_date,client_name,fund_id,ticker_id,actual_completion_time from " + str(sys.argv[1]) + ".pxo_navnow_nds_fundlevel_hist where actual_completion_time != 'NA'")

nds_data = nds_data.filter(nds_data.ticker_id.isin(tickers))
nds_data =  nds_data.filter(nds_data.actual_completion_time > '18:05:00')


nds_predictions = sqlctxt.sql("select * from " + str(sys.argv[1]) + ".pxo_navnow_nds_ticker_prediction_1")
nds_predictions = nds_predictions.filter(nds_predictions.alert.isin(['Definitely Not','Probably Not','Probably','Probably Yes','Definitely Yes']))
#nds_predictions = nds_predictions.withColumn('random',f.monotonically_increasing_id())
nds_predictions = nds_predictions .groupby('ticker','fund_id','client_name','predicted_completed_time','maxpredicted_completion_time','alert_time').pivot("alert").agg(f.max("predicted_completion_time").alias('predicted_completion_time'))
nds_predictions = nds_predictions.withColumnRenamed('ticker','ticker_id')

nds_predictions = nds_predictions.join(nds_data,on = ['client_name','ticker_id','fund_id'],how = 'right')