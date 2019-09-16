"""*********************************************************************************************
 Script Name  :      nds_training.py
 
 Project      :      Nav Now
 
 author       :      kiran reddy



Script for MTMI predictions based on historical data

The scipt will be called from the wrapper file.
***************************************************************************************************"""


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import json,datetime,time
from pyspark.sql import Window
import pandas as pd,time,numpy as np
from pyspark.conf import SparkConf
import os,sys


print ("time:",time.strftime("%H:%M:%S"))
spark = SparkSession.\
        builder.\
        appName("nds_training").\
        getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlctxt = SQLContext(sc)


data = sqlctxt.sql("select * from " + str(sys.argv[1]) + ".pxo_nds_navdel_1")
data = data.filter(data.as_of_date != 'AS_OF_DATE')
data = data.withColumnRenamed('as_of_date','nav_date')

data = data.select('Date_Identifier','nav_date','event_time_numeric','FUND_ID','Client_Name','ticker')

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

data = data.withColumn('nav_date', f.from_unixtime(f.unix_timestamp('nav_date', 'MM/dd/yyy')).cast(DateType()))
#data = data.withColumn("Date_Time", f.from_unixtime(f.unix_timestamp(data.Date_Time, "dd/MM/yy HH:mm")).cast(TimestampType()))
data = data.withColumn("Date_Time",udf_time('event_time_numeric').cast(TimestampType()))

data= data.filter(data.nav_date >= '2018-05-01') 

max_date = '2018-07-31'

data= data.filter(data.nav_date <= max_date) 

dt_truncated = ((f.round(f.unix_timestamp(f.col("Date_Time")) / 60) * 60 + 60).cast(TimestampType()))
data = data.withColumn("bin",dt_truncated)  
data = data.withColumn("bin",(f.unix_timestamp(f.col("bin")) - (f.unix_timestamp(f.col("bin")) % f.lit(300)) + f.lit(240)).cast(TimestampType()))
data = data.withColumn('bin',f.split("bin"," ").getItem(1))   

data = data.withColumn('weekday',(f.date_format('nav_date','u') - f.lit(1)).cast(IntegerType()))


client_info = sqlctxt.sql("select * from " + str(sys.argv[1]) + ".PXO_NAVNOW_FUND_CLIENT_MAPPING_1")
client_info = client_info.select('fund_id','client_name','ticker').drop_duplicates()
client_info = client_info.withColumnRenamed('client_name','Client_Identifier')
data = data.join(client_info,on = 'ticker',how = 'left').drop('Client_Name')


data = data.groupBy('nav_date','Client_Identifier','bin','weekday',f.window("Date_Time", "5 minutes")).agg({'Date_Time':'count'}).withColumnRenamed('count(Date_Time)','Count')
data = data.drop('window').dropna()


temp = data.groupBy('nav_date','Client_Identifier','weekday').max('weekday')
testing = temp.drop('max(weekday)')
temp = sc.parallelize([i for i in range(0,38)])
temp = temp.map(lambda x: (x, )).toDF(['bins'])

initial_time = '16:00:00'
temp = temp.withColumn('bin',f.lit(initial_time).cast(TimestampType()))

def bin_time(bins,bin):
  import pandas as pd
  import datetime
  return bin + datetime.timedelta(minutes = bins * 5 - 1) 
udf_bin_time = f.udf(bin_time,TimestampType())


new_df = temp.withColumn('bin',udf_bin_time(temp.bins,temp.bin)).drop('bins')
new_df = new_df.withColumn('bin',f.split("bin"," ").getItem(1))  

temp = testing.crossJoin(new_df)
data = data.join(temp, on= ['Client_Identifier','bin','nav_date','weekday'],how= 'outer')
data = data.fillna(0)



windowval = Window.partitionBy('nav_date','Client_Identifier','weekday').orderBy('Bin').rangeBetween(Window.unboundedPreceding, 0)
data = data.withColumn('cumsum', f.sum('count').over(windowval))
df_bin_sorted = data.withColumn('cumsum', data.cumsum.cast(IntegerType()))


df_bin_sorted = df_bin_sorted.withColumnRenamed('Client_Identifier','client_identifier')


w = Window().partitionBy('client_identifier','nav_date')
df_bin_sorted = df_bin_sorted.select("*", f.collect_list("cumsum").over(w).alias("group_cumsum"))


def cumsumpct_calculate(num,maximum):
  return num / float(np.max(maximum))
udf_cum_pct = f.udf(cumsumpct_calculate,FloatType())

df_bin_sorted = df_bin_sorted.withColumn('cumsumpct',f.format_number(udf_cum_pct(df_bin_sorted.cumsum,df_bin_sorted.group_cumsum),4))                                               
df_bin_sorted = df_bin_sorted.drop('group_cumsum')


def percent_calculator(values,rank):
  values = [float(i) for i in values]
  value = []
  for i in range(len(values)):
    value.append(values[i])
    value.sort()
  if (rank == 50):
    length = len(value)
    if (length % 2 == 0):
      med = (value[int(length/2) -1] + value[int(length/2)])/ float(2)
    else:
      med = value[length/2]
    return med
  else:
    values.sort()
    import math
    R = (rank / float(100 )) * (len(value) - 1)
    if (type(R) is int):
      IR = int(R)
      return IR
    else:
      FR = math.modf(R)[0]
      high = int(math.ceil(R))
      low = int(math.floor(R))
      IR = value[low] + (value[high] - value[low]) * FR
    return IR
udf_percent_rank = f.udf(percent_calculator,FloatType())


def find_median(values_list):
  median = np.median(values_list) #get the median of values in a list in each row
  return round(float(median),2)

median_finder = f.udf(find_median,FloatType())



####
df_predicted_pct = df_bin_sorted.groupby('client_identifier','weekday','Bin').agg(f.collect_list("cumsumpct").alias("group_cumsumpct"))
df_predicted_pct = df_predicted_pct.withColumn('Red_1_pct',udf_percent_rank('group_cumsumpct',f.lit(1)))
df_predicted_pct = df_predicted_pct.withColumn('Red_5_pct',udf_percent_rank('group_cumsumpct',f.lit(5)))
df_predicted_pct = df_predicted_pct.withColumn('Red_10_pct',udf_percent_rank('group_cumsumpct',f.lit(10)))
df_predicted_pct = df_predicted_pct.withColumn('Red_20_pct',udf_percent_rank('group_cumsumpct',f.lit(20)))
df_predicted_pct = df_predicted_pct.withColumn('Predicted_pct',udf_percent_rank('group_cumsumpct',f.lit(50)))
df_predicted_pct = df_predicted_pct.withColumn('Green_pct',udf_percent_rank('group_cumsumpct',f.lit(95)))



end_volumes = df_bin_sorted.groupby('client_identifier','weekday','nav_date').agg(f.max('cumsum').alias('cumsum'))
df_end_volumes = end_volumes.groupby('client_identifier','weekday').agg(f.collect_list("cumsum").alias("cumsum"))
df_end_volumes = df_end_volumes.withColumn("DEV",median_finder("cumsum")).drop('cumsum')

df_predicted = df_predicted_pct.join(df_end_volumes, on = ['client_identifier','weekday'], how = 'left')

df_predicted = df_predicted.withColumn('Red_1',f.round(df_predicted.Red_1_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Red_5',f.round(df_predicted.Red_5_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Red_10',f.round(df_predicted.Red_10_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Red_20',f.round(df_predicted.Red_20_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Predicted',f.round(df_predicted.Predicted_pct * df_predicted.DEV))
df_predicted = df_predicted.withColumn('Green',f.round(df_predicted.Green_pct * df_predicted.DEV))

def initial_bin(bin):
  if bin == '15:59:00':
    return '16:00:00'
  else:
    return bin
bin_udf = f.udf(initial_bin)
df_predicted = df_predicted.withColumn('bin',bin_udf('bin'))

df_results = df_predicted.select('client_identifier','weekday','Red_1','Red_5','Red_10','Red_20', 'Predicted','bin')
sqlctxt.sql("truncate table " + str(sys.argv[2])+".pxo_navnow_nds_predicted")
df_results.select('client_identifier','weekday','Red_1','Red_5','Red_10','Red_20', 'Predicted','bin').write.mode("overwrite").insertInto(str(sys.argv[2]) + ".pxo_navnow_nds_predicted")

predicted_overall = df_results.groupby('Bin','weekday').agg(f.sum("Red_1").alias("Red_1") ,\
                                                            f.sum("Red_5").alias("Red_5") ,\
                                                            f.sum("Red_10").alias("Red_10") ,\
                                                            f.sum("Red_20").alias("Red_20") ,\
                                                            f.sum("Predicted").alias("Predicted") )

predicted_overall = predicted_overall.withColumn('bin',bin_udf('bin'))