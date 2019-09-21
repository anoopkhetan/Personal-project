"""*********************************************************************************************
 Script Name  :      navow_nds
 
 Project      :      Nav Now
 
 Author       :      kiran reddy kancharla

Spark streaming job to read real-time data from flume sink and after transformation 
write the results to Hive table.

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
import pandas as pd,time
import os,sys
from pyspark.sql import DataFrame

spark = SparkSession.\
        builder.\
        appName("pricing_funds").\
        getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlctxt = SQLContext(sc)

database_m = sys.argv[1]
database = sys.argv[4]
db_user = sys.argv[6]
db_pass = sys.argv[7]
db_server = sys.argv[8]
db_database = sys.argv[9]
env = sys.argv[10]


url="jdbc:oracle:thin:" + str(db_user) + "/" + str(db_pass) + "@" +str(db_server) + ":1528/" + str(db_database)

def unionAll(*dfs):
  return reduce(DataFrame.unionAll, dfs)

def load_predictions(database):
  global joined_data,fund_results,client_info,predictions  
  joined_data = sqlctxt.sql("select * from {}.pxo_navnow_pricing_funds_predicted_1".format(database_m)).coalesce(1)
  joined_data = joined_data.select('fund_id','client_name','predicted_time','max_predictedtime','new_prediction',\
                                   'actual_time','alert_time','signal').drop_duplicates()
  fund_results = sqlctxt.sql("select * from {}.pxo_navnow_pricing_funds_predicted".format(database_m)).coalesce(1)  
  client_info = fund_results.select('fund_id','client_name')
  client_info = client_info.withColumn('fund_id',f.trim(client_info.fund_id)).drop_duplicates().coalesce(1) 
  predictions = sqlctxt.sql("select client_identifier,bin,weekday,red_1,predicted,signal \
                            from {}.pxo_navnow_pricing_predicted".format(database_m)).coalesce(1).dropna()
  
def initial_run():  
    global first_run,joined_data
    if (first_run != 1):
      current_date = str(datetime.datetime.date(datetime.datetime.now()))
      origin_data = sqlctxt.sql("select * from {}.pxo_navnow_pricing_clientlevel \
                                 where nav_date = '{}'".format(database_m,current_date))     
      if (origin_data.rdd.isEmpty() is True):  
        print ("\n\n=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
        print ("initial run")
        origin_data = predictions.filter(predictions.bin == '16:00:00')
        origin_data = origin_data.withColumn('bin',origin_data.bin.cast(TimestampType()))
        origin_data = origin_data.withColumn('bin',origin_data.bin - f.expr('INTERVAL 1 minutes'))
        origin_data = origin_data.withColumn('bin',f.split("bin"," ").getItem(1))  
        origin_data = origin_data.withColumn('nav_date',f.lit(str(datetime.datetime.date(datetime.datetime.now())))) \
                   .withColumn('count',f.lit(0)) \
                   .withColumn('cumsum',f.lit(0)) \
                   .withColumn('error',f.lit(0))
        origin_data.coalesce(1).select('client_identifier','nav_date','bin','count','cumsum','predicted','red_1','error','signal')\
                               .write.mode("overwrite").insertInto("{}.pxo_navnow_pricing_clientlevel".format(database_m))                
    
        updated_data = joined_data.filter(joined_data.new_prediction > time.strftime("%H:%M:%S"))
        updated_data = updated_data.withColumn('nav_date',f.lit(datetime.datetime.today().strftime('%Y-%m-%d')))
        updated_data = updated_data.withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
        w = Window.partitionBy('FUND_ID').orderBy()
        updated_data = f.broadcast(updated_data).withColumn('min_bin', f.min('new_prediction').over(w))\
                                 .where(f.col('new_prediction') == f.col('min_bin')).drop('min_bin')
        updated_data = updated_data.withColumn('status',f.lit('Awaiting'))
        
        if (env == 'UAT' or env == 'PROD'):
        
          geneos_data = updated_data 
          geneos_data = geneos_data.withColumnRenamed('predicted_time','predicted_completed_time')
          geneos_data = geneos_data.withColumnRenamed('max_predictedtime','maxpredicted_completion_time')
          geneos_data = geneos_data.withColumnRenamed('new_prediction','predicted_completion_time')
          geneos_data = geneos_data.withColumnRenamed('actual_time','actual_completion_time')  
          geneos_data.select('fund_id','nav_date','client_name', \
                'predicted_completed_time',\
                'maxpredicted_completion_time','predicted_completion_time',\
                'actual_completion_time','alert_time','updated_time',\
                'status','signal').coalesce(1).write.mode("append").format('jdbc') \
                .options(\
                 url=url,\
                 dbtable='navnow_pricing_table',\
                 driver="oracle.jdbc.OracleDriver"\
                 ).save()
        
        updated_data.coalesce(1).select('fund_id','nav_date','client_name','predicted_time','max_predictedtime',\
                                        'new_prediction','actual_time','updated_time','alert_time','status','signal')\
                                .write.insertInto("{}.pxo_navnow_pricing_fundlevel".format(database_m))

        first_run = 1
        print ("=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")

def empty_rdd(rdd): 
  global joined_data,fund_results,predictions
  if (rdd.isEmpty() is True and time.strftime("%H:%M:%S") >= '16:00:00'):    
    print ("\n\n=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
    print ("no data available") 
    
    current_date = str(datetime.datetime.date(datetime.datetime.now()))
    current = str(datetime.datetime.now() - datetime.timedelta(minutes = 1)).split(" ")[1]
    old_data = joined_data.filter(joined_data.new_prediction > current)
    
    origin_data = sqlctxt.sql("select * from {}.pxo_navnow_pricing_clientlevel where nav_date = '{}'".format(database_m,current_date))
    w = Window.partitionBy('nav_date')
    origin_data = f.broadcast(origin_data).withColumn('max_bin', f.max('bin').over(w))\
                             .where(f.col('bin') == f.col('max_bin')).drop('max_bin')
    origin_data = origin_data.withColumn('bin',origin_data.bin.cast(TimestampType()))
    origin_data = origin_data.withColumn("bin",(f.unix_timestamp(f.col("bin")) + f.lit(60)).cast(TimestampType()))
    origin_data = origin_data.withColumn('bin',f.split("bin"," ").getItem(1))
    origin_data = origin_data.drop('predicted','red_1','error')
    origin_data = origin_data.withColumn('count',f.lit(0))
    origin_data = f.broadcast(origin_data).join(predictions,on = ['bin','client_identifier','signal'],how = 'inner')
    origin_data = origin_data.withColumn('error',origin_data.predicted - origin_data.cumsum)
    origin_data.coalesce(1).select('client_identifier','nav_date','bin','count','cumsum','predicted','red_1','error','signal')\
                           .write.mode("overwrite").insertInto("{}.pxo_navnow_pricing_clientlevel".format(database_m))

    
    w = Window.partitionBy('FUND_ID').orderBy()
    old_data = f.broadcast(old_data).withColumn('min_bin', f.min('new_prediction').over(w))\
                             .where(f.col('new_prediction') == f.col('min_bin')).drop('min_bin')    
    processed_data = sqlctxt.sql("select * from {}.pxo_navnow_pricing_fundlevel where nav_date ='{}' \
                                  and Actual_completion_Time != 'NA'".format(database_m,current_date))
    old_data = old_data.join(f.broadcast(processed_data), on = "fund_id", how = "leftanti")
    old_data = old_data.withColumn('nav_date',f.lit(datetime.datetime.today().strftime('%Y-%m-%d')))
    old_data = old_data.withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
    old_data = old_data.withColumn("new_prediction", f.when(old_data["new_prediction"] == '19:04:00', \
                                                            old_data['max_predictedtime']).otherwise(old_data["new_prediction"]))
    old_data = old_data.withColumn('status1',f.lit('Awaiting'))    
    old_data = old_data.withColumn('status',f.when((old_data['actual_time'] != 'NA') & \
                                                   (old_data['predicted_time'] >= old_data['actual_time']),'on Time')\
                                                   .otherwise(old_data['status1']))
    old_data = old_data.withColumn('status',f.when((old_data['actual_time'] != 'NA') & \
                                                   (old_data['predicted_time'] < old_data['actual_time']),'Delayed Completion')\
                                                   .otherwise(old_data['status']))
    old_data = old_data.withColumn('status',f.when((old_data['actual_time'] != 'NA') & \
                                                   (old_data['max_predictedtime'] < old_data['actual_time']),'Late Completion')\
                                                   .otherwise(old_data['status']))
    old_data = old_data.withColumn('status',f.when((old_data['actual_time'] == 'NA') & \
                                                   (old_data['predicted_time'] > current),'Awaiting')\
                                                   .otherwise(old_data['status']))
    old_data = old_data.withColumn('status',f.when((old_data['actual_time'] == 'NA') & \
                                                   (old_data['predicted_time'] < current),'Delayed').\
                                                   otherwise(old_data['status']))
    old_data = old_data.withColumn('status',f.when((old_data['actual_time'] == 'NA') & \
                                                  (old_data['max_predictedtime'] < current),'Late')\
                                                  .otherwise(old_data['status']))
    
    if (env == 'UAT' or env == 'PROD'):
        geneos_data = old_data 
        geneos_data = geneos_data.withColumnRenamed('predicted_time','predicted_completed_time')
        geneos_data = geneos_data.withColumnRenamed('max_predictedtime','maxpredicted_completion_time')
        geneos_data = geneos_data.withColumnRenamed('new_prediction','predicted_completion_time')
        geneos_data = geneos_data.withColumnRenamed('actual_time','actual_completion_time')    
        geneos_data.select('fund_id','nav_date','client_name', \
            'predicted_completed_time',\
            'maxpredicted_completion_time','predicted_completion_time',\
            'actual_completion_time','alert_time','updated_time',\
            'status','signal').coalesce(1).write.mode("append").format('jdbc') \
            .options(\
             url=url,\
             dbtable='navnow_pricing_table',\
             driver="oracle.jdbc.OracleDriver"\
             ).save()
    
    old_data.coalesce(1).select('fund_id','nav_date','client_name','predicted_time','max_predictedtime','new_prediction',\
                                'actual_time','updated_time','alert_time','status','signal')\
                        .write.insertInto("{}.pxo_navnow_pricing_fundlevel".format(database_m))
    print ("=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
    if (time.strftime("%H:%M:%S") > '19:01:00'):
      sc.stop()
  
def process(rdd):  
    global joined_data,fund_results,predictions
    if (rdd.isEmpty() is False and time.strftime("%H:%M:%S") >= '16:00:00'):    
      print ("\n\n=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
      print ("data available")
      
      current = str(datetime.datetime.now() - datetime.timedelta(minutes = 1)).split(" ")[1]    
      current_date = str(datetime.datetime.date(datetime.datetime.now()))
      processed_data = sqlctxt.sql("select * from {}.pxo_navnow_pricing_fundlevel where nav_date ='{}' \
                                    and Actual_completion_Time != 'NA'".format(database_m,current_date))
      old_data = joined_data.filter(joined_data.new_prediction > current)      
      
      
      # Convert RDD[String] to DataFrame
      data = spark.read.json(rdd)
      data = data.select('nav_date','fund_id','event_ts').drop_duplicates()
      data.coalesce(1).select('nav_date','fund_id','event_ts')\
                      .write.insertInto("{}.pxo_navnow_pricing_raw_data".format(database))
      data = data.groupBy('nav_date','fund_id').agg(f.min('event_ts').alias('event_ts'))
      data = data.withColumn('fund_id',f.trim(data.fund_id))
      new_data = data.join(client_info,on= 'fund_id',how = 'inner') 
      new_data = new_data.dropna()      
      new_data = new_data.withColumnRenamed('client_name','client_identifier') 
      new_data = new_data.join(f.broadcast(processed_data), on = "fund_id", how ="leftanti")           
      data = new_data.withColumn('event_ts',new_data.event_ts.cast(TimestampType())) \
                 .withColumn('nav_date',new_data.nav_date.cast(DateType())) \
                 .withColumn('event_ts',f.date_format('event_ts', 'HH:mm:ss'))\
                 .withColumn('Predicted_completion_Time',f.lit('NA'))\
                 .withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
      data = data.filter(data.event_ts >= '16:00:00')
      data = data.join(fund_results.select('fund_id','client_name','predicted_time','maxtime_before1805','alert_time','signal'),\
                       on = 'fund_id',how = 'left')
      data = data.dropna()
      data = data.withColumn('nav_date',f.lit(datetime.datetime.today().strftime('%Y-%m-%d')))
      data = data.withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
      data = data.withColumn('Predicted_completion_Time',data.predicted_time)
      data = data.withColumn("Predicted_completion_Time", f.when(data["Predicted_completion_Time"] == '19:04:00',\
                                                                 data['maxtime_before1805']).otherwise(data["Predicted_completion_Time"]))
      data = data.select('fund_id','nav_date','client_name','predicted_time','maxtime_before1805','Predicted_completion_Time',\
                         'event_ts','updated_time','alert_time','signal')           
      w = Window.partitionBy('fund_id').orderBy()
      old_data = f.broadcast(old_data).withColumn('min_bin', f.min('new_prediction').over(w))\
                               .where(f.col('new_prediction') == f.col('min_bin')).drop('min_bin')          
      old_data = old_data.join(f.broadcast(data), on = "fund_id",  how = "leftanti")
      old_data = old_data.join(f.broadcast(processed_data), on = "fund_id", how ="leftanti")      
      old_data = old_data.withColumn('nav_date',f.lit(datetime.datetime.today().strftime('%Y-%m-%d')))
      old_data = old_data.withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))    
      old_data = old_data.withColumn("new_prediction", f.when(old_data["new_prediction"] == '19:04:00', old_data['max_predictedtime'])\
                                     .otherwise(old_data["new_prediction"]))    
      old_data = old_data.select('fund_id','nav_date','client_name','predicted_time','max_predictedtime','new_prediction','actual_time',\
                                 'updated_time','alert_time','signal')
      old_data = unionAll(old_data,data)
      old_data = old_data.withColumn('status1',f.lit('Awaiting'))    
      old_data = old_data.withColumn('status',f.when((old_data['actual_time'] != 'NA') & \
                                    (old_data['predicted_time'] >= old_data['actual_time']),'on Time')\
                                     .otherwise(old_data['status1']))
      old_data = old_data.withColumn('status',f.when((old_data['actual_time'] != 'NA') & \
                                    (old_data['predicted_time'] < old_data['actual_time']),'Delayed Completion')\
                                     .otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['actual_time'] != 'NA') & \
                                    (old_data['max_predictedtime'] < old_data['actual_time']),'Late Completion')\
                                     .otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['actual_time'] == 'NA') & \
                                    (old_data['predicted_time'] > current),'Awaiting')\
                                     .otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['actual_time'] == 'NA') & \
                                    (old_data['predicted_time'] < current),'Delayed')\
                                     .otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['actual_time'] == 'NA') & \
                                    (old_data['max_predictedtime'] < current),'Late')\
                                     .otherwise(old_data['status']))
      
      if (env == 'UAT' or env == 'PROD'):
        geneos_data = old_data 
        geneos_data = geneos_data.withColumnRenamed('predicted_time','predicted_completed_time')
        geneos_data = geneos_data.withColumnRenamed('max_predictedtime','maxpredicted_completion_time')
        geneos_data = geneos_data.withColumnRenamed('new_prediction','predicted_completion_time')
        geneos_data = geneos_data.withColumnRenamed('actual_time','actual_completion_time')
        geneos_data.select('fund_id','nav_date','client_name', \
            'predicted_completed_time',\
            'maxpredicted_completion_time','predicted_completion_time',\
            'actual_completion_time','alert_time','updated_time',\
            'status','signal').coalesce(1).write.mode("append").format('jdbc') \
            .options(\
             url=url,\
             dbtable='navnow_pricing_table',\
             driver="oracle.jdbc.OracleDriver"\
             ).save()
      
      old_data.coalesce(1).select('fund_id','nav_date','client_name','predicted_time','max_predictedtime','new_prediction',\
                                  'actual_time','updated_time','alert_time','status','signal')\
                          .write.insertInto("{}.pxo_navnow_pricing_fundlevel".format(database_m))
      
            
      processed_data = sqlctxt.sql("select * from {}.pxo_navnow_pricing_fundlevel where actual_completion_time != 'NA'".format(database_m)).coalesce(1).drop_duplicates()      
      data = processed_data.filter(processed_data.nav_date == current_date)
      data = data.withColumnRenamed('actual_completion_time','event_ts')
      
      data = data.withColumn('event_ts',data.event_ts.cast(TimestampType()))
      data = data.withColumn('event_ts',data.event_ts - f.expr("interval 1 second"))
      data = data.withColumn('nav_date',data.nav_date.cast(DateType()))       
      data = data.withColumn("bin",(f.unix_timestamp(f.col("event_ts")) - (f.unix_timestamp(f.col("event_ts")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
      data = data.withColumn('bin',f.date_format('bin', 'HH:mm:ss'))  
      data = data.withColumnRenamed('client_name','client_identifier')
      data = data.groupby('nav_date','client_identifier','bin','signal').agg(f.count('fund_id').alias('count'))               
      data = f.broadcast(data).join(predictions,on = ['client_identifier','bin','signal'],how = 'outer').fillna(0)
      data = data.withColumn('nav_date',f.lit(str(datetime.date.today())))           
      origin_data = data.filter(data.bin <= current)
      windowval = Window.partitionBy('client_identifier','signal')\
                        .orderBy('bin')\
                        .rangeBetween(Window.unboundedPreceding, 0)
      origin_data = f.broadcast(origin_data).withColumn('cumsum', f.sum('count').over(windowval))
      origin_data = origin_data.withColumn('error',origin_data.predicted - origin_data.cumsum).drop_duplicates()     
      origin_data.coalesce(1).select('client_identifier','nav_date','bin','count','cumsum','predicted','red_1','error','signal')\
                             .write.mode("overwrite").insertInto("{}.pxo_navnow_pricing_clientlevel".format(database_m))              
      print ("=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
      if (time.strftime("%H:%M:%S") > '19:01:00'):
        sc.stop()
    
if __name__ == "__main__":
  ## initiate a streaming context if time passes 16:00:20 
  global joined_data,first_run,processed_data,client_info,predictions
  global client_results,fundr_results    
  first_run = 0
  load_predictions(database_m)
  initial_run()  
  ssc = StreamingContext(sc,60)
  addresses = [(sys.argv[2],int(sys.argv[3]))]
  nds_sink = FlumeUtils.createPollingStream(ssc,addresses)
  batch_data = nds_sink.map(lambda x: json.dumps(x[0]))    
  ##calling process to perform actions 
  batch_data.foreachRDD(process)
  batch_data.foreachRDD(empty_rdd)
  ssc.start()
  ssc.awaitTerminationOrTimeout(20000)
  sc.stop()