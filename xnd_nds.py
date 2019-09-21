"""#################################################################################################
***		                                                                                         ***
***		 Script Name  :      bfds.py                                                             ***
***		 Project      :      BFDS                                                                ***
***		                                                                                         ***
***		 Author       :      kiran reddy kancharla                                               ***
***		                                                                                         ***
***		 Spark streaming job to read real-time data from oracle database                         ***
***		 and after transformation write the results to Hive table.                               ***
***		                                                                                         ***
***		 The scipt will be called from the wrapper file.                                         ***
#################################################################################################"""
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import datetime,time,logging,os,sys
from pyspark.sql import Window
import time
from pyspark.sql import DataFrame

spark = SparkSession.\
        builder.\
        config("spark.jars", "/opt/cloudera/parcels/CDH/lib/sqoop/lib/ojdbc6.jar").\
        appName("NDS-JOB").\
        getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("INFO")
sqlctxt = SQLContext(sc)


database = sys.argv[1]
log_path = sys.argv[2]
user = sys.argv[3]
passwd = sys.argv[4]
properties = sqlctxt.sql("select * from {}.pxo_navnow_db_conn_dets where App_name = 'NDS'".format(database))
properties = properties.collect()
server = str(properties[0][1])
db_name = str(properties[0][2])
schema = str(properties[0][3])
url = "jdbc:oracle:thin:{}/{}@{}:1521/{}".format(str(user),str(passwd),str(server),str(db_name))
to_date = ''.join(e for e in str(datetime.date.today()) if e.isalnum())
nds_sql = ("(SELECT TO_CHAR(S.AS_OF_DATE, 'yyyy-mm-dd') AS AS_OF_DATE" 
                 " , S.EVENT_ID  "         
                 " , S.FUND_ID  "
                 " , cp.TICKER "
                 " , TO_CHAR(S.SEND_TIME, 'yyyy-mm-dd hh24:mi:ss') AS SEND_TIME "
				 " FROM NGAPROD.NASDAQ_MESSAGE_DELIVERY_STATE S, NGAPROD.CLASS_PROFILE cp"
                 " WHERE TO_CHAR(as_of_date, 'yyyyMMdd') = ""'"+ to_date + "'"" "
				 " AND S.BASIS_ID = cp.NDS_BASIS_ID"
                 " AND S.FUND_ID = cp.FUND_ID) " 
                 " nds")
db_user = sys.argv[9]
db_pass = sys.argv[10]
db_server = sys.argv[11]
db_database = sys.argv[12]
env = sys.argv[13]
oracle_url="jdbc:oracle:thin:" + str(db_user) + "/" + str(db_pass) + "@" +str(db_server) + ":1528/" + str(db_database)



def unionAll(*dfs):
  return reduce(DataFrame.unionAll, dfs)

def connection(sql_query):
  data=sqlctxt.read.format('jdbc').options(
        url=url,
        dbtable=sql_query,
        driver="oracle.jdbc.OracleDriver"
        ).load()
  return data

def load_predictions(database):
  global client_info,predictions,joined_data,ticker_results;
  joined_data = sqlctxt.sql("select ticker,fund_id,client_name,Predicted_Completed_Time,MaxPredicted_completion_Time,\
                            Predicted_completion_Time,Actual_completion_Time,Alert,alert_time \
                            from {}.pxo_navnow_nds_ticker_prediction_1".format(database)).coalesce(1)
  joined_data = joined_data.select('ticker','fund_id','client_name','Predicted_Completed_Time','MaxPredicted_completion_Time','Predicted_completion_Time','Actual_completion_Time','Alert','alert_time').drop_duplicates()
  ticker_results = sqlctxt.sql("select client_name,ticker,fund_id,Predicted_Time,maxtime_before1805,maxtime,alert_time \
                                from {}.pxo_navnow_ticker_prediction_quantiles1".format(database)).coalesce(1)
  ticker_results = ticker_results.withColumnRenamed('alert_time','Alert time')
  
  client_info = ticker_results.select('ticker','client_name').drop_duplicates().coalesce(1)
  client_info = client_info.withColumnRenamed('client_name','client_identifier')
  client_info = client_info.withColumn('ticker',f.trim(client_info.ticker))
  predictions = sqlctxt.sql("select client_identifier,bin,weekday,red_1,predicted from {}.pxo_navnow_nds_predicted".format(database)).coalesce(1).dropna()

def geneos_data_push():
  geneos_data= sqlctxt.sql("select * from {}.pxo_navnow_nds_fundlevel \
           where updated_time = (select max(updated_time) from {}.pxo_navnow_nds_fundlevel)".format(database,database))   
  geneos_data.select('ticker_id','fund_id','nav_date','client_name', \
        'predicted_completed_time',\
        'maxpredicted_completion_time','predicted_completion_time',\
        'actual_completion_time','nds_fund_miss','alert_time','updated_time',\
        'status').coalesce(1).write.mode("append").format('jdbc') \
        .options(\
         url=oracle_url,\
         dbtable='navnow_nds_table',\
         driver="oracle.jdbc.OracleDriver"\
         ).save() 

  
def db_connect():
  if(time.strftime("%H:%M:%S") > '15:59:00'):
    data = connection(nds_sql)
    return data

def initial_run():  
      curren_date = str(datetime.datetime.date(datetime.datetime.now()))
      origin_data = sqlctxt.sql("select * from {}.pxo_navnow_nds_clientlevel".format(database))
      origin_data = origin_data.filter(origin_data.nav_date == curren_date)      
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
        origin_data.coalesce(1).select('client_identifier','nav_date','bin','count','cumsum','predicted','red_1','error')\
                             .write.mode("overwrite").insertInto("{}.pxo_navnow_nds_clientlevel".format(database))                
        updated_data = joined_data.filter(joined_data.Predicted_completion_Time > time.strftime("%H:%M:%S"))
        w = Window.partitionBy('client_name','ticker').orderBy()
        updated_data = f.broadcast(updated_data).withColumn('min_bin', f.min('Predicted_completion_Time').over(w))\
                                 .where(f.col('Predicted_completion_Time') == f.col('min_bin')).drop('min_bin')

        updated_data = updated_data.withColumn('nav_date',f.lit(datetime.datetime.today().strftime('%Y-%m-%d')))
        updated_data = updated_data.withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
        updated_data = updated_data.withColumn('best_case',f.lit(0))
        updated_data = updated_data.withColumn('worst_case',f.lit(0))
        updated_data = updated_data.withColumn('status',f.lit('Awaiting'))
        updated_data = updated_data.withColumn("Predicted_completion_Time", f.when(updated_data['Predicted_completion_Time'] < updated_data['Predicted_Completed_Time'], updated_data['Predicted_Completed_Time']).otherwise(updated_data["Predicted_completion_Time"]))        
        updated_data.coalesce(1).select('ticker','fund_id','nav_date','client_name','Predicted_Completed_Time','MaxPredicted_completion_Time',\
                                        'Predicted_completion_Time','Actual_completion_Time','Alert','alert_time','updated_time','best_case',\
                                        'worst_case','status').write.insertInto("{}.pxo_navnow_nds_fundlevel".format(database))
        if (env == 'UAT' or env == 'PROD'):
          geneos_data_push()
        print ("=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
		
		
		
def process(data):  
    global client_info,predictions,joined_data,ticker_results;
    if (time.strftime("%H:%M:%S") > '16:00:00'):
      print ("\n\n=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")
      print ("data available")        
      current = datetime.datetime.now()
      current = str(current).split(" ")[1]
      
      processed_data = sqlctxt.sql("select * from {}.pxo_navnow_nds_fundlevel where actual_completion_time != 'NA'".format(database))
      processed_data = processed_data.filter(processed_data.nav_date == datetime.datetime.today().strftime('%Y-%m-%d')).drop_duplicates()
      processed_data = processed_data.withColumnRenamed('ticker_id','ticker')
      
      data = data.select('AS_OF_DATE','SEND_TIME','TICKER')  
      data = data.withColumn('TICKER',f.trim(data.TICKER))
      data = data.withColumn('SEND_TIME',data.SEND_TIME.cast(TimestampType())) 
      data = data.withColumn('AS_OF_DATE',data.AS_OF_DATE.cast(DateType()))        
      new_data = data.groupby('AS_OF_DATE','TICKER').agg(f.min('SEND_TIME').alias('SEND_TIME'))
      new_data = new_data.drop_duplicates()
      new_data = new_data.join(client_info,on= 'ticker',how = 'left')
      new_data = new_data.dropna() 
      
      new_data = new_data.withColumnRenamed('client_name','client_identifier') 
      new_data = new_data.join(f.broadcast(processed_data), on = "ticker", how ="leftanti")         
      data = new_data.withColumn('SEND_TIME',new_data.SEND_TIME.cast(TimestampType())) \
                 .withColumn('AS_OF_DATE',new_data.AS_OF_DATE.cast(DateType())) \
                 .withColumnRenamed('AS_OF_DATE','nav_date')\
                 .withColumnRenamed('SEND_TIME','event_ts') \
                 .withColumn('event_ts',f.date_format('event_ts', 'HH:mm:ss'))
      data = data.filter(data.event_ts >= '16:00:00')
      data = data.withColumn("Alert", f.when(data["event_ts"] <= '18:05:00', 'Completed').otherwise('Missed'))
        
      data = data.join(ticker_results.select('ticker','fund_id','client_name','Predicted_Time','maxtime_before1805','Alert time'),on = 'ticker',how = 'left')      
      data = data.dropna()
      
      old_data = joined_data.filter(joined_data.Predicted_completion_Time > current)
      w = Window.partitionBy('client_name','ticker').orderBy()
      old_data = f.broadcast(old_data).withColumn('min_bin', f.min('Predicted_completion_Time').over(w))\
                               .where(f.col('Predicted_completion_Time') == f.col('min_bin')).drop('min_bin')
        
      old_data = old_data.join(f.broadcast(data), on = "ticker", how = "leftanti")
      old_data = old_data.join(f.broadcast(processed_data), on = "ticker", how = "leftanti")
    
      old_data = old_data.select('ticker','fund_id','client_name','Predicted_Completed_Time','MaxPredicted_completion_Time','Predicted_completion_Time','Actual_completion_Time','Alert','alert_time')
      data = data.select('TICKER','fund_id','client_name','Predicted_Time','maxtime_before1805','Predicted_Time','event_ts','Alert','Alert time')
      old_data = unionAll(old_data,data)

      old_data = old_data.withColumn('nav_date',f.lit(datetime.datetime.today().strftime('%Y-%m-%d')))
      old_data = old_data.withColumn('updated_time',f.lit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))                

      old_data = old_data.withColumn("Predicted_completion_Time", f.when(old_data["Alert"] == 'last_time', old_data['MaxPredicted_completion_Time']).otherwise(old_data["Predicted_completion_Time"]))
      old_data = old_data.withColumn("Alert", f.when(old_data['Alert'] == 'last_time', 'Missed').otherwise(old_data["Alert"]))
      
      if (current < '18:05:00'):
        old_data = old_data.withColumn('Alert',f.regexp_replace('Alert',"Missed",'Definitely Yes'))
      if (current > '18:05:00'):
        old_data = old_data.withColumn('Alert',f.regexp_replace('Alert',"Definitely Yes",'Missed'))
        old_data = old_data.withColumn('Alert',f.regexp_replace('Alert',"Definitely Not",'Missed'))
        old_data = old_data.withColumn('Alert',f.regexp_replace('Alert',"Probably Yes",'Missed'))
        old_data = old_data.withColumn('Alert',f.regexp_replace('Alert',"Probably",'Missed'))
      old_data = old_data.withColumn('Alert',f.regexp_replace('Alert',"Completed",'Not Missed'))
      old_data = old_data.withColumn('best_case' ,f.lit(0))   
      old_data = old_data.withColumn('worst_case' ,f.lit(0))  
      old_data = old_data.withColumn('status1',f.lit('Awaiting'))    
      old_data = old_data.withColumn('status',f.when((old_data['Actual_completion_Time'] != 'NA') & (old_data['Predicted_Completed_Time'] >= old_data['Actual_completion_Time']),'on Time').otherwise(old_data['status1']))
      old_data = old_data.withColumn('status',f.when((old_data['Actual_completion_Time'] != 'NA') & (old_data['Predicted_Completed_Time'] < old_data['Actual_completion_Time']),'Delayed Completion').otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['Actual_completion_Time'] != 'NA') & (old_data['MaxPredicted_completion_Time'] < old_data['Actual_completion_Time']),'Late Completion').otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['Actual_completion_Time'] == 'NA') & (old_data['Predicted_Completed_Time'] > current),'Awaiting').otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['Actual_completion_Time'] == 'NA') & (old_data['Predicted_Completed_Time'] < current),'Delayed').otherwise(old_data['status']))
      old_data = old_data.withColumn('status',f.when((old_data['Actual_completion_Time'] == 'NA') & (old_data['MaxPredicted_completion_Time'] < current),'Late').otherwise(old_data['status']))           
      old_data.coalesce(1).select('ticker','fund_id','nav_date','client_name','Predicted_Completed_Time','MaxPredicted_completion_Time',\
                                  'Predicted_completion_Time','Actual_completion_Time','Alert','alert_time','updated_time','best_case',\
                                  'worst_case','status').write.insertInto("{}.pxo_navnow_nds_fundlevel".format(database))            
      processed_data = sqlctxt.sql("select * from {}.pxo_navnow_nds_fundlevel where actual_completion_time != 'NA'".format(database))
      processed_data = processed_data.filter(processed_data.nav_date == datetime.datetime.today().strftime('%Y-%m-%d')).drop_duplicates()
      data = processed_data.withColumnRenamed('ticker_id','ticker')
      data = data.withColumnRenamed('actual_completion_time','event_ts')
      data = data.withColumnRenamed('client_name','client_identifier')
      data = data.withColumn('event_ts',data.event_ts.cast(TimestampType()))      
      data = data.withColumn("bin",(f.unix_timestamp(f.col("event_ts")) - (f.unix_timestamp(f.col("event_ts")) % f.lit(60)) + f.lit(60)).cast(TimestampType()))
      data = data.withColumn('bin',f.split("bin"," ").getItem(1))      
      data = data.groupby('client_identifier','bin').agg(f.count('ticker').alias('count'))            
      data = f.broadcast(data).join(predictions,on = ['client_identifier','bin'],how = 'outer').fillna(0)
      data = data.withColumn('nav_date',f.lit(str(datetime.date.today())))       
      origin_data = data.filter(data.bin <= current)            
      windowval = Window.partitionBy('nav_date','client_identifier')\
                        .orderBy('bin')\
                        .rangeBetween(Window.unboundedPreceding, 0)
      origin_data = f.broadcast(origin_data).withColumn('cumsum', f.sum('count').over(windowval))
      origin_data = origin_data.withColumn('error',origin_data.predicted - origin_data.cumsum).drop_duplicates()      
            
      origin_data.coalesce(1).select('client_identifier','nav_date','bin','count','cumsum','predicted','red_1','error')\
                             .write.mode("overwrite").insertInto("{}.pxo_navnow_nds_clientlevel".format(database))                           
      if (env == 'UAT' or env == 'PROD'):
        try:
          geneos_data_push() 
        except:
          pass
      print ("=================" + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "================")


if __name__ == "__main__":
  global first_run
  first_run,n = 0,0
  global client_info,predictions,joined_data,ticker_results
  #load prediction and performing initial run    
  load_predictions(database)
  results = initial_run() 
  while(n != 1):
    data = db_connect()
    process(data)    
    if (time.strftime("%H:%M:%S") > "19:00:00"):
      sc.stop()       
    seconds = int(time.strftime("%H:%M:%S")[6:])
    sleep_time = abs(60 - seconds)
    time.sleep(sleep_time)