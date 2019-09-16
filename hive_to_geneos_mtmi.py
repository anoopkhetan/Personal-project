from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import datetime,time,logging,os,sys
from pyspark.sql import Window
import math


spark = SparkSession.\
        builder.\
        config("spark.jars", "/opt/cloudera/parcels/CDH/lib/sqoop/lib/ojdbc6.jar").\
        appName("MTMI-GENEOS").\
        getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlctxt = SQLContext(sc)


database_m = 'h011pxo_m'
#url="jdbc:oracle:thin:" + str(sys.argv[3]) + "/" + str(sys.argv[4]) + "@gdculvc0029:1528/O04GNS1"
url="jdbc:oracle:thin:" + str('sudgtcg') + "/" + str('e8usv4=b56pufxwhkk#q90bsl') + "@gdculvc0029.statestr.com:1528/O04GNS1"


def send_data():
  print ("start:",datetime.datetime.now())
  data = sqlctxt.sql("select * from {}.pxo_navnow_mtmi_fundlevel".format(database_m))
  data = data.filter(data.nav_date == '2019-06-05')
#  data = data.filter(data.updated_time <= str(datetime.datetime.now()))
#  w = Window.partitionBy('nav_date')
#  data = data.withColumn('max_updated_time', f.max('updated_time').over(w))\
#                          .where((f.col('updated_time') == f.col('max_updated_time')) \
#                          ).drop('max_updated_time')
    
#  print (data.select('updated_time').distinct().show())   
  data.select('fund_id','nav_date','client_name', \
              'predicted_completed_time',\
              'maxpredicted_completion_time','predicted_completion_time',\
              'actual_completion_time','updated_time','alert_time',\
              'status','signal').coalesce(1).write.mode("append").format('jdbc') \
    .options(\
               url=url,\
               dbtable='navnow_mtmi_table',\
               driver="oracle.jdbc.OracleDriver"\
               ).save()

if __name__ == '__main__':
  n = 0
  while(time.strftime("%H:%M:%S") < "00:02:00"):
      time.sleep(10)
  send_data()
  
  seconds = int(time.strftime("%H:%M:%S")[6:])
  time.sleep(abs(60 - seconds))
  while(n != 1):
    now = datetime.datetime.now().second
    print ("now:",now)
    if (time.strftime("%H:%M:%S") > "19:01:00"):
      sc.stop()
    send_data()
    later = datetime.datetime.now().second
    print ("later:",later)
    diff = math.floor((later - now))
    sleep_time = abs(60 - diff)
    print ("sleep:",sleep_time)
    time.sleep(sleep_time)