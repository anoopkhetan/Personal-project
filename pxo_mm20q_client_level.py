from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f
import json,datetime,time
from pyspark.sql import Window
import pandas as pd,time
from pyspark.conf import SparkConf
from pyspark import SparkContext, HiveContext
from pyspark.sql.functions import col
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext,SparkSession,HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from cytoolz import curry
from cytoolz.functoolz import compose
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, collect_list, concat_ws, udf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
import numpy as np
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import trim
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import year, month, dayofmonth
import calendar
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, year, month
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import numpy as np
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when
import sys
from pyspark.sql.functions import concat, col, lit

################setting spark context##############################
sc_conf = SparkConf()
#sc_conf.set('spark.executor.memory', '4g')
#sc_conf.set('spark.num.executors', '4')
#sc_conf.set('spark.executor.cores', '4')

spark = SparkSession\
        .builder.config(conf=sc_conf)\
        .appName("client_analysis")\
        .getOrCreate()\


sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlctxt = SQLContext(sc)


###################getting source data###################################################

data_source=sqlctxt.sql("SELECT CLIENT_ID, CLIENT_NAME, CLIENT_RLUP, PCS_STEP_CFG_KY, STEP_CAPTION, AS_OF_TMS, \
AVG_PROCESSTIME_VALUE, FUND_CNT,FUND_REGION FROM " + str(sys.argv[2]) + ".PXO_DGF_STPS_CLIENT_DETAIL_PHYS_H_1") 

######################Adding fields for date filter#############################
data_source = data_source.withColumn('date', col('AS_OF_TMS').cast(DateType()))
data_source = data_source.withColumn('data_year', year('date'))
data_source = data_source.withColumn('data_month',month('date'))

################Current month first day and last month last day retrival##############################
first_day_of_current_month = date.today().replace(day=1)
last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

first_day_of_previous_month = first_day_of_current_month + relativedelta(months=-1)

####################################previous month year and month retrival#############################
year1 = first_day_of_previous_month.year
month1 = first_day_of_previous_month.month


data_source=data_source.withColumnRenamed('AS_OF_TMS','AS_OF_TMS1')
source_df=data_source

###################Retriving Data for PXO Client name######################

###################Code to be removed as view would be created for doing same################
#data_reference = sqlctxt.sql("select * from  h011pxo.pxo_itd_clientmaster_1")
#
#max_time = data_reference.select(f.max('as_of_tms')).collect()[0][0]
#print (max_time)
#
#data_reference = data_reference.filter(data_reference.as_of_tms == max_time )
#
#data_reference=data_reference.filter(data_reference.source== 'MCH Client ID')
#
#data_source=data_source.withColumn("CLIENT_ID",ltrim(data_source.CLIENT_ID))
#data_reference=data_reference.withColumn("source_client_id",ltrim(data_reference.source_client_id))
#
#
##data_reference.count()
#
#source_df=data_source.join(data_reference,(data_source.CLIENT_ID ==data_reference.source_client_id)\
#                           & (data_source.CLIENT_NAME ==data_reference.src_client_name),'left')

##########################filtering last month data###############################
source_df = source_df.withColumn('data_year', year('date'))
source_df = source_df.withColumn('data_month',month('date'))

first_day_of_current_month = date.today().replace(day=1)
current_date_month= date.today().month
current_date_year=date.today().year


last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

first_day_of_previous_month = first_day_of_current_month + relativedelta(months=-1)

if current_date_year==2019 and current_date_month==4:
  
  source_df = source_df.filter(source_df["data_year"] == 2019) 
  source_df = source_df.where(col("data_month").isin(1,2,3))
else:  
 
  source_df = source_df.filter(source_df["data_year"] == year1) 
  source_df = source_df.filter(source_df["data_month"] == month1)

  

##############Source df column changes##############################
#source_df =source_df.filter(source_df["date"] == lit('2019-01-09')) 
#source_df = source_df.filter(source_df["CLIENT_ID"] == 'MSAA')
#### source_df deleted , 'pxo_client_name'

source_df=source_df.select('CLIENT_ID', 'CLIENT_NAME', 'CLIENT_RLUP', 'PCS_STEP_CFG_KY', 'STEP_CAPTION', 'AS_OF_TMS1', \
 'AVG_PROCESSTIME_VALUE','FUND_REGION', 'FUND_CNT', 'date')

source_df=source_df.fillna({'FUND_REGION':'TBD'})


source_df=source_df.withColumnRenamed('CLIENT_ID','client_id')
source_df=source_df.withColumnRenamed('CLIENT_NAME','client_name')
source_df=source_df.withColumnRenamed('CLIENT_RLUP','client_rlup')
source_df=source_df.withColumnRenamed('PCS_STEP_CFG_KY','pcs_step_cfg_ky')
source_df=source_df.withColumnRenamed('STEP_CAPTION','step_caption')
source_df=source_df.withColumnRenamed('AS_OF_TMS1','as_of_tms')
source_df=source_df.withColumnRenamed('AVG_PROCESSTIME_VALUE','avg_processtime_value')
source_df=source_df.withColumnRenamed('FUND_REGION','fund_region')
source_df=source_df.withColumnRenamed('FUND_CNT','fund_cnt')


####################Creating field to get decimal time###################################
source_df= source_df.withColumn('hour', source_df.avg_processtime_value.substr(1,2))
source_df= source_df.withColumn('minute', source_df.avg_processtime_value.substr(4,5))
source_df= source_df.withColumn('minute1', source_df.minute.substr(1,2))
source_df= source_df.withColumn('second', source_df.avg_processtime_value.substr(7,8))

source_df = source_df.withColumn("hour", source_df["hour"].cast(IntegerType()))
source_df = source_df.withColumn("minute1", source_df["minute1"].cast(IntegerType()))
source_df = source_df.withColumn("second", source_df["second"].cast(IntegerType()))

source_df = source_df.withColumn('decimal_apt', (source_df.hour*24+source_df.minute1*60+source_df.second)/(24*3600))

source_df =source_df.withColumn('combined', \
                    f.concat(f.col('client_id'), f.col('fund_region'), f.col('pcs_step_cfg_ky'),f.col('as_of_tms')))

source_df.registerTempTable('table')
source_df1 = sqlctxt.sql(
    'SELECT *, COUNT(combined) OVER (PARTITION BY combined) AS n FROM table')
#source_df1= source_df.groupBy('client_id', 'pcs_step_cfg_ky','fund_region','as_of_tms','avg_processtime_value', 'decimal_apt' ).count()
#source_df1=source_df1.withColumnRenamed('count','frq')
source_df2= source_df1.filter(source_df1.n ==1)
source_df = source_df2
source_name_df=source_df.select('client_id', 'fund_region').distinct()
source_name_df=source_name_df.withColumnRenamed('client_id','name_client_id')\
                  .withColumnRenamed('fund_region','name_fund_region')

########################Reading rules###############################################################
df_rule= sqlctxt.sql("select * from " + str(sys.argv[3]) + ".pxo_MyMetrics_Rules_v")
#df_rule =spark.read.csv('/user/p809467/rules_with_new_rules.csv',encoding='ISO-8859-1',inferSchema=True,header=True)

df_rule=df_rule.fillna('None')


#############################Renaming columns of rule df##############################################
df_rule= df_rule.withColumnRenamed("no", "No")\
                .withColumnRenamed("type","Type")\
                .withColumnRenamed("from","From")\
                .withColumnRenamed("to","To")\
                .withColumnRenamed("text","Text")\
                .withColumnRenamed("operator","Operator")\
                .withColumnRenamed("sequence","Sequence")\
                .withColumnRenamed("relationship","Relationship")\
                .withColumnRenamed("relationship_value","Relationship_value")\
                .withColumnRenamed("relationship_not","Relationship_not")\
                .withColumnRenamed("relationship_not_value","Relationship_not_value")\
                .withColumnRenamed("order to run rules:","Order to run rules:")
                
df_rule = df_rule.withColumn('Time_minute', col('Time_minute').cast(FloatType()))\
                  .withColumn('Time_minute2', col('Time_minute2').cast(FloatType()))\
                  .withColumn('From', col('From').cast(IntegerType()))\
                  .withColumn('To', col('To').cast(IntegerType()))

df_rule=df_rule.fillna('None')


###############################Getting rule reference######################################
df_rule_reference=sqlctxt.sql("select * from " + str(sys.argv[3]) + ".pxo_MyMetrics_Rules_reference_v")


#################################Renaming rule reference columns##############################
df_rule_reference= df_rule_reference.withColumnRenamed("rule_number", "Rule_Number")\
                                  .withColumnRenamed("type","Type")\
                                  .withColumnRenamed("from","From")\
                .withColumnRenamed("to","To")\
                .withColumnRenamed("rule description","Rule Description")\
                .withColumnRenamed("rule description 2","Rule Description 2")\
                .withColumnRenamed("ds result","DS Result")\
                .withColumnRenamed("result summary","Result summary")\
                .withColumnRenamed("step key","Step Key")\
                .withColumnRenamed("step key caption","Step Key caption")\
                .withColumnRenamed("assessment statement","Assessment Statement")\
                .withColumnRenamed("pairing","Pairing")\
                .withColumnRenamed("action","Action")


df_rule_reference=df_rule_reference.withColumn('From', col('From').cast(IntegerType()))\
                                    .withColumn('To', col('To').cast(IntegerType()))
            
                



##############assign weighted time####################
def wt_avg(timer):
  (h, m, s) = timer.split(':')
  result = int(h) * 3600 + int(m) * 60 + int(s)
  return (result/float(86400))

wt_avg_udf = f.udf(wt_avg,FloatType())

source_df = source_df.withColumn('decimal_apt',wt_avg_udf("avg_processtime_value"))

###########Train weighted average#########################

########code removed as there should be only 1 record for client_id,fund_region,process_step,as_of_tms####################
#df_wt_avg= source_df.groupby("client_id","pcs_step_cfg_ky", "as_of_tms", "fund_region", "pxo_client_name" ).agg(f.collect_set("decimal_apt").alias("time"),f.collect_list("fund_cnt").alias("weight"))
#
#def wt_avg(t,w):
#  fund_cnt=0
#  wt_avg=0
#  for i in range(0,len(t)):
#      time=t[i]
#      weight=w[i]
#      wt_avg+=time*weight
#      fund_cnt+=weight
#  return (wt_avg/fund_cnt)
#wt_avg_udf = f.udf(wt_avg,FloatType())
source_df =source_df.withColumn('weighted_time',source_df["decimal_apt"])
df_stats = source_df



#Getting unique rows with "dshbd_fund_grp_ky","pcs_step_cfg_ky","as_of_tms","fund_region" combination 
df_client_date_list=source_df.groupby(['client_id', 'fund_region', 'as_of_tms']).count()
df_client_date_list=df_client_date_list.withColumn('key', lit(10))

#Getting boolean rule
df_rule_boolean=df_rule.filter(df_rule.Type=='boolean')
df_rule_boolean=df_rule_boolean.withColumn('key', lit(10))


#Getting each unique group with the rule df
df_client_date_list_rule=df_rule_boolean.join(df_client_date_list, df_client_date_list.key==df_rule_boolean.key,'right')
df_client_date_list_rule = df_client_date_list_rule.dropDuplicates()
#Getting dataset with only needed column
df_boolean_subset=df_client_date_list_rule.select('No','Type','From','To','client_id', 'fund_region', 'as_of_tms')

#df_boolean_subset = df_boolean_subset.select(col("From").alias("PCS_STEP_CFG_KY"),col("No").alias("No"),col("Type").alias("Type"),col("To").alias("To"),col("CLIENT_ID").alias("CLIENT_ID"),col("FUND_REGION").alias("FUND_REGION"),col("AS_OF_TMS").alias("AS_OF_TMS"))
#df_boolean_subset = df_boolean_subset.withColumn("pcs_step_cfg_ky2", df_boolean_subset["pcs_step_cfg_ky"])
#source_df = source_df.withColumn("pcs_step_cfg_ky2", source_df["pcs_step_cfg_ky"])

df_boolean_subset = df_boolean_subset.select(col("From").alias("From"),col("No").alias("No"),col("Type").alias("Type"),col("To").alias("To"),col("client_id").alias("R_client_id"), col("fund_region").alias("R_fund_region"), col("as_of_tms").alias("R_as_of_tms"))

#df_boolean_subset = df_boolean_subset.withColumnRenamed("client_id","R_client_id")\
#                                      .withColumnRenamed("fund_region","R_fund_region")\
 #                                      .withColumnRenamed("as_of_tms","R_as_of_tms")

#Combining rule reference with data
         
df_boolean_data=source_df.join(df_boolean_subset,\
                      (df_boolean_subset.From == source_df.pcs_step_cfg_ky) \
                                                &  (df_boolean_subset.R_client_id == source_df.client_id)
                                                           &  (df_boolean_subset.R_fund_region == source_df.fund_region)
                                              &  (df_boolean_subset.R_as_of_tms == source_df.as_of_tms),'right'  )
df_boolean_data= df_boolean_data.na.fill(0) 
#Getting result for boolean  
  
def boolean_result(a):
  if a == 0:
    return 'Followed'
  else:
    return 'Not Followed'
ydf_test = f.udf(boolean_result,StringType())

df_boolean_data = df_boolean_data.withColumn('Result',ydf_test('pcs_step_cfg_ky'))
#df_boolean_data = df_boolean_data[['No','Result']].drop_duplicates()
#df_boolean_data.write.csv('boolean_result.csv')

df_boolean_result= df_boolean_data.select('No', 'R_client_id','Result', 'R_fund_region','R_as_of_tms' )
df_boolean_result=df_boolean_result.withColumnRenamed('R_client_id','client_id')\
                  .withColumnRenamed('R_fund_region','fund_region')\
                  .withColumnRenamed('R_as_of_tms','as_of_tms')
#############Region rule type ################################
df_rule_region=df_rule.filter(df_rule.Type=='region')
df_rule_region=df_rule_region.withColumn('key', lit(10))

#Getting each unique group with the rule adoption df
df_client_date_list_rule_region=df_rule_region.join(df_client_date_list, df_client_date_list.key==df_rule_region.key,'right')

def region_result(a):
  if a == 'NA':
    return 'Followed'
  else:
    return 'Not Followed'
udf_adoption = f.udf(region_result,StringType())

df_client_date_list_rule_region = df_client_date_list_rule_region.withColumn('Result',udf_adoption('fund_region'))

df_region_result=df_client_date_list_rule_region.select('No', 'client_id','Result', 'fund_region','as_of_tms')
  
df_result=df_boolean_result.union(df_region_result)


#############Adoption rule type ################################
df_rule_adoption=df_rule.filter(df_rule.Type=='adoption')
df_rule_adoption=df_rule_adoption.withColumn('key', lit(10))


#Getting each unique group with the rule df
df_client_date_list_rule_adoption=df_rule_adoption.join(df_client_date_list, df_client_date_list.key==df_rule_adoption.key,'right')

#Getting dataset with only needed column
df_adoption_subset=df_client_date_list_rule_adoption.select('No','Type','From','To','client_id','FUND_REGION','AS_OF_TMS')



df_adoption_subset = df_adoption_subset.select(col("From"),col("No"),col("Type"),col("To"),col("client_id").alias("R_client_id"),col("fund_region").alias("R_fund_region"),col("as_of_tms").alias("R_as_of_tms"))

df_adoption_data=source_df.join(df_adoption_subset,\
                      ( df_adoption_subset.From == source_df.pcs_step_cfg_ky) \
                                                &  (df_adoption_subset.R_client_id == source_df.client_id)\
                                            & (df_adoption_subset.R_fund_region == source_df.fund_region) \
                                               & (df_adoption_subset.R_as_of_tms == source_df.as_of_tms) ,'right'  )
df_adoption_data= df_adoption_data.na.fill(0)  
#Getting result for boolean  
  
def adoption_result(a):
  if a == 0:
    return 'Not Followed'
  else:
    return 'Followed'
udf_adoption = f.udf(adoption_result,StringType())
df_adoption_data = df_adoption_data.withColumn('Result',udf_adoption('PCS_STEP_CFG_KY'))

df_adoption_result= df_adoption_data.select('No', 'R_client_id','Result', 'R_fund_region','R_as_of_tms')
df_adoption_result=df_adoption_result.withColumnRenamed('R_client_id','client_id')\
                                   .withColumnRenamed('R_fund_region','fund_region')\
                                  .withColumnRenamed('R_as_of_tms','as_of_tms')

df_result=df_result.union(df_adoption_result) #n. of rows are 13475    
##################Not present ######################################
df_rule_presence=df_rule.filter(df_rule.Type.isin('sequence','timediff','timecheck'))
df_rule_presence=df_rule_presence.withColumn('key', lit(10))


#Getting each unique group with the rule df
df_client_date_list_rule_presence=df_rule_presence.join(df_client_date_list, df_client_date_list.key==df_rule_presence.key,'left')

#Getting dataset with only needed column
df_presence_subset=df_client_date_list_rule_presence.select\
('No','Type','From','To','Operator','Time_minute','Time_minute2','client_id','fund_region','as_of_tms','Sequence')


df_presence_subset=df_presence_subset.withColumnRenamed("client_id","R_client_id")\
                                      .withColumnRenamed("fund_region","R_fund_region")\
                                       .withColumnRenamed("as_of_tms","R_as_of_tms")

df_presence_data=source_df.join(df_presence_subset,\
                                 ( df_presence_subset.From == source_df.pcs_step_cfg_ky) \
                     &  (df_presence_subset.R_client_id == source_df.client_id)\
                                            & (df_presence_subset.R_fund_region == source_df.fund_region) \
                                               & (df_presence_subset.R_as_of_tms == source_df.as_of_tms) ,'right'  )

df_presence_data= df_presence_data.na.fill(0)
def presence_result(rule_type,key):
  if key==0:
    if rule_type=="timecheck" :
      return "Not Present"
    else :
      return "Not Present From"
      
udf_presence = f.udf(presence_result,StringType())
df_presence_data = df_presence_data.withColumn('Result',udf_presence('Type','pcs_step_cfg_ky'))
df_presence_data= df_presence_data.fillna('abcd', subset=['Result'])

##############Time check ############################
df_time_check_data= df_presence_data.filter(df_presence_data.Type=='timecheck')
df_time_check_data_notpresent=df_time_check_data.filter(df_time_check_data.Result!='abcd')
df_time_check_eval=df_time_check_data.filter(df_time_check_data.Result =='abcd')

def timecheck_result(weighted_time,Operator,Time_minute,Time_minute2):
  result= 'Followed'
  if (Operator == 'less') & (weighted_time > Time_minute) :
    result='Not Followed'
    
  elif (Operator == 'greater') & (weighted_time < Time_minute) :
     result='Not Followed' 
  
  elif (Operator == 'between') & (weighted_time < Time_minute):
    result='Not Followed'
    
  elif (Operator == 'between') & (weighted_time > Time_minute2):
    result='Not Followed'
  
  return result
  
udf_time_check_result = f.udf(timecheck_result,StringType())
df_time_check_result = df_time_check_eval\
            .withColumn('Result',udf_time_check_result('weighted_time','Operator','Time_minute','Time_minute2'))

df_timecheck_result=df_time_check_result.union(df_time_check_data_notpresent)
df_timecheck_result=df_timecheck_result.select('No', 'R_client_id','Result', 'R_fund_region','R_as_of_tms')
df_timecheck_result=df_timecheck_result.withColumnRenamed("R_client_id","client_id")\
                                      .withColumnRenamed("R_fund_region","fund_region")\
                                       .withColumnRenamed("R_as_of_tms","as_of_tms")
df_result=df_result.union(df_timecheck_result)#no. of rows are 21560
  
##############Sequence and timedifference ############################
df_seq_timediff_data= df_presence_data.filter(df_presence_data.Type!='timecheck')
df_seq_timediff_data_notpresentfrom=df_seq_timediff_data.filter(df_seq_timediff_data.Result!='abcd')
df_seq_timediff_data_notpresentfrom=df_seq_timediff_data_notpresentfrom.select('No', 'R_client_id','Result', 'R_fund_region','R_as_of_tms')

df_seq_timediff_eval=df_seq_timediff_data.filter(df_seq_timediff_data.Result =='abcd')
df_seq_timediff_eval=df_seq_timediff_eval.select('pcs_step_cfg_ky','weighted_time','No',\
                                             'Type','From','To','Time_minute','R_client_id','R_as_of_tms','Result','R_fund_region','Sequence')
df_seq_timediff_eval=df_seq_timediff_eval.withColumnRenamed('pcs_step_cfg_ky','f_pcs_step_cfg_ky')\
                                    .withColumnRenamed('weighted_time','f_weighted_time')
  
df_seq_timediff_eval_data= source_df.join(df_seq_timediff_eval,\
                      ( df_seq_timediff_eval.To == source_df.pcs_step_cfg_ky) \
                                                &  (df_seq_timediff_eval.R_client_id == source_df.client_id)\
                                            & (df_seq_timediff_eval.R_fund_region == source_df.fund_region) \
                                               & (df_seq_timediff_eval.R_as_of_tms == source_df.as_of_tms) ,'right'  ) 
df_seq_timediff_eval_data= df_seq_timediff_eval_data.na.fill(0)
def seq_timediff(key):
  if key==0:
    return "Not Present To"
      
udf_seq_timediff = f.udf(seq_timediff,StringType())
df_seq_timediff_data = df_seq_timediff_eval_data.withColumn('Result',udf_seq_timediff('pcs_step_cfg_ky'))

df_seq_timediff_data=df_seq_timediff_data.fillna('abcd')
df_seq_timediff_data_notpresentto=df_seq_timediff_data.filter(df_seq_timediff_data.Result!='abcd')
df_seq_timediff_data_notpresentto=df_seq_timediff_data_notpresentto.select('No', 'R_client_id','Result', 'R_fund_region','R_as_of_tms')

df_seq_timediff_data_notpresentfrom=df_seq_timediff_data_notpresentfrom.withColumnRenamed('R_client_id','client_id')\
                                    .withColumnRenamed('R_fund_region','fund_region')\
                                    .withColumnRenamed('R_as_of_tms','as_of_tms')
  

df_seqtimediff_result=df_seq_timediff_data_notpresentfrom.union(df_seq_timediff_data_notpresentto)



df_seq_timediff_data=df_seq_timediff_data.filter(df_seq_timediff_data.Result=='abcd')

#finding the time difference
df_seq_timediff_data=df_seq_timediff_data.withColumn("timedifference",  df_seq_timediff_data.weighted_time - df_seq_timediff_data.f_weighted_time) 

#Result for the sequence and timedifference rules
def seq_timediff_result(timedifference,Type,Time_minute,Sequence):
  
  if Type=='timediff' :
    if  (timedifference < 0) & (Sequence=='yes') :
      result='Not in sequence'
    elif  (timedifference < 0) & (Sequence=='no') :
      timedifference=timedifference * -1
      sec =  timedifference * 86400
      m, s = divmod(sec, 60)
      h, m = divmod(m, 60)
      d, h = divmod(h, 24)
     
      time_diff=((60*h) + m + (s*0.0166667))
      if (time_diff < Time_minute):
        result= 'Followed'
      else:
        result='Not Followed'

    else:
      sec =  timedifference * 86400
      m, s = divmod(sec, 60)
      h, m = divmod(m, 60)
      d, h = divmod(h, 24)
     
      time_diff=(60*h) + m + (s*0.0166667)
      if (time_diff < Time_minute):
        result= 'Followed'
      else:
        result='Not Followed'
      
  elif Type=='sequence' :
    if timedifference < 0.0:
      
      result='Not Followed'
      
    else:
      result='Followed'
  
  return result
  
udf_seq_timediff_result = f.udf(seq_timediff_result)
df_seq_timediff_result = df_seq_timediff_data\
            .withColumn('Result',udf_seq_timediff_result('timedifference','Type','Time_minute','Sequence'))

  
  
  
  
df_seq_timediff_result=df_seq_timediff_result.select('No', 'R_client_id','Result', 'R_fund_region','R_as_of_tms')
#check=df_seq_timediff_result.filter(df_seq_timediff_result.AS_OF_TMS=='3/28/2018')

df_seqtimediff_result=df_seqtimediff_result.union(df_seq_timediff_result)
df_seqtimediff_result=df_seqtimediff_result.withColumnRenamed("R_client_id","client_id")\
                                      .withColumnRenamed("R_fund_region","fund_region")\
                                       .withColumnRenamed("R_as_of_tms","as_of_tms")

df_result=df_result.union(df_seqtimediff_result)#no of rows 72765

#df_result.coalesce(30).write.option("inferSchema","true").csv("/user/e654191/result1.csv",header = 'true')

#df_result.write.csv('mycsv.csv')

  ####################creating rules for evaluation #####################################

  
df_rule=df_rule.withColumn("Relationship_value", f.split("Relationship_value", '\\+'))\
            .withColumn("Relationship",f.split("Relationship",'\\+'))\
            .withColumn("Relationship_not",f.split("Relationship_not",'\\+'))\
            .withColumn("Relationship_not_value",f.split("Relationship_not_value",'\\+'))
      

from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType
from pyspark.sql.functions import col, udf

zip_ = udf(
  lambda x, y: list(zip(x, y)),
  ArrayType(StructType([
      # Adjust types to reflect data types
      StructField("first", StringType()),
      StructField("second", StringType())
  ]))
)

df_rule=(df_rule
    .withColumn("tmp", zip_("Relationship", "Relationship_value"))
    # UDF output cannot be directly passed to explode
    .withColumn("tmp",f.explode("tmp"))
    .select("No",'From','To','Order to run rules:','Relationship_not','Relationship_not_value', col("tmp.first").alias("Relationship"), col("tmp.second").alias("Relationship_value")))

df_rule=(df_rule
    .withColumn("tmp", zip_("Relationship_not", "Relationship_not_value"))
    # UDF output cannot be directly passed to explode
    .withColumn("tmp",f.explode("tmp"))
    .select("No",'From','To','Order to run rules:','Relationship','Relationship_value', col("tmp.first").alias("Relationship_not"), col("tmp.second").alias("Relationship_not_value")))

df_rule=df_rule.withColumnRenamed('No','R_no')

############################################
#############Result with evaluation ##########################################

df_result_relationship= df_rule.join(df_result,df_rule.R_no == df_result.No,'right')
df_result_relationship_no_evaluation=df_result_relationship\
                                    .filter((df_result_relationship.Relationship=='None') & \
                                     (df_result_relationship.Relationship_not=='None'))


df_result_relationship= df_result_relationship.select('No', 'client_id','Result', 'fund_region','as_of_tms','Relationship','Relationship_value','Relationship_not','Relationship_not_value','Order to run rules:', 'From', 'To')


df_result_relationship = df_result_relationship\
                .withColumn('Relationship',trim( regexp_replace('Relationship', 'Rule', '')))\
                .withColumn('Relationship_not',trim( regexp_replace('Relationship_not', 'Rule', '')))

  
df_result_relationship = df_result_relationship.withColumn('Relationship',trim(df_result_relationship.Relationship))  # trim right whitespace from d1
df_result_relationship = df_result_relationship.withColumn('Relationship', ltrim(df_result_relationship.Relationship))  # trim right whitespace from d1
df_result_relationship = df_result_relationship.withColumn("Relationship", trim(col("Relationship")))  
df_result_relationship=df_result_relationship.withColumnRenamed("Relationship","RNo")

  
df_result_relationship_self_join=df_result_relationship.select('No', 'client_id','Result', 'fund_region','as_of_tms', 'From', 'To', 'Order to run rules:')



df_result_relationship_self_join=df_result_relationship_self_join.withColumnRenamed('client_id','j_client_id')\
                                                      .withColumnRenamed("fund_region","j_fund_region")\
                                                      .withColumnRenamed("No","j_No")\
                                                      .withColumnRenamed('as_of_tms','j_as_of_tms')\
                                                      .withColumnRenamed('Result','j_Result')\
                                                      .withColumnRenamed('Order to run rules:','j_Order to run rules:')\
                                                      .withColumnRenamed('From','j_From')\
                                                      .withColumnRenamed('To','j_To')
                                                       
df_result_relationship_self_join  =df_result_relationship_self_join .withColumn("j_No",df_result_relationship_self_join["j_No"].cast("string"))     
df_result_relationship_self_join = df_result_relationship_self_join.withColumn('j_No', rtrim(df_result_relationship_self_join.j_No))  # trim right whitespace from d1
df_result_relationship_self_join = df_result_relationship_self_join.withColumn('j_No', ltrim(df_result_relationship_self_join.j_No))  # trim right whitespace from d1
 

df_result_relationship=df_result_relationship.withColumnRenamed('Order to run rules:','Order_to_run_rules')
df_result_relationship_self_join=df_result_relationship_self_join.withColumnRenamed('j_Order to run rules:','j_Order_to_run_rules')


  
#values assigned

df_result_relationship_value=df_result_relationship.join(df_result_relationship_self_join,
                                                   (df_result_relationship.client_id==df_result_relationship_self_join.j_client_id)    
                                                  &  (df_result_relationship.fund_region==df_result_relationship_self_join.j_fund_region) 
                                                    & (df_result_relationship.as_of_tms==df_result_relationship_self_join.j_as_of_tms)
                                                        &(df_result_relationship.RNo==df_result_relationship_self_join.j_No), 'left' )
          

df_result_relationship_noevaluation=df_result_relationship_value.filter((df_result_relationship_value.RNo=='None') & \
                                                                   (df_result_relationship_value.Relationship_not=='None'))
df_result_relationship_evaluation= df_result_relationship_value.filter((df_result_relationship_value.RNo!='None') | \
                                                                   (df_result_relationship_value.Relationship_not!='None'))

df_result_relationship_evaluation=df_result_relationship_evaluation.select('No','client_id','fund_region','as_of_tms',\
                                                                          'Result','RNo','Relationship_value','Relationship_not','Relationship_not_value','j_Result')

df_result_relationship_evaluation=df_result_relationship_evaluation.withColumnRenamed('j_Result','relation_result')\
                                                                .withColumnRenamed('Relationship_not','nno')
                                                                  


df_result_relationship_and_not_value=df_result_relationship_evaluation.join(df_result_relationship_self_join,\
                                                   (df_result_relationship.client_id==df_result_relationship_self_join.j_client_id)    \
                                                  &  (df_result_relationship.fund_region==df_result_relationship_self_join.j_fund_region) \
                                                    & (df_result_relationship.as_of_tms==df_result_relationship_self_join.j_as_of_tms)\
                                                        & (df_result_relationship_evaluation.nno==df_result_relationship_self_join.j_No),'left' )


df_result_relationship_and_not_value=df_result_relationship_and_not_value.withColumnRenamed('Relationship_value','value')\
                                                                    .withColumnRenamed('Relationship_not_value','not_val')  
########################final result ##########################################

########################final result ##########################################
def flag_result(relation_Value,actual_relation_Value,not_value,not_actual_value):
  if  relation_Value!= actual_relation_Value:
    return 1
  elif  not_value== not_actual_value :
    return 1

  else:
    return 0
  
udf_result_flag = f.udf(flag_result,IntegerType())
df_result = df_result_relationship_and_not_value\
            .withColumn('flag',udf_result_flag('value','relation_result','not_val','j_Result'))

df_result=df_result.groupby('No' ,'client_id','fund_region','as_of_tms','Result').agg(f.sum('flag').alias('flag_sum'))
df_result=df_result.filter(df_result.flag_sum==0)  
df_result_relationship_noevaluation=df_result_relationship_noevaluation.select('No', 'client_id','Result', 'fund_region','as_of_tms')
df_result=df_result.select('No', 'client_id','Result', 'fund_region','as_of_tms')
df_result=df_result.union(df_result_relationship_noevaluation) # no of rows are 51998

df_result= df_result.withColumnRenamed('client_id','j_client_id')\
                                    .withColumnRenamed('fund_region','j_fund_region')\
                                    .withColumnRenamed('as_of_tms','j_as_of_tms')
                                    

df_result=df_result.join(source_df,\
                                   (df_result.j_client_id==source_df.client_id)\
                                      & (df_result.j_fund_region==source_df.fund_region)\
                                          &  (df_result.j_as_of_tms==source_df.as_of_tms),'left' )

df_result=df_result.select('No', 'client_id','Result', 'fund_region','as_of_tms')
df_result = df_result.dropDuplicates()
          

df_rule_reference =df_rule_reference.withColumnRenamed('DS Result','DS_Result')


####################Merging results with rule reference####################################################
df_merge_result=df_result.join(df_rule_reference,\
                                   (df_result.No==df_rule_reference.Rule_Number)\
                                         &  (df_result.Result==df_rule_reference.DS_Result),'left' )
    


###################Addition columns, evaluation_data_range and client identifier#########################
df_merge_result = df_merge_result.withColumn("Year", year(df_merge_result.as_of_tms).cast("string"))
df_merge_result = df_merge_result.withColumn("Month", month(df_merge_result.as_of_tms).cast("string"))
df_merge_result = df_merge_result.withColumn("Year1", year(df_merge_result.as_of_tms).cast("string"))
df_merge_result=df_merge_result.withColumn('key', lit("-"))
concat_udf = f.udf(lambda cols: "".join([x if x is not None else "*" for x in cols]), StringType())
df_merge_result = df_merge_result.withColumn("Evaluation_Date_Range", concat_udf(f.array("Year","key","Month")))
df_merge_result = df_merge_result.withColumn("Client_Identifier", concat_udf(f.array("client_id","key","fund_region")))


df_stats = df_stats.withColumn("Year", year(df_stats.as_of_tms).cast("string"))
df_stats = df_stats.withColumn("Month", month(df_stats.as_of_tms).cast("string"))
df_stats = df_stats.withColumn("Year1", year(df_stats.as_of_tms).cast("string"))
df_stats=df_stats.withColumn('key', lit("-"))



concat_udf = F.udf(lambda cols: "".join([x if x is not None else "*" for x in cols]), StringType())
df_stats = df_stats.withColumn("Evaluation_Date_Range", concat_udf(F.array("Year","key","Month")))
df_stats = df_stats.withColumn("Client_Identifier", concat_udf(F.array("client_id","fund_region")))
    
  
###############################################################################
#getting ppf value for deriving bussiness order followed column
##############################################################################

stats_val_df = df_stats.groupBy('client_id','pcs_step_cfg_ky','Evaluation_Date_Range','fund_region','Year','Month').agg((f.avg('weighted_time')).alias("meanval"),(f.stddev('weighted_time')).alias("sigma"),(f.count('Client_Identifier')).alias("count"))
  
    


stats_val_df = stats_val_df.withColumn("scale", stats_val_df.sigma * ((3 ** 0.5)/(np.pi)))

stats_val_df_round=stats_val_df.withColumn("meanval_round", f.round(stats_val_df["meanval"], 6))\
                                .withColumn("scale_round", f.round(stats_val_df["scale"], 6))\

from scipy.stats import logistic

def logistic_decimal(meanval, scale,count):
  if count > 1:
    #logistic_decimal= logistic.ppf(0.985,loc=meanval, scale=scale)
    logistic_decimal=meanval+scale
  else:
    logistic_decimal=0.0
  return (logistic_decimal)   

#udf_logistic_decimal = F.udf(lambda (meanval, scale): logistic_decimal(meanval, scale))

udf_logistic_decimal = f.udf(logistic_decimal,DoubleType())
stats_val_result = stats_val_df_round\
            .withColumn('logistic_decimal',udf_logistic_decimal('meanval_round','scale_round','count'))

stats_val_result = stats_val_result.withColumn('group',f.when(col('pcs_step_cfg_ky').isin('9873','11369','917','918'), 5)\
                               .when(col('pcs_step_cfg_ky') == '10099',4)\
                               .when(col('pcs_step_cfg_ky') == '10095', 3)\
                                .when(col('pcs_step_cfg_ky') == '9980', 2)\
                                .when(col('pcs_step_cfg_ky') == '4025', 1)\
                               .otherwise(0))

predefine_order_df=stats_val_result.filter(stats_val_result['group']!=0)
predefine_order_df=predefine_order_df.groupby('client_id','Evaluation_Date_Range','group','fund_region','Year','Month').agg((f.min('logistic_decimal')).alias("min"))
#w = Window.partitionBy("Column1").orderBy("Date_Converted")
#predefine_order_df = predefine_order_df.withColumn("Entity_Identifier", concat_udf(F.array("dshbd_fund_grp_ky","fund_region","Evaluation_Date_Range","group")))
column_list = ['client_id','Month','year','fund_region']
df_lag = predefine_order_df.withColumn('prev_val',
                        f.lag(predefine_order_df['min'])
                                 .over(Window.partitionBy([col(x) for x in column_list]).orderBy("group")))


df_lag= df_lag.withColumn("Sub", col("min")- (col("prev_val")))
df_lag=df_lag.withColumn('status',f.when(col('sub')<0,1)\
                        .otherwise(0))
df_overall_stats = df_lag.groupBy('client_id','Evaluation_Date_Range','fund_region').agg((f.max('status')).alias("status_label"))

df_overall_stats=df_overall_stats.withColumn('Overall_Status',f.when(col('status_label')== 0,'Business Order Followed')\
                        .when(col('status_label')== 1,'Business Order Not Followed')\
                        .otherwise("Not applicable"))                     

df_overall_stats=df_overall_stats.withColumnRenamed('client_id','R_client_id')\
                  .withColumnRenamed('fund_region','R_fund_region')\
                  .withColumnRenamed('Evaluation_Date_Range','R_Evaluation_Date_Range')

df_final_result=df_merge_result.join(df_overall_stats,\
                                   (df_merge_result.client_id==df_overall_stats.R_client_id)\
                                         &  (df_merge_result.fund_region==df_overall_stats.R_fund_region)\
                                     & (df_merge_result.Evaluation_Date_Range==df_overall_stats.R_Evaluation_Date_Range),'left' )
                            
df_final_result=df_final_result.join(source_name_df,\
                                   (df_final_result.client_id==source_name_df.name_client_id)\
                                         &  (df_final_result.fund_region==source_name_df.name_fund_region),'left' )
df_final_result = df_final_result.withColumn("date", f.to_date(f.col("as_of_tms")))  
#df_final_result = df_final_result.withColumn('client_name_ID', \
#                    f.concat(f.col('client_name'),f.lit(' ('), f.col('client_id'),f.lit(')')))


df_tableau=df_final_result.select("date","Client_Identifier","client_id", \
                                 "Step Key","Step Key caption","Rule_Number","Rule Description","Rule Description 2","Result" ,\
                                 "Result summary","Assessment Statement","Action","Insight Criticality","Evaluation_Date_Range", "Overall_Status")
df_tableau=df_tableau.withColumnRenamed("Client_Identifier","client_identifier") \
                      .withColumnRenamed("Step Key","step_key") \
                      .withColumnRenamed("Step Key caption","step_key_caption")\
                      .withColumnRenamed("Rule_Number","rule_number") \
                      .withColumnRenamed("Rule Description","rule_description") \
                      .withColumnRenamed("Rule Description 2","rule_description_2") \
                      .withColumnRenamed("Result","ds_result")\
                      .withColumnRenamed("Result summary","result_summary")\
                    .withColumnRenamed("Assessment Statement","assessment_statement")\
                .withColumnRenamed("Action","action")\
                .withColumnRenamed("Insight Criticality","insight_criticality")\
                .withColumnRenamed("Evaluation_Date_Range","evaluation_date_range")\
                .withColumnRenamed("Overall_Status","overall_status")\

max_time_df = df_tableau.select(f.max('date')).collect()[0][0]   
df_tableau=df_tableau.withColumn('end_of_period', lit(max_time_df))


df_tableau = df_tableau.withColumn("rule_number1", df_tableau.rule_number.cast("string"))
#df_tableau  = df_tableau .withColumn("join", month(df_stats.as_of_tms).cast("string"))
#df_stats = df_stats.withColumn("Year1", year(df_stats.as_of_tms).cast("string"))
df_tableau=df_tableau.withColumn('key', lit("-"))
df_tableau =df_tableau.withColumnRenamed('Result summary','result_summary')


df_tableau =df_tableau.withColumn('join', \
                    f.concat(f.col('rule_number1'),f.lit('-'), f.col('result_summary')))



########################requirement to stop invalid finding value#######################################
invalid=[u'9-invalid finding',u'14-invalid finding', u'18-invalid finding', u'21-invalid finding', u'23-invalid finding']

df_tableau = df_tableau.withColumn("result_summary1",when(col("join").isin(invalid), "Rule Not Applicable to Client ID").otherwise(col("result_summary")))

ic = [u'OK - Profile info']

df_tableau = df_tableau.withColumn("insight_criticality1",when(col("result_summary1").isin(ic), "Neutral").otherwise(col("insight_criticality")))


rg_a = [u'Error - Adoption issue',u'Error - Adoption issue (maybe)',  u'Error - Adoption issue (Legacy)', u'OK - No legacy tool',u'OK - Using Beacon tools']
rg_s = [u'Error - Sequence issue', u'OK - In sequence']
rg_t = [u'Error - Time gap',u'Error - Time issue',u'OK - Good Performer',u'OK - Comp Time Reasonable', u'OK - Profile info']


############################Assigning result group#####################################################
df_tableau = df_tableau.withColumn('result_group', f.when(col('result_summary1').isin(rg_a), "Adoption").when(col('result_summary1').isin(rg_s), "Sequence").when(col('result_summary1').isin(rg_t), "Timing").otherwise("N/A"))

#df_tableau=df_final_result.select("date","entity_identifier","dshbd_fund_grp_ky","fund_grp_nme","fund_grp_nme_grp_ky",\
                                # "Step Key","Step Key caption","Rule_Number","Rule Description","Rule Description 2","Result" ,\
                                # "Result summary","Assessment Statement","Action","Insight Criticality","Evaluation_Date_Range","Overall_Status")

df_tableau1 = df_tableau.filter(df_tableau.date.isNotNull()) 

df_tableau1=df_tableau1.select("date","client_identifier","client_id",\
                                 "step_key","rule_number","rule_description","rule_description_2","ds_result" ,\
                                 "result_summary1","assessment_statement","action","insight_criticality1","evaluation_date_range", "overall_status", "result_group" , "end_of_period")

df_tableau1 =df_tableau1.withColumnRenamed('result_summary1','result_summary')
df_tableau1 =df_tableau1.withColumnRenamed('insight_criticality1','insight_criticality')

df_tableau1 = df_tableau1.filter(df_tableau1.rule_number.isNotNull()) 
df_tableau1 = df_tableau1.withColumn('client_name', lit(None).cast(StringType()))
df_tableau1 = df_tableau1.withColumn('client_name_id', lit(None).cast(StringType()))
df_tableau1 = df_tableau1.withColumn('step_key_caption', lit(None).cast(StringType()))
df_tableau1 = df_tableau1.withColumn('pxo_client_name', lit(None).cast(StringType()))

df_tableau1=df_tableau1.select("date","client_identifier","client_id","client_name", "client_name_id",\
                                 "step_key","step_key_caption","rule_number","rule_description","rule_description_2","ds_result" ,\
                                 "result_summary","assessment_statement","action","insight_criticality","evaluation_date_range","pxo_client_name" ,"overall_status", "result_group" , "end_of_period")


data_output = sqlctxt.sql("select * from " + str(sys.argv[2]) + ".pxo_MyMetrics_20q")

max_time_op = data_output.select(f.max('end_of_period')).collect()[0][0]


hc = HiveContext(sc)
if max_time_op !=str(max_time_df):
  df_tableau1.write.mode ("overwrite").insertInto( str(sys.argv[2])  + ".pxo_MyMetrics_20q")
else:
  df_tableau1.write.mode ("overwrite").save(str(sys.argv[2]) + ".pxo_MyMetrics_20q")

sc.stop()