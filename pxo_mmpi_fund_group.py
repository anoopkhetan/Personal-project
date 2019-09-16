from pyspark import SparkConf
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import udf, struct
from pyspark.sql.functions import col,when
import pyspark.sql.functions as f
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import mean,lit,lag
from datetime import date,datetime,timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.window import Window
import pyspark
import os,sys
import math
import calendar


####### creating spark context and hive context #######

conf = SparkConf()
conf.set('spark.executor.memory', '4g')
conf.set('spark.driver.memory','4g')
conf.set('spark.executor.cores', '4')

spark = SparkSession\
        .builder.config(conf=conf)\
        .appName("PI FundLevel Test")\
        .getOrCreate()\

sc = spark.sparkContext
sc.setLogLevel("INFO")
sqlctxt = SQLContext(sc)

####### Current month data or current iteration data for the analysis based on the time #######


first_day_of_current_month = date.today().replace(day=1)
last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)

first_day_of_previous_month = first_day_of_current_month + relativedelta(months=-1)

first_day_of_last_iteration=first_day_of_previous_month+ relativedelta(months=-1)

year_current_iteration = first_day_of_previous_month.year
month_current_iteration = first_day_of_previous_month.month

year_previous_iteration=first_day_of_last_iteration.year
month_previous_iteration=first_day_of_last_iteration.month

current_iteration_min =datetime(year_current_iteration,month_current_iteration,1).date()

previous_iteration_min=datetime(year_previous_iteration,month_previous_iteration,1).date()


####### Read the data from physicalized mart in PXO DATA LAKE #######

query="WITH t1 as (SELECT fund_id,pcs_step_cfg_ky,(HOUR(approved_time)*60*60 + MINUTE(approved_time)*60 + second(approved_time))/86400\
      as decimal_time,to_date(approved_time) as as_of_tms1 FROM  {}.pxo_dgf_pcs_steps_insts_favs_2m_phys_h_1 \
      where pcs_step_cfg_ky in (11369,10099,10095,9980,917,10001,918,9873,892,12770,911,4025,10099)) \
      select t2.fund_id,t2.fund_region,t2.client_id,t1.as_of_tms1,t1.decimal_time,t1.pcs_step_cfg_ky,'{}' as current_iteration_min from t1\
      join {}.pxo_dgf_phys_dshbd_fund_h_1 t2 on   t1.fund_id=t2.fund_id where t1.as_of_tms1 between '{}' and '{}'".format(sys.argv[1],current_iteration_min,sys.argv[1],previous_iteration_min,last_day_of_previous_month)


data=sqlctxt.sql(query)


#######  Rename the fund region values and fill null vaues with TBD   #######

data=data.withColumn('fund_region',f.when(col('fund_region')== 'NA','NA')\
                               .when(col('fund_region')== 'EUR','EUR')\
                               .when(col('fund_region')== 'PAC','PAC')\
                               .otherwise("TBD"))                     


####### Create a Unique Key for the analysis For fund level analysis fund_id and fund_region and pcs_step_cfg_ky combination is the Key #######


data=data.withColumn('Key',f.concat(data.fund_id.cast(StringType()),\
                lit('-'),data.fund_region.cast(StringType()),lit('-'),data.pcs_step_cfg_ky.cast(StringType())))


data=data.withColumn('as_of_tms1',data.as_of_tms1.cast(DateType()))
data=data.withColumn('current_iteration_min',data.current_iteration_min.cast(DateType()))

####### Check weather each Key has minimum 18 datapoints #######

def no_of_datapoints(datapoints_list):
  val=len(datapoints_list)
  return val
udf_datapoints=f.udf(no_of_datapoints)

data=data.withColumn('datapoints',f.collect_set('as_of_tms1').over(Window.partitionBy("Key")))
data=data.withColumn('no_of_datapoints',udf_datapoints(col('datapoints')))  

data=data.where(data.no_of_datapoints>18)

not_req_columns=('fund_region','pcs_step_cfg_ky','datapoints','no_of_datapoints')
data=data.drop(*not_req_columns)

####### To go back to before shift in the previous analysis time period filter the data accordingly #######

def after_transition(datapoint,previous_shift):
  val=1
  if previous_shift != 'null':
    if previous_shift is not None:
      if(datapoint < previous_shift):
        val=0
  return val
udf_after_transition=f.udf(after_transition,IntegerType())


####### Read the historical data to get the previous shifts based on that slice the data #######

historical_report=sqlctxt.sql("select fund_id,fund_region,pcs_step_cfg_ky,transition_date_mean,transition_datevariance from " + str(sys.argv[1]) + ".pxo_mymetrics_pi_fund_group")

if(historical_report):

  historical_report=historical_report.withColumn('Key',f.concat(historical_report.fund_id.cast(StringType()),\
                lit('-'),historical_report.fund_region.cast(StringType()),lit('-'),historical_report.pcs_step_cfg_ky.cast(StringType())))
  
  historical_report = historical_report.withColumn('transition_date_mean',historical_report.transition_date_mean.cast(DateType()))
  historical_report = historical_report.withColumn('transition_datevariance',historical_report.transition_datevariance.cast(DateType()))
  reportWindow=Window.partitionBy('Key')
  historical_report=historical_report.withColumn('transition_date_mean_max',f.max('transition_date_mean').over(reportWindow))
  historical_report=historical_report.withColumn('transition_date_var_max',f.max('transition_datevariance').over(reportWindow))
  historical_report=historical_report.select('Key','transition_date_mean_max','transition_date_var_max')
  data=data.join(historical_report,on='Key',how='left')
  data=data.withColumn('mean_in',udf_after_transition(col('as_of_tms1'),col('transition_date_mean_max')))
  data=data.withColumn('var_in',udf_after_transition(col('as_of_tms1'),col('transition_date_var_max')))
  not_req_cols=('transition_date_mean_max','transition_date_var_max')
  data=data.drop(*not_req_cols)

data=data.dropDuplicates(subset=['Key','as_of_tms1'])

####### count the number of mean shifts and var shifts #######

def leng(x):
  return len(x)

udf_len = f.udf(leng,IntegerType())

####### get the control limits for the both mean shift and variance shifts #######

def control_limits(df):
  clPartition=Window.partitionBy(["Key",'mean_in']).orderBy(col('Key'),col('mean_in'),col("as_of_tms1"))
  partitionWindow=Window.partitionBy(["Key",'mean_in'])
  cumSumPartition = Window.partitionBy(["Key",'mean_in']).orderBy("Key",'mean_in','as_of_tms1')
  df=df.fillna(0)
  df=df.withColumn("Xbar",mean(col("decimal_time")).over(partitionWindow))
  df=df.withColumn('Xchange',df.decimal_time-df.Xbar)
  df=df.withColumn('Xchange_value', when(col('Xchange') <0, 1).otherwise(0))
  df=df.withColumn("mean_change",(f.col("Xchange_value") != f.lag("Xchange_value").over(cumSumPartition)).cast("int")) \
      .fillna( 0,subset=["mean_change"]) \
      .withColumn("indicator",(~((f.col("mean_change") == 0) )).cast("int")) \
      .withColumn("Xchange_group",f.sum(f.col("indicator")).over(Window.partitionBy(['Key','mean_in']).orderBy("Key",'mean_in','as_of_tms1').\
                                                                 rangeBetween(Window.unboundedPreceding, 0)) )
  df=df.withColumn('group_mean',f.count(df.Xchange_group).over(Window.partitionBy(['Key',"mean_in",'Xchange_group'])))
  
  df=df.withColumn('group_mean',f.when(col('mean_in')==1,df.group_mean).otherwise(0))
  
  clPartition=Window.partitionBy(["Key",'var_in']).orderBy(col('Key'),col('var_in'),col("as_of_tms1"))
  partitionWindow=Window.partitionBy(["Key",'var_in'])
  cumSumPartition = Window.partitionBy(["Key",'var_in']).orderBy("Key",'var_in','as_of_tms1')
  mean_drop_columns=['Xbar','Xchange','Xchange_value','mean_change','indicator','Xchange_group']
  df=df.drop(*mean_drop_columns)
  df=df.withColumn("shift_decimal_time",lag(col("decimal_time"),1).over(clPartition))
  df=df.withColumn('MR',f.abs(df.shift_decimal_time-df.decimal_time))
  df=df.fillna(0,subset=['MR'])
  df=df.withColumn("MRbar",mean(col("MR")).over(partitionWindow))
  df=df.withColumn('MRchange',df.MR-df.MRbar)
  df=df.withColumn('MRchange_value', when(col('MRchange') <0, 1).otherwise(0))
  df=df.withColumn("var_change",(f.col("MRchange_value") != f.lag("MRchange_value").over(cumSumPartition)).cast("int")) \
      .fillna( 0,subset=["var_change"]) \
      .withColumn("indicator_var",(~((f.col("var_change") == 0) )).cast("int")) \
      .withColumn("Mrchange_group",f.sum(f.col("indicator_var")).over(Window.partitionBy(['Key','var_in']).orderBy("Key",'var_in','as_of_tms1').\
                                                                      rangeBetween(Window.unboundedPreceding, 0)) )
  df=df.withColumn('group_var',f.count(df.Mrchange_group).over(Window.partitionBy(['Key','var_in','Mrchange_group'])))
  df=df.withColumn('group_var',f.when(col('var_in')==1,df.group_var).otherwise(0))
  var_drop_columns=['shift_decimal_time','MRbar','MRchange','MRchange_value','var_change','indicator_var','Mrchange_group']
  df=df.drop(*var_drop_columns)
  return df

data=control_limits(data)

data=data.persist(pyspark.StorageLevel.MEMORY_AND_DISK)


####### get the shift dates in the present month #######

def dates(x,y):
  dates=[]
  if(len(x)):
    dates=[i for i in x if i>=y ]
  dates=sorted(dates)
  return dates

udf_dates=f.udf(dates,ArrayType(DateType()))


####### get the shift end dates #######

def shift_end_dates(start_list,end_list):
  start=sorted(start_list)[0]
  for date in end_list:
    if(start>date):
      end_list.remove(date)
  end_list=sorted(end_list)
  return end_list

udf_end_dates=f.udf(shift_end_dates,ArrayType(DateType()))

####### get the shift start and end dates for both mean and variance shifts #######

def recent_detected_shifts(detected_list,no_of_shifts):
  if(no_of_shifts>2):
    detected_list=[i for i in detected_list]
    detected_list=[detected_list[-2],detected_list[-1]]
  return detected_list
udf_recent_detected=f.udf(recent_detected_shifts,ArrayType(DateType()))


def mean_shift_dates(df):
  partitionKey = Window.partitionBy(['Key'])
  mean_groupWindow=Window.partitionBy(['Key','group_mean'])
  df=df.where(col('mean_in')==1)
  df=df.withColumn('group_mean',f.when(col('group_mean')>8,df.group_mean).otherwise(None))
  df=df.withColumn('mean_shift',f.when(col('group_mean')>8,df.as_of_tms1).otherwise(None))
  df=df.withColumn('mean_start_date',f.min(df.mean_shift).over(mean_groupWindow))
  df=df.withColumn('mean_end_date',f.max(df.mean_shift).over(mean_groupWindow))
  df=df.withColumn('mean_distinct_start',f.collect_set('mean_start_date').over(partitionKey))
  df=df.withColumn('mean_distinct_end',f.collect_set('mean_end_date').over(partitionKey))
  df=df.withColumn('mean_detected_start',udf_dates(col('mean_distinct_start'),col('current_iteration_min')))
  df=df.withColumn('no_of_mean_shifts',udf_len(df.mean_detected_start))
  data_no_mean_shifts=df.where(col('no_of_mean_shifts')<1)
  df=df.where(col('no_of_mean_shifts')>0)
  df=df.withColumn('mean_detected_end',udf_end_dates(col('mean_detected_start'),col('mean_distinct_end')))
  no_req_cols=['group_mean','group_var','mean_shift','mean_start_date','mean_end_date','mean_distinct_start','mean_distinct_end']
  df=df.drop(*no_req_cols)
  return df,data_no_mean_shifts

def var_shift_dates(df):
  partitionKey = Window.partitionBy(['Key'])
  var_groupWindow=Window.partitionBy(['Key','group_var'])
  df=df.where(col('var_in')==1)
  df=df.withColumn('group_var',f.when(col('group_var')>8,df.group_var).otherwise(None))
  df=df.withColumn('var_shift',f.when(col('group_var')>8,df.as_of_tms1).otherwise(None))
  df=df.withColumn('var_start_date',f.min(df.var_shift).over(var_groupWindow))
  df=df.withColumn('var_end_date',f.max(df.var_shift).over(var_groupWindow))
  df=df.withColumn('var_distinct_start',f.collect_set('var_start_date').over(partitionKey))
  df=df.withColumn('var_distinct_end',f.collect_set('var_end_date').over(partitionKey))
  df=df.withColumn('var_detected_start',udf_dates(col('var_distinct_start'),col('current_iteration_min')))
  df=df.withColumn('no_of_variance_shifts',udf_len(df.var_detected_start))
  data_no_var_shifts=df.where(col('no_of_variance_shifts')<1)
  df=df.where(col('no_of_variance_shifts')>0)
  df=df.withColumn('var_detected_end',udf_end_dates(col('var_detected_start'),col('var_distinct_end')))
  no_req_cols=['group_var','group_mean','var_shift','var_start_date','var_end_date','var_distinct_start','var_distinct_end']
  df=df.drop(*no_req_cols)
  return df,data_no_var_shifts

data_mean,data_no_mean_shifts=mean_shift_dates(data)

data_var,data_no_var_shifts=var_shift_dates(data)

data_no_mean_shifts=data_no_mean_shifts.where(col('as_of_tms1')>=col('current_iteration_min'))
data_no_var_shifts=data_no_var_shifts.where(col('as_of_tms1')>=col('current_iteration_min'))

data_no_mean_shifts=data_no_mean_shifts.groupBy('Key').agg({'decimal_time':'avg'}).withColumnRenamed('avg(decimal_time)','mean_before')
data_no_var_shifts=data_no_var_shifts.groupBy('Key').agg({'MR':'avg'}).withColumnRenamed('avg(MR)','variance_before')


def ninepoint_check(start,all_points):
  points=len([date for date in all_points if date<start])

  return points

udf_ninepoint_check=f.udf(ninepoint_check)


def recent_detected_shifts(detected_list,no_of_shifts):
  if(no_of_shifts>2):
    detected_list=[i for i in detected_list]
    detected_list=[detected_list[-2],detected_list[-1]]
  return detected_list
udf_recent_detected=f.udf(recent_detected_shifts,ArrayType(DateType()))

data_mean=data_mean.withColumn('mean_detected_start',udf_recent_detected(col('mean_detected_start'),col('no_of_mean_shifts')))
data_mean=data_mean.withColumn('mean_detected_end',udf_recent_detected(col('mean_detected_end'),col('no_of_mean_shifts')))
data_mean=data_mean.withColumn('no_of_mean_shifts',udf_len(col('mean_detected_start')))

data_var=data_var.withColumn('var_detected_start',udf_recent_detected(col('var_detected_start'),col('no_of_variance_shifts')))
data_var=data_var.withColumn('var_detected_end',udf_recent_detected(col('var_detected_end'),col('no_of_variance_shifts')))
data_var=data_var.withColumn('no_of_variance_shifts',udf_len(col('var_detected_start')))


####### Get the Max number of detected shifts #######

data_mean=data_mean.select('Key','as_of_tms1','no_of_mean_shifts','mean_detected_start','mean_detected_end','decimal_time','current_iteration_min')
data_var=data_var.select('Key','as_of_tms1','no_of_variance_shifts','var_detected_start','var_detected_end','MR','current_iteration_min')

data_mean=data_mean.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
data_var=data_var.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

df_mean=data_mean.where(col('as_of_tms1')>=col('current_iteration_min')).dropDuplicates(subset=['Key'])
max_mean_shifts=df_mean.agg({"no_of_mean_shifts": "max"}).collect()[0][0]

df_var=data_var.where(col('as_of_tms1')>=col('current_iteration_min')).dropDuplicates(subset=['Key'])
max_var_shifts=df_var.agg({"no_of_variance_shifts": "max"}).collect()[0][0]



####### Check for nine points before every shift if yes keep all detected dates if not remove the dates from the list #######

def ninepoint_check(start,all_points):
  points=len([date for date in all_points if date<start])

  return points

udf_ninepoint_check=f.udf(ninepoint_check)


def shifts_validation(df,shift_no,detect_start,detect_end,no_of_shifts,process_type,before_avg,before_std,after_avg,after_std):
  df_before_after=None
  partitionWindow=Window.partitionBy(["Key"])
  df=df.withColumn('detect1',df[detect_start].getItem(shift_no))
  df=df.withColumn('end1',df[detect_end].getItem(shift_no))
  df=df.withColumn('all_points',f.collect_set('as_of_tms1').over(partitionWindow))
  df=df.withColumn('before_points',udf_ninepoint_check(col('detect1'),col('all_points')))
  df=df.filter(df['before_points']>=9)
  if(df):
    df=df.withColumn('before_bool',f.when(col('as_of_tms1')<col('detect1'), 0).otherwise(1) )
    df=df.withColumn('ba_list', f.collect_list(process_type).over(Window.partitionBy(["Key","before_bool"])))
    df=df.withColumn('ba_mean', f.mean(process_type).over(Window.partitionBy(["Key","before_bool"])))
    df=df.withColumn('ba_std', f.stddev(process_type).over(Window.partitionBy(["Key","before_bool"])))
    df=df.withColumn('ba_len', f.count(process_type).over(Window.partitionBy(["Key","before_bool"])))
    df=df.withColumn('ba_len',col('ba_len').cast(DoubleType()))
    window_asce = Window().partitionBy("Key")
    df=df.sort(['Key','before_bool'],ascending=True)
    df=df.withColumn('before_list',f.first( col('ba_list')).over(window_asce))
    df=df.withColumn('after_list',f.last( col('ba_list')).over(window_asce))
    df=df.withColumn(before_avg,f.first( col('ba_mean')).over(window_asce))
    df=df.withColumn(after_avg,f.last( col('ba_mean')).over(window_asce))
    df=df.withColumn(before_std,f.first( col('ba_std')).over(window_asce))
    df=df.withColumn(after_std,f.last( col('ba_std')).over(window_asce))
    df=df.withColumn('before_len',f.first( col('ba_len')).over(window_asce))
    df=df.withColumn('after_len',f.last( col('ba_len')).over(window_asce))
    df=df.withColumn('before_sqrt',f.sqrt( col('before_len')))
    df=df.withColumn('after_sqrt',f.sqrt( col('after_len')))
    df=df.withColumn('before_ci_lower',col(before_avg)-(1.96*(col(before_std))/col('before_sqrt')))
    df=df.withColumn('before_ci_upper',col(before_avg)+(1.96*(col(before_std))/col('before_sqrt')))
    df=df.withColumn('after_ci_lower',col(after_avg)-(1.96*(col(after_std))/col('after_sqrt')))
    df=df.withColumn('after_ci_upper',col(after_avg)+(1.96*(col(after_std))/col('after_sqrt')))
    df=df.withColumn('significance',f.when(((f.round(col('before_ci_lower'),4)<f.round(col('after_ci_lower'),4)) & \
                                            (f.round(col('after_ci_lower'),4)<f.round(col('before_ci_upper'),4)))| \
                         ((f.round(col('before_ci_lower'),4)<f.round(col('after_ci_upper'),4)) &\
                          (f.round(col('after_ci_upper'),4)<f.round(col('before_ci_upper'),4))),1 ).otherwise(0))
    df=df.where(col('as_of_tms1')>=col('current_iteration_min'))
    df=df.withColumn('significance',f.when(f.round(col(before_avg),4)==f.round(col(after_avg),4),0).otherwise(1))
    df=df.withColumn(after_avg,f.when(col('significance')==0,df[before_avg]).otherwise(df[after_avg]))
    df=df.withColumn('detect1',f.when(col('significance')==0,None).otherwise(df['detect1']))
    df=df.select('Key','significance','detect1',before_avg,before_std,after_avg,after_std,no_of_shifts)
    df=df.dropDuplicates(subset=['Key'])
  return df


def multiple_shifts_validation(df,no_of_shifts,max_shifts,detect_start,detect_end,process_type,before_avg,before_std,after_avg,after_std):
  partitionWindow=Window.partitionBy("Key")
  df_before_after=None
  for i in range(0,max_shifts):
    if(i==0):
      df_one_shift=df.where(col(no_of_shifts)==i+1)
      df_before_after=shifts_validation(df_one_shift,i,detect_start,detect_end,no_of_shifts,process_type,before_avg,before_std,after_avg,after_std)
    elif(i==1):
      df_two_shifts=df.where(col(no_of_shifts)==i+1)
      
      ## validate the first shift out of two shifts

      df_first_shift=shifts_validation(df_two_shifts,i-1,detect_start,detect_end,no_of_shifts,process_type,before_avg,before_std,after_avg,after_std)
      
      df_first_shift=df_first_shift.drop(no_of_shifts)
      
      df_two_shifts=df_two_shifts.join(df_first_shift,on='Key',how='inner')

      ## filter the clients with first shift is not significant so we can check second shift valid or not

      df_first_shift_not_valid=df_two_shifts.where(col('significance')==0)
      if(df_first_shift_not_valid):
        df_second_shift_before_after=shifts_validation(df_first_shift_not_valid,i,detect_start,detect_end,no_of_shifts,process_type,before_avg,before_std,after_avg,after_std)
  
      ## filter the clients with second shift validated and append data to master dataframe
        df_second_shift_valid_before_after=df_second_shift_before_after.where(df_second_shift_before_after.significance==1)
        df_before_after=df_before_after.union(df_second_shift_valid_before_after)

      ## filter the clients with first shift valid AND Check for the second shift validated or not

      df_first_shift_valid=df_two_shifts.where(col('significance')==1)
      if(df_first_shift_valid):
        df_first_shift_valid=df_first_shift_valid.where(col('as_of_tms1')>col('detect1'))
        df_temp2_before_after=shifts_validation(df_first_shift_valid,i,detect_start,detect_end,no_of_shifts,process_type,before_avg,before_std,after_avg,after_std)
      
        if(df_temp2_before_after):
          df_temp2_before_after=df_temp2_before_after.drop(no_of_shifts)
          df_temp2_before_after=df_temp2_before_after.withColumnRenamed('significance','significance2')
          df_temp2_before_after=df_temp2_before_after.withColumnRenamed('detect1','detect2')
          df_temp2_before_after=df_temp2_before_after.withColumnRenamed(before_avg,'before_mean2')
          df_temp2_before_after=df_temp2_before_after.withColumnRenamed(before_std,'before_std2')
          df_temp2_before_after=df_temp2_before_after.withColumnRenamed(after_avg,'after_mean2')
          df_temp2_before_after=df_temp2_before_after.withColumnRenamed(after_std,'after_std2')

          df_temp_final=df_first_shift_valid.join(df_temp2_before_after,on='Key',how='inner')
          df_temp_final=df_temp_final.dropDuplicates(subset=['Key'])

           
          ####### Filter the clients with first shift valid and second shift is not valid  #######
          df_temp_one=df_temp_final.where((df_temp_final.significance==1) &(df_temp_final.significance2==0))
          if(df_temp_one):
            df_temp_one=df_temp_one.select('Key','significance','detect1',before_avg,before_std,after_avg,after_std,no_of_shifts)
            df_before_after=df_before_after.union(df_temp_one)

          ####### Filter the clients with both shifts valid and get the most recent shift values  #######

          df_temp_two=df_temp_final.where((df_temp_final.significance==1) &(df_temp_final.significance2==1))
          if(df_temp_two):
            df_temp_two=df_temp_two.select('Key','significance2','detect2','before_mean2','before_std2','after_mean2','after_std2',no_of_shifts)
            df_temp_two=df_temp_two.withColumnRenamed('significance2','significance')
            df_temp_two=df_temp_two.withColumnRenamed('detect2','detect1')
            df_temp_two=df_temp_two.withColumnRenamed('before_mean2',before_avg)
            df_temp_two=df_temp_two.withColumnRenamed('before_std2',before_std)
            df_temp_two=df_temp_two.withColumnRenamed('after_mean2',after_avg)
            df_temp_two=df_temp_two.withColumnRenamed('after_std2',after_std)
            df_temp_two=df_temp_two.select('Key','significance','detect1',before_avg,before_std,after_avg,after_std,no_of_shifts)
            df_before_after=df_before_after.union(df_temp_two)

  return df_before_after



df_mean_before_after=multiple_shifts_validation(data_mean,'no_of_mean_shifts',max_mean_shifts,'mean_detected_start',\
                                                'mean_detected_end','decimal_time','mean_before','before_std','mean_after','after_std')

df_mean_before_after=df_mean_before_after.withColumnRenamed('significance','valid_mean_shifts')

df_mean_before_after=df_mean_before_after.withColumnRenamed('detect1','transition_date_mean')
df_mean_before_after=df_mean_before_after.withColumn('mean_in1',lit(1))

df_mean_before_after=df_mean_before_after.persist(pyspark.StorageLevel.MEMORY_AND_DISK)


data_mean=data_mean.where(col('as_of_tms1')>=col('current_iteration_min'))

data_mean=data_mean.join(df_mean_before_after.select('Key','mean_in1'),on='Key',how='left')

data_mean=data_mean.fillna(0,subset=['mean_in1'])

data_no_mean_shifts1=data_mean.where(col('mean_in1')==0)

if(data_no_mean_shifts1):
  data_no_mean_shifts1=data_no_mean_shifts1.groupBy('Key').agg({'decimal_time':'avg'}).withColumnRenamed('avg(decimal_time)','mean_before')
  data_no_mean_shifts=data_no_mean_shifts.union(data_no_mean_shifts1)
  
  


df_var_before_after=multiple_shifts_validation(data_var,'no_of_variance_shifts',max_var_shifts,'var_detected_start',
                                               'var_detected_end','MR','variance_before','var_before_std','variance_after','var_after_std')

df_var_before_after=df_var_before_after.withColumn('var_in1',lit(1))

df_var_before_after=df_var_before_after.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

data_var=data_var.where(col('as_of_tms1')>=col('current_iteration_min'))

data_var=data_var.join(df_var_before_after.select('Key','var_in1'),on='Key',how='left')

data_var=data_var.fillna(0,subset=['var_in1'])

data_no_var_shifts1=data_var.where(col('var_in1')==0)

if(data_no_var_shifts1):
  data_no_var_shifts1=data_no_var_shifts1.groupBy('Key').agg({'MR':'avg'}).withColumnRenamed('avg(MR)','variance_before')
  data_no_var_shifts=data_no_var_shifts.union(data_no_var_shifts1)
  

df_var_before_after=df_var_before_after.withColumnRenamed('significance','valid_variance_shifts')

df_var_before_after=df_var_before_after.withColumnRenamed('detect1','transition_datevariance')


def add_mean_columns(df):
  df=df.withColumn('no_of_mean_shifts',lit(0))
  df=df.withColumn('valid_mean_shifts',lit(0))
  df=df.withColumn('transition_date_mean',lit(None).cast(DateType()))
  df=df.withColumn('mean_after',df['mean_before'])
  df=df.withColumn('change_mean',lit(0).cast(FloatType()))
  df=df.withColumn('change_mean_per',lit(0).cast(DoubleType()))
  df=df.withColumn('mean_improvement_status',lit('Same'))
  df=df.withColumn('before_std',lit(0).cast(DoubleType()))
  df=df.withColumn('after_std',lit(0).cast(DoubleType()))
  return df

def add_var_columns(df):
  df=df.withColumn('no_of_variance_shifts',lit(0))
  df=df.withColumn('valid_variance_shifts',lit(0))
  df=df.withColumn('transition_datevariance',lit(None).cast(DateType()))
  df=df.withColumn('variance_after',df['variance_before'])
  df=df.withColumn('change_variance',lit(0).cast(FloatType()))
  df=df.withColumn('change_variance_per',lit(0).cast(DoubleType()))
  df=df.withColumn('variance_improvement_status',lit('Same'))  
  return df


data_no_mean_shifts=add_mean_columns(data_no_mean_shifts)

data_no_var_shifts=add_var_columns(data_no_var_shifts)




df_mean_before_after=df_mean_before_after.withColumn('change_mean',df_mean_before_after.mean_after-df_mean_before_after.mean_before)
df_mean_before_after=df_mean_before_after.withColumn('change_mean_per',(df_mean_before_after.change_mean/df_mean_before_after.mean_before)*100)
df_mean_before_after=df_mean_before_after.withColumn('mean_improvement_status',f.when(col('change_mean_per')> 0,'Regressed')\
                               .when(col('change_mean_per')<0,'Improved')\
                               .otherwise("Same"))                     

df_var_before_after=df_var_before_after.withColumn('change_variance',df_var_before_after.variance_after-df_var_before_after.variance_before)
df_var_before_after=df_var_before_after.withColumn('change_variance_per',(df_var_before_after.change_variance/df_var_before_after.variance_before)*100)
df_var_before_after=df_var_before_after.withColumn('variance_improvement_status',f.when(col('change_variance_per')> 0,'Regressed')\
                               .when(col('change_variance_per')<0,'Improved')\
                               .otherwise("Same"))    



####### some of the required columns for making the final report #######

mean_columns=("Key","no_of_mean_shifts","valid_mean_shifts","transition_date_mean","mean_before","mean_after",\
              "change_mean","change_mean_per","mean_improvement_status","before_std","after_std")


data_no_mean_shifts=data_no_mean_shifts.select(*mean_columns)

df_mean_before_after=df_mean_before_after.select(*mean_columns)

df_mean_final=data_no_mean_shifts.union(df_mean_before_after)



var_columns=("Key","no_of_variance_shifts","valid_variance_shifts","transition_datevariance","variance_before",\
             "variance_after","change_variance","change_variance_per","variance_improvement_status")

data_no_var_shifts=data_no_var_shifts.select(*var_columns)

df_var_before_after=df_var_before_after.select(*var_columns)

df_var_final=data_no_var_shifts.union(df_var_before_after)


report_final=df_mean_final.join(df_var_final,on='Key',how='inner')

report_final=report_final.dropDuplicates(subset=['Key'])


def overall_status(mean_status,var_status,mean_before,mean_after,std_before,std_after):
    val=None
    if(mean_status==var_status):
        val=mean_status
    elif((mean_status=='Same') & (var_status=='Improved')):
        val='Improved'
    elif((mean_status=='Improved')&(var_status=='Same')):
        val='Improved'
    elif((mean_status=='Same')&(var_status=='Regressed')):
        val='Regressed'
    elif((mean_status=='Regressed')&(var_status=='Same')):
        val='Regressed'
    elif(((mean_status=='Regressed')&(var_status=='Improved')) | ((mean_status=='Improved')&(var_status=='Regressed'))):
        USL=0.833333
        Cpk_after=(USL-mean_after)/(3*std_after)
        Cpk_before=(USL-mean_before)/(3*std_before)
        if(Cpk_before>Cpk_after):
          val='Regressed'
        elif(Cpk_before<Cpk_after):
          val='Improved'
    return val

udf_overall_status=f.udf(overall_status)


report_final=report_final.withColumn('overall_improvement_status',udf_overall_status(col('mean_improvement_status'),col('variance_improvement_status'),\
                                 col('mean_before'),col('mean_after'),col('before_std'),col('after_std')))

####### drop the columns which are not required #######

drop_columns=('before_std','after_std')
report_final=report_final.drop(*drop_columns)


month_name=calendar.month_name[month_current_iteration]

data_range='To '+month_name+' '+str(year_current_iteration)

report_final=report_final.withColumn('data_range',lit(data_range))

report_final=report_final.withColumn('mch_client_id_client_name',lit(None))
report_final=report_final.withColumn('client_name_roll_up',lit(None))
report_final=report_final.withColumn('top_60yn',lit(None))
report_final=report_final.withColumn('pxo_client_name',lit(None))

report_final = report_final.withColumn('fund_id', f.split(report_final['Key'], '\-')[0])
report_final = report_final.withColumn('fund_region', f.split(report_final['Key'], '\-')[1])
report_final = report_final.withColumn('pcs_step_cfg_ky',f.split(report_final['Key'], '\-')[2])

report_final=report_final.withColumn('fund_name',lit(None))


data_mapping=data.select('fund_id','client_id').dropDuplicates(subset=['fund_id'])

report_final=report_final.join(data_mapping,on='fund_id',how='left')


report_final=report_final.dropDuplicates(subset=['fund_id','pcs_step_cfg_ky','fund_region'])

report_final=report_final.withColumn('process_step',lit(None))

report_final=report_final.withColumnRenamed('client_id','mch_client_id')


####### Re order the columns as per the report to write into hive tables #######

report_columns_order=('fund_id','fund_name','fund_region','pcs_step_cfg_ky','process_step','mch_client_id','mch_client_id_client_name','pxo_client_name',\
                     'top_60yn','no_of_mean_shifts','valid_mean_shifts','transition_date_mean','mean_before','mean_after','change_mean','change_mean_per',\
                     'mean_improvement_status','no_of_variance_shifts','valid_variance_shifts','transition_datevariance',\
                     'variance_before','variance_after','change_variance','change_variance_per','variance_improvement_status',\
                     'overall_improvement_status','data_range')

report_final=report_final.select(*report_columns_order)

def get_time(decimal_time):
  
  decimal_time=abs(decimal_time)
  decimal_time=round(decimal_time,4)
  sec = decimal_time * 86400
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
      
get_time_udf=f.udf(get_time,StringType())


report_final=report_final.withColumn('mean_before',get_time_udf(col('mean_before')))
report_final=report_final.withColumn('mean_after',get_time_udf(col('mean_after')))
report_final=report_final.withColumn('change_mean',get_time_udf(col('change_mean')))
report_final=report_final.withColumn('variance_before',get_time_udf(col('variance_before')))
report_final=report_final.withColumn('variance_after',get_time_udf(col('variance_after')))
report_final=report_final.withColumn('change_variance',get_time_udf(col('change_variance')))



report_final.select('fund_id','fund_name','fund_region','pcs_step_cfg_ky','process_step','mch_client_id','mch_client_id_client_name','pxo_client_name',\
                     'top_60yn','no_of_mean_shifts','valid_mean_shifts','transition_date_mean','mean_before','mean_after','change_mean','change_mean_per',\
                     'mean_improvement_status','no_of_variance_shifts','valid_variance_shifts','transition_datevariance',\
                     'variance_before','variance_after','change_variance','change_variance_per','variance_improvement_status',\
                     'overall_improvement_status','data_range').\
                                 write.insertInto(str(sys.argv[1]) + ".pxo_mymetrics_pi_fund_group")
sc.stop
