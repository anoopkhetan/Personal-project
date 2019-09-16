#!/bin/bash
###################################################################################
#
#   State Street Bank and Trust Company
#
####################################################################################
####################################################################################
#   Script Name:        pxo_myview_nds_wrapper.sh
#   Project:            Nav Now
#   
#
#   Purpose:
#   This script is a wrapper to run prediction pyspark scripts for MTMI, Pricing and NDS
#
#   Execution steps:
#   1.	Spark read historical data and after transformation write them to Hive table 
# 
######################################################################################
#   Change Log
#   Developer              Date        Description
#   ------------------------------------------------------------------------------
#
#   Anoop Khetan          10/11/2018   script created 
#  
#######################################################################################
#
# Return code 1 - Error while passing parameters to the wrapper script
#             2 - Error while reading properties file
#             3 - Error while executing mtmi_fund_level.py
#             4 - Error while executing mtmi_roll_up.py
#             5 - Error while executing pricing_fund_level.py
#             6 - Error while executing pricing_roll_up.py
#             7 - Error while executing nds_ticker_level_training.py
#             8 - Error while executing nds_fund_level_rollup.py
#             9 - Error while pxo_navnow_backup_prediction.hql
#--------------------------------------------------------------------------------------
#Sample command: 
# $PATH/mtmi_fund_level.sh $PATH/dev.properties 
#
#
#--------------------------------------------------------------------------------------

if [ $# != 1 ]
then
   echo "ERROR: please rerun the script with property file"
   exit 1
else
echo "Script started with command :$0 $1"
fi

echo "Start reading properties file $1"
source $1
  if [ $? != 0 ]
  then
  echo "ERROR: while reading properties file $1"
  exit 2
  fi

DATE=`date '+%Y-%m-%d'_'%H%M%S'`

STARTTIME=`date '+%m/%d/%y TIME:%H:%M:%S'`

echo "========= Script $0 started on $STARTTIME =============" 

echo "                                    " 
echo "Values passed in properties file  :"  
echo "                                    " 

echo "SERVERNAME             		: $SERVERNAME" 
echo "SERVERUSERID             		: $SERVERUSERID" 
echo "BEELINE_URL             		: $BEELINE_URL"
echo "HIVE_DB             			: $HIVE_DB"
echo "HIVE_DB_M            			: $HIVE_DB_M" 
echo "PYTHON_SCRIPT_PATH            : $PYTHON_SCRIPT_PATH" 
echo "HIVE_SCRIPT_PATH              : $HIVE_SCRIPT_PATH" 

echo "INFO: ______________ Kerberos Authentication for server $SERVERNAME and user_id $SERVERUSERID ______________" 


LD_LIBRARY_PATH=/opt/cloakware/cspmclient/lib
JAVA_HOME=/usr/java/default
SERVERPASSWORD=`/opt/cloakware/cspmclient/bin/cspmclient $SERVERNAME$SERVERUSERID | cut -d' ' -f3`
FLAGS=-Djavax.net.ssl.trustStore=/usr/local/admin/uat2_certs/gdch01-cluster02.truststore

echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "SERVERNAME: $SERVERNAME"
echo "SERVERUSERID: $SERVERUSERID"

echo "START: Run Kinit"
kinit ${SERVERUSERID} <<< `echo ${SERVERPASSWORD}`
klist

echo "INFO: ______________ pyspark script mtmi_fund_level.py execution started ______________" 
echo " " 
echo "spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/mtmi_fund_level.py $HIVE_DB_M $HIVE_DB"
echo " " 
spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/mtmi_fund_level.py $HIVE_DB_M $HIVE_DB
  if [ $? != 0 ]
  then
   echo "ERROR: while executing mtmi_fund_level.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 3
  fi
echo "INFO: ______________ pyspark script mtmi_fund_level.py execution completed ______________" 
echo " " 
echo "INFO: ______________ pyspark script mtmi_roll_up.py execution started ______________" 
echo " " 
echo "spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/mtmi_roll_up.py $HIVE_DB_M $HIVE_DB"
echo " " 
spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/mtmi_roll_up.py $HIVE_DB_M $HIVE_DB
  if [ $? != 0 ]
  then
   echo "ERROR: while executing mtmi_roll_up.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 4
  fi
echo "INFO: ______________ pyspark script mtmi_roll_up.py execution completed ______________" 
echo " " 
echo "INFO: ______________ pyspark script pricing_fund_level.py execution started ______________" 
echo " " 
echo "spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/pricing_fund_level.py $HIVE_DB_M $HIVE_DB"
echo " " 
spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/pricing_fund_level.py $HIVE_DB_M $HIVE_DB
  if [ $? != 0 ]
  then
   echo "ERROR: while executing pricing_fund_level.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 5
  fi
echo "INFO: ______________ pyspark script pricing_fund_level.py execution completed ______________" 
echo " " 
echo "INFO: ______________ pyspark script pricing_roll_up.py execution started ______________" 
echo " " 
echo "spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/pricing_roll_up.py $HIVE_DB_M $HIVE_DB"
echo " " 
spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/pricing_roll_up.py $HIVE_DB_M $HIVE_DB
  if [ $? != 0 ]
  then
   echo "ERROR: while executing pricing_roll_up.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 6
  fi
echo "INFO: ______________ pyspark script pricing_roll_up.py execution completed ______________" 
echo " " 
echo "INFO: ______________ pyspark script nds_ticker_level_training.py execution started ______________" 
echo " " 
echo "spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/nds_ticker_level_training.py $HIVE_DB_M $HIVE_DB"
echo " " 
spark2-submit --master yarn --deploy-mode $MODE --conf spark.driver.memoryOverhead=2048 --conf spark.yarn.executor.memoryOverhead=4096 $PYTHON_SCRIPT_PATH/nds_ticker_level_training.py $HIVE_DB_M $HIVE_DB
  if [ $? != 0 ]
  then
   echo "ERROR: while executing nds_ticker_level_training.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 7
  fi
echo "INFO: ______________ pyspark script nds_ticker_level_training.py execution completed ______________" 
echo " " 
echo "INFO: ______________ pyspark script nds_fund_level_rollup.py execution started ______________" 
echo " " 
echo "spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/nds_fund_level_rollup.py $HIVE_DB_M $HIVE_DB"
echo " " 
spark2-submit --master yarn --deploy-mode $MODE $PYTHON_SCRIPT_PATH/nds_fund_level_rollup.py $HIVE_DB_M $HIVE_DB
  if [ $? != 0 ]
  then
   echo "ERROR: while executing nds_fund_level_rollup.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 8
  fi
echo "INFO: ______________ pyspark script nds_fund_level_rollup.py execution completed ______________" 
echo " " 
echo "INFO:######### ___ predictions ran successfully for all events(MTMI, PRICING & NDS) ___ ##########" 
echo " "
echo " "
echo " " 
echo "INFO:######### ___ Backup started for all events(MTMI, PRICING & NDS) ___ ##########"
echo " " 
echo " " 
echo "$HIVE_SCRIPT_PATH/pxo_navnow_backup_prediction.hql $BEELINE_URL $HIVE_DB_M"
echo " " 
$HIVE_SCRIPT_PATH/pxo_navnow_backup_prediction.hql $BEELINE_URL $HIVE_DB_M
  if [ $? != 0 ]
  then
   echo "ERROR: while executing pxo_navnow_backup_prediction.hql " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 9
  fi
echo "INFO: ______________ pyspark script pxo_navnow_backup_prediction.hql execution completed ______________" 
echo " " 
echo "INFO:######### ___ Backup completed for all events(MTMI, PRICING & NDS)___ ##########" 
echo " " 
echo " " 
echo "INFO: ______________ script $0 is completed successfully ______________" 