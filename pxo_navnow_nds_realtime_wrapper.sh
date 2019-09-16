#!/bin/bash
#######################################################################################
#
#   State Street Bank and Trust Company
#
########################################################################################
########################################################################################
#   Script Name:        pxo_navnow_nds_realtime_wrapper.sh
#   Project:            Nav Now
#   
#
#   Purpose:
#   This script is a wrapper to run nds real-time pyspark script
#
#   Execution steps:
#   1.	Spark streaming job read real-time data from flume sink and after transformation 
#       write final output into Hive table
# 
##########################################################################################
#   Change Log
#   Developer              Date        Description
#   ------------------------------------------------------------------------------
#
#   Anoop Khetan          03/05/2019   script created 
#  
###########################################################################################
#
# Return code 1 - Error while passing parameters to the wrapper script
#             2 - Error while reading properties file
#             3 - Error while executing xnd_nds.py
#             
#--------------------------------------------------------------------------------------
#Sample command: 
# $PATH/pxo_navnow_nds_realtime_wrapper.sh $PATH/dev-nds.properties 
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
echo "DB_SERVERNAME             	: $DB_SERVERNAME" 
echo "DB_SERVERUSERID             	: $DB_SERVERUSERID" 
echo "DB_SERVICENAME             	: $DB_SERVICENAME" 
echo "DB_NAME             			: $DB_NAME" 
echo "DB_SCHEMA             		: $DB_SCHEMA" 
echo "DB_SERVERNAME_G             	: $DB_SERVERNAME_G" 
echo "DB_SERVERUSERID_G             : $DB_SERVERUSERID_G" 
echo "DB_SERVICENAME_G             	: $DB_SERVICENAME_G" 
echo "DB_NAME_G             	    : $DB_NAME_G" 
echo "MODE           			    : $MODE" 
echo "HIVE_DB           			: $HIVE_DB" 
echo "HIVE_DB_M            			: $HIVE_DB_M" 
echo "PYTHON_SCRIPT_PATH            : $PYTHON_SCRIPT_PATH" 
echo "LOG_PATH             			: $LOG_PATH" 
echo "IMPALA_URL                    : $IMPALA_URL"
echo "ENV                           : $ENV"

echo "INFO: ______________ Kerberos Authentication for server $SERVERNAME and user_id $SERVERUSERID ______________" 


JAVA_HOME=/usr/java/default
SERVERPASSWORD=`/opt/cloakware/cspmclient/bin/cspmclient $SERVERNAME$SERVERUSERID | cut -d' ' -f3`

echo "SERVERNAME: $SERVERNAME"
echo "SERVERUSERID: $SERVERUSERID"

RC=$?
if [ ${#SERVERPASSWORD} -gt 3 ]; then
    echo "Return Code:[$RC]  Result OK: User ID:[$SERVERUSERID] Password Length: ${#SERVERPASSWORD}"
else
    echo "Something went WRONG; Return Code:[$RC]  Result NOT OK; User ID:[$SERVERUSERID] Password Length: ${#SERVERPASSWORD} NOT GREATER than 3 " 
    exit 100
fi

echo "START: Run Kinit"
kinit ${SERVERUSERID} <<< `echo ${SERVERPASSWORD}`
klist

echo "INFO: ______________ Generate password from clockware for NDS server $DB_SERVERNAME and user_id $DB_SERVERUSERID ______________" 

DB_SERVERPASSWORD=`/opt/cloakware/cspmclient/bin/cspmclient $DB_SERVERNAME$DB_SERVERUSERID | cut -d' ' -f3`

echo "DB_SERVERNAME: $DB_SERVERNAME"
echo "DB_SERVERUSERID: $DB_SERVERUSERID"

RC=$?
if [ ${#DB_SERVERPASSWORD} -gt 3 ]; then
    echo "Return Code:[$RC]  Result OK: User ID:[$DB_SERVERUSERID] Password Length: ${#DB_SERVERPASSWORD}"
else
    echo "Something went WRONG; Return Code:[$RC]  Result NOT OK; User ID:[$DB_SERVERUSERID] Password Length: ${#DB_SERVERPASSWORD} NOT GREATER than 3 " 
    exit 200
fi

echo "INFO: ______________ Generate password from clockware for Geneous server $DB_SERVERNAME_G and user_id $DB_SERVERUSERID_G ______________" 

DB_SERVERPASSWORD_G=`/opt/cloakware/cspmclient/bin/cspmclient $DB_SERVERNAME_G$DB_SERVERUSERID_G | cut -d' ' -f3`

echo "DB_SERVERNAME_G: $DB_SERVERNAME_G"
echo "DB_SERVERUSERID_G: $DB_SERVERUSERID_G"

RC=$?
if [ ${#DB_SERVERPASSWORD_G} -gt 3 ]; then
    echo "Return Code:[$RC]  Result OK: User ID:[$DB_SERVERUSERID_G] Password Length: ${#DB_SERVERPASSWORD_G}"
else
    echo "Something went WRONG; Return Code:[$RC]  Result NOT OK; User ID:[$DB_SERVERUSERID_G] Password Length: ${#DB_SERVERPASSWORD_G} NOT GREATER than 3 " 
    exit 300
fi


echo "INFO: ______________ pyspark script xnd_nds.py execution started ______________" 
echo " " 

echo "spark2-submit \
--master yarn \
--deploy-mode $MODE \
--supervise \
--num-executors 2 \
--executor-cores 2 \
--driver-memory 4g \
--executor-memory 4g \
--conf spark.driver.memoryOverhead=2048 \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.dynamicAllocation.maxExecutors=4 \
--jars /opt/cloudera/parcels/CDH/lib/sqoop/lib/ojdbc6.jar \
$PYTHON_SCRIPT_PATH/xnd_nds.py \
$HIVE_DB_M $LOG_PATH $DB_SERVERUSERID DB_SERVERPASSWORD $DB_SERVICENAME $DB_NAME $DB_SCHEMA $HIVE_DB $DB_SERVERUSERID_G DB_SERVERPASSWORD_G $DB_SERVICENAME_G $DB_NAME_G $ENV"

echo " " 

spark2-submit \
--master yarn \
--deploy-mode $MODE \
--supervise \
--num-executors 2 \
--executor-cores 2 \
--driver-memory 4g \
--executor-memory 4g \
--conf spark.driver.memoryOverhead=2048 \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.dynamicAllocation.maxExecutors=4 \
--jars /opt/cloudera/parcels/CDH/lib/sqoop/lib/ojdbc6.jar \
$PYTHON_SCRIPT_PATH/xnd_nds.py \
$HIVE_DB_M $LOG_PATH $DB_SERVERUSERID $DB_SERVERPASSWORD $DB_SERVICENAME $DB_NAME $DB_SCHEMA $HIVE_DB $DB_SERVERUSERID_G $DB_SERVERPASSWORD_G $DB_SERVICENAME_G $DB_NAME_G $ENV 

  if [ $? != 0 ]
  then
   echo "ERROR: while executing xnd_nds.py " 
   echo "RESTART INSTRUCTIONS:======> Please restart the job with command: $0 $1" 
   exit 3
  fi
echo "INFO xnd_nds.py ran successfully" 

echo " " 
echo "INFO: ______________ script $0 is completed successfully ______________" 
