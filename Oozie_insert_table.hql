set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE ${HIVE_DB}.Geneos_${GENEOS_FILE} partition (ingestion_date) 
select *, regexp_replace(substr(from_unixtime(unix_timestamp(current_date(),'yyyy-MM-dd')),1,10),'-','')
from ${HIVE_DB}.Geneos_${GENEOS_FILE}_stg;

