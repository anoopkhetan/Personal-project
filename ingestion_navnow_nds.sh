#!/bin/sh
i=57600
beeline -u 'jdbc:hive2://gdch01d13:10000/default;principal=hive/gdch01d13@UATBDAKRB.COM' -e  "truncate table h011pxo_sbx_navnow_m.pxo_navnow_nds_clientlevel; truncate table h011pxo_sbx_navnow_m.pxo_navnow_nds_fundlevel;"

impala-shell -k -i impala-uat2.statestr.com -q "refresh h011pxo_sbx_navnow_m.pxo_navnow_nds_clientlevel; compute stats h011pxo_sbx_navnow_m.pxo_navnow_nds_clientlevel;
refresh h011pxo_sbx_navnow_m.pxo_navnow_nds_fundlevel; compute stats h011pxo_sbx_navnow_m.pxo_navnow_nds_fundlevel; exit;"


while [ $i -le 68400 ]
do
j=i
((sec=j%60, j/=60, min=j%60, hrs=j/60))
bin_c=$(printf "'%d:%02d:%02d'" $hrs $min $sec)
bin_f=$(printf "'%d:%02d'" $hrs $min)

echo '-------------->>> ingestion started for nds bin:'$bin_c
beeline -u 'jdbc:hive2://gdch01d13:10000/default;principal=hive/gdch01d13@UATBDAKRB.COM' -e "
insert into h011pxo_sbx_navnow_m.pxo_navnow_nds_clientlevel 
select client_identifier, current_date(), bin, count, cumsum, predicted, red_1, error
from h011pxo_m.pxo_navnow_nds_clientlevel_hist where yyyy_mm = substr('$1',1,7) and nav_date  = '$1' and bin = $bin_c;

insert into h011pxo_sbx_navnow_m.pxo_navnow_nds_fundlevel 
select ticker_id,fund_id,current_date(),client_name,predicted_completed_time,maxpredicted_completion_time,
predicted_completion_time,actual_completion_time,nds_fund_miss,alert_time,updated_time,best_case,worst_case,status
from h011pxo_m.pxo_navnow_nds_fundlevel_hist where yyyy_mm = substr('$1',1,7) and nav_date  = '$1' and substr(updated_time,12,5) = $bin_f;"

impala-shell -k -i impala-uat2.statestr.com -q "refresh h011pxo_sbx_navnow_m.pxo_navnow_nds_clientlevel; compute stats h011pxo_sbx_navnow_m.pxo_navnow_nds_clientlevel;
refresh h011pxo_sbx_navnow_m.pxo_navnow_nds_fundlevel; compute stats h011pxo_sbx_navnow_m.pxo_navnow_nds_fundlevel; exit;"

sleep 28

i=`expr $i + 60`
done
echo '>>>>>>>>>>>>>>>>>>> YOU ARE ALL SET <<<<<<<<<<<<<<<<<<<<<<<<'

