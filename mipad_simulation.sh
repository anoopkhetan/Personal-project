#!/bin/sh

###--------------------------------------->Query to get min_bin & max_bin(run in Hive)<----------------------------------
#  with min_timestamp as (
#  select min(from_unixtime(unix_timestamp(time_stamp,'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm')) as min_timestamp
#  from h011gtcsandbox.pxo_mipad_live_simulation),
#  max_timestamp as (
#  select max(from_unixtime(unix_timestamp(time_stamp,'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm')) as max_timestamp
#  from h011gtcsandbox.pxo_mipad_live_simulation),
#  min_bin as (
#  select date_format(from_unixtime(unix_timestamp(min_timestamp, 'yyyy-MM-dd HH:mm') -
#  (unix_timestamp(min_timestamp, 'yyyy-MM-dd HH:mm') % (300)) + (300)),"yyyy-MM-dd HH:mm")  as min_bin
#  from min_timestamp),
#  max_bin as (
#  select date_format(from_unixtime(unix_timestamp(max_timestamp, 'yyyy-MM-dd HH:mm') -
#  (unix_timestamp(max_timestamp, 'yyyy-MM-dd HH:mm') % (300)) + (300)),"yyyy-MM-dd HH:mm")  as max_bin
#  from max_timestamp)
#  select min_bin, max_bin from min_bin, max_bin
###--------------------------------------------------------------------------------------------------------
# min and max bin value(manual input)
min_bin='2019-01-14 17:10'       
max_bin='2019-01-14 19:35'

# truncate input h011gtcsandbox.pxo_mipad_live_sim_result
echo '=======> truncating table h011gtcsandbox.pxo_mipad_live_sim_result'
beeline -u 'jdbc:hive2://gdch01d13:10000/default;principal=hive/gdch01d13@UATBDAKRB.COM' -e "truncate table h011gtcsandbox.pxo_mipad_live_sim_result;"

impala-shell -k -i impala-uat2.statestr.com -q "invalidate metadata h011gtcsandbox.pxo_mipad_live_sim_result; exit;"

echo 'starting simulation for first bin  :::' $min_bin
echo '-------------------------------------------------------'

while [ `date --date="$min_bin" +%s` -le `date --date="$max_bin" +%s` ]
do

if [ `date +"%H%M%S"` -ge '085400' ]
then
START=$(date +%s)
echo '--------->stating simulation for bin '$min_bin 'at '$START
echo "insert into table h011gtcsandbox.pxo_mipad_live_sim_result
select Number
,App_Code
,Phase
,Estimated_Time_to_Resolution 
,Time_stamp 
,Cause 
,Region 
,Predicted_RootCause 
,pred_rc_cat_prob 
,wordcloud_base64 
,Phase_description_1 
,Phase_description_2 
,Teams_to_bring_on 
,Experts
,c1
,c2
,phase_status
,resolved_at
,c3
,coalesce(curr_bin, c4)
,c5 from
(select 
a.Number
,a.App_Code
,a.Phase
,a.Estimated_Time_to_Resolution 
,a.Time_stamp 
,a.Cause 
,a.Region 
,a.Predicted_RootCause 
,a.pred_rc_cat_prob 
,a.wordcloud_base64 
,a.Phase_description_1 
,a.Phase_description_2 
,a.Teams_to_bring_on 
,a.Experts
,case when a.number = 'INC3050747' and date_format(from_unixtime(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') -
(unix_timestamp(a.time_stamp, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') >= '2019-01-14 17:20' then 'NAV Pricing' else '' end as c1
,case when a.number = 'INC3050747' and date_format(from_unixtime(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') -
(unix_timestamp(a.time_stamp, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') >= '2019-01-14 17:20' then 'Potential 41 NAV misses' else '' end as c2
,a.phase_status
,a.resolved_at
,current_timestamp() as c3
,'$min_bin' as c4
,date_format(from_unixtime(unix_timestamp(resolved_at, 'MM/dd/yyy HH:mm') -
(unix_timestamp(resolved_at, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') as c5
,1 as flag
from h011gtcsandbox.pxo_mipad_live_simulation a
join
(select Number, max(from_unixtime(unix_timestamp(time_stamp,'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm')) as max_tms
from h011gtcsandbox.pxo_mipad_live_simulation where date_format(from_unixtime(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') -
(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') = '$min_bin'
group by Number ) b
on a.Number=b.Number
and from_unixtime(unix_timestamp(a.time_stamp,'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm')=b.max_tms)t
full outer join
(select '$min_bin' as curr_bin, 1 as flag ) t1
on t.flag=t1.flag"

beeline -u 'jdbc:hive2://gdch01d13:10000/default;principal=hive/gdch01d13@UATBDAKRB.COM' -e "
insert into table h011gtcsandbox.pxo_mipad_live_sim_result
select Number
,App_Code
,Phase
,Estimated_Time_to_Resolution 
,Time_stamp 
,Cause 
,Region 
,Predicted_RootCause 
,pred_rc_cat_prob 
,wordcloud_base64 
,Phase_description_1 
,Phase_description_2 
,Teams_to_bring_on 
,Experts
,c1
,c2
,phase_status
,resolved_at
,c3
,coalesce(curr_bin, c4)
,c5 from
(select 
a.Number
,a.App_Code
,a.Phase
,a.Estimated_Time_to_Resolution 
,a.Time_stamp 
,a.Cause 
,a.Region 
,a.Predicted_RootCause 
,a.pred_rc_cat_prob 
,a.wordcloud_base64 
,a.Phase_description_1 
,a.Phase_description_2 
,a.Teams_to_bring_on 
,a.Experts
,case when a.number = 'INC3050747' and date_format(from_unixtime(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') -
(unix_timestamp(a.time_stamp, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') >= '2019-01-14 17:20' then 'NAV Pricing' else '' end as c1
,case when a.number = 'INC3050747' and date_format(from_unixtime(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') -
(unix_timestamp(a.time_stamp, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') >= '2019-01-14 17:20' then 'Potential 41 NAV misses' else '' end as c2
,a.phase_status
,a.resolved_at
,current_timestamp() as c3
,'$min_bin' as c4
,date_format(from_unixtime(unix_timestamp(resolved_at, 'MM/dd/yyy HH:mm') -
(unix_timestamp(resolved_at, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') as c5
,1 as flag
from h011gtcsandbox.pxo_mipad_live_simulation a
join
(select Number, max(from_unixtime(unix_timestamp(time_stamp,'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm')) as max_tms
from h011gtcsandbox.pxo_mipad_live_simulation where date_format(from_unixtime(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') -
(unix_timestamp(time_stamp, 'MM/dd/yyy HH:mm') % (300)) + (300)),'yyyy-MM-dd HH:mm') = '$min_bin'
group by Number ) b
on a.Number=b.Number
and from_unixtime(unix_timestamp(a.time_stamp,'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm')=b.max_tms)t
full outer join
(select '$min_bin' as curr_bin, 1 as flag ) t1
on t.flag=t1.flag;"

impala-shell -k -i impala-uat2.statestr.com -q "refresh h011gtcsandbox.pxo_mipad_live_sim_result;
compute stats h011gtcsandbox.pxo_mipad_live_sim_result; exit;"

END=$(date +%s)
echo '--------->ended simulation for bin '$min_bin 'at '$END

sleep `expr 60 - $(( $END - $START ))`

min_bin=`date -d "$min_bin 5 min" "+%Y-%m-%d %H:%M"`

echo ''
echo 'starting simulation for next bin  ::::' $min_bin
echo '-------------------------------------------------------'

fi
done
echo '>>>>>>>>>>>>>>>>>>> SIMULATION DONE FOR THE DAY ...HAVE A NICE DAY <<<<<<<<<<<<<<<<<<<<<<'