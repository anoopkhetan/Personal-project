--NASDAQ MTMI LINE CHART--


select bin, signal, sum(c_cnt) over (partition by signal order by bin) as cumsum, 
sum(p_cnt) over (partition by signal order by bin) as predicted from
(select a.bin, coalesce(a.signal,b.signal) as signal,coalesce(b.cnt,0) as c_cnt, coalesce(c.cnt,0) as p_cnt from
(with signal as (
select coalesce(a.s,b.s,c.s) as signal, 1 as f from
(select 'NASDAQ' as s, 1 as f) a
full outer join
(select 'BFDS' as s, 2 as f) b
on a.s=b.s
full outer join
(select 'BOTH' as s, 3 as f) c
on b.s=c.s)
select bin,signal from 
(select bin, 1 as f2
from h011pxo_m.pxo_navnow_mtmi_predicted
where substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
group by bin) t, signal where f=f2) a
left join
(select from_unixtime(hour(cast(actual_completion_time as string))*3600+300+MINUTE(cast(actual_completion_time as string))*60-(((MINUTE(cast(actual_completion_time as string))*60) %300)), 'HH:mm:ss') as bin,
count(distinct fund_id) as cnt, signal from h011pxo_m.pxo_navnow_mtmi_fundlevel 
where nav_date = '2019-08-23' and actual_completion_time !='NA' 
group by signal,bin) b
on a.bin=b.bin and a.signal=b.signal
left join
(select from_unixtime(hour(cast(predicted_time as string))*3600+300+MINUTE(cast(predicted_time as string))*60-(((MINUTE(cast(predicted_time as string))*60) %300)), 'HH:mm:ss') as bin,
count(fund_id) as cnt, signal from h011pxo_m.pxo_navnow_mtmi_funds_predicted group by signal, bin) c
on a.bin=c.bin and a.signal=c.signal)t
order by bin

--NASDAQ MTMI LINE CHART------------
    --For NASDAQ
		with clientlevel as (
		select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier, signal
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and
		substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin, signal, client_identifier )
		select a.bin, a.predicted, a.red_1, b.cumsum from (select bin, sum(predicted) as predicted, sum(red_1) as red_1 
		from h011pxo_m.pxo_navnow_mtmi_predicted
		where signal != 'BFDS' and substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin ) a left join (select bin, sum(cumsum) as cumsum  from clientlevel
		group by bin) b on a.bin=b.bin order by a.bin
    --For BFDS
		with clientlevel as (
		select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier, signal
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and
		substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin, signal, client_identifier )
		select a.bin, a.predicted, a.red_1, b.cumsum from (select bin, sum(predicted) as predicted, sum(red_1) as red_1 
		from h011pxo_m.pxo_navnow_mtmi_predicted
		where signal != 'NASDAQ' and substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin ) a left join (select bin, sum(cumsum) as cumsum  from clientlevel
		group by bin) b on a.bin=b.bin order by a.bin

--NASDAQ MTMI ALERT/TRENDING NOW 
	--For BFDS
		with clientlevel as (
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ') group by bin, client_identifier
		) 
		select processing_rate, processed, shouldbeprocessed, percent_slower, historical_processing_rate, 
		client_name,cumsum_client, delayed_client_per, predicted_client from 
		(select cast(processed_f/bin_mins as decimal(5,2)) as processing_rate, processed_f as processed, predicted as shouldbeprocessed,
		case when predicted < 1 then (processed_f - predicted )*100 else
		abs(cast(((predicted - processed_f)*100/predicted)  as decimal(5,2))) end 
		as percent_slower, 
		cast(predicted/bin_mins as decimal(5,2)) as historical_processing_rate, bin 
		from (select bin, 
		case when substr(bin,1,2)='16' then  cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='17' then  60 + cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='18' then  120 + cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='19' then  180 + cast(substr(bin,4,2) as int) end bin_mins, 
		COALESCE(sum(predicted),0) as predicted,
		COALESCE(sum(error),0) as error, 1 as flag
		from clientlevel group by bin order by bin desc limit 1 ) a full outer join
		(select COALESCE(count(distinct fund_id), 0) as processed_f, 1 as flag  from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel where nav_date = to_date(now()) and signal != 'NASDAQ'
		and actual_completion_time !='NA') b on a.flag=b.flag) c
		full outer join (select distinct 
		c.client_identifier as client_name, cumsum_client, delayed_client_per, predicted_client,bin 
		from (select bin, client_identifier, predicted - cumsum as cumsum_client, 
		cast((error*100)/predicted as decimal(5,2)) as delayed_client_per, predicted as predicted_client from clientlevel   
		where error>0   order by bin desc, error desc limit 1) c   ) b  on c.bin=b.bin
	--For NASDAQ
	    with clientlevel as (
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS') group by bin, client_identifier
		) 
		select processing_rate, processed, shouldbeprocessed, percent_slower, historical_processing_rate, 
		client_name,cumsum_client, delayed_client_per, predicted_client from 
		(select cast(processed_f/bin_mins as decimal(5,2)) as processing_rate, processed_f as processed, predicted as shouldbeprocessed,
		case when predicted < 1 then (processed_f - predicted )*100 else
		abs(cast(((predicted - processed_f)*100/predicted)  as decimal(5,2))) end 
		as percent_slower, 
		cast(predicted/bin_mins as decimal(5,2)) as historical_processing_rate, bin 
		from (select bin, 
		case when substr(bin,1,2)='16' then  cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='17' then  60 + cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='18' then  120 + cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='19' then  180 + cast(substr(bin,4,2) as int) end bin_mins, 
		COALESCE(sum(predicted),0) as predicted,
		COALESCE(sum(error),0) as error, 1 as flag
		from clientlevel group by bin order by bin desc limit 1 ) a full outer join
		(select COALESCE(count(distinct fund_id), 0) as processed_f, 1 as flag  from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel where nav_date = to_date(now()) and signal != 'BFDS'
		and actual_completion_time !='NA') b on a.flag=b.flag) c
		full outer join (select distinct 
		c.client_identifier as client_name, cumsum_client, delayed_client_per, predicted_client,bin 
		from (select bin, client_identifier, predicted - cumsum as cumsum_client, 
		cast((error*100)/predicted as decimal(5,2)) as delayed_client_per, predicted as predicted_client from clientlevel   
		where error>0   order by bin desc, error desc limit 1) c   ) b  on c.bin=b.bin
		
--NASDAQ MTMI BIN
 -- For BFDS
		with max_updated_time as (select  max(updated_time) as max_updated_time from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel where nav_date = to_date(now()) and signal != 'NASDAQ')
		select (last_cumsum - processed ) as totalToBeProcessed, processed, delayed, last_cumsum from
		(select count(distinct fund_id) as last_cumsum, 1 as flag from 
		h011pxo_m.pxo_navnow_mtmi_funds_predicted where signal != 'NASDAQ') a full outer join
		(select count(distinct fund_id) as processed, 1 as flag  from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'NASDAQ'
		and actual_completion_time !='NA') b on a.flag = b.flag full outer join
		(select count(distinct fund_id) as delayed, 1 as flag  from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'NASDAQ' 
		and updated_time = max_updated_time and status in ('Delayed','Late')) c
		on ISNULL(a.flag, b.flag) = c.flag
  --- For NASDAQ
		with max_updated_time as (select  max(updated_time) as max_updated_time from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel where nav_date = to_date(now()) and signal != 'BFDS')
		select (last_cumsum - processed ) as totalToBeProcessed, processed, delayed, last_cumsum from
		(select count(distinct fund_id) as last_cumsum, 1 as flag from 
		h011pxo_m.pxo_navnow_mtmi_funds_predicted where signal != 'BFDS') a full outer join
		(select count(distinct fund_id) as processed, 1 as flag  from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'BFDS'
		and actual_completion_time !='NA') b on a.flag = b.flag full outer join
		(select count(distinct fund_id) as delayed, 1 as flag  from 
		h011pxo_m.pxo_navnow_mtmi_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'BFDS' 
		and updated_time = max_updated_time and status in ('Delayed','Late')) c
		on ISNULL(a.flag, b.flag) = c.flag

--NASDAQ MTMI BAR CHART
   -- FOR BFDS
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc  
		limit 10
 -- FOR NASDAQ
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc
		limit 10

--NASDAQ MTMI DATA GRID
   -- FOR BFDS
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc  
		
 -- FOR NASDAQ
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_mtmi_clientlevel where nav_date = to_date(now()) and signal != 'BFDS') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc  



--NASDAQ PRICING LINE CHART-----------------
        --For NASDAQ
		with clientlevel as (
		select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier, signal
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and
		substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin, signal, client_identifier )
		select a.bin, a.predicted, a.red_1, b.cumsum from (select bin, sum(predicted) as predicted, sum(red_1) as red_1 
		from h011pxo_m.pxo_navnow_pricing_predicted
		where signal != 'BFDS' and substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin ) a left join (select bin, sum(cumsum) as cumsum  from clientlevel
		group by bin) b on a.bin=b.bin order by a.bin
    --For BFDS
		with clientlevel as (
		select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier, signal
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and
		substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin, signal, client_identifier )
		select a.bin, a.predicted, a.red_1, b.cumsum from (select bin, sum(predicted) as predicted, sum(red_1) as red_1 
		from h011pxo_m.pxo_navnow_pricing_predicted
		where signal != 'NASDAQ' and substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
		group by bin ) a left join (select bin, sum(cumsum) as cumsum  from clientlevel
		group by bin) b on a.bin=b.bin order by a.bin

--NASDAQ PRICING ALERT/TRENDING NOW 
	--For BFDS
			with clientlevel as (
			select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
			from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and bin = (select max(bin) 
			from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ') group by bin, client_identifier
			) 
			select processing_rate, processed, shouldbeprocessed, percent_slower, historical_processing_rate, 
			client_name,cumsum_client, delayed_client_per, predicted_client from 
			(select cast(processed_f/bin_mins as decimal(5,2)) as processing_rate, processed_f as processed, predicted as shouldbeprocessed,
			case when predicted < 1 then (processed_f - predicted )*100 else
			abs(cast(((predicted - processed_f)*100/predicted)  as decimal(5,2))) end 
			as percent_slower, 
			cast(predicted/bin_mins as decimal(5,2)) as historical_processing_rate, bin 
			from (select bin, 
			case when substr(bin,1,2)='16' then  cast(substr(bin,4,2) as int) 
			when substr(bin,1,2)='17' then  60 + cast(substr(bin,4,2) as int) 
			when substr(bin,1,2)='18' then  120 + cast(substr(bin,4,2) as int) 
			when substr(bin,1,2)='19' then  180 + cast(substr(bin,4,2) as int) end bin_mins, 
			COALESCE(sum(predicted),0) as predicted,
			COALESCE(sum(error),0) as error, 1 as flag
			from clientlevel group by bin order by bin desc limit 1 ) a full outer join
			(select COALESCE(count(distinct fund_id), 0) as processed_f, 1 as flag  from 
			h011pxo_m.pxo_navnow_pricing_fundlevel where nav_date = to_date(now()) and signal != 'NASDAQ'
			and actual_completion_time !='NA') b on a.flag=b.flag) c
			full outer join (select distinct 
			c.client_identifier as client_name, cumsum_client, delayed_client_per, predicted_client,bin 
			from (select bin, client_identifier, predicted - cumsum as cumsum_client, 
			cast((error*100)/predicted as decimal(5,2)) as delayed_client_per, predicted as predicted_client from clientlevel   
			where error>0   order by bin desc, error desc limit 1) c   ) b  on c.bin=b.bin
			
	--For NASDAQ
	    with clientlevel as (
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS') group by bin, client_identifier
		) 
		select processing_rate, processed, shouldbeprocessed, percent_slower, historical_processing_rate, 
		client_name,cumsum_client, delayed_client_per, predicted_client from 
		(select cast(processed_f/bin_mins as decimal(5,2)) as processing_rate, processed_f as processed, predicted as shouldbeprocessed,
		case when predicted < 1 then (processed_f - predicted )*100 else
		abs(cast(((predicted - processed_f)*100/predicted)  as decimal(5,2))) end 
		as percent_slower, 
		cast(predicted/bin_mins as decimal(5,2)) as historical_processing_rate, bin 
		from (select bin, 
		case when substr(bin,1,2)='16' then  cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='17' then  60 + cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='18' then  120 + cast(substr(bin,4,2) as int) 
		when substr(bin,1,2)='19' then  180 + cast(substr(bin,4,2) as int) end bin_mins, 
		COALESCE(sum(predicted),0) as predicted,
		COALESCE(sum(error),0) as error, 1 as flag
		from clientlevel group by bin order by bin desc limit 1 ) a full outer join
		(select COALESCE(count(distinct fund_id), 0) as processed_f, 1 as flag  from 
		h011pxo_m.pxo_navnow_pricing_fundlevel where nav_date = to_date(now()) and signal != 'BFDS'
		and actual_completion_time !='NA') b on a.flag=b.flag) c
		full outer join (select distinct 
		c.client_identifier as client_name, cumsum_client, delayed_client_per, predicted_client,bin 
		from (select bin, client_identifier, predicted - cumsum as cumsum_client, 
		cast((error*100)/predicted as decimal(5,2)) as delayed_client_per, predicted as predicted_client from clientlevel   
		where error>0 order by bin desc, error desc limit 1) c   ) b  on c.bin=b.bin
		
--NASDAQ PRICING BIN
 -- For BFDS
		with max_updated_time as (select  max(updated_time) as max_updated_time from 
		h011pxo_m.pxo_navnow_pricing_fundlevel where nav_date = to_date(now()) and signal != 'NASDAQ')
		select (last_cumsum - processed ) as totalToBeProcessed, processed, delayed, last_cumsum from
		(select count(distinct fund_id) as last_cumsum, 1 as flag from 
		h011pxo_m.pxo_navnow_pricing_funds_predicted where signal != 'NASDAQ') a full outer join
		(select count(distinct fund_id) as processed, 1 as flag  from 
		h011pxo_m.pxo_navnow_pricing_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'NASDAQ'
		and actual_completion_time !='NA') b on a.flag = b.flag full outer join
		(select count(distinct fund_id) as delayed, 1 as flag  from 
		h011pxo_m.pxo_navnow_pricing_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'NASDAQ' 
		and updated_time = max_updated_time and status in ('Delayed','Late')) c
		on ISNULL(a.flag, b.flag) = c.flag
  --- For NASDAQ
		with max_updated_time as (select  max(updated_time) as max_updated_time from 
		h011pxo_m.pxo_navnow_pricing_fundlevel where nav_date = to_date(now()) and signal != 'BFDS')
		select (last_cumsum - processed ) as totalToBeProcessed, processed, delayed, last_cumsum from
		(select count(distinct fund_id) as last_cumsum, 1 as flag from 
		h011pxo_m.pxo_navnow_pricing_funds_predicted where signal != 'BFDS') a full outer join
		(select count(distinct fund_id) as processed, 1 as flag  from 
		h011pxo_m.pxo_navnow_pricing_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'BFDS'
		and actual_completion_time !='NA') b on a.flag = b.flag full outer join
		(select count(distinct fund_id) as delayed, 1 as flag  from 
		h011pxo_m.pxo_navnow_pricing_fundlevel, max_updated_time where nav_date = to_date(now()) and signal != 'BFDS' 
		and updated_time = max_updated_time and status in ('Delayed','Late')) c
		on ISNULL(a.flag, b.flag) = c.flag

--NASDAQ PRICING BAR CHART
   -- FOR BFDS
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc  
		limit 10
 -- FOR NASDAQ
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc
		limit 10

--NASDAQ PRICING DATA GRID
   -- FOR BFDS
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'NASDAQ') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc  
		
 -- FOR NASDAQ
		with clientlevel as ( 
		select bin,sum(cumsum) as cumsum, sum(predicted) as predicted, (sum(predicted) - sum(cumsum)) as error, client_identifier
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS' and bin = (select max(bin) 
		from h011pxo_m.pxo_navnow_pricing_clientlevel where nav_date = to_date(now()) and signal != 'BFDS') group by bin, client_identifier
		)		
		select client_identifier, predicted, cumsum, error, client_identifier as client_name  
		from clientlevel 
		where bin = (select max(bin) from clientlevel)  
		and cumsum<predicted 
		order by error desc, client_name asc
		 
			
-------------------------ClientLevel-------------------------------------

---clientlevel (line chart)
with clientlevel as (
select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier
from pxo_navnow_nds_clientlevel where nav_date = to_date(now()) and
substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55') and upper(client_identifier) = clientName(passed by user)
group by bin, client_identifier)
select a.bin, a.predicted, a.red_1, b.cumsum, coalesce(a.client_identifier,b.client_identifier) as client_name from (select bin, sum(predicted) as predicted, 
sum(red_1) as red_1,client_identifier from pxo_navnow_nds_predicted 
where substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55') and upper(client_identifier) = clientName(passed by user)
group by bin,client_identifier ) a left join (select bin, sum(cumsum) as cumsum, client_identifier  from clientlevel
group by bin,client_identifier) b on a.bin=b.bin order by a.bin


--clientlevel (ALERT/TRENDING NOW)
with clientlevel as (
select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier
from pxo_navnow_mtmi_clientlevel where nav_date = to_date(now())
and client_identifier = '1' 
group by bin, client_identifier),
max_updated_time_pricing as (select  max(updated_time) as max_updated_time_pricing from h011pxo_m.pxo_navnow_pricing_fundlevel 
where nav_date = to_date(now())),
max_updated_time_nds as (select  max(updated_time) as max_updated_time_nds from h011pxo_m.pxo_navnow_nds_fundlevel 
where nav_date = to_date(now())),
max_updated_time_nds_1 as (select  max(updated_time) as max_updated_time_nds from h011gtcsandbox.pxo_navnow_nds_predicted_misses 
where nav_date = to_date(now()))
select 
processed_mtmi, 
processing_rate_mtmi,
percent_rate_mtmi,
historical_processing_rate_mtmi,
delayed_pricing,
late_pricing,
fund_miss_nds,
cast((10*100)/total_funds_nds as decimal(9,2)) as fund_miss_per_nds	
from 
(select cast(processed_f/bin_mins as decimal(9,2)) as processing_rate_mtmi, 
processed_f as processed_mtmi, predicted as shouldbeprocessed,
case when predicted < 1 then (processed_f - predicted )*100 else
abs(cast(((predicted - processed_f)*100/predicted)  as decimal(9,2))) end 
as percent_rate_mtmi, 
cast(predicted/bin_mins as decimal(9,2)) as historical_processing_rate_mtmi, bin, a.flag
from (select bin, 
case when substr(bin,1,2)='16' then  cast(substr(bin,4,2) as int) 
when substr(bin,1,2)='17' then  60 + cast(substr(bin,4,2) as int) 
when substr(bin,1,2)='18' then  120 + cast(substr(bin,4,2) as int) 
when substr(bin,1,2)='19' then  180 + cast(substr(bin,4,2) as int) end bin_mins, 
COALESCE(sum(predicted),0) as predicted,
COALESCE(sum(error),0) as error, 1 as flag from clientlevel group by bin order by bin desc limit 1 ) a 
full outer join
(select COALESCE(count(distinct fund_id), 0) as processed_f, 1 as flag  from pxo_navnow_mtmi_fundlevel where nav_date = to_date(now())
and actual_completion_time !='NA' and client_name = '1') b on COALESCE(a.flag,1)=COALESCE(b.flag,1)) a 
full outer join 
(select count(distinct fund_id) as delayed_pricing, 1 as flag  from h011pxo_m.pxo_navnow_pricing_fundlevel, max_updated_time_pricing 
where nav_date = to_date(now()) and client_name = '1'
and updated_time = max_updated_time_pricing and status ='Delayed') b 
on COALESCE(a.flag,1)=COALESCE(b.flag,1)
full outer join 
(select count(distinct fund_id) as late_pricing, 1 as flag  from h011pxo_m.pxo_navnow_pricing_fundlevel,max_updated_time_pricing 
where nav_date = to_date(now()) and client_name = '1'
and updated_time = max_updated_time_pricing and status ='Late') c 
on COALESCE(a.flag,b.flag,1) = COALESCE(c.flag,1)
full outer join 
(select predicted_miss as fund_miss_nds, 1 as flag  from h011gtcsandbox.pxo_navnow_nds_predicted_misses, max_updated_time_nds_1 
where nav_date = to_date(now()) and client_name = '1') d 
on COALESCE(a.flag,b.flag,c.flag,1) = COALESCE(d.flag,1)
full outer join 
(select count(distinct fund_id) as total_funds_nds, 1 as flag  from h011pxo_m.pxo_navnow_nds_fundlevel where nav_date = to_date(now()) 
and client_name = '1') e 
on COALESCE(a.flag,b.flag,c.flag,d.flag,1) = COALESCE(e.flag,1)



--clientlevel (datagrid)
with stat_tbl as ( 
select coalesce(a.st,b.st,c.st,d.st,e.st,f.st) as stat, coalesce(a.sq,b.sq,c.sq,d.sq,e.sq,f.sq) as seq from 
(select 'Awaiting' as st, 1 as sq) a 
full outer join 
(select 'on Time' as st, 2 as sq) b 
on a.sq=b.sq 
full outer join 
(select 'Delayed' as st, 3 as sq) c 
on b.sq=c.sq 
full outer join 
(select 'Delayed Completion' as st, 4 as sq) d 
on c.sq=d.sq 
full outer join 
(select 'Late' as st, 5 as sq) e 
on d.sq=e.sq 
full outer join 
(select 'Late Completion' as st, 6 as sq) f 
on e.sq=f.sq 
), 
max_updt_tm_mtmi as  
(select  max(updated_time) as max_updt_tm_mtmi from pxo_navnow_mtmi_fundlevel 
where nav_date = to_date(now())), 
max_updt_tm_pricing as (select  max(updated_time) as max_updt_tm_pricing from pxo_navnow_pricing_fundlevel 
where nav_date = to_date(now())), 
max_updt_tm_nds as (select  max(updated_time) as max_updt_tm_nds from pxo_navnow_nds_fundlevel 
where nav_date = to_date(now())), 
nds_funds_list as (select client_name, fund_id  from pxo_navnow_nds_ticker_prediction_1 
group by client_name, fund_id) 
select n.client_name as client_name, n.fund_ct_all as total_no_tickers_funds,  
hostorical_completion_time_mtmi, predicted_completion_today_mtmi,actual_completion_time_mtmi, status_mtmi,concat(m.fund_ct_procd,'(',m.fund_ct_pndg,')') as pending_funds_mtmi, mtmi_oslt1 as optimized_mtmi, mtmi_oslt2 as predicted_max_mtmi, 
hostorical_completion_time_pricing, predicted_completion_today_pricing,actual_completion_time_pricing, status_pricing,concat(p.fund_ct_procd,'(',p.fund_ct_pndg,')') as pending_funds_pricing, pricing_oslt1 as optimized_pricing, pricing_oslt2 as predicted_max_pricing,
hostorical_completion_time_nds, predicted_completion_today_nds,actual_completion_time_nds, status_nds,concat(n.fund_ct_procd,'(',n.fund_ct_pndg,')') as pending_funds_nds, coalesce(n2.predicted_miss, '0') as predicted_miss
from 
(select coalesce(a.client_name,c.client_name) as client_name, cast(fund_ct_all as string) as fund_ct_all, cast(coalesce(fund_ct_procd,0) as string) 
as fund_ct_procd, cast((fund_ct_all-coalesce(fund_ct_procd,0)) as string) as fund_ct_pndg, hostorical_completion_time_nds, predicted_completion_today_nds, 
case when fund_ct_all > coalesce(fund_ct_procd, 0) then 'NA' else coalesce(actual_completion_time_nds,'NA') end as actual_completion_time_nds, status_nds 
from 
(select client_name, count(distinct fund_id) as fund_ct_all, max(predicted_completed_time) as hostorical_completion_time_nds, max(predicted_completion_time) as predicted_completion_today_nds, 
max(seq) as status_nds  
from pxo_navnow_nds_fundlevel, stat_tbl, max_updt_tm_nds  
where nav_date =to_date(now()) and (actual_completion_time != 'NA' or  updated_time = max_updt_tm_nds) 
and status=stat 
group by client_name)a 
full outer join 
(select client_name, count(distinct fund_id) as fund_ct_procd, max(actual_completion_time) as actual_completion_time_nds 
from pxo_navnow_nds_fundlevel  
where nav_date =to_date(now()) and actual_completion_time != 'NA' 
group by client_name)c  
on a.client_name=c.client_name) n  
left outer join 
(select coalesce(a.client_name,b.client_name,c.client_name) as client_name, fund_ct_all, coalesce(fund_ct_procd,'0') as fund_ct_procd, coalesce(fund_ct_pndg,'0') 
as fund_ct_pndg, hostorical_completion_time_mtmi, predicted_completion_today_mtmi, 
actual_completion_time_mtmi, status_mtmi from 
(select m1.client_name, cast(count(distinct m1.fund_id) as string) as fund_ct_all, max(predicted_completed_time) as hostorical_completion_time_mtmi, 
max(predicted_completion_time) as predicted_completion_today_mtmi, 
max(actual_completion_time) as actual_completion_time_mtmi, max(seq) as status_mtmi  
from pxo_navnow_mtmi_fundlevel m1, stat_tbl, max_updt_tm_mtmi, nds_funds_list l  
where nav_date =to_date(now()) and (actual_completion_time != 'NA' or  updated_time = max_updt_tm_mtmi) 
and status=stat and m1.client_name = l.client_name and m1.fund_id=l.fund_id 
group by client_name)a 
full outer join 
(select m1.client_name, cast(count(distinct m1.fund_id) as string) as fund_ct_pndg 
from pxo_navnow_mtmi_fundlevel m1, max_updt_tm_mtmi m, nds_funds_list l 
where nav_date =to_date(now()) and (actual_completion_time = 'NA' and  updated_time = max_updt_tm_mtmi) and m1.client_name = l.client_name and m1.fund_id=l.fund_id 
group by client_name)b 
on a.client_name=b.client_name 
full outer join 
(select m1.client_name, cast(count(distinct m1.fund_id) as string) as fund_ct_procd 
from pxo_navnow_mtmi_fundlevel m1, nds_funds_list l 
where nav_date =to_date(now()) and actual_completion_time != 'NA' and m1.client_name = l.client_name and m1.fund_id=l.fund_id 
group by client_name)c  
on a.client_name=c.client_name  
) m 
on n.client_name=m.client_name 
left outer join 
(select coalesce(a.client_name,b.client_name,c.client_name) as client_name, fund_ct_all, coalesce(fund_ct_procd,'0') 
as fund_ct_procd, coalesce(fund_ct_pndg,'0') as fund_ct_pndg, hostorical_completion_time_pricing, predicted_completion_today_pricing, 
actual_completion_time_pricing, status_pricing from 
(select p1.client_name, cast(count(distinct p1.fund_id) as string) as fund_ct_all, max(predicted_completed_time) 
as hostorical_completion_time_pricing, max(predicted_completion_time) as predicted_completion_today_pricing, 
max(actual_completion_time) as actual_completion_time_pricing, max(seq) as status_pricing  
from pxo_navnow_pricing_fundlevel p1, stat_tbl, max_updt_tm_pricing, nds_funds_list l  
where nav_date =to_date(now()) and (actual_completion_time != 'NA' or  updated_time = max_updt_tm_pricing) 
and status=stat and p1.client_name = l.client_name and p1.fund_id=l.fund_id 
group by client_name)a  
full outer join 
(select p1.client_name, cast(count(distinct p1.fund_id) as string) as fund_ct_pndg 
from pxo_navnow_pricing_fundlevel p1, max_updt_tm_pricing, nds_funds_list l   
where nav_date =to_date(now()) and (actual_completion_time = 'NA' and  updated_time = max_updt_tm_pricing) and p1.client_name = l.client_name and p1.fund_id=l.fund_id 
group by client_name)b  
on a.client_name=b.client_name 
full outer join 
(select p1.client_name, cast(count(distinct p1.fund_id) as string) as fund_ct_procd 
from pxo_navnow_pricing_fundlevel p1, nds_funds_list l   
where nav_date =to_date(now()) and actual_completion_time != 'NA' and p1.client_name = l.client_name and p1.fund_id=l.fund_id 
group by client_name)c  
on a.client_name=c.client_name)p 
on n.client_name=p.client_name
full outer join 
h011gtcsandbox.pxo_navnow_clientlevel_oslts n1   
on n.client_name=n1.client_name
full outer join
(select client_name,predicted_miss from h011gtcsandbox.pxo_navnow_nds_predicted_misses 
where updated_time = (select max(updated_time) from h011gtcsandbox.pxo_navnow_nds_predicted_misses)) n2
on n.client_name=n2.client_name
where upper(coalesce(m.client_name,p.client_name,n.client_name)) like '%'
order by predicted_completion_today_nds;


----nds line chart-------
with clientlevel as (
select bin,max(cumsum) as cumsum, max(predicted) as predicted, (max(predicted) - max(cumsum)) as error, client_identifier
from pxo_navnow_nds_clientlevel where nav_date = to_date(now()) and
substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
group by bin, client_identifier )
select a.bin, a.predicted, a.red_1, b.cumsum, c.pred_model from (select bin, sum(predicted) as predicted, sum(red_1) as red_1
from pxo_navnow_nds_predicted
where substr(bin,4,2) in ('00','05','10','15','20','25','30','35','40','45','50','55')
group by bin ) a 
left join (select bin, sum(cumsum) as cumsum  from clientlevel
group by bin) b on a.bin=b.bin
left join (with max_time_bin as (
select bin, max(updated_time) as max_updated_time from h011gtcsandbox.pxo_navnow_nds_volume 
where nav_date = to_date(now())
group by bin)
select a.bin, sum(predicted) over(order by a.bin) as pred_model from h011gtcsandbox.pxo_navnow_nds_volume a, max_time_bin b
where a.bin=b.bin and a.updated_time = b.max_updated_time and nav_date = to_date(now())
) c 
on a.bin=c.bin 
order by a.bin