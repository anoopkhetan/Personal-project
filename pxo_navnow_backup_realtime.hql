#!/bin/bash
beeline -u $1 -e "SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.execution.engine=mr;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=134217728;
set hive.merge.size.per.task=134217728;
set hive.merge.mapfiles=true;
set mapreduce.input.fileinputformat.split.minsize=134217728;
set mapreduce.input.fileinputformat.split.minsize.per.node=134217728;
set mapreduce.input.fileinputformat.split.minsize.per.rack=134217728;

with max_date as (select coalesce(max(nav_date), '1900-01-01') as max_date from $2.pxo_navnow_mtmi_clientlevel_hist)
insert into $2.pxo_navnow_mtmi_clientlevel_hist PARTITION (yyyy_mm) 
select a.*,substr(a.nav_date,1,7) from $2.pxo_navnow_mtmi_clientlevel a,max_date where a.nav_date>max_date;

with max_date as (select coalesce(max(nav_date), '1900-01-01') as max_date from $2.pxo_navnow_pricing_clientlevel_hist)
insert into $2.pxo_navnow_pricing_clientlevel_hist PARTITION (yyyy_mm) 
select a.*,substr(a.nav_date,1,7) from $2.pxo_navnow_pricing_clientlevel a,max_date where a.nav_date>max_date;

with max_date as (select coalesce(max(nav_date), '1900-01-01') as max_date from $2.pxo_navnow_nds_clientlevel_hist)
insert into $2.pxo_navnow_nds_clientlevel_hist PARTITION (yyyy_mm) 
select a.*,substr(a.nav_date,1,7) from $2.pxo_navnow_nds_clientlevel a,max_date where a.nav_date>max_date;

with max_date as (select coalesce(max(nav_date), '1900-01-01') as max_date from $2.pxo_navnow_mtmi_fundlevel_hist)
insert into $2.pxo_navnow_mtmi_fundlevel_hist PARTITION (yyyy_mm) 
select a.*,substr(a.nav_date,1,7) from $2.pxo_navnow_mtmi_fundlevel a,max_date where a.nav_date>max_date;

with max_date as (select coalesce(max(nav_date), '1900-01-01') as max_date from $2.pxo_navnow_pricing_fundlevel_hist)
insert into $2.pxo_navnow_pricing_fundlevel_hist PARTITION (yyyy_mm) 
select a.*,substr(a.nav_date,1,7) from $2.pxo_navnow_pricing_fundlevel a,max_date where a.nav_date>max_date;


with max_date as (select coalesce(max(nav_date), '1900-01-01') as max_date from $2.pxo_navnow_nds_fundlevel_hist)
insert into $2.pxo_navnow_nds_fundlevel_hist PARTITION (yyyy_mm) 
select a.*,substr(a.nav_date,1,7) from $2.pxo_navnow_nds_fundlevel a,max_date where a.nav_date>max_date;"