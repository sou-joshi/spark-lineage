
%let rpt_dt = 20240215;
libname raw "/data/raw";
libname ods oracle path='ORCL' schema=ODS;
libname stg oracle path='ORCL' schema=STG;
libname mart oracle path='ORCL' schema=MART;

proc fcmp outlib=work.funcs.geo;
  function geo_band(lat);
    if lat >= 0 then return('NORTH');
    else return('SOUTH');
  endsub;
run;
options cmplib=work.funcs;

proc import datafile="/data/raw/customer_latitude_&rpt_dt..csv" out=raw.customer_latitude dbms=csv replace;
run;

data ods.customer_latitude;
  set raw.customer_latitude;
  lat_clean = input(lat_number, 8.4);
  lon_clean = input(lon_number, 8.4);
  event_date = input(scan(file_dt,1,' '), yymmdd10.);
  keep customer_id lat_clean lon_clean event_date msisdn status;
run;

proc sql;
  create table stg.customer_latitude_session as
  select a.customer_id,
         a.lat_clean,
         a.lon_clean,
         a.event_date,
         geo_band(a.lat_clean) as lat_band,
         sha256(trim(a.msisdn)) as msisdn_hash,
         case when a.status='A' then 1 else 0 end as active_flag
  from ods.customer_latitude a;
quit;

proc summary data=stg.customer_latitude_session nway;
  class event_date lat_band;
  var lat_clean;
  output out=mart.customer_latitude_daily
    n=ping_count
    avg=avg_lat
    min=min_lat
    max=max_lat;
run;
