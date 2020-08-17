CREATE OR REPLACE TABLE `aki-recovery-prediction.MimicIV.MIMICIV_kdigo_uo` AS
with ur_stg as
(
  select io.stay_id, io.charttime
  -- we have joined each row to all rows preceding within 24 hours
  -- we can now sum these rows to get total UO over the last 24 hours
  -- we can use case statements to restrict it to only the last 6/12 hours
  -- therefore we have three sums:
  -- 1) over a 6 hour period
  -- 2) over a 12 hour period
  -- 3) over a 24 hour period
  -- note that we assume data charted at charttime corresponds to 1 hour of UO
  -- therefore we use '5' and '11' to restrict the period, rather than 6/12
  -- this assumption may overestimate UO rate when documentation is done less than hourly

  -- 6 hours
  , sum(if(datetime_diff(io.charttime,iosum.charttime,hour) <= 5,iosum.value,null)) as UrineOutput_6hr
  -- 12 hours
  , sum(if(datetime_diff(io.charttime,iosum.charttime,hour) <= 11,iosum.value,null)) as UrineOutput_12hr
  -- 24 hours
  , sum(iosum.VALUE) as UrineOutput_24hr
  -- calculate the number of hours over which we've tabulated UO
  , max(if(datetime_diff(io.charttime,iosum.charttime,hour) <= 5,datetime_diff(io.charttime,iosum.charttime,second)/3600,null)) as uo_tm_6hr
  -- repeat extraction for 12 hours and 24 hours
  , max(if(datetime_diff(io.charttime,iosum.charttime,hour) <= 11,datetime_diff(io.charttime,iosum.charttime,second)/3600,null)) as uo_tm_12hr
  , datetime_diff(io.charttime,min(iosum.charttime),second)/3600 as uo_tm_24hr
  from `physionet-data.mimic_icu.outputevents` io
  -- this join gives all UO measurements over the 24 hours preceding this row
  left join `physionet-data.mimic_icu.outputevents` iosum
    on  io.stay_id = iosum.stay_id
    and io.charttime >= iosum.charttime
    and io.charttime <= (DATETIME_ADD(iosum.charttime, INTERVAL 23 HOUR))
  group by io.stay_id, io.charttime
)
select
  ur.stay_id
, ur.charttime
, wd.patientweight
, ur.UrineOutput_6hr
, ur.UrineOutput_12hr
, ur.UrineOutput_24hr
-- calculate rates - adding 1 hour as we assume data charted at 10:00 corresponds to previous hour
, ROUND((ur.UrineOutput_6hr/wd.patientweight/(uo_tm_6hr+1)), 4) AS uo_rt_6hr
, ROUND((ur.UrineOutput_12hr/wd.patientweight/(uo_tm_12hr+1)), 4) AS uo_rt_12hr
, ROUND((ur.UrineOutput_24hr/wd.patientweight/(uo_tm_24hr+1)), 4) AS uo_rt_24hr
-- time of earliest UO measurement that was used to calculate the rate
, uo_tm_6hr
, uo_tm_12hr
, uo_tm_24hr
from ur_stg ur
left join `aki-recovery-prediction.MimicIV.MIMICIV_weight_durations` wd
  on  ur.icustay_id = wd.icustay_id
  and ur.charttime >= wd.starttime
  and ur.charttime <  wd.endtime
order by icustay_id, charttime;

order by stay_id, charttime;
