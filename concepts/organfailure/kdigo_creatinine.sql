with cr as
(
select
    ie.stay_id
  , ie.intime, ie.outtime
  , le.valuenum as creat
  , le.charttime
  from `physionet-data.mimic_icu.icustays` ie
  left join `physionet-data.mimic_icu.chartevents` le
    on ie.subject_id = le.subject_id
    and le.ITEMID = 220615 # maybe and 229761
    and le.VALUENUM is not null
    --and DATETIME_DIFF(le.charttime, ie.intime, HOUR) <= (7*24-6)
    and le.CHARTTIME >= DATETIME_SUB(ie.intime, INTERVAL 6 HOUR)
    --and le.CHARTTIME <= DATETIME_ADD(ie.intime, INTERVAL 7 DAY)
    )
, firstCr as (
select
    cr.stay_id
  , cr.creat
  , cr.charttime
  -- Create an index that goes from 1, 2, ..., N
  -- The index represents how early in the patient's stay a creatinine value was measured
  -- Consequently, when we later select index == 1, we only select the first (admission) creatinine
  -- In addition, we only select the first stay for the given subject_id
  , ROW_NUMBER ()
          OVER (PARTITION BY cr.stay_id
                ORDER BY cr.charttime
              ) as rn_first
from cr
)
-- add in the lowest value in the previous 48 hours/7 days
SELECT
  cr.stay_id
  , cr.charttime
  , cr.creat
  , MIN(cr48.creat) AS creat_low_past_48hr
  , MIN(cr7.creat) AS creat_low_past_7day
  , firstCr.creat as AdmCreat
  , firstCr.charttime as AdmCreatTime
FROM cr
-- add in all creatinine values in the last 48 hours
LEFT JOIN cr cr48
  ON cr.stay_id = cr48.stay_id
  AND cr48.charttime <  cr.charttime
  AND cr48.charttime >= datetime_sub(cr.charttime, INTERVAL '48' HOUR)
-- add in all creatinine values in the last 7 days hours
LEFT JOIN cr cr7
  ON cr.stay_id = cr7.stay_id
  AND cr7.charttime <  cr.charttime
  AND cr7.charttime >= datetime_sub(cr.charttime, INTERVAL '7' DAY)
LEFT JOIN firstCr
  ON cr.stay_id = firstCr.stay_id
  AND firstCr.rn_first = 1
GROUP BY cr.stay_id, cr.charttime, cr.creat, firstCr.creat, firstCr.charttime
ORDER BY cr.stay_id, cr.charttime, cr.creat
