-- This query extracts weights for adult ICU patients with start/stop times
-- if an admission weight is given, then this is assigned from intime to outtime

-- This query extracts weights for adult ICU patients with start/stop times
-- if an admission weight is given, then this is assigned from intime to outtime
CREATE OR REPLACE TABLE `aki-recovery-prediction.MimicIV.MIMICIV_weight_durations` AS
WITH wt_neonate AS
( 
    SELECT c.stay_id, c.charttime
    , MAX(CASE WHEN c.itemid = 3580 THEN c.valuenum END) as wt_kg
    , MAX(CASE WHEN c.itemid = 3581 THEN c.valuenum END) as wt_lb
    , MAX(CASE WHEN c.itemid = 3582 THEN c.valuenum END) as wt_oz
    FROM `physionet-data.mimic_icu.chartevents` c
    WHERE c.itemid in (3580, 3581, 3582)
    AND c.stay_id IS NOT NULL
#    AND COALESCE(c.error, 0) = 0
    -- wt_oz/wt_lb/wt_kg are only 0 erroneously, so drop these rows
    AND c.valuenum > 0
  -- a separate query was run to manually verify only 1 value exists per
  -- stay_id/charttime/itemid grouping
  -- therefore, we can use max() across itemid to collapse these values to 1 row per group
    GROUP BY c.stay_id, c.charttime
)
, birth_wt AS
(
    SELECT c.stay_id, c.charttime
    , MAX(
      CASE
      WHEN c.itemid = 4183 THEN
        -- clean free-text birth weight data
        CASE
          -- ignore value if there are any non-numeric characters
          WHEN REGEXP_CONTAINS(c.value, '[^0-9\\.]') THEN NULL 
          -- convert grams to kd
          WHEN CAST(c.value AS NUMERIC) > 100 THEN CAST(c.value AS NUMERIC)/1000
          -- keep kg as is, filtering bad values (largest baby ever born was conveniently 9.98kg)
          WHEN CAST(c.value AS NUMERIC) < 10 THEN CAST(c.value AS NUMERIC)
          -- ignore other values (those between 10-100) - junk data
        ELSE NULL END
      -- itemid 3723 happily has all numeric data - also doesn't store any grams data
      WHEN c.itemid = 3723 AND c.valuenum < 10 THEN c.valuenum
      ELSE NULL END) as wt_kg
    FROM `physionet-data.mimic_icu.chartevents` c
    WHERE c.itemid in (3723, 4183)
    AND c.stay_id IS NOT NULL
#    AND COALESCE(c.error, 0) = 0
  -- a separate query was run to manually verify only 1 value exists per
  -- stay_id/charttime/itemid grouping
  -- therefore, we can use max() across itemid to collapse these values to 1 row per group
    GROUP BY c.stay_id, c.charttime
)
, wt_stg as
(
    SELECT
        c.stay_id
      , c.charttime
      , case when c.itemid in (762,226512) then 'admit'
          else 'daily' end as weight_type
      -- TODO: eliminate obvious outliers if there is a reasonable weight
      , c.valuenum as weight
    FROM `physionet-data.mimic_icu.chartevents` c
    WHERE c.valuenum IS NOT NULL
      AND c.itemid in
      (
          762,226512 -- Admit Wt
        , 763,224639 -- Daily Weight
      )
      AND c.stay_id IS NOT NULL
      AND c.valuenum > 0
      -- exclude rows marked as error
#      AND COALESCE(c.error, 0) = 0
    UNION ALL
    SELECT
        n.stay_id
      , n.charttime
      , 'daily' AS weight_type
      , CASE
          WHEN wt_kg IS NOT NULL THEN wt_kg
          WHEN wt_lb IS NOT NULL THEN wt_lb*0.45359237 + wt_oz*0.0283495231
        ELSE NULL END AS weight
    FROM wt_neonate n
    UNION ALL
    SELECT
        b.stay_id
      , b.charttime
      -- birth weight of neonates is treated as admission weight
      , 'admit' AS weight_type
      , wt_kg as weight
    FROM birth_wt b
)
-- assign ascending row number
, wt_stg1 as
(
  select
      stay_id
    , charttime
    , weight_type
    , weight
    , ROW_NUMBER() OVER (partition by stay_id, weight_type order by charttime) as rn
  from wt_stg
  WHERE weight IS NOT NULL
)
-- change charttime to intime for the first admission weight recorded
, wt_stg2 AS
(
  SELECT 
      wt_stg1.stay_id
    , ie.intime, ie.outtime
    , case when wt_stg1.weight_type = 'admit' and wt_stg1.rn = 1
        then DATETIME_SUB(ie.intime, INTERVAL '2' HOUR)
      else wt_stg1.charttime end as starttime
    , wt_stg1.weight
  from wt_stg1
  INNER JOIN `physionet-data.mimic_icu.icustays` ie
    on ie.stay_id = wt_stg1.stay_id
)
, wt_stg3 as
(
  select
    stay_id
    , intime, outtime
    , starttime
    , coalesce(
        LEAD(starttime) OVER (PARTITION BY stay_id ORDER BY starttime),
        DATETIME_ADD(outtime, INTERVAL '2' HOUR)
      ) as endtime
    , weight
  from wt_stg2
)
-- this table is the start/stop times from admit/daily weight in charted data
, wt1 as
(
  select
      stay_id
    , starttime
    , coalesce(endtime,
      LEAD(starttime) OVER (partition by stay_id order by starttime),
      -- impute ICU discharge as the end of the final weight measurement
      -- plus a 2 hour "fuzziness" window
      DATETIME_ADD(outtime, INTERVAL '2' HOUR)
    ) as endtime
    , weight
  from wt_stg3
)
-- if the intime for the patient is < the first charted daily weight
-- then we will have a "gap" at the start of their stay
-- to prevent this, we look for these gaps and backfill the first weight
-- this adds (153255-149657)=3598 rows, meaning this fix helps for up to 3598 stay_id
, wt_fix as
(
  select ie.stay_id
    -- we add a 2 hour "fuzziness" window
    , DATETIME_SUB(ie.intime, INTERVAL '2' HOUR) as starttime
    , wt.starttime as endtime
    , wt.weight
  from `physionet-data.mimic_icu.icustays` ie
  inner join
  -- the below subquery returns one row for each unique stay_id
  -- the row contains: the first starttime and the corresponding weight
  (
    SELECT wt1.stay_id, wt1.starttime, wt1.weight
    , ROW_NUMBER() OVER (PARTITION BY wt1.stay_id ORDER BY wt1.starttime) as rn
    FROM wt1
  ) wt
    ON  ie.stay_id = wt.stay_id
    AND wt.rn = 1
    and ie.intime < wt.starttime
)
-- add the backfill rows to the main weight table
, wt2 as
(
  select
      wt1.stay_id
    , wt1.starttime
    , wt1.endtime
    , wt1.weight
  from wt1
  UNION ALL
  SELECT
      wt_fix.stay_id
    , wt_fix.starttime
    , wt_fix.endtime
    , wt_fix.weight
  from wt_fix
)
-- get more weights from echo - completes data for ~2500 patients
-- we only use echo data if there is *no* charted data
-- we impute the median echo weight for their entire ICU stay
#, echo_lag as
#(
#  select
#    ie.stay_id
#    , ie.intime, ie.outtime
#    , 0.453592*ec.weight as weight_echo
#    , ROW_NUMBER() OVER (PARTITION BY ie.stay_id ORDER BY ec.charttime) as rn
#    , ec.charttime as starttime
#    , LEAD(ec.charttime) OVER (PARTITION BY ie.stay_id ORDER BY ec.charttime) as endtime
#  from `physionet-data.mimic_icu.icustays` ie
#  inner join `physionet-data.mimiciii_notes.echo_data` ec
#    on ie.hadm_id = ec.hadm_id
#  where ec.weight is not null
#)
#, echo_final as
#(
#    select
#      el.stay_id
#      , el.starttime
#        -- we add a 2 hour "fuzziness" window
#      , coalesce(el.endtime, DATETIME_ADD(el.outtime, INTERVAL '2' HOUR)) as endtime
#      , weight_echo
#    from echo_lag el
#    UNION ALL
#    -- if the starttime was later than ICU admission, back-propogate the weight
#    select
#      el.stay_id
#      , DATETIME_SUB(el.intime, INTERVAL '2' HOUR) as starttime
#      , el.starttime as endtime
#      , el.weight_echo
#    from echo_lag el
#    where el.rn = 1
#    and el.starttime > DATETIME_SUB(el.intime, INTERVAL '2' HOUR)
#)
select
  wt2.stay_id, wt2.starttime, wt2.endtime, wt2.weight
from wt2
-- only add echos if we have no charted weight data
#select
#  ef.stay_id, ef.starttime, ef.endtime, ef.weight_echo as weight
#from echo_final ef
#where ef.stay_id not in (select distinct stay_id from wt2)
order by stay_id, starttime, endtime;
