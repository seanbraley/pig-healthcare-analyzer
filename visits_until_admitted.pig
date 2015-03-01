-- Comment

-- Load in file
-- A = LOAD 'NACRS_export3_cleaned.txt' USING PigStorage('|');

-- A = LOAD 'NACRS_export3_cleaned.txt' USING PigStorage('|', '-schema);


A = LOAD 'NACRS_export3_cleaned.txt' USING PigStorage('|') AS (
    data_record_id:chararray,       -- Unique record id
    patient_id:chararray,           -- Unique patient id
    health_link:chararray,          -- Region
    health_link_code:chararray,     -- Code for above region
    fyear:int,                      -- Year
    fqtr:chararray,                 -- {Q1, Q2, Q3, Q4}
    qtr_id:int,                     -- Year concat with quarter number, ie 20073
    mnth_id:int,                    -- Year concat with month#, ie 200
    cyear:int,                      -- Somehow different from fyear????
    cmonth:int,                     -- Lines up with mnth_id, number alone
    sex:chararray,                  -- {M/F}
    age:int,                        -- Age
    agegrp22_num:int,               -- ?????, higher for older patients
    agegrp28:chararray,             -- Age Group, {<1, 1-19, 20-44, 45-64, 65-74, 75-84, 85+}
    ctascode:int,                   -- Priority code
    ctas:chararray,                 -- Priority code+description
    am_casetypecode:chararray,      -- Case Code (EMG)
    am_casetype:chararray,          -- Emergency or Scheduled visit to Emerge
    disp_status:chararray,          -- Discharged or admitted to hospital
    emgtime_hrs:float,              -- Time spent in emerge (hours)
    emgtime_mins:int,               -- Time spent in emerge (mins)
    icd10_mpdx_chapter:chararray,   -- Admittance reason
    ldcause_ishmt:chararray,        -- Doctor assinged cause
    p4rcat:int,                     -- Admittance code
    p4rcat_desc:chararray,          -- Admittance code + desc, {Admitted, Non adm, less urgent, ...}
    dt_start:chararray              -- datestamp, corresponds with fyear not cyear
);


-- Use to remove 'John/Jane Doe Cases, of which there are 50k'
trimmed_dataset = FILTER A BY patient_id != 'LN638180BE';

-- data = ORDER trimmed_dataset BY {cyear DESC, cmonth DESC};
trimmed_dataset = ORDER trimmed_dataset BY qtr_id;

grouped_data = GROUP trimmed_dataset BY patient_id;

--data_out = FOREACH grouped_data {
--    visits = DISTINCT grouped_data.trimmed_dataset.data_record_id;
--    GENERATE group, COUNT(visits) as visits;
--};

-- This produces (BB000143DD,{(20114),(20112),(20094),(20094),(20094),(20094),(20101),(20094),(20091),(20092)})
-- a tuple with id as 1st and bag of tuples as second
-- presumably it limits the thing in the bag by what i speicfy
data_out = FOREACH grouped_data {
    S = ORDER trimmed_dataset BY qtr_id;
    GENERATE S.patient_id, COUNT(trimmed_dataset) AS num_visits, S.qtr_id, S.p4rcat_desc, S.p4rcat;
};

sorted = ORDER data_out BY num_visits DESC;

top_10 = LIMIT sorted 10;

-- STORE top_10 INTO 'output_feb28.txt'

DUMP top_10;