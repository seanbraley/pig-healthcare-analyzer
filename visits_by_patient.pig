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

patients = GROUP trimmed_dataset BY patient_id;
-- Get the number of visits each patient made
patient_counts = FOREACH patients GENERATE
    group as patient_id,
    COUNT(A);

-- Order the result
patient_counts_ordered = ORDER patient_counts BY $1 DESC;

-- Get the top 10 visitors
top_10 = LIMIT patient_counts_ordered 50;
DUMP top_10;