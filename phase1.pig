-- Comment

-- Load in file
-- A = LOAD 'NACRS_export3_cleaned.txt' USING PigStorage('|');
A = LOAD 'NACRS_export3_cleaned.txt' USING PigStorage('|') AS (
    data_record_id:chararray,
    patient_id:chararray,
    health_link:chararray,
    health_link_code:chararray,
    fyear:chararray,
    fqtr:chararray,
    qtr_id:chararray,
    mnth_id:chararray,
    cyear:chararray,
    cmonth:chararray,
    sex:chararray,
    age:chararray,
    agegrp22_num:chararray,
    agegrp28:chararray,
    ctascode:chararray,
    ctas:chararray,
    am_casetypecode:chararray,
    am_casetype:chararray,
    disp_status:chararray,
    emgtime_hrs:float,
    emgtime_mins:chararray,
    icd10_mpdx_chapter:chararray,
    ldcause_ishmt:chararray,
    p4rcat:chararray,
    p4rcat_desc:chararray,
    dt_start:chararray
);

trimmed_dataset = FILTER A BY patient_id != 'LN638180BE';


B = GROUP A BY (icd10_mpdx_chapter, patient_id);

-- B2 = GROUP B BY patient_id;

C = FOREACH B GENERATE flatten($0),COUNT(A);

E = ORDER C BY $2 DESC;

D = LIMIT E 10;

--DUMP D;


-- Which disease has the longest average emerge-hours
by_disease = GROUP A BY icd10_mpdx_chapter;

avg_times = FOREACH by_disease GENERATE
    group as disease,
    AVG(A.emgtime_hrs) as emerge_hours;

avg_times_ordered = ORDER avg_times BY $1 DESC;
lim_50 = LIMIT avg_times_ordered 50;
DUMP lim_50;



patients = GROUP A BY patient_id;
-- Get the number of visits each patient made
patient_counts = FOREACH patients GENERATE
    group as patient_id,
    COUNT(A);

-- Order the result
patient_counts_ordered = ORDER patient_counts BY $1 DESC;

-- Get the top 10 visitors
-- LN638180BE has way too many
top_10 = LIMIT patient_counts_ordered 50;
-- DUMP top_10;

count = FOREACH patients GENERATE $0,COUNT(A);
total = ORDER count BY $1 DESC;
lim = LIMIT total 10;
-- DUMP lim;
-- STORE C INTO 'pig_output';

by_patient_reason = GROUP trimmed_dataset BY (icd10_mpdx_chapter, patient_id, agegrp28);

by_patient_reason_counts = FOREACH by_patient_reason GENERATE
    FLATTEN(group) AS (reason, patient, age_group),
    COUNT(trimmed_dataset) AS count_patients_reason;

ordered = ORDER by_patient_reason_counts BY count_patients_reason DESC;
top_50 = LIMIT ordered 50;
-- DUMP top_50;