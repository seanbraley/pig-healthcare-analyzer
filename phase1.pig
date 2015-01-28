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
    emgtime_hrs:chararray,
    emgtime_mins:chararray,
    icd10_mpdx_chapter:chararray,
    ldcause_ishmt:chararray,
    p4rcat:chararray,
    p4rcat_desc:chararray,
    dt_start:chararray
);


B = GROUP A BY (icd10_mpdx_chapter, patient_id);

-- B2 = GROUP B BY patient_id;

C = FOREACH B GENERATE flatten($0),COUNT(A);

E = ORDER C BY $2 DESC;

D = LIMIT E 10;

--DUMP D;

patients = GROUP A BY patient_id;
count = FOREACH patients GENERATE $0,COUNT(A);
total = ORDER count BY $1 DESC;
lim = LIMIT total 10;
DUMP lim;
-- STORE C INTO 'pig_output';