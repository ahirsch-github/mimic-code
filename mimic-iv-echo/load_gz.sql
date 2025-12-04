-- -----------------------------------------------------------------------------
-- load_gz.sql
-- Load gzipped MIMIC-IV-ECHO CSV files into Postgres
-- Compatible with the schema definitions in mimiciv_echo_create_load_index.sql
-- -----------------------------------------------------------------------------

\set ON_ERROR_STOP on
SET search_path = mimiciv_echo, public;

-- -----------------------------------------------------------------------------
-- Adjust these variables to your dataset location
-- Example: \set mimic_data_dir '/data/mimic-iv-echo'
-- -----------------------------------------------------------------------------
\echo '--- Set your dataset directory path first ---'
\echo 'Example: \set mimic_data_dir /path/to/mimic-iv-echo'

-- Check that the variable is defined
\if :{?mimic_data_dir}
    \echo Using directory :mimic_data_dir
\else
    \error 'You must set the variable "mimic_data_dir" before running this script. Example: \set mimic_data_dir /path/to/mimic-iv-echo'
\endif

-- -----------------------------------------------------------------------------
-- Load echo_record_list.csv.gz
-- -----------------------------------------------------------------------------
\echo 'Loading echo_record_list.csv.gz ...'

COPY echo_record_list (
    dicom_filepath,
    acquisition_datetime,
    study_id,
    subject_id,
    dicom_filename,
    view_number
)
FROM PROGRAM 'gzip -dc ' || :'mimic_data_dir' || '/echo-record-list.csv.gz'
WITH (FORMAT csv, HEADER true);

\echo 'Finished loading echo_record_list.csv.gz.'

-- -----------------------------------------------------------------------------
-- Load echo_study_list.csv.gz
-- -----------------------------------------------------------------------------
\echo 'Loading echo_study_list.csv.gz ...'

COPY echo_study_list (
    study_id,
    subject_id,
    study_datetime,
    note_id,
    note_seq,
    note_charttime
)
FROM PROGRAM 'gzip -dc ' || :'mimic_data_dir' || '/echo-study-list.csv.gz'
WITH (FORMAT csv, HEADER true);

\echo 'Finished loading echo_study_list.csv.gz.'

-- -----------------------------------------------------------------------------
-- Done
-- -----------------------------------------------------------------------------
\echo '✅ All MIMIC-IV-ECHO tables loaded successfully.'
