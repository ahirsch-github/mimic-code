-- -------------------------------------------------------------------------------
--
-- Load data into the MIMIC-IV-ECG tables (mimiciv_ed schema)
--
-- -------------------------------------------------------------------------------
--
-- This script loads ECG data from CSV files into the mimiciv_ed schema.
--
-- To run from a terminal:
--   psql "dbname=<DBNAME> user=<USER>" -v mimic_data_dir=<PATH TO ECG DATA DIR> -f load.sql
--
-- Example:
--   psql "dbname=mimic user=postgres" -v mimic_data_dir=/Users/anja/Documents/Promotion/Daten/MIMIC/MIMIC-IV-ECG -f load.sql
--
-- -------------------------------------------------------------------------------

-- Change to the directory containing the data files
\cd :mimic_data_dir

-- Set the search path to mimiciv_ed schema
SET search_path TO mimiciv_ed;

--------------------------------------------------------
--  Load Data for Table ecg_record_list
--------------------------------------------------------

\echo '##########################'
\echo 'Copying ecg_record_list...'
\copy ecg_record_list FROM 'record_list.csv' DELIMITER ',' CSV HEADER NULL ''
\echo 'Table ecg_record_list successfully loaded.'

--------------------------------------------------------
--  Load Data for Table ecg_machine_measurements
--------------------------------------------------------

\echo '##################################'
\echo 'Copying ecg_machine_measurements...'
\copy ecg_machine_measurements FROM 'machine_measurements.csv' DELIMITER ',' CSV HEADER NULL ''
\echo 'Table ecg_machine_measurements successfully loaded.'

--------------------------------------------------------
--  Load Data for Table ecg_diagnostic_labels
--------------------------------------------------------

\echo '################################'
\echo 'Copying ecg_diagnostic_labels...'
\copy ecg_diagnostic_labels FROM 'mimic-iv-ecg-ext-icd-diagnostic-labels-for-mimic-iv-ecg-1.0.1/records_w_diag_icd10_int.csv' DELIMITER ',' CSV HEADER NULL ''
\echo 'Table ecg_diagnostic_labels successfully loaded.'

\echo ''
\echo 'All ECG tables successfully loaded.'
\echo 'THE END.'
