-- -------------------------------------------------------------------------------
--
-- Load data into the MIMIC-IV Waveform Database tables
--
-- -------------------------------------------------------------------------------
--
-- This script loads waveform metadata from CSV files into the mimiciv_waveforms schema.
--
-- Prerequisites:
--   1. The schema must be created first (run create.sql)
--   2. CSV files must be prepared with the metadata extracted from .hea files
--
-- To run from a terminal:
--   psql "dbname=<DBNAME> user=<USER>" -v mimic_data_dir=<PATH TO WAVEFORM DATA DIR> -f load.sql
--
-- Example:
--   psql "dbname=mimiciv user=postgres" -v mimic_data_dir=/data/mimic4wdb/0.1.0 -f load.sql
--
-- Note: You'll need to first extract the metadata from .hea files into CSV files.
--       Use the provided Python script (extract_metadata.py) to create these CSV files.
--
-- -------------------------------------------------------------------------------

-- Change to the directory containing the data files
\cd :mimic_data_dir

-- Set the search path to mimiciv_waveforms schema
SET search_path TO mimiciv_waveforms;

--------------------------------------------------------
--  Load Data for Table waveform_records
--------------------------------------------------------

\echo ''
\echo '###########################'
\echo 'Loading waveform_records...'
\echo '###########################'

\copy waveform_records (record_id, subject_id, hadm_id, start_datetime, end_datetime, record_duration_sec, file_path, header_file, base_counter_freq, num_segments) FROM 'waveform_records.csv' DELIMITER ',' CSV HEADER NULL ''

\echo 'Table waveform_records successfully loaded.'

-- Show record count
SELECT COUNT(*) || ' records loaded' AS waveform_records_count FROM waveform_records;

--------------------------------------------------------
--  Load Data for Table waveform_segments
--------------------------------------------------------

\echo ''
\echo '############################'
\echo 'Loading waveform_segments...'
\echo '############################'

\copy waveform_segments (record_id, segment_name, segment_num, segment_start_time, segment_duration_sec, segment_header_file, segment_data_file, sampling_frequency, num_signals) FROM 'waveform_segments.csv' DELIMITER ',' CSV HEADER NULL ''

\echo 'Table waveform_segments successfully loaded.'

-- Show segment count
SELECT COUNT(*) || ' segments loaded' AS waveform_segments_count FROM waveform_segments;

--------------------------------------------------------
--  Load Data for Table waveform_signals
--------------------------------------------------------

\echo ''
\echo '###########################'
\echo 'Loading waveform_signals...'
\echo '###########################'

-- Note: segment_id will be matched via record_id and segment_num
-- First, we need to create a temporary table to load the data
CREATE TEMP TABLE temp_waveform_signals (
    record_id VARCHAR(50),
    segment_num INTEGER,
    signal_name VARCHAR(50),
    signal_index INTEGER,
    signal_units VARCHAR(20),
    signal_gain FLOAT,
    signal_baseline INTEGER,
    signal_adc_resolution INTEGER,
    signal_description TEXT,
    signal_type VARCHAR(50)
);

\copy temp_waveform_signals FROM 'waveform_signals.csv' DELIMITER ',' CSV HEADER NULL ''

-- Insert with segment_id lookup
INSERT INTO waveform_signals
    (segment_id, record_id, signal_name, signal_index, signal_units,
     signal_gain, signal_baseline, signal_adc_resolution, signal_description, signal_type)
SELECT
    ws.segment_id,
    t.record_id,
    t.signal_name,
    t.signal_index,
    t.signal_units,
    t.signal_gain,
    t.signal_baseline,
    t.signal_adc_resolution,
    t.signal_description,
    t.signal_type
FROM temp_waveform_signals t
JOIN waveform_segments ws
    ON t.record_id = ws.record_id
    AND t.segment_num = ws.segment_num;

DROP TABLE temp_waveform_signals;

\echo 'Table waveform_signals successfully loaded.'

-- Show signal count
SELECT COUNT(*) || ' signals loaded' AS waveform_signals_count FROM waveform_signals;

--------------------------------------------------------
--  Load Data for Table waveform_numerics
--------------------------------------------------------

\echo ''
\echo '############################'
\echo 'Loading waveform_numerics...'
\echo '############################'

\copy waveform_numerics (record_id, measurement_time, counter_ticks, heart_rate, resp_rate, spo2, nibp_systolic, nibp_diastolic, nibp_mean, abp_systolic, abp_diastolic, abp_mean, cvp, etco2, temperature, measurement_name, measurement_value, measurement_unit) FROM 'waveform_numerics.csv' DELIMITER ',' CSV HEADER NULL ''

\echo 'Table waveform_numerics successfully loaded.'

-- Show numerics count
SELECT COUNT(*) || ' numeric measurements loaded' AS waveform_numerics_count FROM waveform_numerics;

--------------------------------------------------------
--  Summary Statistics
--------------------------------------------------------

\echo ''
\echo '================================'
\echo 'LOADING COMPLETE - SUMMARY'
\echo '================================'

SELECT
    'waveform_records' AS table_name,
    COUNT(*) AS row_count,
    MIN(start_datetime) AS earliest_record,
    MAX(start_datetime) AS latest_record
FROM waveform_records
UNION ALL
SELECT
    'waveform_segments' AS table_name,
    COUNT(*) AS row_count,
    NULL AS earliest_record,
    NULL AS latest_record
FROM waveform_segments
UNION ALL
SELECT
    'waveform_signals' AS table_name,
    COUNT(*) AS row_count,
    NULL AS earliest_record,
    NULL AS latest_record
FROM waveform_signals
UNION ALL
SELECT
    'waveform_numerics' AS table_name,
    COUNT(*) AS row_count,
    MIN(measurement_time) AS earliest_record,
    MAX(measurement_time) AS latest_record
FROM waveform_numerics;

\echo ''
\echo 'Signal type distribution:'
SELECT
    signal_type,
    COUNT(*) AS count,
    COUNT(DISTINCT record_id) AS num_records
FROM waveform_signals
GROUP BY signal_type
ORDER BY count DESC;

\echo ''
\echo '================================'
\echo 'THE END.'
\echo '================================'
