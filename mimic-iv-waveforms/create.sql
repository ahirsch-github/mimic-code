-- -------------------------------------------------------------------------------
--
-- Create the MIMIC-IV Waveform Database tables
--
-- -------------------------------------------------------------------------------
--
-- This script creates tables for waveform data from the MIMIC-IV Waveform Database.
-- The waveform data includes physiological signals (ECG, ABP, Pleth, etc.) from ICU patients.
--
-- Data structure:
--   - Waveform signals (.dat files) are stored in the filesystem (NOT in the database)
--   - Only metadata and references to the waveform files are stored in the database
--   - Numeric measurements (from CSV files) are stored in the database
--
-- Tables:
--   - waveform_records: Main metadata for each waveform recording
--   - waveform_segments: Individual segments within a recording
--   - waveform_signals: Available signals per segment (ECG leads, pressures, etc.)
--   - waveform_numerics: Time-series numeric measurements (SpO2, derived values)
--
-- -------------------------------------------------------------------------------

-- Set the schema (adjust as needed for your database)
-- For consistency with MIMIC-IV, you might use 'mimiciv_waveforms' or similar
DROP SCHEMA IF EXISTS mimiciv_waveforms CASCADE;
CREATE SCHEMA mimiciv_waveforms;

SET search_path TO mimiciv_waveforms;

--------------------------------------------------------
--  DDL for Table waveform_records
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_waveforms.waveform_records CASCADE;
CREATE TABLE mimiciv_waveforms.waveform_records
(
  record_id VARCHAR(50) NOT NULL,
  subject_id INTEGER NOT NULL,
  hadm_id INTEGER,

  -- Temporal information
  start_datetime TIMESTAMP NOT NULL,
  end_datetime TIMESTAMP,
  record_duration_sec FLOAT,

  -- File system references
  file_path TEXT NOT NULL,
  header_file VARCHAR(100) NOT NULL,

  -- Technical metadata
  base_counter_freq FLOAT,

  -- Record structure
  num_segments INTEGER,

  CONSTRAINT pk_waveform_records PRIMARY KEY (record_id)
);

--------------------------------------------------------
--  DDL for Table waveform_segments
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_waveforms.waveform_segments CASCADE;
CREATE TABLE mimiciv_waveforms.waveform_segments
(
  segment_id SERIAL PRIMARY KEY,
  record_id VARCHAR(50) NOT NULL,
  segment_name VARCHAR(100) NOT NULL,
  segment_num INTEGER NOT NULL,

  -- Temporal information
  segment_start_time TIMESTAMP,
  segment_duration_sec FLOAT,

  -- File references
  segment_header_file VARCHAR(100),
  segment_data_file VARCHAR(100),

  -- Technical details
  sampling_frequency FLOAT,
  num_signals INTEGER,

  CONSTRAINT fk_segment_record FOREIGN KEY (record_id)
    REFERENCES mimiciv_waveforms.waveform_records(record_id)
    ON DELETE CASCADE,
  CONSTRAINT uq_record_segment UNIQUE (record_id, segment_num)
);

--------------------------------------------------------
--  DDL for Table waveform_signals
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_waveforms.waveform_signals CASCADE;
CREATE TABLE mimiciv_waveforms.waveform_signals
(
  signal_id SERIAL PRIMARY KEY,
  segment_id INTEGER NOT NULL,
  record_id VARCHAR(50) NOT NULL,

  -- Signal identification
  signal_name VARCHAR(50) NOT NULL,
  signal_index INTEGER NOT NULL,

  -- Signal properties
  signal_units VARCHAR(20),
  signal_gain FLOAT,
  signal_baseline INTEGER,
  signal_adc_resolution INTEGER,

  -- Signal description/type
  signal_description TEXT,
  signal_type VARCHAR(50), -- e.g., 'ECG', 'ABP', 'Pleth', 'Resp'

  CONSTRAINT fk_signal_segment FOREIGN KEY (segment_id)
    REFERENCES mimiciv_waveforms.waveform_segments(segment_id)
    ON DELETE CASCADE,
  CONSTRAINT fk_signal_record FOREIGN KEY (record_id)
    REFERENCES mimiciv_waveforms.waveform_records(record_id)
    ON DELETE CASCADE,
  CONSTRAINT uq_segment_signal UNIQUE (segment_id, signal_index)
);

--------------------------------------------------------
--  DDL for Table waveform_numerics
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_waveforms.waveform_numerics CASCADE;
CREATE TABLE mimiciv_waveforms.waveform_numerics
(
  numeric_id BIGSERIAL PRIMARY KEY,
  record_id VARCHAR(50) NOT NULL,

  -- Temporal information
  measurement_time TIMESTAMP NOT NULL,
  counter_ticks BIGINT,

  -- Vital signs and derived measurements
  heart_rate INTEGER,
  resp_rate INTEGER,
  spo2 INTEGER,

  -- Blood pressure measurements
  nibp_systolic INTEGER,
  nibp_diastolic INTEGER,
  nibp_mean INTEGER,

  abp_systolic INTEGER,
  abp_diastolic INTEGER,
  abp_mean INTEGER,

  -- Other measurements (can be extended)
  cvp FLOAT,
  etco2 FLOAT,
  temperature FLOAT,

  -- Generic columns for other measurements
  measurement_name VARCHAR(100),
  measurement_value FLOAT,
  measurement_unit VARCHAR(20),

  CONSTRAINT fk_numeric_record FOREIGN KEY (record_id)
    REFERENCES mimiciv_waveforms.waveform_records(record_id)
    ON DELETE CASCADE
);

--------------------------------------------------------
--  Create Indexes
--------------------------------------------------------

-- waveform_records indexes
CREATE INDEX idx_wf_records_subject ON mimiciv_waveforms.waveform_records(subject_id);
CREATE INDEX idx_wf_records_hadm ON mimiciv_waveforms.waveform_records(hadm_id);
CREATE INDEX idx_wf_records_start_time ON mimiciv_waveforms.waveform_records(start_datetime);
CREATE INDEX idx_wf_records_path ON mimiciv_waveforms.waveform_records(file_path);

-- waveform_segments indexes
CREATE INDEX idx_wf_segments_record ON mimiciv_waveforms.waveform_segments(record_id);
CREATE INDEX idx_wf_segments_time ON mimiciv_waveforms.waveform_segments(segment_start_time);

-- waveform_signals indexes
CREATE INDEX idx_wf_signals_segment ON mimiciv_waveforms.waveform_signals(segment_id);
CREATE INDEX idx_wf_signals_record ON mimiciv_waveforms.waveform_signals(record_id);
CREATE INDEX idx_wf_signals_name ON mimiciv_waveforms.waveform_signals(signal_name);
CREATE INDEX idx_wf_signals_type ON mimiciv_waveforms.waveform_signals(signal_type);

-- waveform_numerics indexes
CREATE INDEX idx_wf_numerics_record ON mimiciv_waveforms.waveform_numerics(record_id);
CREATE INDEX idx_wf_numerics_time ON mimiciv_waveforms.waveform_numerics(measurement_time);
CREATE INDEX idx_wf_numerics_name ON mimiciv_waveforms.waveform_numerics(measurement_name);

--------------------------------------------------------
--  Table Comments
--------------------------------------------------------

COMMENT ON TABLE mimiciv_waveforms.waveform_records IS
'Main metadata table for waveform recordings. Each record represents one patient stay with waveform monitoring. The actual waveform data (.dat files) remain in the filesystem; only references are stored here.';

COMMENT ON TABLE mimiciv_waveforms.waveform_segments IS
'Individual time segments within a waveform recording. Waveform data is typically split into multiple segments for manageability.';

COMMENT ON TABLE mimiciv_waveforms.waveform_signals IS
'Catalog of available physiological signals within each segment (e.g., ECG leads, arterial blood pressure, plethysmogram, respiration).';

COMMENT ON TABLE mimiciv_waveforms.waveform_numerics IS
'Time-series numeric measurements derived from waveforms or sampled irregularly. These are extracted from the numerics CSV files (e.g., SpO2, heart rate, blood pressure values).';

--------------------------------------------------------
--  Column Comments
--------------------------------------------------------

-- waveform_records columns
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.record_id IS 'Unique identifier for the waveform record (typically the study ID)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.subject_id IS 'Foreign key to patients table - unique patient identifier';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.hadm_id IS 'Hospital admission ID - links to admissions table';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.start_datetime IS 'Date and time when the recording started';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.record_duration_sec IS 'Total duration of the recording in seconds';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.file_path IS 'Relative file system path to the waveform data directory (e.g., p100/p10039708/83411188/)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.header_file IS 'Name of the multi-segment header file (e.g., 83411188.hea)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.base_counter_freq IS 'Base counter frequency for timestamp conversion (from header file)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_records.num_segments IS 'Number of segments in this recording';

-- waveform_segments columns
COMMENT ON COLUMN mimiciv_waveforms.waveform_segments.segment_id IS 'Unique identifier for the segment';
COMMENT ON COLUMN mimiciv_waveforms.waveform_segments.record_id IS 'Foreign key to waveform_records';
COMMENT ON COLUMN mimiciv_waveforms.waveform_segments.segment_name IS 'Name/identifier of the segment (e.g., 83411188_0001)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_segments.segment_num IS 'Sequential segment number within the record';
COMMENT ON COLUMN mimiciv_waveforms.waveform_segments.segment_data_file IS 'Name of the data file (e.g., 83411188_0001e.dat)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_segments.sampling_frequency IS 'Sampling frequency in Hz for this segment';

-- waveform_signals columns
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_id IS 'Unique identifier for the signal';
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_name IS 'Name of the signal (e.g., II, V, ABP, Pleth)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_index IS 'Index/channel number of the signal within the segment';
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_units IS 'Measurement units (e.g., mV for ECG, mmHg for pressure)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_gain IS 'Gain value for converting ADC values to physical units';
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_baseline IS 'Baseline value for ADC conversion';
COMMENT ON COLUMN mimiciv_waveforms.waveform_signals.signal_type IS 'Categorization of signal type (ECG, ABP, Pleth, Resp, CVP, etc.)';

-- waveform_numerics columns
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.numeric_id IS 'Unique identifier for the numeric measurement';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.record_id IS 'Foreign key to waveform_records';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.measurement_time IS 'Timestamp of the measurement';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.counter_ticks IS 'Time in counter ticks from record start (for precise timing)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.heart_rate IS 'Heart rate in beats per minute';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.resp_rate IS 'Respiratory rate in breaths per minute';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.spo2 IS 'Oxygen saturation percentage (0-100)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.nibp_systolic IS 'Non-invasive blood pressure - systolic (mmHg)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.abp_mean IS 'Arterial blood pressure - mean (mmHg)';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.measurement_name IS 'Generic field for other measurement types';
COMMENT ON COLUMN mimiciv_waveforms.waveform_numerics.measurement_value IS 'Generic field for other measurement values';

--------------------------------------------------------
--  Example Queries
--------------------------------------------------------

-- Find all recordings for a specific patient
-- SELECT * FROM mimiciv_waveforms.waveform_records WHERE subject_id = 10039708;

-- Get all available signals for a recording
-- SELECT ws.signal_name, ws.signal_units, ws.signal_type, wseg.sampling_frequency
-- FROM mimiciv_waveforms.waveform_signals ws
-- JOIN mimiciv_waveforms.waveform_segments wseg ON ws.segment_id = wseg.segment_id
-- WHERE ws.record_id = '83411188'
-- ORDER BY wseg.segment_num, ws.signal_index;

-- Get SpO2 measurements for a recording
-- SELECT measurement_time, spo2, heart_rate
-- FROM mimiciv_waveforms.waveform_numerics
-- WHERE record_id = '83411188' AND spo2 IS NOT NULL
-- ORDER BY measurement_time;

-- Link waveform data to MIMIC-IV clinical data
-- SELECT wr.record_id, wr.start_datetime, p.gender, p.anchor_age, a.admittime
-- FROM mimiciv_waveforms.waveform_records wr
-- JOIN mimiciv_hosp.patients p ON wr.subject_id = p.subject_id
-- LEFT JOIN mimiciv_hosp.admissions a ON wr.hadm_id = a.hadm_id
-- WHERE wr.subject_id = 10039708;
