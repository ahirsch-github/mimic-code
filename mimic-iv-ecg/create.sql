-- -------------------------------------------------------------------------------
--
-- Create the MIMIC-IV-ECG tables in mimiciv_ed schema
--
-- -------------------------------------------------------------------------------
--
-- This script creates tables for ECG data in the mimiciv_ed schema.
-- The ECG data can be linked to other MIMIC-IV data using subject_id and study_id.
--
-- Tables:
--   - ecg_record_list: Metadata about ECG recordings (file paths, timestamps)
--   - ecg_machine_measurements: Machine measurements and automated interpretations
--   - ecg_diagnostic_labels: ICD-10 diagnostic labels linked to ECG records
--
-- -------------------------------------------------------------------------------

--------------------------------------------------------
--  DDL for Table ecg_record_list
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_ed.ecg_record_list CASCADE;
CREATE TABLE mimiciv_ed.ecg_record_list
(
  subject_id INTEGER NOT NULL,
  study_id INTEGER NOT NULL,
  file_name VARCHAR(50) NOT NULL,
  ecg_time TIMESTAMP(0) NOT NULL,
  path TEXT NOT NULL
);

--------------------------------------------------------
--  DDL for Table ecg_machine_measurements
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_ed.ecg_machine_measurements CASCADE;
CREATE TABLE mimiciv_ed.ecg_machine_measurements
(
  subject_id INTEGER NOT NULL,
  study_id INTEGER NOT NULL,
  cart_id INTEGER,
  ecg_time TIMESTAMP(0) NOT NULL,

  -- Automated interpretation reports (up to 18 findings)
  report_0 VARCHAR(255),
  report_1 VARCHAR(255),
  report_2 VARCHAR(255),
  report_3 VARCHAR(255),
  report_4 VARCHAR(255),
  report_5 VARCHAR(255),
  report_6 VARCHAR(255),
  report_7 VARCHAR(255),
  report_8 VARCHAR(255),
  report_9 VARCHAR(255),
  report_10 VARCHAR(255),
  report_11 VARCHAR(255),
  report_12 VARCHAR(255),
  report_13 VARCHAR(255),
  report_14 VARCHAR(255),
  report_15 VARCHAR(255),
  report_16 VARCHAR(255),
  report_17 VARCHAR(255),

  -- Recording settings
  bandwidth TEXT,
  filtering TEXT,

  -- Interval measurements (ms)
  rr_interval INTEGER,
  p_onset INTEGER,
  p_end INTEGER,
  qrs_onset INTEGER,
  qrs_end INTEGER,
  t_end INTEGER,

  -- Axis measurements (degrees)
  p_axis INTEGER,
  qrs_axis INTEGER,
  t_axis INTEGER
);

--------------------------------------------------------
--  DDL for Table ecg_diagnostic_labels
--------------------------------------------------------

DROP TABLE IF EXISTS mimiciv_ed.ecg_diagnostic_labels CASCADE;
CREATE TABLE mimiciv_ed.ecg_diagnostic_labels
(
  file_name TEXT NOT NULL,
  study_id INTEGER NOT NULL,
  subject_id INTEGER NOT NULL,
  ecg_time TIMESTAMP(0) NOT NULL,

  -- Stay and admission identifiers
  ed_stay_id INTEGER,
  ed_hadm_id INTEGER,
  hosp_hadm_id INTEGER,

  -- ICD-10 diagnostic codes (semicolon-separated lists)
  ed_diag_ed TEXT,
  ed_diag_hosp TEXT,
  hosp_diag_hosp TEXT,
  all_diag_hosp TEXT,
  all_diag_all TEXT,

  -- Patient demographics
  gender VARCHAR(10),
  age FLOAT,
  anchor_year FLOAT,
  anchor_age FLOAT,
  dod DATE,

  -- ECG context information
  ecg_no_within_stay INTEGER,
  ecg_taken_in_ed BOOLEAN,
  ecg_taken_in_hosp BOOLEAN,
  ecg_taken_in_ed_or_hosp BOOLEAN,

  -- Cross-validation fold assignments
  fold INTEGER,
  strat_fold INTEGER
);

-- Create indexes for common queries
CREATE INDEX idx_ecg_record_list_subject ON mimiciv_ed.ecg_record_list(subject_id);
CREATE INDEX idx_ecg_record_list_study ON mimiciv_ed.ecg_record_list(study_id);
CREATE INDEX idx_ecg_record_list_time ON mimiciv_ed.ecg_record_list(ecg_time);

CREATE INDEX idx_ecg_machine_subject ON mimiciv_ed.ecg_machine_measurements(subject_id);
CREATE INDEX idx_ecg_machine_study ON mimiciv_ed.ecg_machine_measurements(study_id);
CREATE INDEX idx_ecg_machine_time ON mimiciv_ed.ecg_machine_measurements(ecg_time);

CREATE INDEX idx_ecg_diag_subject ON mimiciv_ed.ecg_diagnostic_labels(subject_id);
CREATE INDEX idx_ecg_diag_study ON mimiciv_ed.ecg_diagnostic_labels(study_id);
CREATE INDEX idx_ecg_diag_ed_stay ON mimiciv_ed.ecg_diagnostic_labels(ed_stay_id);
CREATE INDEX idx_ecg_diag_ed_hadm ON mimiciv_ed.ecg_diagnostic_labels(ed_hadm_id);
CREATE INDEX idx_ecg_diag_hosp_hadm ON mimiciv_ed.ecg_diagnostic_labels(hosp_hadm_id);
CREATE INDEX idx_ecg_diag_time ON mimiciv_ed.ecg_diagnostic_labels(ecg_time);

-- Comments on tables
COMMENT ON TABLE mimiciv_ed.ecg_record_list IS 'Metadata for ECG recordings including file paths and timestamps';
COMMENT ON TABLE mimiciv_ed.ecg_machine_measurements IS 'Automated machine measurements and interpretations from ECG recordings';
COMMENT ON TABLE mimiciv_ed.ecg_diagnostic_labels IS 'ICD-10 diagnostic labels and patient context linked to ECG records';

-- Comments on key columns
COMMENT ON COLUMN mimiciv_ed.ecg_record_list.subject_id IS 'Foreign key to patients table';
COMMENT ON COLUMN mimiciv_ed.ecg_record_list.study_id IS 'Unique identifier for this ECG study';
COMMENT ON COLUMN mimiciv_ed.ecg_record_list.path IS 'File path to the ECG waveform data';

COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.subject_id IS 'Foreign key to patients table';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.study_id IS 'Foreign key to ecg_record_list';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.bandwidth IS 'Bandwidth filter settings used during recording';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.filtering IS 'Filtering settings applied to the ECG recording';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.rr_interval IS 'RR interval in milliseconds';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.p_axis IS 'P wave axis in degrees';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.qrs_axis IS 'QRS complex axis in degrees';
COMMENT ON COLUMN mimiciv_ed.ecg_machine_measurements.t_axis IS 'T wave axis in degrees';

COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.subject_id IS 'Foreign key to patients table';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.study_id IS 'Foreign key to ecg_record_list';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.ed_stay_id IS 'Emergency department stay identifier';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.ed_hadm_id IS 'Emergency department hospital admission identifier';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.hosp_hadm_id IS 'Hospital admission identifier';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.ed_diag_ed IS 'ICD-10 diagnoses from ED stay documented in ED (semicolon-separated)';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.ed_diag_hosp IS 'ICD-10 diagnoses from ED stay documented in hospital (semicolon-separated)';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.hosp_diag_hosp IS 'ICD-10 diagnoses from hospital stay (semicolon-separated)';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.all_diag_hosp IS 'All ICD-10 diagnoses from hospital (semicolon-separated)';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.all_diag_all IS 'All ICD-10 diagnoses from all sources (semicolon-separated)';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.fold IS 'Cross-validation fold assignment';
COMMENT ON COLUMN mimiciv_ed.ecg_diagnostic_labels.strat_fold IS 'Stratified cross-validation fold assignment';
