-- -----------------------------------------------------------------------------
-- mimiciv_echo_create_load_index.sql
-- Create + Load + Index for MIMIC-IV-ECHO summary CSVs
-- Assumes Postgres 10+ and that you will run these commands in the target DB.
-- Place the CSV files (or CSV.GZ) in a directory accessible to the DB client
-- or the DB server (see usage notes below).
-- -----------------------------------------------------------------------------

-- 1) SCHEMA (optional) --------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS mimiciv_echo;
SET search_path = mimiciv_echo, public;

-- 2) TABLE DEFINITIONS -------------------------------------------------------
-- echo_record_list: 1 row per DICOM file (view). Fields inferred from PhysioNet
-- - dicom_filepath: path relative to dataset root (string)
-- - acquisition_datetime: datetime when view acquisition started
-- - study_id, subject_id: numeric identifiers (bigint). Use bigint to be safe.
CREATE TABLE IF NOT EXISTS echo_record_list (
  dicom_filepath    TEXT NOT NULL,           -- e.g. files/p10/p10690270/s95240362/95240362_0004.dcm
  acquisition_datetime TIMESTAMPTZ,          -- acquisition datetime (deidentified)
  study_id          BIGINT NOT NULL,
  subject_id        BIGINT NOT NULL,
  -- optional derived columns if present in CSVs (may be absent; kept nullable)
  dicom_filename    TEXT,
  view_number       INTEGER,
  PRIMARY KEY (dicom_filepath)
);

-- echo_study_list: 1 row per study linking to notes when available
CREATE TABLE IF NOT EXISTS echo_study_list (
  study_id          BIGINT NOT NULL,
  subject_id        BIGINT NOT NULL,
  study_datetime    TIMESTAMPTZ,            -- when the study occurred
  note_id           BIGINT,                 -- note id in MIMIC-IV Note module, if available
  note_seq          INTEGER,                -- sequence number for the note (if provided)
  note_charttime    TIMESTAMPTZ,            -- chart time of the note (if provided)
  PRIMARY KEY (study_id)
);

-- 3) LOAD SCRIPTS ------------------------------------------------------------
-- Replace '/path/to/csv/' below with your actual path or run via psql using \copy.
-- If your files are gzipped *.csv.gz and Postgres server cannot read them directly,
-- use psql's \copy with a client-side gzip command (instructions below).

-- NOTE: We DO NOT run COPY here so the file path is visible; users should run the
-- appropriate COPY/\copy commands in their environment.

-- 4) INDEXES & TUNING --------------------------------------------------------
-- Create supporting indexes (useful for joins to MIMIC-IV clinical tables)
CREATE INDEX IF NOT EXISTS idx_echo_record_subject_id ON echo_record_list (subject_id);
CREATE INDEX IF NOT EXISTS idx_echo_record_study_id    ON echo_record_list (study_id);
CREATE INDEX IF NOT EXISTS idx_echo_record_acq_dt      ON echo_record_list (acquisition_datetime);

CREATE INDEX IF NOT EXISTS idx_echo_study_subject_id   ON echo_study_list (subject_id);
CREATE INDEX IF NOT EXISTS idx_echo_study_study_dt     ON echo_study_list (study_datetime);
CREATE INDEX IF NOT EXISTS idx_echo_study_note_id      ON echo_study_list (note_id);

-- 5) SAMPLE CHECK QUERIES ---------------------------------------------------
-- (optional) quick checks after loading
-- SELECT count(*) FROM echo_record_list;
-- SELECT count(*) FROM echo_study_list;
-- SELECT subject_id, min(acquisition_datetime), max(acquisition_datetime)
--   FROM echo_record_list GROUP BY subject_id ORDER BY subject_id LIMIT 10;

-- End of SQL file
