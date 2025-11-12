#!/usr/bin/env python3
"""
Extract metadata from MIMIC-IV Waveform Database .hea files and create CSV files.

This script parses WFDB header files and numerics CSV files to create
CSV files that can be loaded into PostgreSQL using load.sql.

Usage:
    python extract_metadata.py --data-dir /path/to/mimic4wdb/0.1.0/waves --output-dir /path/to/output

Requirements:
    pip install wfdb pandas tqdm
"""

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import csv
import gzip
import re

import wfdb
import pandas as pd
from tqdm import tqdm


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Extract metadata from MIMIC-IV Waveform Database for PostgreSQL loading'
    )
    parser.add_argument(
        '--data-dir',
        required=True,
        help='Path to the MIMIC4WDB data directory (e.g., /data/mimic4wdb/0.1.0/waves)'
    )
    parser.add_argument(
        '--output-dir',
        required=True,
        help='Output directory for CSV files'
    )
    parser.add_argument(
        '--skip-numerics',
        action='store_true',
        help='Skip processing numerics files (can be very large)'
    )

    return parser.parse_args()


def find_all_records(data_dir):
    """Find all waveform records in the data directory."""
    data_path = Path(data_dir)
    records = []

    print(f"Scanning directory: {data_dir}")

    for p_dir in sorted(data_path.glob('p*')):
        if not p_dir.is_dir():
            continue

        for patient_dir in sorted(p_dir.glob('p*')):
            if not patient_dir.is_dir():
                continue

            subject_id = patient_dir.name[1:]  # Remove 'p' prefix

            for record_dir in sorted(patient_dir.iterdir()):
                if not record_dir.is_dir():
                    continue

                record_name = record_dir.name
                header_file = record_dir / f"{record_name}.hea"

                if header_file.exists():
                    rel_path = record_dir.relative_to(data_path)
                    records.append((str(rel_path), record_name, int(subject_id)))

    print(f"Found {len(records)} records")
    return records


def categorize_signal_type(signal_name):
    """Categorize signal by name."""
    signal_name_upper = signal_name.upper()

    ecg_leads = ['I', 'II', 'III', 'AVR', 'AVL', 'AVF', 'V', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6',
                 'MCL', 'AI', 'AS', 'ES']
    pressure_types = ['ABP', 'ART', 'AO', 'BAP', 'CVP', 'FAP', 'ICP', 'IC1', 'IC2',
                      'LAP', 'PAP', 'RAP', 'UAP', 'UVP', 'P', 'P1', 'P2', 'P4']

    if any(signal_name_upper == lead or signal_name_upper.startswith(lead) for lead in ecg_leads):
        return 'ECG'
    elif any(pressure in signal_name_upper for pressure in pressure_types):
        return 'Pressure'
    elif 'PLETH' in signal_name_upper:
        return 'Plethysmogram'
    elif 'RESP' in signal_name_upper:
        return 'Respiration'
    elif 'CO2' in signal_name_upper:
        return 'Capnography'
    else:
        return 'Other'


def extract_metadata(data_dir, records, output_dir, skip_numerics=False):
    """Extract metadata from all records and write to CSV files."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Open CSV files for writing
    records_csv = open(output_path / 'waveform_records.csv', 'w', newline='')
    segments_csv = open(output_path / 'waveform_segments.csv', 'w', newline='')
    signals_csv = open(output_path / 'waveform_signals.csv', 'w', newline='')

    records_writer = csv.writer(records_csv)
    segments_writer = csv.writer(segments_csv)
    signals_writer = csv.writer(signals_csv)

    # Write headers
    records_writer.writerow([
        'record_id', 'subject_id', 'hadm_id', 'start_datetime', 'end_datetime',
        'record_duration_sec', 'file_path', 'header_file', 'base_counter_freq', 'num_segments'
    ])

    segments_writer.writerow([
        'record_id', 'segment_name', 'segment_num', 'segment_start_time',
        'segment_duration_sec', 'segment_header_file', 'segment_data_file',
        'sampling_frequency', 'num_signals'
    ])

    signals_writer.writerow([
        'record_id', 'segment_num', 'signal_name', 'signal_index', 'signal_units',
        'signal_gain', 'signal_baseline', 'signal_adc_resolution',
        'signal_description', 'signal_type'
    ])

    # Process numerics separately if needed
    if not skip_numerics:
        numerics_csv = open(output_path / 'waveform_numerics.csv', 'w', newline='')
        numerics_writer = csv.writer(numerics_csv)
        numerics_writer.writerow([
            'record_id', 'measurement_time', 'counter_ticks', 'heart_rate', 'resp_rate',
            'spo2', 'nibp_systolic', 'nibp_diastolic', 'nibp_mean', 'abp_systolic',
            'abp_diastolic', 'abp_mean', 'cvp', 'etco2', 'temperature',
            'measurement_name', 'measurement_value', 'measurement_unit'
        ])

    print("\nExtracting metadata from records...")

    for record_path, record_name, subject_id in tqdm(records, desc="Processing"):
        try:
            full_path = Path(data_dir) / record_path
            record_file = full_path / record_name

            # Read multi-segment header
            record = wfdb.rdheader(str(record_file))

            # Extract hadm_id and subject_id from comments
            hadm_id = ''
            subject_id_from_header = None
            if hasattr(record, 'comments'):
                for comment in record.comments:
                    # Try different patterns for hadm_id
                    if 'hadm_id' in comment.lower():
                        match = re.search(r'hadm_id\s+(\d+)', comment, re.IGNORECASE)
                        if match:
                            hadm_id = match.group(1)
                    elif 'hospital admission id' in comment.lower():
                        match = re.search(r'hospital admission id[:\s]+(\d+)', comment, re.IGNORECASE)
                        if match:
                            hadm_id = match.group(1)

                    # Also extract subject_id from header if available
                    if 'subject_id' in comment.lower():
                        match = re.search(r'subject_id\s+(\d+)', comment, re.IGNORECASE)
                        if match:
                            subject_id_from_header = int(match.group(1))

            # Use subject_id from header if available, otherwise use from directory
            if subject_id_from_header:
                subject_id = subject_id_from_header

            # Record metadata
            start_datetime = record.base_datetime if hasattr(record, 'base_datetime') else ''
            duration_sec = record.sig_len if hasattr(record, 'sig_len') else ''
            base_freq = record.fs if hasattr(record, 'fs') else ''
            num_segments = len(record.seg_name) if hasattr(record, 'seg_name') else 0

            # Calculate end datetime
            end_datetime = ''
            if start_datetime and duration_sec and base_freq:
                try:
                    end_dt = start_datetime + timedelta(seconds=duration_sec / base_freq)
                    end_datetime = end_dt.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_dt, datetime) else end_dt
                except:
                    pass

            # Format start_datetime
            if isinstance(start_datetime, datetime):
                start_datetime = start_datetime.strftime('%Y-%m-%d %H:%M:%S')

            records_writer.writerow([
                record_name, subject_id, hadm_id, start_datetime, end_datetime,
                duration_sec, record_path, f"{record_name}.hea", base_freq, num_segments
            ])

            # Process segments
            if hasattr(record, 'seg_name'):
                for seg_num, seg_name in enumerate(record.seg_name):
                    # Skip layout segments
                    if seg_name == '~' or '_layout' in seg_name or seg_name.endswith('_0000'):
                        continue

                    try:
                        seg_file = full_path / seg_name
                        seg_header = wfdb.rdheader(str(seg_file))

                        # Calculate segment start time
                        seg_start_time = ''
                        if start_datetime and hasattr(record, 'fs') and record.fs > 0:
                            if seg_num > 0 and hasattr(record, 'seg_len'):
                                try:
                                    offset_samples = sum(record.seg_len[:seg_num])
                                    offset_seconds = offset_samples / record.fs
                                    if isinstance(record.base_datetime, datetime):
                                        seg_start = record.base_datetime + timedelta(seconds=offset_seconds)
                                        seg_start_time = seg_start.strftime('%Y-%m-%d %H:%M:%S')
                                except:
                                    pass

                        seg_duration = ''
                        if seg_header.sig_len and seg_header.fs and seg_header.fs > 0:
                            seg_duration = seg_header.sig_len / seg_header.fs

                        segments_writer.writerow([
                            record_name, seg_name, seg_num, seg_start_time, seg_duration,
                            f"{seg_name}.hea", f"{seg_name}.dat" if hasattr(seg_header, 'file_name') else '',
                            seg_header.fs, seg_header.n_sig
                        ])

                        # Process signals in segment
                        for sig_idx in range(seg_header.n_sig):
                            sig_name = seg_header.sig_name[sig_idx]
                            sig_units = seg_header.units[sig_idx] if hasattr(seg_header, 'units') else ''
                            sig_gain = seg_header.adc_gain[sig_idx] if hasattr(seg_header, 'adc_gain') else ''
                            sig_baseline = seg_header.baseline[sig_idx] if hasattr(seg_header, 'baseline') else ''
                            sig_adc_res = seg_header.adc_res[sig_idx] if hasattr(seg_header, 'adc_res') else ''
                            sig_type = categorize_signal_type(sig_name)

                            signals_writer.writerow([
                                record_name, seg_num, sig_name, sig_idx, sig_units,
                                sig_gain, sig_baseline, sig_adc_res, '', sig_type
                            ])

                    except Exception as e:
                        print(f"\n  Warning: Could not parse segment {seg_name}: {e}")
                        continue

            # Process numerics if not skipped
            if not skip_numerics:
                numerics_file = full_path / f"{record_name}n.csv.gz"
                if numerics_file.exists():
                    try:
                        df = pd.read_csv(numerics_file, compression='gzip')
                        time_col = df.columns[0]

                        for _, row in df.iterrows():
                            counter_ticks = int(row[time_col])

                            # Calculate timestamp
                            measurement_time = ''
                            if isinstance(record.base_datetime, datetime) and base_freq and base_freq > 0:
                                try:
                                    offset_seconds = counter_ticks / base_freq
                                    meas_time = record.base_datetime + timedelta(seconds=offset_seconds)
                                    measurement_time = meas_time.strftime('%Y-%m-%d %H:%M:%S')
                                except:
                                    pass

                            # Initialize all fields as empty
                            hr = rr = spo2 = ''
                            nibp_sys = nibp_dias = nibp_mean = ''
                            abp_sys = abp_dias = abp_mean = ''
                            cvp = etco2 = temp = ''
                            meas_name = meas_val = meas_unit = ''

                            # Extract measurements
                            for col in df.columns[1:]:
                                value = row[col]
                                if pd.isna(value):
                                    continue

                                col_lower = col.lower()

                                if 'spo2' in col_lower or 'sp02' in col_lower:
                                    spo2 = int(value)
                                elif 'hr' in col_lower and 'heart' in col_lower:
                                    hr = int(value)
                                elif 'rr' in col_lower and 'resp' in col_lower:
                                    rr = int(value)
                                elif 'nibp' in col_lower:
                                    if 'sys' in col_lower:
                                        nibp_sys = int(value)
                                    elif 'dias' in col_lower:
                                        nibp_dias = int(value)
                                    elif 'mean' in col_lower:
                                        nibp_mean = int(value)
                                elif 'abp' in col_lower:
                                    if 'sys' in col_lower:
                                        abp_sys = int(value)
                                    elif 'dias' in col_lower:
                                        abp_dias = int(value)
                                    elif 'mean' in col_lower:
                                        abp_mean = int(value)
                                elif 'cvp' in col_lower:
                                    cvp = float(value)
                                elif 'etco2' in col_lower:
                                    etco2 = float(value)
                                elif 'temp' in col_lower:
                                    temp = float(value)
                                else:
                                    # Store in generic fields (only first unmatched column)
                                    if not meas_name:
                                        meas_name = col
                                        meas_val = float(value)

                            numerics_writer.writerow([
                                record_name, measurement_time, counter_ticks, hr, rr, spo2,
                                nibp_sys, nibp_dias, nibp_mean, abp_sys, abp_dias, abp_mean,
                                cvp, etco2, temp, meas_name, meas_val, meas_unit
                            ])

                    except Exception as e:
                        print(f"\n  Warning: Could not parse numerics for {record_name}: {e}")

        except Exception as e:
            print(f"\n  Error processing {record_name}: {e}")
            continue

    # Close all files
    records_csv.close()
    segments_csv.close()
    signals_csv.close()
    if not skip_numerics:
        numerics_csv.close()

    print(f"\n✓ CSV files created in {output_dir}")
    print(f"  - waveform_records.csv")
    print(f"  - waveform_segments.csv")
    print(f"  - waveform_signals.csv")
    if not skip_numerics:
        print(f"  - waveform_numerics.csv")


def main():
    """Main execution function."""
    args = parse_arguments()

    # Verify data directory exists
    if not os.path.isdir(args.data_dir):
        print(f"Error: Data directory not found: {args.data_dir}")
        sys.exit(1)

    # Find all records
    records = find_all_records(args.data_dir)

    if not records:
        print("Error: No records found in the data directory")
        sys.exit(1)

    # Extract metadata
    extract_metadata(args.data_dir, records, args.output_dir, args.skip_numerics)

    print("\n✓ Metadata extraction complete!")
    print(f"\nNext steps:")
    print(f"1. Review the CSV files in {args.output_dir}")
    print(f"2. Load into PostgreSQL:")
    print(f"   psql -d mimiciv -v mimic_data_dir={args.output_dir} -f load.sql")


if __name__ == '__main__':
    main()
