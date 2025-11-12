#!/usr/bin/env python3
"""
Update only the waveform_records.csv file to fix hadm_id extraction.

This script re-parses only the record header files and updates waveform_records.csv
without touching the other CSV files (segments, signals, numerics).

Usage:
    python update_records.py --data-dir /path/to/mimic4wdb/0.1.0/waves --output-file /path/to/waveform_records.csv

Requirements:
    pip install wfdb pandas tqdm
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
import csv
import re

import wfdb
from tqdm import tqdm


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Update waveform_records.csv with corrected hadm_id extraction'
    )
    parser.add_argument(
        '--data-dir',
        required=True,
        help='Path to the MIMIC4WDB data directory (e.g., /data/mimic4wdb/0.1.0/waves)'
    )
    parser.add_argument(
        '--output-file',
        required=True,
        help='Output file path for waveform_records.csv'
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


def extract_records_metadata(data_dir, records, output_file):
    """Extract metadata from record headers and write to CSV."""

    print("\nExtracting metadata from record headers...")

    with open(output_file, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)

        # Write header
        writer.writerow([
            'record_id', 'subject_id', 'hadm_id', 'start_datetime', 'end_datetime',
            'record_duration_sec', 'file_path', 'header_file', 'base_counter_freq', 'num_segments'
        ])

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

                writer.writerow([
                    record_name, subject_id, hadm_id, start_datetime, end_datetime,
                    duration_sec, record_path, f"{record_name}.hea", base_freq, num_segments
                ])

            except Exception as e:
                print(f"\nError processing {record_name}: {e}")
                continue

    print(f"\n✓ waveform_records.csv created: {output_file}")


def main():
    """Main execution function."""
    args = parse_arguments()

    # Verify data directory exists
    if not Path(args.data_dir).is_dir():
        print(f"Error: Data directory not found: {args.data_dir}")
        sys.exit(1)

    # Find all records
    records = find_all_records(args.data_dir)

    if not records:
        print("Error: No records found in the data directory")
        sys.exit(1)

    # Extract metadata
    extract_records_metadata(args.data_dir, records, args.output_file)

    print("\n✓ Update complete!")
    print(f"\nNext step:")
    print(f"  Load into PostgreSQL (only waveform_records table):")
    print(f"  psql -d mimiciv -c \"SET search_path TO mimiciv_waveforms; TRUNCATE waveform_records CASCADE; \\copy waveform_records FROM '{args.output_file}' DELIMITER ',' CSV HEADER NULL '';\"")


if __name__ == '__main__':
    main()
