#!/usr/bin/env python3
"""
Data Integrity Validator for real estate properties and events.
Validates, cleanses, deduplicates, and reports on data quality.
"""

import pandas as pd
import re
import sys
from pathlib import Path


# Allowed values (canonical forms)
ALLOWED_STATUS = {"active": "Active", "pre-foreclosure": "Pre-foreclosure", "sold": "Sold"}
ALLOWED_EVENT_TYPE = {"scheduled": "Scheduled", "postponed": "Postponed", "cancelled": "Cancelled", "sold": "Sold"}
ALLOWED_SOURCE = {"attorney_update": "attorney_update", "trustee_site": "trustee_site", "aggregator": "aggregator"}

# APN regex pattern
APN_PATTERN = re.compile(r'^\d{3}-\d{3}-\d{2}$')


class ValidationStats:
    """Track validation statistics"""
    def __init__(self):
        self.properties_input = 0
        self.events_input = 0
        self.properties_cleaned = 0
        self.events_cleaned = 0
        self.properties_rejected = 0
        self.events_rejected = 0
        self.properties_duplicates_removed = 0
        self.events_duplicates_removed = 0
        self.lag_by_source = {}
        self.postponements_by_apn = {}


def normalize_column_names(df):
    """Normalize column names: strip whitespace and lowercase"""
    df.columns = df.columns.str.strip().str.lower()
    return df


def trim_string_fields(df):
    """Trim whitespace from all string fields"""
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def normalize_value(value, allowed_dict):
    """Normalize a value against allowed values (case-insensitive)"""
    if pd.isna(value) or value == '':
        return None
    value_lower = str(value).strip().lower()
    return allowed_dict.get(value_lower)


def validate_apn(apn):
    """Validate APN format: XXX-XXX-XX"""
    if pd.isna(apn) or apn == '':
        return False
    return bool(APN_PATTERN.match(str(apn)))


def parse_datetime_safe(value):
    """Parse datetime, return NaT on error"""
    if pd.isna(value) or value == '':
        return pd.NaT
    try:
        return pd.to_datetime(value, errors='coerce')
    except:
        return pd.NaT


def validate_properties(df, stats):
    """Validate properties dataset"""
    stats.properties_input = len(df)

    # Normalize and trim first so required-column checks are case/whitespace tolerant
    df = normalize_column_names(df)
    df = trim_string_fields(df)

    # Required columns
    required_cols = ['apn', 'county', 'status', 'estimated_value', 'address', 'last_updated']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in properties: {missing_cols}")

    # Track rejection reasons
    df['_violation_reason'] = ''
    df['_valid'] = True

    # Validate APN format
    df['_apn_valid'] = df['apn'].apply(validate_apn)
    df.loc[~df['_apn_valid'], '_violation_reason'] += 'Invalid APN format; '
    df.loc[~df['_apn_valid'], '_valid'] = False

    # Parse last_updated
    df['last_updated'] = df['last_updated'].apply(parse_datetime_safe)
    df.loc[df['last_updated'].isna(), '_violation_reason'] += 'Invalid last_updated date; '
    df.loc[df['last_updated'].isna(), '_valid'] = False

    # Normalize and validate status
    df['_status_normalized'] = df['status'].apply(lambda x: normalize_value(x, ALLOWED_STATUS))
    df.loc[df['_status_normalized'].isna(), '_violation_reason'] += 'Invalid status value; '
    df.loc[df['_status_normalized'].isna(), '_valid'] = False
    df['status'] = df['_status_normalized']

    # Validate estimated_value
    df['estimated_value'] = pd.to_numeric(df['estimated_value'], errors='coerce')
    df.loc[df['estimated_value'].isna(), '_violation_reason'] += 'Invalid estimated_value (not numeric); '
    df.loc[df['estimated_value'].isna(), '_valid'] = False
    df.loc[(df['estimated_value'].notna()) & (df['estimated_value'] <= 0), '_violation_reason'] += 'Invalid estimated_value (must be > 0); '
    df.loc[(df['estimated_value'].notna()) & (df['estimated_value'] <= 0), '_valid'] = False

    # Split valid and rejected
    rejected = df[~df['_valid']].copy()
    valid = df[df['_valid']].copy()

    # Clean up temporary columns from valid data
    valid = valid.drop(columns=['_violation_reason', '_valid', '_apn_valid', '_status_normalized'])

    # Deduplicate: keep newest last_updated per apn
    if len(valid) > 0:
        initial_count = len(valid)
        valid = valid.sort_values('last_updated', ascending=False)
        valid = valid.drop_duplicates(subset=['apn'], keep='first')
        stats.properties_duplicates_removed = initial_count - len(valid)

    stats.properties_cleaned = len(valid)
    stats.properties_rejected = len(rejected)

    # Prepare rejected rows for output
    if len(rejected) > 0:
        rejected['_source_table'] = 'properties'
        rejected['_violation_reason'] = rejected['_violation_reason'].str.rstrip('; ')
        rejected = rejected.drop(columns=['_valid', '_apn_valid', '_status_normalized'])

    return valid, rejected


def validate_events(df, valid_apns, stats):
    """Validate events dataset"""
    stats.events_input = len(df)

    # Normalize and trim first so required-column checks are case/whitespace tolerant
    df = normalize_column_names(df)
    df = trim_string_fields(df)

    # Required columns
    required_cols = ['apn', 'event_type', 'event_date', 'source', 'updated_at', 'notes']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in events: {missing_cols}")

    # Track rejection reasons
    df['_violation_reason'] = ''
    df['_valid'] = True

    # Validate APN format
    df['_apn_valid'] = df['apn'].apply(validate_apn)
    df.loc[~df['_apn_valid'], '_violation_reason'] += 'Invalid APN format; '
    df.loc[~df['_apn_valid'], '_valid'] = False

    # FK check: apn must exist in properties
    df['_apn_exists'] = df['apn'].isin(valid_apns)
    df.loc[~df['_apn_exists'], '_violation_reason'] += 'APN not found in properties (FK violation); '
    df.loc[~df['_apn_exists'], '_valid'] = False

    # Parse event_date
    df['event_date'] = df['event_date'].apply(parse_datetime_safe)
    df.loc[df['event_date'].isna(), '_violation_reason'] += 'Invalid event_date; '
    df.loc[df['event_date'].isna(), '_valid'] = False

    # Parse updated_at
    df['updated_at'] = df['updated_at'].apply(parse_datetime_safe)
    df.loc[df['updated_at'].isna(), '_violation_reason'] += 'Invalid updated_at; '
    df.loc[df['updated_at'].isna(), '_valid'] = False

    # Normalize and validate event_type
    df['_event_type_normalized'] = df['event_type'].apply(lambda x: normalize_value(x, ALLOWED_EVENT_TYPE))
    df.loc[df['_event_type_normalized'].isna(), '_violation_reason'] += 'Invalid event_type; '
    df.loc[df['_event_type_normalized'].isna(), '_valid'] = False
    df['event_type'] = df['_event_type_normalized']

    # Normalize and validate source
    df['_source_normalized'] = df['source'].apply(lambda x: normalize_value(x, ALLOWED_SOURCE))
    df.loc[df['_source_normalized'].isna(), '_violation_reason'] += 'Invalid source; '
    df.loc[df['_source_normalized'].isna(), '_valid'] = False
    df['source'] = df['_source_normalized']

    # Split valid and rejected
    rejected = df[~df['_valid']].copy()
    valid = df[df['_valid']].copy()

    # Clean up temporary columns from valid data
    valid = valid.drop(columns=['_violation_reason', '_valid', '_apn_valid', '_apn_exists', '_event_type_normalized', '_source_normalized'])

    # Deduplicate: keep newest updated_at per (apn, event_type, event_date, source)
    if len(valid) > 0:
        initial_count = len(valid)
        valid = valid.sort_values('updated_at', ascending=False)
        valid = valid.drop_duplicates(subset=['apn', 'event_type', 'event_date', 'source'], keep='first')
        stats.events_duplicates_removed = initial_count - len(valid)

    stats.events_cleaned = len(valid)
    stats.events_rejected = len(rejected)

    # Prepare rejected rows for output
    if len(rejected) > 0:
        rejected['_source_table'] = 'events'
        rejected['_violation_reason'] = rejected['_violation_reason'].str.rstrip('; ')
        rejected = rejected.drop(columns=['_valid', '_apn_valid', '_apn_exists', '_event_type_normalized', '_source_normalized'])

    return valid, rejected


def calculate_lag_stats(events_df, stats):
    """Calculate average lag hours by source"""
    if len(events_df) == 0:
        return

    # Calculate lag for each event
    events_df['_lag_hours'] = (events_df['updated_at'] - events_df['event_date']).dt.total_seconds() / 3600

    # Average lag by source
    lag_by_source = events_df.groupby('source')['_lag_hours'].mean().to_dict()
    stats.lag_by_source = {k: round(v, 2) for k, v in lag_by_source.items()}


def calculate_postponements(events_df, stats):
    """Count postponements per APN"""
    if len(events_df) == 0:
        return

    postponements = events_df[events_df['event_type'] == 'Postponed'].groupby('apn').size().to_dict()
    stats.postponements_by_apn = postponements


def print_summary(stats):
    """Print validation summary"""
    print("\n" + "="*60)
    print("DATA VALIDATION SUMMARY")
    print("="*60)

    print("\nPROPERTIES:")
    print(f"  Input rows:              {stats.properties_input}")
    print(f"  Cleaned rows:            {stats.properties_cleaned}")
    print(f"  Rejected rows:           {stats.properties_rejected}")
    print(f"  Duplicates removed:      {stats.properties_duplicates_removed}")
    if stats.properties_input > 0:
        pass_rate = (stats.properties_cleaned / stats.properties_input) * 100
        print(f"  Pass rate:               {pass_rate:.1f}%")

    print("\nEVENTS:")
    print(f"  Input rows:              {stats.events_input}")
    print(f"  Cleaned rows:            {stats.events_cleaned}")
    print(f"  Rejected rows:           {stats.events_rejected}")
    print(f"  Duplicates removed:      {stats.events_duplicates_removed}")
    if stats.events_input > 0:
        pass_rate = (stats.events_cleaned / stats.events_input) * 100
        print(f"  Pass rate:               {pass_rate:.1f}%")

    print("\nAVERAGE LAG HOURS BY SOURCE:")
    if stats.lag_by_source:
        for source, lag in sorted(stats.lag_by_source.items()):
            print(f"  {source:20s} {lag:6.2f} hours")
    else:
        print("  No valid events to calculate lag")

    print("\nPOSTPONEMENTS PER APN (Top 10):")
    if stats.postponements_by_apn:
        sorted_postponements = sorted(stats.postponements_by_apn.items(), key=lambda x: x[1], reverse=True)[:10]
        for apn, count in sorted_postponements:
            print(f"  {apn}    {count} postponement(s)")
    else:
        print("  No postponements found")

    print("\n" + "="*60)
    print("OUTPUT FILES:")
    print("  outputs/cleaned_properties.csv")
    print("  outputs/cleaned_events.csv")
    print("  outputs/rejected_rows.csv")
    print("="*60 + "\n")


def main():
    """Main validation workflow"""
    # Parse command-line arguments
    if len(sys.argv) == 3:
        properties_file = sys.argv[1]
        events_file = sys.argv[2]
    elif len(sys.argv) == 1:
        properties_file = "sample_data/properties.csv"
        events_file = "sample_data/events.csv"
    else:
        print("\nUsage:")
        print("  python validator.py")
        print("  python validator.py <properties.csv> <events.csv>")
        sys.exit(1)

    print(f"\nReading data from:")
    print(f"  Properties: {properties_file}")
    print(f"  Events:     {events_file}")

    # Initialize stats
    stats = ValidationStats()

    try:
        # Load data
        properties_df = pd.read_csv(properties_file)
        events_df = pd.read_csv(events_file)

        # Validate properties
        print("\nValidating properties...")
        clean_properties, rejected_properties = validate_properties(properties_df, stats)

        # Get valid APNs for FK check
        valid_apns = set(clean_properties['apn'].tolist())

        # Validate events
        print("Validating events...")
        clean_events, rejected_events = validate_events(events_df, valid_apns, stats)

        # Calculate statistics
        calculate_lag_stats(clean_events, stats)
        calculate_postponements(clean_events, stats)

        # Ensure output directory exists
        Path("outputs").mkdir(exist_ok=True)

        # Write cleaned data
        clean_properties.to_csv("outputs/cleaned_properties.csv", index=False)
        clean_events.to_csv("outputs/cleaned_events.csv", index=False)

        # Combine rejected rows
        all_rejected = []
        if len(rejected_properties) > 0:
            all_rejected.append(rejected_properties)
        if len(rejected_events) > 0:
            all_rejected.append(rejected_events)

        if all_rejected:
            rejected_df = pd.concat(all_rejected, ignore_index=True)
            # Reorder columns to put violation_reason first
            cols = rejected_df.columns.tolist()
            cols.remove('_violation_reason')
            cols.remove('_source_table')
            rejected_df = rejected_df[['_source_table', '_violation_reason'] + cols]
            rejected_df = rejected_df.rename(columns={'_source_table': 'source_table', '_violation_reason': 'violation_reason'})
            rejected_df.to_csv("outputs/rejected_rows.csv", index=False)
        else:
            # Create empty rejected file
            pd.DataFrame(columns=['source_table', 'violation_reason']).to_csv("outputs/rejected_rows.csv", index=False)

        # Print summary
        print_summary(stats)

        print("Validation complete!")

    except FileNotFoundError as e:
        print(f"\nError: Could not find input file: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"\nError: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
