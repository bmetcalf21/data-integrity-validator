# Data Integrity Validator

CLI tool to validate, clean, and dedupe real estate property and event CSVs. Pandas-only dependency.

## Quick Start

```bash
# Setup (one-time)
python3 -m venv venv
./venv/bin/pip install pandas

# Generate sample data
./venv/bin/python scripts/generate_synth_data.py

# Run validator (uses sample_data/*.csv by default)
./venv/bin/python validator.py

# Run with custom files
./venv/bin/python validator.py path/to/properties.csv path/to/events.csv
```

## Outputs

All outputs written to `outputs/` directory:
- `cleaned_properties.csv` - Validated and deduplicated properties
- `cleaned_events.csv` - Validated and deduplicated events
- `rejected_rows.csv` - Invalid rows with `violation_reason` column

## Validation Rules

### Properties (properties.csv)
**Required columns:** apn, county, status, estimated_value, address, last_updated

- **APN format:** Must match `XXX-XXX-XX` (e.g., `123-456-78`)
- **status:** Must be Active, Pre-foreclosure, or Sold (case-insensitive)
- **estimated_value:** Must be numeric and greater than 0
- **last_updated:** Must be valid datetime
- **Deduplication:** Keep newest `last_updated` per apn

### Events (events.csv)
**Required columns:** apn, event_type, event_date, source, updated_at, notes

- **APN format:** Must match `XXX-XXX-XX`
- **APN foreign key:** Must exist in properties
- **event_type:** Must be Scheduled, Postponed, Cancelled, or Sold (case-insensitive)
- **source:** Must be attorney_update, trustee_site, or aggregator (case-insensitive)
- **event_date:** Must be valid datetime
- **updated_at:** Must be valid datetime
- **Deduplication:** Keep newest `updated_at` per (apn, event_type, event_date, source)

### Data Normalization
- Column names normalized to lowercase with whitespace stripped
- String fields trimmed of leading/trailing whitespace
- Allowed values normalized to canonical forms (case-insensitive matching)
- Invalid dates coerced to NaT and flagged as violations

## Statistics Reported

- Input/cleaned/rejected row counts per table
- Duplicate removal counts
- Pass rates
- Average lag hours by source (updated_at - event_date)
- Top postponements per APN

## Safety Features

- Writes rejected rows instead of failing the run on invalid data
- Detailed violation reasons for each rejected row
- Multiple violations per row combined with semicolons
- Foreign key checks prevent orphaned events

## Testing / Verification

To verify the complete workflow:

```bash
# Clean previous outputs
rm outputs/*.csv

# Regenerate sample data
./venv/bin/python scripts/generate_synth_data.py

# Run validator
./venv/bin/python validator.py

# Verify outputs created
ls -lh outputs/
wc -l outputs/*.csv
```

**Expected results:**
- ~182 cleaned properties (82.7% pass rate)
- ~512 cleaned events (77.6% pass rate)
- ~115 rejected rows with violation_reason
- Average lag: attorney_update ~5h, trustee_site ~13h, aggregator ~28h

## Deploying Vercel Signpost

This repo includes a simple landing page in `vercel_signpost/`:

```bash
# Install Vercel CLI
npm install -g vercel

# Deploy signpost
cd vercel_signpost
vercel --prod

# Or via Vercel dashboard:
# 1. Import this Git repository
# 2. Set Root Directory to: vercel_signpost
# 3. Deploy
```

The signpost provides project overview and links back to this repository.

## Project Structure

```
data-integrity-validator/
├── validator.py                  # Main validation script
├── requirements.txt              # pandas dependency
├── scripts/
│   └── generate_synth_data.py   # Synthetic data generator
├── sample_data/
│   ├── properties.csv           # Sample properties (200 rows)
│   └── events.csv               # Sample events (600 rows)
├── outputs/
│   ├── cleaned_properties.csv   # Generated output
│   ├── cleaned_events.csv       # Generated output
│   └── rejected_rows.csv        # Generated output
├── vercel_signpost/
│   └── index.html               # Landing page for Vercel
├── README.md                     # This file
└── DEV_LOG.md                    # Development log template
```

## Requirements

- Python 3.7+
- pandas (only dependency)

## License

MIT
