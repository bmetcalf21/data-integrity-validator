# Data Integrity Validator

CLI tool to validate, clean, and dedupe real estate property and event CSVs. Pandas-only dependency.

**Context:** Built to demonstrate data validation and cleaning workflows for real estate foreclosure tracking data. Handles common data quality issues: malformed IDs, missing values, duplicates, referential integrity, and inconsistent formatting.

**Use case example:** A foreclosure data aggregator receives daily feeds from attorneys, trustee websites, and third-party vendors. This tool validates incoming data, rejects malformed records with clear reasons for manual review, and outputs clean, deduplicated datasets for downstream analysis.

## Live Demo

- Vercel signpost: https://data-integrity-validator.vercel.app/

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

## Technical Highlights

- **Comprehensive validation**: Regex patterns, type checking, FK constraints, duplicate detection
- **Graceful error handling**: Bad data goes to rejected_rows.csv with clear reasons instead of crashing
- **Realistic test data**: Synthetic generator injects ~15% dirty rows mimicking real-world issues
- **Statistical analysis**: Calculates data lag by source and identifies high-postponement properties
- **Simple deployment**: Single dependency, no database, runs anywhere Python does

## Development Testing

To verify the end-to-end workflow:

```bash
# Clean previous outputs
rm -f outputs/*.csv

# Generate synthetic data with intentional quality issues
./venv/bin/python scripts/generate_synth_data.py

# Run validator
./venv/bin/python validator.py

# Check outputs
ls -lh outputs/
head -5 outputs/rejected_rows.csv
```

**Expected behavior:**
- Creates `cleaned_properties.csv`, `cleaned_events.csv`, and `rejected_rows.csv` in `outputs/`
- `rejected_rows.csv` includes `violation_reason` column explaining why each row was rejected
- Most rows pass validation; some are rejected due to intentional data quality issues
- Console prints validation summary with pass rates and statistics

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
│   ├── properties.csv           # Sample properties (220 rows incl. duplicates)
│   └── events.csv               # Sample events (660 rows incl. duplicates)
├── outputs/
│   └── .gitkeep                 # Preserves directory (generated CSVs are .gitignored)
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
