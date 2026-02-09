# Development Log

Track AI-assisted development sessions, prompts, and manual reviews.

## Log Template

```
### YYYY-MM-DD - [Task Name]
**Prompt Summary:** [Brief description of what was requested]
**AI Actions:** [What the AI did - files created/modified, commands run]
**Manual Review:** [Your observations, tests, changes made by hand]
**Status:** [✓ Complete | ⚠ Needs work | ⏸ Paused]
```

---

## Entries

### 2026-01-28 - Initial Scaffold & Data Generator
**Prompt Summary:** Create project structure, requirements.txt, sample_data generator with dirty rows, and generate initial CSV files with 200 properties and 600 events.

**AI Actions:**
- Created `requirements.txt` (pandas only)
- Created `outputs/.gitkeep` placeholder
- Created `scripts/generate_synth_data.py` with synthetic data generator
- Generated `sample_data/properties.csv` (220 rows including duplicates)
- Generated `sample_data/events.csv` (660 rows including duplicates)
- Injected ~15% dirty rows: invalid APNs, bad dates, negative values, unknown enum values, FK violations

**Manual Review:**
- Verified CSV files generated successfully
- Spot-checked dirty rows present in raw data
- Confirmed APN format variations and duplicate keys exist
- Lag simulation appears realistic per source type

**Status:** ✓ Complete

---

### 2026-01-28 - Core Validator Implementation
**Prompt Summary:** Implement validator.py end-to-end with all validation rules, deduplication, rejection tracking, and statistics reporting.

**AI Actions:**
- Created `validator.py` with full validation pipeline
- Implemented column normalization and string trimming
- Added APN regex validation (XXX-XXX-XX format)
- Added foreign key check (events.apn → properties.apn)
- Implemented deduplication logic (newest timestamp wins)
- Added lag calculation by source
- Added postponement counting per APN
- Generated all three output files with proper violation_reason column

**Manual Review:**
- Ran validator on sample data: 82.7% properties pass, 77.6% events pass
- Verified rejected_rows.csv contains violation_reason with semicolon-separated multiple violations
- Confirmed no crashes on malformed data
- Spot-checked lag hours realistic: attorney_update ~5h, trustee_site ~13h, aggregator ~28h
- Tested with custom file paths via command-line args

**Status:** ✓ Complete

---

### 2026-01-28 - Documentation & Deployment Setup
**Prompt Summary:** Create README.md with exact commands and rules, DEV_LOG.md template with examples, and vercel_signpost/index.html landing page.

**AI Actions:**
- Created `README.md` with quick start, validation rules, statistics, and Vercel deployment instructions
- Created `DEV_LOG.md` with template and 3 example log entries
- Created `vercel_signpost/index.html` landing page with project overview

**Manual Review:**
- Verified README setup and run commands execute end-to-end
- Confirmed signpost renders correctly on desktop and mobile widths
- Spot-checked GitHub links and feature bullets for accuracy

**Status:** ✓ Complete

---

## Notes

- Keep entries concise but specific enough to understand decisions later
- Include command outputs or error messages if relevant
- Note any deviations from original requirements
- Track technical debt or future improvements here
