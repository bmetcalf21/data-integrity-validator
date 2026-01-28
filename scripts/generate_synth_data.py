#!/usr/bin/env python3
"""
Generate synthetic properties and events data with intentional dirty rows.
Outputs to sample_data/ directory.
"""

import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

# Configuration
NUM_PROPERTIES = 200
NUM_EVENTS = 600
DIRTY_RATIO = 0.15  # ~15% dirty rows

# Allowed values
STATUSES = ["Active", "Pre-foreclosure", "Sold"]
EVENT_TYPES = ["Scheduled", "Postponed", "Cancelled", "Sold"]
SOURCES = ["attorney_update", "trustee_site", "aggregator"]

# Source lag averages (hours)
LAG_BY_SOURCE = {
    "attorney_update": 2,
    "trustee_site": 12,
    "aggregator": 24
}

COUNTIES = ["Los Angeles", "Orange", "San Diego", "Riverside", "San Bernardino"]
STREETS = ["Main St", "Oak Ave", "Elm Dr", "Maple Ct", "Pine Rd", "Cedar Ln", "Birch Way"]


def generate_apn():
    """Generate valid APN in format XXX-XXX-XX"""
    return f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(10, 99)}"


def generate_malformed_apn():
    """Generate invalid APN formats"""
    patterns = [
        f"{random.randint(10, 99)}-{random.randint(100, 999)}-{random.randint(10, 99)}",  # too short
        f"{random.randint(100, 999)}{random.randint(100, 999)}{random.randint(10, 99)}",  # no dashes
        f"{random.randint(100, 999)}-{random.randint(10, 99)}",  # incomplete
        "INVALID",
        ""
    ]
    return random.choice(patterns)


def generate_properties():
    """Generate properties dataset with dirty rows"""
    properties = []
    valid_apns = []

    # Generate mostly clean properties
    for i in range(NUM_PROPERTIES):
        is_dirty = random.random() < DIRTY_RATIO

        if is_dirty and random.random() < 0.3:
            # Invalid APN
            apn = generate_malformed_apn()
        else:
            apn = generate_apn()
            valid_apns.append(apn)

        # County
        county = random.choice(COUNTIES)

        # Status - sometimes invalid
        if is_dirty and random.random() < 0.2:
            status = random.choice(["ACTIVE", "active  ", "Unknown", "Pending", ""])
        else:
            status = random.choice(STATUSES)

        # Estimated value - sometimes negative or missing
        if is_dirty and random.random() < 0.3:
            estimated_value = random.choice([-50000, 0, -1, None, ""])
        else:
            estimated_value = random.randint(200000, 2000000)

        # Address
        address = f"{random.randint(100, 9999)} {random.choice(STREETS)}, {county}"

        # Last updated
        last_updated = datetime.now() - timedelta(days=random.randint(0, 365))

        properties.append({
            "apn": apn,
            "county": county,
            "status": status,
            "estimated_value": estimated_value,
            "address": address,
            "last_updated": last_updated.strftime("%Y-%m-%d %H:%M:%S")
        })

    # Inject duplicates (same apn, different last_updated)
    num_dupes = int(NUM_PROPERTIES * 0.1)
    for _ in range(num_dupes):
        if valid_apns:
            apn = random.choice(valid_apns)
            county = random.choice(COUNTIES)
            status = random.choice(STATUSES)
            estimated_value = random.randint(200000, 2000000)
            address = f"{random.randint(100, 9999)} {random.choice(STREETS)}, {county}"
            last_updated = datetime.now() - timedelta(days=random.randint(0, 365))

            properties.append({
                "apn": apn,
                "county": county,
                "status": status,
                "estimated_value": estimated_value,
                "address": address,
                "last_updated": last_updated.strftime("%Y-%m-%d %H:%M:%S")
            })

    return pd.DataFrame(properties), valid_apns


def generate_events(valid_apns):
    """Generate events dataset with dirty rows and FK violations"""
    events = []

    # Generate events
    for i in range(NUM_EVENTS):
        is_dirty = random.random() < DIRTY_RATIO

        # APN - sometimes invalid or orphaned
        if is_dirty and random.random() < 0.2 and valid_apns:
            # Orphaned APN (not in properties)
            apn = generate_apn()
        elif is_dirty and random.random() < 0.1:
            # Malformed APN
            apn = generate_malformed_apn()
        elif valid_apns:
            apn = random.choice(valid_apns)
        else:
            apn = generate_apn()

        # Event type - sometimes invalid
        if is_dirty and random.random() < 0.2:
            event_type = random.choice(["POSTPONED", "postponed  ", "Unknown", "Rescheduled", ""])
        else:
            event_type = random.choice(EVENT_TYPES)

        # Source - sometimes invalid
        if is_dirty and random.random() < 0.2:
            source = random.choice(["ATTORNEY_UPDATE", "unknown_source", "manual", ""])
        else:
            source = random.choice(SOURCES)

        # Event date - sometimes missing or invalid
        if is_dirty and random.random() < 0.15:
            event_date = random.choice([None, "", "invalid-date", "2024-13-45"])
        else:
            event_date = datetime.now() - timedelta(days=random.randint(0, 180))
            event_date = event_date.strftime("%Y-%m-%d")

        # Updated at - with realistic lag
        if is_dirty and random.random() < 0.15:
            updated_at = random.choice([None, "", "invalid"])
        else:
            if event_date and event_date not in [None, "", "invalid-date", "2024-13-45"]:
                event_dt = datetime.strptime(event_date, "%Y-%m-%d")
                lag_hours = LAG_BY_SOURCE.get(source if source in SOURCES else "aggregator", 24)
                lag_variance = lag_hours * 0.5
                actual_lag = max(0.1, random.gauss(lag_hours, lag_variance))
                updated_dt = event_dt + timedelta(hours=actual_lag)
                updated_at = updated_dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Notes
        notes = random.choice([
            "Regular update",
            "Court filing received",
            "Trustee notification",
            "Status change confirmed",
            ""
        ])

        events.append({
            "apn": apn,
            "event_type": event_type,
            "event_date": event_date,
            "source": source,
            "updated_at": updated_at,
            "notes": notes
        })

    # Inject duplicate events (same apn, event_type, event_date, source but different updated_at)
    num_dupes = int(NUM_EVENTS * 0.1)
    for _ in range(num_dupes):
        if events:
            base_event = random.choice(events)
            dupe = base_event.copy()
            # Change updated_at to create duplicate
            if dupe["updated_at"] and dupe["updated_at"] not in [None, "", "invalid"]:
                try:
                    updated_dt = datetime.strptime(dupe["updated_at"], "%Y-%m-%d %H:%M:%S")
                    updated_dt += timedelta(hours=random.randint(1, 48))
                    dupe["updated_at"] = updated_dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass
            events.append(dupe)

    return pd.DataFrame(events)


def main():
    print("Generating synthetic data...")

    # Generate properties
    properties_df, valid_apns = generate_properties()
    print(f"Generated {len(properties_df)} properties ({len(valid_apns)} unique valid APNs)")

    # Generate events
    events_df = generate_events(valid_apns)
    print(f"Generated {len(events_df)} events")

    # Save to CSV
    properties_df.to_csv("sample_data/properties.csv", index=False)
    events_df.to_csv("sample_data/events.csv", index=False)

    print("\nSample data written to sample_data/properties.csv and sample_data/events.csv")
    print(f"Dirty data ratio: ~{DIRTY_RATIO*100}%")
    print("\nInjected issues:")
    print("  - Invalid APN formats")
    print("  - Duplicate properties (same APN, different last_updated)")
    print("  - Duplicate events (same key, different updated_at)")
    print("  - Missing/invalid dates")
    print("  - Negative/missing estimated_value")
    print("  - Unknown status/event_type/source values")
    print("  - Whitespace and casing issues")
    print("  - Orphaned events (APN not in properties)")


if __name__ == "__main__":
    main()
