"""
scripts/aadhaar_hash.py
═══════════════════════════════════════════════════════════════════════════════
JanSetu AI — Aadhaar Hash Seeder
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
  Reads citizen Aadhaar numbers from voter_list.csv (or a dedicated
  aadhaar_list.csv), computes SHA-256(aadhaar + pepper) for each, and
  pushes aadhaar_hash onto the matching Citizen node in Neo4j.

  Raw Aadhaar numbers are NEVER written to Neo4j — only the one-way hash.

USAGE:
  # From repo root:
  python scripts/aadhaar_hash.py                        # uses voter_list.csv
  python scripts/aadhaar_hash.py --csv aadhaar_list.csv # custom CSV
  python scripts/aadhaar_hash.py --dry-run              # preview only, no DB writes
  python scripts/aadhaar_hash.py --verify               # check how many nodes have hash

EXPECTED CSV FORMAT (must have these columns, order doesn't matter):
  phone, aadhaar

  Example voter_list.csv:
    name,phone,aadhaar,district,booth_id
    Ravi Kumar,9876543210,123456789012,Raichur,B001
    Suma Devi,9123456780,234567890123,Raichur,B001

ENVIRONMENT (.env):
  NEO4J_URI       = neo4j+s://xxxx.databases.neo4j.io
  NEO4J_USER      = neo4j
  NEO4J_PASSWORD  = <your password>
  AADHAAR_PEPPER  = jansetu-karnataka-2025   ← must match dashboard.py
"""

import os
import re
import sys
import csv
import time
import hashlib
import argparse
import logging
from pathlib import Path

# ── Load .env from repo root (two levels up from scripts/) ────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    print("⚠  python-dotenv not installed. Reading env vars directly.")

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("aadhaar_hash")

# ── Config ────────────────────────────────────────────────────────────────────
# Updated for Local Memgraph (DPDP Compliant)
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "") # Memgraph default is empty
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "") # Memgraph default is empty
PEPPER         = os.getenv("AADHAAR_PEPPER", "jansetu-karnataka-2025")

DEFAULT_CSV    = Path(__file__).parent.parent / "voter_list.csv"


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def make_hash(aadhaar: str) -> str:
    """Return SHA-256(aadhaar + pepper) as a hex string."""
    return hashlib.sha256(f"{aadhaar}{PEPPER}".encode("utf-8")).hexdigest()


def validate_aadhaar(value: str) -> str | None:
    """
    Returns cleaned 12-digit string if valid, else None.
    Accepts formats: 1234 5678 9012 or 123456789012
    """
    cleaned = value.strip().replace(" ", "").replace("-", "")
    return cleaned if re.fullmatch(r"\d{12}", cleaned) else None


def validate_phone(value: str) -> str | None:
    """Returns cleaned 10-digit string if valid, else None."""
    cleaned = value.strip().replace(" ", "")
    return cleaned if re.fullmatch(r"\d{10}", cleaned) else None


def read_csv(csv_path: Path) -> list[dict]:
    """
    Reads the CSV and returns a list of dicts with keys: phone, aadhaar.
    Skips rows with missing or malformed values and logs warnings.
    """
    if not csv_path.exists():
        log.error("CSV not found: %s", csv_path)
        sys.exit(1)

    records = []
    skipped = 0

    # Try UTF-8-sig first (Excel export), fall back to UTF-8
    for encoding in ("utf-8-sig", "utf-8", "utf-16"):
        try:
            with open(csv_path, newline="", encoding=encoding) as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []

                # Normalise headers (strip whitespace, lowercase)
                headers_clean = [h.strip().lower() for h in headers]

                if "phone" not in headers_clean:
                    log.error("CSV missing required column: 'phone'. Found: %s", headers)
                    sys.exit(1)
                if "aadhaar" not in headers_clean:
                    log.error("CSV missing required column: 'aadhaar'. Found: %s", headers)
                    sys.exit(1)

                for i, row in enumerate(reader, start=2):
                    # Normalise row keys
                    row = {k.strip().lower(): v for k, v in row.items()}

                    phone   = validate_phone(row.get("phone", ""))
                    aadhaar = validate_aadhaar(row.get("aadhaar", ""))

                    if not phone:
                        log.warning("Row %d — invalid phone '%s', skipping.", i, row.get("phone"))
                        skipped += 1
                        continue
                    if not aadhaar:
                        log.warning("Row %d — invalid Aadhaar '%s' for phone %s, skipping.",
                                    i, row.get("aadhaar"), phone)
                        skipped += 1
                        continue

                    records.append({"phone": phone, "aadhaar": aadhaar})
            break   # successful read, exit encoding loop
        except (UnicodeDecodeError, UnicodeError):
            continue

    log.info("CSV loaded: %d valid rows, %d skipped  (%s)", len(records), skipped, csv_path.name)
    return records


# ══════════════════════════════════════════════════════════════════════════════
# Neo4j operations
# ══════════════════════════════════════════════════════════════════════════════

def get_driver():
    # Remove the mandatory password check since we are on Local Memgraph
    try:
        # We pass the USER and PASSWORD even if they are empty
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # This is the real test: checking if the database is actually "listening"
        driver.verify_connectivity()
        
        log.info("✅ Connected to Local Memgraph: %s", NEO4J_URI)
        return driver
    except (Neo4jError, ServiceUnavailable) as e:
        log.error("❌ Cannot connect to Memgraph. Is Docker running? Error: %s", e)
        # Instead of crashing, let's give a helpful hint for the demo
        print("\n💡 HINT: Check if your Docker container 'jansetu-db' is started.")
        sys.exit(1)


def seed_hashes(driver, records: list[dict], dry_run: bool = False) -> dict:
    """
    For each record, compute hash and SET c.aadhaar_hash on the matching
    Citizen node. Returns stats dict.
    """
    stats = {"updated": 0, "not_found": 0, "errors": 0, "dry_run": dry_run}

    cypher = """
        MATCH (c:Citizen {phone: $phone})
        SET c.aadhaar_hash = $aadhaar_hash
        RETURN c.name AS name
    """

    with driver.session() as session:
        for rec in records:
            phone        = rec["phone"]
            aadhaar_hash = make_hash(rec["aadhaar"])

            if dry_run:
                # Just preview — no DB write
                log.info("[DRY RUN] phone=%s  hash=%s…", phone, aadhaar_hash[:16])
                stats["updated"] += 1
                continue

            try:
                result = session.run(cypher, phone=phone, aadhaar_hash=aadhaar_hash)
                row    = result.single()
                if row:
                    log.info("✓  %s  (phone=%s)", row["name"], phone)
                    stats["updated"] += 1
                else:
                    log.warning("✗  No Citizen node found for phone=%s", phone)
                    stats["not_found"] += 1
            except (Neo4jError, ServiceUnavailable) as e:
                log.error("Error updating phone=%s: %s", phone, e)
                stats["errors"] += 1
                time.sleep(1)   # brief pause before next attempt

    return stats


def verify_coverage(driver) -> None:
    """Prints how many Citizen nodes have aadhaar_hash set vs total."""
    query = """
        MATCH (c:Citizen)
        RETURN
            count(c) AS total,
            count(c.aadhaar_hash) AS with_hash,
            count(c) - count(c.aadhaar_hash) AS without_hash
    """
    with driver.session() as session:
        row = session.run(query).single()

    if not row:
        log.info("No Citizen nodes found in database.")
        return

    total        = row["total"]
    with_hash    = row["with_hash"]
    without_hash = row["without_hash"]
    coverage_pct = round(100 * with_hash / total, 1) if total else 0

    print("\n" + "═" * 50)
    print("  Aadhaar Hash Coverage Report")
    print("═" * 50)
    print(f"  Total Citizens   : {total}")
    print(f"  With aadhaar_hash: {with_hash}  ({coverage_pct}%)")
    print(f"  Missing hash     : {without_hash}")
    print("═" * 50)

    if without_hash > 0:
        # Show a sample of citizens missing hashes to help the user fix them
        sample_query = """
            MATCH (c:Citizen)
            WHERE c.aadhaar_hash IS NULL
            RETURN c.name AS name, c.phone AS phone
            LIMIT 10
        """
        with driver.session() as session:
            missing = [dict(r) for r in session.run(sample_query)]
        print("\n  Citizens missing hash (first 10):")
        for m in missing:
            print(f"    • {m['name']}  —  phone: {m['phone']}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# CLI entry point
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Seed SHA-256 Aadhaar hashes into Neo4j Citizen nodes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/aadhaar_hash.py                          # seed from voter_list.csv
  python scripts/aadhaar_hash.py --csv aadhaar_list.csv  # custom CSV
  python scripts/aadhaar_hash.py --dry-run                # preview only
  python scripts/aadhaar_hash.py --verify                 # check coverage
        """
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help="Path to CSV file with 'phone' and 'aadhaar' columns (default: voter_list.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview hashes without writing to Neo4j",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Check how many Citizen nodes already have aadhaar_hash set",
    )
    args = parser.parse_args()

    print("\n╔══════════════════════════════════════╗")
    print("║   JanSetu AI — Aadhaar Hash Seeder   ║")
    print("╚══════════════════════════════════════╝\n")
    print(f"  Pepper   : {'*' * len(PEPPER)}  (loaded from .env)")
    print(f"  Neo4j    : {NEO4J_URI}")
    print(f"  CSV      : {args.csv}")
    print(f"  Dry run  : {args.dry_run}")
    print()

    driver = get_driver()

    if args.verify:
        verify_coverage(driver)
        driver.close()
        return

    records = read_csv(args.csv)
    if not records:
        log.error("No valid records to process. Exiting.")
        driver.close()
        sys.exit(1)

    # Confirm before writing if not dry-run
    if not args.dry_run:
        print(f"  About to update {len(records)} Citizen nodes in Neo4j.")
        confirm = input("  Proceed? (yes/no): ").strip().lower()
        if confirm not in ("yes", "y"):
            print("  Aborted.")
            driver.close()
            sys.exit(0)

    stats = seed_hashes(driver, records, dry_run=args.dry_run)
    driver.close()

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "═" * 50)
    print("  Seeding Complete" + ("  [DRY RUN]" if stats["dry_run"] else ""))
    print("═" * 50)
    print(f"  ✓ Updated  : {stats['updated']}")
    if not stats["dry_run"]:
        print(f"  ✗ Not found: {stats['not_found']}  (phone not in Neo4j)")
        print(f"  ⚠ Errors   : {stats['errors']}")
    print("═" * 50 + "\n")

    if not stats["dry_run"] and stats["not_found"] > 0:
        print("  Tip: Citizens 'not found' means their phone number is in the CSV")
        print("  but no matching Citizen node exists in Neo4j yet.")
        print("  Run your main seed script first, then re-run this script.\n")


if __name__ == "__main__":
    main()