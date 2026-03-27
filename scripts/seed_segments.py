"""
scripts/seed_segments.py
═══════════════════════════════════════════════════════════════════════════════
JanSetu AI — Citizen Segment Seeder  (Objective 1)
═══════════════════════════════════════════════════════════════════════════════

PURPOSE:
  Reads every Citizen node in Neo4j and assigns a `segment` property
  based on their age, occupation, income_bracket, and disability_status.

  Segment rules (applied in priority order):
    Disabled   → disability_status = 'Yes'
    Senior     → age >= 60
    Youth      → age < 30
    Farmer     → occupation in ['Farmer', 'Agriculture', 'Kisan']
    Women      → gender = 'Female'  (add gender property to your CSV if needed)
    Businessman→ occupation in ['Small Trader','Artisan','Businessman','Shopkeeper',
                                 'Self Employed','Vendor','Contractor']
    General    → everything else

USAGE:
  python scripts/seed_segments.py             # assign segments to all citizens
  python scripts/seed_segments.py --dry-run   # preview only, no DB write
  python scripts/seed_segments.py --verify    # print segment counts
  python scripts/seed_segments.py --reset     # remove all segment properties

ENVIRONMENT (.env):
  NEO4J_URI       = neo4j+s://xxxx.databases.neo4j.io
  NEO4J_USER      = neo4j
  NEO4J_PASSWORD  = <your password>
"""

import os
import sys
import argparse
import logging
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    print("Warning: python-dotenv not installed. Reading env vars directly.")

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_segments")

# ── Config ────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "neo4j+s://9a1f5a0d.databases.neo4j.io")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")

# ── Segment classification rules ──────────────────────────────────────────────
FARMER_OCCUPATIONS = {
    "farmer", "agriculture", "kisan", "agriculturist",
    "cultivator", "farm labour", "dairy farmer",
}
BUSINESSMAN_OCCUPATIONS = {
    "small trader", "artisan", "businessman", "shopkeeper",
    "self employed", "vendor", "contractor", "trader",
    "merchant", "retailer",
}

# Schemes recommended per segment — used in Gemini prompts
SEGMENT_SCHEMES = {
    "Youth":       ["Mudra Loan (PMMY)", "Post-Matric Scholarship",
                    "PM SVANidhi", "Jan Dhan Yojana"],
    "Farmer":      ["PM Kisan Samman Nidhi", "PM Fasal Bima",
                    "Kisan Credit Card", "PM Kusum"],
    "Women":       ["Ujjwala Yojana", "PM Awas Yojana",
                    "Ayushman Bharat", "PM Matru Vandana"],
    "Businessman": ["Mudra Loan (PMMY)", "PM Vishwakarma",
                    "PM SVANidhi", "Jan Dhan Yojana"],
    "Senior":      ["Old Age Pension", "Ayushman Bharat",
                    "PM Vaya Vandana"],
    "Disabled":    ["Divyang Sahara Yojna", "Ayushman Bharat", "ADIP Scheme"],
    "General":     ["Jan Dhan Yojana", "Ayushman Bharat", "PM Awas Yojana"],
}


def classify_segment(age, occupation, gender, disability) -> str:
    """
    Priority-ordered segment classification.
    All string comparisons are case-insensitive.
    """
    occ  = (occupation  or "").strip().lower()
    dis  = (disability  or "").strip().lower()
    gen  = (gender      or "").strip().lower()

    if dis in ("yes", "true", "1", "y"):
        return "Disabled"
    try:
        age_int = int(age) if age is not None else None
    except (ValueError, TypeError):
        age_int = None

    if age_int is not None and age_int >= 60:
        return "Senior"
    if age_int is not None and age_int < 30:
        return "Youth"
    if occ in FARMER_OCCUPATIONS:
        return "Farmer"
    if gen == "female":
        return "Women"
    if occ in BUSINESSMAN_OCCUPATIONS:
        return "Businessman"
    return "General"


# ── Neo4j helpers ─────────────────────────────────────────────────────────────
def get_driver():
    if NEO4J_PASSWORD is None:
        logger.error("NEO4J_PASSWORD variable is missing .env")
        sys.exit(1)
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        log.info("Neo4j connected: %s", NEO4J_URI)
        return driver
    except (Neo4jError, ServiceUnavailable) as e:
        log.error("Cannot connect to Neo4j: %s", e)
        sys.exit(1)


def fetch_voter(driver) -> list:
    query = """
        MATCH (c:Voter)
        RETURN id(c) AS node_id, c.name AS name,
               c.age AS age, c.occupation AS occupation,
               c.gender AS gender,
               c.disability_status AS disability
    """
    with driver.session() as session:
        return [dict(r) for r in session.run(query)]


def set_segment(driver, node_id: int, segment: str) -> None:
    query = "MATCH (c) WHERE id(c) = $nid SET c.segment = $seg"
    with driver.session() as session:
        session.run(query, nid=node_id, seg=segment)


def reset_segments(driver) -> int:
    query = "MATCH (c:Citizen) WHERE c.segment IS NOT NULL REMOVE c.segment RETURN count(c) AS n"
    with driver.session() as session:
        row = session.run(query).single()
        return row["n"] if row else 0


def verify_segments(driver) -> dict:
    query = """
        MATCH (c:Citizen)
        RETURN c.segment AS segment, count(c) AS cnt
        ORDER BY cnt DESC
    """
    with driver.session() as session:
        return {r["segment"]: r["cnt"] for r in session.run(query)}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Assign citizen segments to Neo4j Citizen nodes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/seed_segments.py              # assign segments
  python scripts/seed_segments.py --dry-run    # preview only
  python scripts/seed_segments.py --verify     # show segment counts
  python scripts/seed_segments.py --reset      # clear all segments
        """
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview classifications without writing to Neo4j")
    parser.add_argument("--verify", action="store_true",
                        help="Print segment counts from Neo4j and exit")
    parser.add_argument("--reset", action="store_true",
                        help="Remove segment property from all Citizen nodes")
    args = parser.parse_args()

    print("\n╔══════════════════════════════════════╗")
    print("║  JanSetu AI — Segment Seeder          ║")
    print("╚══════════════════════════════════════╝\n")

    driver = get_driver()

    if args.reset:
        n = reset_segments(driver)
        print(f"  Removed segment from {n} Citizen nodes.")
        driver.close()
        return

    if args.verify:
        counts = verify_segments(driver)
        print("  Segment distribution in Neo4j:")
        print("  " + "─" * 30)
        total = 0
        for seg, cnt in counts.items():
            label = seg or "(unset)"
            print(f"  {label:<18} {cnt:>5}")
            total += cnt
        print("  " + "─" * 30)
        print(f"  {'Total':<18} {total:>5}\n")
        driver.close()
        return

    # ── Classify and seed ──────────────────────────────────────────────────────
    citizens = fetch_citizens(driver)
    if not citizens:
        log.error("No Citizen nodes found in Neo4j. Seed citizens first.")
        driver.close()
        sys.exit(1)

    log.info("Found %d Citizen nodes. Classifying…", len(citizens))

    counts: dict = {}
    updated = 0

    for c in citizens:
        seg = classify_segment(
            age=c.get("age"),
            occupation=c.get("occupation"),
            gender=c.get("gender"),
            disability=c.get("disability"),
        )
        counts[seg] = counts.get(seg, 0) + 1

        if args.dry_run:
            log.info("[DRY RUN] %s → %s", c.get("name", "?"), seg)
        else:
            try:
                set_segment(driver, c["node_id"], seg)
                updated += 1
            except Exception as e:
                log.warning("Failed to update %s: %s", c.get("name"), e)

    driver.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "═" * 40)
    print(f"  Segment seeding {'preview' if args.dry_run else 'complete'}")
    print("═" * 40)
    for seg, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        schemes = ", ".join(SEGMENT_SCHEMES.get(seg, [])[:2])
        print(f"  {seg:<14} {cnt:>4} citizens   → {schemes}")
    print("═" * 40)
    if not args.dry_run:
        print(f"  Updated {updated}/{len(citizens)} Citizen nodes in Neo4j.")
    print()
    print("  Next step: open the Segments tab in the JanSetu dashboard")
    print("  to see segment cards and trigger bulk nudges.\n")


if __name__ == "__main__":
    main()