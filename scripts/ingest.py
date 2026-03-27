"""
scripts/ingest.py
══════════════════════════════════════════════════════════════════════════════
JanSetu AI — Knowledge Graph Ingestion  v3.1
══════════════════════════════════════════════════════════════════════════════

DATA SOVEREIGNTY NOTE (DPDP Act 2023):
  Citizen PII (name, phone, Aadhaar) must stay on Indian-hosted infrastructure.
  This script defaults to Memgraph on localhost.  If you set NEO4J_URI to a
  foreign cloud endpoint (Neo4j AuraDB outside India, etc.), a hard warning is
  raised.  Set DB_BACKEND=neo4j ONLY for non-PII metadata (booth stats, etc.).

USAGE:
  python scripts/ingest.py                        # auto-locate voter_list.csv
  python scripts/ingest.py --csv path/to/file.csv
  python scripts/ingest.py --dry-run              # preview, no DB writes
"""

import os
import sys
import argparse
import logging
import pandas as pd
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("ingest")

# ── Data sovereignty guard ─────────────────────────────────────────────────────
NEO4J_URI  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DB_BACKEND = os.getenv("DB_BACKEND", "memgraph").lower()

FOREIGN_CLOUD_PATTERNS = [
    "databases.neo4j.io",   # Neo4j AuraDB (AWS us-east-1 / eu-west-1 by default)
    "graphenedb.com",
    "graph.azure.com",
]

def _sovereignty_check():
    for pattern in FOREIGN_CLOUD_PATTERNS:
        if pattern in NEO4J_URI:
            log.error(
                "DATA SOVEREIGNTY VIOLATION: NEO4J_URI points to a foreign cloud "
                "server (%s). Citizen PII cannot be sent outside India under DPDP "
                "Act 2023. Use Memgraph on Indian infra: bolt://localhost:7687. "
                "Aborting.", NEO4J_URI
            )
            sys.exit(1)
    if DB_BACKEND == "neo4j" and "aura" in NEO4J_URI.lower():
        log.warning(
            "Neo4j AuraDB detected. Ensure your instance is in ap-south-1 (Mumbai). "
            "Consider Memgraph on-prem for full DPDP compliance."
        )

_sovereignty_check()

# ── Graph DB connection ───────────────────────────────────────────────────────
try:
    from app.core.graph_engine import graph_db
    USE_APP_ENGINE = True
    log.info("Using app.core.graph_engine (GraphDB singleton)")
except ImportError:
    USE_APP_ENGINE = False
    from neo4j import GraphDatabase

    class _DirectDriver:
        def __init__(self):
            auth = (os.getenv("NEO4J_USER", ""), os.getenv("NEO4J_PASSWORD", ""))
            self._driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=auth if auth[1] else None,
            )
        def execute_query(self, query, params=None):
            with self._driver.session() as session:
                result = session.run(query, **(params or {}))
                return list(result), None, None

    graph_db = _DirectDriver()
    log.info("Using direct Bolt driver — app.core.graph_engine not found")


# ── Segment classification ─────────────────────────────────────────────────────
FARMER_OCCS      = {"farmer", "agriculture", "kisan", "cultivator", "farm labour",
                    "dairy farmer", "animal husbandry", "fisherman"}
BUSINESSMAN_OCCS = {"small trader", "artisan", "businessman", "shopkeeper",
                    "self employed", "vendor", "contractor", "trader",
                    "merchant", "retailer"}

def classify_segment(age, occupation, disability, gender="") -> str:
    occ = str(occupation or "").strip().lower()
    dis = str(disability or "").strip().lower()
    gen = str(gender or "").strip().lower()
    try:
        age_int = int(float(age)) if age is not None and str(age) not in ("", "nan") else None
    except (ValueError, TypeError):
        age_int = None

    if dis in ("yes", "true", "1", "y"):
        return "Disabled"
    if age_int is not None and age_int >= 60:
        return "Senior"
    if age_int is not None and age_int < 30:
        return "Youth"
    if occ in FARMER_OCCS:
        return "Farmer"
    if gen == "female":
        return "Women"
    if occ in BUSINESSMAN_OCCS:
        return "Businessman"
    return "General"


SEGMENT_ELIGIBILITY = {
    "Youth":       ["Mudra Loan (PMMY)", "Post-Matric Scholarship", "Jan Dhan Yojana", "PM SVANidhi"],
    "Farmer":      ["PM Kisan Samman Nidhi", "PM Fasal Bima", "Kisan Credit Card"],
    "Senior":      ["Old Age Pension", "Ayushman Bharat", "PM Vaya Vandana"],
    "Disabled":    ["Divyang Sahara Yojna", "Ayushman Bharat", "ADIP Scheme"],
    "Women":       ["Ujjwala Yojana", "PM Awas Yojana", "Ayushman Bharat", "PM Matru Vandana"],
    "Businessman": ["PM Vishwakarma", "Mudra Loan (PMMY)", "PM SVANidhi"],
    "General":     ["Jan Dhan Yojana", "Ayushman Bharat"],
}
UNIVERSAL_LOW_INCOME = ["Jan Dhan Yojana", "PM Awas Yojana"]


def safe_str(val) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() == "nan" else s


# ── Memgraph-compatible Cypher patterns ───────────────────────────────────────
# NOTE: Memgraph does not support CALL {} subqueries.
# All multi-step operations are broken into separate session.run() calls.

def _upsert_citizen(voter_id, row, segment, params):
    """MERGE citizen node. Memgraph-compatible — no CALL {}."""
    query = """
        MERGE (c:Citizen {voter_id: $v_id})
        SET c.name       = $name,
            c.age        = $age,
            c.occupation = $occ,
            c.income     = $income,
            c.disability = $disability,
            c.phone      = $phone,
            c.segment    = $segment,
            c.gender     = $gender
    """
    graph_db.execute_query(query, params)


def _link_enrolled(voter_id, enrolled_str):
    if not enrolled_str:
        return
    # Split on pipe or comma
    schemes = [s.strip() for s in enrolled_str.replace("|", ",").split(",")
               if s.strip() and s.strip().lower() != "nan"]
    for scheme in schemes:
        graph_db.execute_query("""
            MATCH (c:Citizen {voter_id: $v_id})
            MERGE (s:Scheme {name: $scheme})
            MERGE (c)-[:ENROLLED_IN]->(s)
        """, {"v_id": voter_id, "scheme": scheme})


def _link_eligibility(voter_id, eligible_schemes):
    for scheme in eligible_schemes:
        graph_db.execute_query("""
            MATCH (c:Citizen {voter_id: $v_id})
            MERGE (s:Scheme {name: $scheme})
            MERGE (c)-[:POTENTIAL_ELIGIBILITY]->(s)
        """, {"v_id": voter_id, "scheme": scheme})


def _link_booth(voter_id, booth_id, district):
    if not booth_id:
        return
    graph_db.execute_query("""
        MATCH (c:Citizen {voter_id: $v_id})
        MERGE (b:Booth {booth_id: $booth_id})
        SET b.district = $district
        MERGE (c)-[:ASSIGNED_TO]->(b)
    """, {"v_id": voter_id, "booth_id": booth_id, "district": district})


def _link_gali(voter_id, gali_id, gali_name, booth_id, district):
    if not gali_id:
        return
    graph_db.execute_query("""
        MATCH (c:Citizen {voter_id: $v_id})
        MERGE (g:Gali {gali_id: $gali_id})
        SET g.gali_name = $gali_name,
            g.booth_id  = $booth_id,
            g.district  = $district
        MERGE (c)-[:LOCATED_IN]->(g)
    """, {"v_id": voter_id, "gali_id": gali_id, "gali_name": gali_name,
           "booth_id": booth_id, "district": district})
    # Ensure Gali-Booth COVERS edge exists
    graph_db.execute_query("""
        MATCH (g:Gali {gali_id: $gali_id})
        MATCH (b:Booth {booth_id: $booth_id})
        MERGE (g)-[:COVERS]->(b)
    """, {"gali_id": gali_id, "booth_id": booth_id})


# ── Main ingestion ─────────────────────────────────────────────────────────────

def run_ingestion(csv_path: Path, dry_run: bool = False):
    if not csv_path.exists():
        log.error("CSV not found: %s", csv_path)
        sys.exit(1)

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]

    total = len(df)
    log.info("Loaded %d rows from %s", total, csv_path.name)
    if dry_run:
        log.info("DRY RUN — no DB writes")

    segment_counts: dict = {}

    for i, row in df.iterrows():
        age        = row.get("age")
        occupation = safe_str(row.get("occupation"))
        disability = safe_str(row.get("disability_status", row.get("disability", "")))
        gender     = safe_str(row.get("gender", ""))
        income     = safe_str(row.get("income_bracket", row.get("income", row.get("monthly_income", ""))))
        phone      = safe_str(row.get("phone", "N/A"))
        booth_id   = safe_str(row.get("booth_id", row.get("booth_no", row.get("booth", ""))))
        district   = safe_str(row.get("district", ""))
        enrolled   = safe_str(row.get("enrolled_schemes", ""))
        gali_id    = safe_str(row.get("gali_id", ""))
        gali_name  = safe_str(row.get("gali_name", ""))
        voter_id   = safe_str(row.get("voter_id", row.get("epic_number", str(i))))

        segment = classify_segment(age, occupation, disability, gender)
        segment_counts[segment] = segment_counts.get(segment, 0) + 1

        # Determine eligible schemes
        eligible_schemes = list(SEGMENT_ELIGIBILITY.get(segment, []))
        if str(income).lower() == "low":
            for s in UNIVERSAL_LOW_INCOME:
                if s not in eligible_schemes:
                    eligible_schemes.append(s)

        params = {
            "v_id":       voter_id,
            "name":       safe_str(row.get("name", "")),
            "age":        int(float(age)) if str(age) not in ("", "nan") else None,
            "occ":        occupation,
            "income":     income,
            "disability": disability,
            "phone":      phone,
            "segment":    segment,
            "gender":     gender,
        }

        if not dry_run:
            _upsert_citizen(voter_id, row, segment, params)
            _link_enrolled(voter_id, enrolled)
            _link_eligibility(voter_id, eligible_schemes)
            _link_booth(voter_id, booth_id, district)
            if gali_id:
                _link_gali(voter_id, gali_id, gali_name, booth_id, district)
        else:
            log.info("[DRY RUN] %s → segment=%s  booth=%s  gali=%s",
                     params["name"], segment, booth_id, gali_id)

        if (i + 1) % 50 == 0 or (i + 1) == total:
            print(f"  {'[DRY RUN] ' if dry_run else ''}Processed {i + 1}/{total}...")

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 52)
    print(f"  Ingestion {'preview' if dry_run else 'complete'}  (backend: {DB_BACKEND})")
    print("=" * 52)
    print(f"  Total citizens   : {total}")
    print(f"  Segment breakdown:")
    for seg, cnt in sorted(segment_counts.items(), key=lambda x: -x[1]):
        schemes = ", ".join(SEGMENT_ELIGIBILITY.get(seg, [])[:2])
        print(f"    {seg:<14} {cnt:>3}  →  {schemes}")
    print("=" * 52)
    if not dry_run:
        print("\nNext steps:")
        print("  python scripts/aadhaar_hash.py")
        print("  python -m uvicorn dashboard:app --reload --port 8080\n")


def main():
    parser = argparse.ArgumentParser(description="Ingest voter CSV into graph DB.")
    parser.add_argument("--csv",     type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    if args.csv:
        csv_path = args.csv
    else:
        for candidate in [
            Path(__file__).parent.parent / "voter_list.csv",
            Path(__file__).parent.parent / "data" / "raw" / "voter_list.csv",
        ]:
            if candidate.exists():
                csv_path = candidate
                break
        else:
            log.error("voter_list.csv not found. Use: python scripts/ingest.py --csv path/to/file.csv")
            sys.exit(1)

    run_ingestion(csv_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()