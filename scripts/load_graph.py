"""
scripts/load_graph.py  (replaces load_neo4j.py)
══════════════════════════════════════════════════════════════════════════════
JanSetu AI — Graph Loader  v3.1
Works with Memgraph (default, on-prem) and Neo4j AuraDB (cloud, non-PII).
══════════════════════════════════════════════════════════════════════════════

Run after generate_data.py:
  python scripts/load_graph.py              # Memgraph localhost:7687
  python scripts/load_graph.py --clear      # wipe graph first (default)
  python scripts/load_graph.py --no-clear   # append/upsert mode

MEMGRAPH QUICK START:
  docker run -p 7687:7687 memgraph/memgraph-mage
  # No auth needed — connects on bolt://localhost:7687

DATA SOVEREIGNTY:
  Default target is Memgraph on localhost.
  Script aborts if a foreign AuraDB URI is detected and DB_BACKEND=memgraph.
"""

import os
import sys
import argparse
import logging
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("load_graph")

# ── Config ─────────────────────────────────────────────────────────────────────
DB_BACKEND     = os.getenv("DB_BACKEND",     "memgraph").lower()
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     os.getenv("NEO4J_USERNAME", ""))
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
BATCH_SIZE     = 100

ALL_SCHEMES = [
    "PM Kisan Samman Nidhi", "Mudra Loan (PMMY)", "Ujjwala Yojana",
    "Kisan Credit Card", "Jan Dhan Yojana", "PM Awas Yojana",
    "PM Fasal Bima", "Ayushman Bharat", "Old Age Pension",
    "Post-Matric Scholarship", "PM Vishwakarma", "PM SVANidhi",
    "PM Matru Vandana", "PM Vaya Vandana", "ADIP Scheme",
    "Divyang Sahara Yojna",
]

# ── Sovereignty guard ──────────────────────────────────────────────────────────
def _check_sovereignty():
    foreign = ["databases.neo4j.io", "graphenedb.com"]
    if DB_BACKEND == "memgraph":
        for f in foreign:
            if f in NEO4J_URI:
                log.error(
                    "SOVEREIGNTY GUARD: DB_BACKEND=memgraph but URI points to "
                    "foreign cloud (%s). Set NEO4J_URI=bolt://localhost:7687. "
                    "Aborting.", NEO4J_URI
                )
                sys.exit(1)

_check_sovereignty()


def get_driver():
    auth = (NEO4J_USER, NEO4J_PASSWORD) if NEO4J_PASSWORD else None
    driver = GraphDatabase.driver(NEO4J_URI, auth=auth)
    try:
        driver.verify_connectivity()
        log.info("Connected  backend=%s  uri=%s", DB_BACKEND, NEO4J_URI)
    except Exception as exc:
        log.error("Cannot connect: %s", exc)
        sys.exit(1)
    return driver


def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i: i + size]


# ── Index / constraint creation ────────────────────────────────────────────────

def create_indexes(session):
    """
    Memgraph uses:  CREATE INDEX ON :Label(property)
    Neo4j uses:     CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.property)
    """
    if DB_BACKEND == "memgraph":
        stmts = [
            "CREATE INDEX ON :Citizen(epic_number)",
            "CREATE INDEX ON :Citizen(phone)",
            "CREATE INDEX ON :Citizen(segment)",
            "CREATE INDEX ON :Booth(booth_id)",
            "CREATE INDEX ON :Scheme(name)",
            "CREATE INDEX ON :Gali(gali_id)",
        ]
    else:
        stmts = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Citizen) REQUIRE c.epic_number IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Booth)   REQUIRE b.booth_id    IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Scheme)  REQUIRE s.name        IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Gali)    REQUIRE g.gali_id     IS UNIQUE",
        ]
    for stmt in stmts:
        try:
            session.run(stmt)
        except Exception as exc:
            log.warning("Index skipped: %s", exc)
    log.info("Indexes ready")


def create_schemes(session):
    session.run("UNWIND $schemes AS name MERGE (s:Scheme {name: name})",
                schemes=ALL_SCHEMES)
    log.info("Created %d Scheme nodes", len(ALL_SCHEMES))


# ── Citizens ───────────────────────────────────────────────────────────────────

def load_citizens(session, df):
    records = []
    for _, row in df.iterrows():
        records.append({
            "epic_number":        str(row["epic_number"]),
            "name":               str(row["name"]),
            "age":                int(row["age"]),
            "gender":             str(row["gender"]),
            "phone":              str(row["phone"]),
            "caste_category":     str(row["caste_category"]),
            "monthly_income":     int(row["monthly_income"]),
            "land_holding_acres": float(row["land_holding_acres"]),
            "occupation":         str(row["occupation"]),
            "aadhaar_linked":     str(row["aadhaar_linked"]).upper() == "TRUE",
            "has_lpg":            str(row["has_lpg"]).upper() == "TRUE",
            "has_bank_account":   str(row["has_bank_account"]).upper() == "TRUE",
            "has_pucca_house":    str(row["has_pucca_house"]).upper() == "TRUE",
            "is_income_taxpayer": str(row["is_income_taxpayer"]).upper() == "TRUE",
            "pension_amount":     int(float(row["pension_amount"])),
            "ration_card":        str(row["ration_card"]).upper() == "TRUE",
            "loan_defaulter":     str(row["loan_defaulter"]).upper() == "TRUE",
            "gali_id":            str(row["gali_id"]),
        })
    total = 0
    for batch in chunked(records, BATCH_SIZE):
        session.run("""
            UNWIND $batch AS c
            MERGE (n:Citizen {epic_number: c.epic_number})
            SET n.name               = c.name,
                n.age                = c.age,
                n.gender             = c.gender,
                n.phone              = c.phone,
                n.caste_category     = c.caste_category,
                n.monthly_income     = c.monthly_income,
                n.land_holding_acres = c.land_holding_acres,
                n.occupation         = c.occupation,
                n.aadhaar_linked     = c.aadhaar_linked,
                n.has_lpg            = c.has_lpg,
                n.has_bank_account   = c.has_bank_account,
                n.has_pucca_house    = c.has_pucca_house,
                n.is_income_taxpayer = c.is_income_taxpayer,
                n.pension_amount     = c.pension_amount,
                n.ration_card        = c.ration_card,
                n.loan_defaulter     = c.loan_defaulter,
                n.gali_id            = c.gali_id
        """, batch=batch)
        total += len(batch)
        print(f"   Citizens loaded: {total}/{len(records)}", end="\r")
    print(f"\n   {total} Citizen nodes created/updated")


# ── Booths ─────────────────────────────────────────────────────────────────────

def load_booths(session, df):
    booths = [{"booth_id": str(r["booth_id"]), "district": str(r["district"])}
              for _, r in df[["booth_id", "district"]].drop_duplicates().iterrows()]
    session.run("UNWIND $b AS b MERGE (n:Booth {booth_id: b.booth_id}) SET n.district = b.district",
                b=booths)
    log.info("%d Booth nodes created", len(booths))

    links = [{"epic_number": str(r["epic_number"]), "booth_id": str(r["booth_id"])}
             for _, r in df.iterrows()]
    total = 0
    for batch in chunked(links, BATCH_SIZE):
        session.run("""
            UNWIND $batch AS row
            MATCH (c:Citizen {epic_number: row.epic_number})
            MATCH (b:Booth   {booth_id:    row.booth_id})
            MERGE (c)-[:ASSIGNED_TO]->(b)
        """, batch=batch)
        total += len(batch)
    log.info("%d ASSIGNED_TO relationships", total)


# ── Galis ──────────────────────────────────────────────────────────────────────

def load_galis(session, df):
    galis = [
        {"gali_id": str(r["gali_id"]), "gali_name": str(r["gali_name"]),
         "booth_id": str(r["booth_id"]), "district": str(r["district"])}
        for _, r in df[["gali_id", "gali_name", "booth_id", "district"]].drop_duplicates("gali_id").iterrows()
    ]
    session.run("""
        UNWIND $batch AS g
        MERGE (n:Gali {gali_id: g.gali_id})
        SET n.gali_name = g.gali_name, n.booth_id = g.booth_id, n.district = g.district
    """, batch=galis)
    log.info("%d Gali nodes created", len(galis))

    # Gali → Booth COVERS edges
    session.run("""
        MATCH (g:Gali)
        MATCH (b:Booth {booth_id: g.booth_id})
        MERGE (g)-[:COVERS]->(b)
    """)
    log.info("COVERS relationships created")

    # Citizen → Gali LOCATED_IN edges
    links = [{"epic_number": str(r["epic_number"]), "gali_id": str(r["gali_id"])}
             for _, r in df.iterrows()]
    total = 0
    for batch in chunked(links, BATCH_SIZE):
        session.run("""
            UNWIND $batch AS row
            MATCH (c:Citizen {epic_number: row.epic_number})
            MATCH (g:Gali    {gali_id:     row.gali_id})
            MERGE (c)-[:LOCATED_IN]->(g)
        """, batch=batch)
        total += len(batch)
    log.info("%d LOCATED_IN relationships", total)


# ── Eligibility & Enrollment ───────────────────────────────────────────────────

def _load_relationships(session, df, col: str, rel: str):
    pairs = []
    for _, row in df.iterrows():
        for scheme in str(row[col]).split("|"):
            scheme = scheme.strip()
            if scheme and scheme.lower() != "nan":
                pairs.append({"epic_number": str(row["epic_number"]), "scheme": scheme})
    total = 0
    for batch in chunked(pairs, BATCH_SIZE):
        session.run(f"""
            UNWIND $batch AS row
            MATCH (c:Citizen {{epic_number: row.epic_number}})
            MATCH (s:Scheme  {{name:        row.scheme}})
            MERGE (c)-[:{rel}]->(s)
        """, batch=batch)
        total += len(batch)
    log.info("%d %s relationships", total, rel)


# ── Graph verification ─────────────────────────────────────────────────────────

def verify_graph(session):
    result = session.run("""
        MATCH (c:Citizen) WITH count(c) AS citizens
        MATCH (b:Booth)   WITH citizens, count(b) AS booths
        MATCH (g:Gali)    WITH citizens, booths, count(g) AS galis
        MATCH (s:Scheme)  WITH citizens, booths, galis, count(s) AS schemes
        MATCH ()-[r:ENROLLED_IN]->()
        WITH citizens, booths, galis, schemes, count(r) AS enrolled
        MATCH ()-[r2:POTENTIAL_ELIGIBILITY]->()
        WITH citizens, booths, galis, schemes, enrolled, count(r2) AS eligible
        MATCH ()-[r3:LOCATED_IN]->()
        RETURN citizens, booths, galis, schemes, enrolled, eligible, count(r3) AS located
    """).single()

    print("\nGraph Summary:")
    for k in ["citizens", "booths", "galis", "schemes", "enrolled", "eligible", "located"]:
        print(f"   {k:<12}: {result[k]}")

    # Floating node count
    floating = session.run("""
        MATCH (c:Citizen)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        OPTIONAL MATCH (c)-[e:ENROLLED_IN]->(s)
        WITH count(DISTINCT CASE WHEN e IS NULL THEN c END) AS floating
        RETURN floating
    """).single()
    print(f"   {'floating':<12}: {floating['floating']} citizens with unenrolled schemes")
    print()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Load voter CSV into Memgraph/Neo4j.")
    parser.add_argument("--no-clear", action="store_true",
                        help="Skip DETACH DELETE — run in upsert/append mode")
    args = parser.parse_args()

    csv_path = Path("voter_list.csv")
    if not csv_path.exists():
        log.error("voter_list.csv not found. Run generate_data.py first.")
        sys.exit(1)

    df = pd.read_csv(csv_path, dtype=str)
    if "gali_id" not in df.columns:
        log.error("voter_list.csv missing 'gali_id'. Re-run generate_data.py.")
        sys.exit(1)

    # Fix numeric columns after dtype=str load
    df["age"]             = pd.to_numeric(df["age"], errors="coerce").fillna(0).astype(int)
    df["monthly_income"]  = pd.to_numeric(df["monthly_income"], errors="coerce").fillna(0).astype(int)
    df["land_holding_acres"] = pd.to_numeric(df["land_holding_acres"], errors="coerce").fillna(0.0)
    df["pension_amount"]  = pd.to_numeric(df["pension_amount"], errors="coerce").fillna(0).astype(int)

    log.info("Loaded %d rows from voter_list.csv", len(df))

    driver = get_driver()
    with driver.session() as session:
        if not args.no_clear:
            session.run("MATCH (n) DETACH DELETE n")
            log.info("Graph cleared")

        create_indexes(session)
        create_schemes(session)
        load_citizens(session, df)
        load_booths(session, df)
        load_galis(session, df)
        _load_relationships(session, df, "eligible_schemes", "POTENTIAL_ELIGIBILITY")
        _load_relationships(session, df, "enrolled_schemes",  "ENROLLED_IN")
        verify_graph(session)

    driver.close()
    log.info("Done. Start the server: python -m uvicorn dashboard:app --port 8080")


if __name__ == "__main__":
    main()