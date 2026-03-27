"""
app/core/graph_engine.py
═══════════════════════════════════════════════════════════════════════════════
JanSetu AI — Graph DB Abstraction Layer  v3.1
═══════════════════════════════════════════════════════════════════════════════

BACKEND OPTIONS (set DB_BACKEND in .env):
  memgraph  →  bolt://localhost:7687  (default — on-prem, DPDP-compliant)
  neo4j     →  neo4j+s://...          (cloud — only for non-PII metadata)

DPDP / Data Sovereignty Notes:
  - Citizen PII (name, phone, aadhaar_hash) must NOT go to a foreign cloud DB.
  - Use Memgraph (self-hosted on Indian infra) for all Citizen nodes.
  - If you must use Neo4j AuraDB, ensure the region is "ap-south-1" (Mumbai)
    and strip all PII columns before loading — store only voter_id + segment.
  - The AADHAAR_PEPPER env var must never be logged or exposed.

ENVIRONMENT (.env):
  DB_BACKEND      = memgraph          # or neo4j
  NEO4J_URI       = bolt://localhost:7687
  NEO4J_USER      =                   # leave blank for Memgraph default
  NEO4J_PASSWORD  =                   # leave blank for Memgraph default
  AADHAAR_PEPPER  = <secret>
"""

import os
import csv
import logging
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

from neo4j import GraphDatabase          # Memgraph uses the same Bolt driver
from neo4j.exceptions import Neo4jError, ServiceUnavailable

log = logging.getLogger("jansetu.graph")

# ── Config ─────────────────────────────────────────────────────────────────────
DB_BACKEND = os.getenv("DB_BACKEND", "memgraph").lower()  # "memgraph" or "neo4j"
NEO4J_URI  = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER",     "")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD", "")
NUDGE_CSV  = os.getenv("NUDGE_CSV_PATH", "nudge_reports.csv")

# Warn loudly if someone points a cloud URI at a Memgraph config —
# prevents accidental PII leakage to foreign servers.
if DB_BACKEND == "memgraph" and "databases.neo4j.io" in NEO4J_URI:
    log.warning(
        "DB_BACKEND=memgraph but NEO4J_URI points to Neo4j AuraDB (%s). "
        "Citizen PII will be sent to a foreign cloud server. "
        "Set NEO4J_URI=bolt://localhost:7687 for on-prem Memgraph.",
        NEO4J_URI,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Connection pool
# ══════════════════════════════════════════════════════════════════════════════

def _make_driver():
    """
    Returns a Neo4j/Memgraph Bolt driver.
    Memgraph on localhost has no auth by default — pass auth=None in that case.
    """
    auth = (NEO4J_USER, NEO4J_PASS) if NEO4J_PASS else None

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=auth,
        max_connection_lifetime=200,
        connection_timeout=30,
        max_connection_pool_size=10,
        connection_acquisition_timeout=60,
    )
    try:
        driver.verify_connectivity()
        log.info("Graph DB connected  backend=%s  uri=%s", DB_BACKEND, NEO4J_URI)
    except (ServiceUnavailable, Neo4jError) as exc:
        log.error("Cannot connect to graph DB: %s", exc)
        raise
    return driver


# ══════════════════════════════════════════════════════════════════════════════
# GraphDB wrapper
# ══════════════════════════════════════════════════════════════════════════════

class GraphDB:
    """
    Thin wrapper around the Bolt driver.

    All Cypher queries in this class are written to be compatible with both
    Memgraph ≥2.x and Neo4j ≥5.x.  Key compatibility notes:
      - Memgraph does not support CALL {} (subquery) syntax — use WITH chains.
      - Memgraph supports CASE WHEN … END in RETURN.
      - Use OPTIONAL MATCH + CASE WHEN instead of WHERE NOT EXISTS {…} patterns.
      - count(DISTINCT …) works in both.
    """

    def __init__(self):
        self.driver = _make_driver()

    # ── Low-level execute ──────────────────────────────────────────────────────

    def execute_query(self, query: str, params: dict | None = None):
        """
        Run a Cypher query.  Returns (records, summary, keys).
        Compatible with both neo4j.execute_query() and session.run() style.
        """
        params = params or {}
        try:
            with self.driver.session() as session:
                result  = session.run(query, **params)
                records = list(result)
                summary = result.consume()
                keys    = result.keys() if hasattr(result, "keys") else []
                return records, summary, keys
        except (Neo4jError, ServiceUnavailable) as exc:
            log.error("Cypher error: %s | query: %.120s", exc, query)
            raise

    # ── Dashboard ──────────────────────────────────────────────────────────────

    def get_dashboard_stats(self) -> dict:
        query = """
            MATCH (c:Citizen)
            WITH count(c) AS total
            MATCH (c2:Citizen)
            OPTIONAL MATCH (c2)-[:ENROLLED_IN]->(s:Scheme)
            WITH total,
                 count(DISTINCT CASE WHEN s IS NOT NULL THEN c2 END) AS enrolled
            MATCH (b:Booth)
            WITH total, enrolled, count(b) AS booths
            RETURN total, enrolled, booths,
                   CASE WHEN total > 0
                        THEN round(100.0 * enrolled / total * 10) / 10
                        ELSE 0.0 END AS saturation_pct
        """
        records, _, _ = self.execute_query(query)
        if not records:
            return {"total_citizens": 0, "total_booths": 0,
                    "enrolled": 0, "saturation_pct": 0.0}
        r = records[0]
        return {
            "total_citizens":  r["total"],
            "total_booths":    r["booths"],
            "enrolled":        r["enrolled"] or 0,
            "saturation_pct":  float(r["saturation_pct"] or 0),
            "region":          "Karnataka",
            "db_backend":      DB_BACKEND,
        }

    # ── Segments ───────────────────────────────────────────────────────────────

    def get_segments(self) -> list:
        query = """
            MATCH (c:Citizen)
            WHERE c.segment IS NOT NULL
            RETURN c.segment AS segment, count(c) AS count
            ORDER BY count DESC
        """
        records, _, _ = self.execute_query(query)
        return [{"segment": r["segment"], "count": r["count"]} for r in records]

    def get_segment_citizens(self, segment: str) -> list:
        query = """
            MATCH (c:Citizen {segment: $segment})-[:ASSIGNED_TO]->(b:Booth)
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            RETURN c.name AS name, c.phone AS phone,
                   c.age AS age, c.occupation AS occupation,
                   b.booth_id AS booth, b.district AS district,
                   collect(s.name) AS enrolled_schemes
            ORDER BY c.name
            LIMIT 200
        """
        records, _, _ = self.execute_query(query, {"segment": segment})
        return [dict(r) for r in records]

    # ── Citizen search ─────────────────────────────────────────────────────────

    def search_citizen(self, phone=None, name=None, booth_no=None) -> list:
        filters = []
        params: dict[str, Any] = {}
        if phone:
            filters.append("c.phone = $phone")
            params["phone"] = phone.strip()
        if name:
            filters.append("toLower(c.name) CONTAINS toLower($name)")
            params["name"] = name.strip()
        if booth_no:
            filters.append("b.booth_id = $booth_no")
            params["booth_no"] = booth_no.strip().upper()

        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        query = f"""
            MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
            {where}
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(pe:Scheme)
            WITH c, b,
                 collect(DISTINCT s.name) AS enrolled,
                 collect(DISTINCT pe.name) AS eligible
            RETURN c.name AS name, c.phone AS phone,
                   c.voter_id AS voter_id, c.segment AS segment,
                   b.booth_id AS booth, b.district AS district,
                   enrolled, eligible,
                   [x IN eligible WHERE NOT x IN enrolled] AS gaps
            LIMIT 50
        """
        records, _, _ = self.execute_query(query, params)
        return [dict(r) for r in records]

    # ── Booth intelligence ──────────────────────────────────────────────────────

    def get_booth_stats(self, booth_id: str) -> dict | None:
        query = """
            MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth {booth_id: $booth_id})
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            WITH count(DISTINCT c) AS total,
                 count(DISTINCT CASE WHEN s IS NOT NULL THEN c END) AS enrolled
            RETURN total, enrolled,
                   CASE WHEN total > 0
                        THEN round(100.0 * enrolled / total * 10) / 10
                        ELSE 0.0 END AS saturation_pct
        """
        records, _, _ = self.execute_query(query, {"booth_id": booth_id})
        if not records or records[0]["total"] == 0:
            return None
        r   = records[0]
        pct = float(r["saturation_pct"] or 0)
        return {
            "booth_id":        booth_id,
            "total_voters":    r["total"],
            "enrolled":        r["enrolled"],
            "saturation_level": pct,
            "status": ("Excellent" if pct >= 80 else
                       "Good"      if pct >= 60 else
                       "Moderate"  if pct >= 40 else "Critical"),
        }

    def get_scheme_gaps(self, booth_id: str) -> list:
        """
        Top scheme gaps in a booth.
        Uses LEFT JOIN / OPTIONAL MATCH pattern — Memgraph-compatible.
        """
        query = """
            MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth {booth_id: $booth_id})
            MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
            OPTIONAL MATCH (c)-[e:ENROLLED_IN]->(s)
            WITH s.name AS scheme_name,
                 count(DISTINCT c)                                          AS eligible,
                 count(DISTINCT CASE WHEN e IS NOT NULL THEN c END)        AS enrolled
            WITH scheme_name, (eligible - enrolled) AS gap_count
            WHERE gap_count > 0
            RETURN scheme_name, gap_count
            ORDER BY gap_count DESC
            LIMIT 10
        """
        records, _, _ = self.execute_query(query, {"booth_id": booth_id})
        return [{"scheme_name": r["scheme_name"], "gap_count": r["gap_count"]}
                for r in records]

    # ── Floating nodes ──────────────────────────────────────────────────────────

    def detect_floating_nodes(self, scheme_name: str | None = None) -> list:
        """
        Citizens who are eligible for a scheme but not enrolled.
        Floating Node = unenrolled eligible citizen — core JanSetu metric.
        """
        scheme_filter = "AND s.name = $scheme_name" if scheme_name else ""
        query = f"""
            MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
            MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
            OPTIONAL MATCH (c)-[e:ENROLLED_IN]->(s)
            WITH c, b, s, e
            WHERE e IS NULL {scheme_filter}
            RETURN c.name AS name, c.phone AS phone,
                   c.segment AS segment, c.voter_id AS voter_id,
                   b.booth_id AS booth_no, b.district AS district,
                   s.name AS scheme
            ORDER BY b.booth_id, s.name
            LIMIT 500
        """
        params = {"scheme_name": scheme_name} if scheme_name else {}
        records, _, _ = self.execute_query(query, params)
        return [dict(r) for r in records]

    # ── District saturation ────────────────────────────────────────────────────

    def get_district_saturation(self) -> list:
        query = """
            MATCH (s:Scheme)<-[:ENROLLED_IN]-(c:Citizen)
            WITH s.name AS scheme, count(c) AS enrolled
            MATCH (s2:Scheme {name: scheme})<-[:POTENTIAL_ELIGIBILITY]-(c2:Citizen)
            WITH scheme, enrolled, count(c2) AS eligible
            WHERE eligible > 0
            RETURN scheme, enrolled, eligible,
                   round(100.0 * enrolled / eligible * 10) / 10 AS saturation_pct
            ORDER BY saturation_pct DESC
        """
        records, _, _ = self.execute_query(query)
        return [dict(r) for r in records]

    # ── Sentiment ──────────────────────────────────────────────────────────────

    def get_sentiment_results(self, limit: int = 50) -> list:
        query = """
            MATCH (f:Feedback)
            WHERE f.sentiment <> 'pending' AND f.sentiment IS NOT NULL
            RETURN f.feedback_id AS feedback_id, f.text AS text,
                   f.sentiment AS sentiment, f.score AS score,
                   f.language AS language, f.issues AS issues,
                   f.booth_id AS booth_id, f.district AS district,
                   f.timestamp AS timestamp
            ORDER BY f.timestamp DESC
            LIMIT $limit
        """
        records, _, _ = self.execute_query(query, {"limit": limit})
        return [dict(r) for r in records]

    def get_sentiment_summary(self) -> dict:
        query = """
            MATCH (f:Feedback)
            WHERE f.sentiment <> 'pending' AND f.sentiment IS NOT NULL
            RETURN count(f) AS total,
                   sum(CASE WHEN f.sentiment = 'positive' THEN 1 ELSE 0 END) AS positive,
                   sum(CASE WHEN f.sentiment = 'negative' THEN 1 ELSE 0 END) AS negative,
                   sum(CASE WHEN f.sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neutral,
                   round(avg(f.score) * 100) / 100 AS avg_score
        """
        records, _, _ = self.execute_query(query)
        if not records:
            return {"total": 0, "positive": 0, "negative": 0, "neutral": 0, "avg_score": 0.5}
        return dict(records[0])

    # ── Gali updates ───────────────────────────────────────────────────────────

    def get_gali_updates(self, district: str = "All Districts", limit: int = 50) -> list:
        dist_clause = "" if district == "All Districts" else "AND e.district = $district"
        query = f"""
            MATCH (g:Gali)-[:HAS_EVENT]->(e:InfraEvent)
            WHERE 1=1 {dist_clause}
            RETURN e.event_id AS event_id, e.type AS type,
                   e.description AS description, e.status AS status,
                   e.before_img AS before_img, e.after_img AS after_img,
                   e.timestamp AS timestamp,
                   g.gali_name AS gali_name, g.gali_id AS gali_id,
                   e.booth_id AS booth_id, e.district AS district
            ORDER BY e.timestamp DESC
            LIMIT $limit
        """
        params: dict[str, Any] = {"limit": limit}
        if district != "All Districts":
            params["district"] = district
        records, _, _ = self.execute_query(query, params)
        return [dict(r) for r in records]

    # ── Nudge history ──────────────────────────────────────────────────────────

    def get_nudge_history(self, limit: int = 100) -> list:
        """Read from CSV audit trail. Falls back to empty list gracefully."""
        if not Path(NUDGE_CSV).exists():
            return []
        records = []
        try:
            with open(NUDGE_CSV, "r", encoding="utf-16") as f:
                for row in csv.reader(f):
                    if len(row) == 7:
                        records.append({
                            "timestamp": row[0], "name": row[1], "phone":    row[2],
                            "scheme":    row[3], "booth": row[4], "district": row[5],
                            "message":   row[6],
                        })
        except OSError as exc:
            log.warning("Could not read nudge log: %s", exc)
        records.reverse()
        return records[:limit]

    # ── BLO verify ─────────────────────────────────────────────────────────────

    def blo_verify(self, voter_id: str) -> dict | None:
        query = """
            MATCH (c:Citizen {voter_id: $voter_id})-[:ASSIGNED_TO]->(b:Booth)
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(pe:Scheme)
            WITH c, b,
                 collect(DISTINCT s.name) AS enrolled,
                 collect(DISTINCT pe.name) AS eligible
            RETURN c.name AS name, c.phone AS phone,
                   c.voter_id AS voter_id, c.segment AS segment,
                   c.aadhaar_hash IS NOT NULL AS aadhaar_verified,
                   b.booth_id AS booth, b.district AS district,
                   enrolled,
                   [x IN eligible WHERE NOT x IN enrolled] AS gaps
        """
        records, _, _ = self.execute_query(query, {"voter_id": voter_id})
        return dict(records[0]) if records else None

    def blo_verify_by_phone(self, phone: str) -> dict | None:
        query = """
            MATCH (c:Citizen {phone: $phone})-[:ASSIGNED_TO]->(b:Booth)
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(pe:Scheme)
            WITH c, b,
                 collect(DISTINCT s.name) AS enrolled,
                 collect(DISTINCT pe.name) AS eligible
            RETURN c.name AS name, c.phone AS phone,
                   c.voter_id AS voter_id, c.segment AS segment,
                   c.aadhaar_hash IS NOT NULL AS aadhaar_verified,
                   b.booth_id AS booth, b.district AS district,
                   enrolled,
                   [x IN eligible WHERE NOT x IN enrolled] AS gaps
        """
        records, _, _ = self.execute_query(query, {"phone": phone})
        return dict(records[0]) if records else None

    # ── Constraints & indexes ──────────────────────────────────────────────────

    def create_indexes(self):
        """
        Creates indexes.
        Memgraph uses CREATE INDEX ON :Label(property) syntax.
        Neo4j uses CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.property).
        This method detects the backend and uses the correct syntax.
        """
        memgraph_indexes = [
            "CREATE INDEX ON :Citizen(phone)",
            "CREATE INDEX ON :Citizen(voter_id)",
            "CREATE INDEX ON :Citizen(segment)",
            "CREATE INDEX ON :Booth(booth_id)",
            "CREATE INDEX ON :Scheme(name)",
            "CREATE INDEX ON :Gali(gali_id)",
            "CREATE INDEX ON :Feedback(booth_id)",
            "CREATE INDEX ON :Feedback(sentiment)",
        ]
        neo4j_indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (c:Citizen) ON (c.phone)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Citizen) ON (c.voter_id)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Citizen) ON (c.segment)",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Booth) REQUIRE b.booth_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Scheme) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Gali) REQUIRE g.gali_id IS UNIQUE",
        ]
        stmts = memgraph_indexes if DB_BACKEND == "memgraph" else neo4j_indexes
        for stmt in stmts:
            try:
                self.execute_query(stmt)
            except Exception as exc:
                log.warning("Index/constraint skipped: %s — %s", stmt[:60], exc)
        log.info("Indexes created for backend=%s", DB_BACKEND)


# ── Singleton ─────────────────────────────────────────────────────────────────
graph_db = GraphDB()