"""
JanSetu AI — Unified Backend Server  v3.1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fixes applied over v3.0:
  FIX 1 — datetime import conflict: removed bare `import datetime`, use `from datetime import datetime`
  FIX 2 — health_check used datetime.datetime.utcnow() → datetime.utcnow()
  FIX 3 — create_gali_event / submit_feedback used datetime.datetime.now() → datetime.now()
  FIX 4 — /api/management/leaderboard was missing → added with Worker seeding
  FIX 5 — /api/booths/{booth_id} alias added (was only /api/booth/{booth_id}/stats)
  FIX 6 — segments_overview had duplicate ELSE clause causing SyntaxError
  FIX 7 — pending_nudges / bulk_nudge / segment routes used :Voter label → :Citizen
  FIX 8 — worker_leaderboard Cypher had // comments (invalid in Memgraph) → removed
  FIX 9 — NUDGE_CSV set twice (hardcoded then overridden) — removed first assignment
  FIX 10 — leaderboard Cypher referenced :Voter → :Citizen
"""

import os, re, csv, time, hashlib, logging, uuid, shutil, json
from datetime import datetime
from pathlib import Path
from typing import List, Optional ,Dict

from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException, UploadFile, File ,APIRouter
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from neo4j import GraphDatabase
from google import genai as genai_simple

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("jansetu")

IMAGES_DIR = Path("gali_images")
IMAGES_DIR.mkdir(exist_ok=True)

app = FastAPI(
    title="JanSetu AI — Booth Intelligence API",
    version="3.1",
    description="Knowledge-Graph powered welfare saturation engine for Karnataka. DPDP-compliant via Memgraph.",
)

UI_DIR = Path("ui")
UI_DIR.mkdir(exist_ok=True)
app.mount("/ui", StaticFiles(directory="ui"), name="ui")
app.mount("/gali_images", StaticFiles(directory="gali_images"), name="gali_images")

@app.get("/", include_in_schema=False)
async def read_index():
    return FileResponse(os.path.join("ui", "index.html"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ─────────────────────────────────────────────────────────────────────
MEMGRAPH_URI   = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
MEMGRAPH_USER  = os.getenv("NEO4J_USER",     "")
MEMGRAPH_PASS  = os.getenv("NEO4J_PASSWORD", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
NUDGE_CSV      = os.getenv("NUDGE_CSV_PATH", "nudge_reports.csv")   # FIX 9: single assignment

_driver       = None
_genai_client = None

class GraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

# INITIALIZE THE MISSING OBJECT
# If using Memgraph on default settings, username/password are empty strings
graph_db = GraphDB("bolt://localhost:7687", "", "")

def get_driver():
    global _driver
    if _driver is None:
        log.info("Connecting to Memgraph at %s", MEMGRAPH_URI)
        _driver = GraphDatabase.driver(
            MEMGRAPH_URI,
            auth=(MEMGRAPH_USER, MEMGRAPH_PASS) if MEMGRAPH_PASS else None,
            max_connection_lifetime=200,
            connection_timeout=30,
            max_connection_pool_size=5,
            connection_acquisition_timeout=60,
        )
    return _driver


def get_genai():
    global _genai_client
    if _genai_client is None:
        _genai_client = genai_simple.Client(api_key="AIzaSyD4ZuMtskqXMPxxLj-PVwsmxuM1Plkn1d0")
        log.info("Gemini client initialised")
    return _genai_client


# ── Pydantic models ────────────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str; memgraph: str; gemini: str; timestamp: str

class SchemeStats(BaseModel):
    scheme: str; enrolled: int; eligible: int; saturation_pct: float

class DistrictDashboardResponse(BaseModel):
    data: List[SchemeStats]

class BoothSummary(BaseModel):
    booth_id: str; district: str; saturation_pct: float

class AllBoothsResponse(BaseModel):
    booths: List[BoothSummary]

class BoothStatsResponse(BaseModel):
    total_voters: int; saturation_level: float; status: str

class GapEntry(BaseModel):
    scheme_name: str; gap_count: int

class BoothGapsResponse(BaseModel):
    critical_gaps: List[GapEntry]

class CitizenResponse(BaseModel):
    name: str; phone: str; booth: str; district: str; benefits: List[str]

class NudgeEntry(BaseModel):
    name: str; phone: str; booth: str; district: str; scheme: str
    already_enrolled: List[str]
    occupation: Optional[str] = None
    is_floating: Optional[str] = None

class PendingNudgesResponse(BaseModel):
    pending: List[NudgeEntry]

class NudgeGenerateResponse(BaseModel):
    name: str; scheme: str; message: str; language: str

class OverviewMetrics(BaseModel):
    total_voters: int; enrolled: int; floating_nodes: int; saturation_pct: float

class EligibleScheme(BaseModel):
    scheme: str; enrolled: bool

class CitizenEligibilityResponse(BaseModel):
    name: str; phone: str; schemes: List[EligibleScheme]

class NudgeHistoryEntry(BaseModel):
    timestamp: str = ""; name: str = ""; phone: str = ""
    scheme: str = ""; booth: str = ""; district: str = ""; message: str = ""

class NudgeHistoryResponse(BaseModel):
    history: List[NudgeHistoryEntry]; total: int

class DistrictsResponse(BaseModel):
    districts: List[str]

class SegmentStat(BaseModel):
    segment: str; icon: str; total: int; enrolled: int; floating: int
    saturation_pct: float; top_scheme: Optional[str]

class SegmentOverviewResponse(BaseModel):
    segments: List[SegmentStat]

class SegmentCitizen(BaseModel):
    name: str; phone: str; age: Optional[int]; occupation: Optional[str]
    booth: str; district: str; enrolled_count: int; gap_count: int

class SegmentCitizensResponse(BaseModel):
    segment: str; citizens: List[SegmentCitizen]; total: int

class BulkNudgeResult(BaseModel):
    sent: int; failed: int; skipped: int; messages: List[dict]

class FeedbackRequest(BaseModel):
    citizen_id: str
    feedback_text: str


# ── Helpers ────────────────────────────────────────────────────────────────────
def log_nudge(name, phone, scheme, booth, district, message):
    """
    Write one nudge record to nudge_reports.csv as UTF-8.
    If the existing file is UTF-16 or UTF-8-sig (written by old code versions),
    it is transparently migrated to plain UTF-8 first (one-time conversion).
    """
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = (message or "").replace("\n", " ").replace("\r", " ")
    new_row = [ts, str(name or ""), str(phone or ""), str(scheme or ""),
               str(booth or ""), str(district or ""), msg]
    try:
        csv_path = Path(NUDGE_CSV)

        # ── One-time migration: UTF-16 / UTF-8-sig → UTF-8 ────────────
        if csv_path.exists() and csv_path.stat().st_size > 0:
            enc = detect_csv_encoding(csv_path)
            if enc != "utf-8":
                log.info("Migrating nudge CSV from %s to utf-8", enc)
                existing_rows = []
                try:
                    with open(csv_path, "r", encoding=enc, newline="") as f:
                        for row in csv.reader(f):
                            if len(row) == 7 and row[0].strip().lower() != "timestamp":
                                existing_rows.append(row)
                except Exception as read_exc:
                    log.warning("Could not read existing CSV for migration: %s", read_exc)
                    existing_rows = []
                # Rewrite entire file as UTF-8
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp","name","phone","scheme","booth","district","message"])
                    writer.writerows(existing_rows)
                log.info("Migration complete: %d records converted", len(existing_rows))

        # ── Normal append ──────────────────────────────────────────────
        write_header = not csv_path.exists() or csv_path.stat().st_size == 0
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(["timestamp","name","phone","scheme","booth","district","message"])
            writer.writerow(new_row)
        log.info("Nudge logged for %s (%s)", name, scheme)
    except Exception as exc:
        log.error("Failed to log nudge: %s", exc)


def detect_csv_encoding(path) -> str:
    """
    Detect the encoding of the nudge CSV by reading the BOM (first 3 bytes).
    Handles files written by all historical versions of log_nudge:
      - utf-16 (FF FE BOM)  — original code used encoding="utf-16"
      - utf-8-sig (EF BB BF) — intermediate code used encoding="utf-8-sig"
      - utf-8 (no BOM)       — current code uses encoding="utf-8"
    Returns the encoding string suitable for open().
    """
    try:
        with open(path, "rb") as f:
            bom = f.read(3)
        if bom[:2] in (b"\xff\xfe", b"\xfe\xff"):
            return "utf-16"        # handles both LE (FF FE) and BE (FE FF) automatically
        if bom == b"\xef\xbb\xbf":
            return "utf-8-sig"     # UTF-8 with BOM
        return "utf-8"             # no BOM — plain UTF-8
    except Exception:
        return "utf-8"             # safest fallback


def gemini_generate_with_retry(prompt: str, max_attempts: int = 3) -> str:
    client = get_genai()
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.models.generate_content(
                model="gemini-1.5-flash", contents=prompt
            )
            return response.text.strip()
        except Exception as exc:
            last_exc = exc
            wait = 2 ** attempt
            log.warning("Gemini attempt %d/%d failed (%s). Retrying in %ds…",
                        attempt, max_attempts, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Gemini failed after {max_attempts} attempts: {last_exc}") from last_exc


SEGMENT_ICONS = {
    # Occupation-based segments (from CSV)
    "Farmer":    "F",
    "Student":   "S",
    "Business":  "B",
    "Labour":    "L",
    # Legacy segment names kept for compatibility
    "Youth":        "Y", "Women": "W",
    "Businessman":  "B", "Senior": "S", "Disabled": "D", "General": "G",
}

SEGMENT_TONE = {
    "Farmer":    "Simple, respectful. Focus on crop support and income stability.",
    "Student":   "Energetic, aspirational. Highlight career growth and financial independence.",
    "Business":  "Practical, business-minded. Focus on growth and working capital.",
    "Labour":    "Supportive, empathetic. Emphasise livelihood support and welfare benefits.",
    # Legacy
    "Youth":       "Energetic, aspirational. Highlight career growth and financial independence.",
    "Women":       "Empowering, supportive. Emphasise family strength and independence.",
    "Businessman": "Practical, business-minded. Focus on growth and working capital.",
    "Senior":      "Gentle, respectful. Short sentences. Emphasise security and dignity.",
    "Disabled":    "Compassionate, inclusive. Emphasise accessibility and government support.",
    "General":     "Warm, official tone. Clear and actionable.",
}

# Fallback nudge templates when Gemini is unavailable
# Keyed by language, uses .format(name=, scheme=, booth=, district=)
NUDGE_FALLBACK = {
    "Kannada": (
        "ಗೌರವಾನ್ವಿತ {name} ಅವರೇ, ನೀವು {scheme} ಯೋಜನೆಗೆ ಅರ್ಹರಾಗಿದ್ದೀರಿ. "
        "ದಯವಿಟ್ಟು ಬೂಥ್ {booth} ಗೆ ಆಧಾರ್ ಕಾರ್ಡ್ ತೆಗೆದುಕೊಂಡು ಭೇಟಿ ನೀಡಿ."
    ),
    "Hindi": (
        "आदरणीय {name} जी, आप {scheme} योजना के लिए पात्र हैं। "
        "कृपया अपना आधार कार्ड लेकर बूथ {booth} पर आएं।"
    ),
    "Telugu": (
        "గౌరవనీయ {name} గారూ, మీరు {scheme} పథకానికి అర్హులు. "
        "దయచేసి ఆధార్ కార్డుతో బూత్ {booth} సందర్శించండి."
    ),
    "English": (
        "Dear {name}, you are eligible for the {scheme} scheme. "
        "Please visit Booth {booth}, {district} with your Aadhaar card to enroll."
    ),
}

def make_fallback_message(name: str, scheme: str, booth: str, district: str, language: str) -> str:
    template = NUDGE_FALLBACK.get(language, NUDGE_FALLBACK["English"])
    return template.format(name=name, scheme=scheme, booth=booth, district=district)



# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/health", response_model=HealthResponse, tags=["System"])
def health_check():
    mg_status = "unknown"; gemini_status = "unknown"
    try:
        with get_driver().session() as s:
            s.run("RETURN 1").single()
        mg_status = "connected"
    except Exception as exc:
        mg_status = f"error: {exc}"
    try:
        get_genai(); gemini_status = "initialised"
    except Exception as exc:
        gemini_status = f"error: {exc}"
    return HealthResponse(
        status="healthy" if "error" not in mg_status else "degraded",
        memgraph=mg_status, gemini=gemini_status,
        timestamp=datetime.utcnow().isoformat() + "Z",   # FIX 1 & 2
    )


# ══════════════════════════════════════════════════════════════════════════════
# REFERENCE
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/districts", response_model=DistrictsResponse, tags=["Reference"])
def list_districts():
    try:
        with get_driver().session() as session:
            districts = [r["district"] for r in session.run(
                "MATCH (b:Booth) RETURN DISTINCT b.district AS district ORDER BY district"
            ) if r["district"]]
        return DistrictsResponse(districts=districts)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/metrics/overview", response_model=OverviewMetrics, tags=["Analytics"])
def overview_metrics():
    """
    Saturation = scheme slots filled / total eligible scheme slots.
    Every citizen is enrolled in ≥1 scheme, but many still have gaps.
    E.g. eligible for 2 schemes, enrolled in 1 → 50% slot saturation.
    floating_nodes = citizens who have ANY unenrolled eligible scheme.
    """
    query = """
    MATCH (c:Citizen)
    WITH count(c) AS total_citizens
    MATCH (c2:Citizen)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
    WITH total_citizens, count(*) AS total_eligible_slots
    MATCH (c3:Citizen)-[:ENROLLED_IN]->(s2:Scheme)
    WITH total_citizens, total_eligible_slots, count(*) AS enrolled_slots
    MATCH (f:Citizen) WHERE f.is_floating_node = 'True'
    RETURN total_citizens, total_eligible_slots, enrolled_slots, count(f) AS floating
    """
    try:
        with graph_db.driver.session() as session:
            row = session.run(query).single()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not row or not row["total_citizens"]:
        return OverviewMetrics(total_voters=0, enrolled=0, floating_nodes=0, saturation_pct=0.0)
    total = row["total_citizens"]
    enrolled_slots = row["enrolled_slots"] or 0
    eligible_slots = row["total_eligible_slots"] or 1
    floating = row["floating"] or 0
    return OverviewMetrics(
        total_voters=total,
        enrolled=enrolled_slots,          # enrolled scheme slots (not unique citizens)
        floating_nodes=floating,           # citizens with any gap
        saturation_pct=round(100 * enrolled_slots / eligible_slots, 1),
    )


# Scheme → category mapping for filter support
SCHEME_CATEGORY = {
    "PM-Kisan":          "agri",
    "Ujjwala Yojana":    "welfare",
    "Ayushman Bharat":   "welfare",
    "Mudra Loan (PMMY)": "finance",
}

@app.get("/api/district-dashboard", response_model=DistrictDashboardResponse, tags=["Analytics"])
def district_dashboard(district: str = Query("all"), category: str = Query("all")):
    # Build optional category clause
    if category != "all":
        scheme_names = [s for s, c in SCHEME_CATEGORY.items() if c == category]
        if not scheme_names:
            return DistrictDashboardResponse(data=[])
        cat_filter_in = "AND s.name IN [" + ",".join(f"'{n}'" for n in scheme_names) + "]"
        cat_filter_in2 = "AND s2.name IN [" + ",".join(f"'{n}'" for n in scheme_names) + "]"
    else:
        cat_filter_in = cat_filter_in2 = ""

    if district == "all":
        query = f"""
            MATCH (s:Scheme)<-[:ENROLLED_IN]-(c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
            WHERE 1=1 {cat_filter_in}
            WITH s.name AS scheme, count(c) AS enrolled
            MATCH (s2:Scheme {{name: scheme}})<-[:POTENTIAL_ELIGIBILITY]-(c2:Citizen)
            WHERE 1=1 {cat_filter_in2}
            WITH scheme, enrolled, count(c2) AS eligible
            WHERE eligible > 0
            RETURN scheme, enrolled, eligible,
                   round(100.0 * enrolled / eligible * 10) / 10 AS saturation_pct
            ORDER BY saturation_pct DESC
        """
        params: dict = {}
    else:
        query = f"""
            MATCH (s:Scheme)<-[:ENROLLED_IN]-(c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
            WHERE b.district = $district {cat_filter_in}
            WITH s.name AS scheme, count(c) AS enrolled
            MATCH (s2:Scheme {{name: scheme}})<-[:POTENTIAL_ELIGIBILITY]-(c2:Citizen)
                  -[:ASSIGNED_TO]->(b2:Booth)
            WHERE b2.district = $district {cat_filter_in2}
            WITH scheme, enrolled, count(c2) AS eligible
            WHERE eligible > 0
            RETURN scheme, enrolled, eligible,
                   round(100.0 * enrolled / eligible * 10) / 10 AS saturation_pct
            ORDER BY saturation_pct DESC
        """
        params = {"district": district}
    try:
        with get_driver().session() as session:
            results = [dict(r) for r in session.run(query, **params)]
        return DistrictDashboardResponse(data=results)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# BOOTH ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/booths", response_model=AllBoothsResponse, tags=["Booth"])
def all_booths(district: str = Query("all")):
    if district == "all":
        query = """
            MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            WITH b.booth_id AS booth_id, b.district AS district,
                 count(DISTINCT c) AS total,
                 count(DISTINCT CASE WHEN s IS NOT NULL THEN c END) AS enrolled
            WHERE total > 0
            RETURN booth_id, district,
                   round(100.0 * enrolled / total * 10) / 10 AS saturation_pct
            ORDER BY saturation_pct DESC
        """
        params: dict = {}
    else:
        query = """
            MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
            WHERE b.district = $district
            OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
            WITH b.booth_id AS booth_id, b.district AS district,
                 count(DISTINCT c) AS total,
                 count(DISTINCT CASE WHEN s IS NOT NULL THEN c END) AS enrolled
            WHERE total > 0
            RETURN booth_id, district,
                   round(100.0 * enrolled / total * 10) / 10 AS saturation_pct
            ORDER BY saturation_pct DESC
        """
        params = {"district": district}
    try:
        with get_driver().session() as session:
            booths = [dict(r) for r in session.run(query, **params)]
        return AllBoothsResponse(booths=booths)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/booth/{booth_id}/stats", response_model=BoothStatsResponse, tags=["Booth"])
def booth_stats(booth_id: str):
    query = """
        MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth {booth_id: $booth_id})
        WITH count(DISTINCT c) AS voter_count
        MATCH (c2:Citizen)-[:ASSIGNED_TO]->(b2:Booth {booth_id: $booth_id})
        OPTIONAL MATCH (c2)-[:POTENTIAL_ELIGIBILITY]->(es:Scheme)
        WITH voter_count, count(*) AS total_slots
        MATCH (c3:Citizen)-[:ASSIGNED_TO]->(b3:Booth {booth_id: $booth_id})
        OPTIONAL MATCH (c3)-[:ENROLLED_IN]->(enr:Scheme)
        WITH voter_count, total_slots, count(*) AS enrolled_slots
        RETURN voter_count,
               CASE WHEN total_slots = 0 THEN 0.0
                    ELSE round(100.0 * enrolled_slots / total_slots * 10) / 10
               END AS saturation_pct
    """
    try:
        with get_driver().session() as session:
            row = session.run(query, booth_id=booth_id).single()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not row or row["voter_count"] == 0:
        return BoothStatsResponse(total_voters=0, saturation_level=0.0, status="No Data")
    pct = float(row["saturation_pct"] or 0)
    status = "Excellent" if pct >= 80 else "Good" if pct >= 60 else "Moderate" if pct >= 40 else "Critical"
    return BoothStatsResponse(total_voters=row["voter_count"], saturation_level=pct, status=status)


@app.get("/api/booth/{booth_id}/gaps", response_model=BoothGapsResponse, tags=["Booth"])
def booth_gaps(booth_id: str):
    query = """
        MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth {booth_id: $booth_id})
        MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s)
        WITH s.name AS scheme_name,
             count(DISTINCT c) AS eligible_count,
             count(DISTINCT CASE WHEN s IS NOT NULL THEN c END) AS enrolled_count
        WITH scheme_name, (eligible_count - enrolled_count) AS gap_count
        WHERE gap_count > 0
        RETURN scheme_name, gap_count
        ORDER BY gap_count DESC LIMIT 10
    """
    try:
        with get_driver().session() as session:
            gaps = [dict(r) for r in session.run(query, booth_id=booth_id)]
        return BoothGapsResponse(critical_gaps=gaps)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# FIX 5: alias route so /api/booths/{booth_id} resolves (UI calls plural form)
@app.get("/api/booths/{booth_id}", tags=["Booth"])
def get_booth_details(booth_id: str):
    query = """
        MATCH (b:Booth {booth_id: $booth_id})
        OPTIONAL MATCH (w:Worker)-[:MANAGES]->(b)
        OPTIONAL MATCH (b)-[:HAS_FEEDBACK]->(f:Feedback)
        WITH b, w, collect(f.keywords)[0..3] AS recent_issues,
             round(avg(f.score) * 100) / 100.0 AS sentiment_score
        RETURN b.booth_id AS id, b.district AS district,
               w.name AS leader, w.phone AS leader_phone,
               recent_issues, sentiment_score
    """
    try:
        with get_driver().session() as session:
            result = session.run(query, booth_id=booth_id).single()
        if not result:
            raise HTTPException(status_code=404, detail="Booth not found")
        return {
            "id":         result["id"],
            "district":   result["district"],
            "leader":     result["leader"] or "Unassigned",
            "phone":      result["leader_phone"],
            "sentiment":  result["sentiment_score"] or 0.5,
            "top_issues": [item for sublist in (result["recent_issues"] or [])
                           for item in (sublist or [])][:3],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    
@app.get("/api/booths/{booth_id}/graph")
async def get_booth_social_graph(booth_id: str):
    """
    Returns a rich knowledge graph for a booth:
    - Citizen nodes (coloured by floating/enrolled/fully-saturated status)
    - Scheme nodes
    - Gali nodes
    - ENROLLED_IN, POTENTIAL_ELIGIBILITY, LOCATED_IN edges
    """
    driver = get_driver()
    query = """
    MATCH (v:Citizen)-[:ASSIGNED_TO]->(b:Booth {booth_id: $booth_id})
    OPTIONAL MATCH (v)-[r:ENROLLED_IN|POTENTIAL_ELIGIBILITY|LOCATED_IN]->(target)
    RETURN v, r, target
    LIMIT 200
    UNION
    MATCH (w:Worker)-[:MANAGES]->(b:Booth {booth_id: $booth_id})
    RETURN w AS v, null AS r, null AS target
    """
    try:
        with driver.session() as session:
            result = session.run(query, booth_id=booth_id)
            nodes = {}
            edges = []
            for record in result:
                v = record["v"]
                vid = v.element_id
                if vid not in nodes:
                    node_labels = list(v.labels) if hasattr(v, "labels") else []
                    node_type = node_labels[0] if node_labels else "Citizen"
                    is_float = (v.get("is_floating_node") or "False") == "True"
                    # Color by node type
                    color_map = {
                        "Citizen": "#f5b930" if is_float else "#1edb85",
                        "Worker":  "#ff6b35",   # orange — matches schema diagram
                        "Scheme":  "#a78bfa",   # purple
                        "Gali":    "#3d9ef7",   # blue
                        "Booth":   "#6366f1",   # indigo
                    }
                    nodes[vid] = {
                        "id": vid,
                        "label": v.get("name") or node_type,
                        "type": node_type,
                        "occupation": v.get("occupation") or v.get("worker_id") or "—",
                        "is_floating": is_float,
                        "epic_number": v.get("epic_number") or v.get("worker_id"),
                        "phone": v.get("phone"),
                        "age": v.get("age"),
                        "gender": v.get("gender"),
                        "caste_category": v.get("caste_category"),
                        "color": color_map.get(node_type, "#7a8ba8"),
                    }
                if record["r"] is not None and record["target"] is not None:
                    t = record["target"]
                    tid = t.element_id
                    rel_type = record["r"].type
                    # Add target node (Scheme or Gali) if not yet present
                    if tid not in nodes:
                        node_labels = list(t.labels)
                        t_label = node_labels[0] if node_labels else "Node"
                        nodes[tid] = {
                            "id": tid,
                            "label": t.get("name") or t.get("gali_name") or tid,
                            "type": t_label,
                            "color": "#a78bfa" if t_label == "Scheme" else "#3d9ef7",
                        }
                    edges.append({
                        "from": vid,
                        "to": tid,
                        "type": rel_type,
                        "dashes": rel_type == "POTENTIAL_ELIGIBILITY",
                    })
        return {
            "voters": list(nodes.values()),
            "relationships": edges,
            "stats": {
                "total_nodes": len(nodes),
                "citizens": sum(1 for n in nodes.values() if n.get("type") == "Citizen"),
                "floating":  sum(1 for n in nodes.values() if n.get("type") == "Citizen" and n.get("is_floating")),
                "schemes":   sum(1 for n in nodes.values() if n.get("type") == "Scheme"),
                "workers":   sum(1 for n in nodes.values() if n.get("type") == "Worker"),
            }
        }
    except Exception as e:
        log.error("Graph Data Error: %s", e)
        return {"voters": [], "relationships": [], "error": str(e), "stats": {}}


# ══════════════════════════════════════════════════════════════════════════════
# MANAGEMENT ROUTES
# ══════════════════════════════════════════════════════════════════════════════

# FIX 4: was missing entirely — caused 404 in logs
@app.get("/api/management/leaderboard", tags=["Management"])
def worker_leaderboard():
    """
    Ranks Workers by citizens covered in their assigned booth.
    Provides Proof of Work for party leadership.
    Worker nodes must exist: MERGE (:Worker {name:..., phone:...})-[:MANAGES]->(:Booth {booth_id:...})
    """
    # FIX 8: removed // Cypher comments — invalid in Memgraph
    # FIX 10: :Voter → :Citizen
    query = """
        MATCH (w:Worker)-[:MANAGES]->(b:Booth)
        OPTIONAL MATCH (c:Citizen)-[:ASSIGNED_TO]->(b)
        OPTIONAL MATCH (b)-[:HAS_FEEDBACK]->(f:Feedback)
        WITH w.name AS worker, w.phone AS phone,
             b.booth_id AS booth, b.district AS district,
             count(DISTINCT c) AS total_voters,
             round(avg(f.score) * 100) / 100.0 AS booth_sentiment
        RETURN worker, phone, booth, district, total_voters,
               CASE WHEN booth_sentiment IS NULL THEN 0.5
                    ELSE booth_sentiment END AS booth_sentiment
        ORDER BY total_voters DESC
        LIMIT 10
    """
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query)]
        return {"leaderboard": rows, "total": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/workers", tags=["Management"])
def list_workers(district: str = Query("all")):
    """List all Worker nodes with their managed booth details."""
    dist_clause = "" if district == "all" else "WHERE b.district = $district"
    query = f"""
        MATCH (w:Worker)-[:MANAGES]->(b:Booth)
        {dist_clause}
        OPTIONAL MATCH (c:Citizen)-[:ASSIGNED_TO]->(b)
        WITH w.worker_id AS worker_id, w.name AS name, w.phone AS phone,
             w.district AS district,
             b.booth_id AS booth_id, b.district AS booth_district,
             count(DISTINCT c) AS total_citizens
        RETURN worker_id, name, phone, district,
               booth_id, booth_district, total_citizens
        ORDER BY booth_id
    """
    params = {} if district == "all" else {"district": district}
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
        return {"workers": rows, "total": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# CITIZEN ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/citizen/search", tags=["Citizen"])
async def citizen_search(
    phone:       str = Query(None),
    name:        str = Query(None),
    booth_no:    str = Query(None),
    epic_number: str = Query(None) 
):
    if not any([phone, name, booth_no, epic_number]):
        raise HTTPException(status_code=400, detail="Provide phone, name, booth_id, or epic_number")

    filters = []
    params = {}

    # 1. FIXED MAPPING: Ensure params keys match the $ placeholders in Cypher
    if phone:
        filters.append("c.phone = $phone")
        params["phone"] = phone.strip()
    if name:
        filters.append("toLower(c.name) CONTAINS toLower($name)")
        params["name"] = name.strip()
    if booth_no:
        filters.append("EXISTS((c)-[:ASSIGNED_TO]->(:Booth {booth_id: $booth_no}))")
        params["booth_no"] = booth_no.strip().upper()
    if epic_number:
        # Changed 'c.citizen_id' to 'c.epic_number' to match your RETURN statement
        filters.append("c.epic_number = $epic_number") 
        params["epic_number"] = epic_number.strip().upper()

    where_clause = "WHERE " + " AND ".join(filters)

    # 2. FIXED QUERY: Changed 'MATCH (c:phone)' to 'MATCH (c:Citizen)'
    query = f"""
        MATCH (c:Citizen)
        {where_clause}
        OPTIONAL MATCH (c)-[:ASSIGNED_TO]->(b:Booth)
        OPTIONAL MATCH (c)-[:LOCATED_IN]->(g:Gali)
        RETURN 
            c.name AS name, 
            c.phone AS phone,
            c.epic_number AS epic_number,
            c.age AS age,
            c.occupation AS occupation,
            c.gender AS gender,
            c.caste_category AS caste_category,
            c.monthly_income AS monthly_income,
            c.is_floating_node AS is_floating,
            b.booth_id AS booth,
            g.gali_name AS gali
        LIMIT 50
    """

    try:
        # Make sure your 'graph_db' or 'graph_db_driver' is globally accessible here
        with graph_db.driver.session() as session:
            result = session.run(query, **params)
            rows = [dict(r) for r in result]
            
            if not rows:
                raise HTTPException(status_code=404, detail="No citizens matching these details found.")
            
            return {"total_found": len(rows), "results": rows}

    except Exception as e:
        logging.error(f"Search Error: {e}")
        # Returning the actual error 'e' temporarily will help you debug if it still fails
        raise HTTPException(status_code=500, detail=f"Database search failed: {str(e)}")
    
@app.get("/api/citizen/{phone}/eligible-schemes", response_model=CitizenEligibilityResponse, tags=["Citizen"])
def citizen_eligible_schemes(phone: str):
    query = """
        MATCH (c:Citizen {phone: $phone})
        OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(enrolled_s:Scheme)
        WITH c, s, collect(DISTINCT enrolled_s.name) AS enrolled_names
        RETURN c.name AS name, c.phone AS phone, c.occupation AS occupation,
               s.name AS scheme, s.name IN enrolled_names AS enrolled
        ORDER BY enrolled DESC, scheme
    """
    try:
        with graph_db.driver.session() as session:
            rows = list(session.run(query, phone=phone))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not rows:
        raise HTTPException(status_code=404, detail="Citizen not found")
    schemes = [EligibleScheme(scheme=r["scheme"], enrolled=r["enrolled"])
               for r in rows if r["scheme"]]
    return CitizenEligibilityResponse(name=rows[0]["name"], phone=phone, schemes=schemes)


@app.get("/api/citizen/aadhaar-verify", tags=["Citizen"])
def aadhaar_verify(aadhaar: str = Query(None), epic: str = Query(None), phone: str = Query(None)):
    """
    BLO Field Verification. Supports three lookup modes:
    1. epic=CPT0000001          — EPIC number (most reliable, from voter CSV)
    2. phone=9876543210         — mobile number
    3. aadhaar=XXXXXXXXXXXX     — 12-digit Aadhaar (only works if aadhaar_linked=True citizens
                                   were seeded with a real hash; in demo data, falls back to
                                   matching citizens where aadhaar_linked=True by phone/epic)
    """
    if not any([aadhaar, epic, phone]):
        raise HTTPException(status_code=422, detail="Provide aadhaar, epic, or phone parameter.")

    if epic:
        where = "c.epic_number = $lookup"
        lookup = epic.strip().upper()
    elif phone:
        where = "c.phone = $lookup"
        lookup = phone.strip()
    else:
        # Aadhaar demo mode: treat 12-digit input as phone suffix or epic fallback
        aadhaar = aadhaar.strip().replace(" ", "")
        if not re.fullmatch(r"\d{12}", aadhaar):
            raise HTTPException(status_code=422, detail="Aadhaar must be exactly 12 digits.")
        # Try hash lookup first (works if seeded with real hashes)
        pepper = os.getenv("AADHAAR_PEPPER", "jansetu-karnataka-2025")
        aadhaar_hash = hashlib.sha256(f"{aadhaar}{pepper}".encode()).hexdigest()
        # Fall back to matching aadhaar_linked=True citizens by last 10 digits as phone
        where = "c.aadhaar_hash = $lookup OR (c.aadhaar_linked = 'True' AND c.phone = $lookup)"
        lookup = aadhaar_hash

    query = f"""
        MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
        WHERE {where}
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
        OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(pe:Scheme)
        WITH c, b,
             collect(DISTINCT s.name)  AS enrolled_schemes,
             collect(DISTINCT pe.name) AS all_eligible
        RETURN c.name AS name, c.phone AS phone,
               c.epic_number AS epic_number,
               c.occupation AS occupation,
               c.age AS age, c.gender AS gender,
               c.caste_category AS caste_category,
               c.aadhaar_linked AS aadhaar_linked,
               c.is_floating_node AS is_floating,
               b.booth_id AS booth, b.district AS district,
               enrolled_schemes,
               [x IN all_eligible WHERE NOT x IN enrolled_schemes] AS eligible_gaps,
               size(enrolled_schemes) AS scheme_count
    """
    try:
        with get_driver().session() as session:
            row = session.run(query, lookup=lookup).single()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Memgraph error: {str(exc)}") from exc
    if not row:
        raise HTTPException(status_code=404, detail="No citizen found. Try EPIC number or phone number instead.")
    gap_count = len(row["eligible_gaps"])
    return {
        "name": row["name"], "phone": row["phone"],
        "epic_number": row["epic_number"],
        "occupation": row["occupation"],
        "age": row["age"], "gender": row["gender"],
        "caste_category": row["caste_category"],
        "aadhaar_linked": row["aadhaar_linked"],
        "is_floating": row["is_floating"],
        "booth": row["booth"], "district": row["district"],
        "enrolled_schemes": row["enrolled_schemes"],
        "eligible_gaps": row["eligible_gaps"],
        "scheme_count": row["scheme_count"], "gap_count": gap_count,
        "saturation": "complete" if gap_count == 0 else "incomplete",
    }


# ══════════════════════════════════════════════════════════════════════════════
# NUDGE ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/nudges/pending", response_model=PendingNudgesResponse, tags=["Nudges"])
def pending_nudges(district: str = Query("all"), limit: int = Query(50, ge=1, le=500)):
    dist_clause = "" if district == "all" else "AND b.district = $district"
    # FIX 7: :Voter → :Citizen
    query = f"""
        MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
        WHERE 1=1 {dist_clause}
        MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        WITH c, b, s
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(enrolled_s:Scheme)
        WITH c, b, s, collect(DISTINCT enrolled_s.name) AS already_enrolled
        WHERE NOT s.name IN already_enrolled
        RETURN c.name AS name, c.phone AS phone,
               c.occupation AS occupation,
               c.is_floating_node AS is_floating,
               b.booth_id AS booth, b.district AS district,
               s.name AS scheme, already_enrolled
        ORDER BY c.name, s.name
        LIMIT $limit
    """
    params: dict = {"limit": limit}
    if district != "all":
        params["district"] = district
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
        return PendingNudgesResponse(pending=rows)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/nudge/generate", response_model=NudgeGenerateResponse, tags=["Nudges"])
def generate_nudge(
    name:     str = Query(...), phone:    str = Query(...),
    scheme:   str = Query(...), booth:    str = Query(...),
    district: str = Query(...), language: str = Query("Kannada"),
    segment:  str = Query("General"),
):
    """
    Generate a personalised nudge via Gemini and log it to CSV + Memgraph.
    If Gemini is unavailable, a language-appropriate fallback template is used
    so the nudge is ALWAYS logged and returned — never a 503.
    """
    tone = SEGMENT_TONE.get(segment, SEGMENT_TONE.get("General", "Warm, official tone."))
    prompt = (
        f"You are a government welfare officer in Karnataka, India.\n"
        f"Write a short personalised nudge message in {language} for:\n"
        f"Citizen: {name} | Segment: {segment} | Scheme: {scheme} | Booth: {booth}, {district}\n"
        f"Tone: {tone}\n"
        f"Rules: 2-3 sentences. Mention scheme \"{scheme}\" and booth \"{booth}\". "
        f"Ask for Aadhaar card. Write ENTIRE message in {language} only. Output ONLY the message text."
    )

    ai_used = True
    try:
        message = gemini_generate_with_retry(prompt)
    except Exception as exc:
        log.warning("Gemini unavailable for nudge (%s %s): %s — using fallback", name, scheme, exc)
        message = make_fallback_message(name, scheme, booth, district, language)
        ai_used = False

    # Always log — even fallback messages are useful records
    log_nudge(name, phone, scheme, booth, district, message)

    # Also persist nudge in Memgraph so it survives CSV issues
    try:
        nudge_query = """
            MATCH (c:Citizen {phone: $phone})
            MATCH (s:Scheme {name: $scheme})
            MERGE (n:NudgeRecord {
                phone: $phone, scheme: $scheme,
                timestamp: $ts
            })
            SET n.name=$name, n.booth=$booth, n.district=$district,
                n.message=$message, n.language=$language, n.ai_used=$ai_used
            MERGE (c)-[:RECEIVED_NUDGE]->(n)
            MERGE (n)-[:FOR_SCHEME]->(s)
        """
        with get_driver().session() as session:
            session.run(
                nudge_query,
                phone=phone, scheme=scheme,
                ts=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                name=name, booth=booth, district=district,
                message=message, language=language, ai_used=ai_used,
            )
    except Exception as exc:
        log.warning("Could not persist nudge to Memgraph: %s", exc)

    return NudgeGenerateResponse(name=name, scheme=scheme, message=message, language=language)


@app.get("/api/nudges/history", tags=["Nudges"])
def nudge_history(limit: int = Query(100, ge=1, le=1000)):
    """
    Read nudge history from CSV first, then supplement from Memgraph NudgeRecord nodes.
    Merges both sources, deduplicates by (phone, scheme, timestamp), returns newest-first.
    Entire function is wrapped in try/except so it never raises an unhandled 500.
    """
    try:
        seen: set = set()
        records: list = []

        # ── 1. Read CSV ────────────────────────────────────────
        csv_path = Path(NUDGE_CSV)
        if csv_path.exists():
            try:
                enc = detect_csv_encoding(csv_path)
                log.info("Reading nudge CSV with encoding: %s", enc)
                with open(csv_path, "r", encoding=enc, newline="") as f:
                    for row in csv.reader(f):
                        if len(row) != 7:
                            continue
                        ts, name, phone, scheme, booth, district, message = row
                        # Skip header row written by log_nudge on file creation
                        if ts.strip().lower() == "timestamp":
                            continue
                        key = (phone.strip(), scheme.strip(), ts.strip())
                        if key not in seen:
                            seen.add(key)
                            records.append(NudgeHistoryEntry(
                                timestamp=ts.strip(),
                                name=name.strip(),
                                phone=phone.strip(),
                                scheme=scheme.strip(),
                                booth=booth.strip(),
                                district=district.strip(),
                                message=message.strip(),
                            ))
            except Exception as csv_exc:
                log.warning("Could not read nudge CSV %s: %s", csv_path, csv_exc)

        # ── 2. Read from Memgraph ──────────────────────────────
        # No ORDER BY — Memgraph requires an index for ordering; we sort in Python
        mg_query = """
            MATCH (n:NudgeRecord)
            RETURN n.timestamp AS timestamp, n.name AS name,
                   n.phone AS phone, n.scheme AS scheme,
                   n.booth AS booth, n.district AS district,
                   n.message AS message
            LIMIT $limit
        """
        try:
            with get_driver().session() as session:
                rows = list(session.run(mg_query, limit=limit * 2))
            for row in rows:
                ts     = str(row["timestamp"] or "").strip()
                phone  = str(row["phone"]     or "").strip()
                scheme = str(row["scheme"]    or "").strip()
                key = (phone, scheme, ts)
                if key not in seen:
                    seen.add(key)
                    records.append(NudgeHistoryEntry(
                        timestamp=ts,
                        name=str(row["name"]     or "").strip(),
                        phone=phone,
                        scheme=scheme,
                        booth=str(row["booth"]    or "").strip(),
                        district=str(row["district"] or "").strip(),
                        message=str(row["message"]  or "").strip(),
                    ))
        except Exception as mg_exc:
            log.warning("Could not read nudge history from Memgraph: %s", mg_exc)

        if not records:
            return {"history": [], "total": 0}

        # Sort newest-first; guard against empty timestamp strings
        records.sort(key=lambda r: r.timestamp or "0000", reverse=True)
        result = records[:limit]
        return {"history": [r.dict() for r in result], "total": len(records)}

    except Exception as exc:
        log.error("nudge_history failed: %s", exc, exc_info=True)
        # Return empty history rather than 500
        return {"history": [], "total": 0, "error": str(exc)}


@app.post("/api/nudges/bulk", tags=["Nudges"])
def bulk_nudge(
    segment:  str = Query(...), district: str = Query("all"),
    language: str = Query("Kannada"), limit: int = Query(20, ge=1, le=100),
):
    dist_clause = "" if district == "all" else "AND b.district = $district"
    # FIX 7: :Voter → :Citizen
    query = f"""
        MATCH (c:Citizen {{occupation: $segment}})-[:ASSIGNED_TO]->(b:Booth)
        WHERE 1=1 {dist_clause}
        MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(enrolled_s:Scheme)
        WITH c, b, s, collect(DISTINCT enrolled_s.name) AS already_enrolled
        WHERE NOT s.name IN already_enrolled
        RETURN c.name AS name, c.phone AS phone,
               b.booth_id AS booth, b.district AS district, s.name AS scheme
        LIMIT $limit
    """
    params = {"segment": segment, "limit": limit}
    if district != "all":
        params["district"] = district
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not rows:
        return BulkNudgeResult(sent=0, failed=0, skipped=0, messages=[])
    tone = SEGMENT_TONE.get(segment, "Warm, official.")
    sent, failed, messages = 0, 0, []
    for row in rows:
        prompt = (
            f"Welfare officer in Karnataka. Write {language} nudge for:\n"
            f"{row['name']} | {segment} | {row['scheme']} | Booth {row['booth']}, {row['district']}\n"
            f"Tone: {tone} 2-3 sentences. Name scheme and booth. Ask for Aadhaar card. {language} only."
        )
        try:
            message = gemini_generate_with_retry(prompt, max_attempts=2)
            log_nudge(row["name"], row["phone"], row["scheme"], row["booth"], row["district"], message)
            messages.append({"name": row["name"], "phone": row["phone"],
                             "scheme": row["scheme"], "message": message, "status": "sent"})
            sent += 1
        except Exception as exc:
            messages.append({"name": row["name"], "status": "failed", "error": str(exc)})
            failed += 1
    return BulkNudgeResult(sent=sent, failed=failed, skipped=0, messages=messages)


@app.post("/api/nudge/{citizen_id}", tags=["Nudges"])
def send_nudge(citizen_id: str):
    query = """
        MATCH (c:Citizen {epic_number: $citizen_id})-[:ASSIGNED_TO]->(b:Booth)
        MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(enrolled_s:Scheme)
        WITH c, b, s, collect(DISTINCT enrolled_s.name) AS already_enrolled
        WHERE NOT s.name IN already_enrolled
        RETURN c.name AS name, c.phone AS phone, c.occupation AS segment,
               s.name AS scheme, b.booth_id AS booth, b.district AS district
        LIMIT 1
    """
    try:
        with get_driver().session() as session:
            result = session.run(query, citizen_id=citizen_id).single()
        if not result:
            raise HTTPException(status_code=404, detail="Citizen or eligibility gap not found")
        prompt = (
            f"Write a warm 2-sentence WhatsApp message in Kannada for {result['name']}. "
            f"Tell them they are eligible for {result['scheme']} and should visit "
            f"Booth {result['booth']} with their Aadhaar card."
        )
        message = gemini_generate_with_retry(prompt)
        log_nudge(result["name"], result["phone"], result["scheme"],
                  result["booth"], result["district"], message)
        log.info("[MOCK WHATSAPP] To: %s | %s", result["phone"], message[:60])
        return {"status": "success", "citizen": result["name"], "message": message}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# SEGMENT ROUTES — Objective 1
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/segments/overview", response_model=SegmentOverviewResponse, tags=["Segments"])
def segments_overview(district: str = Query("all")):
    dist_clause = "" if district == "all" else "AND b.district = $district"
    # FIX 6: removed duplicate ELSE clause that caused SyntaxError
    # FIX 7: :Voter → :Citizen
    query = f"""
        MATCH (c:Citizen)-[:ASSIGNED_TO]->(b:Booth)
        WHERE c.occupation IS NOT NULL {dist_clause}
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
        OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(ps:Scheme)
        WITH c.occupation AS segment,
             count(DISTINCT c) AS total,
             count(DISTINCT CASE WHEN s IS NOT NULL THEN c END) AS enrolled,
             count(DISTINCT CASE WHEN ps IS NOT NULL THEN c END) AS floating,
             collect(DISTINCT ps.name)[0] AS top_scheme
        RETURN segment, total, enrolled, floating, top_scheme,
               CASE WHEN total > 0
                    THEN round(1000.0 * enrolled / total) / 10.0
                    ELSE 0.0 END AS saturation_pct
        ORDER BY floating DESC
    """
    params = {} if district == "all" else {"district": district}
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
        if not rows:
            return SegmentOverviewResponse(segments=[])
        return SegmentOverviewResponse(segments=[
            SegmentStat(
                segment=r["segment"],
                icon=SEGMENT_ICONS.get(r["segment"], "G"),
                total=r["total"] or 0,
                enrolled=r["enrolled"] or 0,
                floating=r["floating"] or 0,
                saturation_pct=float(r["saturation_pct"]) if r["saturation_pct"] else 0.0,
                top_scheme=r["top_scheme"] or "N/A",
            ) for r in rows
        ])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/segments/{segment}/citizens", response_model=SegmentCitizensResponse, tags=["Segments"])
def segment_citizens(segment: str, district: str = Query("all"), limit: int = Query(50, ge=1, le=200)):
    dist_clause = "" if district == "all" else "AND b.district = $district"
    # FIX 7: :Voter → :Citizen (was /Voter path param AND :Voter label)
    query = f"""
        MATCH (c:Citizen {{occupation: $segment}})-[:ASSIGNED_TO]->(b:Booth)
        WHERE 1=1 {dist_clause}
        OPTIONAL MATCH (c)-[:ENROLLED_IN]->(s:Scheme)
        OPTIONAL MATCH (c)-[:POTENTIAL_ELIGIBILITY]->(ps:Scheme)
        WITH c, b,
             count(DISTINCT s) AS enrolled_count,
             count(DISTINCT ps) AS eligible_count
        RETURN c.name AS name, c.phone AS phone,
               c.age AS age, c.occupation AS occupation,
               b.booth_id AS booth, b.district AS district,
               enrolled_count,
               (eligible_count - enrolled_count) AS gap_count
        ORDER BY gap_count DESC LIMIT $limit
    """
    params = {"segment": segment, "limit": limit}
    if district != "all":
        params["district"] = district
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
        return SegmentCitizensResponse(
            segment=segment,
            citizens=[SegmentCitizen(**r) for r in rows],
            total=len(rows),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# GALI ROUTES — Objective 3
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/galis", tags=["Gali"])
def list_galis(district: str = Query("all"), booth_id: str = Query("all")):
    filters = []; params: dict = {}
    if district != "all":
        filters.append("b.district = $district"); params["district"] = district
    if booth_id != "all":
        filters.append("b.booth_id = $booth_id"); params["booth_id"] = booth_id
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = f"""
        MATCH (g:Gali)-[:COVERS]->(b:Booth)
        {where}
        OPTIONAL MATCH (g)-[:HAS_EVENT]->(e:InfraEvent)
        OPTIONAL MATCH (c:Citizen)-[:LOCATED_IN]->(g)
        RETURN g.gali_id AS gali_id, g.gali_name AS gali_name,
               b.booth_id AS booth_id, b.district AS district,
               count(DISTINCT e) AS event_count,
               count(DISTINCT c) AS citizen_count
        ORDER BY event_count DESC
    """
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
        return {"galis": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/gali/{gali_id}/events")
def gali_events(gali_id: str):
    query = """
        MATCH (g:Gali {gali_id: $gali_id})
        OPTIONAL MATCH (g)-[:HAS_EVENT]->(e:InfraEvent)
        RETURN e.event_id AS event_id, e.type AS type,
               e.description AS description, e.status AS status,
               e.before_img AS before_img, e.after_img AS after_img,
               e.timestamp AS timestamp
        ORDER BY e.timestamp DESC
    """
    with get_driver().session() as session:
        result = session.run(query, gali_id=gali_id)
        # Filter out null rows if no events exist yet
        rows = [dict(r) for r in result if r["event_id"] is not None]
        return {"events": rows}
    

@app.post("/api/gali/event", tags=["Gali"])
def create_gali_event(
    gali_id:     str = Query(...),
    type:        str = Query(...),
    description: str = Query(...),
):
    event_id  = str(uuid.uuid4())[:8].upper()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")   # FIX 3
    query = """
        MATCH (g:Gali {gali_id: $gali_id})-[:COVERS]->(b:Booth)
        CREATE (e:InfraEvent {
            event_id: $event_id, type: $type, description: $description,
            status: 'pending', before_img: '', after_img: '',
            timestamp: $timestamp, booth_id: b.booth_id, district: b.district
        })
        MERGE (g)-[:HAS_EVENT]->(e)
        RETURN e.event_id AS event_id
    """
    try:
        with get_driver().session() as session:
            row = session.run(query, gali_id=gali_id, event_id=event_id,
                              type=type, description=description, timestamp=timestamp).single()
        return {"event_id": row["event_id"], "status": "created"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/gali/event/{event_id}/upload", tags=["Gali"])
async def upload_event_image(
    event_id: str,
    phase:    str = Query(..., description="before or after"),
    file:     UploadFile = File(...),
):
    if phase not in ("before", "after"):
        raise HTTPException(status_code=422, detail="phase must be 'before' or 'after'")
    ext      = Path(file.filename).suffix.lower() or ".jpg"
    filename = f"{event_id}_{phase}{ext}"
    dest     = IMAGES_DIR / filename
    try:
        with dest.open("wb") as f:
            shutil.copyfileobj(file.file, f)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Upload failed: {exc}") from exc
    img_url    = f"/gali_images/{filename}"
    field      = "before_img" if phase == "before" else "after_img"
    new_status = "in_progress" if phase == "before" else "completed"
    try:
        with get_driver().session() as session:
            session.run(
                f"MATCH (e:InfraEvent {{event_id: $event_id}}) SET e.{field} = $img_url, e.status = $status",
                event_id=event_id, img_url=img_url, status=new_status,
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if phase == "after":
        _generate_gali_notification(event_id)
    return {"event_id": event_id, "phase": phase, "url": img_url, "status": new_status}


def _generate_gali_notification(event_id: str):
    try:
        with get_driver().session() as session:
            row = session.run("""
                MATCH (g:Gali)-[:HAS_EVENT]->(e:InfraEvent {event_id: $event_id})
                RETURN e.type AS type, e.description AS description,
                       g.gali_name AS gali_name, e.booth_id AS booth_id, e.district AS district
            """, event_id=event_id).single()
            if not row: return
            citizens = list(session.run("""
                MATCH (c:Citizen)-[:LOCATED_IN]->(g:Gali)-[:HAS_EVENT]->(e:InfraEvent {event_id: $event_id})
                RETURN c.name AS name, c.phone AS phone LIMIT 50
            """, event_id=event_id))
        prompt = (
            f"You are a government field officer in Karnataka. "
            f"Civic work completed: {row['type']} on {row['gali_name']}, "
            f"Booth {row['booth_id']}, {row['district']}. "
            f"Work details: {row['description']} "
            f"Write a SHORT 2-sentence Kannada notification to citizens. "
            f"Be warm and appreciative. Output ONLY the Kannada message."
        )
        message = gemini_generate_with_retry(prompt)
        for c in citizens:
            log_nudge(c["name"], c["phone"], f"Gali Update: {row['type']}",
                      row["booth_id"], row["district"], message)
        log.info("Gali notification sent to %d citizens on %s", len(citizens), row["gali_name"])
    except Exception as exc:
        log.warning("Gali notification failed for event %s: %s", event_id, exc)


@app.get("/api/gali/events/overview", tags=["Gali"])
def gali_events_overview(district: str = Query("all")):
    dist_clause = "" if district == "all" else "WHERE e.district = $district"
    query = f"MATCH (e:InfraEvent) {dist_clause} RETURN e.status AS status, count(e) AS count"
    params = {} if district == "all" else {"district": district}
    # Also get gali citizen stats as a proxy for coverage
    gali_query = """
        MATCH (g:Gali)-[:COVERS]->(b:Booth)
        OPTIONAL MATCH (c:Citizen)-[:LOCATED_IN]->(g)
        RETURN count(DISTINCT g) AS total_galis, count(DISTINCT c) AS total_citizens
    """
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
            gali_row = session.run(gali_query).single()
        counts = {"pending": 0, "in_progress": 0, "completed": 0}
        for r in rows:
            if r["status"] in counts:
                counts[r["status"]] = r["count"]
        counts["total_galis"] = gali_row["total_galis"] if gali_row else 0
        counts["total_citizens"] = gali_row["total_citizens"] if gali_row else 0
        return counts
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# SENTIMENT ROUTES — Objective 5
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/feedback/submit", tags=["Sentiment"])
def submit_and_analyze_feedback(phone: str = Query(...), text: str = Query(...), language: str = Query("English")):
    # 1. DATABASE LOOKUP: Find Booth via Phone
    booth_query = """
    MATCH (c:Citizen {phone: $phone})-[:ASSIGNED_TO]->(b:Booth)
    RETURN b.booth_id AS booth_id, b.district AS district, c.name AS name
    """
    
    try:
        with get_driver().session() as session:
            booth_record = session.run(booth_query, phone=phone).single()
            
            if not booth_record:
                raise HTTPException(status_code=404, detail="Phone number not found in voter records.")

            booth_id = booth_record["booth_id"]
            citizen_name = booth_record["name"]

            # 2. AI ANALYSIS: Gemini Sentiment Logic
            prompt = (
                f"Analyze this Karnataka citizen feedback and respond ONLY with valid JSON.\n"
                f"Feedback: \"{text}\"\n"
                f"Respond ONLY with: {{\"sentiment\": \"positive\"|\"negative\"|\"neutral\", "
                f"\"score\": 0.0-1.0, \"keywords\": [\"max 3 issues\"], \"summary\": \"1 sentence\"}}"
            )
            
            client = get_genai()
            response = client.models.generate_content(model="gemini-2.5-flash", contents=prompt)
            raw = response.text.strip().replace("```json", "").replace("```", "").strip()
            ai_data = json.loads(raw)

            # 3. GRAPH SAVE: Link Feedback to Citizen and Booth
            feedback_id = str(uuid.uuid4())[:8].upper()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

            session.run("""
                MATCH (c:Citizen {phone: $phone}), (b:Booth {booth_id: $booth_id})
                CREATE (f:Feedback {
                    feedback_id: $fid, text: $text, sentiment: $sentiment, 
                    score: $score, timestamp: $ts, keywords: $keywords
                })
                MERGE (c)-[:GAVE_FEEDBACK]->(f)
                MERGE (f)-[:TAGGED_TO]->(b)
                SET c.sentiment = $sentiment
            """, 
            phone=phone, booth_id=booth_id, fid=feedback_id, 
            text=text, sentiment=ai_data['sentiment'], 
            score=ai_data['score'], ts=timestamp, keywords=ai_data['keywords'])

            return {
                "status": "Success",
                "citizen_name": citizen_name,
                "booth_id": booth_id,
                "feedback_id": feedback_id,
                **ai_data
            }

    except Exception as exc:
        if isinstance(exc, HTTPException): raise exc
        raise HTTPException(status_code=500, detail=f"System Error: {exc}")

# def _classify_and_save(feedback_id: str, text: str, language: str):
#     prompt = (
#         f"You are a government sentiment analysis system for Karnataka.\n"
#         f"Analyze this citizen feedback and respond ONLY with valid JSON (no markdown).\n\n"
#         f"Feedback: \"{text}\"\nLanguage: {language}\n\n"
#         f"Respond ONLY with:\n"
#         f'{{\"sentiment\": \"positive\" or \"negative\" or \"neutral\", '
#         f'\"score\": 0.0-1.0, \"keywords\": [\"up to 3 key issues in English\"], '
#         f'\"summary\": \"one sentence in English\"}}'
#     )
#     try:
#         client = get_genai()
#         response = client.models.generate_content(model="gemini-1.5-flash", contents=prompt)
#         raw    = response.text.strip().replace("```json", "").replace("```", "").strip()
#         result = json.loads(raw)
#         with get_driver().session() as session:
#             session.run("""
#                 MATCH (f:Feedback {feedback_id: $id})
#                 SET f.sentiment = $sentiment, f.score = $score,
#                     f.keywords  = $keywords,  f.summary = $summary
#             """,
#                 id=feedback_id,
#                 sentiment=result.get("sentiment", "neutral"),
#                 score=float(result.get("score", 0.5)),
#                 keywords=result.get("keywords", []),
#                 summary=result.get("summary", ""),
#             )
#         log.info("Sentiment — %s: %s (%.2f)", feedback_id,
#                  result.get("sentiment"), result.get("score", 0.5))
#     except Exception as exc:
#         log.warning("Sentiment classification failed for %s: %s", feedback_id, exc)


# @app.post("/api/feedback/analyze", tags=["Sentiment"])
# def analyze_feedback(request: FeedbackRequest):
#     prompt = (
#         f"You are a government sentiment analysis system for Karnataka.\n"
#         f"Analyze this citizen feedback and respond ONLY with valid JSON (no markdown).\n"
#         f"Feedback: \"{request.feedback_text}\"\n"
#         f'Respond ONLY with: {{\"sentiment\": \"positive\" or \"negative\" or \"neutral\", '
#         f'\"score\": 0.0-1.0, \"keywords\": [\"up to 3 key issues in English\"], '
#         f'\"summary\": \"one sentence in English\"}}'
#     )
#     try:
#         client = get_genai()
#         response = client.models.generate_content(model="gemini-1.5-pro-latest", contents=prompt)
#         raw = response.text.strip().replace("```json", "").replace("```", "").strip()
#         result = json.loads(raw)
#         return {"status": "Analysis Complete", **result}
#     except Exception as exc:
#         raise HTTPException(status_code=503, detail=f"AI service unavailable: {exc}") from exc


@app.get("/api/sentiment/live-feed", tags=["Sentiment"])
def live_feed():
    query = """
        MATCH (f:Feedback)-[:TAGGED_TO]->(b:Booth)
        WHERE f.sentiment IS NOT NULL
        RETURN f.text AS text, f.sentiment AS sentiment, f.timestamp AS timestamp, b.booth_id AS booth_id
        ORDER BY f.timestamp DESC
        LIMIT 5
    """
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query)]
        return {"feed": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
@app.get("/api/sentiment/booth/{booth_id}", tags=["Sentiment"])
def booth_sentiment(booth_id: str):
    # Updated query to use :TAGGED_TO as per your storage logic
    query = """
        MATCH (f:Feedback)-[:TAGGED_TO]->(b:Booth {booth_id: $booth_id})
        WHERE f.sentiment IS NOT NULL
        RETURN count(f) AS total, 
               avg(f.score) AS avg_score,
               sum(CASE WHEN f.sentiment='positive' THEN 1 ELSE 0 END) AS positive,
               sum(CASE WHEN f.sentiment='negative' THEN 1 ELSE 0 END) AS negative,
               sum(CASE WHEN f.sentiment='neutral' THEN 1 ELSE 0 END) AS neutral,
               collect(f.keywords) AS keyword_lists,
               collect({
                   text: f.text, 
                   sentiment: f.sentiment, 
                   score: f.score, 
                   ts: f.timestamp
               })[0..10] AS recent
    """
    try:
        with get_driver().session() as session:
            row = session.run(query, booth_id=booth_id).single()

        if not row or row["total"] == 0:
            return {
                "booth_id": booth_id, "total": 0, "avg_score": 0.5,
                "positive": 0, "negative": 0, "neutral": 0,
                "top_keywords": [], "recent": []
            }

        # Process keywords from the list of lists
        from collections import Counter
        # keyword_lists is a list of lists (e.g., [['water', 'roads'], ['power']])
        all_kw = [kw for sub in (row["keyword_lists"] or []) for kw in (sub or []) if kw]
        top_kw = [k for k, _ in Counter(all_kw).most_common(5)]

        return {
            "booth_id": booth_id,
            "total": row["total"],
            "avg_score": round(float(row["avg_score"] or 0.5), 2),
            "positive": row["positive"],
            "negative": row["negative"],
            "neutral": row["neutral"],
            "top_keywords": top_kw,
            "recent": row["recent"],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/api/sentiment/trends", tags=["Sentiment"])
def get_sentiment_trends():
    query = """
        MATCH (f:Feedback)
        UNWIND f.keywords AS issue
        RETURN issue, count(issue) AS frequency, avg(f.score) AS impact
        ORDER BY frequency DESC
        LIMIT 5
    """
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query)]
        return {"trends": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/api/sentiment/heatmap", tags=["Sentiment"])
def sentiment_heatmap(district: str = Query("all")):
    dist_clause = "" if district == "all" else "WHERE b.district = $district"
    
    # We use (f:Feedback)-[:TAGGED_TO]->(b) to match your POST route
    # Moving the sentiment check inside the OPTIONAL MATCH pattern 
    # ensures booths with 0 feedback still show up in the grid.
    # FIX: split feedback aggregation from citizen/scheme aggregation
    # The original query caused a Cartesian product between feedback rows and
    # citizen-scheme rows, inflating negative_count (1 feedback × 88 rows = 88).
    # Solution: aggregate feedback first in a subquery, then join citizen/scheme counts separately.
    query = f"""
        MATCH (b:Booth) {dist_clause}

        // ── Step 1: aggregate feedback ONLY (isolated, no citizen join) ──
        OPTIONAL MATCH (f:Feedback)-[:TAGGED_TO]->(b)
        WITH b,
             count(DISTINCT CASE WHEN f IS NULL OR f.sentiment = 'pending' THEN null ELSE f END) AS feedback_count,
             avg(CASE WHEN f.sentiment <> 'pending' AND f IS NOT NULL THEN f.score ELSE null END) AS avg_score,
             sum(CASE WHEN f.sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count

        // ── Step 2: aggregate citizens and schemes SEPARATELY (no feedback join) ──
        OPTIONAL MATCH (c:Citizen)-[:ASSIGNED_TO]->(b)
        WITH b, feedback_count, avg_score, negative_count,
             count(DISTINCT c) AS citizen_count

        OPTIONAL MATCH (c2:Citizen)-[:ASSIGNED_TO]->(b)
        OPTIONAL MATCH (c2)-[:POTENTIAL_ELIGIBILITY]->(ps:Scheme)
        OPTIONAL MATCH (c2)-[:ENROLLED_IN]->(es:Scheme)
        WITH b, feedback_count, avg_score, negative_count, citizen_count,
             count(DISTINCT ps) AS eligible_slots,
             count(DISTINCT es) AS enrolled_slots

        RETURN b.booth_id AS booth_id, b.district AS district,
               feedback_count, avg_score, negative_count,
               citizen_count, eligible_slots, enrolled_slots,
               CASE WHEN (eligible_slots + enrolled_slots) > 0
                    THEN round(100.0 * enrolled_slots / (eligible_slots + enrolled_slots) * 10) / 10
                    ELSE 0.0 END AS saturation_pct
        ORDER BY booth_id ASC
    """
    
    params = {} if district == "all" else {"district": district}
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
            
        return {"booths": [{
            "booth_id":       r["booth_id"],
            "district":       r["district"],
            "feedback_count": r["feedback_count"] or 0,
            "avg_score":      round(float(r["avg_score"] or 0.5), 2),
            "negative_count": r["negative_count"] or 0,
            "citizen_count":  r["citizen_count"] or 0,
            "saturation_pct": round(float(r["saturation_pct"] or 0), 1),
            "alert":          (r["negative_count"] or 0) >= 3,
        } for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/api/sentiment/constituency", tags=["Sentiment"])
def constituency_sentiment(district: str = Query("all")):
    dist_clause = "" if district == "all" else "WHERE b.district = $district"
    query = f"""
        MATCH (b:Booth) {dist_clause}
        OPTIONAL MATCH (b)-[:HAS_FEEDBACK]->(f:Feedback)
        WHERE f.sentiment <> 'pending'
        RETURN b.district AS district,
               count(DISTINCT b) AS booth_count,
               count(f) AS total_feedback,
               avg(f.score) AS avg_score,
               sum(CASE WHEN f.sentiment='positive' THEN 1 ELSE 0 END) AS positive,
               sum(CASE WHEN f.sentiment='negative' THEN 1 ELSE 0 END) AS negative
        ORDER BY avg_score ASC
    """
    params = {} if district == "all" else {"district": district}
    try:
        with get_driver().session() as session:
            rows = [dict(r) for r in session.run(query, **params)]
        return {"constituencies": [{
            "district":       r["district"],
            "booth_count":    r["booth_count"],
            "total_feedback": r["total_feedback"] or 0,
            "avg_score":      round(float(r["avg_score"] or 0.5), 2),
            "positive":       r["positive"] or 0,
            "negative":       r["negative"] or 0,
            "alert":          (r["negative"] or 0) > (r["positive"] or 0),
        } for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ══════════════════════════════════════════════════════════════════════════════
# SEED — demo/hackathon convenience
# ══════════════════════════════════════════════════════════════════════════════

# @app.post("/api/seed/demo", tags=["Admin"])
# def seed_demo_data():
#     try:
#         with get_driver().session() as session:
#             session.run("MATCH (n) DETACH DELETE n")

#             for bid, dist in [
#                 ("B001","Bengaluru Urban"),("B002","Bengaluru Urban"),
#                 ("B003","Mysuru"),("B004","Mysuru"),
#                 ("B005","Dharwad"),("B006","Dharwad"),
#             ]:
#                 session.run("CREATE (:Booth {booth_id:$bid, district:$dist, name:$name})",
#                             bid=bid, dist=dist, name=f"Booth {bid}")

#             for s in [
#                 "PM Kisan Samman Nidhi","Ayushman Bharat","Mudra Loan (PMMY)",
#                 "Ujjwala Yojana","Post-Matric Scholarship","PM Awas Yojana",
#                 "Old Age Pension","Divyang Sahara Yojna","PM SVANidhi",
#             ]:
#                 session.run("CREATE (:Scheme {name:$name})", name=s)

#             segment_schemes = {
#                 "Youth":       ["Post-Matric Scholarship","Mudra Loan (PMMY)","PM SVANidhi"],
#                 "Farmer":      ["PM Kisan Samman Nidhi","Ayushman Bharat","PM Awas Yojana"],
#                 "Women":       ["Ujjwala Yojana","Ayushman Bharat","PM Awas Yojana"],
#                 "Businessman": ["Mudra Loan (PMMY)","PM SVANidhi","Ayushman Bharat"],
#                 "Senior":      ["Old Age Pension","Ayushman Bharat"],
#                 "Disabled":    ["Divyang Sahara Yojna","Ayushman Bharat"],
#             }
#             for cid, name, phone, age, occ, seg, bid, dist in [
#                 ("C001","Raju Kumar",    "9876543210",25,"Student",   "Youth",      "B001","Bengaluru Urban"),
#                 ("C002","Kamala Devi",   "9876543211",45,"Farmer",    "Farmer",     "B001","Bengaluru Urban"),
#                 ("C003","Priya Sharma",  "9876543212",32,"Housewife", "Women",      "B002","Bengaluru Urban"),
#                 ("C004","Venkat Reddy",  "9876543213",55,"Merchant",  "Businessman","B002","Bengaluru Urban"),
#                 ("C005","Ramaiah",       "9876543214",68,"Retired",   "Senior",     "B003","Mysuru"),
#                 ("C006","Suma Bai",      "9876543215",38,"Farmer",    "Farmer",     "B003","Mysuru"),
#                 ("C007","Arjun Naik",    "9876543216",22,"Student",   "Youth",      "B004","Mysuru"),
#                 ("C008","Geetha",        "9876543217",41,"Weaver",    "Women",      "B004","Mysuru"),
#                 ("C009","Basavraj",      "9876543218",72,"Farmer",    "Senior",     "B005","Dharwad"),
#                 ("C010","Savitha Patil", "9876543219",29,"Teacher",   "Women",      "B005","Dharwad"),
#                 ("C011","Imran Khan",    "9876543220",35,"Shopkeeper","Businessman","B006","Dharwad"),
#                 ("C012","Lakshmi",       "9876543221",50,"Farmer",    "Farmer",     "B006","Dharwad"),
#             ]:
#                 session.run("""
#                     CREATE (c:Citizen {
#                         citizen_id:$cid, name:$name, phone:$phone,
#                         age:$age, occupation:$occ, segment:$seg, district:$dist
#                     })
#                     WITH c
#                     MATCH (b:Booth {booth_id:$bid})
#                     MERGE (c)-[:ASSIGNED_TO]->(b)
#                 """, cid=cid, name=name, phone=phone, age=age,
#                      occ=occ, seg=seg, bid=bid, dist=dist)
#                 for scheme in segment_schemes.get(seg, []):
#                     session.run("""
#                         MATCH (c:Citizen {citizen_id:$cid})
#                         MATCH (s:Scheme {name:$scheme})
#                         MERGE (c)-[:POTENTIAL_ELIGIBILITY]->(s)
#                     """, cid=cid, scheme=scheme)
#                 eligible = segment_schemes.get(seg, [])
#                 if eligible:
#                     session.run("""
#                         MATCH (c:Citizen {citizen_id:$cid})
#                         MATCH (s:Scheme {name:$scheme})
#                         MERGE (c)-[:ENROLLED_IN]->(s)
#                     """, cid=cid, scheme=eligible[0])

#             # Seed 5 demo workers
#             for wid, wname, wphone, bid in [
#                 ("W001","Raju Naik",   "9000000001","B001"),
#                 ("W002","Suma Bai",    "9000000002","B002"),
#                 ("W003","Arjun Reddy", "9000000003","B003"),
#                 ("W004","Kavitha Rao", "9000000004","B004"),
#                 ("W005","Venkat Patil","9000000005","B005"),
#             ]:
#                 session.run("""
#                     MERGE (w:Worker {worker_id:$wid})
#                     SET w.name=$wname, w.phone=$wphone
#                     WITH w
#                     MATCH (b:Booth {booth_id:$bid})
#                     MERGE (w)-[:MANAGES]->(b)
#                 """, wid=wid, wname=wname, wphone=wphone, bid=bid)

#             for gid, gname, bid in [
#                 ("G001","MG Road Gali","B001"),
#                 ("G002","Siddapura Street","B002"),
#                 ("G003","Chamundi Nagar Lane","B003"),
#             ]:
#                 session.run("""
#                     MATCH (b:Booth {booth_id:$bid})
#                     CREATE (g:Gali {gali_id:$gid, gali_name:$gname})
#                     MERGE (g)-[:COVERS]->(b)
#                 """, gid=gid, gname=gname, bid=bid)

#             session.run("""
#                 MATCH (c:Citizen {citizen_id:'C001'}) MATCH (g:Gali {gali_id:'G001'})
#                 MERGE (c)-[:LOCATED_IN]->(g)
#             """)
#             session.run("""
#                 MATCH (c:Citizen {citizen_id:'C002'}) MATCH (g:Gali {gali_id:'G001'})
#                 MERGE (c)-[:LOCATED_IN]->(g)
#             """)

#         return {"status": "success", "booths": 6, "citizens": 12,
#                 "schemes": 9, "galis": 3, "workers": 5}
#     except Exception as exc:
#         raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.post("/api/admin/seed")
async def seed_from_csv():
    """
    Seeds Memgraph from voters_data.csv.
    Graph schema used:
      (:Citizen)-[:ASSIGNED_TO]->(:Booth)
      (:Citizen)-[:LOCATED_IN]->(:Gali)
      (:Gali)-[:COVERS]->(:Booth)
      (:Citizen)-[:ENROLLED_IN]->(:Scheme)
      (:Citizen)-[:POTENTIAL_ELIGIBILITY]->(:Scheme)

    All CSV columns stored on :Citizen node.
    """
    csv_path = "voters_data.csv"
    counts = {"citizens": 0, "booths": set(), "galis": set(), "schemes": set(), "workers": set()}

    with open(csv_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with get_driver().session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        log.info("Graph cleared — seeding %d rows", len(rows))

        # ── 1. Create Booth nodes ──────────────────────────────
        booth_data = {}
        for row in rows:
            bid = row["booth_id"]
            if bid not in booth_data:
                booth_data[bid] = row["district"]
        for bid, dist in booth_data.items():
            session.run(
                "MERGE (:Booth {booth_id: $bid, district: $dist})",
                bid=bid, dist=dist,
            )
            counts["booths"].add(bid)

        # ── 2. Create Gali nodes + COVERS relationship ─────────
        gali_data = {}
        for row in rows:
            gid = row["gali_id"]
            if gid not in gali_data:
                gali_data[gid] = {"name": row["gali_name"], "booth_id": row["booth_id"]}
        for gid, ginfo in gali_data.items():
            session.run(
                """
                MERGE (g:Gali {gali_id: $gid})
                  ON CREATE SET g.gali_name = $gname
                WITH g
                MATCH (b:Booth {booth_id: $bid})
                MERGE (g)-[:COVERS]->(b)
                """,
                gid=gid, gname=ginfo["name"], bid=ginfo["booth_id"],
            )
            counts["galis"].add(gid)

        # ── 3. Create Scheme nodes ─────────────────────────────
        all_schemes = set()
        for row in rows:
            for s in row["eligible_schemes"].split("|"):
                s = s.strip()
                if s:
                    all_schemes.add(s)
        for scheme in all_schemes:
            session.run("MERGE (:Scheme {name: $name})", name=scheme)
            counts["schemes"].add(scheme)

        # ── 4. Create Citizen nodes + all relationships ─────────
        for row in rows:
            # Store ALL csv columns on Citizen node
            session.run(
                """
                CREATE (c:Citizen {
                    epic_number:        $epic,
                    name:               $name,
                    age:                toInteger($age),
                    gender:             $gender,
                    phone:              $phone,
                    caste_category:     $caste,
                    monthly_income:     toFloat($income),
                    land_holding_acres: toFloat($land),
                    occupation:         $occupation,
                    aadhaar_linked:     $aadhaar_linked,
                    has_lpg:            $has_lpg,
                    has_bank_account:   $has_bank_account,
                    has_pucca_house:    $has_pucca_house,
                    is_income_taxpayer: $is_taxpayer,
                    pension_amount:     toFloat($pension),
                    ration_card:        $ration_card,
                    loan_defaulter:     $loan_defaulter,
                    is_floating_node:   $floating
                })
                WITH c
                MATCH (b:Booth {booth_id: $bid})
                MERGE (c)-[:ASSIGNED_TO]->(b)
                WITH c
                MATCH (g:Gali {gali_id: $gid})
                MERGE (c)-[:LOCATED_IN]->(g)
                """,
                epic=row["epic_number"],
                name=row["name"],
                age=row["age"] or "0",
                gender=row["gender"],
                phone=row["phone"],
                caste=row["caste_category"],
                income=row["monthly_income"] or "0",
                land=row["land_holding_acres"] or "0",
                occupation=row["occupation"],
                aadhaar_linked=row["aadhaar_linked"],
                has_lpg=row["has_lpg"],
                has_bank_account=row["has_bank_account"],
                has_pucca_house=row["has_pucca_house"],
                is_taxpayer=row["is_income_taxpayer"],
                pension=row["pension_amount"] or "0",
                ration_card=row["ration_card"],
                loan_defaulter=row["loan_defaulter"],
                floating=row["is_floating_node"],
                bid=row["booth_id"],
                gid=row["gali_id"],
            )
            counts["citizens"] += 1

            # ENROLLED_IN relationships
            for scheme in row["enrolled_schemes"].split("|"):
                scheme = scheme.strip()
                if scheme:
                    session.run(
                        """
                        MATCH (c:Citizen {epic_number: $epic})
                        MATCH (s:Scheme {name: $scheme})
                        MERGE (c)-[:ENROLLED_IN]->(s)
                        """,
                        epic=row["epic_number"], scheme=scheme,
                    )

            # POTENTIAL_ELIGIBILITY relationships (all eligible, enrolled or not)
            for scheme in row["eligible_schemes"].split("|"):
                scheme = scheme.strip()
                if scheme:
                    session.run(
                        """
                        MATCH (c:Citizen {epic_number: $epic})
                        MATCH (s:Scheme {name: $scheme})
                        MERGE (c)-[:POTENTIAL_ELIGIBILITY]->(s)
                        """,
                        epic=row["epic_number"], scheme=scheme,
                    )

        # ── 5. Create Worker nodes + MANAGES relationships ────────
        # CSV has no worker data — one Worker per Booth, generated deterministically.
        WORKER_NAMES = [
            "Rajesh Kumar", "Sunita Devi", "Mahesh Patil", "Kavitha Rao",
            "Venkatesh Naik", "Anita Sharma", "Suresh Gowda", "Priya Bai",
            "Ramesh Reddy", "Meena Kumari",
        ]
        WORKER_PHONES = [
            "9900000001", "9900000002", "9900000003", "9900000004", "9900000005",
            "9900000006", "9900000007", "9900000008", "9900000009", "9900000010",
        ]
        booth_list = sorted(counts["booths"])
        for i, bid in enumerate(booth_list):
            wid   = f"W{str(i+1).zfill(3)}"
            wname = WORKER_NAMES[i % len(WORKER_NAMES)]
            wphone= WORKER_PHONES[i % len(WORKER_PHONES)]
            dist  = booth_data.get(bid, "Raichur")
            session.run(
                """
                MERGE (w:Worker {worker_id: $wid})
                  ON CREATE SET w.name=$name, w.phone=$phone, w.district=$dist
                  ON MATCH  SET w.name=$name, w.phone=$phone, w.district=$dist
                WITH w
                MATCH (b:Booth {booth_id: $bid})
                MERGE (w)-[:MANAGES]->(b)
                """,
                wid=wid, name=wname, phone=wphone, bid=bid, dist=dist,
            )
            counts["workers"].add(wid)
        log.info("Seeded %d Worker nodes", len(counts["workers"]))

    log.info(
        "Seed complete: %d citizens, %d booths, %d galis, %d schemes, %d workers",
        counts["citizens"], len(counts["booths"]),
        len(counts["galis"]), len(counts["schemes"]), len(counts["workers"]),
    )

    return {
        "status":   "success",
        "citizens": counts["citizens"],
        "booths":   len(counts["booths"]),
        "galis":    len(counts["galis"]),
        "schemes":  len(counts["schemes"]),
        "workers":  len(counts["workers"]),
        "message":  "Graph seeded from voters_data.csv",
    }
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard:app", host="0.0.0.0", port=7000, reload=True)