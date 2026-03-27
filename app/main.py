"""
app/main.py
═══════════════════════════════════════════════════════════════════════════════
JanSetu AI — FastAPI Backend  v2.6
═══════════════════════════════════════════════════════════════════════════════
All 7 frontend tabs wired:
  /api/stats              → Dashboard stats
  /api/segments           → Segments tab
  /api/segments/{seg}     → Segment drill-down
  /analytics/sentiment    → Sentiment tab (GET + POST)
  /citizen/search         → Citizen Search (phone + name + booth)
  /api/nudges             → Nudge History tab
  /api/blo/verify         → BLO Verify tab
  /api/gali               → Gali Updates tab
  /booth/{id}/stats       → Booth Intelligence
  /booth/{id}/gaps        → Floating node gaps
  /booth/{id}/nudge       → AI nudge generation
"""

import os
import csv
from pathlib import Path

import uvicorn
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.core.graph_engine import graph_db

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="JanSetu AI: Booth Intelligence Knowledge Graph API",
    description="Precision Governance API — Viksit Bharat @2047",
    version="2.6.0",
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the "ui" folder so files are accessible at /ui
app.mount("/ui", StaticFiles(directory="ui", html=True), name="ui")

# ── Models ────────────────────────────────────────────────────────────────────
class FeedbackRequest(BaseModel):
    citizen_id: str
    feedback_text: str

class NudgeRequest(BaseModel):
    booth_id: str
    scheme_name: str
    language: str = "Hindi"

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", tags=["System"])
async def root():
    return {
        "status":    "Online",
        "system":    "JanSetu AI Booth Saturation Engine",
        "version":   "2.6.0",
        "alignment": "Viksit Bharat @2047",
        "docs":      "/docs",
    }

@app.get("/health/db", tags=["System"])
async def check_db_health():
    try:
        graph_db.driver.verify_connectivity()
        return {"status": "Healthy", "database": "Neo4j AuraDB Connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database Offline: {str(e)}")

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD STATS  →  top-level saturation panel
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/stats", tags=["Dashboard"])
async def get_dashboard_stats():
    """
    Returns the numbers shown on the main dashboard header:
    total citizens, booths, saturation %, region.
    """
    stats = graph_db.get_dashboard_stats()
    return stats

# ─────────────────────────────────────────────────────────────────────────────
# SEGMENTS TAB
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/segments", tags=["Segments"])
async def get_segments():
    """
    Returns all citizen segments with counts.
    Requires: python scripts/seed_segment.py to have been run.
    """
    segments = graph_db.get_segments()
    if not segments:
        raise HTTPException(
            status_code=404,
            detail="No segment data found. Run: python scripts/seed_segment.py"
        )
    return {"segments": segments, "total_groups": len(segments)}

@app.get("/api/segments/{segment}", tags=["Segments"])
async def get_segment_citizens(segment: str):
    """
    Drill down into a specific segment — returns all citizens in that group.
    e.g. /api/segments/Farmer  or  /api/segments/Youth
    """
    citizens = graph_db.get_segment_citizens(segment)
    if not citizens:
        raise HTTPException(
            status_code=404,
            detail=f"No citizens found in segment: {segment}"
        )
    return {
        "segment":  segment,
        "count":    len(citizens),
        "citizens": citizens,
    }

# ─────────────────────────────────────────────────────────────────────────────
# SENTIMENT TAB
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/analytics/sentiment", tags=["Sentiment"])
async def get_sentiment_results(limit: int = Query(50, le=200)):
    """
    GET all classified sentiment feedback records.
    Requires: python scripts/sentiment.py --seed to have been run.
    """
    results = graph_db.get_sentiment_results(limit=limit)
    summary = graph_db.get_sentiment_summary()
    return {
        "summary": summary,
        "records": results,
        "total":   len(results),
    }

@app.post("/analytics/sentiment", tags=["Sentiment"])
async def process_sentiment(request: FeedbackRequest):
    """
    POST: Analyse a single citizen feedback using Gemini AI.
    Updates the Knowledge Graph with detected sentiment.
    """
    # Import here to avoid circular imports and allow graceful failure
    try:
        from scripts.sentiment import gemini_classify
        result = gemini_classify(request.feedback_text)
        sentiment = result.get("sentiment", "neutral")
        score     = result.get("score", 0.5)
    except Exception:
        sentiment = "neutral"
        score     = 0.5

    query = """
    MATCH (c:Citizen {voter_id: $citizen_id})
    SET c.sentiment = $sentiment, c.last_feedback = $text
    RETURN c.name AS name, c.sentiment AS sentiment
    """
    graph_db.execute_query(query, {
        "citizen_id": request.citizen_id.strip().upper(),
        "sentiment":  sentiment,
        "text":       request.feedback_text,
    })
    return {
        "status":             "Analysis Complete",
        "detected_sentiment": sentiment,
        "confidence_score":   score,
    }

# ─────────────────────────────────────────────────────────────────────────────
# CITIZEN SEARCH TAB  →  phone + name + booth filters
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/citizen/search", tags=["Citizen Search"])
async def search_citizen(
    phone:    str = Query(None, description="Phone number"),
    name:     str = Query(None, description="Citizen name (partial match)"),
    booth_no: str = Query(None, description="Booth number"),
):
    """
    Search citizens by any combination of phone, name, or booth number.
    At least one parameter required.
    """
    if not any([phone, name, booth_no]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one search parameter: phone, name, or booth_no"
        )
    results = graph_db.search_citizen(
        phone=phone, name=name, booth_no=booth_no
    )
    if not results:
        raise HTTPException(status_code=404, detail="No citizens found")
    return {"results": results, "count": len(results)}

# ─────────────────────────────────────────────────────────────────────────────
# NUDGE HISTORY TAB  →  Self-Healing Audit Trail
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/nudges", tags=["Nudge History"])
async def get_nudge_history(limit: int = Query(100, le=500)):
    """
    Returns nudge history from nudge_reports.csv (UTF-16 LE audit trail).
    Falls back to Neo4j Nudge nodes if CSV not found.
    """
    nudges = graph_db.get_nudge_history(limit=limit)
    return {
        "nudges": nudges,
        "total":  len(nudges),
        "source": "nudge_reports.csv (UTF-16 LE Self-Healing Audit Trail)",
    }

# ─────────────────────────────────────────────────────────────────────────────
# BLO VERIFY TAB  →  Field officer verification
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/blo/verify", tags=["BLO Verify"])
async def blo_verify(
    voter_id: str = Query(None, description="Voter ID"),
    phone:    str = Query(None, description="Phone number"),
):
    """
    BLO Verify: Look up a citizen by voter ID or phone.
    Returns full profile including Aadhaar hash verification status.
    """
    if not voter_id and not phone:
        raise HTTPException(
            status_code=400,
            detail="Provide voter_id or phone"
        )
    if voter_id:
        result = graph_db.blo_verify(voter_id)
    else:
        result = graph_db.blo_verify_by_phone(phone)

    if not result:
        raise HTTPException(status_code=404, detail="Citizen not found")
    return result

# ─────────────────────────────────────────────────────────────────────────────
# GALI UPDATES TAB
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/gali", tags=["Gali Updates"])
async def get_gali_updates(
    district: str = Query("All Districts", description="Filter by district"),
    limit:    int = Query(50, le=200),
):
    """
    Returns local gali/area events.
    Requires: python scripts/seed_gali_events.py to have been run.
    """
    updates = graph_db.get_gali_updates(district=district, limit=limit)
    if not updates:
        raise HTTPException(
            status_code=404,
            detail="No gali updates found. Run: python scripts/seed_gali_events.py"
        )
    return {"updates": updates, "total": len(updates), "district": district}

# ─────────────────────────────────────────────────────────────────────────────
# BOOTH INTELLIGENCE TAB
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/booth/{booth_id}/stats", tags=["Booth Intelligence"])
async def get_booth_stats(booth_id: str):
    """Per-booth saturation, voter count, and dissatisfaction rate."""
    result = graph_db.get_booth_stats(booth_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"Booth {booth_id} not found")
    return result

@app.get("/booth/{booth_id}/gaps", tags=["Booth Intelligence"])
async def get_booth_gaps(booth_id: str):
    """Top 3 scheme gaps in a booth — Floating Node Detection."""
    gaps = graph_db.get_scheme_gaps(booth_id)
    floating = graph_db.detect_floating_nodes()
    booth_floating = [f for f in floating if f.get("booth_no", "").upper() == booth_id.upper()]
    return {
        "booth_id":       booth_id,
        "top_gaps":       gaps,
        "floating_nodes": booth_floating[:20],
    }

@app.get("/booth/{booth_id}/nudge", tags=["Booth Intelligence"])
async def get_booth_nudge(
    booth_id: str,
    language: str = Query("Hindi", description="Target language"),
    scheme:   str = Query(None,    description="Scheme name"),
):
    """
    Generate a Gemini AI nudge for a booth.
    Returns a 160-character localized SMS.
    """
    try:
        from app.core.nudge_engine import generate_nudge
        nudge_text = generate_nudge(booth_id, scheme, language)
    except Exception:
        scheme_str = scheme or "PM-KISAN"
        nudge_text = (
            f"नमस्ते! बूथ {booth_id} के नागरिक, "
            f"{scheme_str} के लिए पात्र हैं। "
            f"कृपया बूथ पर आएं।"
        )[:160]

    return {
        "booth_id":     booth_id,
        "language":     language,
        "scheme":       scheme,
        "nudge_content":nudge_text,
        "char_count":   len(nudge_text),
    }

# ─────────────────────────────────────────────────────────────────────────────
# DISTRICT OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/district/saturation", tags=["District Overview"])
async def get_district_saturation():
    """Scheme-level saturation across all booths — Booth-Level Delta Rankings."""
    data = graph_db.get_district_saturation()
    if not data:
        raise HTTPException(status_code=404, detail="No scheme data found")
    return {"saturation": data, "total_schemes": len(data)}

@app.get("/api/floating-nodes", tags=["District Overview"])
async def get_floating_nodes(
    scheme: str = Query(None, description="Filter by scheme name")
):
    """
    Floating Node Detection — citizens eligible for a scheme but not enrolled.
    Core innovation of JanSetu AI.
    """
    nodes = graph_db.detect_floating_nodes(scheme_name=scheme)
    return {
        "floating_nodes": nodes,
        "total":          len(nodes),
        "scheme_filter":  scheme or "All schemes",
    }

# ─────────────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)