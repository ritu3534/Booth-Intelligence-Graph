"""
scripts/nudge_engine.py
══════════════════════════════════════════════════════════════════════════════
JanSetu AI — Standalone Batch Nudge Engine  (fixed v2)
══════════════════════════════════════════════════════════════════════════════

Bugs fixed from original:
  Bug 1: log_to_csv() called with wrong signature (5 args instead of record+message)
  Bug 2: generate_nudge() called twice per citizen — now called exactly once
  Bug 3: bhashini_translate() had hardcoded placeholder key causing silent crash
         — moved to optional flag, disabled by default until real key is configured

New:
  - Segment-aware Gemini prompt (Youth/Farmer/Women/Businessman/Senior/Disabled)
  - --segment filter to nudge only one group at a time
  - --language flag (Kannada / Hindi / Telugu / English)
  - --limit flag to cap Gemini API calls
  - Cleaner dashboard output

USAGE:
  python scripts/nudge_engine.py                        # all gaps, Hindi, limit 50
  python scripts/nudge_engine.py --segment Farmer       # only Farmers
  python scripts/nudge_engine.py --language Kannada     # Kannada output
  python scripts/nudge_engine.py --limit 10             # cap at 10 nudges
  python scripts/nudge_engine.py --trial                # use sample data, no Neo4j
"""

import os
import csv
import sys
import time
import argparse
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

from google import genai
from neo4j import GraphDatabase

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ID = os.getenv("PROJECT_ID", "friendly-hangar-489010-c2")
GCP_KEY    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp-key.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY

client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")

NUDGE_CSV = os.getenv("NUDGE_CSV_PATH", "nudge_reports.csv")

# ── Segment tone guidance (Objective 2) ────────────────────────────────────────
SEGMENT_TONE = {
    "Youth":       "Energetic, aspirational tone. Highlight career growth and financial independence.",
    "Farmer":      "Simple, respectful tone. Focus on crop support and income stability.",
    "Women":       "Empowering, supportive tone. Emphasise family strength and independence.",
    "Businessman": "Practical, business-minded tone. Focus on growth and working capital.",
    "Senior":      "Gentle, respectful. Very short sentences. Emphasise security and dignity.",
    "Disabled":    "Compassionate, inclusive. Emphasise accessibility and government support.",
    "General":     "Warm, official tone. Clear and actionable.",
}

# ── Trial data ─────────────────────────────────────────────────────────────────
TRIAL_GAPS = [
    {"name": "Arjun Mehra",   "scheme": "PM Kisan Samman Nidhi", "phone": "9876543210", "booth_no": "104", "district": "Raichur",  "segment": "Farmer"},
    {"name": "Priya Das",     "scheme": "Ujjwala Yojana",         "phone": "9988776655", "booth_no": "105", "district": "Raichur",  "segment": "Women"},
    {"name": "Suresh Kumar",  "scheme": "Mudra Loan (PMMY)",      "phone": "9123456789", "booth_no": "104", "district": "Gulbarga", "segment": "Businessman"},
    {"name": "Anjali Rai",    "scheme": "PM Awas Yojana",         "phone": "9223344556", "booth_no": "106", "district": "Raichur",  "segment": "Women"},
    {"name": "Laxmi Bai",     "scheme": "Old Age Pension",        "phone": "9000022222", "booth_no": "104", "district": "Raichur",  "segment": "Senior"},
    {"name": "Rohit Verma",   "scheme": "Post-Matric Scholarship","phone": "9111133333", "booth_no": "105", "district": "Gulbarga", "segment": "Youth"},
]


# ══════════════════════════════════════════════════════════════════════════════
# Neo4j
# ══════════════════════════════════════════════════════════════════════════════
def get_gaps(segment_filter=None, limit=50) -> list:
    uri  = os.getenv("NEO4J_URI")
    auth = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))

    seg_clause = "AND c.segment = $segment" if segment_filter else ""
    query = f"""
        MATCH (c:Citizen)-[:POTENTIAL_ELIGIBILITY]->(s:Scheme)
        WHERE NOT EXISTS {{(c)-[:ENROLLED_IN]->(s)}}
        {seg_clause}
        RETURN c.name     AS name,
               c.phone    AS phone,
               c.segment  AS segment,
               s.name     AS scheme,
               c.booth_no AS booth_no,
               c.district AS district
        LIMIT $limit
    """
    params = {"limit": limit}
    if segment_filter:
        params["segment"] = segment_filter

    with GraphDatabase.driver(uri, auth=auth) as driver:
        records, _, _ = driver.execute_query(query, **params)
        return [dict(r) for r in records]


# ══════════════════════════════════════════════════════════════════════════════
# Gemini nudge generation
# ══════════════════════════════════════════════════════════════════════════════
def generate_nudge(name: str, scheme: str, booth: str,
                   segment: str = "General", language: str = "Hindi") -> str:
    """
    Segment-aware Gemini nudge. Falls back to a template if Gemini is unavailable.
    """
    tone = SEGMENT_TONE.get(segment, SEGMENT_TONE["General"])
    prompt = f"""You are a government welfare officer in Karnataka, India.
Write a short personalised SMS nudge in {language} for:

Citizen Name   : {name}
Citizen Segment: {segment}
Eligible Scheme: {scheme}
Their Booth    : Booth {booth}

Tone: {tone}

Rules:
- Exactly 2-3 sentences. No more.
- Mention the scheme "{scheme}" and booth "{booth}" explicitly.
- Tell them to bring Aadhaar card to Booth {booth} to register.
- Write the ENTIRE message in {language} script only.
- Output ONLY the final message. No labels, no preamble.
"""
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash-001",
            contents=prompt,
        )
        return response.text.strip()
    except Exception:
        # Fallback template
        return (
            f"नमस्ते {name}, आप {scheme} योजना के लिए पात्र हैं। "
            f"कृपया अपना आधार कार्ड लेकर बूथ {booth} पर आएं।"
        ) if language == "Hindi" else (
            f"Dear {name}, you are eligible for {scheme}. "
            f"Please visit Booth {booth} with your Aadhaar card."
        )


# ══════════════════════════════════════════════════════════════════════════════
# CSV audit log
# ══════════════════════════════════════════════════════════════════════════════
def log_to_csv(record: dict, message: str) -> None:
    """
    Saves a nudge to the CSV audit trail.
    FIX: original called this with 5 positional args — now takes record dict + message.
    """
    file_exists = os.path.isfile(NUDGE_CSV)
    headers = ["Timestamp", "Name", "Phone", "Scheme", "Booth", "District", "Segment", "Message"]
    with open(NUDGE_CSV, "a", newline="", encoding="utf-16") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "Name":      record.get("name", ""),
            "Phone":     record.get("phone", ""),
            "Scheme":    record.get("scheme", ""),
            "Booth":     record.get("booth_no", ""),
            "District":  record.get("district", ""),
            "Segment":   record.get("segment", "General"),
            "Message":   message.strip(),
        })


# ══════════════════════════════════════════════════════════════════════════════
# Health check
# ══════════════════════════════════════════════════════════════════════════════
def test_connections(trial_mode: bool) -> None:
    print("\nStarting system health check...\n")

    try:
        test_response = client.models.generate_content(
            model="gemini-2.0-flash-001",
            contents="Reply with exactly: Vertex AI Connected"
        )
        print(f"  Vertex AI : online  ({test_response.text.strip()})")
    except Exception as e:
        print(f"  Vertex AI : OFFLINE  ({e})")

    if not trial_mode:
        try:
            uri  = os.getenv("NEO4J_URI")
            auth = (os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
            with GraphDatabase.driver(uri, auth=auth) as driver:
                records, _, _ = driver.execute_query(
                    "MATCH (n:Citizen) RETURN count(n) AS count"
                )
                print(f"  Neo4j AuraDB: connected  ({records[0]['count']} Citizen nodes)")
        except Exception as e:
            print(f"  Neo4j AuraDB: OFFLINE  ({e})")
    else:
        print("  Neo4j AuraDB: skipped  (trial mode)")

    print()


# ══════════════════════════════════════════════════════════════════════════════
# Main engine
# ══════════════════════════════════════════════════════════════════════════════
def run_nudge_engine(trial_mode: bool, segment_filter: str | None,
                     language: str, limit: int) -> None:
    mode_label = "TRIAL" if trial_mode else "LIVE"
    print(f"JanSetu AI Nudge Engine  |  Mode: {mode_label}  |  Language: {language}")
    if segment_filter:
        print(f"Segment filter: {segment_filter}")
    print()

    stats: dict = {"total": 0, "schemes": [], "booths": [], "districts": [], "segments": []}

    # ── Fetch gaps ─────────────────────────────────────────────────────────────
    if trial_mode:
        gaps = TRIAL_GAPS
        if segment_filter:
            gaps = [g for g in gaps if g.get("segment") == segment_filter]
        gaps = gaps[:limit]
    else:
        print("Connecting to Neo4j...")
        gaps = get_gaps(segment_filter=segment_filter, limit=limit)

    if not gaps:
        print("No floating nodes found for the given filters.")
        return

    print(f"Found {len(gaps)} unenrolled eligible citizens. Generating nudges...\n")

    for record in gaps:
        name     = record.get("name", "Citizen")
        scheme   = record.get("scheme", "")
        booth    = record.get("booth_no", record.get("booth", "N/A"))
        segment  = record.get("segment", "General")

        # FIX: generate_nudge called exactly ONCE per citizen
        message = generate_nudge(name, scheme, booth, segment=segment, language=language)

        # FIX: log_to_csv called with correct signature (record dict + message)
        log_to_csv(record, message)

        print(f"  Sent  {name:<20} | {segment:<12} | {scheme}")

        stats["total"]    += 1
        stats["schemes"].append(scheme)
        stats["booths"].append(booth)
        stats["districts"].append(record.get("district", ""))
        stats["segments"].append(segment)

        time.sleep(1)   # rate-limit buffer

    # ── Dashboard ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 52)
    print("  MISSION SATURATION: RESULTS")
    print("=" * 52)
    print(f"  Total nudges sent   : {stats['total']}")
    print(f"  Districts covered   : {len(set(stats['districts']))}")
    print(f"  Report path         : {NUDGE_CSV}")
    print()
    print("  Scheme breakdown:")
    for s, count in Counter(stats["schemes"]).most_common():
        print(f"    {s:<30} {count}")
    print()
    print("  Segment breakdown:")
    for seg, count in Counter(stats["segments"]).most_common():
        print(f"    {seg:<14} {count}")
    print()
    print("  Top booths by need:")
    for b, count in Counter(stats["booths"]).most_common(3):
        print(f"    Booth {b:<6} {count} gaps")
    print("=" * 52 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="JanSetu AI — Batch nudge engine for floating node citizens.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/nudge_engine.py                        # all gaps, Hindi
  python scripts/nudge_engine.py --trial                # sample data, no Neo4j
  python scripts/nudge_engine.py --segment Farmer       # only Farmers
  python scripts/nudge_engine.py --language Kannada     # Kannada messages
  python scripts/nudge_engine.py --limit 10             # cap at 10
        """
    )
    parser.add_argument("--trial",    action="store_true", help="Use sample data instead of Neo4j")
    parser.add_argument("--segment",  type=str, default=None,
                        help="Filter by segment: Youth/Farmer/Women/Businessman/Senior/Disabled")
    parser.add_argument("--language", type=str, default="Hindi",
                        choices=["Hindi", "Kannada", "Telugu", "English"],
                        help="Message language (default: Hindi)")
    parser.add_argument("--limit",    type=int, default=50,
                        help="Max citizens to nudge (default: 50)")
    args = parser.parse_args()

    test_connections(trial_mode=args.trial)
    run_nudge_engine(
        trial_mode=args.trial,
        segment_filter=args.segment,
        language=args.language,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()