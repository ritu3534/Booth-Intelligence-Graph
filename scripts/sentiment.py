"""
JanSetu AI — Sentiment Analysis Engine (Memgraph Optimized)
scripts/sentiment.py

Run: python scripts/sentiment.py --seed    (seeds 20 sample feedback first)
     python scripts/sentiment.py            (classifies pending feedback only)
"""

import os, sys, uuid, time, datetime, json
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv(override=True)

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Updated driver to handle local Memgraph auth
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

from google import genai as _genai
_client = _genai.Client(api_key=GEMINI_API_KEY)


def gemini_classify(text: str) -> dict:
    prompt = f"""You are a sentiment analysis system for a government welfare scheme in Karnataka.
Analyse this citizen feedback. Respond ONLY with valid JSON, no markdown.

Feedback: {text}

JSON format:
{{
  "sentiment": "positive" or "negative" or "neutral",
  "score": 0.0 to 1.0,
  "language": "Kannada" or "Telugu" or "Hindi" or "English" or "Other",
  "issues": ["key issue 1", "key issue 2"]
}}"""
    for attempt in range(3):
        try:
            response = _client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"): raw = raw[4:]
            return json.loads(raw.strip())
        except Exception as exc:
            if attempt < 2: time.sleep(2 ** attempt)
            else:
                print(f"   Gemini failed: {exc}")
                return {"sentiment": "neutral", "score": 0.5, "language": "Unknown", "issues": []}


SAMPLE_FEEDBACK = [
    {"text": "PM Kisan scheme helped my family. Money received directly. Thank you.", "booth_id": "B001", "district": "Raichur", "phone": "9876543201"},
    {"text": "Ayushman Bharat card helped during my mother's surgery. Very good scheme.", "booth_id": "B001", "district": "Raichur", "phone": "9876543202"},
    {"text": "ನಮ್ಮ ಬೂತ್ ಅಧಿಕಾರಿಗಳು ತುಂಬಾ ಸಹಾಯ ಮಾಡಿದ್ದಾರೆ. ಉಜ್ವಲ ಯೋಜನೆ ಸಿಕ್ಕಿತು.", "booth_id": "B002", "district": "Raichur", "phone": "9876543203"},
    {"text": "Jan Dhan account opened easily. Staff was very cooperative.", "booth_id": "B003", "district": "Raichur", "phone": "9876543204"},
    {"text": "Kisan Credit Card process was smooth. Got loan for farming.", "booth_id": "B003", "district": "Raichur", "phone": "9876543205"},
    {"text": "Applied for PM Awas Yojana 6 months ago but no response. Very disappointed.", "booth_id": "B004", "district": "Raichur", "phone": "9876543206"},
    {"text": "ಬೂತ್ ಅಧಿಕಾರಿ ಸಿಗುವುದಿಲ್ಲ. ಮೂರು ಬಾರಿ ಬಂದೆ ಆದರೆ ಕಚೇರಿ ಮುಚ್ಚಿತ್ತು.", "booth_id": "B005", "district": "Raichur", "phone": "9876543207"},
    {"text": "Mudra loan application rejected without reason. No explanation given.", "booth_id": "B005", "district": "Raichur", "phone": "9876543208"},
    {"text": "Very slow process. Waited 3 hours for simple Aadhaar verification.", "booth_id": "B006", "district": "Gulbarga", "phone": "9876543209"},
    {"text": "PM Fasal Bima claim not settled after crop damage. No one responds to calls.", "booth_id": "B006", "district": "Gulbarga", "phone": "9876543210"},
    {"text": "Process is okay but takes time. Should be faster.", "booth_id": "B007", "district": "Gulbarga", "phone": "9876543211"},
    {"text": "Some schemes are good but awareness is less in our village.", "booth_id": "B007", "district": "Gulbarga", "phone": "9876543212"},
    {"text": "ಯೋಜನೆಗಳ ಬಗ್ಗೆ ಮಾಹಿತಿ ಕಡಿಮೆ. ಇನ್ನಷ್ಟು ಜಾಗೃತಿ ಅಗತ್ಯ.", "booth_id": "B008", "district": "Gulbarga", "phone": "9876543213"},
    {"text": "Documents required are too many. Simplify the process please.", "booth_id": "B008", "district": "Gulbarga", "phone": "9876543214"},
    {"text": "Old age pension is helpful but amount should be increased.", "booth_id": "B009", "district": "Gulbarga", "phone": "9876543215"},
    {"text": "Ujjwala gas cylinder scheme is excellent. My wife is very happy.", "booth_id": "B009", "district": "Gulbarga", "phone": "9876543216"},
    {"text": "Post Matric scholarship not received for 2 years. Studies are suffering.", "booth_id": "B010", "district": "Raichur", "phone": "9876543217"},
    {"text": "Very good initiative. Digital payments for farmers is a great step.", "booth_id": "B010", "district": "Raichur", "phone": "9876543218"},
    {"text": "Staff behavior is rude. We are treated poorly when we come for help.", "booth_id": "B011", "district": "Raichur", "phone": "9876543219"},
    {"text": "Scheme is good but implementation is poor in our booth area.", "booth_id": "B011", "district": "Raichur", "phone": "9876543220"},
]


def seed_feedback(session):
    print("Seeding 20 sample feedback records...\n")
    for fb in SAMPLE_FEEDBACK:
        fid = str(uuid.uuid4())[:8].upper()
        ts  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        session.run("""
            MERGE (b:Booth {booth_id: $booth_id})
            ON CREATE SET b.district = $district
            CREATE (f:Feedback {
                feedback_id: $fid, text: $text, phone: $phone,
                booth_id: $booth_id, district: $district,
                timestamp: $ts, sentiment: 'pending',
                score: 0.5, language: 'Unknown', issues: []
            })
            MERGE (f)-[:FROM_BOOTH]->(b)
        """, fid=fid, text=fb["text"], phone=fb["phone"],
            booth_id=fb["booth_id"], district=fb["district"], ts=ts)
        print(f"   {fid} — Booth {fb['booth_id']}")
    print(f"\nSeeded {len(SAMPLE_FEEDBACK)} feedback records")


def classify_all(session):
    rows = list(session.run("""
        MATCH (f:Feedback)
        WHERE f.sentiment = 'pending' OR f.sentiment IS NULL
        RETURN f.feedback_id AS id, f.text AS text
        ORDER BY f.feedback_id
    """))
    if not rows:
        print("No pending feedback to classify."); return
    print(f"Classifying {len(rows)} feedback records...\n")
    for i, row in enumerate(rows):
        print(f"   [{i+1}/{len(rows)}] {row['id']} — {row['text'][:55]}...")
        result = gemini_classify(row["text"])
        session.run("""
            MATCH (f:Feedback {feedback_id: $id})
            SET f.sentiment = $sentiment, f.score = $score,
                f.language  = $language, f.issues = $issues
        """, id=row["id"],
            sentiment=result.get("sentiment", "neutral"),
            score=float(result.get("score", 0.5)),
            language=result.get("language", "Unknown"),
            issues=result.get("issues", []),
        )
        print(f"      → {result.get('sentiment'):8} score:{result.get('score',0.5):.2f}  [{result.get('language','?')}]")
        time.sleep(1)
    print(f"\nClassified {len(rows)} records")


def verify(session):
    print("\nSentiment Distribution:")
    # FIX: Memgraph requires single-argument round(). Using (x * 100) / 100.0 trick.
    query_dist = """
    MATCH (f:Feedback) 
    RETURN f.sentiment AS s, count(f) AS n, 
           round(avg(f.score) * 100) / 100.0 AS avg 
    ORDER BY s
    """
    for r in session.run(query_dist):
        print(f"   {r['s']:10} {r['n']:3} records   avg:{r['avg']}")

    print("\nWorst Performing Booths (by avg sentiment score):")
    # FIX: Applying the same rounding trick here
    query_booths = """
    MATCH (f:Feedback)-[:FROM_BOOTH]->(b:Booth)
    RETURN b.booth_id AS booth, 
           round(avg(f.score) * 100) / 100.0 AS avg_score, 
           count(f) AS total
    ORDER BY avg_score ASC LIMIT 5
    """
    for r in session.run(query_booths):
        print(f"   Booth {r['booth']}  score:{r['avg_score']}  ({r['total']} feedback)")

if __name__ == "__main__":
    print("JanSetu AI — Sentiment Analysis Engine\n")
    try:
        with driver.session() as session:
            if "--seed" in sys.argv:
                seed_feedback(session)
                print()
            classify_all(session)
            verify(session)
    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        driver.close()
    print("\nDone. Open http://localhost:8080 → Sentiment tab.")