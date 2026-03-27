"""
JanSetu AI — Gali Event Seeder
scripts/seed_gali_events.py

Seeds sample InfraEvents in Neo4j for demo purposes.
In production, BLOs create events via the dashboard UI.

Run: python scripts/seed_gali_events.py
"""

import os
import datetime
import uuid
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j"))
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Sample events — realistic Karnataka infrastructure issues
SAMPLE_EVENTS = [
    {
        "type":        "Road Repair",
        "description": "Pothole-filled road repaired with fresh asphalt. Citizens can now commute safely.",
        "status":      "completed",
        "before_img":  "",
        "after_img":   "",
    },
    {
        "type":        "Street Light",
        "description": "5 new LED streetlights installed. Night safety improved for women walking home.",
        "status":      "completed",
        "before_img":  "",
        "after_img":   "",
    },
    {
        "type":        "Drainage",
        "description": "Blocked drainage cleared. Waterlogging issue resolved before monsoon.",
        "status":      "completed",
        "before_img":  "",
        "after_img":   "",
    },
    {
        "type":        "Water Pipeline",
        "description": "Leaking pipeline fixed. Clean water supply restored to 45 households.",
        "status":      "completed",
        "before_img":  "",
        "after_img":   "",
    },
    {
        "type":        "Road Repair",
        "description": "Road widening work in progress. Expected completion in 2 weeks.",
        "status":      "in_progress",
        "before_img":  "",
        "after_img":   "",
    },
    {
        "type":        "Street Light",
        "description": "Non-functional lights reported. Work order raised with BESCOM.",
        "status":      "pending",
        "before_img":  "",
        "after_img":   "",
    },
]


def seed_events(session):
    # Get first 6 galis from the graph
    galis = list(session.run("""
        MATCH (g:Gali)-[:COVERS]->(b:Booth)
        RETURN g.gali_id AS gali_id, g.gali_name AS gali_name,
               b.booth_id AS booth_id, b.district AS district
        LIMIT 6
    """))

    if not galis:
        print("ERROR: No Gali nodes found. Run load_neo4j.py first.")
        return

    created = 0
    for i, gali in enumerate(galis):
        event = SAMPLE_EVENTS[i % len(SAMPLE_EVENTS)]
        event_id  = str(uuid.uuid4())[:8].upper()
        timestamp = (datetime.datetime.now() - datetime.timedelta(days=i*3)).strftime("%Y-%m-%d %H:%M")

        session.run("""
            MATCH (g:Gali {gali_id: $gali_id})
            CREATE (e:InfraEvent {
                event_id:    $event_id,
                type:        $type,
                description: $description,
                status:      $status,
                before_img:  $before_img,
                after_img:   $after_img,
                timestamp:   $timestamp,
                booth_id:    $booth_id,
                district:    $district
            })
            MERGE (g)-[:HAS_EVENT]->(e)
        """,
            gali_id    = gali["gali_id"],
            event_id   = event_id,
            type       = event["type"],
            description= event["description"],
            status     = event["status"],
            before_img = event["before_img"],
            after_img  = event["after_img"],
            timestamp  = timestamp,
            booth_id   = gali["booth_id"],
            district   = gali["district"],
        )

        print(f"   Created event {event_id}: {event['type']} on {gali['gali_name']} ({event['status']})")
        created += 1

    print(f"\nSeeded {created} InfraEvents across {created} Galis")
    print("Open http://localhost:8080 → Gali Updates tab to see them")


def verify(session):
    result = session.run("""
        MATCH (g:Gali)-[:HAS_EVENT]->(e:InfraEvent)
        RETURN g.gali_name AS street, e.type AS type,
               e.status AS status, e.timestamp AS ts
        ORDER BY e.timestamp DESC
        LIMIT 10
    """)
    print("\nSample events in graph:")
    for r in result:
        print(f"   [{r['status']:12}] {r['type']:20} — {r['street']} ({r['ts']})")


if __name__ == "__main__":
    print("JanSetu AI — Seeding Gali Events...\n")
    with driver.session() as session:
        seed_events(session)
        verify(session)
    driver.close()
    print("\nDone.")