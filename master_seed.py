import pandas as pd
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

def run_final_seed():
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"), 
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    )
    
    with driver.session() as session:
        # Step 1: Clear Database for a clean start
        session.run("MATCH (n) DETACH DELETE n")
        
        # Step 2: Load Voters & Booths
        voters = pd.read_csv('voter_list.csv')
        voter_rows = voters.to_dict('records')
        session.run("""
            UNWIND $rows AS row
            MERGE (b:Booth {booth_id: toString(row.booth_no)})
            MERGE (c:Citizen {voter_id: toString(row.voter_id)})
            SET c.name = row.name, 
                c.age = toInteger(row.age), 
                c.phone = toString(row.phone),
                c.income = row.income_bracket
            MERGE (c)-[:ASSIGNED_TO]->(b)
        """, rows=voter_rows)
        print(f"✅ Created {len(voters)} Citizens and linked them to Booths.")

        # Step 3: Load Enrolled Schemes (Mappings)
        schemes = pd.read_csv('scheme_list.csv')
        scheme_rows = schemes.to_dict('records')
        session.run("""
            UNWIND $rows AS row
            MATCH (c:Citizen {voter_id: toString(row.voter_id)})
            MERGE (s:Scheme {name: row.scheme_name})
            SET s.category = row.category
            MERGE (c)-[:ENROLLED_IN]->(s)
        """, rows=scheme_rows)
        print(f"✅ Mapped {len(schemes)} Scheme enrollments.")

    driver.close()
    print("🚀 Knowledge Graph is now LIVE!")

if __name__ == "__main__":
    run_final_seed()