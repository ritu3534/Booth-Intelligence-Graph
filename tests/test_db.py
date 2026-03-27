from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

# Use the variables from your .env file
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

def test_connection():
    try:
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
            print("✅ SUCCESS: JanSetu AI is now connected to Neo4j Aura.")
            
            # Optional: Run a tiny query to be 100% sure
            result = driver.execute_query("RETURN 'Connection Verified' as msg")
            print(f"📡 Database Response: {result.records[0]['msg']}")
            
    except Exception as e:
        print(f"❌ CONNECTION FAILED: {str(e)}")

if __name__ == "__main__":
    test_connection()