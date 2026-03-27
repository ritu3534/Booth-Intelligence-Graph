import os
from neo4j import GraphDatabase
from dotenv import dotenv_values, find_dotenv

# 1. Directly parse the file into a dictionary (skipping os.environ)
dotenv_path = find_dotenv()
config = dotenv_values(dotenv_path)

# 2. Extract values from the dictionary
URI = config.get("NEO4J_URI")
USER = config.get("NEO4J_USERNAME")
PASSWORD = config.get("NEO4J_PASSWORD")

print(f"🔍 Debug: Using file at {dotenv_path}")
print(f"🚀 Debug: URI found: {URI}")

def force_write_test():
    if not URI or not PASSWORD:
        print("❌ ERROR: Still can't find credentials in the .env file!")
        return

    # Initialize driver
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    try:
        with driver.session() as session:
            def create_test_node(tx):
                # Using 'JanSetu' tag for the test node
                return tx.run("MERGE (t:TestNode {project: 'JanSetu AI'}) "
                              "SET t.timestamp = datetime() RETURN t").single()

            record = session.execute_write(create_test_node)
            print(f"✅ Success! Node updated for JanSetu AI at: {record['t']['timestamp']}")
            
            count = session.run("MATCH (t:TestNode) RETURN count(t) as c").single()["c"]
            print(f"📊 Current test nodes in graph: {count}")

    except Exception as e:
        print(f"❌ DATABASE ERROR: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    force_write_test()