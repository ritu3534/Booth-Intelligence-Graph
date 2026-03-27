import os
from google import genai

# Point to your new JSON
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

try:
    client = genai.Client(vertexai=True, project='friendly-hangar-489010-c2', location='us-central1')
    response = client.models.generate_content(
        model="gemini-2.0-flash-001",
        contents="Hello Vertex AI, are you active?"
    )
    print("✅ SUCCESS! Vertex AI is communicating perfectly.")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"❌ CONNECTION FAILED: {e}")