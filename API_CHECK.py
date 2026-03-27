from google import genai

client = genai.Client(api_key="AIzaSyD4ZuMtskqXMPxxLj-PVwsmxuM1Plkn1d0")

for m in client.models.list():
    print(m.name)