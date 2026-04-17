from google import genai

client = genai.Client(api_key="AIzaSyBZmh3R6z_0HxZDX2_vrO-suBeFMwaflGk")

for m in client.models.list():
    print(m.name)