from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def root():
    return "<h1>JanSetu AI Test</h1><p>If you see this, your server is working!</p>"

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)