"""
rag_engine.py — JanSetu AI  RAG Pipeline
==========================================
Uses:
  - ChromaDB       : local vector store (no cloud needed)
  - Google Gemini  : text-embedding-004 for embeddings
                     gemini-1.5-flash for answer generation
  - LangChain      : document splitting utilities

Data indexed:
  1. scheme_knowledge.json — all 10 government schemes (bilingual)
  2. voters_data.csv       — booth/district statistics summary
  3. Live booth data       — pulled from Memgraph at index time

Endpoints used by dashboard.py:
  POST /api/rag/query   → ask anything about your data
  POST /api/rag/index   → (re)build the vector store
  GET  /api/rag/status  → how many docs are indexed
"""

import os, json, logging, threading
from pathlib import Path
from typing import Optional

import chromadb
from chromadb.config import Settings

log = logging.getLogger("jansetu.rag")

# ── Config ────────────────────────────────────────────────────────────────────
CHROMA_DIR       = Path("rag_store")          # local persistent vector store
COLLECTION_NAME  = "jansetu_knowledge"
SCHEME_FILE      = Path("scheme_knowledge.json")
CSV_PATH         = Path("voters_data.csv")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "")

_chroma_client: Optional[chromadb.PersistentClient] = None
_collection    = None
_rag_lock      = threading.Lock()
_index_stats: dict = {"total_docs": 0, "indexed": False, "sources": []}


# ── ChromaDB init ─────────────────────────────────────────────────────────────
def _get_collection():
    global _chroma_client, _collection
    if _collection is not None:
        return _collection
    CHROMA_DIR.mkdir(exist_ok=True)
    _chroma_client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    _collection = _chroma_client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )
    return _collection


# ── Gemini Embedding ──────────────────────────────────────────────────────────
def _embed(texts: list[str]) -> list[list[float]]:
    """Call Gemini gemini-embedding-001 to get vectors for a list of texts."""
    from google import genai
    client = genai.Client(api_key=GEMINI_API_KEY)
    result = client.models.embed_content(
        model="models/gemini-embedding-001",
        contents=texts,
    )
    # result.embeddings is a list of ContentEmbedding objects
    return [e.values for e in result.embeddings]


# ── Document builders ─────────────────────────────────────────────────────────
def _build_scheme_docs() -> list[dict]:
    """Convert scheme_knowledge.json into indexable chunks."""
    if not SCHEME_FILE.exists():
        log.warning("scheme_knowledge.json not found — skipping scheme docs")
        return []

    with open(SCHEME_FILE, encoding="utf-8") as f:
        data = json.load(f)

    docs = []
    for s in data.get("schemes", []):
        # One rich chunk per scheme
        text = (
            f"Scheme: {s['name']} ({s['short_name']})\n"
            f"Category: {s['category']}\n"
            f"Ministry: {s['ministry']}\n"
            f"Benefit: {s['benefit']}\n"
            f"Eligibility: {s['eligibility']}\n"
            f"Not Eligible: {s['not_eligible']}\n"
            f"How to Apply: {s['how_to_apply']}\n"
            f"Documents Required: {', '.join(s['documents'])}\n"
            f"Hindi Summary: {s['hindi_summary']}\n"
            f"Keywords: {', '.join(s['keywords'])}"
        )
        docs.append({
            "id":     f"scheme_{s['short_name'].replace(' ','_')}",
            "text":   text,
            "source": "scheme_knowledge",
            "name":   s["name"],
        })
    log.info("Built %d scheme documents", len(docs))
    return docs


def _build_csv_booth_docs() -> list[dict]:
    """Summarise voters_data.csv per booth for RAG context."""
    if not CSV_PATH.exists():
        return []

    import pandas as pd
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    docs = []
    for booth_id, grp in df.groupby("booth_id"):
        total    = len(grp)
        floating = grp["is_floating_node"].astype(str).str.lower().isin(["true","1"]).sum()
        district = grp["district"].iloc[0]
        avg_inc  = round(grp["monthly_income"].mean(), 0)
        farmers  = (grp["occupation"] == "Farmer").sum()
        students = (grp["occupation"] == "Student").sum()
        no_bank  = (~grp["has_bank_account"].astype(str).str.lower().isin(["true","1"])).sum()
        no_aadhaar = (~grp["aadhaar_linked"].astype(str).str.lower().isin(["true","1"])).sum()

        text = (
            f"Booth ID: {booth_id} | District: {district}\n"
            f"Total Citizens: {total}\n"
            f"Floating (Swing) Voters: {floating} ({round(floating/total*100,1)}%)\n"
            f"Committed Voters: {total-floating} ({round((total-floating)/total*100,1)}%)\n"
            f"Average Monthly Income: Rs. {avg_inc}\n"
            f"Farmers in booth: {farmers} | Students: {students}\n"
            f"No Bank Account: {no_bank} | No Aadhaar: {no_aadhaar}\n"
            f"This booth is in {district} district of Karnataka."
        )
        docs.append({
            "id":       f"booth_{booth_id}",
            "text":     text,
            "source":   "voters_csv",
            "booth_id": booth_id,
        })

    log.info("Built %d booth documents from CSV", len(docs))
    return docs


def _build_general_docs() -> list[dict]:
    """Static general knowledge about the JanSetu project."""
    texts = [
        (
            "JanSetu AI is a Booth Intelligence System for Karnataka, India. "
            "It helps political workers identify welfare scheme gaps, floating (swing) voters, "
            "and citizen needs across booths in districts like Raichur, Bellary, Koppal, Yadgir, Bidar. "
            "The system uses a Knowledge Graph (Memgraph/Neo4j) to store citizen-scheme-booth relationships."
        ),
        (
            "Floating voter (is_floating_node = True) means an undecided or swing voter. "
            "These voters have not committed to any party and can be influenced by welfare scheme delivery. "
            "Common reasons for being floating: low income, welfare gap (eligible but not enrolled in schemes), "
            "poor government satisfaction, young age, or no participation in last 3 elections."
        ),
        (
            "Welfare Gap means a citizen is eligible for a government scheme but is NOT enrolled in it. "
            "For example, if a farmer qualifies for PM-Kisan but has not received benefits, that is a welfare gap. "
            "High welfare gap in a booth means booth workers should prioritize scheme enrollment drives there."
        ),
        (
            "Scheme Saturation Percentage = (Citizens Enrolled) / (Citizens Eligible) * 100. "
            "A booth with 50% saturation means only half the eligible citizens are getting scheme benefits. "
            "Target is to increase saturation to above 80% in all booths."
        ),
    ]
    return [
        {"id": f"general_{i}", "text": t, "source": "general_info"}
        for i, t in enumerate(texts)
    ]


# ── Index Builder ─────────────────────────────────────────────────────────────
def build_index() -> dict:
    """
    Build (or rebuild) the ChromaDB vector store.
    Embeds all scheme docs + booth CSV summaries + general info.
    """
    global _index_stats

    col = _get_collection()

    # Clear existing data
    existing = col.get()
    if existing["ids"]:
        col.delete(ids=existing["ids"])
        log.info("Cleared %d existing vectors", len(existing["ids"]))

    # Gather all documents
    scheme_docs  = _build_scheme_docs()
    booth_docs   = _build_csv_booth_docs()
    general_docs = _build_general_docs()
    all_docs     = scheme_docs + booth_docs + general_docs

    if not all_docs:
        return {"error": "No documents to index"}

    ids      = [d["id"]   for d in all_docs]
    texts    = [d["text"] for d in all_docs]
    metadata = [{k: v for k, v in d.items() if k not in ("id", "text")} for d in all_docs]

    # Embed in batches of 10 (Gemini API limit per request)
    BATCH = 10
    all_embeddings = []
    for i in range(0, len(texts), BATCH):
        batch = texts[i:i+BATCH]
        log.info("Embedding batch %d/%d (%d docs)…", i//BATCH+1, -(-len(texts)//BATCH), len(batch))
        embs = _embed(batch)
        all_embeddings.extend(embs)

    col.add(ids=ids, documents=texts, embeddings=all_embeddings, metadatas=metadata)

    with _rag_lock:
        _index_stats = {
            "indexed": True,
            "total_docs": len(all_docs),
            "sources": {
                "schemes":  len(scheme_docs),
                "booths":   len(booth_docs),
                "general":  len(general_docs),
            },
        }

    log.info("RAG index built: %d total vectors", len(all_docs))
    return _index_stats


# ── Retriever ─────────────────────────────────────────────────────────────────
def retrieve(query: str, k: int = 5) -> list[dict]:
    """Embed the query and return top-k matching document chunks."""
    col = _get_collection()
    if col.count() == 0:
        return []

    query_emb = _embed([query])[0]
    results = col.query(
        query_embeddings=[query_emb],
        n_results=min(k, col.count()),
        include=["documents", "metadatas", "distances"],
    )

    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        chunks.append({
            "text":       doc,
            "source":     meta.get("source", "unknown"),
            "relevance":  round(1 - dist, 3),   # cosine similarity
            "metadata":   meta,
        })
    return chunks


# ── Generator ─────────────────────────────────────────────────────────────────
def generate_answer(question: str, chunks: list[dict]) -> str:
    """Feed retrieved chunks + question to Gemini and get a grounded answer."""
    from google import genai

    if not chunks:
        return "Maafi chahta hoon, is sawaal ka jawab dene ke liye mere paas koi relevant information nahi mili. Pehle /api/rag/index chalao."

    context = "\n\n---\n\n".join(
        f"[Source: {c['source']} | Relevance: {c['relevance']}]\n{c['text']}"
        for c in chunks
    )

    prompt = f"""You are JanSetu AI Assistant — an expert on Indian government welfare schemes and Karnataka booth intelligence data.

INSTRUCTIONS:
- Answer ONLY based on the provided context below
- If the answer is not in the context, say so clearly
- Answer in the same language the user asked (Hindi/Hinglish → answer in Hindi/Hinglish, English → answer in English)
- Be specific with numbers, eligibility criteria, and booth statistics
- Keep answers concise but complete

CONTEXT FROM JanSetu DATABASE:
{context}

USER QUESTION: {question}

ANSWER:"""

    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=prompt,
    )
    return response.text.strip()


# ── Status ────────────────────────────────────────────────────────────────────
def get_status() -> dict:
    col = _get_collection()
    return {
        **_index_stats,
        "vector_count": col.count(),
        "store_path":   str(CHROMA_DIR.absolute()),
    }
