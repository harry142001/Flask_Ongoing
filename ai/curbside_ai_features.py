"""
Curbside AI Features — descriptions + natural-language search, powered by InsForge.

SETUP (do this in your real Curbside repo, not a sandbox):
  1. npx @insforge/cli link --api-base-url <your-host> --api-key <your-key>   (once per machine)
  2. npx @insforge/cli ai setup
       -> writes OPENROUTER_API_KEY to .env.local. Copy that one line into your real .env file.
  3. npx @insforge/cli db connection-string
       -> gives you a Postgres connection URL. Set it as INSFORGE_DB_URL in .env.
  4. pip install openai psycopg2-binary
  5. Run init_embeddings_table() once to set up the vector table in InsForge's Postgres.
  6. Confirm the embedding model name InsForge exposes for your project
     (check the InsForge dashboard) and set EMBEDDING_MODEL + EMBEDDING_DIM to match —
     the values below (openai/text-embedding-3-small, 1536) are a common default but must
     be verified against your actual project config.

ARCHITECTURE:
  - Your SQLite property DB stays the source of truth for all property data. Nothing here
    touches or replaces it.
  - InsForge's Postgres is used ONLY to store embeddings (property_key + vector), so you can
    do fast natural-language similarity search. Search returns property_keys, which you then
    look up in your own SQLite DB as normal.
  - AI calls (descriptions + embeddings) go straight to OpenRouter using the key InsForge
    issued you — no separate InsForge AI gateway URL involved.
"""

import os
import json
import sqlite3
from dotenv import load_dotenv
from openai import OpenAI
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()  # reads your .env file and loads its values so os.environ.get() can see them

# ---------------------------------------------------------------------------
# Config — fill these in from the setup commands above
# ---------------------------------------------------------------------------

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")  # from `ai setup` -> .env.local
INSFORGE_DB_URL = os.environ.get("INSFORGE_DB_URL", "")        # from `db connection-string`

CHAT_MODEL = "anthropic/claude-sonnet-4.5"      # any OpenRouter model string
EMBEDDING_MODEL = "openai/text-embedding-3-small"  # CONFIRM this matches what your project exposes
EMBEDDING_DIM = 1536                               # must match the model's output dimension

SQLITE_DB_PATH = os.environ.get("CURBSIDE_SQLITE_PATH", "data/Database1.db")

ai_client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)


# ---------------------------------------------------------------------------
# One-time setup: create the embeddings table in InsForge's Postgres
# ---------------------------------------------------------------------------

def init_embeddings_table():
    """Run once. Enables pgvector and creates the table that stores search vectors."""
    with psycopg2.connect(INSFORGE_DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS property_embeddings (
                    property_id TEXT PRIMARY KEY,
                    embedding vector({EMBEDDING_DIM}),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
            """)
            # Speeds up nearest-neighbor search once you have more than a few hundred rows
            cur.execute("""
                CREATE INDEX IF NOT EXISTS property_embeddings_idx
                ON property_embeddings USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)
        conn.commit()
    print("property_embeddings table ready.")


# ---------------------------------------------------------------------------
# Feature 1: AI-generated descriptions
# ---------------------------------------------------------------------------

def build_property_prompt(prop: dict) -> str:
    """Turn a property row (dict of column -> value) into a prompt for the model."""
    fields = "\n".join(f"- {k}: {v}" for k, v in prop.items() if v not in (None, ""))
    return (
        "Write a warm, factual real estate listing description (3-4 sentences) "
        "for a property with these details. Do not invent details not listed. "
        "Do not use exclamation points or generic filler phrases.\n\n"
        f"{fields}"
    )


def generate_description(prop: dict) -> str:
    """Calls the InsForge AI gateway to write a description for one property."""
    resp = ai_client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[{"role": "user", "content": build_property_prompt(prop)}],
        max_tokens=300,
    )
    return resp.choices[0].message.content.strip()


def backfill_descriptions(sqlite_path: str = SQLITE_DB_PATH, limit: int = None):
    """
    Finds properties missing a description, generates one, writes it back.
    Keyed on property_key (mls_number isn't reliably filled in, so we use a
    hash of address+city+postal instead — see backfill_property_key.py).
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = "SELECT * FROM properties WHERE description IS NULL OR description = ''"
    if limit:
        query += f" LIMIT {limit}"
    rows = cur.execute(query).fetchall()

    for row in rows:
        prop = dict(row)
        key = prop.get("property_key")
        if not key:
            print(f"Skipping row with no property_key (run backfill_property_key.py first): {prop.get('address')}")
            continue
        try:
            description = generate_description(prop)
            conn.execute(
                "UPDATE properties SET description = ? WHERE property_key = ?",
                (description, key),
            )
            conn.commit()
            print(f"Updated description for {key}")
        except Exception as e:
            print(f"Failed on {key}: {e}")

    conn.close()


# ---------------------------------------------------------------------------
# Feature 2: Natural-language search
# ---------------------------------------------------------------------------

def generate_embedding(text: str) -> list:
    """Calls the InsForge AI gateway to embed a piece of text."""
    resp = ai_client.embeddings.create(model=EMBEDDING_MODEL, input=text)
    return resp.data[0].embedding


def sync_embeddings(sqlite_path: str = SQLITE_DB_PATH, limit: int = None):
    """
    Generates/updates embeddings for every property and upserts them into
    InsForge's Postgres. Re-run this whenever properties are added or edited
    (e.g. on a schedule, or right after your data ingestion job).
    """
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = """
        SELECT property_key, description, address, city, state, postal,
               property_type, bedrooms, bathrooms, square_footage
        FROM properties
        WHERE description IS NOT NULL AND property_key IS NOT NULL
    """
    if limit:
        query += f" LIMIT {limit}"
    rows = cur.execute(query).fetchall()
    conn.close()

    rows_to_upsert = []
    for row in rows:
        prop = dict(row)
        text_for_embedding = (
            f"{prop.get('address', '')}, {prop.get('city', '')}, {prop.get('state', '')} {prop.get('postal', '')}. "
            f"{prop.get('property_type', '')}, {prop.get('bedrooms', '')} bed, {prop.get('bathrooms', '')} bath, "
            f"{prop.get('square_footage', '')} sqft. {prop.get('description', '')}"
        )
        try:
            embedding = generate_embedding(text_for_embedding)
            rows_to_upsert.append((prop["property_key"], embedding))
        except Exception as e:
            print(f"Failed embedding {prop['property_key']}: {e}")

    if not rows_to_upsert:
        return

    with psycopg2.connect(INSFORGE_DB_URL) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            execute_values(
                pg_cur,
                """
                INSERT INTO property_embeddings (property_id, embedding, updated_at)
                VALUES %s
                ON CONFLICT (property_id) DO UPDATE
                SET embedding = EXCLUDED.embedding, updated_at = now()
                """,
                rows_to_upsert,
                template="(%s, %s, now())",
            )
        pg_conn.commit()

    print(f"Synced {len(rows_to_upsert)} property embeddings.")


def search_properties(query_text: str, sqlite_path: str = SQLITE_DB_PATH, top_k: int = 20) -> list:
    """
    Natural-language property search.
    1. Embed the user's query.
    2. Find nearest property_ids in InsForge Postgres.
    3. Pull the full rows back from your own SQLite DB, in similarity order.
    """
    query_embedding = generate_embedding(query_text)

    with psycopg2.connect(INSFORGE_DB_URL) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_cur.execute(
                """
                SELECT property_id
                FROM property_embeddings
                ORDER BY embedding <-> %s::vector
                LIMIT %s
                """,
                (query_embedding, top_k),
            )
            ordered_ids = [r[0] for r in pg_cur.fetchall()]

    if not ordered_ids:
        return []

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in ordered_ids)
    rows = conn.execute(
        f"SELECT * FROM properties WHERE property_key IN ({placeholders})", ordered_ids
    ).fetchall()
    conn.close()

    # Preserve the similarity-ranked order from Postgres
    rows_by_id = {row["property_key"]: dict(row) for row in rows}
    return [rows_by_id[i] for i in ordered_ids if i in rows_by_id]


# ---------------------------------------------------------------------------
# Example Flask route — wire this into your existing Curbside app
# ---------------------------------------------------------------------------
#
# from flask import request, jsonify
#
# @app.route("/search")
# def search():
#     q = request.args.get("q", "")
#     if not q:
#         return jsonify([])
#     results = search_properties(q)
#     return jsonify(results)