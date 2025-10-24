import os
import json
import sqlite3
import uuid
import requests
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, request, jsonify
from flask_cors import CORS

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not installed, skip loading .env file
    pass

# Debug print for server-level MySQL URI used for SHOW DATABASES checks
try:
    _server_uri = os.getenv("MYSQL_SERVER_URI")

except Exception:
    pass

import warnings
from sqlalchemy import exc as sa_exc

# Suppress SQLAlchemy warnings for unrecognized column types (spatial, binary, etc.)
warnings.filterwarnings('ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*')

from ChatOpenAI import ChatOpenAI, ChatPromptTemplate, StrOutputParser, RunnablePassthrough
# from sql_database import SQLDatabase  # Commented out - using API instead

# API configuration for NoQL
API_BASE_URL = "https://api.zigment.ai"
API_HEADERS = {
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "User-Agent": "PostmanRuntime/7.48.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "x-org-id": "6617aafc195dea3f1dbdd894",
    "zigment-x-api-key": "sk_5b1960bfac64b7e2c7f91f2383a11ff3"
}

# NoQL Direct Prompt for complex query generation
NOQL_DIRECT_PROMPT = """
You are an expert NoQL (SQL-to-Document/NoSQL) query generator.

# OBJECTIVE:
Given the USER QUESTION below, output a VALID NoQL query that best answers the request. STRICTLY use the rules and examples in the SYNTAX REFERENCE below.

# RULES:
- Use ONLY the collections and fields shown in the SCHEMA below (do not invent).
- **CRITICAL: DATE/TIME HANDLING** - For any time/datetime/timestamp field stored as a Unix timestamp (seconds), you MUST convert with TO_DATE(field * 1000) before applying ANY date functions (WEEK, MONTH, YEAR, DAY_OF_WEEK, etc.). 
  Example: WEEK(TO_DATE(created_at_timestamp * 1000)) NOT WEEK(created_at_timestamp)
- Collection names in queries must be lowercase, even if the schema uses uppercase names (e.g., CHAT_HISTORY → chathistories).
- For "per entity" questions (e.g., "messages per conversation"), GROUP BY that entity and show the distribution.
- DO NOT use subqueries with FROM (SELECT...) - NoQL may not support nested queries.
- If user wants an average of aggregated values, use existing aggregate fields when available.
- NEVER output natural language, explanation, or comments—output ONLY the pure NoQL query.

# SYNTAX REFERENCE (examples and summaries):
- SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT, OFFSET (SQL-like)
- COUNT(*), COUNT(field), COUNT(DISTINCT field), AVG(field), SUM(field)
- JOIN ... ON (for relationships; only when fields are not in the main collection)
- Always alias computed fields/aggregates with AS
- Use explicit field and table names only
- For "per entity" questions, GROUP BY the entity and ORDER BY the metric

# Example User Questions and Expected Queries:

Q: "What is the total number of messages per user?"
A: SELECT user_id, COUNT(*) AS total_messages FROM chathistories GROUP BY user_id ORDER BY total_messages DESC LIMIT 20

Q: "What is the average number of messages per conversation?"
A: SELECT contact_id, COUNT(*) AS message_count FROM chathistories GROUP BY contact_id ORDER BY message_count DESC LIMIT 20

Q: "Average total messages across all chat histories"
A: SELECT AVG(total_messages) AS avg_messages FROM chathistories

Q: "Show all contacts created after Jan 1, 2024"
A: SELECT * FROM contacts WHERE created_at_timestamp > 1704067200 LIMIT 50

Q: "Top 5 most active agents by chats"
A: SELECT agent_id, COUNT(*) AS chat_count FROM chathistories GROUP BY agent_id ORDER BY chat_count DESC LIMIT 5

Q: "Weekly contact creations" (or "contacts created per week")
A: SELECT WEEK(TO_DATE(created_at_timestamp * 1000)) AS week_number, COUNT(*) AS contact_count FROM contacts GROUP BY week_number ORDER BY week_number LIMIT 20

Q: "Monthly message count"
A: SELECT MONTH(TO_DATE(created_at * 1000)) AS month, COUNT(*) AS message_count FROM chathistories GROUP BY month ORDER BY month LIMIT 12

Q: "Contacts by day of week"
A: SELECT DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000)) AS day_of_week, COUNT(*) AS contact_count FROM contacts GROUP BY day_of_week ORDER BY day_of_week LIMIT 7

# SCHEMA:
{schema}

# USER QUESTION:
{question}

# OUTPUT: The NoQL query ONLY, no explanation.
"""

# In-memory schema cache to avoid repeated API calls
_SCHEMA_CACHE = {
    "schema": None,
    "timestamp": None,
    "ttl_seconds": 3600  # Cache for 1 hour
}

app = Flask(__name__)
# Enable CORS for your frontend on :3001 by default; override via CORS_ORIGINS env (comma-separated)
origins_env = os.getenv("CORS_ORIGINS")
if origins_env:
    _origins = [o.strip() for o in origins_env.split(",") if o.strip()]
else:
    _origins = [
        "http://192.168.0.193:3001",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ]
CORS(
    app,
    resources={r"/*": {"origins": _origins}},
    supports_credentials=True,
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Accept", "Origin"],
)

# Initialize OpenAI API key
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("ERROR: OPENAI_API_KEY environment variable is not set!")
    print("Please create a .env file with your API key or set the environment variable.")
    exit(1)
os.environ["OPENAI_API_KEY"] = api_key

# Database tracking (kept for compatibility, but not used for queries)
databases = {}

# Simplified registration - just tracks database names
def register_database(name: str, uri: str = None):
    """Register a database by name (API-based, no actual connection needed)."""
    databases[name] = {"name": name, "api_based": True}
    return databases[name]

# ===== JSON Serialization Helpers =====
# Custom JSON encoder for database objects (handles Decimal, etc.)
class DatabaseJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return str(obj)

# Helper function for safe JSON serialization
def safe_json_dumps(obj, **kwargs):
    """JSON dumps with custom encoder for database objects"""
    return json.dumps(obj, cls=DatabaseJSONEncoder, **kwargs)


# ===== NoQL API Helper Functions =====
def simplify_schema_for_noql(full_schema: dict) -> dict:
    """Extract only query-relevant information from the full schema."""
    simplified = {"collections": []}
    
    for obj in full_schema.get("data", []):
        collection = {
            "name": obj.get("object_type"),
            "description": obj.get("description"),
            "fields": []
        }
        
        # Extract only essential field info
        for field in obj.get("base_fields", []) + obj.get("custom_fields", []):
            field_name = field.get("field_key")
            field_type = field.get("field_type")
            
            field_info = {
                "name": field_name,
                "type": field_type
            }
            
            # Detect Unix timestamp fields - be aggressive with detection!
            # Common patterns: created_at, updated_at, timestamp, *_timestamp, *_at
            is_timestamp_field = (
                "timestamp" in field_name.lower() or
                field_name.lower().endswith("_at") or
                field_name.lower() in ["created_at", "updated_at", "deleted_at", "timestamp"] or
                (field_type == "DATETIME" and isinstance(field.get("default_value"), (int, float)))
            )
            
            if is_timestamp_field:
                field_info["storage"] = "unix_epoch_seconds"
                field_info["note"] = "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions"
                field_info["example"] = f"DAY_OF_WEEK(TO_DATE({field_name} * 1000))"
            
            # Include only query-relevant metadata
            if field.get("is_indexed"):
                field_info["indexed"] = True
            if field.get("is_unique"):
                field_info["unique"] = True
            if field.get("is_required"):
                field_info["required"] = True
            if field.get("default_value") is not None:
                field_info["default"] = field.get("default_value")
                
            collection["fields"].append(field_info)
        
        # Include indexes for query optimization hints
        if obj.get("indexes"):
            collection["indexes"] = obj["indexes"]
        
        # Include relationships for JOIN support
        if obj.get("relationships"):
            collection["relationships"] = [
                {
                    "name": rel.get("relationship_name"),
                    "target": rel.get("target_object_type"),
                    "type": rel.get("relationship_type"),
                    "foreign_key": rel.get("foreign_key"),
                    "target_field": rel.get("target_field")
                }
                for rel in obj["relationships"]
            ]
        
        simplified["collections"].append(collection)
    
    return simplified


def fetch_schema_from_api(use_cache: bool = True) -> dict:
    """Fetch schema from API and return simplified version (with caching).
    
    Args:
        use_cache: If True, use cached schema if available and not expired
        
    Returns:
        Simplified schema dictionary
    """
    import time
    
    # Check if we have a valid cached schema
    if use_cache and _SCHEMA_CACHE["schema"] is not None and _SCHEMA_CACHE["timestamp"] is not None:
        age = time.time() - _SCHEMA_CACHE["timestamp"]
        if age < _SCHEMA_CACHE["ttl_seconds"]:
            print(f"📦 Using cached schema (age: {age:.1f}s)")
            return _SCHEMA_CACHE["schema"]
        else:
            print(f"⏰ Schema cache expired (age: {age:.1f}s, TTL: {_SCHEMA_CACHE['ttl_seconds']}s)")
    
    # Fetch fresh schema from API
    url = f"{API_BASE_URL}/schemas/schemaForAllowedCollections"
    
    try:
        print(f"🌐 Fetching schema from API...")
        response = requests.get(url, headers=API_HEADERS, timeout=10)
        response.raise_for_status()
        
        full_data = response.json()
        simplified = simplify_schema_for_noql(full_data)
        
        # DEBUG: Print schema for events collection to verify Unix timestamp detection
        for coll in simplified.get("collections", []):
            if coll.get("name") == "events":
                print(f"🔍 DEBUG: Schema for 'events' collection:")
                for field in coll.get("fields", [])[:10]:  # Show first 10 fields
                    print(f"   - {field.get('name')}: {field.get('type')} {field.get('storage', '')} {field.get('note', '')[:50] if field.get('note') else ''}")
                break
        
        # Update cache
        _SCHEMA_CACHE["schema"] = simplified
        _SCHEMA_CACHE["timestamp"] = time.time()
        print(f"✅ Schema cached (TTL: {_SCHEMA_CACHE['ttl_seconds']}s)")
        
        return simplified
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching schema from API: {e}")
        # If we have an expired cache, return it as fallback
        if _SCHEMA_CACHE["schema"] is not None:
            print(f"⚠️ Using stale cached schema as fallback")
            return _SCHEMA_CACHE["schema"]
        raise


def execute_noql_query(sql_query: str) -> dict:
    """Execute NoQL query via API and return results."""
    url = f"{API_BASE_URL}/reporting/preview"
    
    payload = {
        "sqlText": sql_query,
        "type": "table"
    }
    
    try:
        response = requests.post(url, headers=API_HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error executing query: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        raise

# ===== Conversations (SQLite) =====
SQLITE_PATH = os.getenv("CHAT_SQLITE_PATH", os.path.join(os.path.dirname(__file__), "chat_history.sqlite3"))

def _ensure_sqlite():
    try:
        conn = sqlite3.connect(SQLITE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation (
                id TEXT PRIMARY KEY,
                title TEXT,
                database_name TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS message (
                id TEXT PRIMARY KEY,
                conversation_id TEXT,
                role TEXT,
                content_markdown TEXT,
                charts_json TEXT,
                sql_meta_json TEXT,
                database_name TEXT,
                facts TEXT,
                created_at TEXT,
                FOREIGN KEY(conversation_id) REFERENCES conversation(id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS summary (
                id TEXT PRIMARY KEY,
                conversation_id TEXT,
                content TEXT,
                created_at TEXT,
                FOREIGN KEY(conversation_id) REFERENCES conversation(id)
            )
            """
        )
        
        # Migration: Add database_name and facts columns to existing tables if they don't have them
        try:
            # Check if database_name column exists in conversation table
            cur.execute("PRAGMA table_info(conversation)")
            conv_columns = [row[1] for row in cur.fetchall()]
            if 'database_name' not in conv_columns:
                print("🔧 Adding database_name column to conversation table")
                cur.execute("ALTER TABLE conversation ADD COLUMN database_name TEXT")
                
            # Check if database_name and facts columns exist in message table  
            cur.execute("PRAGMA table_info(message)")
            msg_columns = [row[1] for row in cur.fetchall()]
            if 'database_name' not in msg_columns:
                print("🔧 Adding database_name column to message table")
                cur.execute("ALTER TABLE message ADD COLUMN database_name TEXT")
            if 'facts' not in msg_columns:
                print("🔧 Adding facts column to message table")
                cur.execute("ALTER TABLE message ADD COLUMN facts TEXT")
                
        except Exception as migration_error:
            print(f"Migration warning (non-critical): {migration_error}")
        
        conn.commit()
        print("✅ SQLite database schema updated with database_name columns")
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _now_str():
    return datetime.utcnow().isoformat()

def _gen_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

_ensure_sqlite()

def create_conversation(title: str | None = None, database_name: str | None = None) -> str:
    conv_id = _gen_id("conv")
    ts = _now_str()
    with sqlite3.connect(SQLITE_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO conversation (id, title, database_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (conv_id, title or "New conversation", database_name, ts, ts)
        )
        conn.commit()
    return conv_id

def list_conversations(limit: int = 100):
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT id, title, database_name, created_at, updated_at FROM conversation ORDER BY updated_at DESC LIMIT ?", (limit,))
        return [dict(row) for row in cur.fetchall()]

def add_message(conversation_id: str, role: str, content_markdown: str, charts: list | None = None, sql_meta: dict | None = None, title_hint: str | None = None, database_name: str | None = None, facts: str | None = None):
    msg_id = _gen_id("msg")
    ts = _now_str()
    with sqlite3.connect(SQLITE_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO message (id, conversation_id, role, content_markdown, charts_json, sql_meta_json, database_name, facts, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (msg_id, conversation_id, role, content_markdown or "", json.dumps(charts or []), json.dumps(sql_meta or {}), database_name, facts, ts)
        )
        # Update title on first user message if empty
        if role == "user" and title_hint:
            cur.execute("UPDATE conversation SET title=? WHERE id=? AND (title IS NULL OR title='New conversation')", (title_hint[:80], conversation_id))
        # Update conversation updated_at and database_name
        cur.execute("UPDATE conversation SET updated_at=?, database_name=? WHERE id=?", (ts, database_name, conversation_id))
        conn.commit()
    return msg_id

def get_history(conversation_id: str):
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT id, role, content_markdown, charts_json, sql_meta_json, database_name, facts, created_at FROM message WHERE conversation_id=? ORDER BY created_at ASC", (conversation_id,))
        rows = cur.fetchall()
        return [
            {
                "id": r["id"],
                "role": r["role"],
                "content_markdown": r["content_markdown"],
                "charts": json.loads(r["charts_json"]) if r["charts_json"] else [],
                "sql_meta": json.loads(r["sql_meta_json"]) if r["sql_meta_json"] else {},
                "database_name": r["database_name"],
                "facts": r["facts"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

def delete_conversation(conversation_id: str):
    with sqlite3.connect(SQLITE_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM message WHERE conversation_id=?", (conversation_id,))
        cur.execute("DELETE FROM summary WHERE conversation_id=?", (conversation_id,))
        cur.execute("DELETE FROM conversation WHERE id=?", (conversation_id,))
        conn.commit()

def get_message_count(conversation_id: str) -> int:
    with sqlite3.connect(SQLITE_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM message WHERE conversation_id=?", (conversation_id,))
        row = cur.fetchone()
        return int(row[0] if row and row[0] is not None else 0)

def get_oldest_messages(conversation_id: str, limit: int = 10):
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT id, role, content_markdown, created_at FROM message WHERE conversation_id=? ORDER BY created_at ASC LIMIT ?",
            (conversation_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]

def delete_messages_by_ids(conversation_id: str, ids: list[str]):
    if not ids:
        return
    with sqlite3.connect(SQLITE_PATH) as conn:
        cur = conn.cursor()
        qmarks = ",".join(["?"] * len(ids))
        cur.execute(f"DELETE FROM message WHERE conversation_id=? AND id IN ({qmarks})", (conversation_id, *ids))
        conn.commit()

def save_summary(conversation_id: str, content: str) -> str:
    sid = _gen_id("sum")
    ts = _now_str()
    with sqlite3.connect(SQLITE_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO summary (id, conversation_id, content, created_at) VALUES (?, ?, ?, ?)",
            (sid, conversation_id, content, ts),
        )
        conn.commit()
    return sid

def get_summaries(conversation_id: str) -> list[dict]:
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT id, content, created_at FROM summary WHERE conversation_id=? ORDER BY created_at ASC",
            (conversation_id,),
        )
        return [dict(r) for r in cur.fetchall()]

def get_past_facts(database_name: str, limit: int = 10, conversation_id: str | None = None) -> str:
    """Get past exploration facts for the given database to provide context for new queries
    
    Args:
        database_name: Database to get facts for
        limit: Maximum number of past facts to retrieve
        conversation_id: Optional conversation ID to limit facts to current conversation only
    """
    if not database_name:
        return ""
    
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            # If conversation_id provided, only get facts from THIS conversation
            if conversation_id:
                cur.execute(
                    """SELECT facts, created_at FROM message 
                        WHERE conversation_id=? AND database_name=? AND role='assistant' AND facts IS NOT NULL 
                    ORDER BY created_at DESC LIMIT ?""",
                        (conversation_id, database_name, limit)
                )
            else:
                # No conversation_id = new conversation, return empty facts
                return ""
            
            rows = cur.fetchall()
            
            if not rows:
                return ""
            
            # Combine recent facts
            past_facts = []
            for row in rows:
                if row["facts"]:
                    past_facts.append(f"Previous exploration: {row['facts']}")
            
            return "\n".join(past_facts) if past_facts else ""
            
    except Exception as e:
        print(f"Error getting past facts: {e}")
        return ""

# Initialize LLM with temperature for more creative/diverse outputs
# Temperature 0.8 provides good balance: varied enough for diverse charts, but still coherent
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.8)

# Ensure queries include a LIMIT to avoid huge result sets
def ensure_limit(query: str, default_limit: int = 50) -> str:
    try:
        if not isinstance(query, str):
            return query
        q = query.strip()
        # Remove trailing semicolon for uniform handling
        has_semicolon = q.endswith(';')
        if has_semicolon:
            q = q[:-1]
        # If LIMIT already present, keep as-is
        import re
        if re.search(r"\bLIMIT\s+\d+\b", q, re.IGNORECASE):
            return query
        # Only apply to SELECT queries
        if re.match(r"^\s*SELECT\b", q, re.IGNORECASE):
            q = f"{q} LIMIT {int(default_limit)}"
            return q + (';' if has_semicolon else '')
        return query
    except Exception:
        return query

# Best-effort sanitizer for quick exploratory SQL to fit AirportDB schema
# Generic cleaner for any SQL emitted by the LLM
def _strip_sql_fences(q) -> str:
    try:
        # Ensure input is a string
        if not isinstance(q, str):
            q = str(q)
        
        s = q.strip()
        # remove triple backtick blocks and optional language tag
        if s.startswith('```'):
            s = s.lstrip('`')
            # remove leading language tag like sql, noql, etc.
            if len(s) >= 4 and s[:4].lower() == 'noql':
                s = s[4:].lstrip()
            elif len(s) >= 3 and s[:3].lower() == 'sql':
                s = s[3:].lstrip()
        s = s.replace('```', '').strip()
        # remove surrounding quotes if any
        if len(s) >= 2 and ((s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'"))):
            s = s[1:-1].strip()
        return s
    except Exception:
        # Fallback: convert to string and return
        try:
            return str(q)
        except Exception:
            return ""

# ChatGPT-style markdown generation prompt with selective chart embedding (grounded)
chat_markdown_prompt = ChatPromptTemplate.from_template("""
You're having a thoughtful, detailed conversation with someone who's genuinely curious about data. Give them a comprehensive, informative explanation - like you're a knowledgeable colleague sharing insights over coffee.

Their question: {question}
Database: {database_name}

🎯 **CRITICAL: ANSWER EXACTLY WHAT WAS ASKED**
- If they ask about "the world" → show GLOBAL data, not specific countries
- If they ask about "most populous place" → show the actual most populous places globally
- If they ask about "European countries" → show European data only
- If they ask about "US cities" → show US data only
- DO NOT assume geographic scope - stick to what they actually requested

ACTUAL DATABASE SCHEMA:
{schema}

SAMPLE DATA (first few rows per table):
{samples}

Recent conversation (most recent last). Use this context to avoid repeating points and to maintain continuity. Do NOT restate earlier content verbatim:
{history}

Facts (ground truth; ONLY use these for numeric claims):
{facts}

AllowedEntities (you may ONLY name these cities/airlines/aircraft/types; otherwise generalize):
{allowed_entities}

Write a substantial, detailed response (4-6 paragraphs minimum). Be informative and thorough, but maintain a natural conversational tone. Focus on providing rich context, background information, and detailed analysis. Don't be overly excited or use excessive exclamation points - just be genuinely knowledgeable and helpful.

Think of it like explaining something complex to an intelligent colleague who wants to understand the full picture. You'd provide context, explain the nuances, discuss implications, and give them a comprehensive understanding.

Guidelines:
- Write multiple detailed paragraphs with substantial context
- Explain the background and why things are the way they are
- Provide thorough analysis and implications
- Be conversational but informative - thoughtful rather than excited
- Include relevant comparisons and historical context where appropriate
- **Include charts to visualize data** - Use 1-4 charts based on question complexity
- Keep the tone measured and informative, not overly enthusiastic
- **Chart Guidelines:**
  * Simple COUNT/GROUP BY questions ("contacts per org", "top 10 items", "users by status") → **Generate 1 chart to show the data**
  * Analysis/exploration questions ("analyze trends", "explore patterns", "understand behavior") → **Generate 2-4 diverse charts showing different perspectives**
  * **ALWAYS include at least 1 chart when answering data questions** - charts make data easier to understand!
- Numeric grounding rules:
  - Do NOT invent numbers. If a number is not present in Facts or will be shown in a chart, avoid it or phrase qualitatively ("large share", "increased over time").
  - Prefer citing numbers after a chart block, e.g., "According to the chart below…".
  - Only name specific cities/airlines/aircraft that appear in AllowedEntities or in the chart labels. Otherwise, use generic phrases ("major hub", "a large carrier").

**CHART GENERATION RULES:**
- **✅ ALWAYS include charts when discussing data** - they help visualize and explain
- **📊 How many charts to generate:**
  * Simple specific questions ("contacts per org", "top 10 X", "count by Y") → **1 chart**
  * Analysis/exploration questions ("analyze", "explore", "understand patterns") → **2-4 charts showing different perspectives**
- **🚨 IF you generate multiple charts: Each chart MUST show DIFFERENT data/perspectives**
- **NEVER generate 2+ charts that group by the same dimension** - if all charts would be identical, generate only 1
- **🚨 KEEP QUERIES SIMPLE:** 
  * **Group by ONE dimension only** - Avoid complex multi-column grouping
    - ✅ GOOD: `SELECT [dimension], COUNT(*) FROM [table] GROUP BY [dimension]`
    - ❌ BAD: `SELECT [dim1], [dim2], COUNT(*) FROM [table] GROUP BY [dim1], [dim2]` (too complex)
    - ❌ BAD: `SELECT YEAR(...), MONTH(...), DAY(...), [field] FROM [table] GROUP BY year, month, day, [field]` (way too granular)
  
  * **Avoid unnecessary JOINs** - Only join if you NEED data from another table
    - ✅ GOOD: `SELECT t.[field], COUNT(*) FROM [table] t GROUP BY t.[field]` (if field exists in table)
    - ❌ BAD: `SELECT b.[field], COUNT(a._id) FROM [table_a] a JOIN [table_b] b ON a.[key] = b.[key] GROUP BY b.[field]` (unnecessary if field exists in table_a)
    - ⚠️ If you MUST join, use COUNT(DISTINCT ...) to avoid inflated counts from 1:N relationships
  
  * **Use table aliases consistently** - Always prefix columns with table alias to avoid ambiguity
    - ✅ GOOD: `SELECT t.[column], COUNT(t._id) FROM [table] t GROUP BY t.[column]`
    - ❌ BAD: `SELECT [column], COUNT(_id) FROM [table]` (ambiguous if used in joins)
- **DO NOT mention charts conditionally** - Don't say "if data is available" or "should such data exist"
- **DO NOT reference charts that didn't render** - Only discuss charts you actually generated with ```chart blocks
- **DO NOT apologize for missing charts** - Just generate the charts you can and discuss those
- **NEVER generate charts with:**
  * Same table combinations (e.g., don't do "contacttags JOIN contacts" twice)
  * Same GROUP BY column (e.g., don't group by "label" twice - EVEN WITH DIFFERENT CHART TYPES!)
  * Same aggregation on same data (e.g., don't COUNT tags twice)
  * Similar questions (even if phrased differently)
  * Same dimension/category axis (e.g., if Chart 1 groups by tag labels, Chart 2 must group by something else like stage, status, time, agent, etc.)

- **MANDATORY DIVERSITY CHECKLIST** (each chart must differ in at least 2 of these):
  ✓ Different base table (contacts vs events vs contacttags vs corecontacts)
  ✓ Different JOIN pattern (no join vs one join vs multi-join)
  ✓ Different metric type (COUNT vs AVG vs SUM vs MAX vs time-based)
  ✓ Different dimension (by status vs by stage vs by time vs by agent)
  ✓ Different chart type when appropriate (bar vs line vs pie)
  ✓ Different time period (last 30 days vs last 60 days vs last 90 days)

- **ALWAYS aim for 2-4 diverse charts** - BUT only if the question allows for multiple perspectives
- **🚨 CRITICAL: If question has only ONE specific answer → Generate ONLY 1 chart**
  * ONE specific answer questions: "contacts per org", "users by status", "top 10 products" → 1 chart only
  * MULTIPLE perspective questions: "analyze events", "understand contact behavior", "explore data" → 2-4 charts
- Complex analysis → 2-4 charts | Simple COUNT/GROUP BY question → 1 chart
- Each chart should reveal a DIFFERENT aspect: time, category, status, type, agent, comparison, etc.
- Use the exact format: ```chart
{{"type": "bar|line|pie|scatter", "question": "specific query", "title": "Simple title", "db": "{database_name}"}}
```

- **✅ EXAMPLES OF TRULY DIVERSE CHARTS (FOR REFERENCE - DON'T SHOW SQL TO USER):**
  
  **Example 1: "Events per day of week"** → Generate 3 DIFFERENT charts:
  ```chart
  {{"type": "bar", "question": "Count events by day of week", "title": "Events by Day of Week", "db": "{database_name}"}}
  ```
  (Internally groups by: day_of_week)
  
  ```chart
  {{"type": "bar", "question": "Show top 10 event types or labels", "title": "Top Event Types", "db": "{database_name}"}}
  ```
  (Internally groups by: event label/type - DIFFERENT dimension!)
  
  ```chart
  {{"type": "line", "question": "Show daily event count for last 30 days", "title": "Daily Event Trend", "db": "{database_name}"}}
  ```
  (Internally groups by: date - DIFFERENT temporal view!)
  
  ❌ **BAD examples:**
  - "Events by weekday vs weekend" = Still day-of-week grouping = DUPLICATE!
  
  **Example 2: User asks about a specific category/attribute** → Generate 3 DIFFERENT charts (different dimensions):
  ```chart
  {{"type": "bar", "question": "What are the top 10 [items] by [metric]?", "title": "Top 10 [Items]", "db": "{database_name}"}}
  ```
  ✅ Groups by: category/label | From: main table
  
  ```chart
  {{"type": "pie", "question": "What is the distribution by [status/type/category]?", "title": "Distribution by [Dimension]", "db": "{database_name}"}}
  ```
  ✅ Groups by: different status field | From: different table (DIFFERENT dimension!)
  
  ```chart
  {{"type": "line", "question": "How has [metric] changed over [time period]?", "title": "[Metric] Trend Over Time", "db": "{database_name}"}}
  ```
  ✅ Groups by: date/time dimension | Shows trends (DIFFERENT axis - temporal!)
  
  ❌ **BAD 3rd chart would be:** Same GROUP BY as Chart 1, just different chart type = DUPLICATE!
  
  **Example 2: User asks for analysis** → Generate 2-3 DIFFERENT charts:
  ```chart
  {{"type": "bar", "question": "What is the [entity] distribution by [dimension_A]?", "title": "[Entities] by [Dimension A]", "db": "{database_name}"}}
  ```
  ```chart
  {{"type": "bar", "question": "What is the average [metric_B] per [dimension_C]?", "title": "Avg [Metric] by [Dimension C]", "db": "{database_name}"}}
  ```
  ```chart
  {{"type": "horizontal_bar", "question": "Which [entities] have the most [related_items]?", "title": "Top [Entities] Ranked", "db": "{database_name}"}}
  ```
  
  **Example 3: Simple focused question with ONE specific answer** → Generate ONLY 1 chart:
  
  **User asks: "Number of contacts per organization"** → Only 1 way to answer:
  ```chart
  {{"type": "bar", "question": "How many contacts does each organization have?", "title": "Contacts per Organization", "db": "{database_name}"}}
  ```
  (This is the ONLY chart needed - question has ONE specific answer)
  
  **User asks: "What are the most common event types?"** → Only 1 chart needed:
  ```chart
  {{"type": "bar", "question": "Most common event types", "title": "Top Event Types", "db": "{database_name}"}}
  ```
  (This answers the question completely)
  
  **When to generate multiple charts:** User asks "Analyze our contacts" or "Understand event patterns" → Then show 2-4 DIFFERENT perspectives
  
  **🚨 CRITICAL: DON'T include SQL code in your response to users!** The chart blocks will automatically generate the queries. Just include the ```chart blocks and your natural language explanation.
  
- **❌ BAD EXAMPLES (DUPLICATES - NEVER DO THIS):**
  
  **DON'T generate similar charts like this:**
  ```chart
  {{"type": "bar", "question": "Show [item] distribution by [dimension_X]", "title": "[Items] by [Dimension X]", "db": "{database_name}"}}
  ```
  ```chart
  {{"type": "pie", "question": "How many [items] per [dimension_X]?", "title": "[Item] Count by [Dimension X]", "db": "{database_name}"}}
  ```
  ❌ BOTH group by SAME dimension (dimension_X) even though chart types differ = DUPLICATE!
  
  **DON'T generate generic/vague chart questions:**
  ```chart
  {{{{type}}: "bar", "question": "Chart", "title": "Chart", "db": "{{{{database}}}}"}}
  ```
  ❌ "Chart" is NOT a specific question! Always be explicit like: "What are the top 10 [items] by [metric]?"
  
  **DON'T repeat the same GROUP BY column:**
  ```chart
  {{"type": "bar", "question": "Top [items] by name", "title": "Top [Items]", "db": "{database_name}"}}
  ```
  (Groups by: name)
  ```chart
  {{"type": "line", "question": "[Items] by name over time", "title": "[Item] Trends", "db": "{database_name}"}}
  ```
  (Groups by: name) ❌ DUPLICATE - both group by 'name'! Second chart should group by date instead.

- **WHEN IN DOUBT: Generate fewer, more diverse charts rather than similar ones**
- **ONLY mention charts you actually create** - If you generate 2 charts, only discuss those 2. Don't say "a third chart could show..." or "if time-series data is available..."

When a chart would help illustrate your points, mention it naturally:
```chart
{{"type": "bar|line|pie|scatter", "question": "specific query", "title": "Simple title", "db": "{database_name}"}}
```

**After generating charts, ONLY reference the ones that actually appear in your response:**
- ✅ GOOD: "The bar chart above shows..." (if you included a bar chart)
- ❌ BAD: "A line chart could show trends, if such data exists" (don't mention hypothetical charts)

📊 **CRITICAL: CHOOSE THE RIGHT CHART TYPE**

**BAR CHART** → Use for comparing discrete categories (2-20 items)
- ✅ "Compare entity A vs entity B by metric" → bar
- ✅ "Top 10 items by value" → bar
- ✅ "Distribution across categories" → bar
- ❌ NOT for trends over time (use line instead)

**HORIZONTAL_BAR** → Use for rankings with long labels
- ✅ "Top entities ranked by metric" → horizontal_bar
- ✅ "Items ranked by count/value" → horizontal_bar
- ✅ Best when labels are long names that need space

**LINE CHART** → ONLY for trends over time or continuous progression
- ✅ "Metric growth from [start] to [end]" → line
- ✅ "Monthly/daily trends" → line
- ✅ "Changes over time periods" → line
- ❌ NOT for comparing 2 entities (Entity A vs B) → use bar/pie instead
- ❌ NOT for categorical comparisons → use bar/pie instead

**PIE CHART** → Use for proportions/percentages (2-6 slices only)
- ✅ "Market share: Entity A vs Entity B" → pie
- ✅ "Distribution by category (percentage)" → pie
- ✅ "Percentage breakdown by type" → pie
- ❌ NOT for >6 categories (use bar instead)
- ❌ NOT for absolute numbers without context (use bar)

**SCATTER PLOT** → Use for correlation between two numeric variables
- ✅ "Metric A vs Metric B relationship" → scatter
- ✅ "Size vs volume correlation" → scatter
- ❌ NOT for categorical comparisons

**COMMON MISTAKES TO AVOID:**
- ❌ Using line chart for "A vs B comparison" (use bar/pie instead)
- ❌ Using pie chart for >6 categories (use bar instead)
- ❌ Using bar chart for time series trends (use line instead)

Examples of the style and when to include charts:

**EXAMPLE 1: Single Fact (No chart needed)**
"What's China's population?"
"China has approximately 1.4 billion people, which represents about 18% of the global population. This means that nearly one in every five people on Earth lives in China, making it by far the most populous country in the world.
This massive population didn't happen overnight. China experienced rapid population growth throughout much of the 20th century, driven by improvements in healthcare and living conditions, combined with traditional cultural preferences for larger families..."
**EXAMPLE 2: Rankings/Top N (Use BAR chart)**
"Which countries have the largest populations?"
"Global population distribution is highly concentrated among a relatively small number of countries. China leads with approximately 1.4 billion people, followed closely by India with 1.3 billion. The United States comes in third with about 330 million, representing a significant drop from the top two.
```chart
{{"type": "bar", "question": "top 10 countries by population", "title": "Most Populous Countries", "db": "world"}}
```

What's particularly striking about this distribution is how dramatically the numbers fall after the top few countries. Indonesia, Pakistan, and Brazil round out the top six, each with populations between 220-270 million people..."

**EXAMPLE 3: Proportions/Percentages (Use PIE chart)**
"What's the population distribution by continent?"
"Continental population distribution reveals fascinating patterns about global demographics. Asia dominates with nearly 60% of the world's population, primarily due to China and India. Africa accounts for about 17% and is experiencing rapid growth.

```chart
{{"type": "pie", "question": "population by continent", "title": "Global Population by Continent", "db": "world"}}
```

Europe holds about 10% of global population, while North America represents roughly 8%. The remaining continents have much smaller shares..."

**EXAMPLE 4: A vs B Comparison (Use BAR chart, NOT line)**
"Compare Entity A vs Entity B by metric"
"When comparing these two entities, we see distinct differences in their scale. Entity A has a metric value of approximately X, while Entity B shows significantly more with around Y.

```chart
{{"type": "bar", "question": "metric comparison for Entity A and Entity B", "title": "Metric Comparison: Entity A vs Entity B", "db": "{database_name}"}}
```

This difference reflects Entity B's larger operational scope and scale, positioning it as the stronger performer in this category..."

**EXAMPLE 5: Time Series/Trends (Use LINE chart)**
"Show metric growth over time"
"The metric has experienced significant evolution over the past several time periods. The data reveals a steady upward trajectory initially, with notable changes during specific periods.

```chart
{{"type": "line", "question": "metric values by time period from [start] to [end]", "title": "Metric Growth Over Time", "db": "{database_name}"}}
```

The pattern shows clear trends and demonstrates how the metric has evolved, with recovery or growth visible in recent periods..."

"Analyze [category/region]" (Include multiple DIFFERENT charts - different perspectives)
"[Category] presents a fascinating study in diversity across multiple dimensions. From one perspective, the largest items show clear patterns, though the distribution varies significantly.

```chart
{{"type": "bar", "question": "top 10 items by primary metric", "title": "Top Items by Primary Metric", "db": "{database_name}"}}
```

However, the primary metric doesn't necessarily correlate with secondary metrics. When examining a different dimension, we see different leaders emerge, with some items showing exceptional performance on alternative measures.

```chart
{{"type": "bar", "question": "items by secondary metric", "title": "Items by Secondary Metric", "db": "{database_name}"}}
```

This diversity reflects the varied characteristics and patterns across the dataset..."

Be thorough, informative, and conversational - think "detailed professional discussion" not "excited presentation."
""")

# Grounding/rewrite prompt to ensure prose uses only chart-derived facts
grounding_prompt = ChatPromptTemplate.from_template("""
You are refining a markdown answer to ensure all numeric claims are grounded in provided DATA FACTS.

RULES:
- Do NOT invent numbers. Use ONLY numbers present in DATA FACTS below.
- If a sentence mentions quantities without support, make it qualitative or remove the numbers.
- Keep the tone conversational and professional.
- Preserve structure and any chart placeholders ({{chart:cN}}) exactly.
 - Only name entities (cities, airlines, aircraft types) present in ALLOWED ENTITIES or in chart labels. Generalize or remove any other named entities.

DATA FACTS:
{facts}

ALLOWED ENTITIES:
{allowed_entities}

ORIGINAL MARKDOWN:
{markdown}

Return the revised markdown only.
""")

# Deep exploration prompt: propose small, fast exploratory SQLs from a question
deep_explore_prompt = ChatPromptTemplate.from_template(
    """
You propose quick EXPLORATORY NoQL queries for the CURRENT database to understand a question before writing analysis.

**CRITICAL: ACTUAL COLLECTION NAMES (USE THESE IN YOUR QUERY)**
The schema shows uppercase names, but you MUST use these EXACT lowercase collection names in your queries:
- EVENT → events
- CONTACT → contacts
- CORE_CONTACT → corecontacts
- CORECONTACTS → corecontacts
- CHAT_HISTORY → chathistories
- CHATHISTORIES → chathistories
- CONTACT_TAG → contacttags
- CONTACTTAGS → contacttags
- ORG_AGENT → orgagent
- ORGANIZATION → organization

Example: If schema shows "CONTACT", you MUST write "contacts" in your query.

**🚨 CRITICAL: UNDERSTAND "PER" IN USER QUESTIONS**
When user asks for "average PER conversation" or "count PER user", they want grouped results showing EACH entity with its count/metric, NOT a single average.

✅ CORRECT PATTERNS:
- "average messages per conversation" → SELECT contact_id, COUNT(*) AS message_count FROM chathistories GROUP BY contact_id ORDER BY message_count DESC LIMIT 20
- "average contacts per org" → SELECT org_id, COUNT(*) AS contact_count FROM contacts GROUP BY org_id ORDER BY contact_count DESC LIMIT 20
- "messages per conversation" → SELECT contact_id, COUNT(*) AS message_count FROM chathistories GROUP BY contact_id ORDER BY message_count DESC LIMIT 20

⚠️ IMPORTANT: "Average per X" typically means show the DISTRIBUTION of counts per X, not a single average number.
- If they want actual per-entity counts → GROUP BY entity and show results
- If they literally want the mean average → Use existing aggregate fields if available (e.g., AVG(total_messages))

❌ WRONG PATTERNS:
- Using subqueries like: SELECT AVG(cnt) FROM (SELECT x, COUNT(*) FROM y GROUP BY x) - NoQL may not support this
- Simple AVG without context: SELECT AVG(total_messages) FROM chathistories (doesn't show per-conversation breakdown)

**CRITICAL: DATE/TIME HANDLING IN NOQL**
Many timestamp fields are stored as Unix epoch seconds (numbers).

❌ **FORBIDDEN MySQL Functions (NOT supported in NoQL):**
- UNIX_TIMESTAMP(), DATE_SUB(), NOW(), INTERVAL, FROM_UNIXTIME(), DATE_FORMAT()
- MySQL-style: DAYOFWEEK(), DAYOFMONTH(), DAYNAME(), MONTHNAME()
- WEEK(), WEEKDAY(), DATE(), TIME(), STR_TO_DATE()

✅ **SUPPORTED NoQL SYNTAX REFERENCE:**

**Date Functions** (for DATE fields, NOT Unix epoch numbers):
- `DAY_OF_WEEK(date_field)` - Returns 1-7 (Sunday=1, Saturday=7) - **USE THIS for day of week**
- `DAY_OF_MONTH(date_field)`, `MONTH(date_field)`, `YEAR(date_field)`
- `DATE_TRUNC(date_field, 'day'|'month'|'year')` - Truncate to granularity
- `DATE_FROM_STRING('2021-11-15')`, `DATE_TO_STRING(date_field)`
- `CURRENT_DATE()` - Current date
- `DATE_ADD(date, 'hour', 2)`, `DATE_SUBTRACT(date, 'day', 7)`
- `DATE_DIFF(start_date, end_date, 'day')`
- `EXTRACT(day|month|year|hour|minute|second from date_field)` - Use for non-dow extractions

**Aggregates:**
- `COUNT(*)`, `COUNT(DISTINCT field)`, `SUM(field)`, `AVG(field)`, `MIN(field)`, `MAX(field)`
- `FIRSTN(10)`, `LASTN(10)` - Returns first/last N records as array
- `SUM(CASE WHEN condition THEN 1 ELSE 0 END)` - Conditional aggregation

**Joins:** (INNER JOIN, LEFT JOIN supported)
- Explicit ON: `INNER JOIN table AS t ON t.id = other.id`
- Join hints: `table|first`, `table|last`, `table|unwind`
- Array functions: `FIRST_IN_ARRAY(field)`, `LAST_IN_ARRAY(field)`, `UNWIND(field)`

**Window Functions:**
- `RANK() OVER (ORDER BY field)`, `ROW_NUMBER() OVER (ORDER BY field)`, `DENSE_RANK() OVER (...)`

**String Functions:**
- `CONCAT(str1, str2, ...)`, `TRIM(str)`, `LTRIM(str)`, `RTRIM(str)`, `UPPER(str)`, `LOWER(str)`
- `SUBSTRING(str, start, length)`, `LENGTH(str)`, `REPLACE(str, find, replace)`
- `LIKE 'pattern%'`, `NOT LIKE 'pattern%'` - Case insensitive

**Math Functions:**
- `ABS(n)`, `CEIL(n)`, `FLOOR(n)`, `ROUND(n, decimals)`, `SQRT(n)`, `POW(base, exp)`, `MOD(n, d)`
- Operators: `+`, `-`, `*`, `/`, `%`

**Conversion Functions:**
- `CONVERT(expr, 'int'|'double'|'string'|'bool'|'date')`, `TO_DATE(expr)`, `TO_INT(expr)`
- `IFNULL(expr, default_value)`

**Field Functions:**
- `UNSET(field)` - Remove field from result
- `FIELD_EXISTS(field, true|false)` - Check field existence (WHERE only)

**Array Operations:**
- Sub-select: `(SELECT * FROM array_field WHERE condition)` 
- `ALL_ELEMENTS_TRUE(array)`, `IN_ARRAY(value, array)`, `SIZE(array)`

**Clauses:**
- WHERE: Functions must be explicit, can't use computed aliases
- ORDER BY: Field must appear in SELECT
- GROUP BY: Include all non-aggregated columns
- LIMIT/OFFSET, UNION/UNION ALL supported

✅ **For Unix Timestamp Fields (stored as numbers):**
Use direct numeric comparison with pre-calculated Unix timestamps:
  - Last 6 months: WHERE timestamp_field >= 1710374400
  - Last 30 days: WHERE timestamp_field >= 1726444800
  - Specific date: WHERE timestamp_field >= 1704067200 (Jan 1, 2024)

⚠️ **CRITICAL: UNIX TIMESTAMP CONVERSION PATTERN**

**🎯 For Unix Epoch Timestamp Fields (stored as SECONDS):**
Field names like: `created_at`, `updated_at`, `timestamp`, `created_at_timestamp`, `event_time`

These fields store Unix epoch SECONDS (not milliseconds). To use date functions:

**✅ MANDATORY CONVERSION PATTERN:**
```
TO_DATE(field * 1000)
```
- Multiply by 1000 to convert seconds → milliseconds
- Wrap with TO_DATE() to convert to date object
- Then use any date function

**✅ CORRECT Examples for Unix timestamp fields:**
- `SELECT DAY_OF_WEEK(TO_DATE(timestamp * 1000)) AS dow, COUNT(*) FROM events GROUP BY dow` ← **BEST for day of week**
- `SELECT DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day') AS date, COUNT(*) FROM contacts GROUP BY date`
- `SELECT MONTH(TO_DATE(timestamp * 1000)) AS month, COUNT(*) FROM events GROUP BY month`
- `SELECT YEAR(TO_DATE(created_at * 1000)) AS year, COUNT(*) FROM contacts GROUP BY year`
- `WHERE TO_DATE(timestamp * 1000) >= TO_DATE('2024-01-01')` (date comparison)

**❌ WRONG Examples (will fail or return empty):**
- `SELECT DAY_OF_WEEK(timestamp) FROM events` (timestamp is a number!)
- `SELECT TO_DATE(timestamp) FROM events` (missing * 1000!)
- `SELECT EXTRACT(dow FROM TO_DATE(timestamp * 1000)) FROM events` (returns empty - use DAY_OF_WEEK instead!)
- `SELECT DAYOFWEEK(FROM_UNIXTIME(timestamp)) FROM events` (MySQL syntax - not supported!)

**✅ For Regular DATE Fields (already date objects):**
- `SELECT DAY_OF_WEEK(order_date) as dow, COUNT(*) FROM orders GROUP BY dow`
- `SELECT DATE_TRUNC(event_date, 'month') as month, COUNT(*) FROM logs GROUP BY month`

**⚡ Performance Tip:**
For simple date filtering on Unix timestamps, numeric comparison is faster:
- `WHERE timestamp >= 1704067200` (faster than TO_DATE conversion)

ACTUAL DATABASE SCHEMA:
{schema}

SAMPLE DATA (first few rows per table):
{samples}

COUNTS (table row counts + column non-null counts):
{counts}

{table_size_guidance}

CRITICAL RULES:
🚨 **SCHEMA ADHERENCE (MANDATORY):**
- ONLY use tables and columns that exist in the SCHEMA above
- NEVER assume column names - verify every column exists in the schema
- Always prefix columns with table aliases (a., b., c., d.)
- Use the lowercase collection names specified above (contacts, events, etc.)
- Always use lowercase table names: contacts NOT CONTACTS or CONTACT

🚨 **QUERY SIMPLICITY (MANDATORY):**
- **Group by ONE dimension only** - Avoid multi-column grouping
- **Avoid unnecessary JOINs** - If field exists in main table, don't join for the same field
  * ✅ GOOD: `SELECT t.[field], COUNT(*) FROM [table] t GROUP BY t.[field]`
  * ❌ BAD: `SELECT b.[field], COUNT(a._id) FROM [table_a] a JOIN [table_b] b ON a.[key] = b.[key]` (join not needed if field in table_a)
- **If you MUST join:** Use COUNT(DISTINCT ...) to avoid inflated counts
- **Always use table alias prefixes** (e.g., `t.[column]` not `[column]`) to avoid ambiguity

🔧 **OUTPUT FORMAT (CRITICAL):**
🚨 **RETURN ONLY JSON - NO TEXT BEFORE OR AFTER**
Return JSON in this exact format:
{"explorations": [{"purpose": "...", "sql": "SELECT ..."}]}

❌ **WRONG:** Adding explanations before JSON
✅ **CORRECT:** Only the JSON object, nothing else

(Note: No markdown fences, no explanatory text, just pure JSON)

⚡ **PERFORMANCE RULES:**
- 1–3 queries per round. Fast and small: SELECT-only, no DDL/DML
- Include ORDER BY and LIMIT (max 20 rows)
- Aggregate early, minimal joins, only required columns
- Use backticks for reserved words if needed
- Pay attention to table sizes above - use aggressive LIMIT for large tables


Question:
{question}

Prior facts:
{prior_facts}
"""
)

# Helper to format a value safely for facts
def _fmt(v):
    try:
        if v is None:
            return ""
        return str(v)
    except Exception:
        return ""

# Active exploration using deep_explore_prompt
def run_deep_exploration(question: str, database_name: str, max_queries: int = 3, conversation_id: str | None = None) -> dict:
    """Use deep_explore_prompt to generate targeted exploratory queries based on the question
    
    Args:
        question: User's question
        database_name: Database to explore
        max_queries: Maximum number of exploratory queries to run
        conversation_id: Optional conversation ID to scope facts to current conversation
    """
    print(f"\n🔎 === DEEP EXPLORATION ({database_name}) — intelligent probing ===")
    import time
    start_time = time.time()
    
    facts: list[str] = []
    allowed: set[str] = set()
    
    try:
        # Get schema, counts and samples for the prompt
        t1 = time.time()
        schema_text = get_schema(database_name)
        print(f"⏱️ Schema fetch: {time.time()-t1:.2f}s")
        
        t1 = time.time()
        counts_data = get_table_and_column_counts(database_name)
        print(f"⏱️ Counts fetch: {time.time()-t1:.2f}s")
        
        counts_text = safe_json_dumps(counts_data, ensure_ascii=False)[:2000]
        samples_text = safe_json_dumps(sample_database_tables(database_name), ensure_ascii=False)[:2000]
        table_guidance = generate_table_size_guidance(counts_data, threshold=100000)
        
        # Get past facts for this conversation only (not from other conversations)
        past_facts = get_past_facts(database_name, limit=5, conversation_id=conversation_id)
        prior_facts_text = past_facts if past_facts else "(initial exploration)"
        if conversation_id:
            print(f"📚 Using past facts from conversation {conversation_id}: {len(past_facts)} characters")
        else:
            print(f"🆕 NEW conversation: No past facts (starting fresh)")
        
        # Create the exploration chain with table size guidance
        # Build input data with all required fields
        input_data = {
            "question": question,
            "prior_facts": prior_facts_text,
            "schema": schema_text,
            "counts": counts_text,
            "samples": samples_text,
            "table_size_guidance": table_guidance
        }
        
        chain = (
            deep_explore_prompt
            | llm.bind(stop=["\nResult:"])
            | StrOutputParser()
        )
        
        # Generate exploration queries
        t1 = time.time()
        response = chain.invoke(input_data)
        print(f"⏱️ LLM call: {time.time()-t1:.2f}s")
        
        print(f"🤖 LLM exploration response: {response}")
        
        # Parse the JSON response - strip markdown fences if present
        try:
            # Ensure response is a string
            if not isinstance(response, str):
                response = str(response)
            
            # Clean the response by removing markdown fences
            json_text = response.strip()
            if json_text.startswith('```'):
                # Find the actual JSON content between fences
                lines = json_text.split('\n')
                start_idx = 0
                end_idx = len(lines)
                
                # Skip the opening fence line
                if lines[0].startswith('```'):
                    start_idx = 1
                
                # Find the closing fence
                for i in range(len(lines)-1, -1, -1):
                    if lines[i].strip() == '```':
                        end_idx = i
                        break

                json_text = '\n'.join(lines[start_idx:end_idx]).strip()
            
            exploration_data = json.loads(json_text)
            explorations = exploration_data.get("explorations", [])
            
            # Execute each exploration query
            for i, exploration in enumerate(explorations[:max_queries]):
                purpose = exploration.get("purpose", f"Query {i+1}")
                sql_query = exploration.get("sql", "")
                
                if not sql_query:
                    continue
                    
                print(f"🔍 Exploration {i+1}: {purpose}")
                print(f"   SQL: {sql_query}")
                
                try:
                    # Strip SQL fences and execute
                    t1 = time.time()
                    clean_query = _strip_sql_fences(sql_query)
                    clean_query = ensure_limit(clean_query, 20)  # Safety limit
                    
                    # Validate query against schema before execution
                    validation_result = validate_sql_against_schema(clean_query, database_name)
                    if not validation_result["valid"]:
                        print(f"   ❌ Schema validation failed: {validation_result['error']}")
                        facts.append(f"{purpose}: schema validation error - {validation_result['error']}")
                        continue
                    
                    rows, columns = run_query(clean_query, database_name, return_columns=True)
                    print(f"   ⏱️ Query execution: {time.time()-t1:.2f}s")
                    
                    if rows:
                        print(f"   ✅ Found {len(rows)} results")
                        # Add facts from this exploration
                        facts.append(f"{purpose}: {len(rows)} records found")
                        
                        # Extract allowed entities from string columns
                        for row in rows[:5]:  # First 5 rows
                            for cell in row:
                                if isinstance(cell, (str, bytes)) and len(str(cell)) > 2:
                                    try:
                                        entity = str(cell)[:80].strip()
                                        if entity and not entity.isdigit():
                                            allowed.add(entity)
                                    except Exception:
                                        pass
                        
                        # Add some specific facts from the data
                        if len(rows) > 0 and len(columns) >= 2:
                            first_row = rows[0]
                            if len(first_row) >= 2:
                                facts.append(f"{purpose}: top result - {columns[0]}: {first_row[0]}, {columns[1]}: {first_row[1]}")
                            
                            # Add SQL methodology for transparency
                            if "ranking" in purpose.lower() or "top" in purpose.lower():
                                facts.append(f"Ranking methodology: {clean_query[:200]}...")
                        else:
                            print(f"   ⚠️ No results found")
                            facts.append(f"{purpose}: no matching records")
                            
                except Exception as e:
                    print(f"   ❌ Query failed: {e}")
                    facts.append(f"{purpose}: query error - {str(e)}")
                    
        except json.JSONDecodeError as e:
            print(f"⚠️ Failed to parse exploration JSON: {e}")
            print(f"Raw response: {response[:500]}...")
            print(f"🔄 Falling back to passive exploration for {database_name}")
            return passive_exploration_fallback(database_name)
            
    except Exception as e:
        print(f"⚠️ Deep exploration failed: {e}")
        # Fallback to passive sampling
        return passive_exploration_fallback(database_name)
    
    facts_text = "\n".join(facts) if facts else "(no exploration facts)"
    allowed_text = "\n".join(sorted(allowed)) if allowed else "(none)"
    
    print(f"⏱️ TOTAL DEEP EXPLORATION TIME: {time.time()-start_time:.2f}s")
    print(f"🔎 Deep exploration complete. Facts: {len(facts)} | Allowed entities: {len(allowed)}\n")
    return {"facts": facts_text, "allowed": allowed_text}

# Fallback passive exploration (original method)
def passive_exploration_fallback(database_name: str) -> dict:
    """Fallback to original passive sampling method"""
    print(f"🔄 Falling back to passive exploration for {database_name}")
    facts: list[str] = []
    allowed: set[str] = set()
    try:
        samples = sample_database_tables(database_name, max_rows=5, max_tables=100)
        for tname, info in samples.items():
            if isinstance(info, dict) and info.get('error'):
                facts.append(f"table {tname}: preview error")
                continue
            cols = (info or {}).get('columns') or []
            rows = (info or {}).get('rows') or []
            facts.append(f"table {tname}: {len(cols)} columns, {len(rows)} sample rows")
            # Add some string-like values from first two columns as allowed entities
            for row in rows[:3]:
                for cell in list(row)[:2]:
                    if isinstance(cell, (str, bytes)):
                        try:
                            allowed.add(str(cell)[:80])
                        except Exception:
                            continue
            
    except Exception as e:
        print(f"⚠️ Passive sampling failed: {e}")

    facts_text = "\n".join(facts) if facts else "(no precomputed facts)"
    allowed_text = "\n".join(sorted(allowed)) if allowed else "(none)"
    return {"facts": facts_text, "allowed": allowed_text}

# Detect if query is casual conversation (not data-related) using LLM
def is_casual_conversation(question: str) -> bool:
    """Use LLM to intelligently detect if the question is casual conversation or a data query"""
    if not question or not question.strip():
        return True
    
    # Very short queries (< 3 chars) are definitely casual - save LLM call
    if len(question.strip()) < 3:
        return True
    
    try:
        # Create a classification prompt
        classification_prompt = ChatPromptTemplate.from_template("""
You are a classifier that determines if a user's message is casual conversation or a data/database query.

**User Message:** "{question}"

**Classification Rules:**

✅ **CASUAL CONVERSATION** (respond with "CASUAL"):
- Greetings: "hi", "hello", "hey", "good morning"
- How are you: "how are you", "how's it going", "what's up"
- Thanks: "thanks", "thank you", "appreciate it"
- Goodbyes: "bye", "goodbye", "see you later"
- Small talk: "cool", "nice", "awesome", "okay"
- Questions about the bot: "who are you", "what can you do", "what's your name"
- General chat: "lol", "haha", random comments

❌ **DATA QUERY** (respond with "DATA"):
- Questions about data: "show me...", "list...", "how many...", "what are..."
- Database queries: "top 10...", "compare...", "analyze...", "find..."
- Statistical questions: "average", "total", "count", "distribution"
- Business questions: "revenue", "sales", "customers", "orders"
- Analytical requests: "trends", "patterns", "correlations"

**IMPORTANT:** 
- If the message contains BOTH casual AND data elements, classify as DATA
- If unsure, default to DATA (better to provide data than miss a query)
- Focus on the PRIMARY INTENT of the message

**Output:** Reply with ONLY one word - either "CASUAL" or "DATA" (no explanation, no punctuation)
""")
        
        # Call LLM for classification
        chain = classification_prompt | llm | StrOutputParser()
        result = chain.invoke({"question": question})
        # Ensure result is a string
        if not isinstance(result, str):
            result = str(result)
        result = result.strip().upper()
        
        # Parse result
        is_casual = "CASUAL" in result
        
        print(f"🤖 LLM Classification: '{question[:50]}...' → {result} ({'CASUAL' if is_casual else 'DATA QUERY'})")
        
        return is_casual
        
    except Exception as e:
        print(f"⚠️ LLM classification failed: {e}, defaulting to data query")
        # On error, default to treating as data query (safer)
        return False

# Generate casual conversational response
def generate_casual_response(question: str, database_name: str) -> str:
    """Generate a friendly, short conversational response without database exploration"""
    q_lower = question.lower().strip()
    
    # Greetings
    if any(greeting in q_lower for greeting in ['hi', 'hello', 'hey']):
        return "Hi there! 👋 I'm **Insight**, your AI data analyst. Ask me anything about your database, and I'll help you explore the data with insights and visualizations!"
    
    # How are you
    if 'how are you' in q_lower or 'how r u' in q_lower:
        return "I'm doing great, thank you! 😊 Ready to dive into your data. What would you like to explore today?"
    
    # Thanks
    if any(thanks in q_lower for thanks in ['thanks', 'thank you', 'ty', 'thx']):
        return "You're very welcome! 🙂 Happy to help anytime. Let me know if you need anything else!"
    
    # What can you do
    if 'what can you do' in q_lower or 'what do you do' in q_lower or 'help' in q_lower:
        return f"""I'm **Insight**, your conversational data analyst! Here's what I can do:

📊 **Natural language queries** - Just ask in plain English, no SQL needed
📈 **Smart visualizations** - I automatically create the best charts for your data
🔍 **Deep insights** - I explore patterns and trends you might miss
💡 **Intelligent suggestions** - I'll recommend follow-up questions to uncover more

**Current database:** `{database_name}`

**Try asking things like:**
- "Show me the total number of contacts by status"
- "What are the most common event types?"
- "Which organizations have the most contacts?"
- "Show me chat engagement trends over time"

What would you like to discover?"""
    
    # Who are you / What are you / What's your name
    if 'who are you' in q_lower or 'what are you' in q_lower or 'your name' in q_lower:
        return f"I'm **Insight** 🤖 - your AI-powered data analyst! I turn your questions into SQL queries, create beautiful visualizations, and help you discover insights in your `{database_name}` database. Think of me as your friendly data expert who speaks plain English! 😊"
    
    # Goodbye
    if any(bye in q_lower for bye in ['bye', 'goodbye', 'see you', 'cya']):
        return "Goodbye! 👋 It was great exploring data with you. Come back anytime!"
    
    # Default short acknowledgment
    if len(q_lower) < 10:
        return "I'm here to help! Ask me anything about your data, and I'll create insights for you. 📊"
    
    # Fallback
    return "I'm **Insight**, your data assistant! Ask me questions about your database, and I'll help you explore the data with smart queries and visualizations. What would you like to know?"

# Updated main exploration function
def explore_data_for_facts(question: str = "", rounds: int = 0, per_round: int = 0, database_name: str | None = None, conversation_id: str | None = None) -> dict:
    """
    Intelligent exploration: Use deep_explore_prompt for question-specific queries when a question is provided,
    otherwise fall back to passive sampling.
    
    Args:
        question: User's question
        rounds: (deprecated) Number of exploration rounds
        per_round: (deprecated) Queries per round
        database_name: Database to explore
        conversation_id: Optional conversation ID to scope facts to current conversation
    """
    database_name = database_name or 'zigment'
    
    # If we have a meaningful question, use deep exploration
    if question and question.strip() and len(question.strip()) > 5:
        print(f"🧠 Using intelligent deep exploration for question: '{question[:50]}...'")
        return run_deep_exploration(question, database_name, conversation_id=conversation_id)
    else:
        # No question or very short question - use passive sampling
        print(f"📊 Using passive exploration (no specific question provided)")
        return passive_exploration_fallback(database_name)

# Chart validation prompt - decides if a chart is truly necessary
chart_validator_prompt = ChatPromptTemplate.from_template("""
You are a data visualization critic. Your job is to decide if a proposed chart is truly necessary or if the information would be better conveyed through text alone.

**ORIGINAL QUESTION:** {question}
**CHART PROPOSAL:**
- Type: {chart_type}
- Title: {title}
- Purpose: {chart_purpose}
- Data Preview: {data_preview}

**STRICT VALIDATION CRITERIA:**

🚫 **REJECT THE CHART IF (USELESS VISUALIZATIONS):**
1. **Single item only** - Only 1 data point (e.g., "Colombia Airlines: 500" - nothing to compare)
   - Example: Bar chart with ONE airline, pie chart with ONE slice
   - Reason: No comparison, distribution, or trend - just one number
   - **CRITICAL**: Bar charts with 1 data point are ALWAYS useless - they show no comparison
   
2. **No data at all** - Completely empty or null dataset

3. **Wrong chart type for data:**
   - Line chart with only 2 discrete categories (not time series)
   - Pie chart with >10 slices (use bar instead)
   - Scatter plot of non-numeric data

4. **Question asks for comparison but chart shows only 1 entity:**
   - Question: "Compare airlines" → Chart shows only 1 airline ❌
   - Question: "Flight frequency" → Chart shows only 1 route ❌

✅ **APPROVE THE CHART IF:**
1. **2+ data points for comparison** - Shows ranking, comparison, or distribution
2. **Time series with 3+ points** - Shows trends over time
3. **Meaningful visualization** - Adds value beyond just stating a number
4. **Chart type matches data structure** - Bar for comparison, line for time, pie for proportions

**EXAMPLES OF CHARTS TO REJECT:**

❌ **REJECT** - "Flight Frequency Comparison in Colombia Airlines"
   - Data: Colombia Airlines: 500 (ONLY 1 ITEM!)
   - Reason: Single bar chart is useless, just say "Colombia Airlines has 500 flights"
   - Better: Show frequency across MULTIPLE airlines or routes

❌ **REJECT** - "Top Airlines by Flight Count"
   - Data: American Airlines: 1500 (ONLY 1 ITEM!)
   - Reason: Bar chart with single data point provides no comparison value
   - Better: Show top 5-10 airlines or use text: "American Airlines operates 1,500 flights"

❌ **REJECT** - "Airport Count by Country"
   - Data: United States: 15000 (ONLY 1 COUNTRY!)
   - Reason: No comparison, just one number
   - Better: Show top 10 countries or regional distribution

❌ **REJECT** - "Market Share"
   - Data: Company A: 100% (ONLY 1 SLICE!)
   - Reason: Pie chart with 1 slice is meaningless
   - Better: Show breakdown by multiple companies or segments

**EXAMPLES OF CHARTS TO APPROVE:**

✅ **APPROVE** - "Top 5 Airlines by Flight Frequency"
   - Data: Colombia: 500, Tunisia: 400, Kenya: 350, Ghana: 300, Uganda: 250
   - Reason: Shows meaningful comparison across 5 airlines

✅ **APPROVE** - "Flight Frequency: Colombia Airlines vs Tunisia Airlines"
   - Data: Colombia: 500, Tunisia: 400
   - Reason: Direct comparison between 2 entities (minimum for comparison)

✅ **APPROVE** - "Airport Traffic Growth 2010-2023"
   - Data: 14 years of time series data
   - Reason: Shows trend over time with sufficient data points

**DECISION:**
Respond with EXACTLY one of these:
- `APPROVE: [brief reason why chart adds value]`
- `REJECT: [brief reason why useless] | REPLACEMENT: [0-2 short sentences that state the fact naturally, no chart references]`

**REPLACEMENT TEXT RULES (STRICT):**
- Never mention: chart, graph, visual, validator, rejected, replacement, pie, bar, line, scatter
- Just state the fact naturally: "Colombia Airlines operates 500 flights, making it the primary carrier in the region."
- Keep it conversational and informative

Your decision:
""")

# Narrative generation prompt for charts (single or multiple) - DETAILED CONVERSATIONAL
narrative_prompt = ChatPromptTemplate.from_template("""
You're having a thoughtful, detailed conversation with someone curious about data. Give them a rich, informative discussion with lots of context and detail. Don't be overly excited or "hysterical" - just be knowledgeable and genuinely interested.

Their question: {question}
Charts available: {chart_info}

Write substantial, detailed responses that really dive into the topic. Think of it like explaining something fascinating to a colleague over coffee - thorough, informative, and conversational but not overly enthusiastic.

Return JSON format with detailed, natural conversation:
{{
  "introduction": "Start with a thoughtful, detailed explanation of what you discovered. Give context, background, and set up the topic thoroughly. Write 3-4 sentences minimum.",
  "transitions": [
    "Write substantial text between charts - explain what the previous chart showed, provide additional context, and naturally introduce the next perspective. Give detailed commentary. 3-4 sentences each.",
    "Continue with more detailed discussion, connecting the data points and providing deeper analysis. Explain patterns, comparisons, and what's interesting about the progression."
  ],
  "insights": [
    "Share detailed observations about the data - not bullet points, but full conversational paragraphs. Explain the implications and context behind what you're seeing.",
    "More detailed analysis and thoughtful commentary. Connect different aspects of the data and explain why these patterns matter."
  ],
  "conclusion": "Wrap up with a substantial summary that ties everything together. Provide final thoughts and context. 3-4 sentences minimum."
}}

Write like you're having a detailed, informative conversation - not excited or hysterical, just knowledgeable and thorough.

🔍 **RANKING TRANSPARENCY REQUIREMENTS:**
When discussing rankings or "top" lists, you MUST include:
1. **Methodology Explanation**: Clearly state the ranking criteria (e.g., "Ranked by total count", "Sorted by value DESC")
2. **Data Source Transparency**: Mention the specific tables/columns used
3. **Chart References**: For each ranking mentioned, include a chart reference {{chart:id}} 
4. **Sample Data**: Show actual numbers from your analysis (e.g., "Entity A: 5,234 items, Entity B: 4,891 items")
5. **Time Period**: Specify the data period if relevant

**Example of GOOD ranking explanation:**
"The top entities by volume are ranked using `COUNT(id)` from the main table, grouped by category. The analysis shows:

{{chart:volume_ranking}}

Based on this data:
- **#1: Category A** - 5,234 total records (18.2% of all data)  
- **#2: Category B** - 4,891 total records (17.0% of all data)
- **#3: Category C** - 3,567 total records (12.4% of all data)

This ranking uses all available data and counts all record types equally."

❌ **AVOID vague statements** like:
- "Items rank highly based on various factors"
- "The data shows interesting patterns"
- "Rankings can vary depending on methodology"

✅ **PROVIDE specific statements** like:
- "Ranked by COUNT(record_id) grouped by category_id"
- "Top performer leads with 1,247 records, followed by second place with 1,156 records"
- "This analysis covers data from the current dataset period"
""")

# Chart suggestion prompt for both single and multiple chart generation
chart_suggestion_prompt = ChatPromptTemplate.from_template("""
Analyze this question and suggest 4-6 different chart types that would best visualize the data. Always aim for MAXIMUM INSIGHT DIVERSITY by creating multiple complementary perspectives.

🚀 **ENHANCED ROBUSTNESS REQUIREMENTS:**
- For simple questions: Generate at least 4 different chart perspectives
- For complex questions: Generate 5-6 charts covering all major analytical dimensions
- Always include at least one overview chart and one detailed breakdown chart
- Create charts at different aggregation levels (individual → grouped → summary)
- Include temporal analysis when date/time data is available
- Add comparative analysis whenever possible (vs averages, vs previous periods, vs segments)

🎯 **CRITICAL: AVOID DATA REDUNDANCY - ENSURE DIVERSE INSIGHTS**
Each chart must provide UNIQUE data perspectives, NOT just different metrics of the same entities:

❌ **FORBIDDEN REDUNDANT PATTERNS:**
- "Top 20 Countries by Population" + "Top 20 Countries by GNP" (same countries, different metrics)
- "Sales by Product A-M" + "Sales by Product N-Z" (same data type, different ranges)
- "Customer Revenue Q1" + "Customer Revenue Q2" (same customers, different periods)

📊 **RANKING VISUALIZATION BEST PRACTICES:**

**Chart Type Selection by Data Size:**
- **3-5 items**: Use "bar" (vertical bars work well)
- **6-10 items**: Use "horizontal_bar" (better for category names)
- **11-20 items**: Use "lollipop" (cleaner, less cluttered)
- **21+ items**: Use "table" with embedded bars or "treemap"

**Chart Type Selection by Purpose:**
- **Value comparison**: "horizontal_bar" or "lollipop"
- **Ranking order focus**: "lollipop" or "slope"
- **Changes over time**: "slope" or "bump"
- **Hierarchical data**: "treemap"
- **Proportional view**: "pie" (only for ≤6 categories)

**Ranking Enhancement Rules:**
- Always sort data in descending order (highest first)
- For rankings, include rank numbers in titles
- Use color gradients to emphasize top performers
- Add value labels for precise reading

**Ranking Subsection Charts (CRITICAL):**
When the question involves rankings or "top" analysis, generate specific charts for EACH ranking dimension mentioned:
**Entity Volume Rankings Should Include:**
- "Volume by Category" (horizontal_bar) - Total counts per category
- "Activity Rankings" (lollipop) - Busiest entities by volume  
- "Frequency Analysis" (horizontal_bar) - Most active relationships
- "Temporal Patterns" (line) - Activity frequency by time period
- "Performance Comparison" (scatter) - Entities plotted by multiple metrics
- "Operational Metrics" (table) - Detailed performance data
**Location/Geographic Rankings Should Include:**
- "Outbound Activity Rankings" (horizontal_bar) - Entities by outgoing activity
- "Inbound Activity Analysis" (lollipop) - Entities by incoming activity  
- "Total Activity Rankings" (horizontal_bar) - Combined inbound + outbound activity
- "Geographic Distribution" (treemap) - Activity by region/location
- "Capacity vs Usage" (scatter) - Entity size vs activity volume
**Category/Group Rankings Should Include:**
- "Size vs Activity" (scatter) - Operational efficiency analysis
- "Coverage Rankings" (horizontal_bar) - Categories by scope/reach
- "Market Share Analysis" (pie) - Category share by total volume
- "Performance Trends" (line) - Category growth/decline over time

✅ **REQUIRED DIVERSIFICATION STRATEGIES:**
1. **Entity Diversity**: Different countries/customers/products in each chart
2. **Grouping Diversity**: Individual items vs grouped categories vs aggregated summaries
3. **Perspective Diversity**: Geographic vs demographic vs temporal vs performance views
4. **Scale Diversity**: Detailed breakdowns vs high-level overviews
5. **Analytical Diversity**: Rankings, distributions, correlations, trends, comparisons
6. **Temporal Diversity**: Current state, historical trends, period comparisons, growth rates
7. **Statistical Diversity**: Averages, medians, percentiles, outliers, variance analysis

🎯 **COMPREHENSIVE CHART STRATEGY FRAMEWORK:**

**Level 1 - Overview Charts (Always Include):**
- High-level summary or total view
- Key performance indicators
- Overall distribution or breakdown

**Level 2 - Detailed Analysis Charts:**
- Top/bottom performers with rankings
- Detailed breakdowns by categories
- Individual entity analysis

**Level 3 - Comparative Analysis Charts:**
- Period-over-period comparisons
- Segment vs segment analysis
- Performance vs benchmarks/averages

**Level 4 - Advanced Analytics Charts:**
- Trend analysis and growth patterns
- Correlation and relationship analysis
- Statistical distributions and outliers

**Level 5 - Contextual Charts:**
- Geographic/regional perspectives
- Time-based patterns and seasonality
- Market share or relative performance

**GOOD DIVERSE EXAMPLES:**
- "Top 20 Countries by Population" + "Population by Continent" + "Population Growth Trends"
- "Sales by Product Category" + "Sales by Region" + "Monthly Sales Trends"
- "Customer Demographics" + "Purchase Patterns" + "Geographic Distribution"

**CHART COMBINATION RULES:**
- If Chart 1 shows individual entities (countries), Chart 2 should show grouped categories (continents)
- If Chart 1 shows current data, Chart 2 should show trends or comparisons  
- If Chart 1 shows one dimension, Chart 2 should show a different dimension entirely

**COMPREHENSIVE EXAMPLES FOR ROBUST CHART GENERATION:**

**Example 1: "Customer Analysis" (6 diverse charts):**
✅ EXCELLENT:
1. "Top 20 Customers by Revenue" (bar) - Individual rankings
2. "Customer Revenue Distribution" (pie) - Segment breakdown  
3. "Customer Acquisition Trends" (line) - Temporal analysis
4. "Revenue vs Order Frequency" (scatter) - Correlation analysis
5. "Customer Geographic Distribution" (table) - Regional perspective
6. "Customer Lifetime Value Percentiles" (bar) - Statistical analysis

**Example 2: "Product Performance Analysis" (5 diverse charts):**
✅ EXCELLENT:
1. "Product Category Revenue Share" (pie) - Overview breakdown
2. "Top Performing Individual Products" (bar) - Detailed rankings
3. "Product Sales Trends Over Time" (line) - Temporal patterns
4. "Price vs Sales Volume Correlation" (scatter) - Relationship analysis
5. "Product Performance by Region" (table) - Geographic context

**Example 3: "Financial Analysis" (6 diverse charts):**
✅ EXCELLENT:
1. "Monthly Revenue Overview" (line) - Temporal overview
2. "Revenue by Business Unit" (pie) - Categorical breakdown
3. "Top Revenue Generating Accounts" (bar) - Individual rankings
4. "Profit Margin Distribution" (bar) - Statistical distribution
5. "Revenue Growth Rate by Quarter" (line) - Growth analysis
6. "Revenue vs Expenses Correlation" (scatter) - Relationship analysis

**Example 4: "Geographic Analysis" (5 diverse charts):**
✅ EXCELLENT:
1. "Sales by Country/Region" (bar) - Geographic rankings
2. "Regional Market Share" (pie) - Proportional view
3. "Geographic Growth Trends" (line) - Temporal geographic analysis
4. "Population vs Sales Correlation by Region" (scatter) - Demographic analysis
5. "Regional Performance Metrics" (table) - Comprehensive regional data

Available chart types: bar, horizontal_bar, line, pie, scatter, table, lollipop, slope, bump, treemap, area, histogram, heatmap, stacked_bar

Guidelines:
- bar → categorical comparisons, counts, rankings, top/bottom lists (best for 3-20 categories)
- horizontal_bar → rankings with long category names, top 10 lists, professional dashboards (6-15 items)
- lollipop → clean rankings, emphasis on values, modern look (10-25 items)
- line → trends over time, sequential data, year-over-year analysis  
- slope → ranking changes between two periods, before/after comparisons
- bump → ranking changes over multiple time periods, trend analysis
- pie → ONLY for part-to-whole relationships with 3-6 categories MAX (never use for customer lists, individual records, or >6 items)
- scatter → ONLY for correlation/relationship between TWO NUMERIC variables (never use categorical data like names/government forms)
- table → detailed data, individual records, or >10 categories (20+ items)
- treemap → hierarchical data, nested categories, proportional visualization
- area → cumulative trends, stacked time series, filled line charts
- histogram → distribution analysis, frequency analysis, statistical patterns
- heatmap → two-dimensional correlation, geographic data, intensity mapping
- stacked_bar → component breakdown, multi-category comparisons

⚠️ **CRITICAL PIE CHART RESTRICTIONS:**
🚫 **ABSOLUTELY FORBIDDEN PIE CHARTS:**
- Individual customers/people (like "Customer Purchase Distribution")
- Individual products, cities, or detailed records
- ANY data with >6 distinct categories
- Lists of names, IDs, or specific entities

✅ **ONLY ALLOWED PIE CHARTS:**
- High-level categories: Continents (7 max), Product Lines (5-6), Departments
- Geographic regions: North/South/East/West, Major regions only
- Time periods: Quarters (4), Seasons (4), Years (if ≤6)
- Status categories: Active/Inactive, High/Medium/Low

**CRITICAL RULE: If you're tempted to use a pie chart, ask:**
- "Are there more than 6 slices?" → Use bar chart instead
- "Are these individual people/customers?" → Use bar chart instead  
- "Are these specific items/products?" → Use bar chart instead

**GOOD pie chart examples:**
- "Sales by Continent" (7 continents max)
- "Revenue by Product Category" (4-5 categories)
- "Orders by Quarter" (4 quarters)

**BAD pie chart examples (USE BAR INSTEAD):**
- "Customer Purchase Distribution" (59 customers = TERRIBLE!)
- "Sales by Individual Product" (100+ products)
- "Revenue by City" (50+ cities)
- "Purchases by Employee" (20+ employees)

⚠️ **SCATTER CHART REQUIREMENTS:**
🚫 **ABSOLUTELY FORBIDDEN SCATTER CHARTS:**
- Any chart where X or Y axis would be categorical text (names, government forms, categories)
- Charts with only one numeric variable
- Lists of countries/cities without numeric relationships

✅ **ONLY ALLOWED SCATTER CHARTS:**
- Two numeric variables showing correlation: Population vs GNP, Sales vs Profit, Age vs Income
- Numeric performance metrics: Revenue vs Customers, Rating vs Box Office
- Scientific/statistical relationships: Height vs Weight, Temperature vs Sales

**GOOD scatter chart examples:**
- "Population vs Economic Output" (Population on X, GNP on Y)
- "Movie Rating vs Box Office Revenue" (Rating on X, Revenue on Y)
- "Customer Age vs Purchase Amount" (Age on X, Amount on Y)

**BAD scatter chart examples (USE BAR/TABLE INSTEAD):**
- "Countries by Government Form" (Government Form is categorical!)
- "Customer Names vs Purchase History" (Names are not numeric!)
- "Product Categories vs Sales" (Categories are not continuous!)

**CRITICAL RULE: If either axis would show text labels instead of numeric scales, DO NOT use scatter plot!**

⚠️ **TABLE CHART REQUIREMENTS:**
- For table charts, ALWAYS select MULTIPLE DIVERSE columns to show comprehensive data
- NEVER repeat the same column twice in a table
- Include identifying columns (Name, Title, etc.) + multiple data columns (Population, GNP, LifeExpectancy, etc.)
- Example: For countries, select Name, Population, GNP, LifeExpectancy, SurfaceArea (NOT just Name, LifeExpectancy, LifeExpectancy)
- For population data: Show UNIQUE entities (countries/cities), not repetitive regions
- AVOID: Multiple rows with same region/category name - this creates confusing tables
- PREFER: Country-level data over historical/regional breakdowns for tables
- Example: Instead of "Australia and New Zealand" appearing 3 times, show 3 different countries

🔥 **CRITICAL SQL GENERATION RULES:**
- When generating SQL for any chart type, ensure each column in SELECT is UNIQUE
- For detailed/table data: Select 4-6 DIFFERENT columns that provide varied insights
- FORBIDDEN: SELECT a.Name, a.Population, a.Population (duplicate Population)
- REQUIRED: SELECT a.Name, a.Population, a.GNP, a.LifeExpectancy (all different)
- Each column must add new information, not repeat existing data

📊 **CHART-SPECIFIC SQL REQUIREMENTS:**
- **Bar Charts**: For "largest X by Y", ensure ONE result per category (use window functions/subqueries)
- **Pie Charts**: ONLY for high-level groupings with ≤6 categories. Group individual items into broader categories.
  - ✅ GOOD: "SELECT continent, SUM(population) FROM country GROUP BY continent" (7 continents)
  - ❌ BAD: "SELECT customer_name, total_purchases FROM customers" (59 individual customers)
  - **RULE**: If query returns >6 rows, use bar chart instead of pie
- **Tables**: Show UNIQUE entities with comprehensive data - avoid repetitive rows.
  - ✅ GOOD: "SELECT DISTINCT a.Name, a.Population, a.GNP, a.LifeExpectancy FROM country a ORDER BY a.Population DESC LIMIT 50"
  - ✅ GOOD: "SELECT a.continent, AVG(a.population), COUNT(*) as countries FROM country a GROUP BY a.continent"  
  - ❌ BAD: "SELECT a.region, a.year, a.population FROM historicaldata a" (repetitive regions)
  - ❌ BAD: Historical/time-series data that shows same entity multiple times
  - **RULE**: Each row should represent a UNIQUE entity (country, customer, product), not repeated categories

🎯 **CRITICAL: ALWAYS SORT AND LIMIT LARGE DATASETS**
- **ALWAYS use ORDER BY** to get the most important/relevant results first
- **For rankings**: ORDER BY [metric] DESC (highest first)
- **For alphabetical**: ORDER BY [name] ASC  
- **For time series**: ORDER BY [date] DESC (most recent first)
- **Examples**:
  - ✅ "SELECT a.Name, a.Population FROM country a ORDER BY a.Population DESC" (largest countries first)
  - ✅ "SELECT a.title, b.avg_rating FROM movie a JOIN ratings b ON a.id = b.movie_id ORDER BY b.avg_rating DESC" (best movies first)
  - ✅ "SELECT a.Name, SUM(b.Total) FROM customer a JOIN invoice b ON a.CustomerId = b.CustomerId GROUP BY a.Name ORDER BY SUM(b.Total) DESC" (top customers first)
- **Backend will automatically limit results**: Pie (6), Bar (20), Line (50), Scatter (100), Table (50)

Database Schema: {schema}
Question: {question}

Return JSON format with 3-6 suggestions for maximum analytical robustness:
{{
  "suggestions": [
    {{
      "chart_type": "bar|line|pie|scatter|table",
      "title": "Descriptive title for this chart",
      "reason": "Why this chart type is useful for this data",
      "sql_focus": "What aspect of the data this chart should focus on",
      "analysis_level": "overview|detailed|comparative|advanced|contextual"
    }}
    // MINIMUM 3 charts required for any question
    // MAXIMUM 6 charts to maintain performance
    // Each chart MUST provide unique analytical perspective
    // Include mix of analysis levels: overview + detailed + comparative/advanced
  ]
}}

🔥 **ROBUSTNESS VALIDATION CHECKLIST:**
Before finalizing suggestions, verify:
✅ At least 3 different chart types
✅ At least 2 different analysis levels (overview, detailed, comparative, etc.)
✅ No redundant data perspectives (same entities with different metrics)
✅ Mix of aggregation levels (individual, grouped, summary)
✅ Include temporal analysis if date/time data available
✅ Include comparative analysis when possible
✅ Each chart answers a different analytical question

📊 **COMPREHENSIVE RANKING ANALYSIS EXAMPLE:**
For a question like "How are entity rankings determined?", generate charts like:

1. **"Volume Rankings by Category"** (horizontal_bar)
   - SQL Focus: "COUNT(record_id) grouped by category, ordered by count DESC"
   - Purpose: Show which categories have the most records/activity
   
2. **"Entity Activity Distribution"** (lollipop)  
   - SQL Focus: "Entity inbound + outbound activity counts, ranked by total volume"
   - Purpose: Identify most active entities by combined metrics
   
3. **"Relationship Frequency Analysis"** (horizontal_bar)
   - SQL Focus: "Most common relationships by count, entity-to-entity pairs"
   - Purpose: Show which connections have the highest frequency
   
4. **"Temporal Activity Patterns"** (line)
   - SQL Focus: "Activity counts by time period, showing operational patterns"
   - Purpose: Reveal temporal patterns in entity behavior
   
5. **"Category Market Share"** (pie)
   - SQL Focus: "Percentage of total activity per category, top 6 categories only"
   - Purpose: Show market dominance and competitive landscape
""")

# Cache for schema ordering (per database)
_schema_order_cache = {}

def get_schema(database_name="zigment"):
    """Get database schema information from API"""
    try:
        schema_dict = fetch_schema_from_api()
        # Return as formatted JSON string for compatibility with existing code
        return json.dumps(schema_dict, indent=2)
    except Exception as e:
        print(f"Error fetching schema: {e}")
        return "{}"


def get_available_databases():
    """Return registered database names with simple descriptions."""
    available: dict[str, str] = {}
    for name in databases.keys():
        available[name] = f"Configured database '{name}'"
    return available

# Simple in-memory cache for counts (expires after 5 minutes)
_counts_cache = {}
_counts_cache_time = {}

# Persistent SQLite cache for schema counts
SCHEMA_CACHE_DB = "schema_counts_cache.db"

def init_schema_cache_db():
    """Initialize the persistent schema counts cache database"""
    conn = sqlite3.connect(SCHEMA_CACHE_DB)
    cursor = conn.cursor()
    
    # Create table to store schema metadata and counts
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_counts (
            database_name TEXT PRIMARY KEY,
            schema_text TEXT,
            counts_json TEXT,
            computed_at TEXT,
            row_count INTEGER,
            table_count INTEGER
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"📊 Schema counts cache database initialized: {SCHEMA_CACHE_DB}")

# Initialize on startup
try:
    init_schema_cache_db()
except Exception as e:
    print(f"⚠️ Failed to initialize schema cache DB: {e}")

def manage_schema_cache(database_name: str, schema_text: str = None, counts: dict = None, max_age_hours: int = 24):
    """Manage schema counts cache - save or load based on parameters provided"""
    try:
        import json
        from datetime import timedelta
        
        conn = sqlite3.connect(SCHEMA_CACHE_DB)
        cursor = conn.cursor()
        
        if schema_text is not None and counts is not None:
            # Save mode
            total_rows = sum(counts.get('tables', {}).values())
            table_count = len(counts.get('tables', {}))
        
            cursor.execute("""
                INSERT OR REPLACE INTO schema_counts 
                (database_name, schema_text, counts_json, computed_at, row_count, table_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                database_name,
                schema_text[:5000],  # Store first 5000 chars of schema
                json.dumps(counts),
                datetime.now().isoformat(),
                total_rows,
                table_count
            ))
            
            conn.commit()
            conn.close()
            print(f"💾 Saved counts to persistent cache for {database_name} ({total_rows:,} rows, {table_count} tables)")
            return True
            
        else:
            # Load mode
            cursor.execute("""
                SELECT counts_json, computed_at, row_count, table_count
                FROM schema_counts
                WHERE database_name = ?
            """, (database_name,))
            
            row = cursor.fetchone()
            conn.close()
            
        if not row:
            return None
        
        counts_json, computed_at_str, total_rows, table_count = row
        computed_at = datetime.fromisoformat(computed_at_str)
        age = datetime.now() - computed_at
        
        # Check if cache is still fresh
        if age > timedelta(hours=max_age_hours):
            print(f"⏰ Persistent cache expired for {database_name} (age: {age.total_seconds()/3600:.1f}h)")
            return None
        
        counts = json.loads(counts_json)
        print(f"📦 Loaded counts from persistent cache for {database_name} (age: {age.total_seconds()/60:.1f}m, {total_rows:,} rows)")
        return counts
        
    except Exception as e:
        print(f"⚠️ Failed to manage persistent cache: {e}")
        return None

def get_table_and_column_counts(database_name: str, use_cache: bool = True) -> dict:
    """Return table row counts for available collections (API-based).
    
    Note: Fetches counts by running COUNT(*) queries for each collection.
    """
    try:
        # Fetch schema to get list of collections
        schema = fetch_schema_from_api()
        collections = schema.get("collections", [])
        
        table_counts = {}
        
        # Get count for each collection
        for collection in collections[:10]:  # Limit to first 10 to avoid slowdown
            collection_name = collection.get("name", "").upper()
            # Use lowercase name for actual query
            query_name = collection_name.lower().replace("_", "")
            
            try:
                # Run COUNT query
                count_query = f"SELECT COUNT(*) as count FROM {query_name} LIMIT 1"
                result = execute_noql_query(count_query)
                
                if result.get("success") and result.get("data"):
                    data = result["data"]
                    if isinstance(data, dict) and "rows" in data:
                        rows = data["rows"]
                        if rows and len(rows) > 0:
                            count_val = rows[0][0] if isinstance(rows[0], (list, tuple)) else rows[0].get("count", 0)
                            table_counts[query_name] = int(count_val) if count_val else 0
                        else:
                            table_counts[query_name] = 0
                    else:
                        table_counts[query_name] = 0
            except Exception as e:
                print(f"⚠️ Could not get count for {query_name}: {e}")
                table_counts[query_name] = 0
        
        print(f"📊 Fetched counts for {len(table_counts)} collections")
        return {
            "tables": table_counts,
            "columns": {}  # Column-level counts not implemented for API mode
        }
    except Exception as e:
        print(f"⚠️ Error fetching table counts: {e}")
        return {
            "tables": {},
            "columns": {}
        }

def generate_table_size_guidance(counts: dict, threshold: int = 100000) -> str:
    """Generate smart prompting guidance based on table sizes.
    
    Args:
        counts: Dict with 'tables' and 'columns' from get_table_and_column_counts
        threshold: Row count threshold for considering a table "large" (default 100k)
    
    Returns:
        String with guidance to inject into prompts
    """
    if not counts or "tables" not in counts:
        return ""
    
    large_tables = []
    for table, row_count in counts["tables"].items():
        if row_count > threshold:
            large_tables.append(f"{table} ({row_count:,} rows)")
    
    if not large_tables:
        return ""
    
    guidance = f"""
⚠️ **LARGE TABLE WARNING - MANDATORY QUERY OPTIMIZATION:**

The following tables contain substantial data:
{chr(10).join(f"- {t}" for t in large_tables)}

🚨 **CRITICAL PERFORMANCE RULES (You MUST follow these):**
1. **ALWAYS use LIMIT clauses** - Default to LIMIT 50, max 500 for large tables
2. **Use WHERE filters aggressively** - Filter BEFORE joins/aggregations
3. **Avoid SELECT *** - Only select columns you need
4. **Use indexed columns in WHERE/JOIN** - Prefer primary/foreign keys
5. **Aggregate early with CTEs** - Pre-filter large tables in WITH clauses
6. **Use simple queries for exploration** - Avoid complex multi-table joins on first query
7. **Consider sampling** - For very large tables (>1M rows), use LIMIT even for aggregations
8. **Prefer COUNT(*) LIMIT X over full scans** - Show representative samples, not entire datasets

📊 **SMART QUERY PATTERNS FOR LARGE TABLES:**

Instead of: `SELECT * FROM large_table` (BAD - will timeout)
Use: `SELECT col1, col2, col3 FROM large_table WHERE indexed_col = 'value' LIMIT 50`

Instead of: `SELECT category, COUNT(*) FROM large_table GROUP BY category` (SLOW - full scan)
Use: `SELECT category, COUNT(*) FROM large_table WHERE date_col >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY category LIMIT 20`

Instead of: Complex 4-table join on large tables (VERY SLOW)
Use: CTE with early filtering:
```sql
WITH filtered_large AS (
  SELECT id, key_col FROM large_table WHERE filter_condition LIMIT 1000
)
SELECT ... FROM filtered_large fl JOIN small_table st ON fl.key_col = st.key_col LIMIT 50
```

💡 **GUIDELINE:** If a table has >100k rows, treat it as "expensive" and optimize aggressively.
If a table has >1M rows, always use LIMIT even in subqueries/CTEs.
"""
    return guidance.strip()

def run_query(query, database_name="zigment", return_columns=False):
    """Execute NoQL query via API, optionally returning column names"""
    try:
        # Normalize LLM output: strip markdown fences and enforce safe LIMIT
        cleaned_query = _strip_sql_fences(str(query))
        cleaned_query = ensure_limit(cleaned_query, 50)
        
        # Execute via API
        result = execute_noql_query(cleaned_query)
        
        print(f"🔍 API Response type: {type(result)}")
        print(f"🔍 API Response keys: {result.keys() if isinstance(result, dict) else 'N/A'}")
        
        # Check for API errors first
        if isinstance(result, dict):
            # Check if the API returned an error
            if result.get("success") == False or ("errors" in result and result.get("errors")):
                errors = result.get("errors", ["Unknown error"])
                error_msg = f"API Error: {errors}"
                print(f"❌ {error_msg}")
                print(f"🔍 Full API response: {result}")
                if not return_columns:
                    raise Exception(error_msg)
                else:
                    return [], []
            
            # Extract data from API response
            # API returns: {"success": true, "data": {"headers": [...], "rows": [...]}, "metadata": {...}}
            # Check if response has the nested structure
            if "data" in result and isinstance(result["data"], dict):
                # Extract from nested structure
                data_obj = result["data"]
                rows = data_obj.get("rows", [])
                headers = data_obj.get("headers", [])
                
                # Extract column names from headers
                columns = [h.get("key") for h in headers] if headers else []
                
                # If no headers, try metadata
                if not columns and "metadata" in result:
                    metadata_cols = result["metadata"].get("columns", [])
                    columns = [c.get("key") for c in metadata_cols] if metadata_cols else []
                
                # If still no columns and we have rows, extract from first row
                if not columns and rows and isinstance(rows[0], dict):
                    columns = list(rows[0].keys())
                
                # Convert list of dicts to list of tuples for compatibility
                if rows and isinstance(rows[0], dict):
                    # Convert each row, handling nested arrays/objects
                    data = []
                    for row in rows:
                        row_values = []
                        for col in columns:
                            val = row.get(col)
                            # Handle arrays: take first element or convert to string
                            if isinstance(val, list):
                                if len(val) == 1:
                                    val = val[0]  # Single item array -> scalar
                                elif len(val) == 0:
                                    val = None
                                else:
                                    val = ', '.join(str(v) for v in val[:3])  # Multiple items -> comma-separated string (max 3)
                            # Handle empty strings for numeric fields
                            elif val == '' and any(keyword in col.lower() for keyword in ['count', 'total', 'sum', 'avg', 'average']):
                                val = 0
                            row_values.append(val)
                        data.append(tuple(row_values))
                else:
                    data = rows if rows else []
                    
                print(f"📊 Extracted {len(data)} rows with {len(columns)} columns: {columns}")
                
            else:
                # Fallback: try old format
                data = result.get("data", result.get("rows", result.get("results", [])))
                columns = result.get("columns", result.get("fields", []))
                
                # Ensure data is not None
                if data is None:
                    data = []
                if columns is None:
                    columns = []
                
                # If data is a list of dicts, extract column names from first row
                if data and isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict) and not columns:
                        columns = list(data[0].keys())
                        # Convert list of dicts to list of tuples for compatibility
                        data = [tuple(row.values()) for row in data]
                
                print(f"📊 Extracted {len(data)} rows with {len(columns)} columns (fallback format)")
            
            if not return_columns:
                # Return as string representation for compatibility
                return str(data)
            else:
                return data, columns
        else:
            if not return_columns:
                return str(result)
            else:
                return result, []
                
    except Exception as e:
        print(f"❌ Error in run_query: {e}")
        import traceback
        traceback.print_exc()
        if not return_columns:
            raise e
        else:
            return [], []
# ===== Data Validation and Guardrails =====

def validate_sql_result(data, columns, query_type="general"):
    """Validate SQL query results and return validation status"""
    if not data:
        return {
            "valid": False,
            "error": "No data returned from query",
            "suggestion": "Try a different question or check if the data exists"
        }
    
    if not columns:
        return {
            "valid": False,
            "error": "No column information available",
            "suggestion": "Query may have failed to execute properly"
        }
    
    # Check for meaningful data (not just empty rows)
    meaningful_rows = 0
    for row in data:
        if any(cell is not None and str(cell).strip() != "" for cell in row):
            meaningful_rows += 1
    
    if meaningful_rows == 0:
        return {
            "valid": False,
            "error": "Query returned empty or null data",
            "suggestion": "Try a different question or check your criteria"
        }
    
    # ⚠️ CRITICAL: Reject single data point (useless for charts)
    if meaningful_rows == 1:
        return {
            "valid": False,
            "error": "Only one data point returned - not enough for visualization",
            "suggestion": "Try asking for a comparison, trend, or distribution instead of a single value"
        }
    
    # Check for reasonable data size
    if len(data) > 10000:
        return {
            "valid": False,
            "error": f"Query returned too much data ({len(data)} rows)",
            "suggestion": "Please be more specific in your question"
        }
    
    return {
        "valid": True,
        "row_count": len(data),
        "meaningful_rows": meaningful_rows,
        "columns": len(columns)
    }

def check_question_relevance(question: str, database_name: str) -> dict:
    """Check if the question is relevant to the database schema"""
    if not question or len(question.strip()) < 3:
        return {
            "relevant": False,
            "error": "Question is too short or empty",
            "suggestion": "Please provide a more detailed question"
        }
    
    # Get basic schema info
    try:
        schema = get_schema(database_name)
        if not schema:
            return {
                "relevant": False,
                "error": "Unable to access database schema",
                "suggestion": "Please check if the database is available"
            }
    except Exception as e:
        return {
            "relevant": False,
            "error": f"Database access failed: {str(e)}",
            "suggestion": "Please check if the database is properly configured"
        }
    
    # Basic relevance checks
    question_lower = question.lower()
    
    # Check for obviously irrelevant questions
    irrelevant_patterns = [
        "how to cook", "recipe", "weather", "news", "sports score",
        "stock price", "cryptocurrency", "bitcoin", "movie", "song",
        "book recommendation", "travel advice", "medical advice"
    ]
    
    for pattern in irrelevant_patterns:
        if pattern in question_lower:
            return {
                "relevant": False,
                "error": "Question appears to be unrelated to database content",
                "suggestion": "Please ask questions about the data in this database"
            }
    
    return {
        "relevant": True,
        "schema_accessible": True
    }

def create_error_response(error_type: str, message: str, suggestion: str = None) -> dict:
    """Create a standardized error response"""
    return {
        "success": False,
        "error_type": error_type,
        "error": message,
        "suggestion": suggestion or "Please try rephrasing your question or check if the data exists",
        "data": None,
        "charts": []
    }

def create_no_data_response(question: str) -> dict:
    """Create a response when no meaningful data is found"""
    return {
        "success": False,
        "error_type": "no_data",
        "error": "No relevant data found for your question",
        "suggestion": "Try asking about different aspects of the data or use more general terms",
        "question": question,
        "data": None,
        "charts": []
    }

# ===== Any-DB dynamic introspection and universal prompt =====

def sample_database_tables(database_name: str, max_rows: int = 3, max_tables: int = 10) -> dict:
    """Return a small sample from each collection (API-based).
    
    Note: Fetches sample rows by running SELECT * LIMIT queries for each collection.
    """
    try:
        # Fetch schema to get list of collections
        schema = fetch_schema_from_api()
        collections = schema.get("collections", [])
        
        samples = {}
        
        # Get sample for each collection
        for collection in collections[:max_tables]:
            collection_name = collection.get("name", "").upper()
            # Use lowercase name for actual query
            query_name = collection_name.lower().replace("_", "")
            
            try:
                # Run sample query
                sample_query = f"SELECT * FROM {query_name} LIMIT {max_rows}"
                result = execute_noql_query(sample_query)
                
                if result.get("success") and result.get("data"):
                    data = result["data"]
                    if isinstance(data, dict) and "rows" in data:
                        rows = data["rows"]
                        if rows:
                            samples[query_name] = rows[:max_rows]
                        else:
                            samples[query_name] = []
                    else:
                        samples[query_name] = []
            except Exception as e:
                print(f"⚠️ Could not get sample for {query_name}: {e}")
                samples[query_name] = []
        
        print(f"📄 Fetched samples for {len(samples)} collections")
        return samples
    except Exception as e:
        print(f"⚠️ Error fetching table samples: {e}")
        return {}

# Generic, database-agnostic SQL generation prompt
anydb_sql_prompt = ChatPromptTemplate.from_template(
    """
You are an expert NoQL query generator.
You will be given a database SCHEMA (tables and columns), TABLE/ COLUMN COUNTS, and small SAMPLES (first rows per table).
Write ONE NoQL query that answers the QUESTION using only the available tables/columns.

**CRITICAL: ACTUAL COLLECTION NAMES (USE THESE IN YOUR QUERY)**
The schema shows uppercase names, but you MUST use these EXACT lowercase collection names in your queries:
- EVENT → events
- CONTACT → contacts
- CORE_CONTACT → corecontacts
- CORECONTACTS → corecontacts
- CHAT_HISTORY → chathistories
- CHATHISTORIES → chathistories
- CONTACT_TAG → contacttags
- CONTACTTAGS → contacttags
- ORG_AGENT → orgagent
- ORGANIZATION → organization
Example: If schema shows "CONTACT" or "CONTACTS", you MUST write "contacts" in your query.
**CRITICAL: DATE/TIME HANDLING IN NOQL**
❌ NEVER use MySQL: UNIX_TIMESTAMP(), DATE_SUB(), NOW(), FROM_UNIXTIME(), DAYOFWEEK(), etc.
🎯 **Unix Timestamp Conversion (stored as SECONDS):**
For fields like `timestamp`, `created_at`, `updated_at`, `created_at_timestamp`:
- **MUST convert**: `TO_DATE(field * 1000)` before using date functions
- Example: `SELECT DAY_OF_WEEK(TO_DATE(timestamp * 1000)) AS dow FROM events`
- Example: `SELECT DATE_TRUNC(TO_DATE(created_at * 1000), 'day') FROM contacts`

✅ NoQL Date Functions (after TO_DATE conversion):
- `DAY_OF_WEEK(TO_DATE(timestamp * 1000))` ← **BEST for day of week analysis**
- `MONTH(TO_DATE(field * 1000))`, `YEAR(TO_DATE(field * 1000))`
- `DATE_TRUNC(TO_DATE(field * 1000), 'day'|'month'|'year')`
- ⚠️ Avoid `EXTRACT(dow ...)` on converted timestamps - use `DAY_OF_WEEK()` instead

⚡ For filtering only (faster without conversion):
- `WHERE timestamp >= 1710374400` (numeric comparison)

NoQL Query Rules:
- **Keep it simple:** Group by ONE dimension only; avoid multi-column grouping unless absolutely necessary
- **Avoid unnecessary JOINs:** If a field exists in the main table, don't join just to get the same field
  * ✅ GOOD: `SELECT t.[field], COUNT(*) FROM [table] t GROUP BY t.[field]`
  * ❌ BAD: `SELECT b.[field], COUNT(a._id) FROM [table_a] a JOIN [table_b] b ON a.[key] = b.[key] GROUP BY b.[field]` (join not needed)
- **If you MUST join:** Use COUNT(DISTINCT ...) to avoid inflated counts from 1:N relationships
- **Always prefix columns with table alias** to avoid ambiguous column errors (e.g., `t.[column]` not `[column]`)
- Use explicit JOIN ... ON; never use NATURAL JOIN
- Only reference columns/tables that exist in SCHEMA
- For grouped queries, include all non-aggregated columns in GROUP BY
- Prefer efficient filters and aggregations; avoid SELECT * for large outputs
- Add a reasonable LIMIT if the result set could be large (default 50)
- Return ONLY the SQL, no explanations
- Use lowercase collection names as specified above

🚨 **CRITICAL: AVOID SINGLE-ITEM RESULTS FOR VISUALIZATIONS**
- If question asks for "frequency", "comparison", "distribution", or "top N" → Return MULTIPLE rows (at least 2-10)
- ❌ BAD: SELECT status, COUNT(*) FROM contacts WHERE status = 'NEW' (only 1 row!)
- ✅ GOOD: SELECT status, COUNT(*) FROM contacts GROUP BY status ORDER BY COUNT(*) DESC LIMIT 10 (multiple rows for comparison)
- For specific entities: If user mentions ONE entity, show it compared to others
- Example: "NEW status contacts" → Show NEW AND other statuses for context

SCHEMA:
{schema}

COUNTS (tables + non-null per column):
{counts}

SAMPLES (first rows):
{samples}

{table_size_guidance}

QUESTION: {question}
"""
)

def create_anydb_sql_chain(database_name: str):
    def _invoke(payload):
        question = payload["question"]
        schema_str = get_schema(database_name)
        llm = ChatOpenAI(model_name="gpt-3.5-turbo")
        formatted_prompt = NOQL_DIRECT_PROMPT.format(schema=schema_str, question=question)
        result = llm.invoke(formatted_prompt)
        return result.text.strip()
    class DummyChain:
        def invoke(self, payload):
            return _invoke(payload)
    return DummyChain()

def validate_sql_against_schema(query: str, database_name: str) -> dict:
    """
    Validate SQL query against database schema (API-based - validation happens server-side).
    Returns {"valid": bool, "error": str, "corrected_query": str}
    """
    # In API-based mode, validation happens server-side
    print(f"ℹ️ API-based mode: Schema validation skipped (handled by server)")
    return {
        "valid": True,
        "error": None,
        "corrected_query": None
    }

def answer_anydb_question(question: str, database_name: str):
    """Answer question with strict validation and guardrails"""
    
    # Step 1: Check question relevance
    relevance_check = check_question_relevance(question, database_name)
    if not relevance_check["relevant"]:
        return create_error_response(
            "irrelevant_question", 
            relevance_check["error"], 
            relevance_check["suggestion"]
        )
    
    try:
        # Step 2: Generate SQL query
        chain = create_anydb_sql_chain(database_name)
        query = chain.invoke({"question": question})
        query = _strip_sql_fences(query)
        query = ensure_limit(query, 50)
        
        # Step 3: Execute query with error handling
        try:
            rows, columns = run_query(query, database_name, return_columns=True)
        except Exception as e:
            return create_error_response(
                "sql_execution_error",
                f"Failed to execute SQL query: {str(e)}",
                "Please try rephrasing your question or check if the data exists"
            )
        
        # Step 4: Validate query results
        validation = validate_sql_result(rows, columns)
        if not validation["valid"]:
            return create_error_response(
                "invalid_data",
                validation["error"],
                validation["suggestion"]
            )
        
        # Step 5: Format data for presentation
        try:
            formatted = format_data_for_chart_type(rows, "table", question, columns)
        except Exception as e:
            return create_error_response(
                "data_formatting_error",
                f"Failed to format data: {str(e)}",
                "The query returned data but it couldn't be formatted for display"
            )
        
        # Step 6: Final validation - ensure we have meaningful formatted data
        if not formatted or len(formatted) == 0:
            return create_no_data_response(question)
        
        return {
            "success": True,
            "sql": query, 
            "columns": columns, 
            "rows": rows, 
            "data": formatted,
            "validation": validation
        }
        
    except Exception as e:
        return create_error_response(
            "processing_error",
            f"An unexpected error occurred: {str(e)}",
            "Please try again or contact support if the issue persists"
        )


# SQL prompt template
sql_prompt = ChatPromptTemplate.from_template(
    """
You are an expert NoQL query generator specializing in advanced analytics and complex query generation.
Your task is to generate sophisticated, insightful NoQL queries that go beyond simple SELECT statements.
The query should run directly without any extra formatting (no ```sql ...``` blocks, no explanations).

**CRITICAL: ACTUAL COLLECTION NAMES (USE THESE IN YOUR QUERY)**
The schema shows uppercase names, but you MUST use these EXACT lowercase collection names in your queries:
- EVENT → events
- CONTACT → contacts
- CONTACTS → contacts
- CORE_CONTACT → corecontacts
- CORECONTACTS → corecontacts
- CHAT_HISTORY → chathistories
- CHATHISTORIES → chathistories
- CONTACT_TAG → contacttags
- CONTACTTAGS → contacttags
- ORG_AGENT → orgagent
- ORGANIZATION → organization

Example: If schema shows "CONTACT" or "CONTACTS", you MUST write "contacts" in your query.
Always use lowercase collection names as specified above.

**CRITICAL: DATE/TIME HANDLING IN NOQL**
Many timestamp fields are stored as Unix epoch seconds (numbers). For date filtering:
- ❌ NEVER use: UNIX_TIMESTAMP(), DATE_SUB(), NOW(), INTERVAL, FROM_UNIXTIME(), DATE_FORMAT()
- ✅ Instead use direct numeric comparison with pre-calculated Unix timestamps:
  - Last 6 months: WHERE timestamp_field >= 1710374400 (calculate: current_time - 6*30*24*3600)
  - Last 30 days: WHERE timestamp_field >= 1726444800 (calculate: current_time - 30*24*3600)
  - Specific date: WHERE timestamp_field >= 1704067200 (Jan 1, 2024 = 1704067200)
- For current time reference: Use a recent timestamp like 1729000000 (Oct 2024)
- Example: `WHERE created_at_timestamp >= 1710000000` NOT `WHERE created_at_timestamp >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 6 MONTH))`

🎯 **PRIORITY: Generate Complex, Analytical Queries**
- Favor window functions, subqueries, and advanced SQL features over simple SELECT statements
- **CRITICAL: Use RANK(), ROW_NUMBER(), DENSE_RANK() ONLY when explicitly requested**
- Implement business intelligence patterns (cohorts, trends, comparisons)
- Create queries that provide deep insights, not just basic data retrieval
- Use advanced aggregations, conditional logic, and statistical functions
- **CRITICAL: Apply performance optimization techniques in every query**

🚨 **RANKING FUNCTION RULES:**
- **DON'T add ranking functions unless the question explicitly asks for rankings**
- Simple counts/aggregations should use ORDER BY, not RANK() functions
- Questions like "count of X by Y" should NOT include RANK() columns
- Only use ranking when user asks for "rank", "position", "1st/2nd", etc.

⚡ **PERFORMANCE-FIRST QUERY GENERATION:**
- Always select specific columns, never use SELECT *
- Apply strategic LIMIT clauses to control result set size
- Use efficient WHERE clause patterns (avoid functions on columns)
- Implement optimal JOIN strategies and ordering
- Prefer EXISTS over IN for subqueries
- Use CTEs to break down complex logic
- Apply early filtering to reduce data processing
- Choose appropriate operators and data type comparisons

ACTUAL DATABASE SCHEMA:
{schema}

SAMPLE DATA (first few rows per table):
{samples}

Follow these strict rules when generating SQL:

⚠️  CRITICAL: ALWAYS use table aliases for ALL column references to avoid ambiguous column errors!
⚠️  CRITICAL: Once you assign a table alias (e.g., table AS b), ALWAYS use that exact alias (b) for ALL columns from that table!
⚠️  CRITICAL: Use ONLY tables and columns that exist in the provided SCHEMA above!

1. **Schema Usage and Validation**
   - CRITICAL: Use ONLY tables and columns that exist in the provided schema above
   - Before writing any query, carefully examine the schema to identify:
     * Available table names and their exact spelling
     * Column names and their exact case-sensitive spelling
     * Data types of each column
     * Primary and foreign key relationships
   - Never assume column names based on common database patterns
   - If a requested data point doesn't exist in the schema, use the closest available alternative
   - Always verify that every column referenced in your query exists in the schema

2. **Table Relationships and Join Patterns**
   - Examine the provided SCHEMA carefully to identify foreign key relationships
   - Look for columns with similar names across tables (e.g., id, user_id, product_id)
   - Use appropriate JOIN types based on the relationship requirements
   - Always use explicit JOIN syntax (JOIN...ON) instead of WHERE clause joins
   - **PERFORMANCE**: Order JOINs from smallest to largest tables when possible
   - **PERFORMANCE**: Ensure JOIN conditions use indexed columns (primary/foreign keys)

3. **Query Optimization Rules (MANDATORY)**
   ⚡ **SELECT Clause Optimization:**
   - NEVER use SELECT * - always specify exact columns needed
   - Select only columns that will be used in the final output
   - Use column aliases for calculated fields to improve readability
   
   ⚡ **WHERE Clause Optimization:**
   - Place most selective conditions first in WHERE clause
   - Avoid functions on columns: use `date >= '2023-01-01' AND date < '2024-01-01'` instead of `YEAR(date) = 2023`
   - Use = instead of LIKE when exact matches are needed
   - Use EXISTS instead of IN for subqueries: `WHERE EXISTS (SELECT 1 FROM...)` 

⚡ **CRITICAL: CTE and Column Reference Rules:**
   - When using CTEs, ONLY reference columns that exist in the CTE output
   - In JOINs with CTEs, use columns from the CTE, not from newly joined tables
   - Example: If CTE selects `customer_id`, use `cte.customer_id`, not `new_table.customer_id`
   - Always verify column sources: CTE columns vs. joined table columns
   - Use table aliases consistently: if you alias a table as 'a', always use 'a.column_name'
   
   ⚡ **JOIN Optimization:**
   - Start JOINs with the table that will return the fewest rows
   - Use INNER JOIN for required relationships, LEFT JOIN only when needed
   - Avoid RIGHT JOIN (reorder tables to use LEFT JOIN instead)
   
   ⚡ **Aggregation and Grouping Optimization:**
   - Use GROUP BY instead of DISTINCT when possible
   - Only use ORDER BY when the output order is actually required
   - Use UNION ALL instead of UNION when duplicates are acceptable
   - Apply LIMIT clauses to prevent unnecessarily large result sets


    **Table Aliasing and Reference Standards - MANDATORY**
   🚨 **CRITICAL RULE: ONLY use simple single-letter aliases in alphabetical order:**
     * First table: `a` (main table)
     * Second table: `b` (first join)
     * Third table: `c` (second join) 
     * Fourth table: `d` (third join)
     * Fifth table: `e` (fourth join)
     * Sixth table: `f` (fifth join)
   
   🚫 **FORBIDDEN:** Never use aliases like c1, c2, co, ci, sub_c, etc.
   ✅ **REQUIRED:** Always use a, b, c, d, e, f in order
   
   - CRITICAL: Ensure all column references use the correct table alias
   - Example: If country is aliased as 'a', use 'a.Population', not 'country.Population'
   - Always prefix columns with their table alias to avoid ambiguity
   - When self-joining, use distinct aliases like 'a' and 'b' for the same table
   - In subqueries, continue the pattern: use 'c', 'd', 'e', 'f' for subquery tables

7. **Query Structure and Formatting Standards**
   - Write clean SQL with proper indentation and line breaks:
     ```
     SELECT column1, column2, calculation
     FROM table1 AS a
     JOIN table2 AS b ON a.key = b.key
     WHERE condition
     GROUP BY grouping_columns
     HAVING having_condition
     ORDER BY sort_columns
     LIMIT number;
     ```
   - Always use explicit JOIN syntax (JOIN...ON) instead of WHERE clause joins
   - Use appropriate JOIN types (MySQL-specific guidance):
     * INNER JOIN: Use for strict matches on FK relationships (default for most analytics queries)
     * LEFT JOIN: Use when you must keep all rows from the left side (e.g., show entities with zero activity)
     * RIGHT JOIN: Generally avoid by swapping table order; use only if it improves clarity
     * FULL OUTER JOIN: Not supported in MySQL. Emulate via UNION of LEFT and RIGHT joins when needed
       Example:
       ```sql
       SELECT a.id, b.id
       FROM t1 a LEFT JOIN t2 b ON a.id=b.id
       UNION
       SELECT a.id, b.id
       FROM t1 a RIGHT JOIN t2 b ON a.id=b.id;
       ```
     * NATURAL JOIN: Avoid. Always specify explicit ON conditions for clarity and to prevent accidental column matches
   - For aggregations, ensure all non-aggregate columns are in GROUP BY
   - Use meaningful column names in SELECT, especially for calculations
   - Sort results appropriately (DESC for top/highest, ASC for lowest)

7.a **Join Strategy**
   - Prefer INNER JOIN for standard relationships
   - Use LEFT JOIN to include entities with zero related rows
   - Avoid RIGHT JOIN by reordering tables; use only if necessary
   - FULL OUTER JOIN: emulate with LEFT JOIN UNION RIGHT JOIN in MySQL
   - Do NOT use NATURAL JOIN; always use explicit ON keys
   - **CRITICAL:** Study the SCHEMA carefully to understand which table owns which columns

8. **Alias Sanity Checklist (MANDATORY before returning the query)**
  - List the aliases you defined in FROM/JOIN (e.g., a, b, c). Use ONLY these aliases everywhere.
  - Ensure every column reference is prefixed with a defined alias (a.col), never bare column names.
  - Ensure no alias is used that was not defined (e.g., selecting c.country when only a, b exist).
  - For AirportDB reserved columns, ensure backticks with alias: a.`from`, a.`to`.
  - Verify all GROUP BY columns match non-aggregate columns in SELECT.
  - Verify join keys match the FK patterns provided (e.g., flight.flightno = flightschedule.flightno).

9. **Data Validation and Error Prevention**
   - Before finalizing the query, mentally trace through each table alias
   - Verify that all JOIN conditions match the foreign key relationships in the schema
   - Ensure all column names exactly match the schema (case-sensitive)
   - Check that aggregate functions (SUM, COUNT, AVG) are used appropriately
   - Validate that date columns are handled correctly if time-based filtering is needed
   - For calculations, ensure mathematical operations make logical sense
10. **Advanced Complex Query Patterns**
   🚀 **Window Functions for Advanced Analytics:**
   - Use ROW_NUMBER(), RANK(), DENSE_RANK() for ranking within groups
   - Use LEAD(), LAG() for comparing values across rows
   - Use SUM() OVER(), AVG() OVER() for running totals and moving averages
   
   Examples:
   ```sql
   -- Top 3 products by sales in each category
   SELECT category, product_name, sales,
          ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rank_in_category
   FROM products
   HAVING rank_in_category <= 3;
   
   -- Running total of sales by month
   SELECT month, sales,
          SUM(sales) OVER (ORDER BY month) as running_total
   FROM monthly_sales;
   
   -- Compare current vs previous period
   SELECT month, sales,
          LAG(sales, 1) OVER (ORDER BY month) as prev_sales,
          sales - LAG(sales, 1) OVER (ORDER BY month) as growth
   FROM monthly_sales;
   ```
   
   🔍 **Complex Subqueries and CTEs:**
   - Use correlated subqueries for row-by-row comparisons
   - Use EXISTS/NOT EXISTS for filtering based on related data
   - Use IN/NOT IN with subqueries for set-based filtering
   - Use scalar subqueries in SELECT for calculated columns
   
   **CTE BEST PRACTICES (CRITICAL):**
   - Each CTE should have a clear, single purpose
   - CTE column names must match what you SELECT in the CTE
   - When JOINing to a CTE, only use columns that the CTE actually outputs
   - Don't mix CTE columns with new table columns incorrectly
   - Validate that every column reference exists in its source table/CTE
   
   Examples:
   ```sql
   -- ✅ CORRECT CTE Usage - Column references match CTE output
   WITH customer_totals AS (
     SELECT customer_id, SUM(order_total) as total_spent
     FROM orders
     GROUP BY customer_id
   ),
   customer_ranks AS (
     SELECT ct.customer_id, ct.total_spent,  -- ✅ Use ct.customer_id from CTE
            RANK() OVER (ORDER BY ct.total_spent DESC) as rank
     FROM customer_totals AS ct  -- ✅ Reference CTE columns correctly
   )
   SELECT cr.customer_id, cr.total_spent, cr.rank  -- ✅ Use CTE columns
   FROM customer_ranks AS cr
   WHERE cr.rank <= 10;
   
   -- ❌ WRONG CTE Usage - Mixing CTE and joined table columns incorrectly
   WITH order_counts AS (
     SELECT customer_id, COUNT(*) as order_count
     FROM orders
     GROUP BY customer_id
   )
   SELECT c.customer_name, c.customer_id,  -- ❌ WRONG: c.customer_id doesn't exist in this context
          oc.order_count
   FROM order_counts AS oc
   JOIN customers AS c ON oc.customer_id = c.customer_id;
   
   -- ✅ CORRECT Version
   WITH order_counts AS (
     SELECT customer_id, COUNT(*) as order_count
     FROM orders
     GROUP BY customer_id
   )
   SELECT c.customer_name, oc.customer_id,  -- ✅ Use oc.customer_id from CTE
          oc.order_count
   FROM order_counts AS oc
   JOIN customers AS c ON oc.customer_id = c.customer_id;
   ```
   
   📊 **Advanced Aggregations and Analytics:**
   - Use CASE WHEN for conditional aggregations
   - Use GROUP BY with ROLLUP for subtotals and grand totals
   - Use HAVING for filtering aggregated results
   - Combine multiple aggregation levels in single query
   
   Examples:
   ```sql
   -- Conditional aggregations (pivot-like behavior)
   SELECT 
     region,
     SUM(CASE WHEN product_type = 'A' THEN sales ELSE 0 END) as type_a_sales,
     SUM(CASE WHEN product_type = 'B' THEN sales ELSE 0 END) as type_b_sales,
     SUM(sales) as total_sales
   FROM sales_data
   GROUP BY region;
   
   -- Multi-level aggregations with subtotals
   SELECT region, product_category, SUM(sales) as total_sales
   FROM sales_data
   GROUP BY region, product_category WITH ROLLUP;
   ```
   
   🔗 **Advanced Join Patterns:**
   - Use self-joins for hierarchical data or comparisons within same table
   - Use multiple joins to traverse complex relationships
   - Use join conditions beyond simple equality (ranges, inequalities)
   - Use UNION/UNION ALL to combine results from different sources
   
   🚀 **OR Condition Optimization (CRITICAL):**
   - AVOID: `LEFT JOIN table t ON a.id = t.col1 OR a.id = t.col2` (forces full table scan)
   - BETTER: Use UNION ALL approach to normalize data first, then join
   - BEST: Use separate aggregated subqueries for each condition, then combine
   
   Example - Instead of inefficient OR join:
   ```sql
   -- ❌ SLOW: OR condition in JOIN
   SELECT a.name, COUNT(f.flight_id) AS total_flights
   FROM airport a
   LEFT JOIN flight f ON a.airport_id = f.`to` OR a.airport_id = f.`from`
   GROUP BY a.airport_id;
   
   -- ✅ FAST: UNION ALL approach
   WITH flight_endpoints AS (
     SELECT `from` AS airport_id, flight_id FROM flight
     UNION ALL
     SELECT `to` AS airport_id, flight_id FROM flight
   )
   SELECT a.name, COUNT(fe.flight_id) AS total_flights
   FROM airport a
   LEFT JOIN flight_endpoints fe ON a.airport_id = fe.airport_id
   GROUP BY a.airport_id;
   
   -- ✅ FASTEST: Separate aggregated subqueries (for large datasets)
   WITH departures AS (
     SELECT `from` AS airport_id, COUNT(*) AS dep_count
     FROM flight GROUP BY `from`
   ),
   arrivals AS (
     SELECT `to` AS airport_id, COUNT(*) AS arr_count  
     FROM flight GROUP BY `to`
   )
   SELECT a.name, 
          COALESCE(d.dep_count, 0) + COALESCE(arr.arr_count, 0) AS total_flights
   FROM airport a
   LEFT JOIN departures d ON a.airport_id = d.airport_id
   LEFT JOIN arrivals arr ON a.airport_id = arr.airport_id;
   ```
   
   Examples:
   ```sql
   -- Self-join for employee hierarchy
   SELECT e.name as employee, m.name as manager
   FROM employees AS e
   LEFT JOIN employees AS m ON e.manager_id = m.employee_id;
   
   -- Range joins for time-based analysis
   SELECT a.customer_id, COUNT(b.order_id) as orders_in_period
   FROM customers AS a
   LEFT JOIN orders AS b ON b.customer_id = a.customer_id 
     AND b.order_date BETWEEN a.registration_date AND DATE_ADD(a.registration_date, INTERVAL 30 DAY)
   GROUP BY a.customer_id;
   ```
   
   📈 **Time Series and Trend Analysis:**
   - Use DATE functions for time-based grouping and filtering
   - Calculate period-over-period changes and growth rates
   - Handle missing periods with date generation or LEFT JOINs
   - Use time-based window functions for trends
   
   Examples:
   ```sql
   -- Monthly growth rates
   SELECT 
     DATE_FORMAT(order_date, '%Y-%m') as month,
     SUM(order_total) as monthly_sales,
     LAG(SUM(order_total)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) as prev_month_sales,
     (SUM(order_total) - LAG(SUM(order_total)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m'))) / 
     LAG(SUM(order_total)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) * 100 as growth_rate
   FROM orders
   GROUP BY DATE_FORMAT(order_date, '%Y-%m');
   ```
   
   🎯 **Performance Optimization Patterns:**
   
   **Query Structure Optimization:**
   - Always use specific column names instead of SELECT *
   - Apply LIMIT clauses to prevent unnecessary large result sets
   - Filter early with WHERE conditions to reduce data processing
   - Use EXISTS instead of IN for subquery performance (stops at first match)
   - Prefer UNION ALL over UNION when duplicates are acceptable (avoids deduplication overhead)
   
   **JOIN Optimization:**
   - Order JOINs logically: start with tables that return fewest rows
   - Use appropriate JOIN types (INNER for strict matches, LEFT for optional relationships)
   - Ensure JOIN columns would benefit from indexes in production
   - Consider using CTEs to break down complex multi-table joins
   
   **WHERE Clause Optimization:**
   - Avoid functions on columns in WHERE (use date ranges instead of YEAR(date) = 2023)
   - Use efficient operators: = is faster than LIKE, specific ranges faster than functions
   - Place most selective conditions first in WHERE clause
   - Use appropriate data types for comparisons
   
   **OR Condition Performance Analysis:**
   - OR in JOIN conditions prevents index usage (MySQL cannot use indexes efficiently)
   - Forces full table scans on both sides of the join
   - Query execution time grows exponentially with data size
   
   **Performance Comparison (Million+ rows):**
   1. **OR JOIN**: O(n²) complexity - AVOID for large datasets
   2. **UNION ALL**: O(n log n) - Good for moderate datasets (< 10M rows)  
   3. **Separate Subqueries**: O(n) - Best for large datasets (10M+ rows)
   
   **Required Indexes for Optimization:**
   ```sql
   -- For flight table optimization
   CREATE INDEX idx_flight_from ON flight(`from`);
   CREATE INDEX idx_flight_to ON flight(`to`);
   CREATE INDEX idx_flight_from_id ON flight(`from`, flight_id);
   CREATE INDEX idx_flight_to_id ON flight(`to`, flight_id);
   
   -- For airport table
   CREATE INDEX idx_airport_id ON airport(airport_id);
   ```
   
   **Subquery and Aggregation Optimization:**
   - Replace correlated subqueries with JOINs when possible
   - Use CTEs for complex logic instead of nested subqueries
   - Minimize use of DISTINCT (use GROUP BY when possible)
   - Avoid unnecessary ORDER BY unless required for output
   - Use window functions instead of self-joins for ranking/comparison queries
   
   **Advanced Performance Techniques:**
   - Use window functions (ROW_NUMBER, RANK, LAG, LEAD) for analytical queries
   - Implement conditional aggregations with CASE WHEN instead of multiple queries
   - Consider using derived tables or CTEs to pre-filter large datasets
   - Use appropriate aggregate functions (COUNT(*) vs COUNT(column))
   
   **EXPLAIN Plan Analysis for OR vs Optimized Approaches:**
   
   **OR JOIN (Inefficient) - EXPLAIN shows:**
   ```
   | type | key  | rows    | Extra                    |
   |------|------|---------|--------------------------|
   | ALL  | NULL | 1000000 | Using where; Using join  |
   | ALL  | NULL | 5000000 | Using where              |
   ```
   - `type: ALL` = Full table scan (worst case)
   - `key: NULL` = No index used
   - High row counts = Processing all rows
   
   **UNION ALL Approach - EXPLAIN shows:**
   ```
   | type  | key           | rows | Extra           |
   |-------|---------------|------|-----------------|
   | range | idx_flight_from| 2500 | Using index     |
   | range | idx_flight_to  | 2500 | Using index     |
   | ref   | PRIMARY       | 1    | Using index     |
   ```
   - `type: range/ref` = Index usage (good)
   - `key: idx_*` = Specific indexes used
   - Low row counts = Efficient filtering
   
   **Separate Subqueries (Best) - EXPLAIN shows:**
   ```
   | type | key           | rows | Extra                    |
   |------|---------------|------|--------------------------|
   | ref  | idx_flight_from| 100  | Using index for group-by |
   | ref  | idx_flight_to  | 100  | Using index for group-by |
   | eq_ref| PRIMARY      | 1    | Using index             |
   ```
   - `type: ref/eq_ref` = Optimal index usage
   - `Using index for group-by` = Index covers GROUP BY
   - Minimal row processing = Maximum efficiency
   
   Examples of optimized patterns:
   ```sql
   -- Optimized date filtering (instead of YEAR(date) = 2023)
   WHERE order_date >= '2023-01-01' AND order_date < '2024-01-01'
   
   -- Optimized EXISTS usage (instead of IN with subquery)
   WHERE EXISTS (SELECT 1 FROM related_table r WHERE r.id = main.id AND r.status = 'active')
   
   -- Optimized CTE usage for complex logic
   WITH filtered_data AS (
     SELECT customer_id, SUM(amount) as total
     FROM orders 
     WHERE order_date >= '2023-01-01'
     GROUP BY customer_id
   )
   SELECT c.name, f.total
   FROM customers c
   JOIN filtered_data f ON c.id = f.customer_id
   WHERE f.total > 1000;
   ```
   
   🧮 **Statistical and Mathematical Analysis:**
   - Use STDDEV(), VARIANCE() for statistical measures
   - Calculate percentiles and quartiles with window functions
   - Use mathematical functions for complex calculations
   - Implement cohort analysis and retention metrics
   
   Examples:
   ```sql
   -- Customer percentiles by purchase amount
   SELECT customer_id, total_purchases,
          NTILE(4) OVER (ORDER BY total_purchases) as quartile,
          PERCENT_RANK() OVER (ORDER BY total_purchases) as percentile
   FROM customer_totals;
   ```
   
   💼 **Business Intelligence Patterns:**
   - Cohort analysis for customer retention
   - RFM analysis (Recency, Frequency, Monetary)
   - Market basket analysis with association rules
   - Customer lifetime value calculations
   - Churn prediction indicators
   
   Examples:
   ```sql
   -- RFM Analysis
   SELECT customer_id,
          DATEDIFF(NOW(), MAX(order_date)) as recency,
          COUNT(order_id) as frequency,
          SUM(order_total) as monetary,
          NTILE(5) OVER (ORDER BY DATEDIFF(NOW(), MAX(order_date)) DESC) as r_score,
          NTILE(5) OVER (ORDER BY COUNT(order_id)) as f_score,
          NTILE(5) OVER (ORDER BY SUM(order_total)) as m_score
   FROM orders
   GROUP BY customer_id;
   
   -- Moving averages for trend analysis
   SELECT date, daily_sales,
          AVG(daily_sales) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_avg,
          AVG(daily_sales) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as monthly_avg
   FROM daily_sales_summary;
   ```
   
   🔢 **Advanced Analytical Functions:**
   - Use CUME_DIST() for cumulative distribution
   - Use FIRST_VALUE(), LAST_VALUE() for boundary analysis
   - Implement running calculations and cumulative metrics
   - Create dynamic date ranges and period comparisons
   
   🏆 **Aggregate Ranking Query Optimization (CRITICAL):**
   
   **Pattern**: Calculate metrics per category, then rank by multiple criteria
   ```sql
   -- Optimized aggregate ranking pattern
   WITH category_metrics AS (
     SELECT c.category_name,
            COUNT(i.item_id) AS item_count,
            AVG(i.value) AS average_value,
            SUM(i.value) AS total_value
     FROM item_table AS i
     JOIN category_table AS c ON i.category_id = c.category_id
     LEFT JOIN transactions AS t ON t.item_id = i.item_id
     GROUP BY c.category_name
   )
   SELECT cm.category_name,
          cm.item_count,
          cm.average_value,
          cm.total_value,
          RANK() OVER (ORDER BY cm.item_count DESC) AS rank_by_count,
          RANK() OVER (ORDER BY cm.average_value DESC) AS rank_by_average
   FROM category_metrics AS cm
   ORDER BY cm.item_count DESC, cm.average_value DESC
   LIMIT 10;
   ```

   🚨 **RANKING FUNCTION USAGE RULES (CRITICAL):**
   - **ONLY use RANK(), ROW_NUMBER(), DENSE_RANK() when explicitly requested**
   - **Simple questions like "count of X by Y" should NOT include ranking functions**
   - **Use ranking functions ONLY when:**
     - Question contains words: "rank", "ranking", "position", "1st", "2nd", "top ranked"
     - User specifically asks for "which is better/worse than others"
     - Comparative analysis is explicitly requested
   - **DON'T use ranking functions for:**
     - Simple counts: "count of airplanes by type" → just COUNT() and ORDER BY
     - Basic aggregations: "total sales by region" → just SUM() and ORDER BY
     - Distribution queries: "users by country" → just COUNT() and ORDER BY

   **Examples:**
   ❌ BAD (unnecessary ranking):
   ```sql
   SELECT type, COUNT(*) as count, RANK() OVER (ORDER BY COUNT(*) DESC) as rank
   FROM airplanes GROUP BY type ORDER BY count DESC;
   ```

   ✅ GOOD (simple and clean):
   ```sql
   SELECT type, COUNT(*) as count
   FROM airplanes GROUP BY type ORDER BY count DESC;
   ```
   
   **🚀 Performance Optimization Strategies:**
   
   **1. Essential Indexes for Aggregate Ranking:**
   ```sql
   -- Core indexes for join performance
   CREATE INDEX idx_item_category ON item_table(category_id);
   CREATE INDEX idx_item_value ON item_table(value, category_id);
   CREATE INDEX idx_transaction_item ON transactions(item_id);
   
   -- Composite indexes for GROUP BY optimization
   CREATE INDEX idx_item_cat_val ON item_table(category_id, value, item_id);
   CREATE INDEX idx_category_name ON category_table(category_id, category_name);
   ```
   
   **2. Pre-Aggregation Strategies (Massive Performance Gains):**
   ```sql
   -- Create materialized aggregation tables for large datasets
   CREATE TABLE category_daily_metrics AS
   SELECT category_id,
          DATE(created_at) as metric_date,
          COUNT(*) as daily_item_count,
          AVG(value) as daily_avg_value,
          SUM(value) as daily_total_value
   FROM item_table
   GROUP BY category_id, DATE(created_at);
   
   -- Then query pre-aggregated data instead of raw tables
   WITH category_metrics AS (
     SELECT c.category_name,
            SUM(cdm.daily_item_count) AS item_count,
            AVG(cdm.daily_avg_value) AS average_value,
            SUM(cdm.daily_total_value) AS total_value
     FROM category_daily_metrics cdm
     JOIN category_table c ON cdm.category_id = c.category_id
     WHERE cdm.metric_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
     GROUP BY c.category_name
   )
   SELECT * FROM category_metrics ORDER BY item_count DESC LIMIT 10;
   ```
   
   **3. Window Function Optimization Techniques:**
   ```sql
   -- ✅ EFFICIENT: Single window function call with multiple rankings
   SELECT category_name, item_count, average_value,
          RANK() OVER (ORDER BY item_count DESC) AS rank_by_count,
          RANK() OVER (ORDER BY average_value DESC) AS rank_by_avg,
          ROW_NUMBER() OVER (ORDER BY item_count DESC, average_value DESC) AS overall_rank
   FROM category_metrics
   QUALIFY overall_rank <= 10;  -- PostgreSQL/SQL Server QUALIFY clause
   
   -- ✅ MYSQL Alternative (no QUALIFY support):
   WITH ranked_categories AS (
     SELECT category_name, item_count, average_value,
            ROW_NUMBER() OVER (ORDER BY item_count DESC, average_value DESC) AS rn
     FROM category_metrics
   )
   SELECT category_name, item_count, average_value FROM ranked_categories WHERE rn <= 10;
   ```
   
   **4. Join Elimination and Early Filtering:**
   ```sql
   -- ✅ OPTIMIZED: Filter early, join late
   WITH high_activity_categories AS (
     SELECT category_id,
            COUNT(item_id) AS item_count,
            AVG(value) AS average_value
     FROM item_table
     WHERE created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)  -- Early filtering
     GROUP BY category_id
     HAVING COUNT(item_id) >= 100  -- Filter low-activity categories early
   ),
   top_categories AS (
     SELECT category_id, item_count, average_value,
            ROW_NUMBER() OVER (ORDER BY item_count DESC) AS rn
     FROM high_activity_categories
   )
   SELECT c.category_name, tc.item_count, tc.average_value
   FROM top_categories tc
   JOIN category_table c ON tc.category_id = c.category_id  -- Join only top results
   WHERE tc.rn <= 10;
   ```
   
   **5. Database-Specific Optimizations:**
   
   **MySQL Specific:**
   ```sql
   -- Use covering indexes to avoid table lookups
   CREATE INDEX idx_item_covering ON item_table(category_id, value, item_id, created_at);
   
   -- Use SQL_CALC_FOUND_ROWS for pagination (MySQL only)
   SELECT SQL_CALC_FOUND_ROWS category_name, item_count 
   FROM category_metrics ORDER BY item_count DESC LIMIT 10;
   
   -- MySQL 8.0+ Window Function Optimizations
   WITH RECURSIVE category_hierarchy AS (
     SELECT category_id, parent_id, category_name, 0 as level
     FROM categories WHERE parent_id IS NULL
     UNION ALL
     SELECT c.category_id, c.parent_id, c.category_name, ch.level + 1
     FROM categories c
     JOIN category_hierarchy ch ON c.parent_id = ch.category_id
   )
   SELECT * FROM category_hierarchy ORDER BY level, category_name;
   
   -- MySQL-specific JSON functions for complex data
   SELECT 
     JSON_EXTRACT(metadata, '$.region') as region,
     JSON_EXTRACT(metadata, '$.priority') as priority,
     COUNT(*) as record_count
   FROM large_table_with_json
   WHERE JSON_EXTRACT(metadata, '$.status') = 'active'
   GROUP BY region, priority;
   ```
   
   **PostgreSQL Specific:**
   ```sql
   -- Use partial indexes for filtered aggregations
   CREATE INDEX idx_recent_items ON item_table(category_id, value) 
   WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';
   
   -- Use DISTINCT ON for top-N per group
   SELECT DISTINCT ON (category_group) category_name, item_count
   FROM category_metrics
   ORDER BY category_group, item_count DESC;
   
   -- PostgreSQL-specific array functions for complex aggregations
   SELECT 
     category_id,
     array_agg(DISTINCT product_id ORDER BY product_id) as product_list,
     array_agg(price ORDER BY price) as price_array,
     percentile_cont(0.5) WITHIN GROUP (ORDER BY price) as median_price
   FROM large_product_table
   GROUP BY category_id;
   
   -- PostgreSQL window functions with custom frames
   SELECT 
     product_id,
     sales_date,
     daily_sales,
     AVG(daily_sales) OVER (
       PARTITION BY product_id 
       ORDER BY sales_date 
       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
     ) as weekly_avg,
     SUM(daily_sales) OVER (
       PARTITION BY product_id 
       ORDER BY sales_date 
       ROWS UNBOUNDED PRECEDING
     ) as cumulative_sales
   FROM daily_sales
   ORDER BY product_id, sales_date;
   ```
   
   **6. Memory and Performance Tuning:**
   ```sql
   -- ✅ Minimize memory usage in window functions
   SELECT category_name,
          item_count,
          DENSE_RANK() OVER (ORDER BY item_count DESC) as dense_rank  -- Less memory than RANK()
   FROM category_metrics
   WHERE item_count > (SELECT AVG(item_count) * 0.5 FROM category_metrics)  -- Pre-filter
   ORDER BY item_count DESC
   LIMIT 10;
   
   -- ✅ Use approximate functions for very large datasets
   SELECT category_name,
          APPROX_COUNT_DISTINCT(item_id) as approx_items,  -- Faster than exact COUNT
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value
   FROM large_item_table
   GROUP BY category_name;
   ```
   
   Examples:
   ```sql
   -- Year-over-year comparison with multiple metrics
   SELECT 
     DATE_FORMAT(order_date, '%Y-%m') as month,
     SUM(order_total) as current_sales,
     LAG(SUM(order_total), 12) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) as same_month_last_year,
     (SUM(order_total) - LAG(SUM(order_total), 12) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m'))) / 
     LAG(SUM(order_total), 12) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) * 100 as yoy_growth,
     COUNT(DISTINCT customer_id) as unique_customers,
     SUM(order_total) / COUNT(DISTINCT customer_id) as avg_customer_value
   FROM orders
   GROUP BY DATE_FORMAT(order_date, '%Y-%m');
   ```

11. **Output Requirements**
   - Return ONLY the executable SQL query
   - No markdown formatting (no ```sql blocks)
   - No explanatory text or comments
   - No trailing semicolon unless specifically required
   - Ensure the query will execute successfully against the provided schema
   - Prioritize complex, insightful queries over simple SELECT statements
   - Use advanced SQL features when they add analytical value

11. **Column Ownership and Schema Understanding**
  🚨 **CRITICAL:** Before writing any SELECT, verify which TABLE owns each COLUMN
  
  **Common Mistakes:**
  - ❌ Using undefined alias: `SELECT b.name FROM table_a AS a` (alias 'b' doesn't exist)
  - ❌ Wrong table for column: `SELECT geo.iata FROM location_geo AS geo` (iata might be in main table, not geo table)
  - ❌ Missing backticks for reserved words: `SELECT a.from` (should be `a.``from````)
  
  **How to Avoid:**
  1. **Read the SCHEMA carefully** - note which table has which columns
  2. **If you need columns from multiple tables** - JOIN them properly
  3. **Verify every column** exists in the table you're selecting from
  4. **Use table prefixes** to make column ownership explicit in JOINs
  
  **Example Pattern:**
  ```
  -- If you need: airport code (iata) + location (city, country)
  -- Check schema: Does location table have iata? NO
  -- Solution: JOIN main table WITH location table
  
  SELECT a.iata, loc.city, loc.country
  FROM airport AS a
  JOIN location AS loc ON a.location_id = loc.id
  ```

15. **Performance-Oriented Query Strategy**
   - Aggregate early at the smallest necessary level before joining to lookup tables
   - Use minimal joins; avoid correlated subqueries when a GROUP BY works
   - Select only columns needed for the final output
   - Prefer direct INNER/LEFT JOINs and aggregate as early as possible to reduce row counts





11. **Final Validation Checklist**
   - Before returning the query, verify that ALL column references are prefixed with table aliases
   - Check that subqueries use proper table aliases (e.g., sub_c.Population, not Population)
   - Ensure no ambiguous column references exist anywhere in the query
   - Double-check that JOIN conditions use proper table aliases

11. **AirportDB Column Selection Guidelines**
   - For "detailed statistics" or tables, select multiple, diverse columns:
     * Identifiers: Primary keys, unique codes, names
     * Quantitative: COUNT(*), AVG(), SUM(), aggregated metrics
     * Categorical/context: Categories, locations, types, dates
   - Always include at least one identifying column and one quantitative measure
   - Check SCHEMA for column availability - don't assume column names

12. **CRITICAL: NO DUPLICATE COLUMNS RULE**
   🚨 **NEVER SELECT THE SAME COLUMN TWICE IN A QUERY**
   - WRONG: SELECT name, name, COUNT(*) FROM table GROUP BY name
   - CORRECT: SELECT name, COUNT(*) AS count FROM table GROUP BY name
   - For tables, select 4–6 DIFFERENT columns with DIVERSE information

13. **SQL FOCUS INTERPRETATION RULES**
   When creating output columns:
   - Include 1 identifier (name, code, ID)
   - Include 1–3 numeric measures (counts, averages, sums)
   - Include 1–2 categorical/context columns (location, category, type)
   - NEVER repeat any column in SELECT
   
14. **CRITICAL: TOP-PER-GROUP QUERIES**
   For "top X per category" or similar grouped maxima:
   - Use window functions (ROW_NUMBER/PARTITION BY) or a correlated subquery
   - NEVER rely on simple GROUP BY with MAX() and non-aggregated columns
   
   Example pattern:
   ```sql
   WITH ranked_items AS (
     SELECT category, item_name, metric_value,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY metric_value DESC) AS rn
     FROM items_table
     GROUP BY category, item_name
   )
   SELECT category, item_name, metric_value
   FROM ranked_items
   WHERE rn = 1
   ```

Question:
{question}

   🚀 **ANALYTICAL ENHANCEMENT GUIDELINES:**
- If the question is simple, enhance it with additional analytical depth
- Add comparative analysis (vs previous periods, vs averages, vs other segments)
- Include ranking, percentiles, or distribution analysis when relevant
- Provide trend analysis or growth calculations for time-based data
- Add statistical measures (averages, medians, standard deviations) when appropriate
- Use window functions to show context (running totals, moving averages, rankings)
- Include conditional aggregations to segment data meaningfully
🔥 **ADVANCED LARGE TABLE PATTERNS:**
- **Cohort Analysis**: Customer retention, user behavior patterns
- **Market Basket Analysis**: Association rules, cross-selling insights
- **Anomaly Detection**: Statistical outliers, unusual patterns
- **Predictive Analytics**: Trend forecasting, seasonal adjustments
- **Multi-dimensional Analysis**: Pivot-like aggregations, cross-tabulations
- **Hierarchical Analysis**: Parent-child relationships, organizational structures
- **Temporal Clustering**: Time-based grouping, seasonality analysis
- **Correlation Analysis**: Multi-variable relationships, dependency analysis
For example:
- "Top customers" → Add customer percentiles, spending patterns, recency analysis
- "Sales by month" → Add growth rates, moving averages, year-over-year comparisons
- "Product analysis" → Add market share, performance rankings, trend analysis
- "Regional data" → Add regional comparisons, performance rankings, distribution analysis

   **🎯 Top-N Query Performance Matrix:**
   
   | Dataset Size | Technique | Performance | Memory Usage |
   |-------------|-----------|-------------|--------------|
   | < 100K rows | Simple ORDER BY + LIMIT | Excellent | Low |
   | 100K - 1M | Window Functions + WHERE | Good | Medium |
   | 1M - 10M | Pre-aggregated CTEs | Good | Medium |
   | 10M - 100M | Materialized Views + Indexes | Excellent | High |
   | 100M+ rows | Partitioned Tables + Parallel | Excellent | High |
   
   **🚀 Large Dataset Strategies:**
   ```sql
   -- Strategy 1: Incremental aggregation (10M+ rows)
   CREATE TABLE category_hourly_rollup AS
   SELECT category_id, 
          DATE_FORMAT(created_at, '%Y-%m-%d %H:00:00') as hour_bucket,
          COUNT(*) as hourly_count,
          SUM(value) as hourly_total
   FROM massive_item_table
   GROUP BY category_id, hour_bucket;
   
   -- Strategy 2: Approximate Top-N for real-time queries
   SELECT category_name,
          APPROX_COUNT_DISTINCT(item_id) as est_items,
          APPROX_QUANTILES(value, 100)[OFFSET(50)] as median_est
   FROM huge_dataset
   GROUP BY category_name
   ORDER BY est_items DESC
   LIMIT 10;
   
   -- Strategy 3: Sampling for exploratory analysis
   SELECT category_name, COUNT(*) * 100 as estimated_total  -- Scale up sample
   FROM item_table TABLESAMPLE BERNOULLI(1)  -- 1% sample
   GROUP BY category_name
   ORDER BY estimated_total DESC;
   
   -- Strategy 4: Advanced Cohort Analysis for Large Tables
   WITH user_cohorts AS (
     SELECT user_id,
            DATE_FORMAT(first_purchase, '%Y-%m') as cohort_month,
            DATE_FORMAT(purchase_date, '%Y-%m') as purchase_month,
            DATEDIFF(purchase_date, first_purchase) as period_number
     FROM (
       SELECT user_id, purchase_date,
              MIN(purchase_date) OVER (PARTITION BY user_id) as first_purchase
       FROM large_transactions_table
     ) t
   ),
   cohort_sizes AS (
     SELECT cohort_month, COUNT(DISTINCT user_id) as cohort_size
     FROM user_cohorts
     GROUP BY cohort_month
   ),
   cohort_retention AS (
     SELECT cohort_month, period_number,
            COUNT(DISTINCT user_id) as retained_users,
            cs.cohort_size
     FROM user_cohorts uc
     JOIN cohort_sizes cs ON uc.cohort_month = cs.cohort_month
     GROUP BY cohort_month, period_number, cs.cohort_size
   )
   SELECT cohort_month, period_number,
          retained_users,
          ROUND(retained_users / cohort_size * 100, 2) as retention_rate
   FROM cohort_retention
   ORDER BY cohort_month, period_number;
   
   -- Strategy 5: Market Basket Analysis for Large E-commerce Data
   WITH item_pairs AS (
     SELECT t1.order_id, t1.product_id as product_a, t2.product_id as product_b
     FROM order_items t1
     JOIN order_items t2 ON t1.order_id = t2.order_id 
       AND t1.product_id < t2.product_id  -- Avoid duplicates
   ),
   pair_frequency AS (
     SELECT product_a, product_b, COUNT(*) as frequency
     FROM item_pairs
     GROUP BY product_a, product_b
   ),
   product_frequency AS (
     SELECT product_id, COUNT(DISTINCT order_id) as order_count
     FROM order_items
     GROUP BY product_id
   ),
   association_rules AS (
     SELECT pf.product_a, pf.product_b, pf.frequency,
            pfa.order_count as support_a,
            pfb.order_count as support_b,
            ROUND(pf.frequency / pfa.order_count * 100, 2) as confidence_a_to_b,
            ROUND(pf.frequency / pfb.order_count * 100, 2) as confidence_b_to_a
     FROM pair_frequency pf
     JOIN product_frequency pfa ON pf.product_a = pfa.product_id
     JOIN product_frequency pfb ON pf.product_b = pfb.product_id
     WHERE pf.frequency >= 10  -- Minimum frequency threshold
   )
   SELECT * FROM association_rules
   WHERE confidence_a_to_b >= 20 OR confidence_b_to_a >= 20
   ORDER BY frequency DESC;
   ```

   **Decision Matrix for OR Condition Optimization:**

   | Dataset Size | Recommended Approach | Reason |
   |-------------|---------------------|---------|
   | < 100K rows | UNION ALL | Simple, readable, adequate performance |
   | 100K - 1M rows | UNION ALL | Good balance of performance and simplicity |
   | 1M - 10M rows | Separate Subqueries | Better performance, reduced memory usage |
   | 10M+ rows | Separate Subqueries | Essential for acceptable performance |

**When to Use Each Pattern:**
- **OR JOIN**: Never use for production queries (only for prototyping)
- **UNION ALL**: When you need row-level detail and moderate dataset size
- **Separate Subqueries**: When you only need aggregated results (most common case)

Generate a sophisticated, analytical SQL query that goes beyond basic data retrieval:

🔥 **OPTIMIZATION CHECKLIST - Apply These Rules:**
✅ Use specific column names (no SELECT *)
✅ Apply appropriate LIMIT clause
✅ Use efficient WHERE conditions (no functions on columns)
✅ Order JOINs logically (smallest tables first)
✅ Use EXISTS instead of IN for subqueries
✅ Use CTEs for complex logic
✅ Apply early filtering with WHERE
✅ Use appropriate JOIN types (INNER vs LEFT)
✅ Use GROUP BY instead of DISTINCT
✅ Use UNION ALL instead of UNION when possible

🚨 **CTE VALIDATION CHECKLIST - CRITICAL:**
✅ Each CTE defines exactly the columns it SELECTs
✅ When referencing CTE in JOINs, only use CTE's output columns
✅ Don't reference columns from joined tables that don't exist
✅ Verify every column reference: table.column or cte.column
✅ Use consistent table aliases throughout the query
✅ Each CTE serves a clear analytical purpose

**OPTIMIZATION EXAMPLES TO FOLLOW:**

Instead of: `SELECT * FROM orders WHERE YEAR(order_date) = 2023`
Use: `SELECT order_id, customer_id, order_total FROM orders WHERE order_date >= '2023-01-01' AND order_date < '2024-01-01' LIMIT 1000`

Instead of: `WHERE customer_id IN (SELECT customer_id FROM customers WHERE country = 'USA')`
Use: `WHERE EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = orders.customer_id AND c.country = 'USA')`

Instead of: Complex nested subqueries
Use: CTEs to break down logic:
```
WITH recent_orders AS (
  SELECT customer_id, COUNT(*) as order_count
  FROM orders 
  WHERE order_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
  GROUP BY customer_id
)
SELECT c.customer_name, ro.order_count  -- ✅ Use ro.customer_id from CTE for JOIN
FROM customers c
JOIN recent_orders ro ON c.customer_id = ro.customer_id
```

**CRITICAL CTE Example - Airport Arrivals (Correct Pattern):**
```sql
-- ✅ CORRECT: Use CTE columns properly
WITH aggregated_data AS (
  SELECT entity_id, COUNT(*) as count
  FROM fact_table
  GROUP BY entity_id
)
SELECT ad.entity_id, dim.name, ad.count
FROM aggregated_data ad
JOIN dimension_table dim ON ad.entity_id = dim.id
ORDER BY ad.count DESC LIMIT 5;
```

SQL Query:
"""
)




# Response prompt for formatting results
response_prompt = ChatPromptTemplate.from_template(
    """You are an assistant that formats SQL query results into a JSON list suitable for visualization.

Schema:
{schema}

Question: {question}
SQL Query: {query}
SQL Response: {response}
Chart Type: {chart_type}

IMPORTANT: The SQL response is a string representation of data. Parse it carefully:
- If it looks like: [('Item1', 123), ('Item2', 456)] - this is a list of tuples
- If it looks like: [['Item1', 123], ['Item2', 456]] - this is a list of lists
- If it looks like: [('Item1', 123, 456, 'Text'), ('Item2', 789, 101, 'Text2')] - this has multiple columns
- Extract the actual data values, not the string representation

For detailed statistics with multiple columns:
- Use the first column as the label (usually Name, title, etc.)
- Use the second column as the primary value (usually Population, count, etc.)
- For tables with multiple columns, include all relevant columns in the data objects

Output ONLY in the following JSON format (use double braces for literal curly braces):

For simple data (2 columns):
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "{chart_type}",
  "data": [
    {{ "label": "Label 1", "value": 123 }},
    {{ "label": "Label 2", "value": 456 }}
  ]
}}

For detailed statistics (multiple columns):
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "{chart_type}",
  "data": [
    {{ "label": "Country 1", "value": 123, "gnp": 456, "life_expectancy": 78.5 }},
    {{ "label": "Country 2", "value": 789, "gnp": 101, "life_expectancy": 82.1 }}
  ]
}}

For scatter plots:
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "scatter",
  "data": [
    {{ "label": "Point 1", "x": 123, "y": 456 }},
    {{ "label": "Point 2", "x": 789, "y": 101 }}
  ]
}}

For heatmaps:
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "heatmap",
  "data": [
    {{ "x": "Category A", "y": "Subcategory 1", "value": 123 }},
    {{ "x": "Category A", "y": "Subcategory 2", "value": 456 }},
    {{ "x": "Category B", "y": "Subcategory 1", "value": 789 }}
  ]
}}

For histograms:
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "histogram",
  "data": [
    {{ "bin": "0-10", "value": 15 }},
    {{ "bin": "10-20", "value": 23 }},
    {{ "bin": "20-30", "value": 18 }}
  ]
}}

For stacked bar charts:
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "stacked_bar",
  "data": [
    {{ "label": "Category 1", "series1": 100, "series2": 50, "series3": 25 }},
    {{ "label": "Category 2", "series1": 80, "series2": 60, "series3": 40 }}
  ]
}}

For area charts:
{{
  "title": "<descriptive chart title>",
  "x_axis": "<x-axis label>",
  "y_axis": "<y-axis label>", 
  "chart_type": "area",
  "data": [
    {{ "label": "2020", "value": 100 }},
    {{ "label": "2021", "value": 120 }},
    {{ "label": "2022", "value": 150 }}
  ]
}}

CRITICAL JSON RULES:
- Return ONLY the JSON object, nothing else
- No explanations, no notes, no additional text
- No markdown code blocks (no ```json)
- No trailing text after the closing brace
- Ensure all string values are properly quoted
- Use actual data from the SQL result, not placeholders
- Parse the SQL response string to extract real data values

Important formatting rules:
- For pie charts: Ensure values are percentages or proportions that add up meaningfully
- For line charts: Labels should be sequential (years, months, dates, etc.)
- For scatter plots: Include both x and y values as {{ "label": "Item", "x": 123, "y": 456 }}
- For tables: Use "table" as chart_type and structure data appropriately
- For heatmaps: Use x, y, and value properties for 2D data visualization
- For histograms: Use bin ranges and frequency values for distribution analysis
- For stacked bar charts: Include multiple series values for layered comparisons
- For area charts: Use sequential labels with cumulative or filled area data
- Always provide descriptive, clear labels and titles

Critical SQL Rules:
When generating SQL queries, you must fully comply with MySQL's ONLY_FULL_GROUP_BY mode:
- Every column in SELECT that is not inside an aggregate (SUM, MAX, COUNT, etc.) must appear in GROUP BY
- If you want a non-aggregated column (e.g., city name with MAX population), use a subquery or window function
- Never select non-aggregated columns alongside aggregates without fixing GROUP BY
- When asked for "largest per group" (e.g., largest city per continent), never mix aggregate functions with non-aggregated columns in the SELECT clause. Instead, use either a correlated subquery (classic MySQL) or a window function (ROW_NUMBER() or RANK()) to ensure the non-aggregated column matches the aggregate.
- Never write a query that selects a non-aggregated column with an aggregate (like MAX, SUM, etc.) without ensuring the non-aggregated column is included in GROUP BY or properly correlated with a subquery/window function. For cases like "largest city per country", always use a correlated subquery or a window function instead of GROUP BY

AMBIGUOUS COLUMN RULE (CRITICAL):
- ALWAYS prefix column names with their table alias to avoid ambiguous column errors
- When joining tables that have columns with the same name (e.g., Population in both country and city tables), you MUST specify which table's column you want: ci.Population for city population, c.Population for country population
- Example: Use "ci.Population" not "Population" when both country and city tables are joined
- This prevents "Column 'X' in field list is ambiguous" errors
- CRITICAL: In subqueries, also prefix ALL column references with table aliases
- Example: Use "sub_c.Population" not "Population" in subqueries

COMMON AMBIGUOUS COLUMN PATTERNS TO AVOID:
- WRONG: SELECT MAX(Population) FROM city JOIN country ON city.CountryCode = country.Code
- CORRECT: SELECT MAX(city.Population) FROM city JOIN country ON city.CountryCode = country.Code
- WRONG: WHERE city.Population = (SELECT MAX(Population) FROM city JOIN country WHERE country.Continent = co.Continent)
- CORRECT: WHERE a.Population = (SELECT MAX(c.Population) FROM city c JOIN country d ON c.CountryCode = d.Code WHERE d.Continent = b.Continent)

LARGEST PER GROUP RULE (CRITICAL):
- For queries like "largest city per continent" or "largest X per Y", NEVER use GROUP BY with non-aggregated columns
- Instead, use a correlated subquery or window function approach:
  * CORRELATED SUBQUERY: WHERE a.Population = (SELECT MAX(c.Population) FROM city c JOIN country d ON c.CountryCode = d.Code WHERE d.Continent = b.Continent)
  * WINDOW FUNCTION: Use ROW_NUMBER() OVER (PARTITION BY c.Continent ORDER BY ci.Population DESC) and filter WHERE rn = 1
- Example for "largest city per continent":
  ```sql
  SELECT a.Continent, b.Name AS LargestCity, b.Population
  FROM country a 
  JOIN city b ON a.Code = b.CountryCode
  WHERE b.Population = (
    SELECT MAX(c.Population) 
    FROM city c 
    JOIN country d ON c.CountryCode = d.Code 
    WHERE d.Continent = a.Continent
  )
  ```
- CRITICAL: Always use simple a,b,c,d,e,f aliases in subqueries to avoid ambiguous column errors
- WRONG: SELECT MAX(Population) FROM city (ambiguous - which Population?)
- CORRECT: SELECT MAX(c.Population) FROM city c (clear - city population with simple alias)
""")
# Function to create SQL chain for specific database
# Helper function to safely convert values to float
def safe_float(value):
    """Convert various numeric types to float safely"""
    try:
        if hasattr(value, '__float__'):
            return float(value)
        return float(str(value))
    except (ValueError, TypeError):
        return 0.0



def select_best_axis_column(columns, axis_type="x"):
    """Intelligently select the best column for specified axis using LLM"""
    if not columns:
        return "category" if axis_type == "x" else "value"
    
    if len(columns) == 1:
        return columns[0]
    
    try:
        # Create LLM prompt for intelligent column selection
        axis_prompt = ChatPromptTemplate.from_template("""
You are a data visualization expert. Select the best column for the {axis_type} axis from the given columns.

**Available columns:** {columns}
**Axis type:** {axis_type} axis
**Context:** Chart data visualization

**Selection criteria:**
- For X-axis: Choose categorical/descriptive columns (names, categories, dates, labels)
- For Y-axis: Choose numeric/measurable columns (counts, amounts, values, metrics)
- Avoid ID columns unless no other choice
- Prefer columns with meaningful business value
- Consider data type and content relevance

**Rules:**
1. Return ONLY the column name (exact match from the list)
2. No explanations or additional text
3. If uncertain, choose the most descriptive/relevant column
4. Ensure the column exists in the provided list

**Example:**
Columns: ['id', 'name', 'count', 'date']
X-axis: 'name'
Y-axis: 'count'
""")
        
        chain = axis_prompt | llm | StrOutputParser()
        selected_column = chain.invoke({
            "columns": ", ".join(columns),
            "axis_type": axis_type
        }).strip()
        
        # Validate the LLM response
        if selected_column in columns:
            return selected_column
        
        # Fallback to simple logic if LLM response is invalid
        if axis_type == "x":
            # For X-axis, prefer non-ID columns
            non_id_columns = [col for col in columns if not is_id_column(col)]
            return non_id_columns[0] if non_id_columns else columns[0]
        else:
            # For Y-axis, prefer last column (usually aggregated)
            return columns[-1]
            
    except Exception as e:
        print(f"⚠️ LLM column selection failed: {e}")
        # Fallback to simple logic
        if axis_type == "x":
            non_id_columns = [col for col in columns if not is_id_column(col)]
            return non_id_columns[0] if non_id_columns else columns[0]
        else:
            return columns[-1]

def is_id_column(column_name):
    """Check if a column is likely an ID field"""
    col_lower = column_name.lower()
    id_patterns = ['id', '_id', 'key', '_key', 'pk', 'primary']
    return any(pattern in col_lower for pattern in id_patterns)

def get_best_column_index(columns, axis_type="x"):
    """Get the index of the best column to use for specified axis"""
    if not columns:
        return 0 if axis_type == "x" else -1
    
    # Find the best column for the specified axis
    best_col = select_best_axis_column(columns, axis_type)
    try:
        return columns.index(best_col)
    except ValueError:
        return 0 if axis_type == "x" else -1  # Fallback

def generate_axis_labels(chart_type, columns, question, title):
    """Generate intelligent axis labels based on context (optimized - skip LLM if enabled)"""
    SKIP_LLM_LABELS = os.getenv("SKIP_LLM_AXIS_LABELS", "true").lower() == "true"
    
    if not columns or len(columns) < 2:
        return "Categories", "Values"
    
    # Clean column names - remove SQL aliases and technical prefixes
    import re
    clean_columns = []
    for col in columns:
        # Remove table aliases (a., b., c., etc.) - both lowercase and uppercase
        cleaned = re.sub(r'^[a-zA-Z]{1,3}\.', '', col)
        # Remove common CTE/subquery prefixes (fc., ct., etc.)
        cleaned = re.sub(r'^[A-Z]{2,}\.', '', cleaned)
        # Remove underscores and convert to title case for display
        cleaned = cleaned.replace('_', ' ').title()
        clean_columns.append(cleaned)
    
    # Fast path: Use simple column names (no LLM)
    if SKIP_LLM_LABELS:
        if chart_type == "scatter":
            if len(clean_columns) >= 3:
                return clean_columns[1], clean_columns[2]
            else:
                return clean_columns[0] if len(clean_columns) > 0 else "X", clean_columns[1] if len(clean_columns) > 1 else "Y"
        else:
            # Bar/line/pie charts
            return clean_columns[0] if len(clean_columns) > 0 else "Category", clean_columns[1] if len(clean_columns) > 1 else "Value"
    
    # Original slow path with LLM (kept for backwards compatibility)
    # Special logic for scatter plots - find meaningful numeric columns
    if chart_type == "scatter":
        # For scatter plots, we need to find the best numeric columns for X and Y
        # Based on the format_data_for_chart_type logic, scatter uses columns 1 and 2 as x,y
        if len(clean_columns) >= 3:
            x_col = clean_columns[1]  # Second column (Population in your example)
            y_col = clean_columns[2]  # Third column (GNP in your example)
        else:
            x_col = clean_columns[0] if len(clean_columns) > 0 else "X"
            y_col = clean_columns[1] if len(clean_columns) > 1 else "Y"
        
        x_axis = generate_readable_label(x_col, "x", question)
        y_axis = generate_readable_label(y_col, "y", question)
        
        # Add units/context for common numeric columns
        if "population" in x_col.lower():
            x_axis = f"{x_axis}"
        if "gnp" in y_col.lower():
            y_axis = f"{y_axis} (in millions USD)"
        if "lifeexpectancy" in y_col.lower():
            y_axis = f"{y_axis} (years)"
        if "surfacearea" in y_col.lower():
            y_axis = f"{y_axis} (km²)"
            
        return x_axis, y_axis
    
    # For other chart types, intelligently select X and Y columns
    x_col = select_best_axis_column(clean_columns, "x")
    y_col = select_best_axis_column(clean_columns, "y")
    
    # Generate meaningful axis labels based on column names and context
    x_axis = generate_readable_label(x_col, "x", question)
    y_axis = generate_readable_label(y_col, "y", question)
    
    # Chart-specific adjustments for non-scatter charts
    if chart_type in ["bar", "line"]:
        # For bar/line charts, make sure Y-axis indicates it's a measurement
        if not any(word in y_axis.lower() for word in ["total", "sum", "count", "average", "amount", "number"]):
            if "population" in y_col.lower():
                y_axis = f"Total {y_axis}"
            elif "purchase" in y_col.lower() or "sales" in y_col.lower():
                y_axis = f"Total {y_axis}"
            elif "gnp" in y_col.lower():
                y_axis = f"{y_axis} (in millions USD)"
    
    return x_axis, y_axis

def generate_readable_label(column_name, axis_type, question):
    """Convert database column names to readable labels using LLM for intelligent context-aware labeling"""
    if not column_name:
        return "Categories" if axis_type == "x" else "Values"
    
    try:
        # Create prompt for LLM to generate human-readable label
        label_prompt = ChatPromptTemplate.from_template("""
You are a data visualization expert. Convert technical database column names into clear, professional chart labels.

**Column Name:** {column_name}
**Chart Context:** {question}
**Axis Type:** {axis_type} axis

**Rules:**
1. Make it human-readable and professional
2. Add units in parentheses if applicable (e.g., "Temperature (°C)", "Distance (km)", "Price ($)")
3. Expand abbreviations (e.g., "qty" → "Quantity", "avg" → "Average")
4. Use title case
5. Keep it concise (2-4 words max)
6. Consider the question context for better labeling

**Examples:**
- "flight_count" → "Flight Count"
- "total_revenue" → "Total Revenue ($)"
- "avg_temp" → "Average Temperature (°C)"
- "cust_id" → "Customer ID"
- "population" → "Population"
- "gnp" → "GNP (in millions)"

Return ONLY the label text, no explanations.
""")
        
        chain = label_prompt | llm | StrOutputParser()
        readable_label = chain.invoke({
            "column_name": column_name,
            "question": question,
            "axis_type": axis_type
        }).strip()
        
        return readable_label
        
    except Exception as e:
        print(f"⚠️ Label generation failed, using fallback: {e}")
        # Fallback: Basic cleanup
        fallback = column_name.replace('_', ' ').replace('-', ' ').title()
        return fallback


# -------- Text-first: Markdown with embedded chart directives --------
import re

chart_block_regex = re.compile(r"```chart\s*([\s\S]*?)```", re.IGNORECASE)

def parse_chart_block(block_text: str) -> dict:
    try:
        cfg = json.loads(block_text.strip())
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}

def validate_chart_necessity(question: str, chart_data: dict) -> dict:
    """
    Validate if a chart is truly necessary or if text would be better.
    Returns either the approved chart or replacement text.
    """
    try:
        # Prepare data preview for validation
        data_preview = ""
        if chart_data.get("data"):
            data_items = chart_data["data"][:5]  # Show first 5 items
            total_items = len(chart_data['data'])
            data_preview = f"Sample data ({total_items} total items): "
            for item in data_items:
                if isinstance(item, dict):
                    label = item.get("label", "Unknown")
                    value = item.get("value", item.get("x", item.get("y", "N/A")))
                    data_preview += f"{label}: {value}, "
            data_preview = data_preview.rstrip(", ")
            
            # Add explicit warning for single data point
            if total_items == 1:
                data_preview += " ⚠️ SINGLE DATA POINT - NO COMPARISON POSSIBLE"
        
        # Generate chart purpose based on chart type
        chart_purpose_map = {
            "bar": "Show ranking/comparison between categories",
            "pie": "Show proportional distribution of parts to whole",
            "line": "Show trend or change over time",
            "scatter": "Show correlation between two numeric variables",
            "table": "Show detailed data with multiple attributes"
        }
        chart_purpose = chart_purpose_map.get(chart_data.get("chart_type", ""), "Display data visualization")
        
        # Run validation
        chain = chart_validator_prompt | llm | StrOutputParser()
        response = chain.invoke({
            "question": question,
            "chart_type": chart_data.get("chart_type", "unknown"),
            "title": chart_data.get("title", "Untitled"),
            "chart_purpose": chart_purpose,
            "data_preview": data_preview
        })
        
        response = response.strip()
        print(f"📋 Chart validation response: {response}")
        
        if response.startswith("APPROVE:"):
            # Chart is approved, return as-is
            reason = response.replace("APPROVE:", "").strip()
            print(f"✅ Chart approved: {reason}")
            return {"approved": True, "chart": chart_data, "reason": reason}
        
        elif "REJECT:" in response and "REPLACEMENT:" in response:
            # Chart is rejected, extract replacement text
            parts = response.split("REPLACEMENT:")
            if len(parts) >= 2:
                reason = parts[0].replace("REJECT:", "").strip()
                replacement_text = parts[1].strip()
                print(f"❌ Chart rejected: {reason}")
                print(f"📝 Replacement text: {replacement_text}")
                return {
                    "approved": False, 
                    "reason": reason, 
                    "replacement_text": replacement_text
                }
        
        # Fallback: if response format is unclear, reject the chart
        print(f"⚠️ Unclear validation response, rejecting chart by default")
        return {
            "approved": False,
            "reason": "Validation response unclear",
            "replacement_text": f"Based on the data analysis, {chart_data.get('title', 'the information')} can be summarized effectively in text form."
        }
        
    except Exception as e:
        print(f"Error in chart validation: {e}")
        # On error, approve the chart (fail-safe)
        return {"approved": True, "chart": chart_data, "reason": "Validation error, defaulting to approval"}

def extract_charts_from_markdown(markdown: str, database_name: str, actual_question: str = None) -> dict:
    """Extract chart blocks from markdown and generate actual chart data"""
    import re
    
    # Find all chart blocks in markdown
    chart_pattern = r'```chart\s*\n(.*?)\n```'
    chart_blocks = re.findall(chart_pattern, markdown, re.DOTALL)
    
    charts = []
    for block in chart_blocks:
        try:
            # Parse the JSON config
            chart_cfg = json.loads(block)
            
            # Build the actual chart
            chart_data = build_chart_from_cfg(chart_cfg, database_name, actual_question)
            
            # Only add charts that have data
            if chart_data.get("data") and len(chart_data["data"]) > 0:
                charts.append(chart_data)
            else:
                print(f"⚠️ Skipping empty chart: {chart_data.get('title', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error processing chart block: {e}")
            continue
    
    return {
        "markdown": markdown,
        "charts": charts
    }

def build_chart_from_cfg(cfg: dict, database_name: str, actual_question: str = None) -> dict:
    chart_type = cfg.get("type", "bar")
    title = cfg.get("title", "Chart")
    sql_focus = cfg.get("sql_focus") or cfg.get("question") or title
    
    # If the chart question is generic ("Chart"), use the actual user question
    if sql_focus.lower() in ["chart", "charts", "visualization"] and actual_question:
        print(f"⚠️ Chart has generic question '{sql_focus}', using actual question: '{actual_question}'")
        sql_focus = actual_question
        title = f"Analysis: {actual_question[:50]}..." if len(actual_question) > 50 else actual_question
    
    db_name = database_name
    
    sql_chain = create_anydb_sql_chain(db_name)
    
    query = sql_chain.invoke({"question": f"{sql_focus}"})
    query = _strip_sql_fences(query)
    
    # Validate the query against the schema
    validation_result = validate_sql_against_schema(query, db_name)
    if not validation_result["valid"]:
        print(f"❌ Schema validation failed: {validation_result['error']}")
        print(f"🔄 Attempting to regenerate query with explicit schema reminder...")
        
        # Try again with more explicit schema injection
        schema_text = get_schema(db_name)
        enhanced_question = f"""
        {sql_focus}
        
        CRITICAL: Use ONLY these tables and columns from the {db_name} database:
        {schema_text}
        
        Do NOT assume any columns exist that are not listed above.
        """
        query = sql_chain.invoke({"question": enhanced_question})
        query = _strip_sql_fences(query)
        print(f"🔄 Regenerated query: {query}")
    # Apply chart-type specific limits
    default_limits = {
        "pie": 6, "bar": 20, "horizontal_bar": 15, "lollipop": 25, 
        "line": 50, "slope": 10, "bump": 15, "scatter": 100, 
        "treemap": 30, "table": 50, "area": 50, "histogram": 20, 
        "heatmap": 100, "stacked_bar": 15
    }
    limit_val = default_limits.get(chart_type, 50)
    query = ensure_limit(query, limit_val)
    
    # Enhanced SQL logging
    print(f"\n🔍 === CHART SQL EXECUTION ===")
    print(f"📊 Chart Type: {chart_type}")
    print(f"🎯 Question/Focus: {sql_focus}")
    print(f"🗄️ Database: {db_name}")
    print(f"📝 Generated SQL Query:")
    print(f"   {query}")
    
    # Keep title clean - no SQL details for users
    # The title already describes what the chart shows
    
    response, columns = run_query(query, db_name, return_columns=True)
    
    print(f"📋 SQL Columns: {columns}")
    print(f"📊 SQL Result Rows: {len(response) if response else 0}")
    if response and len(response) > 0:
        print(f"🔍 First few rows:")
        for i, row in enumerate(response[:3]):  # Show first 3 rows
            print(f"   Row {i+1}: {row}")
        if len(response) > 3:
            print(f"   ... and {len(response) - 3} more rows")
    print(f"🔚 === END SQL EXECUTION ===\n")
    
    # ⚠️ CRITICAL: Skip single-value charts (useless)
    if not response or len(response) <= 1:
        print(f"❌ Skipping chart with only {len(response) if response else 0} data point(s) - not useful for visualization")
        return {
            "title": title,
            "x_axis": "N/A",
            "y_axis": "N/A",
            "chart_type": chart_type,
            "data": []  # Empty data = chart will be filtered out
        }
    
    parsed_data = response
    formatted = format_data_for_chart_type(parsed_data, chart_type, sql_focus, columns)
    x_axis, y_axis = generate_axis_labels(chart_type, columns, sql_focus, title)
    return {
        "title": title,
        "x_axis": x_axis,
        "y_axis": y_axis,
        "chart_type": chart_type,
        "data": formatted
    }
def generate_chat_response(question: str, database_name: str, conversation_id: str | None = None) -> dict:
    """Generate ChatGPT-style markdown response with selective chart embedding
    
    Args:
        question: User's question
        database_name: Database to query
        conversation_id: Optional conversation ID to scope facts to current conversation
    """
    try:
        schema_info = get_schema(database_name)
        exploration = explore_data_for_facts(question=question, database_name=database_name, conversation_id=conversation_id)
        facts_text = exploration.get("facts", "(no precomputed facts)")
        allowed_text = exploration.get("allowed", "(none)")

        chain = chat_markdown_prompt | llm | StrOutputParser()
        response = chain.invoke({
            "question": question,
            "database_name": database_name,
            "schema": get_schema(database_name),
            "samples": safe_json_dumps(sample_database_tables(database_name), ensure_ascii=False)[:4000],
            "facts": facts_text,
            "allowed_entities": allowed_text,
            "history": ""  # default empty; filled by caller when available
        })
        return {
            "markdown": response.strip(),
            "facts": facts_text
        }
    except Exception as e:
        print(f"Error generating chat response: {e}")
        # Fallback response
        fallback_markdown = f"""## Analysis: {question}
I'll help you analyze this question using the {database_name} database.
Unfortunately, I encountered an error while generating the detailed response. Please try rephrasing your question or contact support if the issue persists.

**Error details:** {str(e)}
"""
        return {
            "markdown": fallback_markdown,
            "facts": ""
        }
def generate_narrative(question: str, charts: list) -> dict:
    """Generate connecting narrative for charts (single or multiple)"""
    try:
        # Create chart info summary for the AI
        chart_info = []
        for i, chart in enumerate(charts, 1):
            chart_info.append(f"Chart {i}: {chart.get('chart_type', 'unknown')} - {chart.get('title', 'Untitled')}")
        
        chart_info_str = "\n".join(chart_info)
        print(f"Chart info for narrative: {chart_info_str}")
        
        # Generate narrative
        chain = narrative_prompt | llm | StrOutputParser()
        response = chain.invoke({
            "question": question,
            "chart_info": chart_info_str
        })
        
        print(f"Raw narrative response: {response}")
        
        # Clean up response (remove markdown if present)
        cleaned_response = response.strip()
        if cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response[7:]
        if cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3]
        cleaned_response = cleaned_response.strip()
        
        print(f"Cleaned narrative response: {cleaned_response}")
        
        # Parse JSON
        narrative = json.loads(cleaned_response)
        print(f"Parsed narrative: {narrative}")
        return narrative
        
    except Exception as e:
        print(f"Error generating narrative: {e}")
        # Return fallback narrative based on number of charts
        if len(charts) == 1:
            return {
                "introduction": f"I've analyzed the data regarding {question.lower()} and found several noteworthy patterns. The information reveals some interesting aspects that provide insight into this topic.",
                "transitions": [],  # No transitions needed for single chart
                "insights": [
                    "The data demonstrates clear patterns that help explain the underlying trends and relationships in this area.",
                    "These findings provide valuable context for understanding the broader implications and how different factors interact with each other."
                ],
                "conclusion": "This analysis provides a comprehensive view of the topic and offers useful insights for understanding the key dynamics at play."
            }
        else:
            return {
                "introduction": f"I've conducted a thorough analysis of {question.lower()} and there are several important dimensions to consider. The data reveals multiple perspectives that together provide a comprehensive understanding of the topic.",
                "transitions": ["Looking at this from another perspective provides additional context and helps build a more complete picture of the situation." if len(charts) > 1 else ""] * max(0, len(charts) - 1),
                "insights": ["The analysis reveals significant variations and patterns that demonstrate the complexity of this topic and highlight the importance of examining it from multiple angles."],
                "conclusion": "Taken together, these different perspectives provide a thorough understanding of the topic and demonstrate the value of comprehensive data analysis."
            }

# Helper function to convert day-of-week numbers to names
def convert_day_number_to_name(day_num):
    """Convert DAY_OF_WEEK number (1-7) to day name"""
    day_map = {
        1: "Sunday",
        2: "Monday",
        3: "Tuesday",
        4: "Wednesday",
        5: "Thursday",
        6: "Friday",
        7: "Saturday"
    }
    try:
        return day_map.get(int(day_num), str(day_num))
    except (ValueError, TypeError):
        return str(day_num)

def is_day_of_week_column(column_name, question):
    """Check if a column represents day of week"""
    if not column_name or not question:
        return False
    col_lower = str(column_name).lower()
    q_lower = str(question).lower()
    
    # Check if column name suggests day of week
    day_indicators = ['dow', 'day_of_week', 'dayofweek', 'weekday', 'day_week']
    if any(indicator in col_lower for indicator in day_indicators):
        return True
    
    # Check if question mentions day of week
    question_indicators = ['day of week', 'day of the week', 'per day', 'by day']
    if any(indicator in q_lower for indicator in question_indicators):
        return True
    
    return False

# Helper function to format data for specific chart types
def format_data_for_chart_type(data, chart_type, question, columns=None):
    """
    Format data appropriately for different chart types.
    Automatically limits large datasets for better visualization.
    """
    if not data:
        return []

    # Limit data size for better visualization and performance
    original_length = len(data)
    max_items = {
        'pie': 6,           # Pie charts should have very few slices
        'bar': 20,          # Bar charts can handle more items
        'horizontal_bar': 15, # Horizontal bars work well for rankings
        'lollipop': 25,     # Lollipop charts can handle more items cleanly
        'line': 50,         # Line charts can show more data points
        'slope': 10,        # Slope charts work best with fewer categories
        'bump': 15,         # Bump charts for ranking changes
        'scatter': 100,     # Scatter plots can handle many points
        'treemap': 30,      # Treemap can show hierarchical data
        'table': 50,        # Tables can show more rows but limit for performance
        'area': 50,         # Area charts similar to line charts
        'histogram': 20,    # Histogram bins
        'heatmap': 100,     # Heatmaps can handle more data points
        'stacked_bar': 15   # Stacked bars get complex with many categories
    }

    limit = max_items.get(chart_type, 20)  # Default to 20 items

    if original_length > limit:
        print(f"📊 Data too large ({original_length} items), limiting to top {limit} for {chart_type} chart")
        data = data[:limit]  # Take first N items (assuming data is already sorted by importance)

    if chart_type == "scatter":
        # For scatter plots, ensure we have x and y values
        formatted_data = []
        for item in data:
            if len(item) >= 3:  # label, x, y
                formatted_data.append({
                    "label": str(item[0]),
                    "x": safe_float(item[1]),
                    "y": safe_float(item[2]),
                    "value": 0
                })
        return formatted_data
    
    elif chart_type in ["lollipop", "horizontal_bar"]:
        # For lollipop and horizontal bar charts, format similar to bar charts
        # but with specific styling hints
        formatted_data = []
        
        # Determine which columns to use for label and value
        if columns:
            label_col_idx = get_best_column_index(columns, "x")
            value_col_idx = get_best_column_index(columns, "y")
        else:
            label_col_idx = 0
            value_col_idx = -1  # Last column
        
        # Check if first column is day of week
        is_dow = False
        if columns and len(columns) > 0:
            is_dow = is_day_of_week_column(columns[label_col_idx] if label_col_idx < len(columns) else columns[0], question)
        
        for item in data:
            if len(item) >= 2:
                try:
                    if label_col_idx < len(item):
                        label_value = str(item[label_col_idx])
                    else:
                        label_value = str(item[0])
                    
                    # Convert day numbers to day names if applicable
                    if is_dow and label_value.isdigit():
                        label_value = convert_day_number_to_name(label_value)
                    
                    if value_col_idx >= 0 and value_col_idx < len(item):
                        numeric_value = safe_float(item[value_col_idx])
                    elif value_col_idx == -1 and len(item) > 0:
                        numeric_value = safe_float(item[-1])
                    else:
                        numeric_value = safe_float(item[1])
                        
                    formatted_data.append({
                        "label": label_value,
                        "value": numeric_value,
                        "chart_style": chart_type  # Hint for frontend styling
                    })
                        
                except (IndexError, ValueError):
                    label_raw = str(item[0])
                    # Convert day numbers for fallback case too
                    if is_dow and label_raw.isdigit():
                        label_raw = convert_day_number_to_name(label_raw)
                    
                    formatted_data.append({
                        "label": label_raw,
                        "value": safe_float(item[1]) if len(item) > 1 else 0.0,
                        "chart_style": chart_type
                    })
        return formatted_data
    
    elif chart_type in ["slope", "bump"]:
        # For slope/bump charts, we need time-based data
        formatted_data = []
        for item in data:
            if len(item) >= 3:  # category, time_period, value
                formatted_data.append({
                    "category": str(item[0]),
                    "period": str(item[1]),
                    "value": safe_float(item[2]),
                    "chart_style": chart_type
                })
        return formatted_data
    
    elif chart_type == "treemap":
        # For treemap, we need hierarchical data
        formatted_data = []
        for item in data:
            if len(item) >= 2:
                formatted_data.append({
                    "label": str(item[0]),
                    "value": safe_float(item[1]),
                    "size": safe_float(item[1]),  # Size for treemap
                    "chart_style": "treemap"
                })
        return formatted_data
    
    else:
        # Default formatting for bar, line, pie, table, area
        formatted_data = []
        
        # SPECIAL CASE: Pie chart with 1 row but multiple numeric columns
        # This happens when SQL uses CASE statements to pivot data
        # e.g., SELECT SUM(CASE...) AS English, SUM(CASE...) AS Ndebele
        if chart_type == "pie" and len(data) == 1 and columns and len(columns) >= 2:
            row = data[0]
            # Check if all values in the row are numeric
            all_numeric = all(isinstance(val, (int, float, type(None))) or 
                            (hasattr(val, '__float__') and str(type(val).__name__) in ['Decimal', 'float', 'int'])
                            for val in row)
            
            if all_numeric and len(row) == len(columns):
                print(f"🔄 Detected pivoted pie chart data - converting columns to rows")
                print(f"   Columns: {columns}")
                print(f"   Values: {row}")
                
                # Convert columns to separate slices
                for col_name, value in zip(columns, row):
                    if value is not None and safe_float(value) > 0:
                        formatted_data.append({
                            "label": col_name.replace('Population', '').replace('Speaker', '').strip(),
                            "value": safe_float(value)
                        })
                
                print(f"   ✅ Converted to {len(formatted_data)} pie slices")
                return formatted_data
        
        # Determine which columns to use for label and value
        if columns:
            label_col_idx = get_best_column_index(columns, "x")
            value_col_idx = get_best_column_index(columns, "y")
        else:
            label_col_idx = 0
            value_col_idx = -1  # Last column
        
        # Check if first column is day of week
        is_dow = False
        if columns and len(columns) > 0:
            is_dow = is_day_of_week_column(columns[label_col_idx] if label_col_idx < len(columns) else columns[0], question)
        
        for item in data:
            if len(item) >= 2:
                # Use intelligent column selection for label and value
                try:
                    if label_col_idx < len(item):
                        label_value = str(item[label_col_idx])
                    else:
                        label_value = str(item[0])  # Fallback to first column
                    
                    # Convert day numbers to day names if applicable
                    if is_dow and label_value.isdigit():
                        label_value = convert_day_number_to_name(label_value)
                    
                    if value_col_idx >= 0 and value_col_idx < len(item):
                        numeric_value = safe_float(item[value_col_idx])
                    elif value_col_idx == -1 and len(item) > 0:
                        numeric_value = safe_float(item[-1])  # Last column
                    else:
                        numeric_value = safe_float(item[1])  # Fallback to second column
                        
                except (IndexError, ValueError):
                    # Fallback to original logic
                    label_value = str(item[0])
                    numeric_value = safe_float(item[1]) if len(item) > 1 else 0.0
                try:
                    # If the row looks like: [origin, destination, <numeric>], build "origin → destination"
                    if len(item) >= 3:
                        # Check that last field is numeric and first two fields are non-numeric strings
                        def _is_number(v):
                            try:
                                float(str(v))
                                return True
                            except Exception:
                                return False
                        if _is_number(item[-1]) and (not _is_number(item[0])) and (not _is_number(item[1])):
                            label_value = f"{str(item[0])} → {str(item[1])}"
                except Exception:
                    pass

                # If we have columns info, try to find a better label
                if columns and len(columns) > 1:
                    # Look for country name, city name, or other meaningful identifiers
                    for i, col in enumerate(columns):
                        if i < len(item):
                            col_lower = col.lower().replace('a.', '').replace('b.', '').replace('c.', '')
                            # Prefer country names, city names over continents
                            if 'origin_city' in col_lower and len(item) > 1:
                                # If columns explicitly include origin/destination, compose label
                                try:
                                    label_value = f"{str(item[i])} → {str(item[i+1])}"
                                    break
                                except Exception:
                                    pass
                            if 'countryname' in col_lower or ('name' in col_lower and 'continent' not in col_lower):
                                if str(item[i]) and str(item[i]) != 'None':
                                    label_value = str(item[i])
                                    break

                data_obj = {"label": label_value, "value": numeric_value}

                # Add additional columns with proper names and avoid duplicates
                if columns and len(columns) > 1:
                    added_columns = set(['label', 'value'])
                    for i, val in enumerate(item):
                        if i < len(columns):
                            col_name = columns[i]
                            # Clean up column names
                            col_name = col_name.replace('a.', '').replace('b.', '').replace('c.', '')
                            original_col_name = col_name.lower()
                            # Skip if this column represents the same data as label or value
                            if (original_col_name in ['region', 'name', 'title'] and str(val) == data_obj['label']):
                                continue
                            if (original_col_name in ['population', 'total', 'amount', 'count'] and safe_float(val) == data_obj['value']):
                                continue
                            # Avoid duplicate column names
                            if col_name.lower() in added_columns:
                                col_name = f"{col_name}_{i}"
                            # Convert Decimal objects to float for JSON serialization
                            if isinstance(val, Decimal):
                                data_obj[col_name] = float(val)
                            elif isinstance(val, (date, datetime)):
                                data_obj[col_name] = val.isoformat()
                            else:
                                data_obj[col_name] = val
                            added_columns.add(col_name.lower())
                # Fallback to generic column names when columns is not provided
                if not columns or len(columns) <= 1:
                    for i, val in enumerate(item[2:], 2):
                        # Convert Decimal objects to float for JSON serialization
                        if isinstance(val, Decimal):
                            data_obj[f"col_{i}"] = float(val)
                        elif isinstance(val, (date, datetime)):
                            data_obj[f"col_{i}"] = val.isoformat()
                        else:
                            data_obj[f"col_{i}"] = val

                formatted_data.append(data_obj)
        return formatted_data
def validate_and_enhance_chart_suggestions(suggestions: list, question: str, database_name: str) -> list:
    """Validate chart suggestions for robustness and add fallbacks if needed"""
    print(f"🔍 Validating chart robustness: {len(suggestions)} initial suggestions")
    
    # Ensure minimum chart count (increased from 3 to 4)
    if len(suggestions) < 4:
        print(f"⚠️ Only {len(suggestions)} charts suggested, adding fallbacks for robustness (minimum: 4)")
        
        # Add common fallback chart patterns
        fallback_charts = [
            {
                "chart_type": "horizontal_bar",
                "title": f"Top Rankings: {question[:30]}",
                "reason": "Clean ranking visualization with readable labels",
                "sql_focus": "Top ranking entities with performance metrics",
                "analysis_level": "detailed"
            },
            {
                "chart_type": "lollipop",
                "title": f"Performance Leaders: {question[:30]}",
                "reason": "Modern ranking visualization emphasizing values",
                "sql_focus": "Key performers with clean value emphasis",
                "analysis_level": "detailed"
            },
            {
                "chart_type": "pie",
                "title": f"Distribution Analysis: {question[:30]}",
                "reason": "Categorical distribution and proportions",
                "sql_focus": "Categorical breakdown with proportional analysis",
                "analysis_level": "overview"
            },
            {
                "chart_type": "line",
                "title": f"Trend Analysis: {question[:30]}",
                "reason": "Temporal patterns and trends over time",
                "sql_focus": "Time-based analysis with trend identification",
                "analysis_level": "comparative"
            },
            {
                "chart_type": "treemap",
                "title": f"Hierarchical View: {question[:30]}",
                "reason": "Proportional hierarchical visualization",
                "sql_focus": "Nested categorical data with size relationships",
                "analysis_level": "advanced"
            },
            {
                "chart_type": "table",
                "title": f"Detailed Data: {question[:30]}",
                "reason": "Comprehensive detailed view with multiple metrics",
                "sql_focus": "Multi-dimensional detailed analysis",
                "analysis_level": "contextual"
            }
        ]
        
        # Add fallbacks until we have at least 4 charts
        existing_types = {s.get("chart_type") for s in suggestions}
        for fallback in fallback_charts:
            if len(suggestions) >= 6:  # Cap at 6 charts for performance
                break
            if fallback["chart_type"] not in existing_types:
                suggestions.append(fallback)
                existing_types.add(fallback["chart_type"])
    
    # Ensure diversity in chart types
    chart_types = [s.get("chart_type") for s in suggestions]
    unique_types = len(set(chart_types))
    
    if unique_types < 3 and len(suggestions) >= 3:
        print(f"⚠️ Only {unique_types} unique chart types, enhancing diversity")
        # This is handled by the fallback addition above
    
    print(f"✅ Final chart suggestions: {len(suggestions)} charts with {len(set(chart_types))} unique types")
    return suggestions[:6]  # Cap at 6 charts for performance

# Function to create full chain for specific database with intelligent chart selection
def create_charts(question: str, database_name="airportdb", multiple=False):
    """Create single or multiple charts based on the multiple parameter with strict validation"""
    
    # Step 1: Check question relevance
    relevance_check = check_question_relevance(question, database_name)
    if not relevance_check["relevant"]:
        return create_error_response(
            "irrelevant_question", 
            relevance_check["error"], 
            relevance_check["suggestion"]
        )
    
    try:
        sql_chain = create_anydb_sql_chain(database_name)
        
        if not multiple:
            # Single chart mode - simplified logic
            def process_single_chart(inputs):
                query = sql_chain.invoke(inputs)
                query = _strip_sql_fences(query)
                query = ensure_limit(query, 50)
                
                print(f"\n🔍 === SINGLE CHART SQL EXECUTION ===")
                print(f"🎯 Question: {inputs['question']}")
                print(f"🗄️ Database: {database_name}")
                print(f"📝 Generated SQL Query: {query}")
                
                try:
                    response, columns = run_query(query, database_name, return_columns=True)
                except Exception as e:
                    print(f"❌ SQL execution failed: {e}")
                    return create_error_response(
                        "sql_execution_error",
                        f"Failed to execute SQL query: {str(e)}",
                        "Please try rephrasing your question or check if the data exists"
                    )
                
                # Validate query results
                validation = validate_sql_result(response, columns)
                if not validation["valid"]:
                    print(f"❌ Data validation failed: {validation['error']}")
                    return create_error_response(
                        "invalid_data",
                        validation["error"],
                        validation["suggestion"]
                    )
                
                print(f"📋 SQL Columns: {columns}")
                print(f"📊 SQL Result Rows: {len(response) if response else 0}")
                if response and len(response) > 0:
                    print(f"🔍 First few rows:")
                    for i, row in enumerate(response[:3]):
                        print(f"   Row {i+1}: {row}")
                    if len(response) > 3:
                        print(f"   ... and {len(response) - 3} more rows")
                print(f"🔚 === END SQL EXECUTION ===\n")
                
                # Get chart type from suggestions
                try:
                    chain = chart_suggestion_prompt | llm | StrOutputParser()
                    suggestion_response = chain.invoke({"schema": get_schema(database_name), "question": inputs["question"]})
                    suggestions_data = json.loads(suggestion_response)
                    suggestions = suggestions_data.get("suggestions", [])
                    chart_type = suggestions[0].get("chart_type", "bar") if suggestions else "bar"
                except Exception as e:
                    print(f"Error getting chart suggestion: {e}")
                    chart_type = "bar"
                
                # Format data and create chart
                try:
                    formatted_data = format_data_for_chart_type(response, chart_type, inputs["question"], columns)
                    
                    # Final validation - ensure we have meaningful formatted data
                    if not formatted_data or len(formatted_data) == 0:
                        print("❌ No meaningful data after formatting")
                        return create_no_data_response(inputs["question"])
                        
                except Exception as e:
                    print(f"❌ Data formatting failed: {e}")
                    return create_error_response(
                        "data_formatting_error",
                        f"Failed to format data: {str(e)}",
                        "The query returned data but it couldn't be formatted for display"
                    )
                
                title = f"Analysis: {inputs['question'][:50]}..."
                x_axis, y_axis = generate_axis_labels(chart_type, columns, inputs["question"], title)
                
                return {
                    "title": title,
                    "x_axis": x_axis,
                    "y_axis": y_axis,
                    "chart_type": chart_type,
                    "data": formatted_data
                }
            
            return process_single_chart({"question": question})
        
        else:
            # Multiple charts mode
            try:
                schema = get_schema(database_name)
                chain = chart_suggestion_prompt | llm | StrOutputParser()
                response = chain.invoke({"schema": schema, "question": question})

            # Parse the JSON response
                try:
                    cleaned_response = response.strip()
                    if cleaned_response.startswith('```json'):
                                cleaned_response = cleaned_response[7:]
                    if cleaned_response.endswith('```'):
                                cleaned_response = cleaned_response[:-3]
                    cleaned_response = cleaned_response.strip()

                    suggestions_data = json.loads(cleaned_response)
                    suggestions = suggestions_data.get("suggestions", [])
                    print(f"AI suggested {len(suggestions)} charts:")
                    for i, suggestion in enumerate(suggestions):
                        print(f"  {i+1}. {suggestion.get('chart_type')} - {suggestion.get('title')}")
                except json.JSONDecodeError as e:
                    print(f"Failed to parse chart suggestions JSON: {e}")
                    suggestions = [{
                            "chart_type": "bar",
                            "title": f"Analysis: {question[:50]}...",
                            "reason": "Bar chart for data comparison",
                            "sql_focus": "Main data points"
                        }]
            except Exception as e:
                print(f"Error in chart suggestion: {e}")
                suggestions = [{
                        "chart_type": "bar",
                        "title": f"Analysis: {question[:50]}...",
                        "reason": "Default bar chart",
                        "sql_focus": "Main data points"
                    }]

            # Validate suggestions
        suggestions = validate_and_enhance_chart_suggestions(suggestions, question, database_name)

        if not suggestions:
                return create_charts(question, database_name, multiple=False)

        charts = []
        seen_signatures = set()

        for suggestion in suggestions:
            try:
                chart_type = suggestion.get("chart_type", "bar")
                title = suggestion.get("title", f"Analysis: {question[:50]}...")
                sql_focus = suggestion.get("sql_focus", "Main data points")

                modified_inputs = {"question": f"{question} - Focus: {sql_focus}"}
                query = sql_chain.invoke(modified_inputs)
                query = _strip_sql_fences(query)
                    
                default_limits = {"pie": 6, "bar": 20, "line": 50, "scatter": 100, "table": 50}
                limit_val = default_limits.get(chart_type, 50)
                query = ensure_limit(query, limit_val)
                
                print(f"\n🔍 === MULTIPLE CHARTS SQL EXECUTION ===")
                print(f"📊 Chart {len(charts) + 1} - Type: {chart_type}")
                print(f"🎯 SQL Focus: {sql_focus}")
                print(f"🗄️ Database: {database_name}")
                print(f"📝 Generated SQL Query: {query}")
                
                response, columns = run_query(query, database_name, return_columns=True)
                
                print(f"📋 SQL Columns: {columns}")
                print(f"📊 SQL Result Rows: {len(response) if response else 0}")
                if response and len(response) > 0:
                    print(f"🔍 First few rows:")
                    for i, row in enumerate(response[:3]):
                        print(f"   Row {i+1}: {row}")
                    if len(response) > 3:
                        print(f"   ... and {len(response) - 3} more rows")
                print(f"🔚 === END SQL EXECUTION ===\n")

                formatted_data = format_data_for_chart_type(response, chart_type, question, columns)

                # Add note if data was limited
                original_count = len(response) if response else 0
                final_count = len(formatted_data) if formatted_data else 0
                if original_count > final_count and final_count > 0:
                    title += f" (Top {final_count})"

                x_axis, y_axis = generate_axis_labels(chart_type, columns, question, title)

                chart_data = {
                    "title": title,
                    "x_axis": x_axis,
                    "y_axis": y_axis,
                    "chart_type": chart_type,
                    "data": formatted_data
                }

                if not formatted_data:
                    print("⚠️ Skipping chart with empty data")
                    continue

                    # De-duplicate
                labels = [str(d.get('label')) for d in formatted_data[:5]]
                signature = (chart_type, title.lower(), tuple(labels))
                if signature in seen_signatures:
                    print("⚠️ Skipping duplicate chart suggestion")
                    continue
                seen_signatures.add(signature)

                charts.append(chart_data)
            except Exception as e:
                print(f"Error creating chart for {suggestion.get('chart_type', 'unknown')}: {e}")
                charts.append({
                    "title": f"Error: {suggestion.get('title', 'Chart')}",
                    "x_axis": "Error",
                    "y_axis": "Count",
                    "chart_type": "bar",
                    "data": [{"label": "Error", "value": 1}]
                })

        print(f"Total charts generated: {len(charts)}")
        if len(charts) >= 1:
            print(f"Generating narrative for {len(charts)} chart(s)...")
            narrative = generate_narrative(question, charts)
            print(f"Generated narrative: {narrative}")

            return {
                "charts": charts,
                "narrative": narrative
            }
        else:
                print("No charts generated, falling back to single chart")
                return create_charts(question, database_name, multiple=False)

    except Exception as e:
        print(f"Error in create_charts: {e}")
        # Ultimate fallback
        try:
            return create_charts(question, database_name, multiple=False)
        except:
            return None

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "message": "Backend is running"})
@app.route('/api/ask', methods=['POST'])
def ask_question():
    """Process natural language question and return chart data"""
    # Initialize output_str to avoid UnboundLocalError
    output_str = ""
    
    try:
        data = request.get_json()
        
        if not data or 'question' not in data:
            return jsonify({"error": "Question is required"}), 400
        
        question = data['question']
        database = (data.get('database') or 'airportdb')
        try:
            register_database(database)
        except Exception:
            pass
        
        # Check if user wants multiple charts (default to True for now)
        generate_multiple = data.get('multiple_charts', True)
        anydb_mode = bool(data.get('anydb_mode'))
        text_first = data.get('text_first', False)
        markdown = data.get('markdown')
        
        # Conversation ID (optional). If missing, we'll create one for chat-style flow
        conversation_id = data.get('conversation_id')

        if anydb_mode and not text_first:
            # Universal any-DB path: introspect + generate SQL
            result = answer_anydb_question(question, database)
            
            # Check if result is an error response
            if isinstance(result, dict) and "success" in result and not result["success"]:
                return jsonify(result)
            
            return jsonify({
                "success": True,
                "mode": "anydb",
                "sql": result.get("sql"),
                "data": {
                    "title": f"Answer for: {question[:50]}...",
                    "x_axis": result.get("columns", ["col_0"])[0] if result.get("columns") else "Row",
                    "y_axis": "Values",
                    "chart_type": "table",
                    "data": result.get("data", [])
                },
                "question": question,
                "database": database
            })

        if text_first:
            if markdown:
                # Text-first flow with provided markdown: render markdown and extract charts
                # COMMENTED OUT: render_markdown_with_charts is not defined
                # rendered = render_markdown_with_charts(markdown, database, question)
                rendered = {"markdown": markdown, "charts": [], "facts": ""}
                # Persist if a conversation id exists
                if conversation_id:
                    try:
                        add_message(conversation_id, 'user', question, [], {}, title_hint=question, database_name=database)
                        add_message(conversation_id, 'assistant', rendered["markdown"], rendered.get("charts", []), {"mode": "text_first_provided"}, database_name=database, facts=rendered.get("facts"))
                    except Exception as _:
                        pass
                return jsonify({
                    "success": True,
                    "mode": "text_first_provided",
                    "markdown": rendered["markdown"],
                    "charts": rendered["charts"],
                    "question": question,
                    "database": database,
                    "conversation_id": conversation_id
                })
            else:
                # ChatGPT-style mode: generate markdown automatically with selective charts
                print(f"🤖 Generating ChatGPT-style response for: {question}")
                
                # Check if this is casual conversation (greetings, small talk, etc.)
                if is_casual_conversation(question):
                    print(f"💬 CASUAL CONVERSATION detected: {question[:50]}...")
                    casual_response = generate_casual_response(question, database)
                    
                    # Create or use existing conversation
                    if not conversation_id:
                        conversation_id = create_conversation(title=question[:100], database_name=database)
                    
                    # Store the casual exchange
                    try:
                        add_message(conversation_id, 'user', question, [], {}, title_hint=question, database_name=database)
                        add_message(conversation_id, 'assistant', casual_response, [], {"mode": "casual"}, database_name=database)
                    except Exception as e:
                        print(f"⚠️ Failed to store casual conversation: {e}")
                    
                    return jsonify({
                        "success": True,
                        "mode": "casual",
                        "markdown": casual_response,
                        "charts": [],
                        "question": question,
                        "database": database,
                        "conversation_id": conversation_id
                    })
                
                # Build recent history (last 6 messages) if conversation_id provided
                # IMPORTANT: Only load history for EXISTING conversations, not new ones
                history_text = ""
                is_new_conversation = conversation_id is None
                
                if is_new_conversation:
                    print("🆕 NEW CONVERSATION: Starting fresh with no prior history")
                else:
                    print(f"🔄 EXISTING CONVERSATION: Loading history for {conversation_id}")
                
                try:
                    if conversation_id and not is_new_conversation:
                        # Summarize if too many messages
                        try:
                            total = get_message_count(conversation_id)
                            if total > 10:
                                oldest = get_oldest_messages(conversation_id, limit=10)
                                # Build short text to summarize
                                hist_for_sum = []
                                for m in oldest:
                                    role = (m.get('role') or '').upper()
                                    content = (m.get('content_markdown') or '').strip()
                                    if content:
                                        hist_for_sum.append(f"{role}: {content[:600]}")
                                sum_input = "\n".join(hist_for_sum)
                                try:
                                    summarizer = ChatPromptTemplate.from_template(
                                        """Summarize the following chat turns into 4-6 concise bullet points capturing key facts, user intent, and decisions. Keep numbers if present.\n\n{content}\n\nSummary:"""
                                    ) | llm | StrOutputParser()
                                    result = summarizer.invoke({"content": sum_input})
                                    # Ensure result is a string
                                    if not isinstance(result, str):
                                        result = str(result)
                                    summary_text = result.strip()
                                except Exception:
                                    summary_text = "Previous context summarized: (details omitted)."
                                save_summary(conversation_id, summary_text)
                                delete_messages_by_ids(conversation_id, [m['id'] for m in oldest])
                        except Exception:
                            pass
                        msgs = get_history(conversation_id)
                        last_msgs = msgs[-6:] if len(msgs) > 6 else msgs
                        lines = []
                        # Prepend existing summaries
                        try:
                            sums = get_summaries(conversation_id)
                            for s in sums[-3:]:
                                lines.append(f"SUMMARY: {(s.get('content') or '').strip()}")
                        except Exception:
                            pass
                        for m in last_msgs:
                            role = m.get('role', 'assistant')
                            content = (m.get('content_markdown') or '').strip()
                            if not content:
                                continue
                            # truncate very long content
                            if len(content) > 1200:
                                content = content[:1200] + "..."
                            lines.append(f"{role.upper()}: {content}")
                        history_text = "\n".join(lines)
                except Exception:
                    history_text = ""

                # Temporarily inject history into the prompt by overriding at call time
                def _invoke_with_history(q: str, h: str) -> str:
                    try:
                        register_database(database)
                        schema_info = get_schema(database)
                        # Pass conversation_id to scope facts properly
                        exploration = explore_data_for_facts(question=q, database_name=database, conversation_id=conversation_id)
                        facts_text = exploration.get('facts', '(no precomputed facts)')
                        allowed_text = exploration.get('allowed', '(none)')
                        chain = chat_markdown_prompt | llm | StrOutputParser()
                        return chain.invoke({
                            "question": q,
                            "database_name": database,
                            "schema": get_schema(database),
                            "samples": safe_json_dumps(sample_database_tables(database), ensure_ascii=False)[:4000],
                            "facts": facts_text,
                            "allowed_entities": allowed_text,
                            "history": h or ""
                        }).strip()
                    except Exception as ie:
                        print(f"History-invoke failed, falling back without history: {ie}")
                        response_data = generate_chat_response(q, database, conversation_id=conversation_id)
                        return response_data["markdown"]

                # For new conversations, explicitly pass empty history
                final_history = "" if is_new_conversation else history_text
                chat_markdown = _invoke_with_history(question, final_history)
                print(f"📝 Generated markdown length: {len(chat_markdown)} characters")
                
                # Get facts from exploration for this question (for internal storage)
                # Pass conversation_id to scope facts to this conversation only
                exploration = explore_data_for_facts(question=question, database_name=database, conversation_id=conversation_id)
                facts_text = exploration.get("facts", "")
                
                # Extract and generate charts from markdown
                print("🎯 Extracting and generating charts from markdown...")
                rendered = extract_charts_from_markdown(chat_markdown, database, question)
                rendered["facts"] = facts_text
                print(f"📊 Found {len(rendered['charts'])} charts in response")
                
                # Ensure there's a conversation to save into
                try:
                    if not conversation_id:
                        conversation_id = create_conversation(title=question[:80], database_name=database)
                    add_message(conversation_id, 'user', question, [], {}, title_hint=question, database_name=database)
                    # Store facts in database for LLM context, but don't send to frontend
                    add_message(conversation_id, 'assistant', rendered["markdown"], rendered.get("charts", []), {"mode": "chat_style"}, database_name=database, facts=facts_text)
                except Exception as _:
                    pass

                return jsonify({
                    "success": True,
                    "mode": "chat_style",
                    "markdown": rendered["markdown"],
                    "charts": rendered["charts"],
                    "question": question,
                    "database": database,
                    "conversation_id": conversation_id
                })
        
        if generate_multiple:
            # Generate multiple charts
            result = create_charts(question, database, multiple=True)
            
            # Check if result is an error response
            if isinstance(result, dict) and "success" in result and not result["success"]:
                return jsonify(result)
            
            # Check if result contains narrative (multiple charts) or is a single chart
            if isinstance(result, dict) and "charts" in result and "narrative" in result:
                # Multiple charts with narrative
                return jsonify({
                    "success": True,
                    "data": result["charts"],
                    "narrative": result["narrative"],
                    "question": question,
                    "database": database
                })
            else:
                # Single chart or fallback
                return jsonify({
                    "success": True,
                    "data": result,
                    "question": question,
                    "database": database
                })
        else:
            # Generate single chart (original behavior)
            chart_data = create_charts(question, database, multiple=False)
            
            # Check if result is an error response
            if isinstance(chart_data, dict) and "success" in chart_data and not chart_data["success"]:
                return jsonify(chart_data)
        
        return jsonify({
            "success": True,
            "data": chart_data,
            "question": question,
            "database": database
        })
            
    except json.JSONDecodeError as e:
        return jsonify({
            "error": "Failed to parse chart data",
            "details": str(e)
        }), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/check-database', methods=['POST'])
def check_database():
    """Check if a given database exists on the server via SHOW DATABASES.

    Payloads supported (mirrors test_show_databases.py behavior):
    - { "name": "chinook" }  → uses MYSQL_SERVER_URI env to connect, runs SHOW DATABASES
    - { "uri": "mysql+pymysql://user:pass@host:3306/", "name": "chinook" } → uses provided URI
    - { "text": "chinook" }  → same as name
    Returns: { success, available, checked_via, name, match_type?, matched? }
    """
    try:
        data = request.get_json(silent=True) or {}
        text = (data.get('text') or '').strip()
        name = (data.get('name') or data.get('database') or text or '').strip()
        uri = (data.get('uri') or '').strip()

        def _attempt_connect(test_uri: str):
            import sqlalchemy as sa
            engine = sa.create_engine(test_uri, pool_pre_ping=True)
            return engine

        def _show_databases(engine) -> list:
            import sqlalchemy as sa
            with engine.connect() as conn:
                rows = conn.execute(sa.text("SHOW DATABASES")).fetchall()
                return [r[0] for r in rows]

        def _normalize_name(value) -> str:
            try:
                if isinstance(value, bytes):
                    value = value.decode("utf-8", errors="ignore")
                s = str(value)
            except Exception:
                s = ""
            # Strip common wrappers like quotes, backticks, and the python bytes repr artifacts
            s = s.strip().strip("`").strip("\"").strip("'")
            if s.startswith("b(") and s.endswith(")"):
                s = s[2:-1]
            if s.startswith("b'") and s.endswith("'"):
                s = s[2:-1]
            if s.startswith('b"') and s.endswith('"'):
                s = s[2:-1]
            return s.lower()

        # If a URI is provided, attempt real connection check against that server
        if uri:
            try:
                engine = _attempt_connect(uri)
                dbs = _show_databases(engine)
                normalized_list = [_normalize_name(d) for d in dbs]
                normalized_name = _normalize_name(name) if name else None
                exact = (normalized_name in normalized_list) if normalized_name else True
                return jsonify({
                    "success": True,
                    "available": bool(exact),
                    "checked_via": "uri",
                    "name": name or None
                })
            except Exception as exc:
                return jsonify({
                    "success": True,
                    "available": False,
                    "checked_via": "uri",
                    "name": name or None,
                    "error": str(exc)
                })

        # Otherwise, require a name and use server-level URI from environment
        if not name:
            return jsonify({"success": False, "available": False, "error": "Provide either 'uri' or 'name'"}), 400

        server_uri = os.getenv('MYSQL_SERVER_URI')
        if not server_uri:
            return jsonify({
                "success": False,
                "available": False,
                "error": "MYSQL_SERVER_URI is not set. Provide 'uri' or set MYSQL_SERVER_URI."
            }), 400

        try:
            engine = _attempt_connect(server_uri)
            dbs = _show_databases(engine)
            normalized_list = [_normalize_name(d) for d in dbs]
            normalized_name = _normalize_name(name)
            exact = (normalized_name in normalized_list)
            return jsonify({
                "success": True,
                "available": bool(exact),
                "checked_via": "server",
                "name": name
            })
        except Exception as exc:
            return jsonify({
                "success": False,
                "available": False,
                "error": str(exc)
            }), 500
    except Exception as e:
        return jsonify({"success": False, "available": False, "error": str(e)}), 500

@app.route('/api/conversations', methods=['GET'])
def api_list_conversations():
    try:
        items = list_conversations()
        return jsonify({"success": True, "conversations": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/conversations', methods=['POST'])
def api_create_conversation():
    try:
        data = request.get_json(silent=True) or {}
        title = (data.get('title') or '').strip() or None
        cid = create_conversation(title)
        return jsonify({"success": True, "conversation_id": cid})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def api_get_history():
    try:
        cid = request.args.get('conversation_id')
        if not cid:
            return jsonify({"error": "conversation_id is required"}), 400
        messages = get_history(cid)
        return jsonify({"success": True, "messages": messages})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/conversations/<cid>', methods=['DELETE'])
def api_delete_conversation(cid):
    try:
        delete_conversation(cid)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/execute-sql', methods=['POST'])
def execute_sql():
    """Execute raw SQL query (for debugging)"""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({"error": "SQL query is required"}), 400
        
        query = data['query']
        database = (data.get('database') or 'airportdb')
        
        result = run_query(query, database)
        
        return jsonify({
            "success": True,
            "result": result,
            "query": query,
            "database": database
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """Get list of registered databases"""
    try:
        databases_info = get_available_databases()
        return jsonify({
            "success": True,
            "databases": databases_info
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/api/register-database', methods=['POST'])
def api_register_database():
    """Register a new database by name/URI after verifying connectivity.

    Payloads supported:
    - { uri }
    - { name }
    - { name, uri }
    If only name is provided, builds a DB-specific URI using MYSQL_SERVER_URI + name.
    Returns { success, name } on success.
    """
    try:
        data = request.get_json(silent=True) or {}
        name = (data.get('name') or '').strip()
        uri = (data.get('uri') or '').strip()

        # If only URI provided, attempt to infer name from its path
        if uri and not name:
            try:
                # Extract DB part after last '/'
                from urllib.parse import urlparse
                parsed = urlparse(uri)
                path = parsed.path or ''
                inferred = path.strip('/').split('/')[-1] if path else ''
                name = inferred or name
            except Exception:
                pass

        # If only name is provided, construct a URI from MYSQL_SERVER_URI
        if name and not uri:
            base = os.getenv('MYSQL_SERVER_URI')
            if not base:
                return jsonify({"success": False, "error": "MYSQL_SERVER_URI not set; provide 'uri' or set server URI"}), 400
            if not base.endswith('/'):
                base = base + '/'
            uri = base + name

        if not name or not uri:
            return jsonify({"success": False, "error": "Provide at least 'name' or 'uri'"}), 400

        # Verify connectivity to the specific database URI
        try:
            import sqlalchemy as sa
            engine = sa.create_engine(uri, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
        except Exception as exc:
            return jsonify({"success": False, "error": f"Connection failed: {exc}"}), 400

        register_database(name, uri)
        return jsonify({"success": True, "name": name})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/schema', methods=['GET'])
def get_database_schema():
    """Get database schema information for selected database"""
    try:
        db_name = request.args.get('database') or 'airportdb'
        try:
            register_database(db_name)
        except Exception:
            pass
        schema = get_schema(db_name)
        counts = get_table_and_column_counts(db_name)
        return jsonify({
            "success": True,
            "database": db_name,
            "schema": schema,
            "counts": counts
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/schema/cache', methods=['GET', 'POST', 'DELETE'])
def api_manage_schema_cache():
    """Manage both in-memory schema cache and persistent counts cache"""
    import time
    
    try:
        if request.method == 'GET':
            # View cache status (both in-memory and persistent)
            
            # In-memory schema cache status
            schema_cache_status = {
                "cached": _SCHEMA_CACHE["schema"] is not None,
                "age_seconds": None,
                "ttl_seconds": _SCHEMA_CACHE["ttl_seconds"],
                "status": "empty"
            }
            
            if _SCHEMA_CACHE["timestamp"] is not None:
                age = time.time() - _SCHEMA_CACHE["timestamp"]
                schema_cache_status["age_seconds"] = round(age, 1)
                schema_cache_status["status"] = "fresh" if age < _SCHEMA_CACHE["ttl_seconds"] else "expired"
            
            # Persistent counts cache status
            conn = sqlite3.connect(SCHEMA_CACHE_DB)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT database_name, computed_at, row_count, table_count
                FROM schema_counts
                ORDER BY computed_at DESC
            """)
            rows = cursor.fetchall()
            conn.close()
            
            cache_entries = []
            for db_name, computed_at_str, total_rows, table_count in rows:
                computed_at = datetime.fromisoformat(computed_at_str)
                age_minutes = (datetime.now() - computed_at).total_seconds() / 60
                
                cache_entries.append({
                    "database": db_name,
                    "computed_at": computed_at_str,
                    "age_minutes": round(age_minutes, 1),
                    "total_rows": total_rows,
                    "table_count": table_count,
                    "status": "fresh" if age_minutes < 1440 else "expired"  # 24 hours
                })
            
            return jsonify({
                "success": True,
                "schema_cache": schema_cache_status,
                "cache_file": SCHEMA_CACHE_DB,
                "counts_cache": cache_entries
            })
        
        elif request.method == 'POST':
            # Refresh cache for specific database
            db_name = request.args.get('database') or request.json.get('database')
            if not db_name:
                return jsonify({"success": False, "error": "Database name required"}), 400
            
            # Force recompute by bypassing cache
            print(f"🔄 Forcing cache refresh for {db_name}...")
            counts = get_table_and_column_counts(db_name, use_cache=False)
            
            return jsonify({
                "success": True,
                "message": f"Cache refreshed for {db_name}",
                "counts": counts
            })
        
        elif request.method == 'DELETE':
            # Clear cache for specific database or all
            db_name = request.args.get('database')
            
            # Clear in-memory schema cache
            _SCHEMA_CACHE["schema"] = None
            _SCHEMA_CACHE["timestamp"] = None
            print(f"🗑️ Cleared in-memory schema cache")
            
            # Clear persistent counts cache
            conn = sqlite3.connect(SCHEMA_CACHE_DB)
            cursor = conn.cursor()
            
            if db_name:
                cursor.execute("DELETE FROM schema_counts WHERE database_name = ?", (db_name,))
                message = f"Cache cleared for {db_name} (both schema and counts)"
                # Also clear old in-memory cache if exists
                if '_counts_cache' in globals():
                    _counts_cache.pop(db_name, None)
                    _counts_cache_time.pop(db_name, None)
            else:
                cursor.execute("DELETE FROM schema_counts")
                message = "All caches cleared (both schema and counts)"
                # Also clear old in-memory cache if exists
                if '_counts_cache' in globals():
                    _counts_cache.clear()
                    _counts_cache_time.clear()
            
            conn.commit()
            conn.close()
            
            return jsonify({
                "success": True,
                "message": message
            })
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/inspect', methods=['GET'])
def api_inspect_database():
    """Return database schema information (API-based mode)."""
    try:
        db_name = request.args.get('database') or 'zigment'
        
        # In API mode, return simplified schema from API
        schema_data = fetch_schema_from_api()
        
        # Format it similar to old inspect format for compatibility
        preview = {}
        for collection in schema_data.get("collections", []):
            coll_name = collection.get("name", "").lower()
            fields = collection.get("fields", [])
            cols = [f.get("name") for f in fields]
            
            preview[coll_name] = {
                "columns": cols,
                "rows": [],  # No sample data in API mode
                "primary_key": ["_id"]  # MongoDB default
            }
        
        return jsonify({"success": True, "tables": preview})
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# Old SQL-based inspect code removed - API mode only

@app.route('/api/ping', methods=['GET'])
def api_ping():
    """Simple health check to verify connectivity over LAN."""
    return jsonify({
        "success": True,
        "message": "pong",
        "client": request.remote_addr,
        "host": request.host,
        "origin": request.headers.get('Origin')
    })

if __name__ == '__main__':
    host = os.getenv('HOST', '0.0.0.0')  # listen on all interfaces by default
    try:
        port = int(os.getenv('PORT', '1000'))
    except Exception:
        port = 1000
    print(f"\n🚀 Starting Flask on http://{host}:{port} (LAN IP example: http://192.168.0.193:{port})")
    print("🔐 CORS origins:", os.getenv('CORS_ORIGINS', '*'))
    app.run(debug=True, host=host, port=port)