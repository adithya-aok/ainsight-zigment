# Detailed Flowchart: Question Input → Markdown with Charts Output

**Example Question:** "Most common event types in the last 30 days"

---

## 1. INITIAL REQUEST HANDLING

```
┌─────────────────────────────────────┐
│   POST /api/ask                      │
│   {question, database,               │
│    conversation_id, text_first}       │
└──────────────┬────────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   @app.route('/api/ask')            │
│   ask_question()                     │
└──────────────┬────────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Extract Parameters:               │
│   - question                        │
│   - database (default: 'zigment')   │
│   - conversation_id (optional)      │
│   - text_first (default: False)     │
└──────────────┬────────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Check: text_first == False?        │
│   → YES: Go to Markdown Generation   │
│   → NO: Go to Chart Generation      │
└──────────────┬────────────────────────┘
               │
               ▼
```

---

## 2. CLASSIFICATION PHASE

```
┌─────────────────────────────────────┐
│   classify_question_intent()        │
│   → Calls LLM (gpt-4o-mini)          │
│   → Returns: 'DATA', 'CASUAL', etc. │
└──────────────┬────────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Classification Result:             │
│   'Most common event types...'       │
│   → DATA (DATA QUERY)                │
└──────────────┬────────────────────────┘
               │
               ├─────────────────┐
               │                 │
               ▼                 ▼
┌────────────────────────┐  ┌────────────────────────┐
│ is_casual_conversation()│  │ Check conversation_id  │
│ → Returns False         │  │ → NEW: Create new       │
└──────────┬──────────────┘  │   conversation          │
           │                 │ → EXISTS: Load history  │
           └─────────┬───────┘  └──────────┬────────────┘
                     │                     │
                     └──────────┬──────────┘
                                │
                                ▼
```

---

## 3. DEEP EXPLORATION PHASE (Data Understanding)

```
┌─────────────────────────────────────┐
│   explore_data_for_facts()          │
│   Called twice:                      │
│   1. For markdown generation         │
│   2. For storing facts               │
└──────────────┬────────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   run_deep_exploration()             │
│   ┌────────────────────────────┐    │
│   │ Purpose: Generate           │    │
│   │ exploratory queries to      │    │
│   │ understand data before      │    │
│   │ generating final query      │    │
│   └────────────────────────────┘    │
└──────────┬───────────────────────────┘
           │
           ├─────────────────────────┐
           │                         │
           ▼                         ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│ _SCHEMA_JSON            │  │ get_table_and_column_  │
│ (module-level constant) │  │ counts()                │
│                         │  │                         │
│ Returns cached JSON     │  │ Fetches actual row     │
│ string (computed once)  │  │ counts via API          │
│                         │  │                         │
│ Time: 0.00s (instant)   │  │ Time: 5.93s - 9.47s    │
└──────────┬──────────────┘  └──────────┬─────────────┘
           │                           │
           └──────────────┬────────────┘
                          │
                          ▼
┌─────────────────────────────────────┐
│   sample_database_tables()           │
│   Fetches sample rows from each      │
│   table via API                      │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   get_past_facts()                  │
│   (Optional - scoped to conversation)│
│   → NEW conversation: "(initial      │
│      exploration)"                   │
│                                     │
│   Calls: get_summaries()            │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   generate_table_size_guidance()    │
│   Generates guidance text about     │
│   table sizes for LLM               │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   deep_explore_prompt               │
│   + LLM (gpt-4o-mini)               │
│   → Generates JSON with             │
│      exploratory queries            │
│                                     │
│   Input includes:                   │
│   - question                        │
│   - _SCHEMA_JSON (schema)           │
│   - counts_data                     │
│   - samples                         │
│   - past_facts                      │
│   - table_size_guidance             │
│                                     │
│   Time: 2.41s - 6.04s              │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Parse LLM Response:               │
│   {                                │
│     "explorations": [{             │
│       "purpose": "...",             │
│       "sql": "SELECT e.type, ..."   │
│     }]                             │
│   }                                │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   For each exploration:              │
│   1. _strip_query_fences()          │
│   2. ensure_limit(query, 20)        │
│   3. run_query()                     │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   run_query()                       │
│   → execute_noql_query()            │
│   → POST to https://api.zigment.ai  │
│   → Returns rows + columns          │
│                                     │
│   Execution Time: 0.99s - 3.45s      │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Extract Facts:                     │
│   - "Identify...: 7 records found"  │
│   - "top result - type: ..."        │
│                                     │
│   Extract Allowed Entities:          │
│   - String values from results       │
│                                     │
│   Total Deep Exploration Time:      │
│   17.98s - 32.35s                   │
└──────────┬───────────────────────────┘
           │
           ▼
```

---

## 4. MARKDOWN GENERATION PHASE

```
┌─────────────────────────────────────┐
│   generate_chat_response()           │
│   ┌────────────────────────────┐    │
│   │ Purpose: Create natural    │    │
│   │ language response with     │    │
│   │ exactly 1 embedded chart   │    │
│   │ directive                  │    │
│   └────────────────────────────┘    │
└──────────┬───────────────────────────┘
           │
           ├─────────────────────────┐
           │                         │
           ▼                         ▼
┌─────────────────────────┐  ┌─────────────────────────┐
│ _SCHEMA_JSON            │  │ explore_data_for_facts() │
│ (direct constant access) │  │ → run_deep_exploration() │
│                         │  │ → Returns facts +        │
│ Time: 0.00s (instant)   │  │   allowed entities       │
└──────────┬──────────────┘  └──────────┬───────────────┘
           │                          │
           └──────────────┬───────────┘
                          │
                          ▼
┌─────────────────────────────────────┐
│   sample_database_tables()           │
│   Fetches fresh samples              │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   chat_markdown_prompt               │
│   + LLM (gpt-4o-mini)               │
│                                     │
│   🚨 CRITICAL: Prompt explicitly    │
│   instructs to generate EXACTLY     │
│   1 chart only (not 2-4)             │
│                                     │
│   Input:                            │
│   - question                        │
│   - _SCHEMA_JSON (schema)           │
│   - samples                         │
│   - facts (from exploration)         │
│   - allowed entities                │
│   - history (if available)          │
│                                     │
│   Output:                           │
│   - Markdown text with exactly      │
│     1 ```chart block embedded       │
│                                     │
│   Generated markdown length:        │
│   3325 characters                  │
└──────────┬───────────────────────────┘
           │
           ▼
```

---

## 5. CHART EXTRACTION & GENERATION PHASE

```
┌─────────────────────────────────────┐
│   extract_charts_from_markdown()     │
│   ┌────────────────────────────┐    │
│   │ Searches for ```chart      │    │
│   │ blocks using regex         │    │
│   └────────────────────────────┘    │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Found: 1 chart block               │
│   {                                 │
│     "type": "bar",                  │
│     "question": "Most common...",   │
│     "title": "Top Event Types...",  │
│     "db": "zigment"                 │
│   }                                 │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   parse_chart_block()               │
│   → Parses JSON from chart block    │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   build_chart_from_cfg()            │
│   For each chart block:             │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   create_anydb_sql_chain()          │
│   → Returns NoQLChain object        │
│     (wraps LLM query generation)    │
│                                     │
│   Uses: _SCHEMA_JSON directly        │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   noql_chain.invoke()               │
│   ┌────────────────────────────┐    │
│   │ 1. _SCHEMA_JSON (schema)   │    │
│   │ 2. NOQL_DIRECT_PROMPT       │    │
│   │ 3. LLM (gpt-3.5-turbo)     │    │
│   │ 4. Generate NoQL query     │    │
│   └────────────────────────────┘    │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Generated NoQL Query:             │
│   SELECT type as event_type,        │
│          COUNT(*) as event_count    │
│   FROM events                        │
│   WHERE is_deleted = false          │
│   AND created_at >= ...             │
│   GROUP BY event_type                │
│   ORDER BY event_count DESC          │
│   LIMIT 20                           │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Query Processing:                 │
│   1. _strip_query_fences()          │
│   2. ensure_limit(query, 20)        │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   run_query()                       │
│   → execute_noql_query()            │
│   → POST to https://api.zigment.ai  │
│                                     │
│   Response:                         │
│   - 7 rows with 2 columns           │
│   - ['event_type', 'event_count']   │
│   - Example:                        │
│     ('WHATSAPP_MESSAGE_SENT', 383) │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   format_data_for_chart_type()      │
│   ┌────────────────────────────┐    │
│   │ Chart Type: bar            │    │
│   │ → Formats as:              │    │
│   │   [{                       │    │
│   │     "label": "...",        │    │
│   │     "value": 383           │    │
│   │   }, ...]                 │    │
│   └────────────────────────────┘    │
│                                     │
│   Calls: safe_float()               │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   generate_axis_labels()            │
│   → "Event Type" (x_axis)           │
│   → "Event Count" (y_axis)          │
│                                     │
│   Calls (if LLM enabled):          │
│   - select_best_axis_column()      │
│   - is_id_column()                 │
│   - generate_readable_label()      │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   validate_chart_necessity()        │
│   → Checks if chart has 2+ data     │
│     points (not useless)            │
│   → APPROVED (has 7 data points)     │
│                                     │
│   Uses: chart_validator_prompt       │
│   + LLM (gpt-4o-mini)               │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Chart Object Created:              │
│   {                                 │
│     "id": "chart_bac0c680",        │
│     "title": "Top Event Types...",  │
│     "x_axis": "Event Type",         │
│     "y_axis": "Event Count",         │
│     "chart_type": "bar",            │
│     "data": [{...}, {...}, ...]     │
│   }                                 │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Replace ```chart block with:       │
│   {{chart:chart_bac0c680}}          │
│   in markdown                       │
└──────────┬───────────────────────────┘
           │
           ▼
```

---

## 6. DATABASE STORAGE PHASE

```
┌─────────────────────────────────────┐
│   create_conversation()              │
│   (if conversation_id is new)       │
│   → Creates SQLite record            │
│                                     │
│   Calls:                            │
│   - _ensure_sqlite()                │
│   - _gen_id()                       │
│   - _now_str()                      │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   add_message()                     │
│   ┌────────────────────────────┐   │
│   │ 1. User message:            │   │
│   │    - question              │   │
│   │    - conversation_id       │   │
│   │                            │   │
│   │ 2. Assistant message:      │   │
│   │    - markdown (with        │   │
│   │      chart placeholders)   │   │
│   │    - charts (array with    │   │
│   │      exactly 1 chart)      │   │
│   │    - facts (for context)   │   │
│   └────────────────────────────┘   │
│                                     │
│   Calls:                            │
│   - _gen_id()                       │
│   - _now_str()                      │
│   - safe_json_dumps()               │
└──────────┬───────────────────────────┘
           │
           ▼
```

---

## 7. FINAL RESPONSE

```
┌─────────────────────────────────────┐
│   Return JSON Response:             │
│   {                                 │
│     "success": true,                │
│     "markdown": "...",              │
│     "charts": [{                    │
│       "id": "chart_bac0c680",      │
│       "title": "...",               │
│       "chart_type": "bar",          │
│       "data": [...]                 │
│     }],                             │
│     "facts": "...",                  │
│     "conversation_id": "..."        │
│   }                                 │
└──────────┬───────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Frontend Receives:                │
│   1. Markdown text with exactly     │
│      1 {{chart:id}} placeholder     │
│   2. Charts array with exactly      │
│      1 chart object                 │
│   3. Renders markdown and           │
│      replaces placeholder with      │
│      chart component                │
└─────────────────────────────────────┘
```

---

## KEY FUNCTIONS MAPPING

| Phase | Function | Purpose |
|-------|----------|---------|
| **Entry** | `ask_question()` | Main Flask route handler |
| **Classification** | `classify_question_intent()` | Categorize question type |
| **Classification** | `is_casual_conversation()` | Check if question is casual |
| **Conversation** | `create_conversation()` | Create new conversation record |
| **Conversation** | `get_history()` | Retrieve conversation history |
| **Exploration** | `explore_data_for_facts()` | Entry point for data exploration |
| **Exploration** | `run_deep_exploration()` | Generate & execute exploratory queries |
| **Schema** | `_SCHEMA_JSON` | **Module-level constant** - cached schema JSON |
| **Schema** | `get_hardcoded_schema()` | Returns schema dict (for dict format needs) |
| **Counts** | `get_table_and_column_counts()` | Fetches row counts via API |
| **Samples** | `sample_database_tables()` | Fetches sample rows via API |
| **Past Facts** | `get_past_facts()` | Gets past exploration facts |
| **Past Facts** | `get_summaries()` | Gets conversation summaries |
| **Query Execution** | `run_query()` | Execute NoQL query via API |
| **Query Execution** | `execute_noql_query()` | POST to Zigment API |
| **Query Processing** | `_strip_query_fences()` | Remove markdown fences from queries |
| **Query Processing** | `ensure_limit()` | Ensure query has LIMIT clause |
| **Markdown** | `generate_chat_response()` | Generate natural language response |
| **Chart Extraction** | `extract_charts_from_markdown()` | Find and parse ```chart blocks |
| **Chart Parsing** | `parse_chart_block()` | Parse JSON from chart block |
| **Chart Generation** | `build_chart_from_cfg()` | Build chart from config |
| **Query Chain** | `create_anydb_sql_chain()` | Create NoQL query generator |
| **Data Formatting** | `format_data_for_chart_type()` | Format query results for charts |
| **Data Formatting** | `safe_float()` | Safely convert values to float |
| **Validation** | `validate_chart_necessity()` | Check if chart is useful |
| **Axis Labels** | `generate_axis_labels()` | Generate readable axis labels |
| **Axis Labels** | `select_best_axis_column()` | Select best column for axis |
| **Axis Labels** | `is_id_column()` | Check if column is ID field |
| **Axis Labels** | `generate_readable_label()` | Convert column names to labels |
| **Storage** | `add_message()` | Save messages to SQLite |
| **Storage** | `_gen_id()` | Generate unique IDs |
| **Storage** | `_now_str()` | Get current timestamp |
| **Storage** | `_ensure_sqlite()` | Ensure SQLite tables exist |

---

## FUNCTION CALL FLOW SUMMARY

### Complete Call Chain for "Most common event types in the last 30 days":

```
1. ask_question()
   ├── register_database() [no-op, kept for compatibility]
   ├── is_casual_conversation()
   │   └── (returns False - not casual)
   ├── create_conversation() [if new]
   │   ├── _ensure_sqlite()
   │   ├── _gen_id()
   │   └── _now_str()
   ├── generate_chat_response()
   │   ├── _SCHEMA_JSON [direct constant access - instant]
   │   ├── explore_data_for_facts()
   │   │   └── run_deep_exploration()
   │   │       ├── _SCHEMA_JSON [direct constant access - instant]
   │   │       ├── get_table_and_column_counts()
   │   │       │   └── execute_noql_query()
   │   │       ├── sample_database_tables()
   │   │       │   ├── get_hardcoded_schema() [returns _SCHEMA_DICT]
   │   │       │   └── run_query()
   │   │       │       └── execute_noql_query()
   │   │       ├── get_past_facts()
   │   │       │   └── get_summaries()
   │   │       ├── generate_table_size_guidance()
   │   │       ├── (LLM call via deep_explore_prompt)
   │   │       ├── _strip_query_fences()
   │   │       ├── ensure_limit()
   │   │       └── run_query()
   │   │           └── execute_noql_query()
   │   └── sample_database_tables()
   │       ├── get_hardcoded_schema() [returns _SCHEMA_DICT]
   │       └── run_query()
   │           └── execute_noql_query()
   ├── extract_charts_from_markdown()
   │   ├── parse_chart_block()
   │   └── build_chart_from_cfg()
   │       ├── create_anydb_sql_chain()
   │       │   └── _SCHEMA_JSON [direct constant access]
   │       ├── (LLM call via NOQL_DIRECT_PROMPT)
   │       ├── _strip_query_fences()
   │       ├── ensure_limit()
   │       ├── run_query()
   │       │   └── execute_noql_query()
   │       ├── format_data_for_chart_type()
   │       │   └── safe_float()
   │       └── generate_axis_labels()
   │           ├── select_best_axis_column()
   │           │   └── is_id_column()
   │           └── generate_readable_label()
   └── add_message() [saves user message]
       ├── _gen_id()
       └── _now_str()
   └── add_message() [saves assistant response]
       ├── _gen_id()
       ├── _now_str()
       └── safe_json_dumps()
```

---

## LLM CALLS SUMMARY

1. **Classification** (gpt-4o-mini): Determine question intent
2. **Deep Exploration** (gpt-4o-mini): Generate exploratory queries
3. **Markdown Generation** (gpt-4o-mini): Create natural language response with **exactly 1 chart block**
4. **Chart Query Generation** (gpt-3.5-turbo): Generate NoQL query for chart
5. **Chart Validation** (gpt-4o-mini): Validate chart usefulness

---

## API CALLS SUMMARY

1. **get_table_and_column_counts()**: POST to Zigment API for counts
2. **sample_database_tables()**: POST to Zigment API for samples  
3. **run_query()** (during exploration): POST to Zigment API
4. **run_query()** (for charts): POST to Zigment API

---

## KEY OPTIMIZATIONS

### Schema Access Optimization:
- **OLD**: `get_schema()` → `get_hardcoded_schema()` → `json.dumps()` (function call overhead + JSON conversion each time)
- **NEW**: `_SCHEMA_JSON` (module-level constant, computed once at module load)
- **Result**: Schema access is now instant (0.00s) instead of computing each time

### Chart Generation Optimization:
- **OLD**: Prompt could generate 2-4 charts for "analysis" questions
- **NEW**: Prompt explicitly instructs to generate **EXACTLY 1 chart** unless explicitly requested
- **Result**: Always generates 1 chart, avoiding unnecessary multiple chart generation

---

## TIMING BREAKDOWN (from terminal output)

- **Schema fetch**: 0.00s (now instant - using cached `_SCHEMA_JSON`)
- **Counts fetch**: 5.93s - 9.47s (API call)
- **LLM exploration**: 2.41s - 6.04s
- **Query execution**: 0.99s - 3.45s
- **Total deep exploration**: 17.98s - 32.35s
- **Markdown generation**: ~2-3s (estimated)
- **Chart generation**: ~1-2s (estimated)

**Total estimated time**: ~25-40 seconds

---

## CRITICAL CHANGES FROM PREVIOUS VERSION

1. **Schema Access**: Now uses `_SCHEMA_JSON` constant directly instead of `get_schema()` function
2. **Chart Generation**: Updated to generate **EXACTLY 1 chart** by default (not 2-4)
3. **Prompt Instructions**: Extremely explicit instructions with multiple warnings about single chart generation
