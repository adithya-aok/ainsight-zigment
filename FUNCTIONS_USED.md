# Complete List of Functions Used: Question Input → Markdown with Charts

**Example Question:** "Most common event types in the last 30 days"

This document lists **EVERY function from app.py** that is called during the flow from question input to markdown with charts output.

---

## 📋 FUNCTIONS BY PHASE

### 1. ENTRY POINT & REQUEST HANDLING

#### `ask_question()` (Line 5401)
- **Main Flask route handler**: `@app.route('/api/ask', methods=['POST'])`
- **Purpose**: Entry point for all question processing
- **Calls**:
  - `register_database()`
  - `is_casual_conversation()`
  - `generate_casual_response()` (if casual)
  - `generate_chat_response()` (if not casual)
  - `explore_data_for_facts()`
  - `extract_charts_from_markdown()`
  - `create_conversation()`
  - `add_message()`
  - `get_history()`
  - `get_message_count()`
  - `get_oldest_messages()`
  - `delete_messages_by_ids()`
  - `save_summary()`

---

### 2. QUESTION CLASSIFICATION

#### `is_casual_conversation(question: str) -> bool` (Line 1916)
- **Purpose**: Detects casual/greeting questions vs data queries
- **Called by**: `ask_question()`

#### `generate_casual_response(question: str, database_name: str) -> str` (Line 1979)
- **Purpose**: Generates responses for casual questions (greetings, help, etc.)
- **Called by**: `ask_question()` (when question is casual)
- **Calls**: None (returns static responses)

---

### 3. CONVERSATION MANAGEMENT

#### `create_conversation(title: str | None = None, database_name: str | None = None) -> str` (Line 887)
- **Purpose**: Creates new conversation record in SQLite
- **Called by**: `ask_question()`
- **Calls**:
  - `_ensure_sqlite()`
  - `_gen_id()`
  - `_now_str()`

#### `get_history(conversation_id: str)` (Line 923)
- **Purpose**: Retrieves all messages for a conversation
- **Called by**: `ask_question()`, `api_get_history()`

#### `get_message_count(conversation_id: str) -> int` (Line 951)
- **Purpose**: Returns number of messages in conversation
- **Called by**: `ask_question()` (for message limit checks)

#### `get_oldest_messages(conversation_id: str, limit: int = 10)` (Line 958)
- **Purpose**: Gets oldest messages for summarization
- **Called by**: `ask_question()` (for context trimming)

#### `delete_messages_by_ids(conversation_id: str, ids: list[str])` (Line 968)
- **Purpose**: Deletes specific messages
- **Called by**: `ask_question()` (when trimming context)

#### `save_summary(conversation_id: str, content: str) -> str` (Line 977)
- **Purpose**: Saves conversation summary
- **Called by**: `ask_question()` (for long conversations)
- **Calls**:
  - `_gen_id()`
  - `_now_str()`

---

### 4. SCHEMA & DATABASE FUNCTIONS

#### `get_schema(database_name="zigment")` (Line 2141)
- **Purpose**: Returns schema information (wrapper around hardcoded schema)
- **Called by**: Multiple functions throughout the flow
- **Calls**:
  - `get_hardcoded_schema()`

#### `get_hardcoded_schema() -> dict` (Line 660)
- **Purpose**: Returns hardcoded schema for 5 tables
- **Called by**: `get_schema()`

#### `get_table_and_column_counts(database_name: str) -> dict` (Line 2160)
- **Purpose**: Fetches row counts and column counts via API
- **Called by**: `run_deep_exploration()`
- **Calls**:
  - `execute_noql_query()`

#### `generate_table_size_guidance(counts: dict, threshold: int = 100000) -> str` (Line 2210)
- **Purpose**: Generates guidance text about table sizes for LLM prompts
- **Called by**: `run_deep_exploration()`

---

### 5. DATA EXPLORATION & FACTS

#### `explore_data_for_facts(question: str = "", rounds: int = 0, per_round: int = 0, database_name: str | None = None, conversation_id: str | None = None) -> dict` (Line 2030)
- **Purpose**: Main entry point for data exploration
- **Called by**: `generate_chat_response()`, `ask_question()`
- **Calls**:
  - `run_deep_exploration()` (if question provided)
  - `passive_exploration_fallback()` (if no question)

#### `run_deep_exploration(question: str, database_name: str, max_queries: int = 3, conversation_id: str | None = None) -> dict` (Line 1720)
- **Purpose**: Generates and executes exploratory queries using LLM
- **Called by**: `explore_data_for_facts()`
- **Calls**:
  - `get_schema()`
  - `get_table_and_column_counts()`
  - `sample_database_tables()`
  - `get_past_facts()`
  - `generate_table_size_guidance()`
  - `_strip_query_fences()`
  - `ensure_limit()`
  - `run_query()`
  - `_fmt()` (helper for formatting)

#### `passive_exploration_fallback(database_name: str) -> dict` (Line 1885)
- **Purpose**: Fallback exploration when no specific question provided
- **Called by**: `explore_data_for_facts()`
- **Calls**:
  - `get_schema()`
  - `get_table_and_column_counts()`
  - `sample_database_tables()`
  - `run_query()`

#### `get_past_facts(database_name: str, limit: int = 10, conversation_id: str | None = None) -> str` (Line 999)
- **Purpose**: Retrieves past exploration facts from current conversation
- **Called by**: `run_deep_exploration()`
- **Calls**:
  - `get_summaries()`

#### `get_summaries(conversation_id: str) -> list[dict]` (Line 989)
- **Purpose**: Gets conversation summaries from SQLite
- **Called by**: `get_past_facts()`

---

### 6. SAMPLE DATA RETRIEVAL

#### `sample_database_tables(database_name: str, max_rows: int = 3, max_tables: int = 10) -> dict` (Line 2455)
- **Purpose**: Fetches sample rows from each table
- **Called by**: `run_deep_exploration()`, `passive_exploration_fallback()`, `generate_chat_response()`
- **Calls**:
  - `get_schema()`
  - `run_query()`

---

### 7. QUERY EXECUTION

#### `run_query(query, database_name="zigment", return_columns=False)` (Line 2269)
- **Purpose**: Executes NoQL query and returns results
- **Called by**: Multiple functions (exploration, chart generation, etc.)
- **Calls**:
  - `execute_noql_query()`

#### `execute_noql_query(sql_query: str) -> dict` (Line 784)
- **Purpose**: Makes POST request to Zigment API
- **Called by**: `run_query()`, `get_table_and_column_counts()`

---

### 8. QUERY PROCESSING

#### `_strip_query_fences(q) -> str` (Line 1072)
- **Purpose**: Removes markdown fences (```noql, ```sql) from query strings
- **Called by**: `run_deep_exploration()`, `build_chart_from_cfg()`, `create_charts()`, `answer_anydb_question()`

#### `ensure_limit(query: str, default_limit: int = 50) -> str` (Line 1049)
- **Purpose**: Ensures query has LIMIT clause
- **Called by**: `run_deep_exploration()`, `build_chart_from_cfg()`, `create_charts()`, `answer_anydb_question()`

---

### 9. MARKDOWN GENERATION

#### `generate_chat_response(question: str, database_name: str, conversation_id: str | None = None) -> dict` (Line 5041)
- **Purpose**: Generates ChatGPT-style markdown response with chart directives
- **Called by**: `ask_question()`
- **Calls**:
  - `get_schema()`
  - `explore_data_for_facts()`
  - `sample_database_tables()`

---

### 10. CHART EXTRACTION & GENERATION

#### `extract_charts_from_markdown(markdown: str, database_name: str, actual_question: str = None) -> dict` (Line 4894)
- **Purpose**: Finds ```chart blocks in markdown and generates charts
- **Called by**: `ask_question()`
- **Calls**:
  - `parse_chart_block()`
  - `build_chart_from_cfg()`

#### `parse_chart_block(block_text: str) -> dict` (Line 4808)
- **Purpose**: Parses JSON from chart block text
- **Called by**: `extract_charts_from_markdown()`

#### `build_chart_from_cfg(cfg: dict, database_name: str, actual_question: str = None) -> dict` (Line 4973)
- **Purpose**: Builds chart from configuration dictionary
- **Called by**: `extract_charts_from_markdown()`
- **Calls**:
  - `create_anydb_sql_chain()`
  - `_strip_query_fences()`
  - `ensure_limit()`
  - `run_query()`
  - `format_data_for_chart_type()`
  - `generate_axis_labels()`

#### `create_charts(question: str, database_name="airportdb")` (Line 5298)
- **Purpose**: Creates single chart for a question
- **Called by**: `ask_question()` (alternative path)
- **Calls**:
  - `check_question_relevance()`
  - `create_anydb_sql_chain()`
  - `_strip_query_fences()`
  - `ensure_limit()`
  - `run_query()`
  - `format_data_for_chart_type()`
  - `generate_axis_labels()`
  - `create_no_data_response()`
  - `create_error_response()`

#### `create_anydb_sql_chain(database_name: str)` (Line 2740)
- **Purpose**: Creates NoQL query generation chain (wraps LLM)
- **Called by**: `build_chart_from_cfg()`, `create_charts()`, `answer_anydb_question()`
- **Calls**:
  - `get_schema()`

---

### 11. DATA FORMATTING & PROCESSING

#### `format_data_for_chart_type(data, chart_type, question, columns=None)` (Line 5119)
- **Purpose**: Formats query results for specific chart types (bar, line, pie, scatter, table)
- **Called by**: `build_chart_from_cfg()`, `create_charts()`, `answer_anydb_question()`
- **Calls**:
  - `safe_float()`
  - `convert_day_number_to_name()` (indirectly via day handling)

#### `safe_float(value)` (Line 4583)
- **Purpose**: Safely converts values to float
- **Called by**: `format_data_for_chart_type()`, scatter plot formatting

#### `convert_day_number_to_name(day_num)` (Line 5083)
- **Purpose**: Converts day-of-week number (1-7) to day name
- **Called by**: `format_data_for_chart_type()` (indirectly)

#### `is_day_of_week_column(column_name, question)` (Line 5099)
- **Purpose**: Checks if column represents day of week
- **Called by**: (Potentially used in formatting logic)

---

### 12. AXIS LABEL GENERATION

#### `generate_axis_labels(chart_type, columns, question, title)` (Line 4676)
- **Purpose**: Generates readable axis labels for charts
- **Called by**: `build_chart_from_cfg()`, `create_charts()`
- **Calls**:
  - `generate_readable_label()` (if LLM labels enabled)
  - `select_best_axis_column()` (if LLM labels enabled)

#### `generate_readable_label(column_name, axis_type, question)` (Line 4754)
- **Purpose**: Converts column names to readable labels using LLM
- **Called by**: `generate_axis_labels()`

#### `select_best_axis_column(columns, axis_type="x")` (Line 4594)
- **Purpose**: Intelligently selects best column for specified axis
- **Called by**: `generate_axis_labels()`

#### `is_id_column(column_name)` (Line 4658)
- **Purpose**: Checks if column is likely an ID field
- **Called by**: `select_best_axis_column()`, `generate_axis_labels()`

#### `get_best_column_index(columns, axis_type="x")` (Line 4664)
- **Purpose**: Gets index of best column for specified axis
- **Called by**: (Potentially used in axis label generation)

---

### 13. CHART VALIDATION

#### `validate_chart_necessity(question: str, chart_data: dict) -> dict` (Line 4815)
- **Purpose**: Validates if chart is truly necessary or should be replaced with text
- **Called by**: `extract_charts_from_markdown()` (indirectly, via chart validation chain)
- **Calls**: LLM via `chart_validator_prompt`

---

### 14. QUESTION RELEVANCE & VALIDATION

#### `check_question_relevance(question: str, database_name: str) -> dict` (Line 2382)
- **Purpose**: Validates if question is relevant to database
- **Called by**: `create_charts()`, `answer_anydb_question()`
- **Calls**:
  - `get_schema()`

---

### 15. ERROR HANDLING

#### `create_error_response(error_type: str, message: str, suggestion: str = None) -> dict` (Line 2430)
- **Purpose**: Creates standardized error response
- **Called by**: `create_charts()`, `answer_anydb_question()`, error handlers

#### `create_no_data_response(question: str) -> dict` (Line 2441)
- **Purpose**: Creates response when no data found
- **Called by**: `create_charts()`, `answer_anydb_question()`

---

### 16. TABLE FORMATTING (Alternative Path)

#### `answer_anydb_question(question: str, database_name: str)` (Line 2754)
- **Purpose**: Alternative path: answers question and returns table-formatted data
- **Called by**: `ask_question()` (when `anydb_mode=True`)
- **Calls**:
  - `check_question_relevance()`
  - `create_anydb_sql_chain()`
  - `_strip_query_fences()`
  - `ensure_limit()`
  - `run_query()`
  - `format_data_for_chart_type()`
  - `create_no_data_response()`
  - `create_error_response()`

---

### 17. UTILITY FUNCTIONS

#### `_ensure_sqlite()` (Line 806)
- **Purpose**: Ensures SQLite database and tables exist
- **Called by**: `create_conversation()`

#### `_now_str()` (Line 879)
- **Purpose**: Returns current timestamp as string
- **Called by**: `create_conversation()`, `add_message()`, `save_summary()`

#### `_gen_id(prefix: str) -> str` (Line 882)
- **Purpose**: Generates unique ID with prefix
- **Called by**: `create_conversation()`, `add_message()`, `save_summary()`

#### `_fmt(v)` (Line 1711)
- **Purpose**: Safely formats values for facts
- **Called by**: `run_deep_exploration()`

#### `safe_json_dumps(obj, **kwargs)` (Line 651)
- **Purpose**: Safely converts objects to JSON strings
- **Called by**: `run_deep_exploration()`, `generate_chat_response()`, `add_message()`

#### `register_database(name: str, uri: str = None)` (Line 633)
- **Purpose**: Registers database (currently no-op, kept for compatibility)
- **Called by**: `ask_question()`

---

## 📊 FUNCTION CALL FLOW SUMMARY

### Complete Call Chain for "Most common event types in the last 30 days":

```
1. ask_question()
   ├── register_database()
   ├── is_casual_conversation()
   │   └── (returns False - not casual)
   ├── create_conversation() [if new]
   │   ├── _ensure_sqlite()
   │   ├── _gen_id()
   │   └── _now_str()
   ├── generate_chat_response()
   │   ├── get_schema()
   │   │   └── get_hardcoded_schema()
   │   ├── explore_data_for_facts()
   │   │   └── run_deep_exploration()
   │   │       ├── get_schema()
   │   │       │   └── get_hardcoded_schema()
   │   │       ├── get_table_and_column_counts()
   │   │       │   └── execute_noql_query()
   │   │       ├── sample_database_tables()
   │   │       │   ├── get_schema()
   │   │       │   │   └── get_hardcoded_schema()
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
   │       ├── get_schema()
   │       │   └── get_hardcoded_schema()
   │       └── run_query()
   │           └── execute_noql_query()
   ├── extract_charts_from_markdown()
   │   ├── parse_chart_block()
   │   └── build_chart_from_cfg()
   │       ├── create_anydb_sql_chain()
   │       │   └── get_schema()
   │       │       └── get_hardcoded_schema()
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

## 📈 FUNCTION USAGE STATISTICS

### Most Called Functions:
1. **`get_schema()`** - Called ~8-10 times (fetches schema for various purposes)
2. **`get_hardcoded_schema()`** - Called ~8-10 times (via `get_schema()`)
3. **`run_query()`** - Called ~4-6 times (exploration + chart generation)
4. **`execute_noql_query()`** - Called ~4-6 times (all API calls)
5. **`_strip_query_fences()`** - Called ~3-4 times (query cleanup)
6. **`ensure_limit()`** - Called ~3-4 times (query safety)

### Functions Called Once:
- `ask_question()` - Entry point
- `generate_chat_response()` - Markdown generation
- `extract_charts_from_markdown()` - Chart extraction
- `build_chart_from_cfg()` - Chart building
- `format_data_for_chart_type()` - Data formatting
- `generate_axis_labels()` - Label generation

### Total Unique Functions Used: **~50 functions**

---

## 🔄 LLM INTERACTIONS (Not Functions, but Components)

1. **`deep_explore_prompt`** (Prompt Template, Line 1453)
   - Used in: `run_deep_exploration()`
   - Model: gpt-4o-mini
   - Purpose: Generate exploratory queries

2. **`chat_markdown_prompt`** (Prompt Template, Line 1100)
   - Used in: `generate_chat_response()`
   - Model: gpt-4o-mini
   - Purpose: Generate markdown with chart directives

3. **`NOQL_DIRECT_PROMPT`** (Prompt Template, Line 37)
   - Used in: `create_anydb_sql_chain()`
   - Model: gpt-3.5-turbo
   - Purpose: Generate NoQL queries

4. **`chart_validator_prompt`** (Prompt Template, Line 2054)
   - Used in: `validate_chart_necessity()`
   - Model: gpt-4o-mini
   - Purpose: Validate chart necessity

5. **`noql_prompt`** (Prompt Template, Line 2799)
   - Used in: Chart generation (backup/alternative)
   - Model: gpt-3.5-turbo
   - Purpose: Generate NoQL queries

---

## 📝 NOTES

- **SQLite Functions**: Helper functions for conversation management are always available via `_ensure_sqlite()` initialization
- **Error Handling**: Most functions have try/except blocks that call `create_error_response()` on failure
- **Caching**: Schema is hardcoded (no caching needed), but counts and samples are fetched fresh each time
- **Parallel Execution**: Some calls could be parallelized (e.g., fetching counts + samples simultaneously)

