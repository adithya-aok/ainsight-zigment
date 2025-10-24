import sys
import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()  
from ChatOpenAI import ChatOpenAI

# API configuration
API_BASE_URL = "https://staging-api.zigment.ai"
API_HEADERS = {
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "User-Agent": "PostmanRuntime/7.48.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "x-org-id": "64dce1d5e1bed3c6117f78ae",
    "zigment-x-api-key": "sk_7835f5a00766f965476190821229b452"
}

def simplify_schema_for_noql(full_schema: dict) -> dict:
    """Extract only query-relevant information from the full schema.
    
    Removes UI metadata, display settings, and other non-essential info
    to make the schema compact and focused for NoQL generation.
    """
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
            
            # Detect Unix timestamp fields - aggressive detection
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


def fetch_schema_from_api() -> str:
    """Fetch schema from API and simplify for NoQL query generation."""
    url = f"{API_BASE_URL}/schemas/schemaForAllowedCollections"
    
    try:
        response = requests.get(url, headers=API_HEADERS, timeout=10)
        response.raise_for_status()
        
        full_data = response.json()
        simplified = simplify_schema_for_noql(full_data)
        serialized = json.dumps(simplified, indent=2)
        return serialized
    except requests.exceptions.RequestException as e:
        print(f"Error fetching schema from API: {e}")
        raise

def get_noql_syntax_rules() -> str:
    """Return comprehensive NoQL syntax rules and examples as a single prompt."""
    return """
# NoQL Syntax Reference

NoQL is a SQL-to-MongoDB interpreter combining SQL syntax with MongoDB's document model and aggregation pipeline.

## Core Rules

1. **Field Names**: Case-sensitive, cannot contain `.` or start with `$`
2. **Values**: Case-sensitive and type-exact
3. **Aliasing**: REQUIRED for functions and sub-queries using `AS`
4. **Dot Notation**: Use for nested field traversal (e.g., `Address.City`)
5. **Backticks**: Use for field/table names with spaces or special chars

## Basic Query Structure

```sql
SELECT field1, field2 AS alias
FROM `collection`
WHERE condition
GROUP BY field
ORDER BY field ASC|DESC
LIMIT n OFFSET m
```

## WHERE Clause Rules

- Functions in WHERE must be explicit (can't use computed field aliases)
- Correct: `WHERE ABS(id) > 1`
- Incorrect: `WHERE aliasedField > 1` (unless in aggregates)

## Comparison Operators

- Standard: `>`, `<`, `=`, `>=`, `<=`, `!=`
- Null checks: `IS NULL`, `IS NOT NULL`
- String: `LIKE 'pattern%'` (case-insensitive, supports %)
- List: `IN (value1, value2)`, `NOT IN (...)`
- Functions: `GT()`, `LT()`, `EQ()`, `GTE()`, `LTE()`, `NE()`
- Case: `CASE WHEN condition THEN value ELSE value END`

## Aggregate Functions

- `SUM(field)` - Sum values
- `AVG(field)` - Average values
- `COUNT(*)` or `COUNT(field)` - Count rows
- `COUNT(DISTINCT field)` - Distinct count
- `MIN(field)`, `MAX(field)` - Min/max values
- `FIRSTN(n)` - First n records as array
- `LASTN(n)` - Last n records as array

Example with GROUP BY:
```sql
SELECT SUM(amount) AS total, city
FROM customers
GROUP BY city
```

## Array Operations

### Sub-selects for Arrays
```sql
SELECT (SELECT * FROM Rentals WHERE staffId=2) AS rentals
FROM customers
```

### Array Functions
- `UNWIND(array)` - Unwind array to multiple docs
- `SIZE_OF_ARRAY(array)` - Array length
- `FIRST_IN_ARRAY(array)`, `LAST_IN_ARRAY(array)` - First/last element
- `ARRAY_ELEM_AT(array, pos)` - Element at position
- `SUM_ARRAY(array, 'field')` - Sum field in array
- `AVG_ARRAY(array, 'field')` - Average field in array
- `CONCAT_ARRAYS(arr1, arr2)` - Concatenate arrays
- `REVERSE_ARRAY(array)` - Reverse array
- `JOIN(array, delimiter)` - Join array to string

### $$ROOT in Arrays
```sql
SELECT (SELECT filmId AS `$$ROOT` FROM Rentals) AS filmIds
FROM customers
```

## Object Operations

### $$ROOT for Result
```sql
SELECT t AS `$$ROOT`
FROM (SELECT id, name FROM customers) AS t
```

### Object Functions
- `MERGE_OBJECTS(obj1, obj2)` - Merge objects
- `PARSE_JSON('{"key":"val"}')` - Parse JSON string
- `EMPTY_OBJECT()` - Create empty object
- `OBJECT_TO_ARRAY(obj)` - Convert to array
- `ARRAY_TO_OBJECT(arr)` - Convert to object
- `FLATTEN(field, 'prefix')` - Flatten nested object

### Creating Objects
```sql
SELECT (SELECT id, name AS fullName) AS person
FROM customers
```

## JOIN Operations

### Basic Joins
```sql
SELECT *
FROM orders
INNER JOIN inventory ON sku = item
```

### Join Hints (using pipe `|`)
- `|first` - Return first element as object
- `|last` - Return last element as object
- `|unwind` - Unwind to multiple records
- `|optimize` - Optimize for sub-queries

Example:
```sql
SELECT *
FROM orders
INNER JOIN inventory AS `inv|first` ON sku = item
```

## Mathematical Functions

- `ABS(n)`, `CEIL(n)`, `FLOOR(n)`, `ROUND(n, places)`
- `SQRT(n)`, `POW(base, exp)`, `EXP(n)`, `LN(n)`, `LOG(n, base)`
- `SIN(n)`, `COS(n)`, `TAN(n)`, `ASIN(n)`, `ACOS(n)`, `ATAN(n)`
- Operators: `+`, `-`, `*`, `/`, `%`

## String Functions

- `CONCAT(str1, str2, ...)` - Concatenate strings
- `SUBSTR(str, start, length)` - Substring
- `UPPER(str)`, `LOWER(str)` - Case conversion
- `TRIM(str)`, `LTRIM(str)`, `RTRIM(str)` - Trim whitespace
- `LENGTH(str)` - String length
- `INDEXOF(str, substr)` - Find substring position
- `REPLACE(str, find, replace)` - Replace substring
- `SPLIT(str, delimiter)` - Split to array

## Date/Time Handling

### Unix Timestamps (Epoch Time)
When fields store Unix timestamps (seconds since 1970-01-01), use numeric comparison:
```sql
-- For timestamp fields stored as numbers (e.g., created_at_timestamp: 1692204294)
SELECT * FROM customers 
WHERE created_at_timestamp > 1704067200  -- 2024-01-01 as Unix timestamp
```

To convert human-readable date to Unix timestamp:
- 2024-01-01 00:00:00 UTC = 1704067200
- 2024-12-31 23:59:59 UTC = 1735689599
- Use online converters or: `date -d "2024-01-01" +%s` (Linux/Mac)

### Date Functions (for actual Date objects)
- `NOW()` - Current timestamp
- `DATE_TO_STRING(date, 'format')` - Format date
- `DATE_FROM_STRING(str, 'format')` - Parse date string to date object
- `YEAR(date)`, `MONTH(date)`, `DAY(date)` - Extract parts
- `HOUR(date)`, `MINUTE(date)`, `SECOND(date)` - Time parts
- `DATE_ADD(date, amount, 'unit')` - Add duration
- `DATE_DIFF(date1, date2, 'unit')` - Difference

**Important**: Check the schema to determine if date fields are:
- Unix timestamps (NUMBER type) → use numeric comparison
- Date objects (DATETIME type) → use date functions

## Conversion Functions

- `TO_STRING(val)`, `TO_INT(val)`, `TO_DOUBLE(val)`
- `TO_BOOL(val)`, `TO_DATE(val)`
- `CONVERT(val, type)` - Generic conversion

## Window Functions (MongoDB 5.0+)

```sql
SELECT 
  name,
  amount,
  SUM(amount) OVER (PARTITION BY category ORDER BY date) AS running_total
FROM transactions
```

## UNION

```sql
SELECT id, name FROM customers
UNION
SELECT id, name FROM vendors
```

## Limits and Offsets

```sql
SELECT * FROM customers LIMIT 10 OFFSET 20
```

## Best Practices

1. Always use backticks for field names with spaces
2. Alias all functions and sub-queries with AS
3. Use dot notation for nested fields
4. Use join hints to control array results
5. Remember field names are case-sensitive
6. Use $$ROOT to promote values in sub-selects

## Common Patterns

### Filtering nested fields:
```sql
SELECT * FROM customers WHERE `Address.City` = 'Tokyo'
```

### Aggregation with nested field:
```sql
SELECT COUNT(*) AS total, `Address.Country` AS country
FROM customers
GROUP BY `Address.Country`
```

### Filtering by Unix timestamp date range:
```sql
-- Get all records created after 2024-01-01
SELECT * FROM `CONTACT` 
WHERE created_at_timestamp > 1704067200

-- Get records between two dates
SELECT * FROM `CONTACT`
WHERE created_at_timestamp >= 1704067200 
  AND created_at_timestamp < 1735689600
```

### Array filtering with sub-select:
```sql
SELECT (SELECT * FROM orders WHERE status='completed') AS completed_orders
FROM customers
```

### Join with hint:
```sql
SELECT c.*, o.total
FROM customers c
INNER JOIN orders AS `o|first` ON o.customerId = c.id
```
"""

NOQL_TASK_PROMPT = """
You are an expert NoQL (SQL-to-Document/NoSQL) query generator. STRICTLY use only the syntax, functions, operators, and rules IN THE SYNTAX BELOW and NO OTHER (do NOT use standard MongoDB, only this NoQL SQL-like syntax). Refer to the provided database schema for field and collection names.

**CRITICAL: Collection Name Mapping**
The schema uses uppercase names, but the actual MongoDB collections are lowercase. ALWAYS use these exact collection names in your queries:
- EVENT → events
- CONTACT → contacts
- CORE_CONTACT → corecontacts
- CHAT_HISTORY → chathistories
- CONTACT_TAG → contacttags

Use lowercase, remove underscores, and combine words as shown above.

**CRITICAL: Unix Timestamp Conversion**
For timestamp fields (created_at, updated_at, timestamp, created_at_timestamp):
- These are stored as Unix epoch SECONDS (not milliseconds)
- MUST convert before using date functions: TO_DATE(field * 1000)
- Example: SELECT DAY_OF_WEEK(TO_DATE(timestamp * 1000)) FROM events
- Example: SELECT DATE_TRUNC(TO_DATE(created_at * 1000), 'day') FROM contacts

NoQL syntax/rules: {noql_rules}

Schema (partial): {schema}

User Question: {question}

Output ONLY the core NoQL (SQL-like, not MongoDB-native) query that achieves the goal, no natural language, no explanation, no comments.
Use the correct lowercase collection names and timestamp conversion pattern from above.
"""

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
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        raise

def main():
    print("🔄 Fetching schema from API...")
    try:
        schema = fetch_schema_from_api()
        print("✅ Schema fetched successfully\n")
    except Exception as e:
        print(f"❌ Failed to fetch schema: {e}")
        return
    
    if len(sys.argv) > 1:
        question = ' '.join(sys.argv[1:])
    else:
        question = input('Enter your question: ')
    
    print(f"\n💭 Question: {question}")
    print("\n🤖 Generating NoQL query...")
    
    noql_rules = get_noql_syntax_rules()
    prompt = NOQL_TASK_PROMPT.format(noql_rules=noql_rules, schema=schema, question=question)

    llm = ChatOpenAI(model_name="gpt-3.5-turbo")
    result = llm.invoke(prompt)
    generated_query = result.text.strip()
    
    print('\n📝 Generated NoQL Query:')
    print('-' * 60)
    print(generated_query)
    print('-' * 60)
    
    # Ask user if they want to execute the query
    execute = input('\n🚀 Execute this query? (y/n): ').lower().strip()
    
    if execute == 'y':
        print("\n⏳ Executing query...")
        try:
            query_result = execute_noql_query(generated_query)
            print("\n✅ Query Results:")
            print('=' * 60)
            print(json.dumps(query_result, indent=2))
            print('=' * 60)
        except Exception as e:
            print(f"\n❌ Query execution failed: {e}")
    else:
        print("\n👍 Query not executed. You can copy and use it manually.")

if __name__ == '__main__':
    main()
