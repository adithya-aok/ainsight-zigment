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

import warnings
from sqlalchemy import exc as sa_exc

# Suppress SQLAlchemy warnings for unrecognized column types (spatial, binary, etc.)
warnings.filterwarnings('ignore', category=sa_exc.SAWarning, message='.*Did not recognize type.*')

from ChatOpenAI import ChatOpenAI, ChatPromptTemplate, StrOutputParser, RunnablePassthrough

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
    "zigment-x-api-key": os.getenv("ZIGMENT_API_KEY", "")
}

# Pre-defined schema for zigment database
ZIGMENT_SCHEMA = {
    "contacts": {
        "columns": [
            "_id", "full_name", "email", "phone", "status", "contact_stage", 
            "created_at_timestamp", "updated_at_timestamp", "is_deleted", 
            "source", "tags", "notes", "company", "position"
        ],
        "description": "Main contacts/leads table with personal and business information"
    },
    "events": {
        "columns": [
            "_id", "event_type", "timestamp", "contact_id", "agent_id", 
            "event_data", "created_at", "updated_at"
        ],
        "description": "Event tracking table for contact interactions and activities"
    },
    "chathistories": {
        "columns": [
            "_id", "contact_id", "agent_id", "message", "timestamp", 
            "message_type", "created_at", "updated_at"
        ],
        "description": "Chat history and conversation tracking"
    },
    "corecontacts": {
        "columns": [
            "_id", "contact_id", "core_data", "created_at_timestamp", 
            "updated_at_timestamp", "status"
        ],
        "description": "Core contact data and extended information"
    },
    "contacttags": {
        "columns": [
            "_id", "contact_id", "tag_name", "tag_value", "created_at", 
            "updated_at"
        ],
        "description": "Contact tagging and categorization system"
    },
    "orgagent": {
        "columns": [
            "_id", "agent_name", "agent_type", "status", "created_at", 
            "updated_at", "permissions"
        ],
        "description": "Organization agents and user management"
    },
    "organization": {
        "columns": [
            "_id", "org_name", "org_type", "created_at", "updated_at", 
            "settings", "status"
        ],
        "description": "Organization information and settings"
    }
}

# SQL validation function
def validate_sql_query(query):
    """
    Validate NoQL query for forbidden functions and syntax
    """
    forbidden_functions = [
        'UNIX_TIMESTAMP', 'DATE_SUB', 'CURRENT_DATE', 'NOW', 'INTERVAL',
        'HOUR(', 'MINUTE(', 'SECOND(', 'DAYOFWEEK', 'DATE_FORMAT',
        'FROM_UNIXTIME', 'DATE_ADD', 'DATE_SUBTRACT'
    ]
    
    query_upper = query.upper()
    
    for func in forbidden_functions:
        if func in query_upper:
            return False, f"Forbidden function detected: {func}"
    
    # Check for proper timestamp conversion
    if 'TO_DATE(' in query_upper and '* 1000' not in query:
        return False, "Timestamp fields must be converted with TO_DATE(field * 1000)"
    
    # Check for boolean values (should use false/true, not 0/1)
    if '= 0' in query and 'is_deleted' in query_upper:
        return False, "Use false instead of 0 for boolean values"
    
    return True, "Query is valid"

# Enhanced SQL execution function with validation
def execute_sql_query(query, database_name="zigment"):
    """
    Execute SQL query against the zigment database via API with validation
    """
    try:
        print(f"🔍 Executing SQL: {query}")
        
        # Validate query first
        is_valid, error_msg = validate_sql_query(query)
        if not is_valid:
            print(f"❌ Query validation failed: {error_msg}")
            return {
                'success': False,
                'error': f'Query validation failed: {error_msg}',
                'query': query
            }
        
        # Prepare the API request
        api_url = f"{API_BASE_URL}/reporting/preview"
        payload = {
            "sqlText": query,
            "type": "table"
        }
        
        # Make API request
        response = requests.post(api_url, headers=API_HEADERS, json=payload, timeout=30)
        
        if response.status_code in [200, 201]:  # Accept both 200 and 201
            result = response.json()
            print(f"🔍 API Response: {result}")  # Debug: show full API response
            
            if result.get('success'):
                # Handle different data formats from Zigment API
                data = result.get('data', {})
                if isinstance(data, dict) and 'rows' in data:
                    # Convert rows to list format for compatibility
                    rows = data.get('rows', [])
                    converted_data = []
                    for row in rows:
                        if isinstance(row, dict):
                            # Convert dict to list of values
                            converted_data.append(list(row.values()))
                        else:
                            converted_data.append(row)
                    data = converted_data
                elif isinstance(data, list):
                    # Data is already in list format
                    pass
                else:
                    data = []
                
                metadata = result.get('metadata', {})
                print(f"✅ Query executed successfully: {len(data)} rows returned")
                return {
                    'success': True,
                    'data': data,
                    'metadata': metadata,
                    'query': query
                }
            else:
                errors = result.get('errors', ['Unknown error'])
                print(f"❌ Query failed: {errors}")
                print(f"🔍 Full API response: {result}")  # Debug: show full response
                return {
                    'success': False,
                    'error': errors[0] if errors else 'Unknown error',
                    'query': query
                }
        else:
            print(f"❌ API request failed: {response.status_code}")
            return {
                'success': False,
                'error': f'API request failed with status {response.status_code}',
                'query': query
            }
            
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        return {
            'success': False,
            'error': str(e),
            'query': query
        }

# Flask app initialization
app = Flask(__name__)

# Enable CORS for frontend
origins_env = os.getenv("CORS_ORIGINS")
if origins_env:
    origins = [origin.strip() for origin in origins_env.split(',')]
else:
    origins = ["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000", "http://127.0.0.1:3001"]

CORS(
    app,
    origins=origins,
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"]
)

# Chat history storage
def get_chat_history(conversation_id):
    """Get chat history from SQLite database"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT role, content, timestamp 
            FROM chat_history 
            WHERE conversation_id = ? 
            ORDER BY timestamp ASC
        ''', (conversation_id,))
        
        history = []
        for row in cursor.fetchall():
            history.append({
                'role': row[0],
                'content': row[1],
                'timestamp': row[2]
            })
        
        conn.close()
        return history
    except Exception as e:
        print(f"Error getting chat history: {e}")
        return []

def save_chat_message(conversation_id, role, content):
    """Save chat message to SQLite database"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO chat_history (conversation_id, role, content, timestamp)
            VALUES (?, ?, ?, ?)
        ''', (conversation_id, role, content, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving chat message: {e}")

# Initialize chat history table
def init_chat_history():
    """Initialize chat history table if it doesn't exist"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing chat history: {e}")

# Initialize on startup
init_chat_history()

# Convert SQL data to frontend chart format
def convert_data_to_chart_format(data, chart_config):
    """
    Convert SQL result data to frontend chart format
    """
    try:
        chart_data = []
        
        for row in data:
            if len(row) >= 2:
                chart_data.append({
                    "label": str(row[0]),
                    "value": float(row[1]) if isinstance(row[1], (int, float)) else 0
                })
        
        # Create chart data in the format expected by frontend
        chart_id = f"chart_{uuid.uuid4().hex[:8]}"
        frontend_chart_data = {
            "id": chart_id,
            "title": chart_config.get('title', 'Chart'),
            "x_axis": "Category",
            "y_axis": "Value", 
            "chart_type": chart_config.get('type', 'bar'),
            "data": chart_data
        }
        
        return frontend_chart_data
        
    except Exception as e:
        print(f"❌ Error converting data to chart format: {e}")
        return None

# Chart generation and storage
def generate_chart(chart_config, data, metadata=None):
    """
    Generate chart image and return chart ID
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime
        import os
        
        # Create charts directory if it doesn't exist
        charts_dir = "charts"
        if not os.path.exists(charts_dir):
            os.makedirs(charts_dir)
        
        # Generate unique chart ID
        chart_id = f"chart_{uuid.uuid4().hex[:8]}"
        
        # Extract chart configuration
        chart_type = chart_config.get('type', 'bar')
        title = chart_config.get('title', 'Chart')
        
        # Create figure
        plt.figure(figsize=(12, 8))
        
        if chart_type == 'bar':
            if data and len(data) > 0:
                # Extract labels and values
                labels = []
                values = []
                
                for row in data:
                    if len(row) >= 2:
                        labels.append(str(row[0]))
                        values.append(float(row[1]) if isinstance(row[1], (int, float)) else 0)
                
                if labels and values:
                    plt.bar(labels, values)
                    plt.title(title)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
        
        elif chart_type == 'line':
            if data and len(data) > 0:
                labels = []
                values = []
                
                for row in data:
                    if len(row) >= 2:
                        labels.append(str(row[0]))
                        values.append(float(row[1]) if isinstance(row[1], (int, float)) else 0)
                
                if labels and values:
                    plt.plot(labels, values, marker='o')
                    plt.title(title)
                    plt.xticks(rotation=45)
                    plt.tight_layout()
        
        elif chart_type == 'pie':
            if data and len(data) > 0:
                labels = []
                values = []
                
                for row in data:
                    if len(row) >= 2:
                        labels.append(str(row[0]))
                        values.append(float(row[1]) if isinstance(row[1], (int, float)) else 0)
                
                if labels and values:
                    plt.pie(values, labels=labels, autopct='%1.1f%%')
                    plt.title(title)
        
        # Save chart
        chart_filename = f"{chart_id}_{chart_type}.png"
        chart_path = os.path.join(charts_dir, chart_filename)
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Chart generated: {chart_id}")
        return chart_id, chart_path
        
    except Exception as e:
        print(f"❌ Error generating chart: {e}")
        return None, None

def extract_charts_from_markdown(markdown_content):
    """
    Extract chart blocks from markdown and generate charts
    """
    import re
    
    chart_blocks = re.findall(r'```chart\s*\n(.*?)\n```', markdown_content, re.DOTALL)
    chart_placeholders = []
    
    for block in chart_blocks:
        try:
            # Parse chart configuration
            chart_config = json.loads(block.strip())
            
            # Generate SQL query for chart data
            question = chart_config.get('question', '')
            if question:
                sql_query = generate_sql_from_question(question)
                result = execute_sql_query(sql_query)
                
                if result['success'] and result['data']:
                    # Generate chart
                    chart_id, chart_path = generate_chart(chart_config, result['data'])
                    
                    if chart_id:
                        chart_placeholders.append({
                            'id': chart_id,
                            'config': chart_config,
                            'data': result['data'],
                            'sql': sql_query
                        })
                        
                        # Replace chart block with placeholder
                        markdown_content = markdown_content.replace(
                            f'```chart\n{block}\n```',
                            f'{{{{chart:{chart_id}}}}}'
                        )
        
        except Exception as e:
            print(f"❌ Error processing chart block: {e}")
            continue
    
    return markdown_content, chart_placeholders

def get_chart_data(chart_id):
    """
    Get chart data by ID
    """
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT chart_config, chart_data FROM chart_history 
            WHERE chart_id = ?
        ''', (chart_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'config': json.loads(row[0]),
                'data': json.loads(row[1])
            }
        return None
        
    except Exception as e:
        print(f"Error getting chart data: {e}")
        return None

def save_chart_history(chart_id, chart_config, chart_data):
    """
    Save chart to history
    """
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO chart_history (chart_id, chart_config, chart_data, created_at)
            VALUES (?, ?, ?, ?)
        ''', (chart_id, json.dumps(chart_config), json.dumps(chart_data), datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error saving chart history: {e}")

# Initialize chart history table
def init_chart_history():
    """Initialize chart history table if it doesn't exist"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chart_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chart_id TEXT UNIQUE NOT NULL,
                chart_config TEXT NOT NULL,
                chart_data TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing chart history: {e}")

# Initialize chart history on startup
init_chart_history()

# Chart helper functions
def should_generate_chart(question, data):
    """
    Determine if a chart should be generated based on question and data
    """
    question_lower = question.lower()
    chart_keywords = ['chart', 'graph', 'plot', 'visualize', 'show', 'display', 'trend', 'distribution', 'count', 'monthly', 'weekly', 'daily']
    
    # Check if question contains chart keywords
    has_chart_keywords = any(keyword in question_lower for keyword in chart_keywords)
    
    # Check if data is suitable for charting (has numeric values)
    has_numeric_data = False
    if data and len(data) > 0:
        for row in data:
            if len(row) >= 2 and isinstance(row[1], (int, float)):
                has_numeric_data = True
                break
    
    return has_chart_keywords and has_numeric_data and len(data) > 1

def create_chart_config(question, data):
    """
    Create chart configuration based on question and data
    """
    question_lower = question.lower()
    
    # Determine chart type
    if 'pie' in question_lower or 'percentage' in question_lower:
        chart_type = 'pie'
    elif 'line' in question_lower or 'trend' in question_lower or 'over time' in question_lower:
        chart_type = 'line'
    else:
        chart_type = 'bar'  # Default to bar chart
    
    # Create title
    title = question.replace('?', '').replace('show', '').replace('display', '').strip()
    if not title:
        title = f"Data Visualization"
    
    # Create chart configuration
    chart_config = {
        "type": chart_type,
        "title": title,
        "question": question,
        "db": "zigment"
    }
    
    return chart_config

# Routes
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "message": "Appy backend is running"})

@app.route('/api/ask', methods=['POST'])
def ask_question():
    """
    Main endpoint for asking questions and getting SQL results with chart support
    """
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        conversation_id = data.get('conversation_id', str(uuid.uuid4()))
        
        if not question:
            return jsonify({"error": "No question provided"}), 400
        
        print(f"🤖 Processing question: {question}")
        
        # Get chat history
        history = get_chat_history(conversation_id)
        
        # Run deep exploration first
        exploration_result = run_deep_exploration(question, "zigment")
        
        # Generate SQL query based on question and exploration
        sql_query = generate_sql_from_question(question)
        
        # Execute the SQL query
        result = execute_sql_query(sql_query)
        
        # Save user question
        save_chat_message(conversation_id, 'user', question)
        
        # Generate response with chart support
        if result['success']:
            response_content = f"Query executed successfully:\n\n```sql\n{sql_query}\n```\n\nResults: {len(result['data'])} rows returned"
            
            # Check if this should be a chart
            print(f"🔍 Checking if should generate chart for question: {question}")
            print(f"🔍 Data length: {len(result['data']) if result['data'] else 0}")
            
            if should_generate_chart(question, result['data']):
                print(f"🔍 Should generate chart - creating chart config")
                chart_config = create_chart_config(question, result['data'])
                if chart_config:
                    print(f"🔍 Chart config created: {chart_config}")
                    # Convert data to frontend format
                    frontend_chart_data = convert_data_to_chart_format(result['data'], chart_config)
                    if frontend_chart_data:
                        print(f"🔍 Frontend chart data created: {frontend_chart_data}")
                        chart_id, chart_path = generate_chart(chart_config, result['data'])
                        if chart_id:
                            save_chart_history(chart_id, chart_config, result['data'])
                            response_content += f"\n\n```chart\n{json.dumps(frontend_chart_data)}\n```"
                    else:
                        print(f"🔍 Failed to create frontend chart data")
                else:
                    print(f"🔍 Failed to create chart config")
            else:
                print(f"🔍 Should not generate chart")
            
            if result['data']:
                response_content += f"\n\nFirst few rows:\n{json.dumps(result['data'][:5], indent=2)}"
        else:
            response_content = f"Query failed: {result['error']}\n\n```sql\n{sql_query}\n```"
        
        save_chat_message(conversation_id, 'assistant', response_content)
        
        # Prepare response with chart data
        response_data = {
            "response": response_content,
            "conversation_id": conversation_id,
            "sql_query": sql_query,
            "success": result['success'],
            "data": result.get('data', []),
            "error": result.get('error')
        }
        
        # Add chart data if available
        if 'frontend_chart_data' in locals() and frontend_chart_data:
            response_data["charts"] = [frontend_chart_data]
            print(f"🔍 Adding chart to response: {frontend_chart_data}")
        else:
            print(f"🔍 No chart data available - frontend_chart_data in locals: {'frontend_chart_data' in locals()}")
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"❌ Error in ask_question: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/execute-sql', methods=['POST'])
def execute_sql():
    """
    Direct SQL execution endpoint
    """
    try:
        data = request.get_json()
        sql_query = data.get('sql', '').strip()
        
        if not sql_query:
            return jsonify({"error": "No SQL query provided"}), 400
        
        print(f"🔍 Executing direct SQL: {sql_query}")
        
        # Execute the SQL query
        result = execute_sql_query(sql_query)
        
        return jsonify({
            "success": result['success'],
            "data": result.get('data', []),
            "metadata": result.get('metadata', {}),
            "error": result.get('error'),
            "query": sql_query
        })
        
    except Exception as e:
        print(f"❌ Error in execute_sql: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/schema', methods=['GET'])
def get_schema():
    """
    Get the pre-defined schema
    """
    return jsonify({
        "database": "zigment",
        "schema": ZIGMENT_SCHEMA,
        "description": "Pre-defined schema for zigment database"
    })

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """
    Get available databases (for frontend compatibility)
    """
    return jsonify({
        "databases": [
            {
                "name": "zigment",
                "description": "Main zigment database",
                "status": "active"
            }
        ]
    })

@app.route('/api/conversations', methods=['GET'])
def get_conversations():
    """Get list of conversations"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT conversation_id, MAX(timestamp) as last_activity
            FROM chat_history 
            GROUP BY conversation_id 
            ORDER BY last_activity DESC
        ''')
        
        conversations = []
        for row in cursor.fetchall():
            conversations.append({
                'id': row[0],
                'title': f"Conversation {row[0][:8]}",  # Generate a title
                'created_at': row[1],
                'updated_at': row[1]
            })
        
        conn.close()
        return jsonify({
            "success": True,
            "conversations": conversations
        })
        
    except Exception as e:
        print(f"Error getting conversations: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/conversations', methods=['POST'])
def create_conversation():
    """Create a new conversation"""
    try:
        data = request.get_json()
        title = data.get('title', 'New conversation')
        
        # Generate new conversation ID
        conversation_id = str(uuid.uuid4())
        
        return jsonify({
            "success": True,
            "conversation": {
                "id": conversation_id,
                "title": title,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        print(f"Error creating conversation: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    """Get chat history for a conversation"""
    try:
        conversation_id = request.args.get('conversation_id')
        if not conversation_id:
            return jsonify({"error": "conversation_id required"}), 400
        
        history = get_chat_history(conversation_id)
        return jsonify({"history": history})
        
    except Exception as e:
        print(f"Error getting history: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/conversations/<cid>', methods=['DELETE'])
def delete_conversation(cid):
    """Delete a conversation"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM chat_history WHERE conversation_id = ?', (cid,))
        conn.commit()
        conn.close()
        
        return jsonify({"message": "Conversation deleted successfully"})
        
    except Exception as e:
        print(f"Error deleting conversation: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/charts/<chart_id>', methods=['GET'])
def get_chart(chart_id):
    """Get chart data by ID"""
    try:
        chart_data = get_chart_data(chart_id)
        if chart_data:
            return jsonify({
                "success": True,
                "chart_id": chart_id,
                "config": chart_data['config'],
                "data": chart_data['data']
            })
        else:
            return jsonify({"error": "Chart not found"}), 404
            
    except Exception as e:
        print(f"Error getting chart: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/charts', methods=['GET'])
def get_all_charts():
    """Get all charts"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT chart_id, chart_config, created_at 
            FROM chart_history 
            ORDER BY created_at DESC
        ''')
        
        charts = []
        for row in cursor.fetchall():
            charts.append({
                'id': row[0],
                'config': json.loads(row[1]),
                'created_at': row[2]
            })
        
        conn.close()
        return jsonify({"charts": charts})
        
    except Exception as e:
        print(f"Error getting charts: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/charts/<chart_id>', methods=['DELETE'])
def delete_chart(chart_id):
    """Delete a chart"""
    try:
        conn = sqlite3.connect('chat_history.sqlite3')
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM chart_history WHERE chart_id = ?', (chart_id,))
        conn.commit()
        conn.close()
        
        return jsonify({"message": "Chart deleted successfully"})
        
    except Exception as e:
        print(f"Error deleting chart: {e}")
        return jsonify({"error": str(e)}), 500

# Deep exploration functionality (like app.py)
def run_deep_exploration(question: str, database_name: str = "zigment", max_queries: int = 3):
    """
    Run deep exploration to understand the question before generating final query
    """
    try:
        print(f"🔎 === DEEP EXPLORATION ({database_name}) — intelligent probing ===")
        
        # Create exploration prompt
        exploration_prompt = ChatPromptTemplate.from_template(
            """
🚨🚨🚨 **CRITICAL: YOU MUST GENERATE NoQL QUERIES, NOT SQL!** 🚨🚨🚨

You are an expert NoQL query generator for deep data exploration.
Generate 1-3 exploratory NoQL queries to understand the user's question before writing the final analysis.

**AVAILABLE TABLES AND COLUMNS:**
{schema}

**USER QUESTION:** {question}

**NoQL SYNTAX RULES:**
1. Use ONLY the tables and columns shown above
2. For timestamp fields, convert with TO_DATE(field * 1000)
3. For boolean fields, use 0/1 instead of true/false
4. Always include LIMIT for large result sets
5. Use proper NoQL syntax - no MySQL functions

**EXPLORATION EXAMPLES:**
- Count exploration: SELECT COUNT(*) FROM contacts WHERE is_deleted = 0
- Stage exploration: SELECT contact_stage, COUNT(*) FROM contacts WHERE is_deleted = 0 GROUP BY contact_stage
- Event exploration: SELECT contact_id, COUNT(*) FROM events GROUP BY contact_id ORDER BY COUNT(*) DESC LIMIT 10

Return JSON format:
{{"explorations": [{{"purpose": "description", "sql": "SELECT ..."}}]}}

Generate ONLY the JSON, no explanations.
            """
        )
        
        # Create schema string
        schema_str = "ZIGMENT DATABASE SCHEMA:\n"
        for table_name, table_info in ZIGMENT_SCHEMA.items():
            schema_str += f"\n{table_name}:\n"
            schema_str += f"  Description: {table_info['description']}\n"
            schema_str += f"  Columns: {', '.join(table_info['columns'])}\n"
        
        # Generate exploration queries
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        chain = exploration_prompt | llm | StrOutputParser()
        
        exploration_response = chain.invoke({
            "question": question,
            "schema": schema_str
        })
        
        # Parse exploration response
        try:
            exploration_data = json.loads(exploration_response)
            explorations = exploration_data.get('explorations', [])
        except:
            print("❌ Failed to parse exploration response")
            return {"facts": [], "allowed_entities": []}
        
        facts = []
        allowed_entities = []
        
        # Execute exploration queries
        for i, exploration in enumerate(explorations[:max_queries]):
            try:
                purpose = exploration.get('purpose', f'Exploration {i+1}')
                sql_query = exploration.get('sql', '')
                
                print(f"🔍 Exploration {i+1}: {purpose}")
                print(f"   SQL: {sql_query}")
                
                # Execute exploration query
                result = execute_sql_query(sql_query)
                
                if result['success'] and result['data']:
                    facts.append(f"Exploration {i+1}: {purpose} - Found {len(result['data'])} results")
                    allowed_entities.extend([str(item) for row in result['data'][:5] for item in row])
                    print(f"   ✅ Found {len(result['data'])} results")
                else:
                    print(f"   ❌ Exploration failed: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                print(f"❌ Error in exploration {i+1}: {e}")
                continue
        
        print(f"🔎 Deep exploration complete. Facts: {len(facts)} | Allowed entities: {len(set(allowed_entities))}")
        return {"facts": facts, "allowed_entities": list(set(allowed_entities))}
        
    except Exception as e:
        print(f"❌ Error in deep exploration: {e}")
        return {"facts": [], "allowed_entities": []}

# Enhanced SQL generation with LLM and guardrails
def create_noql_chain():
    """Create NoQL generation chain with proper prompts"""
    prompt = ChatPromptTemplate.from_template(
        """
🚨🚨🚨 **CRITICAL: YOU MUST GENERATE NoQL QUERIES ONLY!** 🚨🚨🚨

❌ **ABSOLUTELY FORBIDDEN - THESE DO NOT EXIST IN NoQL:**
- UNIX_TIMESTAMP(), DATE_SUB(), CURRENT_DATE(), NOW(), INTERVAL
- HOUR(), MINUTE(), SECOND(), DAYOFWEEK(), DATE_FORMAT()
- FROM_UNIXTIME(), DATE_ADD(), DATE_SUBTRACT()

✅ **ONLY USE THESE FOR DATE FILTERING:**
- WHERE timestamp >= 1704067200 (numeric Unix timestamp)
- WHERE created_at_timestamp >= 1710000000 (numeric Unix timestamp)

🚨 **IF YOU USE ANY FORBIDDEN FUNCTION, THE QUERY WILL FAIL!**

You are an expert NoQL query generator for the zigment database.
Generate a valid NoQL query that answers the user's question.

**AVAILABLE TABLES AND COLUMNS:**
{schema}

**USER QUESTION:** {question}

**NoQL SYNTAX RULES:**
1. Use ONLY the tables and columns shown above
2. For timestamp fields, convert with TO_DATE(field * 1000)
3. For boolean fields, use false/true (not 0/1)
4. Always include LIMIT for large result sets
5. Use GROUP BY and ORDER BY appropriately

**NoQL EXAMPLES:**
- Count contacts: SELECT COUNT(*) FROM contacts
- Count non-deleted contacts: SELECT COUNT(*) FROM contacts WHERE is_deleted = false
- Contact stages: SELECT contact_stage, COUNT(*) AS contact_count FROM contacts GROUP BY contact_stage ORDER BY contact_stage
- Events by contact: SELECT contact_id, COUNT(*) AS event_count FROM events GROUP BY contact_id ORDER BY event_count DESC LIMIT 10

**CRITICAL: Generate ONLY the NoQL query, no explanations or markdown formatting.**

**MANDATORY PATTERN FOR CONTACT STAGES:**
For questions about contact stages, use EXACTLY this pattern:
SELECT contact_stage, COUNT(*) AS contact_count FROM contacts WHERE is_deleted = false GROUP BY contact_stage ORDER BY contact_stage

**MANDATORY PATTERN FOR EVENTS BY CONTACT:**
For questions about events by contact, use EXACTLY this pattern:
SELECT contact_id, COUNT(*) AS event_count FROM events GROUP BY contact_id ORDER BY event_count DESC LIMIT 10

**MANDATORY PATTERN FOR CONTACT COUNTS:**
For questions about contact counts, use EXACTLY this pattern:
SELECT COUNT(*) AS total_contacts FROM contacts WHERE is_deleted = false

**NEVER DEVIATE FROM THESE PATTERNS!**
        """
    )
    
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    chain = prompt | llm | StrOutputParser()
    return chain

def generate_sql_from_question(question):
    """
    Generate SQL using LLM with proper guardrails
    """
    try:
        # Create schema string
        schema_str = "ZIGMENT DATABASE SCHEMA:\n"
        for table_name, table_info in ZIGMENT_SCHEMA.items():
            schema_str += f"\n{table_name}:\n"
            schema_str += f"  Description: {table_info['description']}\n"
            schema_str += f"  Columns: {', '.join(table_info['columns'])}\n"
        
        # Generate NoQL using LLM
        chain = create_noql_chain()
        sql_query = chain.invoke({
            "question": question,
            "schema": schema_str
        })
        
        # Clean up the response
        sql_query = sql_query.strip()
        if sql_query.startswith('```sql'):
            sql_query = sql_query[6:]
        if sql_query.endswith('```'):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()
        
        print(f"🤖 Generated SQL: {sql_query}")
        return sql_query
        
    except Exception as e:
        print(f"❌ Error generating SQL: {e}")
        # Fallback to simple pattern matching
        return generate_sql_fallback(question)

def generate_sql_fallback(question):
    """
    Fallback NoQL generation with simple pattern matching
    """
    question_lower = question.lower()
    
    # Simple pattern matching for common queries
    if "count" in question_lower and "contact" in question_lower:
        if "deleted" in question_lower:
            return "SELECT COUNT(*) as total_leads FROM contacts WHERE is_deleted = false"
        else:
            return "SELECT COUNT(*) as total_contacts FROM contacts"
    
    elif "contacts" in question_lower and "status" in question_lower:
        return "SELECT status, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY status ORDER BY count DESC"
    
    elif "contacts" in question_lower and "stage" in question_lower:
        return "SELECT contact_stage, COUNT(*) as contact_count FROM contacts WHERE is_deleted = false GROUP BY contact_stage ORDER BY contact_stage"
    
    elif "contacts" in question_lower and "month" in question_lower:
        return """SELECT 
            CASE MONTH(TO_DATE(created_at_timestamp * 1000))
                WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
                WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
                WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
                WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
            END AS month,
            COUNT(*) AS count
        FROM contacts
        WHERE is_deleted = false
        GROUP BY MONTH(TO_DATE(created_at_timestamp * 1000))
        ORDER BY MONTH(TO_DATE(created_at_timestamp * 1000))"""
    
    elif "events" in question_lower and "contact" in question_lower:
        return "SELECT contact_id, COUNT(*) AS event_count FROM events GROUP BY contact_id ORDER BY event_count DESC LIMIT 10"
    
    elif "recent" in question_lower or "last" in question_lower:
        return "SELECT * FROM contacts WHERE is_deleted = false ORDER BY created_at_timestamp DESC LIMIT 10"
    
    elif "all" in question_lower and "contact" in question_lower:
        return "SELECT * FROM contacts WHERE is_deleted = false LIMIT 20"
    
    # Default fallback - try simplest query first
    return "SELECT COUNT(*) FROM contacts"

if __name__ == '__main__':
    host = os.getenv('HOST', '127.0.0.1')
    port = int(os.getenv('PORT', 5001))  # Different port to avoid conflict
    
    print(f"🚀 Starting Appy backend on {host}:{port}")
    print(f"🔐 CORS origins: {origins}")
    app.run(debug=True, host=host, port=port)
