# Database Analytics Backend

This Flask backend converts natural language questions into SQL queries using OpenAI GPT-4, then executes them against a MySQL database. Features a custom LLM implementation for optimal performance.

## Features

- Natural language to SQL conversion using OpenAI GPT-4
- MySQL database integration with Chinook sample database
- RESTful API endpoints for frontend integration
- Error handling and validation
- CORS support for cross-origin requests

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Environment Variables**
   Create a `.env` file in the backend directory:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ```

3. **Database Setup**
   Make sure your MySQL server is running with the Chinook database.
   Update the connection string in `app.py` if needed:
   ```python
   mysql_uri = "mysql+pymysql://username:password@localhost:3306/chinook"
   ```

4. **Run the Server**
   ```bash
   python app.py
   ```
   The server will run on `http://localhost:5000`

## API Endpoints

### `GET /health`
Health check endpoint to verify the server is running.

### `POST /api/ask`
Process natural language questions and return chart data.

**Request Body:**
```json
{
  "question": "Show me the top 5 most popular albums"
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "title": "Top 5 Most Popular Albums",
    "x_axis": "Albums",
    "y_axis": "Popularity",
    "data": [
      {"label": "Album 1", "value": 150},
      {"label": "Album 2", "value": 130}
    ]
  },
  "question": "Show me the top 5 most popular albums"
}
```

### `POST /api/execute-sql`
Execute raw SQL queries (for debugging purposes).

### `GET /api/schema`
Get database schema information.

## Error Handling

The API returns appropriate HTTP status codes and error messages for various scenarios:
- 400: Bad Request (missing required parameters)
- 500: Internal Server Error (database or processing errors)

## Dependencies

- Flask: Web framework
- OpenAI: Language model for SQL generation
- PyMySQL: MySQL database connector
- Flask-CORS: Cross-origin resource sharing
- Custom LLM Implementation: Optimized for performance

