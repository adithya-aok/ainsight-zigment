AirportDB Conversational Analytics

> Ask natural-language questions about flights, airports, airlines, routes and aircraft. Get grounded, conversational answers with embedded charts — all driven directly from your AirportDB MySQL database.

Badges: Python 3.10+, Next.js 14, OpenAI GPT-4o-mini, MySQL 8

What’s inside
- ChatGPT-style responses: Rich, free‑flowing markdown prose rendered in the frontend with inline charts.
- Data grounding (no hallucinations): The backend “explores first,” collects facts, and rewrites prose against actual chart results and allowed entities.
- Frequent, relevant charts: 2–3+ charts when suitable; soft validation keeps bad charts out while keeping the flow natural.
- AirportDB‑focused SQL generation: A custom prompt with alias rules, column hints, performance guidance (early aggregation, minimal joins, UNION ALL patterns, LIMITs).
- Route and aircraft‑type intelligence: Proactive charts for origin→destination routes and specific aircraft types (e.g., Airbus A380).
- Conversation history: Persistent threads with auto‑summarization once a convo exceeds 10 messages.

Architecture (high‑level)
- Backend (Flask):
  - Endpoints: /api/ask, /api/execute-sql, /api/databases, /api/schema, /api/conversations, /api/history, /api/conversations/:id
  - LLM prompts: chat_markdown_prompt (free‑flowing markdown), sql_prompt (AirportDB only), chart_validator_prompt (soft approval), grounding_prompt (post‑chart rewrite)
  - Explore‑first pass: Runs targeted queries (routes, airlines, aircraft types, busiest cities) to assemble Facts and AllowedEntities.
  - Chart pipeline: Parses markdown for chart blocks → generates SQL → validates chart → injects grounded notes → final grounded rewrite.
  - Proactive charts: Adds complementary charts (arrivals, total activity) and route/aircraft‑type charts when implied in the question.
  - Conversation memory: SQLite (conversation/message/summary). Over 10 messages? Oldest 10 are summarized to bullets and removed; summaries feed future context.
- Frontend (Next.js + React):
  - Left sidebar with conversations (independent scrolling), new chat action, selectable threads.
  - Main pane renders ReactMarkdown prose and injects charts inline at {{chart:...}} placeholders.
  - Uses Tailwind + @tailwindcss/typography for clean reading experience.

Setup
Prerequisites
- Python 3.10+
- Node.js 18+
- MySQL 8 with AirportDB loaded
- OpenAI API key

Environment
Create backend/.env or export env vars equivalent:
```
OPENAI_API_KEY=sk-...
AIRPORTDB_URI=mysql+pymysql://user:password@localhost:3306/airportdb
```

Install & run
Backend
```
cd backend
pip install -r requirements.txt
python app.py
```

Frontend
```
cd frontend
npm install
npm run dev
```

Open
- App: http://localhost:3000
- API: http://localhost:5000
- Health: http://localhost:5000/health

Using the app
1) Start a new chat in the left sidebar. Type a question like:
- “Top 15 busiest origin airports by departing flights”
- “Which aircraft types are most commonly used?”
- “EL SALVADOR → VILSECK aircraft types”

2) Backend flow per question
- Explore first: small SQL probes build Facts and AllowedEntities.
- Generate markdown: a conversational answer with optional fenced chart blocks.
- Build charts: each block turns into a SQL query with limits tuned per type.
- Validate lightly: obviously wrong/empty charts are removed, replacement text is natural.
- Ground output: inject concise data notes under each chart and rewrite the prose against chart facts.

SQL generation (AirportDB only)
- Strict aliasing: simple a, b, c, ... aliases; always prefix columns; backtick `flight`.`from` and `flight`.`to` via table alias (a.`from`).
- Column location hints: airport_geo.city/country, airline.airlinename, airplane_type.identifier, flight.airline_id/airplane_id.
- Performance rules:
  - Aggregate early at the smallest level (e.g., per flight_id/airport_id), then roll up.
  - Minimal joins; avoid correlated subqueries if GROUP BY suffices.
  - When combining departures + arrivals, use UNION ALL into a subquery, then outer GROUP BY + SUM; ORDER BY/LIMIT only at the outer level.
  - Always LIMIT for charts (bar ~20, pie ≤6, line ~50, etc.).
- Example (pre‑aggregation by flight_id, then roll up by city):
```
SELECT ag.city, SUM(fb.bookings) AS total_bookings
FROM (
  SELECT b.flight_id, COUNT(*) AS bookings
  FROM booking AS b
  GROUP BY b.flight_id
) AS fb
JOIN flight AS f ON fb.flight_id = f.flight_id
JOIN airport_geo AS ag ON ag.airport_id = f.`from`
GROUP BY ag.city
ORDER BY total_bookings DESC
LIMIT 20;
```

Route and aircraft‑type intelligence
- Routes: Detects “CITY1 → CITY2” or “from CITY1 to CITY2”, normalizes city tokens, and renders “Aircraft Types on Route CITY1 → CITY2”.
- Aircraft types: Detects tokens (e.g., Airbus A380, Boeing 737) and ranks cities by flights for that type.
- Both paths enforce hard WHERE filters, avoid OR joins, and keep queries fast.

Conversation memory
- Stored in backend/chat_history.sqlite3
  - Tables
    - conversation(id, title, created_at, updated_at)
    - message(id, conversation_id, role, content_markdown, charts_json, sql_meta_json, created_at)
    - summary(id, conversation_id, content, created_at)
- Auto‑summarization: When a conversation exceeds 10 messages, the oldest 10 are summarized into 4–6 bullets and deleted. Latest summaries plus the last ~6 messages are injected into the prompt to maintain context without repetition.

API quick reference
- POST /api/ask { question, conversation_id? } → { markdown, charts, conversation_id }
- POST /api/execute-sql { query } → raw SQL result (debug)
- GET /api/databases → { airportdb }
- GET /api/schema → AirportDB schema text
- POST /api/conversations → create a conversation
- GET /api/conversations → list conversations
- GET /api/history?conversation_id=... → message history
- DELETE /api/conversations/:id → delete a conversation

Frontend notes
- ReactMarkdown for prose; Tailwind typography for readability.
- Inline charts appear where {{chart:...}} placeholders occur.
- Left sidebar (fixed width) has independent scrolling; main chat scrolls separately.

Performance tips
- Add indexes if needed: flight(`from`), flight(`to`), flight(airplane_id), airline(airline_id), airport_geo(airport_id).
- Prefer early aggregation and UNION ALL + outer aggregation patterns.

Troubleshooting
- SQL alias/column errors: verify alias usage and column locations per prompt hints.
- Empty charts: validator removes obviously wrong/empty results; adjust question to be more specific.
- Slow queries: ensure filters (WHERE) exist, avoid OR in JOINs, prefer pre‑aggregation.

License
MIT

AI-Powered Data Storytelling Platform

> Transform natural language questions into beautiful, narrative-driven data visualizations across multiple databases using AI.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org)
[![Next.js](https://img.shields.io/badge/Next.js-14.0-black.svg)](https://nextjs.org)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o--mini-green.svg)](https://openai.com)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg)](https://mysql.com)

## 🌟 What Makes This Special?

This isn't just another database query tool. It's an **intelligent data storytelling platform** that:

- 🧠 **Understands context** - Ask complex questions in plain English
- 📈 **Creates multiple perspectives** - Generates diverse, non-redundant visualizations
- 📝 **Tells the story** - Provides narrative explanations connecting your data
- 🎯 **Prevents information overload** - Smart data limiting (top 20 results automatically)
- 🌍 **Works with real databases** - Chinook (music), World (countries), IMDB (movies)
- 🚀 **Zero SQL knowledge required** - Natural language to insights in seconds

## 🎥 See It In Action

**Input:** *"What's the distribution of movies by genre?"*

**AI-Generated Output:**

### 📊 **Chart 1: Distribution of Movies by Genre (Bar Chart)**
Shows the overall movie count across 13 genres, with Drama leading at ~4,000 movies, followed by Comedy at ~2,400 movies.

### 🥧 **Chart 2: Proportion of Movie Genres (Top 6) (Pie Chart)**  
Visualizes the market share of the 6 most popular genres, making it easy to see Drama dominates with the largest slice, followed by Comedy, Thriller, and others.

### 📋 **Chart 3: Detailed Genre Distribution (Table)**
Comprehensive breakdown showing movie counts AND average ratings for each genre, revealing that while Drama has the most movies (4,285), genres like Romance have higher average ratings (5.987).

### 📝 **AI-Generated Narrative:**
- **Overview:** Contextual introduction about exploring movie genre distribution
- **Smart Transitions:** Explains how each chart builds on the previous one
- **Key Insights:** 
  - Drama and Comedy dominate the market
  - Top 6 genres make up majority of film production  
  - Niche genres still maintain quality with good ratings
- **Conclusion:** Ties together the story about diversity in film production

### 🎯 **What Makes This Output Special:**

✅ **Three Different Chart Types** - Each provides unique insights (overview → focus → details)
✅ **Smart Data Limiting** - Shows exactly the right amount of data (13 total genres, top 6 for pie chart)  
✅ **Diverse Perspectives** - Same data, three completely different analytical angles
✅ **Flowing Narrative** - AI explains WHY each chart matters and how they connect
✅ **Professional Presentation** - Clean, modern UI with proper chart labeling
✅ **Actionable Insights** - Not just charts, but meaningful business intelligence

**This is what "AI-powered data storytelling" looks like in practice!** 🎬

## Here is a screenshort of the full working
<img width="1920" height="3502" alt="dashboard" src="https://github.com/user-attachments/assets/ba6afade-8751-49f9-8077-47bb34602f3e" />

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+** with pip
- **Node.js 16+** with npm  
- **MySQL Server** with sample databases
- **OpenAI API Key** ([Get one here](https://platform.openai.com/api-keys))

### ⚡ 30-Second Setup

1. **Clone & Navigate**
   ```bash
<<<<<<< HEAD
   cd "langchain+zigment"
=======
   # Navigate to your project directory
   cd "natural-query-visualizer"
>>>>>>> b204e198e32288360c26bd2160c6c9bfffbebc99
   ```

2. **Environment Setup**
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Add your OpenAI API key to .env
   OPENAI_API_KEY=sk-your-key-here
   ```

3. **Backend Setup**
   ```bash
   cd backend
   pip install -r requirements.txt
   ```

4. **Frontend Setup**
   ```bash
   cd ../frontend
   npm install
   ```

### 🏃‍♂️ Launch the Platform

#### Option 1: One-Click Start (Windows)
```bash
# Start backend
start_backend.bat

# Start frontend (new terminal)
start_frontend.bat
```

#### Option 2: Manual Start
```bash
# Terminal 1: Backend
cd backend && python app.py

# Terminal 2: Frontend
cd frontend && npm run dev
```

### 🌐 Access Your Platform

- **🎨 Main App:** http://localhost:3000
- **🔧 API:** http://localhost:5000  
- **❤️ Health Check:** http://localhost:5000/health

## 💡 Try These Questions

### 🎵 Music Database (Chinook)
- *"Which artists have the most albums and what are their top genres?"*
- *"Show me customer purchasing patterns across different countries"*
- *"What are the trends in music sales over time?"*

### 🌍 World Database
- *"Compare population and economic data for European countries"*
- *"Which continents have the highest life expectancy?"*
- *"Show me the relationship between country size and population density"*

### 🎬 Movie Database (IMDB)
- *"What are the trends in movie ratings and popularity over recent years?"*
- *"Which genres have the highest average ratings?"*
- *"Show me the relationship between movie duration and ratings"*

## 🏗️ Architecture & AI Flow

```mermaid
graph TD
    A[Natural Language Question] --> B[Custom LLM + GPT-4o-mini]
    B --> C[Chart Suggestion AI]
    C --> D[Smart SQL Generation]
    D --> E[MySQL Query Execution]
    E --> F[Data Processing & Limiting]
    F --> G[Chart Formatting]
    G --> H[Narrative Generation AI]
    H --> I[React Frontend]
    I --> J[Chart.js Visualizations]
    I --> K[Story Flow with Transitions]
```

### 🧠 AI Intelligence Features (As Shown in Screenshots)

1. **Smart Chart Selection**: 
   - Bar Chart for overall distribution (13 genres)
   - Pie Chart for top 6 market share visualization  
   - Table for detailed breakdown with multiple metrics

2. **Data Diversity**: Each visualization tells a different story
   - **Bar**: "Which genres are most popular?"
   - **Pie**: "What's the market share breakdown?"  
   - **Table**: "How do quantity and quality compare?"

3. **Automatic Data Limiting**: 
   - Full dataset: 13 genres in bar chart
   - Focused view: Top 6 genres in pie chart (perfect readability)
   - Detailed view: Complete breakdown in table

4. **Narrative Intelligence**: 
   - Contextual overview explaining the exploration
   - Smart transitions connecting each visualization
   - Key insights highlighting important patterns
   - Cohesive conclusion tying everything together

5. **Professional Formatting**:
   - Proper chart titles and axis labels
   - Color-coded visualizations  
   - Clean, modern dark theme UI
   - Responsive layout with smooth transitions

## 🛠️ Technology Stack

<table>
<tr>
<td><strong>🔧 Backend</strong></td>
<td><strong>🎨 Frontend</strong></td>
</tr>
<tr>
<td>

- **Flask** - Web framework
- **Custom LLM Implementation** - Optimized LLM orchestration  
- **OpenAI GPT-4o-mini** - AI reasoning
- **PyMySQL** - Database connector
- **python-dotenv** - Environment management
- **Flask-CORS** - Cross-origin support

</td>
<td>

- **Next.js 14** - React framework
- **TypeScript** - Type safety
- **Tailwind CSS** - Modern styling
- **Chart.js** - Data visualization
- **Axios** - HTTP requests

</td>
</tr>
</table>

## 📁 Project Structure

```
langchain+zigment/
├── 🔧 backend/
│   ├── app.py                 # Main Flask application with AI logic
│   ├── start.py              # Development server startup
│   ├── requirements.txt      # Python dependencies
│   ├── charts/              # Generated chart images
│   └── static/charts/       # Static chart assets
├── 🎨 frontend/
│   ├── app/
│   │   ├── components/
│   │   │   └── ChartComponent.tsx  # Main visualization component
│   │   ├── page.tsx         # Main application page
│   │   └── layout.tsx       # App layout and styling
│   ├── package.json         # Node.js dependencies
│   └── tailwind.config.js   # Styling configuration
├── 📊 databases/
│   ├── Chinook_MySql.sql    # Music industry database
│   ├── world.sql            # Global countries database  
│   └── imdb.sql             # Movie industry database
├── 🔒 .env                  # Environment variables (create from .env.example)
├── .gitignore               # Git security configuration
└── 📖 README.md             # This comprehensive guide
```

## ⚙️ Configuration

### 🔐 Environment Variables

Create `.env` in the root directory:

```env
# OpenAI Configuration
OPENAI_API_KEY=sk-your-openai-api-key-here

# Database Configuration  
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here

# Flask Configuration
FLASK_ENV=development
FLASK_DEBUG=True
```

### 🗄️ Database Setup

Update connection strings in `backend/app.py` if needed:

```python
# Multiple database support
mysql_uri_chinook = "mysql+pymysql://root:password@localhost:3306/chinook"
mysql_uri_world = "mysql+pymysql://root:password@localhost:3306/world"  
mysql_uri_imdb = "mysql+pymysql://root:password@localhost:3306/imdb"
```

## 🎯 API Reference

### Core Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | System health check |
| `POST` | `/api/ask` | Natural language to visualizations |
| `GET` | `/api/schema` | Database schema information |

### Example API Usage

```bash
# Ask a question
curl -X POST http://localhost:5000/api/ask \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Show me European countries by population",
    "database": "world"
  }'
```

## 🔍 Advanced Features

### 🎨 Smart Visualization Rules

- **Pie Charts**: Only for ≤6 categories (continents, not countries)
- **Bar Charts**: Perfect for rankings and comparisons (auto-limited to top 20)
- **Line Charts**: Time series and trends (up to 50 data points)
- **Scatter Plots**: Numeric correlations only (never categorical data)
- **Tables**: Detailed data with multiple diverse columns (up to 50 rows)

### 📝 Narrative Intelligence

The AI generates:
- **Introduction**: Context-setting overview
- **Transitions**: Smooth connections between charts
- **Key Insights**: 2-3 most important findings  
- **Conclusion**: Summary tying everything together

### 🚫 Anti-Redundancy System

Prevents boring repetitive charts:
- ❌ "Population by Country" + "GNP by Country" (same entities)
- ✅ "Population by Country" + "Population by Region" + "Size vs Density"

## 🐛 Troubleshooting

<details>
<summary><strong>🔑 OpenAI API Issues</strong></summary>

**Problem**: `ERROR: OPENAI_API_KEY environment variable is not set!`

**Solutions**:
1. Create `.env` file in project root with your API key
2. Verify API key is valid and has credits
3. Check for typos in environment variable name
4. Restart both backend and frontend after adding key

</details>

<details>
<summary><strong>🗄️ Database Connection Issues</strong></summary>

**Problem**: `Can't connect to MySQL server`

**Solutions**:
1. Ensure MySQL server is running
2. Verify credentials in connection strings
3. Check if databases (chinook, world, imdb) exist
4. Test connection with MySQL client first

</details>

<details>
<summary><strong>🌐 CORS Issues</strong></summary>

**Problem**: `Access to XMLHttpRequest has been blocked by CORS policy`

**Solutions**:
1. Backend includes CORS support for localhost:3000
2. If using different ports, update CORS configuration in `app.py`
3. Check if both frontend and backend are running

</details>

<details>
<summary><strong>📦 Dependency Issues</strong></summary>

**Problem**: `ModuleNotFoundError` or `Cannot find module`

**Solutions**:
1. Backend: `cd backend && pip install -r requirements.txt`
2. Frontend: `cd frontend && npm install`
3. Use virtual environment for Python dependencies
4. Clear npm cache: `npm cache clean --force`

</details>

## 🔒 Security & Best Practices

### 🛡️ Environment Security
- ✅ API keys in `.env` file (never committed)
- ✅ Comprehensive `.gitignore` protection
- ✅ Database credentials externalized
- ✅ CORS properly configured

### 🏗️ Production Deployment
- Use production WSGI server (not Flask dev server)
- Set `FLASK_ENV=production`
- Use environment-specific `.env` files
- Enable HTTPS for API endpoints

## 🤝 Contributing

We welcome contributions! Here's how:

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b amazing-feature`
3. **Make** your changes with tests
4. **Commit** with clear messages: `git commit -m 'Add amazing feature'`
5. **Push** to your branch: `git push origin amazing-feature`
6. **Submit** a pull request

### 🧪 Development Guidelines

- Follow PEP 8 for Python code
- Use TypeScript for frontend components
- Add comprehensive error handling
- Test with all three databases
- Update documentation for new features

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

## 🙏 Acknowledgments

- **Custom LLM Implementation** for optimized LLM orchestration
- **OpenAI** for GPT-4o-mini language model
- **Chart.js** for beautiful data visualizations
- **Next.js** for the amazing React framework
- **Chinook Database** for sample music data
- **MySQL Sample Databases** for world and IMDB data

---

<div align="center">

**🎉 Ready to turn your data into stories? Get started now! 🚀**

[⭐ Star this repo](https://github.com/your-repo) • [🐛 Report issues](https://github.com/your-repo/issues) • [💡 Request features](https://github.com/your-repo/issues)

</div>