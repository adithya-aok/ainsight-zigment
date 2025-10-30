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

from dotenv import load_dotenv
load_dotenv()



from ChatOpenAI import ChatOpenAI, ChatPromptTemplate, StrOutputParser, RunnablePassthrough
# from sql_database import SQLDatabase  # Commented out - using API instead

# API configuration for NoQL database queries
API_BASE_URL = "https://api.zigment.ai"
API_HEADERS = {
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "User-Agent": "PostmanRuntime/7.48.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "x-org-id": "6617aafc195dea3f1dbdd894",
    "zigment-x-api-key": os.environ.get("ZIGMENT_API_KEY")
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
- **COMMUNICATION CHANNELS**: For questions about "communication channels", "messages by channel", or "channel distribution", use the `channel` field from the `chathistories` table, NOT `event_type` from `events`.
- **DATE GROUPING**: When grouping by date/time, use `DATE_TRUNC(TO_DATE(field * 1000), 'day')` for daily grouping, `DATE_TRUNC(..., 'month')` for monthly, `DATE_TRUNC(..., 'week')` for weekly. NEVER use full timestamps for grouping as this creates unique groups for each event.
- **WEEKLY FORMATTING**: For weekly queries, ALWAYS use `DATE_TO_STRING(DATE_TRUNC(..., 'week'), '%b %d, %Y', 'UTC')` to get readable week labels (e.g., "Jan 15, 2024"). Include both the formatted label (for display) and the numeric week_start (for ordering).
- **DATA FILTERING**: Use `WHERE is_deleted = false` ONLY for tables that have this field! This excludes soft-deleted records and ensures accurate business data.
  
  **Tables WITH `is_deleted` field (ALWAYS use the filter):**
  - ✅ `corecontacts` - Use `WHERE is_deleted = false` to exclude deleted unified contacts
  - ✅ `events` - Use `WHERE is_deleted = false` to exclude deleted event records  
  - ✅ `chathistories` - Use `WHERE is_deleted = false` to exclude deleted chat sessions
  
  **Tables WITHOUT `is_deleted` field (DO NOT use this filter):**
  - ❌ `contacts` - No soft delete, all records are active
  - ❌ `contacttags` - No soft delete, all tags are active
- **NOQL SYNTAX**: Use `HOUR(TO_DATE(field * 1000))` for hour extraction. This is the correct NoQL syntax! NEVER use `EXTRACT()` - it doesn't exist in NoQL!
- **NO BETWEEN OPERATOR**: NoQL does NOT support `BETWEEN` operator. Use `>=` and `<=` instead. Example: `HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 3` instead of `HOUR(TO_DATE(timestamp * 1000)) BETWEEN 0 AND 3`.
- **TABLE SELECTION**: For "hourly activity" or "activity for today" questions, use the `contacts` table, NOT the `events` table!
- **WHO QUESTIONS**: For "who" questions, ALWAYS JOIN to get names! Never show just IDs - users want actual names!

# TABLE USAGE GUIDE:
**When to use each table based on the question type:**

**📊 CONTACTS Table** - Use for:
- Lead/contact counts, distributions, and analytics
- Contact status tracking (IN_PROGRESS, CONVERTED, NOT_QUALIFIED)
- Lead source analysis (source field)
- Contact creation trends and timelines
- Contact demographics (timezone, company)
- Lead conversion funnel analysis
- Contact engagement levels (total_messages field)

**📊 EVENTS Table** - Use for:
- User activity tracking and analytics
- Event type distributions (PAGE_VIEW, CLICK, FORM_SUBMIT, etc.)
- Meeting status tracking (MEETING_SCHEDULED, MEETING_ATTENDED, etc.)
- User behavior analysis
- Event timeline analysis
- Activity patterns and trends

**📊 CHATHISTORIES Table** - Use for:
- Chat session analytics and message counts
- Communication channel analysis (channel field)
- Response tracking (first_response_received)
- Chat engagement metrics
- Agent performance (agent_id field)
- Message volume analysis

**📊 CONTACTTAGS Table** - Use for:
- Contact tagging and categorization
- Tag distribution analysis
- Contact segmentation by tags

**📊 CORECONTACTS Table** - Use for:
- Extended contact data and lifecycle stages
- Contact lifecycle analysis (lifecycle_stage field)
- Advanced contact segmentation

**📊 CHATHISTORYDETAILEDMESSAGES Table** - Use for:
- Individual message analysis
- Detailed message counts and trends
- Message-level analytics

**📊 ORGANIZATIONS Table** - Use for:
- Company/organization data
- Industry analysis
- Organization size analysis
- Company-level metrics

**📊 USERS Table** - Use for:
- User account information
- User role analysis
- User activity tracking
- User performance metrics

# SYNTAX REFERENCE (examples and summaries):
- SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT, OFFSET (SQL-like)
- COUNT(*), COUNT(field), COUNT(DISTINCT field), AVG(field), SUM(field)
- JOIN ... ON (for relationships; only when fields are not in the main collection)
- Always alias computed fields/aggregates with AS
- Use explicit field and table names only
- For "per entity" questions, GROUP BY the entity and ORDER BY the metric

# Example User Questions and Expected Queries:

a) Q: "What is the total number of messages per user?"
A: SELECT user_id, COUNT(*) AS total_messages FROM chathistories GROUP BY user_id ORDER BY total_messages DESC LIMIT 20

b) Q: "What is the average number of messages per conversation?"
A: SELECT contact_id, COUNT(*) AS message_count FROM chathistories GROUP BY contact_id ORDER BY message_count DESC LIMIT 20

c) Q: "Average total messages across all chat histories"
A: SELECT AVG(total_messages) AS avg_messages FROM chathistories

d) Q: "Show all contacts created after Jan 1, 2024"
A: SELECT * FROM contacts WHERE created_at_timestamp > 1704067200 LIMIT 50

e) Q: "Top 5 most active agents by chats"
A: SELECT agent_id, COUNT(*) AS chat_count FROM chathistories GROUP BY agent_id ORDER BY chat_count DESC LIMIT 5

f) Q: "Weekly contact creations" (or "contacts created per week")
A: SELECT WEEK(TO_DATE(created_at_timestamp * 1000)) AS week_number, COUNT(*) AS contact_count FROM contacts GROUP BY week_number ORDER BY week_number LIMIT 20

g) Q: "Monthly message count"
A: SELECT MONTH(TO_DATE(created_at * 1000)) AS month, COUNT(*) AS message_count FROM chathistories GROUP BY month ORDER BY month LIMIT 12

h) Q: "Day-wise breakdown of message activity"
A: SELECT 
    DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000) AS date,
    COUNT(*) AS chat_sessions,
    SUM(total_messages) AS total_messages,
    AVG(total_messages) AS avg_messages_per_session
    FROM chathistories 
    WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(
      DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
      MULTIPLY(30, 86400)
    )
    GROUP BY DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000)
    ORDER BY date

i) Q: "Distribution of leads by ad source"
A: SELECT CASE WHEN meta_ad_data_synced = true THEN 'Meta/Facebook' WHEN google_ad_data_synced = true THEN 'Google' ELSE 'Other' END as ad_source, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY ad_source ORDER BY count DESC

j) Q: "Number of contacts who has replied to an agent in the last 30 days"
A: SELECT 
      'Last 30 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )

    UNION ALL

    SELECT 
      '30-60 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(60, 86400)
          )

k) Q: "Month-wise breakdown of key metrics over the last 12 months"
A: SELECT 
      DATE_TO_STRING(
        DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month'),
        '%b %Y',
        'UTC'
      ) AS month,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN first_response_received = true THEN 1 ELSE 0 END) AS responded_leads,
      DIVIDE(
        TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month')),
        1000
      ) AS month_start
    FROM contacts 
    WHERE is_deleted = false 
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
            MULTIPLY(365, 86400)
          )
    GROUP BY 
      DATE_TO_STRING(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month'), '%b %Y', 'UTC'),
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month')), 1000)
    ORDER BY month_start DESC

l) Q: "Daily breakdown of leads through the conversion funnel"
A: SELECT 
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000) AS date,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN contact_stage = 'LEAD' THEN 1 ELSE 0 END) AS leads,
      SUM(CASE WHEN contact_stage = 'QUALIFIED' THEN 1 ELSE 0 END) AS qualified,
      SUM(CASE WHEN contact_stage = 'PROPOSAL' THEN 1 ELSE 0 END) AS proposals,
      SUM(CASE WHEN contact_stage = 'CONVERTED' THEN 1 ELSE 0 END) AS converted,
      DIVIDE(SUM(CASE WHEN contact_stage = 'CONVERTED' THEN 1 ELSE 0 END), COUNT(*)) AS conversion_rate
    FROM contacts 
    WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(
      DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
      MULTIPLY(30, 86400)
    )
    GROUP BY DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000)
    ORDER BY date

m) Q: "Distribution of contacts by timezone"
A: SELECT timezone, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY timezone ORDER BY count DESC LIMIT 10

n) Q: "Distribution of meetings by status"
A: SELECT type as meeting_status, COUNT(*) as count FROM events WHERE is_deleted = false
    AND type IN ('MEETING_SCHEDULED', 'MEETING_ATTENDED', 'MEETING_CANCELLED', 'MEETING_RESCHEDULED')
    GROUP BY type ORDER BY count DESC

o) Q: "Distribution of contacts by tags"
A: SELECT label, COUNT(*) AS count FROM contacttags WHERE label IS NOT NULL AND label != '' GROUP BY label ORDER BY count DESC, label ASC LIMIT 20

p) Q: "Distribution of contacts by lifecycle stage"
A: SELECT
      lifecycle_stage,
      COUNT(*) AS total_contacts
      FROM corecontacts
      WHERE lifecycle_stage IS NOT NULL
      and is_deleted = false
      and lifecycle_stage != 'NONE' 
      GROUP BY lifecycle_stage
      ORDER BY total_contacts DESC

q) Q: "Week-wise breakdown of new leads over the last 8 weeks"
A: SELECT 
      DATE_TO_STRING(
        DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week'),
        '%b %d, %Y',
        'UTC'
      ) AS week_label,
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')), 1000) AS week_start,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN status = 'NOT_QUALIFIED' THEN 1 ELSE 0 END) AS not_qualified_leads
    FROM contacts 
    WHERE is_deleted = false 
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
            MULTIPLY(56, 86400)
          )
    GROUP BY 
      DATE_TO_STRING(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week'), '%b %d, %Y', 'UTC'),
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')), 1000)
    ORDER BY week_start

r) Q: "Contacts grouped by message engagement"
A: SELECT CASE WHEN total_messages = 0 THEN 'No Messages' WHEN total_messages <= 5 THEN 'Low (1-5)' WHEN total_messages <= 20 THEN 'Medium (6-20)' WHEN total_messages <= 50 THEN 'High (21-50)' ELSE 'Very High (50+)' END as engagement_level, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY engagement_level ORDER BY MIN(total_messages)

s) Q: "Comparison of activity between weekends and weekdays"
A: SELECT 
    CASE 
      WHEN DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000)) IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END as day_type,
    COUNT(*) as total_leads,
    SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) as converted_leads,
    AVG(total_messages) as avg_messages
  FROM contacts 
  WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(30, 86400))
  GROUP BY CASE 
    WHEN DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000)) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END

t) Q: "Number of leads that have been qualified"
A: SELECT COUNT(*) AS value, 'Qualified Leads' AS label
      FROM contacts
      WHERE is_deleted = false
      AND status = 'QUALIFIED'

u) Q: "Contacts who have exchanged messages"
A: SELECT COUNT(*) as contacts_with_messages FROM contacts WHERE is_deleted = false AND total_messages > 0

v) Q: "Contacts with recent message activity"
A: SELECT COUNT(*) as active_contacts FROM contacts WHERE is_deleted = false AND last_message_timestamp > 0

w) Q: "Count of all messages (Agent + User)"
A: SELECT 
      'Last 30 Days' AS period,
      COUNT(*) AS total_messages
    FROM chathistorydetailedmessages
    WHERE created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )

    UNION ALL

    SELECT 
      '30-60 Days' AS period,
      COUNT(*) AS total_messages
    FROM chathistorydetailedmessages
    WHERE created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(60, 86400)
          )

x) Q: "Distribution of leads by their source"
A: SELECT source, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY source ORDER BY count DESC

y) Q: "Distribution of messages by communication channel"
A: SELECT channel, COUNT(*) as chat_sessions, SUM(total_messages) as total_messages FROM chathistories WHERE is_deleted = false GROUP BY channel ORDER BY total_messages DESC

z) Q: "Messages grouped by communication channels"
A: SELECT channel, COUNT(*) as chat_sessions, SUM(total_messages) as total_messages FROM chathistories WHERE is_deleted = false GROUP BY channel ORDER BY total_messages DESC

aa) Q: "Conversion rates by lead source"
A: SELECT source, COUNT(*) as total_leads, SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) as converted_leads FROM contacts WHERE is_deleted = false GROUP BY source ORDER BY total_leads DESC

bb) Q: "Current week vs previous week key metrics comparison"
A: SELECT 
      'This Week' AS period,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN first_response_received = true THEN 1 ELSE 0 END) AS responded_leads
    FROM contacts
    WHERE is_deleted = false
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(7, 86400)
          )
      AND created_at_timestamp < DIVIDE(TO_LONG(CURRENT_DATE()), 1000)

    UNION ALL

    SELECT 
      'Last Week' AS period,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN first_response_received = true THEN 1 ELSE 0 END) AS responded_leads
    FROM contacts
    WHERE is_deleted = false
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(14, 86400)
          )
      AND created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(7, 86400)
          )

cc) Q: "Distribution of leads by their current status"
A: SELECT status, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY status ORDER BY count DESC

dd) Q: "Distribution of leads by their contact stage"
A: SELECT contact_stage, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY contact_stage ORDER BY count DESC

ee) Q: "Day-wise breakdown of new leads over the last 30 days"
A: SELECT 
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000) AS date,
      COUNT(*) AS new_leads
    FROM contacts 
    WHERE is_deleted = false 
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
            MULTIPLY(30, 86400)
          )
    GROUP BY DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000)
    ORDER BY date

ff) Q: "Number of contacts who has replied to an agent in the last 30 days"
A: SELECT 
      'Last 30 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )

    UNION ALL

    SELECT 
      '30-60 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(60, 86400)
          )

gg) Q: "Average number of messages per conversation (Last 30 Days)"
A: SELECT 
      ROUND(AVG(total_messages)) AS value,
      'Avg Total Message' AS label
    FROM contacts
    WHERE is_deleted = false
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
    GROUP BY NULL

hh) Q: "Hour-by-hour breakdown of activity for today"
A: SELECT 
    HOUR(TO_DATE(created_at_timestamp * 1000)) as hour,
    COUNT(*) as new_contacts,
    SUM(CASE WHEN total_messages > 0 THEN 1 ELSE 0 END) as active_contacts
  FROM contacts 
  WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(1, 86400))
  GROUP BY HOUR(TO_DATE(created_at_timestamp * 1000))
  ORDER BY hour

ii) Q: "Events by contact with contact details"
A: SELECT 
    c.name as contact_name,
    c.email,
    e.event_type,
    COUNT(*) as event_count
  FROM events e
  JOIN contacts c ON e.contact_id = c._id
  WHERE e.is_deleted = false
  GROUP BY c.name, c.email, e.event_type
  ORDER BY event_count DESC

jj) Q: "User activity with user details"
A: SELECT 
    u.username,
    u.first_name,
    u.last_name,
    e.event_type,
    COUNT(*) as activity_count
  FROM events e
  JOIN users u ON e.user_id = u._id
  WHERE e.is_deleted = false
  GROUP BY u.username, u.first_name, u.last_name, e.event_type
  ORDER BY activity_count DESC

kk) Q: "Contacts with their organization details"
A: SELECT 
    c.name as contact_name,
    c.email,
    o.name as organization_name,
    o.industry,
    o.size
  FROM contacts c
  JOIN organizations o ON c.company = o.name
  WHERE c.is_deleted = false
  ORDER BY o.name, c.name

ll) Q: "Events grouped by date and type"
A: SELECT 
    DATE_TRUNC(TO_DATE(created_at * 1000), 'day') AS event_date,
    event_type,
    COUNT(*) AS event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY DATE_TRUNC(TO_DATE(created_at * 1000), 'day'), event_type
  ORDER BY event_date, event_count DESC

mm) Q: "Monthly event activity by type"
A: SELECT 
    DATE_TRUNC(TO_DATE(created_at * 1000), 'month') AS event_month,
    event_type,
    COUNT(*) AS event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY DATE_TRUNC(TO_DATE(created_at * 1000), 'month'), event_type
  ORDER BY event_month, event_count DESC

nn) Q: "Weekly contact creation trends"
A: SELECT 
    DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week') AS week_start,
    COUNT(*) AS new_contacts
  FROM contacts
  WHERE is_deleted = false
  GROUP BY DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')
  ORDER BY week_start DESC

oo) Q: "Hourly event activity breakdown"
A: SELECT 
    HOUR(TO_DATE(timestamp * 1000)) as hour,
    COUNT(*) as event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY HOUR(TO_DATE(timestamp * 1000))
  ORDER BY hour

pp) Q: "Event activity by time ranges"
A: SELECT 
    CASE
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 3 THEN '00:00-03:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 4 AND HOUR(TO_DATE(timestamp * 1000)) <= 6 THEN '04:00-06:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 7 AND HOUR(TO_DATE(timestamp * 1000)) <= 9 THEN '07:00-09:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 10 AND HOUR(TO_DATE(timestamp * 1000)) <= 12 THEN '10:00-12:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 13 AND HOUR(TO_DATE(timestamp * 1000)) <= 15 THEN '13:00-15:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 16 AND HOUR(TO_DATE(timestamp * 1000)) <= 18 THEN '16:00-18:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 19 AND HOUR(TO_DATE(timestamp * 1000)) <= 21 THEN '19:00-21:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 22 AND HOUR(TO_DATE(timestamp * 1000)) <= 23 THEN '22:00-23:00'
    END as time_range,
    COUNT(*) as event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY time_range
  ORDER BY time_range

qq) Q: "Hour-by-hour breakdown of activity for today"
A: SELECT 
    HOUR(TO_DATE(created_at_timestamp * 1000)) as hour,
    COUNT(*) as new_contacts,
    SUM(CASE WHEN total_messages > 0 THEN 1 ELSE 0 END) as active_contacts
  FROM contacts 
  WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(1, 86400))
  GROUP BY HOUR(TO_DATE(created_at_timestamp * 1000))
  ORDER BY hour

# SCHEMA:
{schema}

# TABLE RELATIONSHIPS:
**CRITICAL: Understanding Table Relationships for JOINs**

1. **events** ↔ **contacts** (Primary Relationship)
   - `events.contact_id` → `contacts._id` (Foreign Key)
   - Use: JOIN events ON events.contact_id = contacts._id
   - Purpose: Link events/activities to specific contacts

2. **events** ↔ **users** (User Activity Tracking)
   - `events.user_id` → `users._id` (Foreign Key)
   - Use: JOIN events ON events.user_id = users._id
   - Purpose: Track which user performed which events

3. **contacts** ↔ **organizations** (Company Association)
   - `contacts.company` → `organizations.name` (Logical relationship)
   - Use: JOIN contacts ON contacts.company = organizations.name
   - Purpose: Link contacts to their organizations

4. **Additional Tables** (Referenced in examples but not in core schema):
   - `chathistories` - Chat session data (contact_id → contacts._id)
   - `contacttags` - Contact tagging (contact_id → contacts._id)
   - `corecontacts` - Extended contact data (contact_id → contacts._id)
   - `chathistorydetailedmessages` - Individual messages (chat_id → chathistories._id)

**JOIN PATTERNS:**
- For contact-related events: JOIN events ON events.contact_id = contacts._id
- For user activity: JOIN events ON events.user_id = users._id
- For organization data: JOIN contacts ON contacts.company = organizations.name
- For chat data: JOIN chathistories ON chathistories.contact_id = contacts._id

# USER QUESTION:
{question}

# OUTPUT: The NoQL query ONLY, no explanation.
"""

# Schema cache removed - using hardcoded schema

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
def safe_json_dumps(obj, **kwargs):
    """JSON dumps with default handler for non-serializable objects (Decimal, datetime, etc.)"""
    def default_handler(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return str(obj)
    return json.dumps(obj, default=default_handler, **kwargs)


# ===== NoQL API Helper Functions =====
# Schema simplification function removed - using hardcoded schema


# Cached schema as JSON string (computed once at module load)
_SCHEMA_DICT = {
        "collections": [
            {
                "name": "contacts",
                "description": "Customer/Lead contact information",
                "fields": [
                    {"name": "_id", "type": "STRING", "unique": True},
                    {"name": "first_name", "type": "TEXT"},
                    {"name": "last_name", "type": "TEXT"},
                    {"name": "full_name", "type": "TEXT"},
                    {"name": "email", "type": "EMAIL"},
                    {"name": "phone", "type": "PHONE"},
                    {"name": "status", "type": "SELECT", "options": ["NEW", "INITIATED", "IN_PROGRESS", "NOT_INTERESTED", "NOT_QUALIFIED", "CONVERTED", "IN_ACTIVE"]},
                    {"name": "contact_stage", "type": "TEXT"},
                    {"name": "source", "type": "SELECT", "options": ["GOOGLE_AD", "META_AD", "META_LEADGEN", "META_CTWA", "TIKTOK_AD", "LINKEDIN_AD", "OUTBOUND_CALL", "INBOUND_CALL", "INBOUND_FACEBOOK", "INBOUND_INSTAGRAM", "INBOUND_WHATSAPP", "LANDING_PAGE", "WEBSITE_FORM", "IMPORT", "MANUAL", "OTHER"]},
                    {"name": "timezone", "type": "TEXT"},
                    {"name": "notes", "type": "TEXTAREA"},
                    {"name": "total_messages", "type": "NUMBER"},
                    {"name": "total_user_messages", "type": "NUMBER"},
                    {"name": "total_agent_messages", "type": "NUMBER"},
                    {"name": "last_message_timestamp", "type": "DATETIME"},
                    {"name": "first_agent_message_timestamp", "type": "DATETIME"},
                    {"name": "first_user_message_timestamp", "type": "DATETIME"},
                    {"name": "first_response_received", "type": "BOOLEAN"},
                    {"name": "is_archived", "type": "BOOLEAN"},
                    {"name": "created_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000))"},
                    {"name": "updated_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(updated_at_timestamp * 1000))"}
                ]
            },
            {
                "name": "corecontacts",
                "description": "Unified contact information across all agents and channels",
                "fields": [
                    {"name": "_id", "type": "STRING", "unique": True},
                    {"name": "first_name", "type": "TEXT"},
                    {"name": "last_name", "type": "TEXT"},
                    {"name": "full_name", "type": "TEXT"},
                    {"name": "email", "type": "EMAIL"},
                    {"name": "phone", "type": "PHONE"},
                    {"name": "lifecycle_stage", "type": "TEXT"},
                    {"name": "status", "type": "TEXT"},
                    {"name": "sub_status", "type": "TEXT"},
                    {"name": "timezone", "type": "TEXT"},
                    {"name": "notes", "type": "TEXTAREA"},
                    {"name": "is_spam", "type": "BOOLEAN"},
                    {"name": "is_unsubscribed", "type": "BOOLEAN"},
                    {"name": "is_consent_received", "type": "BOOLEAN"},
                    {"name": "is_archived", "type": "BOOLEAN"},
                    {"name": "is_active", "type": "BOOLEAN"},
                    {"name": "is_deleted", "type": "BOOLEAN"},
                    {"name": "created_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000))"},
                    {"name": "updated_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(updated_at_timestamp * 1000))"}
                ]
            },
            {
                "name": "contacttags",
                "description": "Customer/Lead contact tag information",
                "fields": [
                    {"name": "_id", "type": "STRING", "unique": True},
                    {"name": "label", "type": "TEXT", "required": True},
                    {"name": "color", "type": "TEXT", "required": True},
                    {"name": "contact_id", "type": "REFERENCE"},
                    {"name": "created_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000))"},
                    {"name": "updated_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(updated_at_timestamp * 1000))"}
                ]
            },
            {
                "name": "events",
                "description": "Tracks lifecycle, marketing, and communication events across channels",
                "fields": [
                    {"name": "_id", "type": "STRING", "unique": True},
                    {"name": "event_id", "type": "TEXT", "required": True, "unique": True},
                    {"name": "org_id", "type": "REFERENCE", "required": True},
                    {"name": "anonymous_id", "type": "TEXT"},
                    {"name": "email", "type": "EMAIL"},
                    {"name": "phone", "type": "PHONE"},
                    {"name": "core_contact_id", "type": "REFERENCE"},
                    {"name": "agent_contact_id", "type": "REFERENCE"},
                    {"name": "category", "type": "SELECT", "options": ["ENGAGEMENT", "LIFECYCLE", "SYSTEM", "COMMUNICATION", "CUSTOM"]},
                    {"name": "type", "type": "SELECT", "options": ["MESSAGE_SENT", "MESSAGE_RECEIVED", "STATUS_CHANGE", "STAGE_CHANGE", "LIFECYCLE_CHANGE", "CUSTOM"]},
                    {"name": "change_value_for_type", "type": "TEXT"},
                    {"name": "timestamp", "type": "NUMBER", "required": True, "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(timestamp * 1000))"},
                    {"name": "channel", "type": "SELECT", "options": ["WEB", "SMS", "WHATSAPP", "EMAIL", "QR", "VOICE", "SOCIAL"]},
                    {"name": "sub_channel", "type": "TEXT"},
                    {"name": "message_content", "type": "TEXTAREA"},
                    {"name": "message_sentiment", "type": "SELECT", "options": ["POSITIVE", "NEGATIVE", "NEUTRAL"]},
                    {"name": "message_type", "type": "SELECT", "options": ["TEXT", "IMAGE", "VIDEO", "AUDIO", "DOCUMENT"]},
                    {"name": "created_at", "type": "DATETIME", "required": True},
                    {"name": "updated_at", "type": "DATETIME", "required": True},
                    {"name": "is_deleted", "type": "BOOLEAN"}
                ]
            },
            {
                "name": "chathistories",
                "description": "Stores conversation history, feedback metrics, and message analytics for each contact across channels",
                "fields": [
                    {"name": "_id", "type": "STRING", "unique": True},
                    {"name": "org_id", "type": "REFERENCE"},
                    {"name": "org_agent_id", "type": "REFERENCE"},
                    {"name": "contact_id", "type": "REFERENCE"},
                    {"name": "provider", "type": "TEXT"},
                    {"name": "channel", "type": "SELECT", "options": ["WEB", "WHATSAPP", "INSTAGRAM", "FACEBOOK_MESSENGER", "EMAIL", "VOICE", "OTHER"]},
                    {"name": "contact_phone_number", "type": "PHONE"},
                    {"name": "contact_email", "type": "EMAIL"},
                    {"name": "body", "type": "TEXTAREA"},
                    {"name": "last_message_timestamp", "type": "DATETIME"},
                    {"name": "first_agent_message_timestamp", "type": "DATETIME"},
                    {"name": "first_user_message_timestamp", "type": "DATETIME"},
                    {"name": "total_guardrail_triggers", "type": "NUMBER"},
                    {"name": "first_response_received", "type": "BOOLEAN"},
                    {"name": "total_messages", "type": "NUMBER"},
                    {"name": "total_user_messages", "type": "NUMBER"},
                    {"name": "total_agent_messages", "type": "NUMBER"},
                    {"name": "created_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000))"},
                    {"name": "updated_at_timestamp", "type": "DATETIME", "storage": "unix_epoch_seconds", "note": "⚠️ Unix epoch SECONDS! Must convert: TO_DATE(field * 1000) before using date functions", "example": "DAY_OF_WEEK(TO_DATE(updated_at_timestamp * 1000))"},
                    {"name": "is_deleted", "type": "BOOLEAN"}
                ]
            }
        ]
    }

# Convert to JSON string once at module load (used directly throughout the code)
_SCHEMA_JSON = json.dumps(_SCHEMA_DICT, indent=2)

def get_hardcoded_schema() -> dict:
    """Return hardcoded schema for the application."""
    return _SCHEMA_DICT

# get_schema() removed - use _SCHEMA_JSON directly for JSON string format
# or get_hardcoded_schema() for dict format

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
        print("SQLite database schema updated with database_name columns")
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

# Best-effort sanitizer for quick exploratory NoQL queries
# Generic cleaner for any NoQL query emitted by the LLM
def _strip_query_fences(q) -> str:
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

# Normalize query: strip fences and enforce limit
def normalize_query(query: str, limit: int = 50) -> str:
    """Normalize a NoQL query by stripping markdown fences and ensuring LIMIT clause."""
    query = _strip_query_fences(query)
    query = ensure_limit(query, limit)
    return query

# ChatGPT-style markdown generation prompt with selective chart embedding (grounded)
chat_markdown_prompt = ChatPromptTemplate.from_template("""
You are having a natural conversation about CRM data and insights. Write as if you're talking to a colleague - friendly, informative, and conversational, but still professional and business-focused.

User asked: {question}

🎯 **CRITICAL: ANSWER EXACTLY WHAT WAS ASKED**
- Be conversational, not formal or robotic
- Use natural language and a friendly tone
- Focus on actionable business insights
- Don't mention database names or technical details - just discuss the findings naturally
- Write like you're explaining to a friend who needs the insights

ACTUAL DATABASE SCHEMA:
{schema}

SAMPLE DATA (first few rows per table):
{samples}

Recent conversation (most recent last). Use this context to maintain continuity and build on previous points naturally. Do NOT restate earlier content verbatim:
{history}

Facts (ground truth; ONLY use these for numeric claims):
{facts}

AllowedEntities (you may ONLY reference these specific entities by name; otherwise use generic terms):
{allowed_entities}

Write a conversational response (3-5 paragraphs). Talk naturally about the data:
- Start by acknowledging what they're asking about and give a quick overview
- Walk through what the data shows in a conversational way
- Point out interesting patterns or insights as you would in a conversation
- Discuss what this might mean for their business or operations
- Keep it friendly and natural - like explaining to a colleague over coffee, not reading from a formal report

Guidelines:
- Write in a conversational, natural tone - like talking to a colleague
- Use phrases like "I noticed...", "What's interesting is...", "Here's what I found...", "Looking at the data..."
- Focus on what the data reveals about leads, contacts, conversions, and engagement
- Share insights naturally - don't sound like you're reading from a report
- Use business terminology naturally: "leads", "conversions", "engagement", "channels", "lifecycle stages"
- **Include charts to visualize data** - **🚨🚨🚨 YOU MUST GENERATE EXACTLY 1 CHART - ONLY 1 CHART, NO MORE 🚨🚨🚨**
- Never mention database names, technical implementation details, or query structures
- Avoid formal report language - be conversational and friendly while staying professional
- **Chart Guidelines:**
  * **🚨🚨🚨 CRITICAL RULE: Generate EXACTLY 1 CHART - ALWAYS 1 CHART, NEVER 2 OR MORE 🚨🚨🚨**
  * **ONLY 1 CHART for ALL questions** - This is not optional, this is mandatory
  * **DO NOT generate 2, 3, or 4 charts** - Generate ONLY 1 chart
  * **ONLY exception:** If question EXPLICITLY says "show me multiple charts" or "different perspectives" - then you can generate 2-4 charts
  * **ALL questions get 1 chart**: "contacts per org", "top 10 items", "hourly activity", "users by status", "event types", "most common X", "hourly breakdown", "messages by channel", etc. - ALL get exactly 1 chart
  * **ALWAYS include exactly 1 chart when answering data questions** - charts make data easier to understand!
- Numeric grounding rules:
  - Do NOT invent numbers. If a number is not present in Facts or will be shown in a chart, avoid it or phrase qualitatively ("large share", "increased over time").
  - Prefer citing numbers after a chart block, e.g., "According to the chart below…".
  - Only name specific cities/airlines/aircraft that appear in AllowedEntities or in the chart labels. Otherwise, use generic phrases ("major hub", "a large carrier").

**CHART GENERATION RULES:**
- **✅ ALWAYS include charts when discussing data** - they help visualize and explain
- **🚨🚨🚨 MANDATORY: GENERATE EXACTLY 1 CHART - ONE CHART ONLY - DO NOT GENERATE MULTIPLE CHARTS 🚨🚨🚨**
  * **RULE: ALWAYS generate EXACTLY 1 chart for EVERY question** - This is mandatory, not optional
  * **DO NOT generate 2 charts, DO NOT generate 3 charts, DO NOT generate 4 charts** - Generate ONLY 1 chart
  * **The ONLY exception** is if the question EXPLICITLY contains phrases like:
    - "show me multiple charts", "different perspectives", "multiple views", "various breakdowns"
    - ONLY then you can generate 2-4 charts
  * **All questions get 1 chart**: "contacts per org", "top 10 X", "count by Y", "hourly activity", "event types", "most common X", "breakdown by status", "messages by channel", "hourly breakdown", etc. - ALL get exactly 1 chart
  * **If the question doesn't EXPLICITLY say "multiple charts" or "different perspectives" → You MUST generate exactly 1 chart**
- **🚨 IF you generate multiple charts: Each chart MUST show DIFFERENT data/perspectives**
- **NEVER generate 2+ charts that group by the same dimension** - if all charts would be identical, generate only 1
- **🚨 KEEP QUERIES SIMPLE:** 
  * **Group by ONE dimension only** - Avoid complex multi-column grouping
    - ✅ GOOD: `SELECT [dimension], COUNT(*) FROM [table] GROUP BY [dimension]`
    - ❌ BAD: `SELECT [dim1], [dim2], COUNT(*) FROM [table] GROUP BY [dim1], [dim2]` (too complex)
    - ❌ BAD: `SELECT YEAR(...), MONTH(...), DAY(...), [field] FROM [table] GROUP BY year, month, day, [field]` (way too granular)
  
  * **🕐 HOURLY DATA AGGREGATION:**
    - When asked about hourly patterns, data by hour, or time of day analysis:
    - ✅ ALWAYS aggregate hours into 3-hour or 4-hour ranges for better visualization
    - ✅ GOOD: Ask for "events grouped by time ranges: 0-3, 3-6, 6-9, 9-12, 12-15, 15-18, 18-21, 21-24"
    - ✅ GOOD: For NoQL databases with timestamp fields, use EXTRACT to get hour then bucket with CASE:
      * "CASE WHEN HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 2 THEN '0-3' ..."
    - ❌ BAD: "Show events for each hour 0-23" (too granular, 24 bars is too many)
    - ❌ BAD: Grouping by individual hours when asked about hourly patterns
    - ❌ BAD: Using HOUR() function directly - use EXTRACT(hour FROM ...) for NoQL
    - Examples of good hourly chart questions:
      * "Show event distribution by time ranges (0-3, 3-6, 6-9, 9-12, 12-15, 15-18, 18-21, 21-24)"
      * "What are the busiest time ranges throughout the day using 3-hour intervals?"
  
  * **📅 MONTHLY DATA - CONVERT TO MONTH NAMES:**
    - When asking for monthly data, ALWAYS request month names, NOT numbers
    - ✅ GOOD: "Show new contacts created per month with month names (January, February, March...)"
    - ✅ GOOD: Include instruction: "convert month numbers to month names using CASE statement"
    - ❌ BAD: "Show contacts per month" (will return numbers 1, 2, 3... which is confusing)
    - Example chart question: "Count of new contacts created per month, with months shown as January, February, March, etc."

🚨 **MANDATORY NoQL PATTERN FOR MONTHLY CHARTS:**
```noql
SELECT 
  CASE MONTH(TO_DATE(created_at_timestamp * 1000))
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month,
  COUNT(*) AS count
FROM contacts
GROUP BY MONTH(TO_DATE(created_at_timestamp * 1000))
ORDER BY MONTH(TO_DATE(created_at_timestamp * 1000))
```
- **NEVER** use `SELECT MONTH(...) AS month` - this returns numbers!
- **ALWAYS** use the CASE statement above for month names!
  
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

- **🚨🚨🚨🚨🚨 ABSOLUTE RULE: GENERATE EXACTLY 1 CHART - ONE CHART ONLY - NEVER 2 OR MORE 🚨🚨🚨🚨🚨**
- **MANDATORY: Always generate EXACTLY 1 chart** - This rule applies to 100% of questions
- **ONE CHART ONLY for ALL questions**: "contacts per org", "users by status", "top 10 products", "hourly activity", "event types", "most common X", "count by Y", "messages by hour", "breakdown by channel", "hourly breakdown", "activity by time", etc. - ALL get exactly 1 chart
- **NEVER generate 2-4 charts** unless question EXPLICITLY says: "show me multiple charts", "different perspectives", "multiple views", "analyze from different angles"
- **When in doubt: Generate EXACTLY 1 chart** - This is always the correct answer
- **DO NOT generate multiple charts for ANY reason** - Complexity doesn't mean multiple charts, it means a better single chart
- **REMEMBER: ONE QUESTION = ONE CHART** - Always
- Each chart should reveal a DIFFERENT aspect: time, category, status, type, agent, comparison, etc.
- Use the exact format: ```chart
{{"type": "bar|line|pie|scatter", "question": "specific query", "title": "Simple title", "db": "{database_name}"}}
```

- **✅ EXAMPLES OF TRULY DIVERSE CHARTS (FOR REFERENCE - DON'T SHOW QUERIES TO USER):**
  
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
  
  **When to generate multiple charts:** ONLY when user EXPLICITLY says "show me multiple charts" or "different perspectives" - Otherwise generate exactly 1 chart
  
  **🚨 CRITICAL: DON'T include NoQL code in your response to users!** The chart blocks will automatically generate the queries. Just include the ```chart blocks and your natural language explanation.
  
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

Examples of conversational style for CRM data:

**EXAMPLE 1: Lead Status Distribution**
"What are the leads by status?"
"Looking at your current pipeline, I can see the leads are spread across different stages pretty evenly. Most of them are actively being worked on, which is good - you've got movement in the funnel.

```chart
{{"type": "bar", "question": "Distribution of leads by status", "title": "Leads by Status", "db": "zigment"}}
```

What's interesting is that you have a good mix of leads in progress and converted ones. The fact that you're seeing leads move through the stages suggests your follow-up process is working. You might want to focus on pushing those in-progress ones toward conversion if possible."

**EXAMPLE 2: Source Performance Analysis**
"Which lead sources perform best?"
"I pulled up the numbers on your lead sources, and there's definitely a clear winner here. Some channels are bringing in not just more leads, but better quality ones that actually convert.

```chart
{{"type": "bar", "question": "Conversion rates by lead source", "title": "Lead Source Performance", "db": "zigment"}}
```

This is really useful because it tells you where to focus your marketing budget. If one source is giving you high volumes but low conversion, and another is the opposite, you might want to double down on what's actually working."

**EXAMPLE 3: Channel Engagement**
"Which communication channels are most effective?"
"So I looked at where you're getting the most engagement, and it's pretty clear which channels your contacts prefer. WhatsApp seems to be where most of the action happens.

```chart
{{"type": "pie", "question": "Distribution of messages by channel", "title": "Messages by Channel", "db": "zigment"}}
```

This makes sense - people tend to respond faster on channels they use regularly. You might want to prioritize outreach on the channels where you're seeing the most engagement, since that's where your contacts are actually active."

**EXAMPLE 4: Conversion Trends**
"Show conversion trends over time"
"I tracked your conversions over the past few months, and there's a pretty interesting pattern here. You had some strong months, then things dipped a bit, and now it's picking back up.

```chart
{{"type": "line", "question": "Monthly conversion rates over the last 12 months", "title": "Conversion Trend Analysis", "db": "zigment"}}
```

The trend shows some seasonality which is normal, but what I'd watch is whether those dips are something you can address. Maybe there's a pattern - like certain campaigns perform better at certain times, or maybe it's about following up faster when leads come in."

**EXAMPLE 5: Contact Activity Patterns**
"What times of day see the most contact activity?"
"This is cool - I looked at when your contacts are most active, and there's a clear pattern. Most of the engagement happens during business hours, which makes total sense.

```chart
{{"type": "bar", "question": "Contact activity by time ranges", "title": "Daily Activity Patterns", "db": "zigment"}}
```

The peak times are mid-morning and early afternoon. So if you're doing outreach, that's probably when you'll get the best response rates. Early morning or late evening might work for some people, but the bulk of activity is when you'd expect - during normal business hours."

**EXAMPLE 6: Lifecycle Stage Distribution**
"What's the breakdown by lifecycle stage?"
"Looking at where your contacts are in the journey, I can see most of them are in the middle stages - which is actually pretty good. It means they're progressing, not stuck at the beginning.

```chart
{{"type": "bar", "question": "Contacts by lifecycle stage", "title": "Lifecycle Stage Analysis", "db": "zigment"}}
```

What I'd pay attention to is if there's a stage where contacts are getting stuck. If you see a huge pile-up at one stage, that's probably where you need to focus more effort - maybe it needs better nurturing or a different approach."

**CONVERSATIONAL STRUCTURE:**
1. **Natural Opening**: Acknowledge what they asked, maybe with a quick observation
2. **Walk Through the Data**: Talk through what you see in the chart naturally, like explaining to a friend
3. **Share Insights**: Point out interesting patterns or what stands out to you
4. **Actionable Thoughts**: Suggest what they might want to consider, but do it conversationally

Write like you're having a friendly chat with someone who needs insights, not like you're delivering a formal presentation. Be engaging, natural, and helpful.
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
🚨🚨🚨 **CRITICAL BLOCKING RULES - READ FIRST!** 🚨🚨🚨

❌ **ABSOLUTELY FORBIDDEN - USING THESE WILL CAUSE IMMEDIATE FAILURE:**
- EXTRACT() function - DOES NOT EXIST IN NoQL! Use HOUR() instead!
- BETWEEN operator - DOES NOT EXIST IN NoQL! Use >= AND <= instead!
- UNIX_TIMESTAMP() - DOES NOT EXIST IN NoQL!
- DATE_SUB() - DOES NOT EXIST IN NoQL!
- CURRENT_DATE() - DOES NOT EXIST IN NoQL!
- NOW() - DOES NOT EXIST IN NoQL!
- INTERVAL - DOES NOT EXIST IN NoQL!

✅ **ONLY USE THESE FOR DATE FILTERING:**
- WHERE timestamp >= 1704067200 (numeric Unix timestamp)
- WHERE created_at_timestamp >= 1710000000 (numeric Unix timestamp)

🚨 **CRITICAL: is_deleted FIELD RULES - FOLLOW EXACTLY OR QUERY WILL FAIL!**
- ✅ Tables WITH `is_deleted` field (ALWAYS use WHERE is_deleted = false):
  * corecontacts - Use WHERE is_deleted = false
  * events - Use WHERE is_deleted = false  
  * chathistories - Use WHERE is_deleted = false
- ❌ Tables WITHOUT `is_deleted` field (DO NOT use this filter):
  * contacts - No is_deleted field, don't use this filter
  * contacttags - No is_deleted field, don't use this filter

🚨 **IF YOU USE ANY FORBIDDEN FUNCTION, THE QUERY WILL FAIL WITH INVALID_QUERY ERROR!**

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
- "average messages per conversation" → SELECT c.full_name, COUNT(ch._id) AS message_count FROM chathistories ch JOIN contacts c ON ch.contact_id = c._id WHERE ch.is_deleted = false GROUP BY c.full_name ORDER BY message_count DESC LIMIT 20
- "average contacts per org" → SELECT org_id, COUNT(*) AS contact_count FROM contacts GROUP BY org_id ORDER BY contact_count DESC LIMIT 20
- "messages per conversation" → SELECT c.full_name, COUNT(ch._id) AS message_count FROM chathistories ch JOIN contacts c ON ch.contact_id = c._id WHERE ch.is_deleted = false GROUP BY c.full_name ORDER BY message_count DESC LIMIT 20
- "who sent most messages" → SELECT c.full_name, COUNT(ch._id) AS message_count FROM chathistories ch JOIN contacts c ON ch.contact_id = c._id WHERE ch.is_deleted = false GROUP BY c.full_name ORDER BY message_count DESC LIMIT 20

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
- MySQL-style: DAYOFWEEK(), DAYOFMONTH(), DAYNAME(), MONTHNAME(), HOUR()
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
- `EXTRACT(day|month|year|hour|minute|second from date_field)` - Use for hour/minute/second extractions

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
- `SELECT HOUR(TO_DATE(timestamp * 1000)) AS hour, COUNT(*) FROM events WHERE is_deleted = false GROUP BY hour` ← **For hour extraction**
- `WHERE TO_DATE(timestamp * 1000) >= TO_DATE('2024-01-01')` (date comparison)

**✅ CORRECT Example for hourly time ranges:**
```sql
SELECT 
  CASE 
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 2 THEN '0-3'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 3 AND HOUR(TO_DATE(timestamp * 1000)) <= 5 THEN '3-6'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 6 AND HOUR(TO_DATE(timestamp * 1000)) <= 8 THEN '6-9'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 9 AND HOUR(TO_DATE(timestamp * 1000)) <= 11 THEN '9-12'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 12 AND HOUR(TO_DATE(timestamp * 1000)) <= 14 THEN '12-15'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 15 AND HOUR(TO_DATE(timestamp * 1000)) <= 17 THEN '15-18'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 18 AND HOUR(TO_DATE(timestamp * 1000)) <= 20 THEN '18-21'
    ELSE '21-24'
  END AS time_range,
  COUNT(*) AS event_count
FROM events
WHERE is_deleted = false
GROUP BY time_range
ORDER BY time_range
```

**❌ WRONG Examples (will fail or return empty):**
- `SELECT DAY_OF_WEEK(timestamp) FROM events` (timestamp is a number!)
- `SELECT TO_DATE(timestamp) FROM events` (missing * 1000!)
- `SELECT EXTRACT(dow FROM TO_DATE(timestamp * 1000)) FROM events` (returns empty - use DAY_OF_WEEK instead!)
- `SELECT DAYOFWEEK(FROM_UNIXTIME(timestamp)) FROM events` (MySQL syntax - not supported!)
- `SELECT EXTRACT(hour FROM TO_DATE(timestamp * 1000)) FROM events` (EXTRACT() not supported - use HOUR() instead!)

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
  * ❌ BAD: `SELECT b.[field], COUNT(a._id) FROM [table_a] a JOIN [table_b] b ON a.[key] = b.[key] GROUP BY b.[field]` (join not needed if field in table_a)
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
        schema_text = _SCHEMA_JSON
        print(f"Schema fetch: {time.time()-t1:.2f}s")
        
        t1 = time.time()
        counts_data = get_table_and_column_counts(database_name)
        print(f"Counts fetch: {time.time()-t1:.2f}s")
        
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
        print(f"LLM call: {time.time()-t1:.2f}s")
        
        print(f"LLM exploration response: {response}")
        
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
                noql_query = exploration.get("sql", "")
                
                if not noql_query:
                    continue
                    
                print(f"Exploration {i+1}: {purpose}")
                print(f"   Query: {noql_query}")
                
                try:
                    # Strip query fences and execute
                    t1 = time.time()
                    clean_query = normalize_query(noql_query, 20)
                    
                    # Schema validation disabled - using hardcoded schema
                    
                    rows, columns = run_query(clean_query, database_name, return_columns=True)
                    print(f"   Query execution: {time.time()-t1:.2f}s")
                    
                    if rows:
                        print(f"   SUCCESS: Found {len(rows)} results")
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
                            
                            # Add query methodology for transparency
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

📊 **Natural language queries** - Just ask in plain English, no NoQL needed
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
        return f"I'm **Insight** 🤖 - your AI-powered data analyst! I turn your questions into NoQL queries, create beautiful visualizations, and help you discover insights in your `{database_name}` database. Think of me as your friendly data expert who speaks plain English! 😊"
    
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
1. **Single item only** - Only 1 data point (e.g., "John Smith: 5 messages" - nothing to compare)
   - Example: Bar chart with ONE contact, pie chart with ONE slice
   - Reason: No comparison, distribution, or trend - just one number
   - **CRITICAL**: Bar charts with 1 data point are ALWAYS useless - they show no comparison
   
2. **No data at all** - Completely empty or null dataset

3. **Wrong chart type for data:**
   - Line chart with only 2 discrete categories (not time series)
   - Pie chart with >10 slices (use bar instead)
   - Scatter plot of non-numeric data

4. **Question asks for comparison but chart shows only 1 entity:**
   - Question: "Compare contacts" → Chart shows only 1 contact ❌
   - Question: "Message activity" → Chart shows only 1 channel ❌

✅ **APPROVE THE CHART IF:**
1. **2+ data points for comparison** - Shows ranking, comparison, or distribution
2. **Time series with 3+ points** - Shows trends over time
3. **Meaningful visualization** - Adds value beyond just stating a number
4. **Chart type matches data structure** - Bar for comparison, line for time, pie for proportions

**EXAMPLES OF CHARTS TO REJECT:**

❌ **REJECT** - "Top Contacts by Message Count"
   - Data: John Smith: 25 (ONLY 1 ITEM!)
   - Reason: Single bar chart is useless, just say "John Smith sent 25 messages"
   - Better: Show top 5-10 contacts or use text: "John Smith is the most active contact with 25 messages"

❌ **REJECT** - "WhatsApp Message Activity"
   - Data: WhatsApp: 150 (ONLY 1 CHANNEL!)
   - Reason: Bar chart with single data point provides no comparison value
   - Better: Show all channels or use text: "WhatsApp generated 150 messages"

❌ **REJECT** - "Contact Status Distribution"
   - Data: CONVERTED: 50 (ONLY 1 STATUS!)
   - Reason: No comparison, just one number
   - Better: Show all statuses or regional distribution

❌ **REJECT** - "Channel Usage"
   - Data: WhatsApp: 100% (ONLY 1 SLICE!)
   - Reason: Pie chart with 1 slice is meaningless
   - Better: Show breakdown by multiple channels or segments

**EXAMPLES OF CHARTS TO APPROVE:**

✅ **APPROVE** - "Top 5 Contacts by Message Count"
   - Data: John Smith: 25, Jane Doe: 20, Bob Wilson: 15, Alice Brown: 12, Tom Green: 10
   - Reason: Shows meaningful comparison across 5 contacts

✅ **APPROVE** - "Message Activity: WhatsApp vs Email"
   - Data: WhatsApp: 150, Email: 80
   - Reason: Direct comparison between 2 channels (minimum for comparison)

✅ **APPROVE** - "Contact Growth 2020-2024"
   - Data: 5 years of time series data
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




# get_schema() removed - use _SCHEMA_JSON directly (JSON string) or get_hardcoded_schema() (dict)

def get_table_and_column_counts(database_name: str) -> dict:
    """Return table row counts for available collections (API-based).
    
    Note: Fetches counts by running COUNT(*) queries for each collection.
    """
    try:
        # Use hardcoded schema to get list of collections
        schema = get_hardcoded_schema()
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
        cleaned_query = normalize_query(str(query), 50)
        
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
        schema = _SCHEMA_JSON
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

# Shared query execution helper
def execute_noql_question(question: str, database_name: str, output_format: str = "table", debug: bool = False) -> dict:
    """
    Shared function to execute a NoQL query for a question.
    
    Args:
        question: The user's question
        database_name: Database to query
        output_format: "table" or "chart" (affects return structure)
        debug: If True, print debug information
    
    Returns:
        dict with query results or error response
    """
    # Step 1: Check question relevance
    relevance_check = check_question_relevance(question, database_name)
    if not relevance_check["relevant"]:
        return create_error_response(
            "irrelevant_question", 
            relevance_check["error"], 
            relevance_check["suggestion"]
        )
    
    try:
        # Step 2: Generate query
        noql_chain = create_anydb_sql_chain(database_name)
        query = noql_chain.invoke({"question": question})
        query = normalize_query(query, 50)
        
        if debug:
            print(f"\n🔍 === QUERY EXECUTION ===")
            print(f"🎯 Question: {question}")
            print(f"🗄️ Database: {database_name}")
            print(f"📝 Generated NoQL Query: {query}")
        
        # Step 3: Execute query
        try:
            rows, columns = run_query(query, database_name, return_columns=True)
        except Exception as e:
            if debug:
                print(f"❌ Query execution failed: {e}")
            return create_error_response(
                "query_execution_error",
                f"Failed to execute NoQL query: {str(e)}",
                "Please try rephrasing your question or check if the data exists"
            )
        
        # Step 4: Validate data
        if not rows or not columns:
            if debug:
                print(f"❌ No data returned from query")
            return create_error_response(
                "invalid_data",
                "No data returned from query",
                "Try a different question or check if the data exists"
            )
        
        if debug:
            print(f"📋 Query Columns: {columns}")
            print(f"📊 Query Result Rows: {len(rows) if rows else 0}")
            if rows and len(rows) > 0:
                print(f"🔍 First few rows:")
                for i, row in enumerate(rows[:3]):
                    print(f"   Row {i+1}: {row}")
                if len(rows) > 3:
                    print(f"   ... and {len(rows) - 3} more rows")
            print(f"🔚 === END QUERY EXECUTION ===\n")
        
        # Step 5: Format data
        chart_type = "table" if output_format == "table" else "bar"
        formatted_data = format_data_for_chart_type(rows, chart_type, question, columns)
        
        if not formatted_data or len(formatted_data) == 0:
            if debug:
                print("❌ No meaningful data after formatting")
            return create_no_data_response(question)
        
        # Step 6: Return result based on format
        if output_format == "table":
            return {
                "success": True,
                "query": query, 
                "columns": columns, 
                "rows": rows, 
                "data": formatted_data
            }
        else:  # chart format
            title = f"Analysis: {question[:50]}..."
            x_axis, y_axis = generate_axis_labels(chart_type, columns, question, title)
            return {
                "title": title,
                "x_axis": x_axis,
                "y_axis": y_axis,
                "chart_type": chart_type,
                "data": formatted_data
            }
            
    except Exception as e:
        if debug:
            print(f"❌ Error in execute_noql_question: {e}")
            import traceback
            traceback.print_exc()
        return create_error_response(
            "processing_error" if output_format == "table" else "chart_generation_error",
            f"An unexpected error occurred: {str(e)}",
            "Please try rephrasing your question or check if the data exists"
        )

# ===== Any-DB dynamic introspection and universal prompt =====

def sample_database_tables(database_name: str, max_rows: int = 3, max_tables: int = 10) -> dict:
    """Return a small sample from each collection (API-based).
    
    Note: Fetches sample rows by running SELECT * LIMIT queries for each collection.
    """
    try:
        # Use hardcoded schema to get list of collections
        schema = get_hardcoded_schema()
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
🚨🚨🚨 **CRITICAL BLOCKING RULES - READ FIRST!** 🚨🚨🚨

❌ **ABSOLUTELY FORBIDDEN - USING THESE WILL CAUSE IMMEDIATE FAILURE:**
- UNIX_TIMESTAMP() - DOES NOT EXIST IN NoQL!
- DATE_SUB() - DOES NOT EXIST IN NoQL!
- CURRENT_DATE() - DOES NOT EXIST IN NoQL!
- NOW() - DOES NOT EXIST IN NoQL!
- INTERVAL - DOES NOT EXIST IN NoQL!
- HOUR() - DOES NOT EXIST IN NoQL!

✅ **ONLY USE THESE FOR DATE FILTERING:**
- WHERE timestamp >= 1704067200 (numeric Unix timestamp)
- WHERE created_at_timestamp >= 1710000000 (numeric Unix timestamp)

🚨 **IF YOU USE ANY FORBIDDEN FUNCTION, THE QUERY WILL FAIL WITH INVALID_QUERY ERROR!**

You are an expert NoQL query generator.
You will be given a database SCHEMA (tables and columns), TABLE/ COLUMN COUNTS, and small SAMPLES (first rows per table).
Write ONE NoQL query that answers the QUESTION using only the available tables/columns.

🚨 **CRITICAL: THESE MySQL FUNCTIONS DO NOT EXIST IN NoQL - USING THEM WILL CAUSE INVALID_QUERY ERROR:**
❌ UNIX_TIMESTAMP(), DATE_SUB(), CURRENT_DATE(), NOW(), INTERVAL, DATE_ADD()
❌ FROM_UNIXTIME(), DAYOFWEEK(), DATE_FORMAT(), HOUR(), MINUTE(), SECOND()
❌ **NEVER USE**: `UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH))` - THIS WILL FAIL!
❌ **NEVER USE**: `WHERE timestamp > UNIX_TIMESTAMP(...)` - THIS WILL FAIL!
✅ **USE**: `WHERE timestamp >= 1704067200` (numeric Unix timestamp in seconds)

📅 **FOR "LAST 6 MONTHS" QUERIES - USE THIS EXACT PATTERN:**
```sql
SELECT 
  CASE MONTH(TO_DATE(created_at_timestamp * 1000))
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month,
  COUNT(*) AS count
FROM contacts
WHERE created_at_timestamp >= 1704067200  -- Jan 1, 2024 (6 months ago)
GROUP BY MONTH(TO_DATE(created_at_timestamp * 1000))
ORDER BY MONTH(TO_DATE(created_at_timestamp * 1000))
```
- **NEVER** use `UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH))`
- **ALWAYS** use numeric timestamps like `1704067200`
- **ALWAYS** use CASE statement for month names

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
**CRITICAL: DATE/TIME HANDLING IN NoQL**
❌ NEVER use MySQL functions - they DO NOT EXIST in NoQL:
- ❌ UNIX_TIMESTAMP(), DATE_SUB(), NOW(), FROM_UNIXTIME(), DAYOFWEEK()
- ❌ CURRENT_DATE(), INTERVAL, DATE_SUB(), DATE_ADD() - These are MySQL functions!
- ❌ **HOUR() - THIS FUNCTION DOES NOT EXIST! Use EXTRACT(hour FROM ...) instead!**
- ❌ MINUTE(), SECOND() - Use EXTRACT instead

🚨 **FORBIDDEN MySQL Date Functions (will cause INVALID_QUERY error):**
- ❌ `UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))` - WRONG!
- ❌ `WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)` - WRONG!
- ❌ `WHERE timestamp >= UNIX_TIMESTAMP('2024-01-01')` - WRONG!

✅ **CORRECT NoQL Date Filtering:**
- ✅ `WHERE created_at_timestamp >= 1704067200` (Unix timestamp in seconds)
- ✅ `WHERE TO_DATE(timestamp * 1000) >= TO_DATE('2024-01-01')` (date comparison)
- ✅ For last 6 months: Calculate Unix timestamp manually or use date ranges

🚨 **SPECIFIC EXAMPLES FOR COMMON DATE FILTERS:**
- ❌ **WRONG**: `WHERE created_at_timestamp >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))`
- ✅ **CORRECT**: `WHERE created_at_timestamp >= 1704067200` (Jan 1, 2024 = 1704067200)
- ✅ **CORRECT**: `WHERE created_at_timestamp >= 1726444800` (Sept 15, 2024 = 1726444800)
- ✅ **CORRECT**: `WHERE TO_DATE(created_at_timestamp * 1000) >= TO_DATE('2024-01-01')`

📅 **MONTHLY QUERIES WITH PROPER FILTERING:**
```sql
-- ✅ CORRECT: Monthly data with date filter and month names
SELECT 
  CASE MONTH(TO_DATE(created_at_timestamp * 1000))
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month,
  COUNT(*) AS count
FROM contacts
WHERE created_at_timestamp >= 1704067200  -- Jan 1, 2024
GROUP BY MONTH(TO_DATE(created_at_timestamp * 1000))
ORDER BY MONTH(TO_DATE(created_at_timestamp * 1000))
```

🎯 **Unix Timestamp Conversion (stored as SECONDS):**
For fields like `timestamp`, `created_at`, `updated_at`, `created_at_timestamp`:
- **MUST convert**: `TO_DATE(field * 1000)` before using date functions
- Example: `SELECT DAY_OF_WEEK(TO_DATE(timestamp * 1000)) AS dow FROM events`
- Example: `SELECT DATE_TRUNC(TO_DATE(created_at * 1000), 'day') FROM contacts`

✅ NoQL Date Functions (after TO_DATE conversion):
- `DAY_OF_WEEK(TO_DATE(timestamp * 1000))` ← **For day of week (1-7)**
- `MONTH(TO_DATE(field * 1000))`, `YEAR(TO_DATE(field * 1000))` ← **For month/year numbers**
- `DATE_TRUNC(TO_DATE(field * 1000), 'day'|'month'|'year')` ← **For date truncation**
- **`HOUR(TO_DATE(field * 1000))`** ← **MANDATORY for hour extraction (0-23)**
- **`EXTRACT(minute FROM TO_DATE(field * 1000))`** ← **For minute extraction**
- **`EXTRACT(second FROM TO_DATE(field * 1000))`** ← **For second extraction**
- ⚠️ Avoid `EXTRACT(dow ...)` on converted timestamps - use `DAY_OF_WEEK()` instead

🚨 **CRITICAL - HOUR EXTRACTION:**
- ❌ **WRONG**: `EXTRACT(hour FROM TO_DATE(timestamp * 1000))` - EXTRACT() function DOES NOT EXIST!
- ✅ **CORRECT**: `HOUR(TO_DATE(timestamp * 1000))` - This is the ONLY way!

📅 **CRITICAL - MONTH NAMES FOR CHARTS:**
- When grouping by month for charts/visualization, ALWAYS convert month numbers to month names
- ❌ **WRONG**: `SELECT MONTH(TO_DATE(timestamp * 1000)) AS month, COUNT(*) ...` (returns numbers 1-12)
- ✅ **CORRECT**: Use CASE statement to convert to month names:
```sql
SELECT 
  CASE MONTH(TO_DATE(timestamp * 1000))
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month,
  COUNT(*) AS count
FROM events
GROUP BY MONTH(TO_DATE(timestamp * 1000))
ORDER BY MONTH(TO_DATE(timestamp * 1000))
```
- **Why**: Charts with "1, 2, 3..." are confusing; "January, February, March..." is much clearer
- **Always do this** for any chart question involving months (monthly trends, data by month, etc.)

⚡ For filtering only (faster without conversion):
- `WHERE timestamp >= 1710374400` (numeric comparison)

NoQL Query Rules:

🚨 **CRITICAL: MONTHLY CHARTS MUST USE MONTH NAMES!**
- **NEVER** return month numbers (1, 2, 3) for charts - users can't understand them!
- **ALWAYS** use CASE statement to convert to month names (January, February, March...)
- **MANDATORY** for any question containing "month", "monthly", "per month"

**📌 MOST COMMON MISTAKES TO AVOID:**
1. ❌ Using month numbers (1-12) instead of month names in charts - ALWAYS use CASE statement for month names!
2. ❌ Using `HOUR(date)` - Does NOT exist! Use `EXTRACT(hour FROM date)` instead
3. ❌ Using `MINUTE(date)` or `SECOND(date)` - Use `EXTRACT(minute/second FROM date)`
4. ❌ Forgetting to convert timestamps: Must use `TO_DATE(timestamp * 1000)` first
5. ❌ **Using MySQL time functions: UNIX_TIMESTAMP(), DATE_SUB(), CURRENT_DATE(), NOW(), INTERVAL** - NONE of these exist!
6. ❌ **Using DATE_ADD() in WHERE clauses** - Not supported!

**🎯 MONTHLY DATA QUERIES - MANDATORY PATTERN:**
When question asks for "per month", "monthly", "by month", or "last N months":
```sql
SELECT 
  CASE MONTH(TO_DATE(created_at_timestamp * 1000))
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month,
  COUNT(*) AS count
FROM contacts
WHERE created_at_timestamp >= 1696118400  -- Oct 1, 2023 (6 months ago in Unix seconds)
GROUP BY MONTH(TO_DATE(created_at_timestamp * 1000))
ORDER BY MONTH(TO_DATE(created_at_timestamp * 1000))
```
- **ALWAYS** use CASE statement for month names
- **NEVER** use UNIX_TIMESTAMP(), DATE_SUB(), INTERVAL, CURRENT_DATE()
- **USE** numeric timestamps for WHERE filtering
- **GROUP BY** the numeric month, **ORDER BY** numeric month, but **SELECT** month name!

**🚨 CRITICAL - TIME-BASED FILTERING:**
- ❌ **WRONG**: `WHERE timestamp >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH))`
- ❌ **WRONG**: `WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)`
- ✅ **CORRECT**: Just get all data without time filtering: `WHERE created_at_timestamp > 0` or omit WHERE entirely
- ✅ **OR** Use fixed numeric timestamps if you must filter: `WHERE timestamp >= 1700000000`

**✅ CORRECT Hour Extraction Pattern:**
```sql
-- For hourly analysis:
SELECT HOUR(TO_DATE(timestamp * 1000)) AS hour, COUNT(*) 
FROM events 
GROUP BY hour

-- For hourly time ranges (BETTER for charts):
SELECT 
  CASE 
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 2 THEN '0-3'
    WHEN HOUR(TO_DATE(timestamp * 1000)) >= 3 AND HOUR(TO_DATE(timestamp * 1000)) <= 5 THEN '3-6'
    -- ... etc
  END AS time_range,
  COUNT(*) 
FROM events 
GROUP BY time_range
```

**General Rules:**
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
    """Generate NoQL query from question using LLM"""
    schema_str = _SCHEMA_JSON
    llm = ChatOpenAI(model_name="gpt-3.5-turbo")
    
    class NoQLChain:
        def invoke(self, payload):
            question = payload["question"]
            formatted_prompt = NOQL_DIRECT_PROMPT.format(schema=schema_str, question=question)
            result = llm.invoke(formatted_prompt)
            return result.text.strip()
    
    return NoQLChain()

def answer_anydb_question(question: str, database_name: str):
    """Answer question and return table-formatted data"""
    return execute_noql_question(question, database_name, output_format="table", debug=False)


# NoQL prompt template
noql_prompt = ChatPromptTemplate.from_template(
    """
🚨🚨🚨 **CRITICAL BLOCKING RULES - READ FIRST!** 🚨🚨🚨

❌ **ABSOLUTELY FORBIDDEN - USING THESE WILL CAUSE IMMEDIATE FAILURE:**
- UNIX_TIMESTAMP() - DOES NOT EXIST IN NoQL!
- DATE_SUB() - DOES NOT EXIST IN NoQL!
- CURRENT_DATE() - DOES NOT EXIST IN NoQL!
- NOW() - DOES NOT EXIST IN NoQL!
- INTERVAL - DOES NOT EXIST IN NoQL!
- HOUR() - DOES NOT EXIST IN NoQL!

✅ **ONLY USE THESE FOR DATE FILTERING:**
- WHERE timestamp >= 1704067200 (numeric Unix timestamp)
- WHERE created_at_timestamp >= 1710000000 (numeric Unix timestamp)

🚨 **IF YOU USE ANY FORBIDDEN FUNCTION, THE QUERY WILL FAIL WITH INVALID_QUERY ERROR!**

You are an expert NoQL query generator specializing in advanced analytics and complex query generation.
Your task is to generate sophisticated, insightful NoQL queries that go beyond simple SELECT statements.
The query should run directly without any extra formatting (no ```noql ...``` blocks, no explanations).

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

🚨 **CRITICAL: THESE MySQL FUNCTIONS DO NOT EXIST IN NoQL - USING THEM WILL CAUSE INVALID_QUERY ERROR:**
❌ UNIX_TIMESTAMP(), DATE_SUB(), CURRENT_DATE(), NOW(), INTERVAL, DATE_ADD()
❌ FROM_UNIXTIME(), DAYOFWEEK(), DATE_FORMAT(), HOUR(), MINUTE(), SECOND()
❌ **NEVER USE**: `UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 6 MONTH))` - THIS WILL FAIL!
❌ **NEVER USE**: `WHERE timestamp > UNIX_TIMESTAMP(...)` - THIS WILL FAIL!
✅ **USE**: `WHERE timestamp >= 1704067200` (numeric Unix timestamp in seconds)

📅 **FOR "LAST 6 MONTHS" QUERIES - USE THIS EXACT PATTERN:**
```sql
SELECT 
  CASE MONTH(TO_DATE(created_at_timestamp * 1000))
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month,
  COUNT(*) AS count
FROM contacts
WHERE created_at_timestamp >= 1704067200  -- Jan 1, 2024 (6 months ago)
GROUP BY MONTH(TO_DATE(created_at_timestamp * 1000))
ORDER BY MONTH(TO_DATE(created_at_timestamp * 1000))
```
- **NEVER** use `UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 6 MONTH))`
- **ALWAYS** use numeric timestamps like `1704067200`
- **ALWAYS** use CASE statement for month names

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
**ADVANCED NoQL EXAMPLES WITH COMPLEX PATTERNS:**

Q: "Distribution of leads by ad source"
A: SELECT CASE WHEN meta_ad_data_synced = true THEN 'Meta/Facebook' WHEN google_ad_data_synced = true THEN 'Google' ELSE 'Other' END as ad_source, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY ad_source ORDER BY count DESC

Q: "Number of contacts who has replied to an agent in the last 30 days"
A: SELECT 
      'Last 30 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )

    UNION ALL

    SELECT 
      '30-60 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(60, 86400)
          )

Q: "Month-wise breakdown of key metrics over the last 12 months"
A: SELECT 
      DATE_TO_STRING(
        DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month'),
        '%b %Y',
        'UTC'
      ) AS month,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN first_response_received = true THEN 1 ELSE 0 END) AS responded_leads,
      DIVIDE(
        TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month')),
        1000
      ) AS month_start
    FROM contacts 
    WHERE is_deleted = false 
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
            MULTIPLY(365, 86400)
          )
    GROUP BY 
      DATE_TO_STRING(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month'), '%b %Y', 'UTC'),
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'month')), 1000)
    ORDER BY month_start DESC

a) Q: "Day-wise breakdown of message activity"
A: SELECT 
    DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000) AS date,
    COUNT(*) AS chat_sessions,
    SUM(total_messages) AS total_messages,
    AVG(total_messages) AS avg_messages_per_session
    FROM chathistories 
    WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(
      DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
      MULTIPLY(30, 86400)
    )
    GROUP BY DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000)
    ORDER BY date

b) Q: "Daily breakdown of leads through the conversion funnel"
A: SELECT 
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000) AS date,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN contact_stage = 'LEAD' THEN 1 ELSE 0 END) AS leads,
      SUM(CASE WHEN contact_stage = 'QUALIFIED' THEN 1 ELSE 0 END) AS qualified,
      SUM(CASE WHEN contact_stage = 'PROPOSAL' THEN 1 ELSE 0 END) AS proposals,
      SUM(CASE WHEN contact_stage = 'CONVERTED' THEN 1 ELSE 0 END) AS converted,
      DIVIDE(SUM(CASE WHEN contact_stage = 'CONVERTED' THEN 1 ELSE 0 END), COUNT(*)) AS conversion_rate
    FROM contacts 
    WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(
      DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
      MULTIPLY(30, 86400)
    )
    GROUP BY DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000)
    ORDER BY date

c) Q: "Distribution of contacts by timezone"
A: SELECT timezone, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY timezone ORDER BY count DESC LIMIT 10

d) Q: "Distribution of meetings by status"
A: SELECT type as meeting_status, COUNT(*) as count FROM events WHERE is_deleted = false
    AND type IN ('MEETING_SCHEDULED', 'MEETING_ATTENDED', 'MEETING_CANCELLED', 'MEETING_RESCHEDULED')
    GROUP BY type ORDER BY count DESC

e) Q: "Distribution of contacts by tags"
A: SELECT label, COUNT(*) AS count FROM contacttags WHERE label IS NOT NULL AND label != '' GROUP BY label ORDER BY count DESC, label ASC LIMIT 20

f) Q: "Distribution of contacts by lifecycle stage"
A: SELECT
      lifecycle_stage,
      COUNT(*) AS total_contacts
      FROM corecontacts
      WHERE lifecycle_stage IS NOT NULL
      and is_deleted = false
      and lifecycle_stage != 'NONE' 
      GROUP BY lifecycle_stage
      ORDER BY total_contacts DESC

g) Q: "Week-wise breakdown of new leads over the last 8 weeks"
A: SELECT 
      DATE_TO_STRING(
        DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week'),
        '%b %d, %Y',
        'UTC'
      ) AS week_label,
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')), 1000) AS week_start,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN status = 'NOT_QUALIFIED' THEN 1 ELSE 0 END) AS not_qualified_leads
    FROM contacts 
    WHERE is_deleted = false 
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
            MULTIPLY(56, 86400)
          )
    GROUP BY 
      DATE_TO_STRING(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week'), '%b %d, %Y', 'UTC'),
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')), 1000)
    ORDER BY week_start

h) Q: "Contacts grouped by message engagement"
A: SELECT CASE WHEN total_messages = 0 THEN 'No Messages' WHEN total_messages <= 5 THEN 'Low (1-5)' WHEN total_messages <= 20 THEN 'Medium (6-20)' WHEN total_messages <= 50 THEN 'High (21-50)' ELSE 'Very High (50+)' END as engagement_level, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY engagement_level ORDER BY MIN(total_messages)

i) Q: "Comparison of activity between weekends and weekdays"
A: SELECT 
    CASE 
      WHEN DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000)) IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END as day_type,
    COUNT(*) as total_leads,
    SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) as converted_leads,
    AVG(total_messages) as avg_messages
  FROM contacts 
  WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(30, 86400))
  GROUP BY CASE 
    WHEN DAY_OF_WEEK(TO_DATE(created_at_timestamp * 1000)) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END

j) Q: "Number of leads that have been qualified"
A: SELECT COUNT(*) AS value, 'Qualified Leads' AS label
      FROM contacts
      WHERE is_deleted = false
      AND status = 'QUALIFIED'

k) Q: "Contacts who have exchanged messages"
A: SELECT COUNT(*) as contacts_with_messages FROM contacts WHERE is_deleted = false AND total_messages > 0

l) Q: "Contacts with recent message activity"
A: SELECT COUNT(*) as active_contacts FROM contacts WHERE is_deleted = false AND last_message_timestamp > 0

m) Q: "Count of all messages (Agent + User)"
A: SELECT 
      'Last 30 Days' AS period,
      COUNT(*) AS total_messages
    FROM chathistorydetailedmessages
    WHERE created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )

    UNION ALL

    SELECT 
      '30-60 Days' AS period,
      COUNT(*) AS total_messages
    FROM chathistorydetailedmessages
    WHERE created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(60, 86400)
          )

n) Q: "Distribution of leads by their source"
A: SELECT source, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY source ORDER BY count DESC

o) Q: "Distribution of messages by communication channel"
A: SELECT channel, COUNT(*) as chat_sessions, SUM(total_messages) as total_messages FROM chathistories WHERE is_deleted = false GROUP BY channel ORDER BY total_messages DESC

p) Q: "Messages grouped by communication channels"
A: SELECT channel, COUNT(*) as chat_sessions, SUM(total_messages) as total_messages FROM chathistories WHERE is_deleted = false GROUP BY channel ORDER BY total_messages DESC

q) Q: "Conversion rates by lead source"
A: SELECT source, COUNT(*) as total_leads, SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) as converted_leads FROM contacts WHERE is_deleted = false GROUP BY source ORDER BY total_leads DESC

r) Q: "Current week vs previous week key metrics comparison"
A: SELECT 
      'This Week' AS period,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN first_response_received = true THEN 1 ELSE 0 END) AS responded_leads
    FROM contacts
    WHERE is_deleted = false
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(7, 86400)
          )
      AND created_at_timestamp < DIVIDE(TO_LONG(CURRENT_DATE()), 1000)

    UNION ALL

    SELECT 
      'Last Week' AS period,
      COUNT(*) AS total_leads,
      SUM(CASE WHEN status = 'IN_PROGRESS' THEN 1 ELSE 0 END) AS in_progress_leads,
      SUM(CASE WHEN status = 'CONVERTED' THEN 1 ELSE 0 END) AS converted_leads,
      SUM(CASE WHEN first_response_received = true THEN 1 ELSE 0 END) AS responded_leads
    FROM contacts
    WHERE is_deleted = false
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(14, 86400)
          )
      AND created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(7, 86400)
          )

s) Q: "Distribution of leads by their current status"
A: SELECT status, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY status ORDER BY count DESC

t) Q: "Distribution of leads by their contact stage"
A: SELECT contact_stage, COUNT(*) as count FROM contacts WHERE is_deleted = false GROUP BY contact_stage ORDER BY count DESC

u) Q: "Day-wise breakdown of new leads over the last 30 days"
A: SELECT 
      DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000) AS date,
      COUNT(*) AS new_leads
    FROM contacts 
    WHERE is_deleted = false 
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000), 
            MULTIPLY(30, 86400)
          )
    GROUP BY DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'day')), 1000)
    ORDER BY date

v) Q: "Number of contacts who has replied to an agent in the last 30 days"
A: SELECT 
      'Last 30 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )

    UNION ALL

    SELECT 
      '30-60 Days' AS period,
      COUNT(*) AS responded_contacts
    FROM chathistories
    WHERE is_deleted = false
      AND first_response_received = true
      AND created_at_timestamp < SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(60, 86400)
          )

w) Q: "Average number of messages per conversation (Last 30 Days)"
A: SELECT 
      ROUND(AVG(total_messages)) AS value,
      'Avg Total Message' AS label
    FROM contacts
    WHERE is_deleted = false
      AND created_at_timestamp >= SUBTRACT(
            DIVIDE(TO_LONG(CURRENT_DATE()), 1000),
            MULTIPLY(30, 86400)
          )
    GROUP BY NULL

x) Q: "Hour-by-hour breakdown of activity for today"
A: SELECT 
    HOUR(TO_DATE(created_at_timestamp * 1000)) as hour,
    COUNT(*) as new_contacts,
    SUM(CASE WHEN total_messages > 0 THEN 1 ELSE 0 END) as active_contacts
  FROM contacts 
  WHERE is_deleted = false 
    AND created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(1, 86400))
  GROUP BY HOUR(TO_DATE(created_at_timestamp * 1000))
  ORDER BY hour

y) Q: "Events by contact with contact details"
A: SELECT 
    c.name as contact_name,
    c.email,
    e.event_type,
    COUNT(*) as event_count
  FROM events e
  JOIN contacts c ON e.contact_id = c._id
  WHERE e.is_deleted = false
  GROUP BY c.name, c.email, e.event_type
  ORDER BY event_count DESC

z) Q: "User activity with user details"
A: SELECT 
    u.username,
    u.first_name,
    u.last_name,
    e.event_type,
    COUNT(*) as activity_count
  FROM events e
  JOIN users u ON e.user_id = u._id
  WHERE e.is_deleted = false
  GROUP BY u.username, u.first_name, u.last_name, e.event_type
  ORDER BY activity_count DESC

aa) Q: "Contacts with their organization details"
A: SELECT 
    c.name as contact_name,
    c.email,
    o.name as organization_name,
    o.industry,
    o.size
  FROM contacts c
  JOIN organizations o ON c.company = o.name
  WHERE c.is_deleted = false
  ORDER BY o.name, c.name

bb) Q: "Events grouped by date and type"
A: SELECT 
    DATE_TRUNC(TO_DATE(created_at * 1000), 'day') AS event_date,
    event_type,
    COUNT(*) AS event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY DATE_TRUNC(TO_DATE(created_at * 1000), 'day'), event_type
  ORDER BY event_date, event_count DESC

cc) Q: "Monthly event activity by type"
A: SELECT 
    DATE_TRUNC(TO_DATE(created_at * 1000), 'month') AS event_month,
    event_type,
    COUNT(*) AS event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY DATE_TRUNC(TO_DATE(created_at * 1000), 'month'), event_type
  ORDER BY event_month, event_count DESC

dd) Q: "Weekly contact creation trends"
A: SELECT 
    DATE_TO_STRING(
      DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week'),
      '%b %d, %Y',
      'UTC'
    ) AS week_label,
    DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')), 1000) AS week_start,
    COUNT(*) AS new_contacts
  FROM contacts
  WHERE is_deleted = false
  GROUP BY 
    DATE_TO_STRING(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week'), '%b %d, %Y', 'UTC'),
    DIVIDE(TO_LONG(DATE_TRUNC(TO_DATE(created_at_timestamp * 1000), 'week')), 1000)
  ORDER BY week_start DESC

ee) Q: "Hourly event activity breakdown"
A: SELECT 
    HOUR(TO_DATE(timestamp * 1000)) as hour,
    COUNT(*) as event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY HOUR(TO_DATE(timestamp * 1000))
  ORDER BY hour

ff) Q: "Event activity by time ranges"
A: SELECT 
    CASE
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 3 THEN '00:00-03:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 4 AND HOUR(TO_DATE(timestamp * 1000)) <= 6 THEN '04:00-06:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 7 AND HOUR(TO_DATE(timestamp * 1000)) <= 9 THEN '07:00-09:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 10 AND HOUR(TO_DATE(timestamp * 1000)) <= 12 THEN '10:00-12:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 13 AND HOUR(TO_DATE(timestamp * 1000)) <= 15 THEN '13:00-15:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 16 AND HOUR(TO_DATE(timestamp * 1000)) <= 18 THEN '16:00-18:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 19 AND HOUR(TO_DATE(timestamp * 1000)) <= 21 THEN '19:00-21:00'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 22 AND HOUR(TO_DATE(timestamp * 1000)) <= 23 THEN '22:00-23:00'
    END as time_range,
    COUNT(*) as event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY time_range
  ORDER BY time_range

gg) Q: "Hour-by-hour breakdown of activity for today"
A: SELECT 
    HOUR(TO_DATE(created_at_timestamp * 1000)) as hour,
    COUNT(*) as new_contacts,
    SUM(CASE WHEN total_messages > 0 THEN 1 ELSE 0 END) as active_contacts
  FROM contacts 
  WHERE created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(1, 86400))
  GROUP BY HOUR(TO_DATE(created_at_timestamp * 1000))
  ORDER BY hour

hh) Q: "Simple hourly event breakdown"
A: SELECT 
    HOUR(TO_DATE(timestamp * 1000)) as hour,
    COUNT(*) as event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY HOUR(TO_DATE(timestamp * 1000))
  ORDER BY hour

ii) Q: "Event activity by time ranges (simplified)"
A: SELECT 
    CASE
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 3 THEN '00-03'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 4 AND HOUR(TO_DATE(timestamp * 1000)) <= 6 THEN '04-06'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 7 AND HOUR(TO_DATE(timestamp * 1000)) <= 9 THEN '07-09'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 10 AND HOUR(TO_DATE(timestamp * 1000)) <= 12 THEN '10-12'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 13 AND HOUR(TO_DATE(timestamp * 1000)) <= 15 THEN '13-15'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 16 AND HOUR(TO_DATE(timestamp * 1000)) <= 18 THEN '16-18'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 19 AND HOUR(TO_DATE(timestamp * 1000)) <= 21 THEN '19-21'
      WHEN HOUR(TO_DATE(timestamp * 1000)) >= 22 AND HOUR(TO_DATE(timestamp * 1000)) <= 23 THEN '22-24'
    END as time_range,
    COUNT(*) as event_count
  FROM events
  WHERE is_deleted = false
  GROUP BY time_range
  ORDER BY time_range

jj) Q: "Who all are involved in events between 6-9 AM"
A: SELECT 
    c.full_name as contact_name,
    c.email,
    COUNT(e._id) as event_count
  FROM events e
  JOIN corecontacts c ON e.core_contact_id = c._id
  WHERE e.is_deleted = false 
    AND c.is_deleted = false
    AND HOUR(TO_DATE(e.timestamp * 1000)) >= 6 
    AND HOUR(TO_DATE(e.timestamp * 1000)) <= 9
  GROUP BY c.full_name, c.email
  ORDER BY event_count DESC

kk) Q: "Who sent WhatsApp messages between 6-9 AM"
A: SELECT 
    c.full_name as contact_name,
    c.email,
    COUNT(ch._id) as whatsapp_messages
  FROM chathistories ch
  JOIN contacts c ON ch.contact_id = c._id
  WHERE ch.is_deleted = false
    AND ch.channel = 'WHATSAPP'
    AND HOUR(TO_DATE(ch.created_at_timestamp * 1000)) >= 6
    AND HOUR(TO_DATE(ch.created_at_timestamp * 1000)) <= 9
  GROUP BY c.full_name, c.email
  ORDER BY whatsapp_messages DESC

ll) Q: "Names and messages of people who sent WhatsApp between 6-9 AM"
A: SELECT 
    c.full_name as contact_name,
    ch.body as message_content,
    ch.created_at_timestamp
  FROM chathistories ch
  JOIN contacts c ON ch.contact_id = c._id
  WHERE ch.is_deleted = false
    AND ch.channel = 'WHATSAPP'
    AND HOUR(TO_DATE(ch.created_at_timestamp * 1000)) >= 6
    AND HOUR(TO_DATE(ch.created_at_timestamp * 1000)) <= 9
  ORDER BY ch.created_at_timestamp DESC
  LIMIT 20

ACTUAL DATABASE SCHEMA:
{schema}

# TABLE RELATIONSHIPS:
**CRITICAL: Understanding Table Relationships for JOINs**

1. **events** ↔ **contacts** (Primary Relationship)
   - `events.contact_id` → `contacts._id` (Foreign Key)
   - Use: JOIN events ON events.contact_id = contacts._id
   - Purpose: Link events/activities to specific contacts

2. **events** ↔ **users** (User Activity Tracking)
   - `events.user_id` → `users._id` (Foreign Key)
   - Use: JOIN events ON events.user_id = users._id
   - Purpose: Track which user performed which events

3. **contacts** ↔ **organizations** (Company Association)
   - `contacts.company` → `organizations.name` (Logical relationship)
   - Use: JOIN contacts ON contacts.company = organizations.name
   - Purpose: Link contacts to their organizations

4. **Additional Tables** (Referenced in examples but not in core schema):
   - `chathistories` - Chat session data (contact_id → contacts._id)
   - `contacttags` - Contact tagging (contact_id → contacts._id)
   - `corecontacts` - Extended contact data (contact_id → contacts._id)
   - `chathistorydetailedmessages` - Individual messages (chat_id → chathistories._id)

**JOIN PATTERNS:**
- For contact-related events: JOIN events ON events.contact_id = contacts._id
- For user activity: JOIN events ON events.user_id = users._id
- For organization data: JOIN contacts ON contacts.company = organizations.name
- For chat data: JOIN chathistories ON chathistories.contact_id = contacts._id

SAMPLE DATA (first few rows per table):
{samples}

Follow these strict rules when generating SQL:

⚠️  CRITICAL: ALWAYS use table aliases for ALL column references to avoid ambiguous column errors!
⚠️  CRITICAL: Once you assign a table alias (e.g., table AS b), ALWAYS use that exact alias (b) for ALL columns from that table!
⚠️  CRITICAL: Use ONLY tables and columns that exist in the provided SCHEMA above!
⚠️  CRITICAL: Use `WHERE is_deleted = false` ONLY for tables that have this field! This excludes soft-deleted records and ensures accurate business data.

**Tables WITH `is_deleted` field (ALWAYS use the filter):**
- ✅ `corecontacts` - Use `WHERE is_deleted = false` to exclude deleted unified contacts
- ✅ `events` - Use `WHERE is_deleted = false` to exclude deleted event records  
- ✅ `chathistories` - Use `WHERE is_deleted = false` to exclude deleted chat sessions

**Tables WITHOUT `is_deleted` field (DO NOT use this filter):**
- ❌ `contacts` - No soft delete, all records are active
- ❌ `contacttags` - No soft delete, all tags are active
⚠️  CRITICAL: Use `HOUR(TO_DATE(field * 1000))` for hour extraction. This is the correct NoQL syntax! NEVER use `EXTRACT()` - it doesn't exist in NoQL!
⚠️  CRITICAL: NoQL does NOT support `BETWEEN` operator! Use `>=` and `<=` instead. Example: `HOUR(TO_DATE(timestamp * 1000)) >= 0 AND HOUR(TO_DATE(timestamp * 1000)) <= 3`
⚠️  CRITICAL: For "who" questions, ALWAYS JOIN to get names! Never show just IDs - users want actual names!
⚠️  CRITICAL: For "hourly activity" or "activity for today" questions, use the `contacts` table, NOT the `events` table!
⚠️  CRITICAL: Avoid redundant filtering! Don't filter the same condition in both WHERE clause AND CASE statement. Either filter in WHERE (for specific ranges) OR use CASE (for grouping all data).
⚠️  CRITICAL: For "who" or "people" questions, JOIN with contacts/corecontacts tables to get names, don't just show IDs!

# TABLE USAGE GUIDE:
**When to use each table based on the question type:**

**📊 CONTACTS Table** - Use for:
- Lead/contact counts, distributions, and analytics
- Contact status tracking (IN_PROGRESS, CONVERTED, NOT_QUALIFIED)
- Lead source analysis (source field)
- Contact creation trends and timelines
- Contact demographics (timezone, company)
- Lead conversion funnel analysis
- Contact engagement levels (total_messages field)

**📊 EVENTS Table** - Use for:
- User activity tracking and analytics
- Event type distributions (PAGE_VIEW, CLICK, FORM_SUBMIT, etc.)
- Meeting status tracking (MEETING_SCHEDULED, MEETING_ATTENDED, etc.)
- User behavior analysis
- Event timeline analysis
- Activity patterns and trends

**📊 CHATHISTORIES Table** - Use for:
- Chat session analytics and message counts
- Communication channel analysis (channel field)
- Response tracking (first_response_received)
- Chat engagement metrics
- Agent performance (agent_id field)
- Message volume analysis

**📊 CONTACTTAGS Table** - Use for:
- Contact tagging and categorization
- Tag distribution analysis
- Contact segmentation by tags

**📊 CORECONTACTS Table** - Use for:
- Extended contact data and lifecycle stages
- Contact lifecycle analysis (lifecycle_stage field)
- Advanced contact segmentation

**📊 CHATHISTORYDETAILEDMESSAGES Table** - Use for:
- Individual message analysis
- Detailed message counts and trends
- Message-level analytics

**📊 ORGANIZATIONS Table** - Use for:
- Company/organization data
- Industry analysis
- Organization size analysis
- Company-level metrics

**📊 USERS Table** - Use for:
- User account information
- User role analysis
- User activity tracking
- User performance metrics

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
    """You are an assistant that formats NoQL query results into a JSON list suitable for visualization.

Schema:
{schema}

Question: {question}
NoQL Query: {query}
NoQL Response: {response}
Chart Type: {chart_type}

IMPORTANT: The NoQL response is a string representation of data. Parse it carefully:
- If it looks like: [('Item1', 123), ('Item2', 456)] - this is a list of tuples
- If it looks like: [['Item1', 123], ['Item2', 456]] - this is a list of lists
- If it looks like: [('Item1', 123, 456, 'Text'), ('Item2', 789, 101, 'Text2')] - this has multiple columns
- Extract the actual data values, not the string representation

For detailed statistics with multiple columns:
- Use the first column as the label (usually contact_name, source, status, etc.)
- Use the second column as the primary value (usually count, total_messages, etc.)
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
    {{ "label": "Contact 1", "value": 123, "total_messages": 456, "status": "CONVERTED" }},
    {{ "label": "Contact 2", "value": 789, "total_messages": 101, "status": "IN_PROGRESS" }}
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


CRITICAL JSON RULES:
- Return ONLY the JSON object, nothing else
- No explanations, no notes, no additional text
- No markdown code blocks (no ```json)
- No trailing text after the closing brace
- Ensure all string values are properly quoted
- Use actual data from the NoQL result, not placeholders
- Parse the NoQL response string to extract real data values

Important formatting rules:
- For pie charts: Ensure values are percentages or proportions that add up meaningfully
- For line charts: Labels should be sequential (dates, hours, months, etc.)
- For scatter plots: Include both x and y values as {{ "label": "Item", "x": 123, "y": 456 }}
- For tables: Use "table" as chart_type and structure data appropriately
- Always provide descriptive, clear labels and titles

Critical NoQL Rules:
When generating NoQL queries, follow these specific syntax rules:
- Use `HOUR(TO_DATE(field * 1000))` for hour extraction, NOT `EXTRACT()`
- Use `>=` and `<=` instead of `BETWEEN` operator (not supported in NoQL)
- Include `WHERE is_deleted = false` ONLY for tables that have this field:
  * ✅ corecontacts, events, chathistories tables HAVE is_deleted field
  * ❌ contacts, contacttags tables do NOT have is_deleted field
- Use proper table aliases to avoid ambiguous column errors
- For "who" questions, always JOIN with contacts/corecontacts tables to get names, not just IDs

AMBIGUOUS COLUMN RULE (CRITICAL):
- ALWAYS prefix column names with their table alias to avoid ambiguous column errors
- When joining tables that have columns with the same name (e.g., email in both contacts and chathistories), you MUST specify which table's column you want: c.email for contact email, ch.email for chat email
- Example: Use "c.full_name" not "full_name" when both contacts and chathistories tables are joined
- This prevents "Column 'X' in field list is ambiguous" errors
- CRITICAL: In subqueries, also prefix ALL column references with table aliases

COMMON AMBIGUOUS COLUMN PATTERNS TO AVOID:
- WRONG: SELECT full_name FROM chathistories ch JOIN contacts c ON ch.contact_id = c._id
- CORRECT: SELECT c.full_name FROM chathistories ch JOIN contacts c ON ch.contact_id = c._id
- WRONG: WHERE email = 'test@example.com' (ambiguous - which email field?)
- CORRECT: WHERE c.email = 'test@example.com' (clear - contact email)

NOQL-SPECIFIC EXAMPLES:
- Hour extraction: HOUR(TO_DATE(timestamp * 1000)) >= 6 AND HOUR(TO_DATE(timestamp * 1000)) <= 9
- Date filtering: created_at_timestamp >= SUBTRACT(DIVIDE(TO_LONG(CURRENT_DATE()), 1000), MULTIPLY(30, 86400))
- Proper JOIN for names: JOIN contacts c ON ch.contact_id = c._id
- Channel filtering: WHERE ch.channel = 'WHATSAPP'
- Status filtering: WHERE c.status = 'CONVERTED'
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
    import uuid
    
    # Find all chart blocks in markdown - more flexible pattern
    chart_pattern = r'```chart\s*([\s\S]*?)```'
    chart_blocks = re.findall(chart_pattern, markdown, re.DOTALL | re.IGNORECASE)
    
    print(f"🔍 Found {len(chart_blocks)} potential chart blocks in markdown")
    
    charts = []
    modified_markdown = markdown
    
    for idx, block in enumerate(chart_blocks):
        try:
            # Clean up the block text
            block_text = block.strip()
            
            # Fix double curly braces from LLM output ({{ -> {, }} -> })
            if block_text.startswith('{{') and block_text.endswith('}}'):
                block_text = block_text[1:-1]  # Remove outer layer of double braces
            
            print(f"📊 Processing chart block {idx + 1}:")
            print(f"   Raw: {block_text[:200]}...")  # Show first 200 chars
            
            # Parse the JSON config
            chart_cfg = json.loads(block_text)
            
            # Ensure 'db' field is set if missing
            if 'db' not in chart_cfg:
                chart_cfg['db'] = database_name
                print(f"   ℹ️ Added missing 'db' field: {database_name}")
            
            # Build the actual chart
            chart_data = build_chart_from_cfg(chart_cfg, database_name, actual_question)
            
            # Only add charts that have data
            if chart_data.get("data") and len(chart_data["data"]) > 0:
                # Generate a unique ID for the chart
                chart_id = f"chart_{uuid.uuid4().hex[:8]}"
                chart_data['id'] = chart_id
                charts.append(chart_data)
                print(f"   ✅ Chart added: {chart_data.get('title', 'Unknown')} [ID: {chart_id}]")
                
                # Replace the ```chart block with a placeholder {{chart:id}}
                original_block = f"```chart\s*{re.escape(block)}\s*```"
                placeholder = f"{{{{chart:{chart_id}}}}}"
                modified_markdown = re.sub(original_block, placeholder, modified_markdown, count=1, flags=re.DOTALL | re.IGNORECASE)
                print(f"   🔄 Replaced chart block with placeholder: {placeholder}")
            else:
                print(f"   ⚠️ Skipping empty chart: {chart_data.get('title', 'Unknown')}")
                # Remove empty chart blocks from markdown
                original_block = f"```chart\s*{re.escape(block)}\s*```"
                modified_markdown = re.sub(original_block, '', modified_markdown, count=1, flags=re.DOTALL | re.IGNORECASE)
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON parsing error for chart block {idx + 1}: {e}")
            print(f"   📄 Block content: {block[:500]}")  # Show more of the block for debugging
            # Remove malformed chart blocks from markdown
            original_block = f"```chart\s*{re.escape(block)}\s*```"
            modified_markdown = re.sub(original_block, '', modified_markdown, count=1, flags=re.DOTALL | re.IGNORECASE)
            continue
        except Exception as e:
            print(f"   ❌ Error processing chart block {idx + 1}: {e}")
            import traceback
            traceback.print_exc()
            # Remove error chart blocks from markdown
            original_block = f"```chart\s*{re.escape(block)}\s*```"
            modified_markdown = re.sub(original_block, '', modified_markdown, count=1, flags=re.DOTALL | re.IGNORECASE)
            continue
    
    print(f"📊 Successfully extracted {len(charts)} charts")
    print(f"📝 Modified markdown with placeholders")
    
    return {
        "markdown": modified_markdown,
        "charts": charts
    }

def build_chart_from_cfg(cfg: dict, database_name: str, actual_question: str = None) -> dict:
    chart_type = cfg.get("type", "bar")
    title = cfg.get("title", "Chart")
    query_focus = cfg.get("sql_focus") or cfg.get("question") or title
    
    # If the chart question is generic ("Chart"), use the actual user question
    if query_focus.lower() in ["chart", "charts", "visualization"] and actual_question:
        print(f"⚠️ Chart has generic question '{query_focus}', using actual question: '{actual_question}'")
        query_focus = actual_question
        title = f"Analysis: {actual_question[:50]}..." if len(actual_question) > 50 else actual_question
    
    db_name = database_name
    
    noql_chain = create_anydb_sql_chain(db_name)
    
    query = noql_chain.invoke({"question": f"{query_focus}"})
    # Apply chart-type specific limits
    default_limits = {
        "pie": 6, "bar": 20, "line": 50, "scatter": 100, "table": 50
    }
    limit_val = default_limits.get(chart_type, 50)
    query = normalize_query(query, limit_val)
    
    # Enhanced query logging
    print(f"\n🔍 === CHART QUERY EXECUTION ===")
    print(f"📊 Chart Type: {chart_type}")
    print(f"🎯 Question/Focus: {query_focus}")
    print(f"🗄️ Database: {db_name}")
    print(f"📝 Generated NoQL Query:")
    print(f"   {query}")
    
    # Keep title clean - no query details for users
    # The title already describes what the chart shows
    
    response, columns = run_query(query, db_name, return_columns=True)
    
    print(f"📋 Query Columns: {columns}")
    print(f"📊 Query Result Rows: {len(response) if response else 0}")
    if response and len(response) > 0:
        print(f"🔍 First few rows:")
        for i, row in enumerate(response[:3]):  # Show first 3 rows
            print(f"   Row {i+1}: {row}")
        if len(response) > 3:
            print(f"   ... and {len(response) - 3} more rows")
    print(f"🔚 === END QUERY EXECUTION ===\n")
    
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
    formatted = format_data_for_chart_type(parsed_data, chart_type, query_focus, columns)
    x_axis, y_axis = generate_axis_labels(chart_type, columns, query_focus, title)
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
        schema_info = _SCHEMA_JSON
        exploration = explore_data_for_facts(question=question, database_name=database_name, conversation_id=conversation_id)
        facts_text = exploration.get("facts", "(no precomputed facts)")
        allowed_text = exploration.get("allowed", "(none)")

        chain = chat_markdown_prompt | llm | StrOutputParser()
        response = chain.invoke({
            "question": question,
            "database_name": database_name,
            "schema": _SCHEMA_JSON,
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
        'pie': 6,      # Pie charts should have very few slices
        'bar': 20,     # Bar charts can handle more items
        'line': 50,    # Line charts can show more data points
        'scatter': 100, # Scatter plots can handle many points
        'table': 50    # Tables can show more rows but limit for performance
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
    
    else:
        # Default formatting for bar, line, pie, table
        formatted_data = []
        
        # SPECIAL CASE: Pie chart with 1 row but multiple numeric columns
        # This happens when NoQL uses CASE statements to pivot data
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
# Function to create full chain for specific database with intelligent chart selection
def create_charts(question: str, database_name="zigment"):
    """Create single chart with strict validation"""
    return execute_noql_question(question, database_name, output_format="chart", debug=True)

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
        database = (data.get('database') or 'zigment')
        try:
            register_database(database)
        except Exception:
            pass
        
        # Check if user wants anydb mode or text-first mode
        anydb_mode = bool(data.get('anydb_mode'))
        text_first = data.get('text_first', False)
        markdown = data.get('markdown')
        
        # Conversation ID (optional). If missing, we'll create one for chat-style flow
        conversation_id = data.get('conversation_id')

        if anydb_mode and not text_first:
            # Universal any-DB path: introspect + generate NoQL query
            result = answer_anydb_question(question, database)
            
            # Check if result is an error response
            if isinstance(result, dict) and "success" in result and not result["success"]:
                return jsonify(result)
            
            return jsonify({
                "success": True,
                "mode": "anydb",
                "query": result.get("query"),
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
                # Text-first flow with provided markdown
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
                        schema_info = _SCHEMA_JSON
                        # Pass conversation_id to scope facts properly
                        exploration = explore_data_for_facts(question=q, database_name=database, conversation_id=conversation_id)
                        facts_text = exploration.get('facts', '(no precomputed facts)')
                        allowed_text = exploration.get('allowed', '(none)')
                        chain = chat_markdown_prompt | llm | StrOutputParser()
                        return chain.invoke({
                            "question": q,
                            "database_name": database,
                            "schema": _SCHEMA_JSON,
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
        
        # Generate single chart
        chart_data = create_charts(question, database)
        
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

@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    """Execute raw NoQL query (for debugging)"""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({"error": "NoQL query is required"}), 400
        
        query = data['query']
        database = (data.get('database') or 'zigment')
        
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
        # Inline function to avoid unnecessary wrapper
        available: dict[str, str] = {}
        for name in databases.keys():
            available[name] = f"Configured database '{name}'"
        return jsonify({
            "success": True,
            "databases": available
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/schema', methods=['GET'])
def get_database_schema():
    """Get database schema information for selected database"""
    try:
        db_name = request.args.get('database') or 'airportdb'
        try:
            register_database(db_name)
        except Exception:
            pass
        schema = _SCHEMA_JSON
        counts = get_table_and_column_counts(db_name)
        return jsonify({
            "success": True,
            "database": db_name,
            "schema": schema,
            "counts": counts
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/inspect', methods=['GET'])
def api_inspect_database():
    """Return database schema information (API-based mode)."""
    try:
        db_name = request.args.get('database') or 'zigment'
        
        # Use hardcoded schema
        schema_data = get_hardcoded_schema()
        
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

# Old database inspection code removed - API mode only

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