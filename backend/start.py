#!/usr/bin/env python3
"""
Startup script for the LangChain Database Analytics Backend
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Check if OpenAI API key is set
if not os.getenv("OPENAI_API_KEY"):
    print("❌ Error: OPENAI_API_KEY environment variable is not set!")
    print("Please create a .env file with your OpenAI API key:")
    print("OPENAI_API_KEY=your_api_key_here")
    sys.exit(1)

# Import and run the Flask app
from app import app

if __name__ == '__main__':
    print("🚀 Starting LangChain Database Analytics Backend...")
    print("📍 Server will be available at: http://localhost:5000")
    print("💡 Use Ctrl+C to stop the server")
    print("-" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
