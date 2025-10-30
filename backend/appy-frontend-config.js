// Frontend configuration for Appy backend
// Add this to your frontend's API configuration

const APPY_API_BASE_URL = 'http://localhost:5001/api';

// Example usage in your frontend:
export const appyApi = {
  // Ask a question and get SQL results
  askQuestion: async (question, conversationId) => {
    const response = await fetch(`${APPY_API_BASE_URL}/ask`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        question: question,
        conversation_id: conversationId
      })
    });
    return response.json();
  },

  // Execute SQL directly
  executeSql: async (sql) => {
    const response = await fetch(`${APPY_API_BASE_URL}/execute-sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        sql: sql
      })
    });
    return response.json();
  },

  // Get schema information
  getSchema: async () => {
    const response = await fetch(`${APPY_API_BASE_URL}/schema`);
    return response.json();
  },

  // Get conversations
  getConversations: async () => {
    const response = await fetch(`${APPY_API_BASE_URL}/conversations`);
    return response.json();
  },

  // Get chat history
  getHistory: async (conversationId) => {
    const response = await fetch(`${APPY_API_BASE_URL}/history?conversation_id=${conversationId}`);
    return response.json();
  }
};

// Example React component usage:
/*
import { appyApi } from './appy-config';

const AppyChat = () => {
  const [question, setQuestion] = useState('');
  const [response, setResponse] = useState('');
  const [conversationId] = useState(() => Math.random().toString(36).substr(2, 9));

  const handleAsk = async () => {
    try {
      const result = await appyApi.askQuestion(question, conversationId);
      setResponse(result.response);
    } catch (error) {
      console.error('Error:', error);
    }
  };

  return (
    <div>
      <input 
        value={question} 
        onChange={(e) => setQuestion(e.target.value)}
        placeholder="Ask a question..."
      />
      <button onClick={handleAsk}>Ask</button>
      <pre>{response}</pre>
    </div>
  );
};
*/
