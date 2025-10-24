'use client'

import { useState, useEffect } from 'react'
import axios from 'axios'
import ReactMarkdown from 'react-markdown'

const API_BASE = 'http://localhost:1000'
import ChartComponent from './components/ChartComponent'

interface ChartData {
  title: string
  x_axis: string
  y_axis: string
  chart_type?: string
  data: { label: string; value: number; x?: number; y?: number; [key: string]: any }[]
}

interface Narrative {
  introduction: string
  transitions: string[]
  insights: string[]
  conclusion: string
}

interface ChartWithId {
  id: string
  title: string
  x_axis: string
  y_axis: string
  chart_type: string
  data: { label: string; value: number; x?: number; y?: number; [key: string]: any }[]
}

interface Conversation { id: string; title: string; created_at: string; updated_at: string }
interface Message { id: string; role: 'user' | 'assistant'; content_markdown: string; charts: ChartWithId[]; facts?: string; created_at: string }

interface ApiResponse {
  success: boolean
  data?: ChartData | ChartData[]
  narrative?: Narrative
  question: string
  database?: string
  error?: string
  // New chat-style response fields
  mode?: string
  markdown?: string
  charts?: ChartWithId[]
  conversation_id?: string
}

interface Database {
  [key: string]: string
}

interface DatabaseResponse {
  success: boolean
  databases: Database
}

export default function Home() {
  const [question, setQuestion] = useState('')
  const [chartData, setChartData] = useState<ChartData | ChartData[] | null>(null)
  const [narrative, setNarrative] = useState<Narrative | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedDatabase, setSelectedDatabase] = useState('zigment')
  const [databases, setDatabases] = useState<Database>({})
  const [loadingDatabases, setLoadingDatabases] = useState(true)
  
  // New state for chat-style mode
  const [chatMode, setChatMode] = useState(true) // Default to ChatGPT-style
  const [markdown, setMarkdown] = useState<string>('')
  const [charts, setCharts] = useState<ChartWithId[]>([])
  const [responseMode, setResponseMode] = useState<string>('')
  const [multipleCharts, setMultipleCharts] = useState(true)
  const [dbCheckName, setDbCheckName] = useState('')
  const [checkingDb, setCheckingDb] = useState(false)
  const [dbStatus, setDbStatus] = useState<{state: 'idle'|'ok'|'missing'|'error', message?: string}>({ state: 'idle' })
  const [debugOpen, setDebugOpen] = useState(false)
  const [lastReq, setLastReq] = useState<any>(null)
  const [lastRes, setLastRes] = useState<any>(null)

  const [conversations, setConversations] = useState<Conversation[]>([])
  const [selectedConversationId, setSelectedConversationId] = useState<string | null>(null)
  const [messages, setMessages] = useState<Message[]>([])
  const [loadingHistory, setLoadingHistory] = useState(false)

  const loadConversations = async () => {
    try {
      const res = await axios.get(`${API_BASE}/api/conversations`)
      if (res.data?.success) setConversations(res.data.conversations || [])
    } catch (e) {
      // ignore for now
    }
  }

  const loadHistory = async (conversationId: string) => {
    setLoadingHistory(true)
    try {
      const res = await axios.get(`${API_BASE}/api/history`, { params: { conversation_id: conversationId } })
      if (res.data?.success) setMessages(res.data.messages || [])
    } catch (e) {
      setMessages([])
    } finally {
      setLoadingHistory(false)
    }
  }

  const handleNewConversation = async () => {
    try {
      const res = await axios.post(`${API_BASE}/api/conversations`, { title: 'New conversation' })
      if (res.data?.success) {
        const cid = res.data.conversation_id as string
        await loadConversations()
        setSelectedConversationId(cid)
        await loadHistory(cid)
        setQuestion('')
      }
    } catch (e) {
      // ignore
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!question.trim()) return

    setLoading(true)
    setError(null)
    setChartData(null)
    setNarrative(null)
    setMarkdown('')
    setCharts([])
    setResponseMode('')
    setDbStatus({ state: 'idle' })

    try {
      const requestBody: any = {
        question: question.trim(),
        database: selectedDatabase,
      }

      if (chatMode) {
        // ChatGPT-style mode
        requestBody.text_first = true
      } else {
        // Original chart-first mode
        requestBody.multiple_charts = multipleCharts
      }

      if (selectedConversationId) {
        (requestBody as any).conversation_id = selectedConversationId
      }
      // Route through universal any-DB flow
      if (selectedDatabase && selectedDatabase !== 'zigment') {
        requestBody.anydb_mode = true
      }
      setLastReq(requestBody)
      console.log('[ASK] request', requestBody)
      const response = await axios.post<ApiResponse>(`${API_BASE}/api/ask`, requestBody)

      if (response.data.success) {
        setLastRes(response.data)
        console.log('[ASK] response', response.data)
        setResponseMode(response.data.mode || (response.data.markdown ? 'chat_style' : 'legacy'))
        
        const cid = response.data.conversation_id || selectedConversationId
        if (cid && !selectedConversationId) setSelectedConversationId(cid)
        if (cid) {
          await loadConversations()
          await loadHistory(cid)
        }

        if (response.data.mode === 'chat_style' || response.data.mode === 'text_first_provided') {
          // Quick preview; history will render full
          setMarkdown(response.data.markdown || '')
          setCharts(response.data.charts || [])
        } else if (response.data.data) {
          // Handle legacy chart-first response
          setChartData(response.data.data || null)
          if (response.data.narrative) {
            setNarrative(response.data.narrative)
          }
        } else if ((response.data as any).charts) {
          // Some backends return charts instead of data
          setChartData((response.data as any).charts)
        }
      } else {
        setError(response.data.error || 'Unknown error occurred')
      }
    } catch (err: any) {
      setError(err.response?.data?.error || 'Failed to connect to the backend')
    } finally {
      setLoading(false)
    }
  }

  const clearResults = () => {
    setChartData(null)
    setError(null)
    setQuestion('')
    setMarkdown('')
    setCharts([])
    setNarrative(null)
    setResponseMode('')
    setDbStatus({ state: 'idle' })
  }

  const handleCheckDatabase = async () => {
    setCheckingDb(true)
    setDbStatus({ state: 'idle' })
    try {
      const name = dbCheckName.trim()
      if (!name) {
        setDbStatus({ state: 'error', message: 'Enter a database name' })
        return
      }
      const isUri = name.includes('://')
      const payload: any = isUri ? { uri: name, name: selectedDatabase || 'zigment' } : { name }
      const res = await axios.post(`${API_BASE}/api/check-database`, payload)
      if (res.data?.success && res.data?.available) {
        setDbStatus({ state: 'ok' })
      } else if (res.data?.success) {
        setDbStatus({ state: 'missing' })
      } else {
        setDbStatus({ state: 'error', message: res.data?.error || 'Unknown error' })
      }
    } catch (e: any) {
      setDbStatus({ state: 'error', message: e.response?.data?.error || 'Failed to reach backend' })
    } finally {
      setCheckingDb(false)
    }
  }

  // Function to render markdown with embedded charts
  const renderMarkdownWithCharts = (markdownText: string, perMessageCharts?: ChartWithId[]) => {
    const parts = markdownText.split(/(\{\{chart:[^}]+\}\})/g)
    
    return parts.map((part, index) => {
      const chartMatch = part.match(/\{\{chart:([^}]+)\}\}/)
      if (chartMatch) {
        const chartId = chartMatch[1]
        const sourceCharts = perMessageCharts || charts
        const chart = sourceCharts.find(c => c.id === chartId)
        if (chart) {
          return (
            <div key={index} className="my-8">
              <ChartComponent data={[chart]} />
            </div>
          )
        }
        return <div key={index} className="text-gray-500 italic">Chart {chartId} not found</div>
      }
      
      // Render proper markdown with better styling
      if (part.trim()) {
        return (
          <div key={index} className="prose prose-invert max-w-none prose-lg">
            <ReactMarkdown 
              components={{
                h1: ({children}) => <h1 className="text-3xl font-bold text-white mb-6 mt-8">{children}</h1>,
                h2: ({children}) => <h2 className="text-2xl font-semibold text-white mb-4 mt-6">{children}</h2>,
                h3: ({children}) => <h3 className="text-xl font-semibold text-white mb-3 mt-5">{children}</h3>,
                p: ({children}) => <p className="text-gray-100 mb-4 leading-relaxed text-base">{children}</p>,
                strong: ({children}) => <strong className="text-white font-semibold">{children}</strong>,
                em: ({children}) => <em className="text-gray-200 italic">{children}</em>,
                ul: ({children}) => <ul className="list-disc list-inside mb-4 text-gray-100 space-y-1">{children}</ul>,
                ol: ({children}) => <ol className="list-decimal list-inside mb-4 text-gray-100 space-y-1">{children}</ol>,
                li: ({children}) => <li className="text-gray-100">{children}</li>,
                blockquote: ({children}) => <blockquote className="border-l-4 border-blue-500 pl-4 italic text-gray-200 my-4">{children}</blockquote>,
                code: ({children}) => <code className="bg-gray-700 text-blue-300 px-2 py-1 rounded text-sm font-mono">{children}</code>,
                pre: ({children}) => <pre className="bg-gray-800 border border-gray-600 rounded-lg p-4 overflow-x-auto mb-4">{children}</pre>
              }}
            >
              {part}
            </ReactMarkdown>
          </div>
        )
      }
      return null
    }).filter(Boolean)
  }

  // Load available databases on component mount
  useEffect(() => {
    const loadDatabases = async () => {
      try {
        const response = await axios.get<DatabaseResponse>(`${API_BASE}/api/databases`)
        if (response.data.success) {
          setDatabases(response.data.databases)
        }
      } catch (err) {
        console.error('Failed to load databases:', err)
      } finally {
        setLoadingDatabases(false)
      }
    }
    
    loadDatabases()
    loadConversations()
  }, [])

  // Sample questions for different databases
  const sampleQuestions = {
    zigment: [
      // Contacts & Engagement
      "Show me the total number of contacts by status",
      "Top 10 contacts by number of events tracked",
      "Distribution of contacts across different contact stages",

      // Events & Analytics
      "Most common event types in the last 30 days",
      "Event categories breakdown with count distribution",
      "Show events by channel (email, SMS, WhatsApp, etc.)",

      // Communication Insights
      "Average number of messages per chat history",
      "Chat histories by channel type",
      "Contacts with the most active chat engagement",

      // Organizations
      "Number of contacts per organization",
      "Organizations with the highest contact engagement",
      "Distribution of contacts across org agents",

      // Tags & Categorization
      "Most commonly used contact tags",
      "Contacts grouped by tag categories",
      "Tag distribution across different contact stages",

      // Time-based Analysis
      "New contacts created per month (last 6 months)",
      "Contact creation trends by week",
      "Events triggered per day of the week"
    ],
    world: [
      // Population & Demographics
      "Top 10 most populated countries",
      "Most populated cities by country",
      "Countries with the highest population density",
      "Cities with population over 1 million",
      
      // Geography & Regions
      "Countries by continent - show distribution",
      "Largest countries by surface area",
      "Countries in Europe with their capitals",
      "Island nations and their populations",
      
      // Languages & Culture
      "Most spoken languages worldwide",
      "Countries with the most official languages",
      "Languages spoken in Asia",
      "Countries where English is official",
      
      // Comparisons & Analysis
      "Population growth by continent",
      "Urban vs rural population by region",
      "Countries with similar population sizes",
      "Language diversity by country"
    ],
    chinook: [
      // Music & Artists
      "Top selling artists by total sales",
      "Most popular music genres",
      "Albums with the most tracks",
      "Artists with the most albums",
      
      // Sales & Revenue
      "Top customers by total purchases",
      "Sales by country - revenue breakdown",
      "Monthly sales trends over time",
      "Best selling tracks of all time",
      
      // Customer Analysis
      "Customer demographics by country",
      "Average purchase amount per customer",
      "Most active customers by number of orders",
      "Customer lifetime value analysis",
      
      // Business Intelligence
      "Revenue by music genre",
      "Peak sales periods throughout the year",
      "Employee performance by sales territory",
      "Media type preferences (CD, MP3, etc.)"
    ],
    imdb: [
      // Movies & Ratings
      "Highest rated movies of all time",
      "Most popular movies by vote count",
      "Movies by release year - trends over time",
      "Longest and shortest movies",
      
      // Genres & Categories
      "Most popular movie genres",
      "Genre trends over the decades",
      "Action movies with highest ratings",
      "Comedy movies from the 1990s",
      
      // Industry Analysis
      "Most prolific directors by movie count",
      "Actors who appeared in the most movies",
      "Movies with the largest cast",
      "International vs domestic movie distribution",
      
      // Performance Metrics
      "Movies with the most user reviews",
      "Rating distribution across all movies",
      "Movies that improved in rating over time",
      "Comparison of critic vs audience scores"
    ]
  }

  const currentSamples: string[] = ((sampleQuestions as any)[selectedDatabase] || sampleQuestions.zigment) as string[]

  const handleSelectConversation = async (cid: string) => {
    setSelectedConversationId(cid)
    await loadHistory(cid)
    setQuestion('')
  }

  const handleDeleteConversation = async (cid: string) => {
    try {
      await axios.delete(`${API_BASE}/api/conversations/${cid}`)
      await loadConversations()
      if (selectedConversationId === cid) {
        setSelectedConversationId(null)
        setMessages([])
      }
    } catch (e) {
      // ignore
    }
  }

  return (
    <div className="h-screen overflow-hidden bg-gray-900 flex">
      {/* Sidebar */}
      <div className="w-72 h-full bg-gray-800/80 backdrop-blur border-r border-gray-700/70 flex flex-col">
        <div className="p-4 border-b border-gray-700/70">
          <button
            onClick={handleNewConversation}
            className="w-full px-3 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm shadow"
          >
            + New chat
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {conversations.map((c) => (
            <div
              key={c.id}
              className={`group rounded px-3 py-2 text-sm cursor-pointer flex items-center justify-between transition ${selectedConversationId === c.id ? 'bg-gray-700 text-white' : 'bg-transparent text-gray-300 hover:bg-gray-700/50'}`}
              onClick={() => handleSelectConversation(c.id)}
            >
              <span className="truncate mr-2" title={c.title}>{c.title || 'Untitled'}</span>
              <button
                className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-red-400"
                onClick={(e) => { e.stopPropagation(); handleDeleteConversation(c.id) }}
                title="Delete"
              >
                ×
              </button>
            </div>
          ))}
          {conversations.length === 0 && (
            <div className="text-gray-500 text-sm px-3 py-2">No conversations yet</div>
          )}
        </div>
      </div>

      {/* Main area */}
      <div className="flex-1 h-full flex flex-col">
        {/* Header with gradient and controls */}
        <div className="bg-gradient-to-r from-blue-600/20 via-indigo-600/20 to-purple-600/20 border-b border-gray-700 px-6 py-4">
          <div className="max-w-6xl mx-auto flex items-center justify-between gap-4">
            <div>
              <h1 className="text-2xl font-semibold text-white tracking-tight">
            Database Analytics Assistant
          </h1>
              <p className="text-sm text-gray-300 mt-1">
                Ask questions about your data. Get insights and charts instantly.
              </p>
            </div>
            <div className="flex items-center gap-3">
              {/* Database Selection */}
              <div className="flex items-center gap-2">
                <label htmlFor="database" className="text-xs font-medium text-gray-300 whitespace-nowrap">
                  Database
                </label>
                {loadingDatabases ? (
                  <div className="flex items-center space-x-2">
                    <div className="w-4 h-4 border-2 border-gray-600 border-t-gray-300 rounded-full animate-spin"></div>
                    <span className="text-gray-400 text-xs">Loading...</span>
                  </div>
                ) : (
                  <select
                    id="database"
                    value={selectedDatabase}
                    onChange={(e) => setSelectedDatabase(e.target.value)}
                    className="px-3 py-2 border border-gray-600 rounded-md text-xs focus:ring-2 focus:ring-blue-500 focus:border-blue-500 bg-gray-800 text-gray-200"
                    disabled={loading}
                  >
                    {Object.entries(databases).map(([key]) => (
                      <option key={key} value={key}>
                        {key.charAt(0).toUpperCase() + key.slice(1)}
                      </option>
                    ))}
                  </select>
                )}
              </div>
              {/* Add DB (uses text field value as URI if contains ://, otherwise as name) */}
              <button
                type="button"
                onClick={async () => {
                  const text = dbCheckName.trim()
                  if (!text) { alert('Enter a database name or URI in the textbox'); return }
                  const isUri = text.includes('://')
                  const payload:any = isUri ? { uri: text } : { name: text }
                  try {
                    const res = await axios.post(`${API_BASE}/api/register-database`, payload)
                    // refresh list and select new database
                    const response = await axios.get(`${API_BASE}/api/databases`)
                    if (response.data?.success) {
                      setDatabases(response.data.databases || {})
                      const newName = res.data?.name || (isUri ? text.split('/').pop() : text)
                      if (newName) setSelectedDatabase(newName)
                    }
                  } catch (e:any) {
                    alert(e?.response?.data?.error || 'Failed to register database')
                  }
                }}
                className="px-3 py-2 bg-green-600 hover:bg-green-700 text-white rounded text-xs"
                title="Add this database to the dropdown if it exists"
              >
                Add
              </button>
              {/* Inspect DB */}
              <button
                type="button"
                onClick={async () => {
                  try {
                    const res = await axios.get(`${API_BASE}/api/inspect`, { params: { database: selectedDatabase } })
                    if (res.data?.success) {
                      console.log('DB preview', res.data.preview)
                      alert('Inspection complete: check console for preview of first 5 rows per table')
                    } else {
                      alert(res.data?.error || 'Inspect failed')
                    }
                  } catch (e:any) {
                    alert(e?.response?.data?.error || 'Inspect failed')
                  }
                }}
                className="px-3 py-2 bg-gray-700 hover:bg-gray-600 text-gray-200 rounded text-xs"
                title="Preview first 5 rows of all tables"
              >
                Inspect
              </button>
              {/* Mode toggle */}
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="chatMode"
                  checked={chatMode}
                  onChange={(e) => setChatMode(e.target.checked)}
                  className="w-4 h-4 text-blue-600 bg-gray-800 border-gray-600 rounded focus:ring-blue-500 focus:ring-2"
                  disabled={loading}
                />
                <label htmlFor="chatMode" className="text-xs text-gray-300 whitespace-nowrap">
                  Chat style
                </label>
              </div>
              {/* Check DB controls moved to header */}
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  value={dbCheckName}
                  onChange={(e) => setDbCheckName(e.target.value)}
                  placeholder="Database name or URI"
                  className="px-3 py-2 w-64 bg-gray-800 text-gray-100 placeholder-gray-400 border border-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-xs"
                />
                <button
                  type="button"
                  onClick={handleCheckDatabase}
                  disabled={checkingDb || loading}
                  className="px-3 py-2 bg-gray-800 text-gray-300 rounded-lg hover:bg-gray-700 border border-gray-700 text-xs flex items-center gap-2"
                  title="Check if database exists on server"
                >
                  {checkingDb ? (
                    <div className="w-4 h-4 border-2 border-gray-400 border-t-transparent rounded-full animate-spin"></div>
                  ) : (
                    <span>Check DB</span>
                  )}
                </button>
                {dbStatus.state !== 'idle' && (
                  <span className={`text-xs ${dbStatus.state==='ok' ? 'text-green-400' : dbStatus.state==='missing' ? 'text-yellow-400' : 'text-red-400'}`}>
                    {dbStatus.state==='ok' ? 'Available' : dbStatus.state==='missing' ? 'Not found' : 'Error'}
                  </span>
                )}
              </div>
            </div>
          </div>
        </div>

      {/* Chat Messages Area */}
      <div className="flex-1 overflow-y-auto px-6 py-6">
        <div className="max-w-4xl mx-auto space-y-4">
          {/* Render history if a conversation is selected */}
          {selectedConversationId && messages.map((m) => (
            m.role === 'user' ? (
              <div key={m.id} className="flex justify-end">
                <div className="max-w-2xl">
                  <div className="bg-blue-600 text-white rounded-lg px-4 py-3 shadow-lg">
                    <p className="text-sm leading-relaxed">{m.content_markdown}</p>
                  </div>
                </div>
              </div>
            ) : (
              <div key={m.id} className="flex justify-start">
                <div className="max-w-full w-full">
                  <div className="bg-gray-800 rounded-lg px-8 py-6 shadow-lg border border-gray-700">
                    <div className="text-gray-100 space-y-2">
                      {renderMarkdownWithCharts(m.content_markdown, m.charts as any)}
                      {/* Facts are used internally for LLM context, not displayed to user */}
                    </div>
                  </div>
                </div>
              </div>
            )
          ))}

          {/* Fallback one-off preview when no conversation yet */}
          {!selectedConversationId && responseMode === 'chat_style' && markdown && (
            <div className="flex justify-start">
              <div className="max-w-full w-full">
                <div className="bg-gray-800 rounded-lg px-8 py-6 shadow-lg border border-gray-700">
                  <div className="text-gray-100 space-y-2">
                    {renderMarkdownWithCharts(markdown)}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* AI Response - Legacy Chart Style */}
          {chartData && responseMode !== 'chat_style' && (
            <div className="flex justify-start">
              <div className="max-w-2xl">
                <div className="bg-gray-800 rounded-lg px-4 py-3 shadow-lg border border-gray-700">
                  <ChartComponent data={chartData} narrative={narrative || undefined} />
                </div>
              </div>
            </div>
          )}

          {/* Error Display */}
          {error && (
            <div className="flex justify-start">
              <div className="max-w-2xl w-full">
                <div className="bg-red-900/70 border border-red-700 rounded-lg px-4 py-3 shadow-lg">
                  <div className="flex items-start space-x-3">
                    <div className="text-red-400 text-xl">⚠️</div>
                    <div className="flex-1">
                      <div className="flex items-center justify-between">
                        <p className="text-red-300 text-sm font-medium">There was a problem</p>
                        <button onClick={() => setError(null)} className="text-red-300 hover:text-red-200 text-xs">Dismiss</button>
                      </div>
                      <div className="bg-red-800/70 rounded p-3 text-xs font-mono text-red-100 overflow-x-auto mt-2">
                        {error}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Sample Questions */}
          {!chartData && !error && !selectedConversationId && (
            <div className="flex justify-start">
              <div className="max-w-2xl w-full">
                <div className="bg-gray-800 rounded-lg px-5 py-5 shadow-lg border border-gray-700">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="text-sm font-medium text-gray-200">Suggested questions</h3>
                    <button
                      onClick={() => setQuestion(currentSamples[Math.floor(Math.random() * currentSamples.length)])}
                      className="text-xs text-blue-400 hover:text-blue-300"
                    >
                      Random
                    </button>
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                    {currentSamples.map((sample: string, index: number) => (
                      <button
                        key={index}
                        onClick={() => setQuestion(sample)}
                        className="text-left p-3 bg-gray-700/70 hover:bg-gray-600 rounded-lg text-sm text-gray-200 border border-gray-600 transition-colors"
                        disabled={loading}
                        title={sample}
                      >
                        <span className="line-clamp-2 leading-snug">{sample}</span>
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Input Area */}
      <div className="px-6 py-4 border-t border-gray-800 bg-gray-900/80 backdrop-blur sticky bottom-0">
        <div className="max-w-4xl mx-auto">
          <form onSubmit={handleSubmit}>
            <div className="flex justify-center">
              <div className="w-full max-w-2xl">
                <div className="flex items-end space-x-3">
                  {/* Input Field */}
                  <div className="flex-1">
                    <textarea
                      id="question"
                      rows={1}
                      className="w-full px-4 py-3 border border-gray-700 rounded-lg resize-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-sm bg-gray-800 text-gray-100 placeholder-gray-400"
                      placeholder={
                        selectedDatabase === 'zigment'
                          ? 'Ask about contacts, events, organizations, chat histories, tags...'
                          : 'Ask a question about your database...'
                      }
                      value={question}
                      onChange={(e) => setQuestion(e.target.value)}
                      disabled={loading}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' && !e.shiftKey) {
                          e.preventDefault();
                          handleSubmit(e);
                        }
                      }}
                    />
                  </div>

                  {/* Clear and Send Buttons */}
                  <button
                    type="button"
                    onClick={clearResults}
                    disabled={loading}
                    className="px-3 py-3 bg-gray-800 text-gray-300 rounded-lg hover:bg-gray-700 border border-gray-700 text-sm"
                    title="Clear"
                  >
                    Clear
                  </button>
                  <button
                    type="submit"
                    disabled={loading || !question.trim()}
                    className="px-4 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium shadow"
                  >
                    {loading ? (
                      <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    ) : (
                      'Send'
                    )}
                  </button>
              <button
                type="button"
                onClick={() => setDebugOpen(v => !v)}
                className="px-3 py-2 bg-gray-700 hover:bg-gray-600 text-gray-200 rounded text-xs"
                title="Toggle debug panel"
              >
                {debugOpen ? 'Hide debug' : 'Show debug'}
              </button>
                </div>
              </div>
            </div>
        {debugOpen && (
          <div className="border-b border-gray-700 bg-gray-900/80">
            <div className="max-w-6xl mx-auto px-6 py-3 grid md:grid-cols-2 gap-4 text-xs text-gray-300">
              <div>
                <div className="font-semibold mb-1">Last request</div>
                <pre className="bg-gray-800 border border-gray-700 rounded p-2 overflow-x-auto">{JSON.stringify(lastReq, null, 2)}</pre>
              </div>
              <div>
                <div className="font-semibold mb-1">Last response</div>
                <pre className="bg-gray-800 border border-gray-700 rounded p-2 overflow-x-auto">{JSON.stringify(lastRes, null, 2)}</pre>
                </div>
              </div>
            </div>
        )}
          </form>
        </div>
      </div>
    </div>
  </div>
  )
}
