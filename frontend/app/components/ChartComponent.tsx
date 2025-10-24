'use client'

import { useEffect, useRef } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line, Pie, Scatter } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
)

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

interface ChartComponentProps {
  data: ChartData | ChartData[]
  narrative?: Narrative
}

export default function ChartComponent({ data, narrative }: ChartComponentProps) {
  
  // Normalize data to always be an array
  const chartsData = Array.isArray(data) ? data : [data]
  
  // Safety check: if no valid chart data, don't render anything
  if (!chartsData || chartsData.length === 0 || !chartsData[0]) {
    return null
  }
  
  // If we have multiple charts, render them as one flowing conversation
  if (chartsData.length > 1) {
    return (
      <div className="space-y-6">
        
        {narrative?.introduction && (
          <div className="bg-gray-800/50 rounded-lg p-6 border border-gray-700/50">
            <p className="text-gray-200 leading-relaxed">{narrative.introduction}</p>
          </div>
        )}
        
        {chartsData.map((chartData, index) => (
          <div key={index} className="space-y-6">
            <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
              <SingleChart data={chartData} />
            </div>
            
            {narrative?.transitions && narrative.transitions[index] && (
              <div className="bg-gray-800/50 rounded-lg p-6 border border-gray-700/50">
                <p className="text-gray-200 leading-relaxed">{narrative.transitions[index]}</p>
              </div>
            )}
          </div>
        ))}
        
        {narrative?.insights && narrative.insights.map((insight, index) => (
          <div key={index} className="bg-gray-800/50 rounded-lg p-6 border border-gray-700/50">
            <p className="text-gray-200 leading-relaxed">{insight}</p>
          </div>
        ))}
        
        {narrative?.conclusion && (
          <div className="bg-gray-800/50 rounded-lg p-6 border border-gray-700/50">
            <p className="text-gray-200 leading-relaxed">{narrative.conclusion}</p>
          </div>
        )}
      </div>
    )
  }
  
  // For single chart, render normally
  return <SingleChart data={chartsData[0]} />
}

// Single chart component
function SingleChart({ data }: { data: ChartData }) {
  const chartType = data.chart_type || 'bar'
  
  const labels = data.data.map(item => item.label)
  const values = data.data.map(item => item.value)
  
  // Handle scatter plot data differently
  const scatterData = data.data.map(item => ({
    x: item.x || item.value,
    y: item.y || item.value,
    label: item.label  // Keep the label for tooltips
  }))

  // Debug logging for scatter plots
  if (chartType === 'scatter') {
    console.log('Scatter plot data:', data.data.slice(0, 3))
    console.log('Processed scatter data:', scatterData.slice(0, 3))
    console.log('Chart type:', chartType)
    console.log('Data length:', data.data.length)
  }

  // If chart type is table, render table instead
  if (chartType === 'table') {
    return (
      <div className="space-y-3">
        <div className="flex justify-between items-center mb-3">
          <h2 className="text-lg font-semibold text-white">{data.title}</h2>
          <div className="text-xs text-gray-400">
Table View - {data.data.length} items
          </div>
        </div>
        
        
        <div className="overflow-x-auto">
          <table className="min-w-full bg-gray-800 rounded-lg border border-gray-700">
            <thead className="bg-gray-700">
              <tr>
                {Object.keys(data.data[0] || {}).map((key, index) => (
                  <th key={key} className={`px-4 py-2 text-xs font-medium text-gray-300 uppercase tracking-wider ${
                    index === 0 ? 'text-left' : 'text-right'
                  }`}>
                    {key === 'label' ? (data.x_axis || 'Item') : 
                     key === 'value' ? (data.y_axis || 'Value') : 
                     key.charAt(0).toUpperCase() + key.slice(1)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-700">
              {data.data.map((item, index) => (
                <tr key={index} className="hover:bg-gray-700">
                  {Object.entries(item).map(([key, value], cellIndex) => (
                    <td key={key} className={`px-4 py-2 text-xs ${
                      cellIndex === 0 ? 'text-gray-200' : 'text-gray-300 text-right font-medium'
                    }`}>
                      {typeof value === "number" 
                        ? value.toLocaleString() 
                        : value || "-"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        
        <div className="text-xs text-gray-400 mt-3">
          💡 This data is best displayed as a table due to its complexity or detailed nature.
        </div>

      </div>
    )
  }

  // Color palette for charts
  const colors = [
    'rgba(54, 162, 235, 0.8)',
    'rgba(255, 99, 132, 0.8)',
    'rgba(255, 205, 86, 0.8)',
    'rgba(75, 192, 192, 0.8)',
    'rgba(153, 102, 255, 0.8)',
    'rgba(255, 159, 64, 0.8)',
    'rgba(199, 199, 199, 0.8)',
    'rgba(83, 102, 255, 0.8)',
    'rgba(255, 99, 255, 0.8)',
    'rgba(99, 255, 132, 0.8)'
  ]

  const borderColors = [
    'rgba(54, 162, 235, 1)',
    'rgba(255, 99, 132, 1)',
    'rgba(255, 205, 86, 1)',
    'rgba(75, 192, 192, 1)',
    'rgba(153, 102, 255, 1)',
    'rgba(255, 159, 64, 1)',
    'rgba(199, 199, 199, 1)',
    'rgba(83, 102, 255, 1)',
    'rgba(255, 99, 255, 1)',
    'rgba(99, 255, 132, 1)'
  ]

  // Configure chart data based on type
  const chartData = {
    labels: chartType === 'scatter' ? undefined : labels,
    datasets: [
      {
        label: data.y_axis || 'Value',
        data: chartType === 'scatter' ? scatterData : values,
        backgroundColor: chartType === 'pie' ? colors.slice(0, values.length) : colors[0],
        borderColor: chartType === 'pie' ? borderColors.slice(0, values.length) : borderColors[0],
        borderWidth: chartType === 'line' ? 2 : 1,
        fill: chartType === 'line' ? false : undefined,
        pointBackgroundColor: chartType === 'scatter' ? colors[0] : undefined,
        pointBorderColor: chartType === 'scatter' ? borderColors[0] : undefined,
        pointRadius: chartType === 'scatter' ? 6 : undefined,
      },
    ],
  }

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top' as const,
        display: chartType === 'pie'
      },
      title: {
        display: true,
        text: data.title,
        font: {
          size: 16,
          weight: 'bold' as const
        }
      },
      tooltip: {
        callbacks: {
          label: function(context: any) {
            if (chartType === 'scatter') {
              const point = context.raw
              return `${point.label}: (${point.x}, ${point.y})`
            }
            const value = context.parsed.y || context.parsed
            return `${context.dataset.label}: ${value}`
          }
        }
      }
    },
    scales: chartType !== 'pie' ? {
      y: {
        beginAtZero: true,
        title: {
          display: true,
          text: data.y_axis || 'Value'
        }
      },
      x: {
        type: chartType === 'scatter' ? 'linear' as const : 'category' as const,
        beginAtZero: chartType === 'scatter',
        title: {
          display: true,
          text: data.x_axis || 'Categories'
        }
      }
    } : undefined,
  }

  const renderChart = () => {
    // Debug logging for scatter plots
    if (chartType === 'scatter') {
      console.log('Final chartData for scatter:', chartData)
      console.log('Dataset data:', chartData.datasets[0].data)
    }
    
    switch (chartType) {
      case 'line':
        return <Line data={chartData} options={options} />
      case 'pie':
        return <Pie data={chartData} options={options} />
      case 'scatter':
        return <Scatter data={chartData} options={options} />
      case 'horizontal_bar':
        // Use horizontal bar chart with modified options
        const horizontalOptions = {
          ...options,
          indexAxis: 'y' as const,
          scales: {
            ...options.scales,
            x: options.scales?.y,
            y: options.scales?.x
          }
        }
        return <Bar data={chartData} options={horizontalOptions} />
      case 'lollipop':
        // Render lollipop chart as a styled horizontal bar
        const lollipopOptions = {
          ...options,
          indexAxis: 'y' as const,
          elements: {
            bar: {
              borderRadius: 20,
              borderSkipped: false,
            }
          },
          scales: {
            ...options.scales,
            x: options.scales?.y,
            y: options.scales?.x
          }
        }
        const lollipopData = {
          ...chartData,
          datasets: [{
            ...chartData.datasets[0],
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 2,
            barThickness: 8,
          }]
        }
        return <Bar data={lollipopData} options={lollipopOptions} />
      case 'slope':
      case 'bump':
        // For slope/bump charts, render as line chart with special styling
        const slopeData = {
          ...chartData,
          datasets: [{
            ...chartData.datasets[0],
            fill: false,
            borderWidth: 3,
            pointRadius: 8,
            pointHoverRadius: 10,
            tension: 0.1
          }]
        }
        return <Line data={slopeData} options={options} />
      case 'treemap':
        // Simple treemap representation using nested divs
        return (
          <div className="grid grid-cols-4 gap-1 h-full">
            {data.data.slice(0, 12).map((item, index) => {
              const maxValue = Math.max(...data.data.map(d => d.value))
              const size = Math.max(0.3, item.value / maxValue)
              return (
                <div
                  key={index}
                  className="bg-blue-500 rounded flex items-center justify-center text-white text-xs font-medium"
                  style={{
                    opacity: size,
                    transform: `scale(${size})`,
                    minHeight: '40px'
                  }}
                  title={`${item.label}: ${item.value}`}
                >
                  <div className="text-center p-1">
                    <div className="truncate">{item.label}</div>
                    <div className="text-xs">{item.value}</div>
                  </div>
                </div>
              )
            })}
          </div>
        )
      case 'bar':
      default:
        return <Bar data={chartData} options={options} />
    }
  }

  // Get chart type icon
  const getChartIcon = () => {
    return '' // Removed emojis for professional appearance
  }

  // Get chart type name
  const getChartTypeName = () => {
    switch (chartType) {
      case 'line': return 'Line Chart'
      case 'pie': return 'Pie Chart'
      case 'scatter': return 'Scatter Plot'
      case 'table': return 'Data Table'
      case 'horizontal_bar': return 'Horizontal Bar Chart'
      case 'lollipop': return 'Lollipop Chart'
      case 'slope': return 'Slope Chart'
      case 'bump': return 'Bump Chart'
      case 'treemap': return 'Treemap'
      case 'area': return 'Area Chart'
      case 'histogram': return 'Histogram'
      case 'heatmap': return 'Heatmap'
      case 'stacked_bar': return 'Stacked Bar Chart'
      default: return 'Bar Chart'
    }
  }

  return (
    <div className="space-y-3">
      
      {/* Chart Section */}
      <div className="flex justify-between items-center">
        <h2 className="text-lg font-semibold text-white">{data.title}</h2>
        <div className="flex items-center space-x-2 text-xs text-gray-400">
          <span>{getChartTypeName()}</span>
          <span>•</span>
          <span>{data.data.length} items</span>
        </div>
      </div>
      
      <div className="h-64 w-full">
        {renderChart()}
      </div>


      {/* Data Summary - Only show for table charts */}
      {chartType === 'table' && (
        <div className="mt-4">
          <h3 className="text-sm font-medium text-gray-300 mb-2">📊 Data Summary:</h3>
          <div className="overflow-x-auto">
            <table className="min-w-full bg-gray-800 rounded-lg">
              <thead>
                <tr className="bg-gray-700">
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-300">
                    {data.x_axis || 'Label'}
                  </th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-300">
                    {data.y_axis || 'Value'}
                  </th>
                </tr>
              </thead>
              <tbody>
                {data.data.map((item, index) => (
                  <tr key={index} className="border-t border-gray-700">
                    <td className="px-3 py-2 text-xs text-gray-200">{item.label}</td>
                    <td className="px-3 py-2 text-xs text-gray-300 text-right font-medium">
                      {typeof item.value === 'number' ? item.value.toLocaleString() : (item.value ?? '-')}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
