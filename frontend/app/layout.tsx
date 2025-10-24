import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'LangChain Database Analytics',
  description: 'Ask questions about your database in natural language',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-gray-900 text-gray-100 min-h-screen antialiased">{children}</body>
    </html>
  )
}

