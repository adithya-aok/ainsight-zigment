# LangChain Database Analytics Frontend

A modern Next.js frontend for querying databases using natural language. Built with TypeScript, Tailwind CSS, and Chart.js for beautiful data visualizations.

## Features

- 🎯 Natural language database querying
- 📊 Interactive data visualizations (Bar, Line, Pie charts)
- 💎 Modern, responsive UI with Tailwind CSS
- ⚡ Fast and optimized with Next.js 14
- 🎨 Beautiful gradient backgrounds and smooth animations
- 📱 Mobile-friendly responsive design

## Setup

1. **Install Dependencies**
   ```bash
   npm install
   # or
   yarn install
   ```

2. **Run Development Server**
   ```bash
   npm run dev
   # or
   yarn dev
   ```

3. **Open Your Browser**
   Navigate to `http://localhost:3001`

## Project Structure

```
frontend/
├── app/
│   ├── components/
│   │   └── ChartComponent.tsx    # Chart visualization component
│   ├── globals.css               # Global styles and Tailwind
│   ├── layout.tsx               # Root layout component
│   └── page.tsx                 # Main page component
├── package.json
├── tailwind.config.js
├── tsconfig.json
└── next.config.js
```

## Technologies Used

- **Next.js 14**: React framework with App Router
- **TypeScript**: Type-safe JavaScript
- **Tailwind CSS**: Utility-first CSS framework
- **Chart.js + React-Chart.js-2**: Chart visualization library
- **Axios**: HTTP client for API requests

## Components

### ChartComponent
Renders interactive charts based on data from the backend:
- Supports Bar, Line, and Pie charts
- Responsive design with customizable colors
- Data summary table below charts
- Smooth animations and hover effects

### Main Page
- Question input form with validation
- Loading states and error handling
- Sample questions for user guidance
- Real-time chart updates

## Sample Questions

Try these example queries:
- "Show me top 5 most popular albums with their number of songs"
- "Which artists have sold the most tracks?"
- "What are the most popular music genres by sales?"
- "Show me customer purchases by country"

## Styling

The app uses a modern gradient design with:
- Purple-blue gradient backgrounds
- Card-based layouts with subtle shadows
- Smooth hover animations
- Responsive grid layouts
- Clean typography with proper spacing

## API Integration

The frontend connects to the Flask backend running on `localhost:9000`:
- `POST /api/ask` - Submit natural language questions
- Error handling for network issues
- Loading states during API calls
- Automatic data parsing and chart rendering

## Build and Deploy

```bash
# Build for production
npm run build

# Start production server
npm start
```

The app will be optimized and ready for deployment to Vercel, Netlify, or any static hosting service.

