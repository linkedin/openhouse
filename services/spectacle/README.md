# OpenHouse Spectacle Application

A Next.js web application for searching and exploring OpenHouse tables.

## Features

- Built with Next.js 14 and React 18
- TypeScript support
- Standalone output for Docker deployment
- Modern, responsive design
- Search tables by database ID
- Filter results in real-time
- Display table metadata (ID, type, creator, last modified time)

## Development

### Prerequisites

- Node.js 20 or higher
- npm

### Local Development

1. Install dependencies:
```bash
npm install
```

2. Run the development server:
```bash
npm run dev
```

3. Open [http://localhost:3000](http://localhost:3000) in your browser.

### Build

```bash
npm run build
```

### Production

```bash
npm start
```

## Docker Deployment

### Build and Run with Docker Compose

From the root of the OpenHouse repository:

```bash
docker-compose -f infra/recipes/docker-compose/oh-hadoop-spark/docker-compose.spectacle.yml up --build
```

This will:
1. Build and start the tables-service on port 8000
2. Build and start the jobs-service on port 8002
3. Build and start the spectacle-service on port 3000 (depends on the above services)

### Access the Application

Once all services are running, access the web application at:
- **Spectacle App**: http://localhost:3000
- **Tables Service**: http://localhost:8000
- **Jobs Service**: http://localhost:8002

### Stop Services

```bash
docker-compose -f infra/recipes/docker-compose/oh-hadoop-spark/docker-compose.spectacle.yml down
```

## Project Structure

```
services/spectacle/
├── src/
│   └── app/
│       ├── layout.tsx    # Root layout component
│       └── page.tsx      # Home page with "Hello World"
├── package.json          # Dependencies and scripts
├── next.config.js        # Next.js configuration
├── tsconfig.json         # TypeScript configuration
└── README.md            # This file
```

## Environment Variables

The following environment variables are available:

- `NEXT_PUBLIC_TABLES_SERVICE_URL`: URL for the tables service API (default: http://localhost:8000)
- `NEXT_PUBLIC_JOBS_SERVICE_URL`: URL for the jobs service API (default: http://localhost:8002)

**Note:** Environment variables prefixed with `NEXT_PUBLIC_` are exposed to the browser and can be used in client-side code.

## Usage

1. **Start the services** using Docker Compose (see Docker Deployment section above)
2. **Open the web app** at http://localhost:3000
3. **Enter a database ID** in the search box (e.g., "my_database")
4. **Click Search** to fetch all tables in that database
5. **Use the filter box** to narrow down results by table name or database ID
6. **View table details** including ID, type, creator, and last modified time
