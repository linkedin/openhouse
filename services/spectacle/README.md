# OpenHouse Spectacle Application

A simple Next.js web application that displays "Hello World" message.

## Features

- Built with Next.js 14 and React 18
- TypeScript support
- Standalone output for Docker deployment
- Modern, responsive design

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

The following environment variables are available in the Docker container:

- `TABLES_SERVICE_URL`: URL for the tables service (default: http://openhouse-tables:8080)
- `JOBS_SERVICE_URL`: URL for the jobs service (default: http://openhouse-jobs:8080)
