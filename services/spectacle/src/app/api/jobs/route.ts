import { NextRequest, NextResponse } from 'next/server';
import { getBearerToken } from '@/lib/auth';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const jobNamePrefix = searchParams.get('jobNamePrefix');
    const limit = searchParams.get('limit') || '10';

    if (!jobNamePrefix) {
      return NextResponse.json(
        { error: 'jobNamePrefix query parameter is required' },
        { status: 400 }
      );
    }

    const jobsServiceUrl = process.env.NEXT_PUBLIC_JOBS_SERVICE_URL || 'http://localhost:8002';
    const bearerToken = getBearerToken();

    const url = new URL(`${jobsServiceUrl}/jobs`);
    url.searchParams.set('jobNamePrefix', jobNamePrefix);
    url.searchParams.set('limit', limit);

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${bearerToken}`,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      return NextResponse.json(
        { error: `API Error: ${response.status} ${response.statusText}`, details: errorText },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Jobs search API route error:', error);
    return NextResponse.json(
      { error: 'Failed to search jobs', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    const jobsServiceUrl = process.env.NEXT_PUBLIC_JOBS_SERVICE_URL || 'http://localhost:8002';
    const bearerToken = getBearerToken();

    const response = await fetch(`${jobsServiceUrl}/jobs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${bearerToken}`,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errorText = await response.text();
      return NextResponse.json(
        { error: `API Error: ${response.status} ${response.statusText}`, details: errorText },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('Jobs API route error:', error);
    return NextResponse.json(
      { error: 'Failed to submit job', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
