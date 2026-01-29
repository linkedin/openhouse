import { NextRequest, NextResponse } from 'next/server';
import { getBearerToken } from '@/lib/auth';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ jobId: string }> }
) {
  try {
    const { jobId } = await params;

    const jobsServiceUrl = process.env.NEXT_PUBLIC_JOBS_SERVICE_URL || 'http://localhost:8002';
    const bearerToken = getBearerToken();

    const response = await fetch(`${jobsServiceUrl}/jobs/${jobId}`, {
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
    console.error('Job status API route error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch job status', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
