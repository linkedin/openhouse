import { NextRequest, NextResponse } from 'next/server';
import { getBearerToken } from '@/lib/auth';

export async function POST(request: NextRequest) {
  try {
    const { databaseId, tableId } = await request.json();

    const tablesServiceUrl = process.env.NEXT_PUBLIC_TABLES_SERVICE_URL || 'http://localhost:8000';
    const bearerToken = getBearerToken();

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (bearerToken) {
      headers['Authorization'] = `Bearer ${bearerToken}`;
    }

    const response = await fetch(`${tablesServiceUrl}/v1/databases/${databaseId}/tables/${tableId}`, {
      method: 'GET',
      headers,
    });

    if (!response.ok) {
      // If backend returns 400 (bad request, often due to Iceberg metadata issues),
      // return partial table info instead of failing completely
      if (response.status === 400) {
        console.warn(`Table details returned 400 for ${databaseId}.${tableId}, returning partial info`);
        return NextResponse.json({
          databaseId,
          tableId,
          clusterId: 'unknown',
          _partial: true,
          _error: 'Iceberg metadata unavailable',
        });
      }

      const errorText = await response.text();
      return NextResponse.json(
        { error: `API Error: ${response.status} ${response.statusText}`, details: errorText },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('API route error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch table details', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
