import { NextRequest, NextResponse } from 'next/server';
import { getBearerToken } from '@/lib/auth';

export async function POST(request: NextRequest) {
  try {
    const { databaseId, tableId, limit = 10 } = await request.json();

    const tablesServiceUrl = process.env.NEXT_PUBLIC_TABLES_SERVICE_URL || 'http://localhost:8000';
    const bearerToken = getBearerToken();

    const url = `${tablesServiceUrl}/internal/tables/${databaseId}/${tableId}/data?limit=${limit}`;
    console.log(`[DataPreview] Fetching: ${url}`);

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${bearerToken}`,
      },
    });

    console.log(`[DataPreview] Response status: ${response.status}`);

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[DataPreview] Error response: ${errorText}`);
      if (response.status === 400) {
        console.warn(`Table data unavailable for ${databaseId}.${tableId}: ${errorText}`);
        return NextResponse.json(
          { error: 'Table data unavailable', details: 'Could not read data from the table. The table may be empty or have access restrictions.' },
          { status: 400 }
        );
      }
      return NextResponse.json(
        { error: `API Error: ${response.status} ${response.statusText}`, details: errorText },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('[DataPreview] API route error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch table data', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
