import { NextRequest, NextResponse } from 'next/server';
import { getBearerToken } from '@/lib/auth';

export async function POST(request: NextRequest) {
  try {
    const { databaseId, tableId } = await request.json();

    const tablesServiceUrl = process.env.NEXT_PUBLIC_TABLES_SERVICE_URL || 'http://localhost:8000';
    const bearerToken = getBearerToken();

    const response = await fetch(`${tablesServiceUrl}/internal/tables/${databaseId}/${tableId}/metadata`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${bearerToken}`,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      // Provide more descriptive error message for 400 (Iceberg metadata issues)
      if (response.status === 400) {
        console.warn(`Iceberg metadata unavailable for ${databaseId}.${tableId}: ${errorText}`);
        return NextResponse.json(
          { error: 'Iceberg metadata unavailable', details: 'The table exists but Iceberg metadata could not be loaded. This may be due to metadata file corruption or access issues.' },
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
    console.error('API route error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch table metadata', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
