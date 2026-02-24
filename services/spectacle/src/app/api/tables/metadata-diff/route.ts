import { NextRequest, NextResponse } from 'next/server';
import { getBearerToken } from '@/lib/auth';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const databaseId = searchParams.get('databaseId');
    const tableId = searchParams.get('tableId');
    const metadataFile = searchParams.get('metadataFile');

    console.log('[metadata-diff] Request params:', { databaseId, tableId, metadataFile });

    if (!databaseId || !tableId || !metadataFile) {
      console.error('[metadata-diff] Missing parameters');
      return NextResponse.json(
        { error: 'Missing required parameters: databaseId, tableId, metadataFile' },
        { status: 400 }
      );
    }

    const tablesServiceUrl = process.env.NEXT_PUBLIC_TABLES_SERVICE_URL || 'http://localhost:8000';
    const bearerToken = getBearerToken();
    const backendUrl = `${tablesServiceUrl}/internal/tables/${databaseId}/${tableId}/metadata/diff?metadataFile=${encodeURIComponent(metadataFile)}`;

    console.log('[metadata-diff] Calling backend:', backendUrl);

    const response = await fetch(backendUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${bearerToken}`,
      },
    });

    console.log('[metadata-diff] Backend response status:', response.status);

    if (!response.ok) {
      const errorText = await response.text();
      console.error('[metadata-diff] Backend error:', errorText);
      return NextResponse.json(
        {
          error: `API Error: ${response.status} ${response.statusText}`,
          details: errorText,
          backendUrl
        },
        { status: response.status }
      );
    }

    const data = await response.json();
    console.log('[metadata-diff] Success, returning data');
    return NextResponse.json(data);
  } catch (error) {
    console.error('[metadata-diff] API route error:', error);
    return NextResponse.json(
      {
        error: 'Failed to fetch metadata diff',
        details: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : undefined
      },
      { status: 500 }
    );
  }
}
