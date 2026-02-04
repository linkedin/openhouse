'use client';

import { useState, useEffect } from 'react';

interface TableDataResponse {
  tableId: string;
  databaseId: string;
  schema: string;
  rows: Array<Record<string, any>>;
  totalRowsFetched: number;
  hasMore: boolean;
}

interface DataPreviewProps {
  databaseId: string;
  tableId: string;
}

export default function DataPreview({ databaseId, tableId }: DataPreviewProps) {
  const [data, setData] = useState<TableDataResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchTableData();
  }, [databaseId, tableId]);

  const fetchTableData = async () => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/api/tables/data', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId, tableId, limit: 10 }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to fetch table data');
      }

      const result = await response.json();
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data preview');
      console.error('Data preview error:', err);
    } finally {
      setLoading(false);
    }
  };

  // Extract column names from schema or first row
  const getColumns = (): string[] => {
    if (!data) return [];

    // Try to get columns from schema
    if (data.schema) {
      try {
        const schema = JSON.parse(data.schema);
        if (schema.fields && Array.isArray(schema.fields)) {
          return schema.fields.map((field: any) => field.name);
        }
      } catch (e) {
        console.error('Error parsing schema:', e);
      }
    }

    // Fallback: get columns from first row
    if (data.rows && data.rows.length > 0) {
      return Object.keys(data.rows[0]);
    }

    return [];
  };

  const formatCellValue = (value: any): string => {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }
    return String(value);
  };

  if (loading) {
    return (
      <div style={{
        backgroundColor: 'white',
        padding: '1.5rem',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        marginBottom: '1.5rem'
      }}>
        <h2 style={{
          fontSize: '1.25rem',
          fontWeight: '600',
          marginBottom: '1rem',
          color: '#1f2937'
        }}>
          Data Preview
        </h2>
        <div style={{ textAlign: 'center', padding: '2rem', color: '#6b7280' }}>
          Loading data preview...
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{
        backgroundColor: 'white',
        padding: '1.5rem',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        marginBottom: '1.5rem'
      }}>
        <h2 style={{
          fontSize: '1.25rem',
          fontWeight: '600',
          marginBottom: '1rem',
          color: '#1f2937'
        }}>
          Data Preview
        </h2>
        <div style={{
          backgroundColor: '#fef9c3',
          color: '#854d0e',
          padding: '1rem',
          borderRadius: '6px',
          border: '1px solid #fde047',
          display: 'flex',
          alignItems: 'center',
          gap: '0.75rem'
        }}>
          <span style={{ fontSize: '1.25rem' }}>⚠️</span>
          <div>
            <div style={{ fontWeight: '500' }}>Unable to load data preview</div>
            <div style={{ fontSize: '0.875rem', marginTop: '0.25rem', opacity: 0.9 }}>
              {error}
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (!data || !data.rows || data.rows.length === 0) {
    return (
      <div style={{
        backgroundColor: 'white',
        padding: '1.5rem',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        marginBottom: '1.5rem'
      }}>
        <h2 style={{
          fontSize: '1.25rem',
          fontWeight: '600',
          marginBottom: '1rem',
          color: '#1f2937'
        }}>
          Data Preview
        </h2>
        <div style={{
          backgroundColor: '#f9fafb',
          padding: '2rem',
          borderRadius: '6px',
          textAlign: 'center',
          color: '#6b7280'
        }}>
          <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>📭</div>
          <div>This table has no data yet.</div>
        </div>
      </div>
    );
  }

  const columns = getColumns();

  return (
    <div style={{
      backgroundColor: 'white',
      padding: '1.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      marginBottom: '1.5rem'
    }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '1rem'
      }}>
        <h2 style={{
          fontSize: '1.25rem',
          fontWeight: '600',
          color: '#1f2937',
          margin: 0
        }}>
          Data Preview
        </h2>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.75rem'
        }}>
          <span style={{
            fontSize: '0.875rem',
            color: '#6b7280'
          }}>
            Showing {data.totalRowsFetched} row{data.totalRowsFetched !== 1 ? 's' : ''}
            {data.hasMore && ' (more available)'}
          </span>
          <button
            onClick={fetchTableData}
            style={{
              padding: '0.375rem 0.75rem',
              backgroundColor: '#eff6ff',
              color: '#3b82f6',
              border: '1px solid #dbeafe',
              borderRadius: '6px',
              fontSize: '0.75rem',
              fontWeight: '500',
              cursor: 'pointer',
              transition: 'all 0.2s'
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = '#dbeafe';
              e.currentTarget.style.borderColor = '#93c5fd';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = '#eff6ff';
              e.currentTarget.style.borderColor = '#dbeafe';
            }}
          >
            ↻ Refresh
          </button>
        </div>
      </div>

      <div style={{
        overflowX: 'auto',
        borderRadius: '6px',
        border: '1px solid #e5e7eb'
      }}>
        <table style={{
          width: '100%',
          borderCollapse: 'collapse',
          minWidth: columns.length > 5 ? `${columns.length * 150}px` : 'auto'
        }}>
          <thead>
            <tr style={{ backgroundColor: '#f9fafb' }}>
              {columns.map((column, index) => (
                <th
                  key={index}
                  style={{
                    padding: '0.75rem 1rem',
                    textAlign: 'left',
                    fontWeight: '600',
                    color: '#374151',
                    fontSize: '0.875rem',
                    borderBottom: '2px solid #e5e7eb',
                    whiteSpace: 'nowrap',
                    position: 'sticky',
                    top: 0,
                    backgroundColor: '#f9fafb'
                  }}
                >
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row, rowIndex) => (
              <tr
                key={rowIndex}
                style={{
                  borderBottom: '1px solid #e5e7eb',
                  backgroundColor: rowIndex % 2 === 0 ? 'white' : '#fafafa'
                }}
              >
                {columns.map((column, colIndex) => {
                  const value = row[column];
                  const isNull = value === null || value === undefined;
                  return (
                    <td
                      key={colIndex}
                      style={{
                        padding: '0.75rem 1rem',
                        fontSize: '0.875rem',
                        fontFamily: 'monospace',
                        color: isNull ? '#9ca3af' : '#374151',
                        fontStyle: isNull ? 'italic' : 'normal',
                        maxWidth: '300px',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap'
                      }}
                      title={formatCellValue(value)}
                    >
                      {formatCellValue(value)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data.hasMore && (
        <div style={{
          marginTop: '0.75rem',
          fontSize: '0.875rem',
          color: '#6b7280',
          textAlign: 'center',
          fontStyle: 'italic'
        }}>
          Table contains more rows. This preview shows the first {data.totalRowsFetched} rows.
        </div>
      )}
    </div>
  );
}
