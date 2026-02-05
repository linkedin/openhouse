'use client';

import { useState, useEffect, useRef } from 'react';

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

type ViewMode = 'table' | 'json';
type ExportFormat = 'csv' | 'tsv' | 'json';

export default function DataPreview({ databaseId, tableId }: DataPreviewProps) {
  const [data, setData] = useState<TableDataResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<ViewMode>('table');
  const [exportDropdownOpen, setExportDropdownOpen] = useState(false);
  const exportDropdownRef = useRef<HTMLDivElement>(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (exportDropdownRef.current && !exportDropdownRef.current.contains(event.target as Node)) {
        setExportDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

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

  const escapeForCSV = (value: any): string => {
    if (value === null || value === undefined) {
      return '';
    }
    const str = typeof value === 'object' ? JSON.stringify(value) : String(value);
    // Escape quotes and wrap in quotes if contains comma, quote, or newline
    if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\t')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  };

  const downloadFile = (content: string, filename: string, mimeType: string) => {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const handleExport = (format: ExportFormat) => {
    if (!data || !data.rows || data.rows.length === 0) return;

    const columns = getColumns();
    const filename = `${databaseId}_${tableId}`;

    switch (format) {
      case 'json': {
        const jsonContent = JSON.stringify(data.rows, null, 2);
        downloadFile(jsonContent, `${filename}.json`, 'application/json');
        break;
      }
      case 'csv': {
        const header = columns.map(col => escapeForCSV(col)).join(',');
        const rows = data.rows.map(row =>
          columns.map(col => escapeForCSV(row[col])).join(',')
        );
        const csvContent = [header, ...rows].join('\n');
        downloadFile(csvContent, `${filename}.csv`, 'text/csv');
        break;
      }
      case 'tsv': {
        const header = columns.join('\t');
        const rows = data.rows.map(row =>
          columns.map(col => {
            const value = row[col];
            if (value === null || value === undefined) return '';
            const str = typeof value === 'object' ? JSON.stringify(value) : String(value);
            return str.replace(/\t/g, ' ').replace(/\n/g, ' ');
          }).join('\t')
        );
        const tsvContent = [header, ...rows].join('\n');
        downloadFile(tsvContent, `${filename}.tsv`, 'text/tab-separated-values');
        break;
      }
    }

    setExportDropdownOpen(false);
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
        marginBottom: '1rem',
        flexWrap: 'wrap',
        gap: '0.75rem'
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
          gap: '0.75rem',
          flexWrap: 'wrap'
        }}>
          <span style={{
            fontSize: '0.875rem',
            color: '#6b7280'
          }}>
            Showing {data.totalRowsFetched} row{data.totalRowsFetched !== 1 ? 's' : ''}
            {data.hasMore && ' (more available)'}
          </span>

          {/* View Mode Toggle */}
          <div style={{
            display: 'flex',
            backgroundColor: '#f3f4f6',
            borderRadius: '6px',
            padding: '2px',
            border: '1px solid #e5e7eb'
          }}>
            <button
              onClick={() => setViewMode('table')}
              style={{
                padding: '0.25rem 0.625rem',
                backgroundColor: viewMode === 'table' ? 'white' : 'transparent',
                color: viewMode === 'table' ? '#1f2937' : '#6b7280',
                border: 'none',
                borderRadius: '4px',
                fontSize: '0.75rem',
                fontWeight: '500',
                cursor: 'pointer',
                transition: 'all 0.15s',
                boxShadow: viewMode === 'table' ? '0 1px 2px rgba(0,0,0,0.05)' : 'none'
              }}
            >
              Table
            </button>
            <button
              onClick={() => setViewMode('json')}
              style={{
                padding: '0.25rem 0.625rem',
                backgroundColor: viewMode === 'json' ? 'white' : 'transparent',
                color: viewMode === 'json' ? '#1f2937' : '#6b7280',
                border: 'none',
                borderRadius: '4px',
                fontSize: '0.75rem',
                fontWeight: '500',
                cursor: 'pointer',
                transition: 'all 0.15s',
                boxShadow: viewMode === 'json' ? '0 1px 2px rgba(0,0,0,0.05)' : 'none'
              }}
            >
              JSON
            </button>
          </div>

          {/* Export Dropdown */}
          <div ref={exportDropdownRef} style={{ position: 'relative' }}>
            <button
              onClick={() => setExportDropdownOpen(!exportDropdownOpen)}
              style={{
                padding: '0.375rem 0.75rem',
                backgroundColor: '#f0fdf4',
                color: '#16a34a',
                border: '1px solid #bbf7d0',
                borderRadius: '6px',
                fontSize: '0.75rem',
                fontWeight: '500',
                cursor: 'pointer',
                transition: 'all 0.2s',
                display: 'flex',
                alignItems: 'center',
                gap: '0.375rem'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor = '#dcfce7';
                e.currentTarget.style.borderColor = '#86efac';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = '#f0fdf4';
                e.currentTarget.style.borderColor = '#bbf7d0';
              }}
            >
              Export
              <span style={{ fontSize: '0.625rem' }}>▼</span>
            </button>
            {exportDropdownOpen && (
              <div style={{
                position: 'absolute',
                top: '100%',
                right: 0,
                marginTop: '4px',
                backgroundColor: 'white',
                borderRadius: '6px',
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                border: '1px solid #e5e7eb',
                overflow: 'hidden',
                zIndex: 50,
                minWidth: '100px'
              }}>
                {(['csv', 'tsv', 'json'] as ExportFormat[]).map((format) => (
                  <button
                    key={format}
                    onClick={() => handleExport(format)}
                    style={{
                      width: '100%',
                      padding: '0.5rem 0.75rem',
                      backgroundColor: 'transparent',
                      color: '#374151',
                      border: 'none',
                      fontSize: '0.75rem',
                      fontWeight: '500',
                      cursor: 'pointer',
                      textAlign: 'left',
                      transition: 'background-color 0.15s'
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.backgroundColor = '#f3f4f6';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.backgroundColor = 'transparent';
                    }}
                  >
                    {format.toUpperCase()}
                  </button>
                ))}
              </div>
            )}
          </div>

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

      {viewMode === 'table' ? (
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
      ) : (
        <div style={{
          borderRadius: '6px',
          border: '1px solid #e5e7eb',
          backgroundColor: '#1e1e1e',
          overflow: 'auto',
          maxHeight: '500px'
        }}>
          <pre style={{
            margin: 0,
            padding: '1rem',
            fontSize: '0.8125rem',
            fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
            color: '#d4d4d4',
            lineHeight: '1.5',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word'
          }}>
            {JSON.stringify(data.rows, null, 2)}
          </pre>
        </div>
      )}

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
