'use client';

import { useState, useEffect } from 'react';

interface Table {
  tableId: string;
  databaseId: string;
  tableUri: string;
  tableCreator: string;
  lastModifiedTime: number;
  creationTime: number;
  tableType: string;
}

export default function Home() {
  const [databaseId, setDatabaseId] = useState('');
  const [tables, setTables] = useState<Table[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchFilter, setSearchFilter] = useState('');

  const searchTables = async () => {
    if (!databaseId.trim()) {
      setError('Please enter a database ID');
      return;
    }

    setLoading(true);
    setError('');
    
    try {
      const tablesServiceUrl = process.env.NEXT_PUBLIC_TABLES_SERVICE_URL || 'http://localhost:8000';
      const response = await fetch(`${tablesServiceUrl}/v1/databases/${databaseId}/tables/search`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to fetch tables: ${response.status} ${response.statusText}`);
      }

      const data = await response.json();
      setTables(data.results || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  const filteredTables = tables.filter(table => 
    table.tableId.toLowerCase().includes(searchFilter.toLowerCase()) ||
    table.databaseId.toLowerCase().includes(searchFilter.toLowerCase())
  );

  const formatDate = (timestamp: number) => {
    return new Date(timestamp).toLocaleString();
  };

  return (
    <main style={{
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif',
      backgroundColor: '#f9fafb'
    }}>
      <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
        <h1 style={{
          fontSize: '2.5rem',
          fontWeight: 'bold',
          marginBottom: '0.5rem',
          background: 'linear-gradient(to right, #3b82f6, #8b5cf6)',
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          backgroundClip: 'text',
          textAlign: 'center'
        }}>
          OpenHouse Spectacle
        </h1>
        <p style={{
          fontSize: '1rem',
          color: '#6b7280',
          textAlign: 'center',
          marginBottom: '2rem'
        }}>
          Search and explore OpenHouse tables
        </p>

        {/* Search Section */}
        <div style={{
          backgroundColor: 'white',
          padding: '1.5rem',
          borderRadius: '8px',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
          marginBottom: '2rem'
        }}>
          <div style={{ display: 'flex', gap: '1rem', marginBottom: '1rem' }}>
            <input
              type="text"
              placeholder="Enter Database ID"
              value={databaseId}
              onChange={(e) => setDatabaseId(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && searchTables()}
              style={{
                flex: 1,
                padding: '0.75rem',
                border: '1px solid #d1d5db',
                borderRadius: '6px',
                fontSize: '1rem',
                outline: 'none'
              }}
            />
            <button
              onClick={searchTables}
              disabled={loading}
              style={{
                padding: '0.75rem 2rem',
                backgroundColor: loading ? '#9ca3af' : '#3b82f6',
                color: 'white',
                border: 'none',
                borderRadius: '6px',
                fontSize: '1rem',
                fontWeight: '500',
                cursor: loading ? 'not-allowed' : 'pointer',
                transition: 'background-color 0.2s'
              }}
            >
              {loading ? 'Searching...' : 'Search'}
            </button>
          </div>

          {tables.length > 0 && (
            <input
              type="text"
              placeholder="Filter tables..."
              value={searchFilter}
              onChange={(e) => setSearchFilter(e.target.value)}
              style={{
                width: '100%',
                padding: '0.75rem',
                border: '1px solid #d1d5db',
                borderRadius: '6px',
                fontSize: '1rem',
                outline: 'none'
              }}
            />
          )}
        </div>

        {/* Error Message */}
        {error && (
          <div style={{
            backgroundColor: '#fee2e2',
            color: '#991b1b',
            padding: '1rem',
            borderRadius: '6px',
            marginBottom: '1rem'
          }}>
            {error}
          </div>
        )}

        {/* Results Section */}
        {tables.length > 0 && (
          <div style={{
            backgroundColor: 'white',
            borderRadius: '8px',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
            overflow: 'hidden'
          }}>
            <div style={{
              padding: '1rem 1.5rem',
              borderBottom: '1px solid #e5e7eb',
              backgroundColor: '#f9fafb'
            }}>
              <h2 style={{ fontSize: '1.25rem', fontWeight: '600', margin: 0 }}>
                Found {filteredTables.length} table{filteredTables.length !== 1 ? 's' : ''}
              </h2>
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ backgroundColor: '#f9fafb' }}>
                    <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>Table ID</th>
                    <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>Database ID</th>
                    <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>Type</th>
                    <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>Creator</th>
                    <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>Last Modified</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTables.map((table, index) => (
                    <tr key={`${table.databaseId}-${table.tableId}-${index}`} style={{
                      borderBottom: '1px solid #e5e7eb',
                      transition: 'background-color 0.2s'
                    }}
                    onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f9fafb'}
                    onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'white'}
                    >
                      <td style={{ padding: '0.75rem 1.5rem', color: '#1f2937', fontWeight: '500' }}>{table.tableId}</td>
                      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280' }}>{table.databaseId}</td>
                      <td style={{ padding: '0.75rem 1.5rem' }}>
                        <span style={{
                          padding: '0.25rem 0.75rem',
                          backgroundColor: '#dbeafe',
                          color: '#1e40af',
                          borderRadius: '9999px',
                          fontSize: '0.875rem',
                          fontWeight: '500'
                        }}>
                          {table.tableType}
                        </span>
                      </td>
                      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280' }}>{table.tableCreator || 'N/A'}</td>
                      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280', fontSize: '0.875rem' }}>
                        {formatDate(table.lastModifiedTime)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!loading && tables.length === 0 && !error && (
          <div style={{
            backgroundColor: 'white',
            padding: '3rem',
            borderRadius: '8px',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
            textAlign: 'center',
            color: '#6b7280'
          }}>
            <p style={{ fontSize: '1.125rem', marginBottom: '0.5rem' }}>No tables found</p>
            <p style={{ fontSize: '0.875rem' }}>Enter a database ID and click Search to get started</p>
          </div>
        )}
      </div>
    </main>
  );
}
