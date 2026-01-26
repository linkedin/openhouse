'use client';

import { useParams, useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useState, Suspense } from 'react';
import { Table } from '@/types/table';

function TableDetailContent() {
  const params = useParams();
  const router = useRouter();
  const searchParams = useSearchParams();
  const databaseId = params.databaseId as string;
  const tableId = params.tableId as string;
  const searchDatabaseId = searchParams.get('db') || databaseId;
  
  const [table, setTable] = useState<Table | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchTableDetails();
  }, [databaseId, tableId]);

  const fetchTableDetails = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/tables/details', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId, tableId }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to fetch table details');
      }

      const data = await response.json();
      setTable(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (timestamp: number) => {
    // If timestamp is in seconds (less than year 2000 in milliseconds), convert to milliseconds
    const timestampMs = timestamp < 10000000000 ? timestamp * 1000 : timestamp;
    return new Date(timestampMs).toLocaleString();
  };

  if (loading) {
    return (
      <main style={{
        minHeight: '100vh',
        padding: '2rem',
        fontFamily: 'system-ui, -apple-system, sans-serif',
        backgroundColor: '#f9fafb'
      }}>
        <div style={{ maxWidth: '1200px', margin: '0 auto', textAlign: 'center', paddingTop: '4rem' }}>
          <p style={{ fontSize: '1.25rem', color: '#6b7280' }}>Loading table details...</p>
        </div>
      </main>
    );
  }

  if (error) {
    return (
      <main style={{
        minHeight: '100vh',
        padding: '2rem',
        fontFamily: 'system-ui, -apple-system, sans-serif',
        backgroundColor: '#f9fafb'
      }}>
        <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
          <button
            onClick={() => router.push('/')}
            style={{
              padding: '0.5rem 1rem',
              backgroundColor: '#3b82f6',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
              marginBottom: '1rem'
            }}
          >
            ← Back to Search
          </button>
          <div style={{
            backgroundColor: '#fee2e2',
            color: '#991b1b',
            padding: '1rem',
            borderRadius: '6px'
          }}>
            <strong>Error:</strong> {error}
          </div>
        </div>
      </main>
    );
  }

  if (!table) {
    return (
      <main style={{
        minHeight: '100vh',
        padding: '2rem',
        fontFamily: 'system-ui, -apple-system, sans-serif',
        backgroundColor: '#f9fafb'
      }}>
        <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
          <button
            onClick={() => router.push('/')}
            style={{
              padding: '0.5rem 1rem',
              backgroundColor: '#3b82f6',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
              marginBottom: '1rem'
            }}
          >
            ← Back to Search
          </button>
          <p style={{ textAlign: 'center', color: '#6b7280' }}>Table not found</p>
        </div>
      </main>
    );
  }

  return (
    <main style={{
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif',
      backgroundColor: '#f9fafb'
    }}>
      <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
        {/* Back Button */}
        <button
          onClick={() => router.push(`/?db=${encodeURIComponent(searchDatabaseId)}`)}
          style={{
            padding: '0.5rem 1rem',
            backgroundColor: '#3b82f6',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: 'pointer',
            marginBottom: '2rem',
            fontSize: '1rem',
            fontWeight: '500'
          }}
        >
          ← Back to Search
        </button>

        {/* Header */}
        <div style={{
          backgroundColor: 'white',
          padding: '2rem',
          borderRadius: '8px',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
          marginBottom: '2rem'
        }}>
          <h1 style={{
            fontSize: '2rem',
            fontWeight: 'bold',
            marginBottom: '0.5rem',
            color: '#1f2937'
          }}>
            {table.tableId}
          </h1>
          <p style={{ color: '#6b7280', fontSize: '1rem' }}>
            Database: <span style={{ fontWeight: '500', color: '#374151' }}>{table.databaseId}</span>
          </p>
        </div>

        {/* Two Column Layout: Basic Info (left) and URIs (right) */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(450px, 1fr))',
          gap: '1.5rem',
          marginBottom: '1.5rem'
        }}>
          {/* Basic Information - Vertical */}
          <div style={{
            backgroundColor: 'white',
            padding: '1.5rem',
            borderRadius: '8px',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
          }}>
            <h2 style={{
              fontSize: '1.25rem',
              fontWeight: '600',
              marginBottom: '1rem',
              color: '#1f2937'
            }}>
              Basic Information
            </h2>

            <div style={{ display: 'grid', gap: '1rem' }}>
              <CompactDetailRow label="Table ID" value={table.tableId} />
              <CompactDetailRow label="Database ID" value={table.databaseId} />
              <CompactDetailRow label="Cluster ID" value={table.clusterId || 'N/A'} />
              <CompactDetailRow label="Table UUID" value={table.tableUUID || 'N/A'} />
              <CompactDetailRow label="Table Version" value={table.tableVersion || 'N/A'} />
              <CompactDetailRow label="Table Type" value={
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
              } />
              <CompactDetailRow label="Creator" value={table.tableCreator || 'N/A'} />
              <CompactDetailRow label="Created" value={formatDate(table.creationTime)} />
              <CompactDetailRow label="Last Modified" value={formatDate(table.lastModifiedTime)} />
            </div>
          </div>

          {/* Table URI and Storage Location - Vertical Stack */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
            <div style={{
              backgroundColor: 'white',
              padding: '1.5rem',
              borderRadius: '8px',
              boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
            }}>
              <h3 style={{
                fontSize: '1rem',
                fontWeight: '600',
                marginBottom: '0.75rem',
                color: '#1f2937'
              }}>
                Table URI
              </h3>
              <div style={{
                backgroundColor: '#f9fafb',
                padding: '0.75rem',
                borderRadius: '6px',
                fontFamily: 'monospace',
                fontSize: '0.875rem',
                wordBreak: 'break-all',
                color: '#374151'
              }}>
                {table.tableUri}
              </div>
            </div>

            {table.tableLocation && (
              <div style={{
                backgroundColor: 'white',
                padding: '1.5rem',
                borderRadius: '8px',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
              }}>
                <h3 style={{
                  fontSize: '1rem',
                  fontWeight: '600',
                  marginBottom: '0.75rem',
                  color: '#1f2937'
                }}>
                  Storage Location
                </h3>
                <div style={{
                  backgroundColor: '#f9fafb',
                  padding: '0.75rem',
                  borderRadius: '6px',
                  fontFamily: 'monospace',
                  fontSize: '0.875rem',
                  wordBreak: 'break-all',
                  color: '#374151'
                }}>
                  {table.tableLocation}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Schema - Full Width */}
        {table.schema && (
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
              Schema
            </h2>
            <pre style={{
              backgroundColor: '#f9fafb',
              padding: '1rem',
              borderRadius: '6px',
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              overflow: 'auto',
              maxHeight: '400px',
              color: '#374151',
              margin: 0
            }}>
              {JSON.stringify(JSON.parse(table.schema), null, 2)}
            </pre>
          </div>
        )}

        {/* Table Properties with Policies Section */}
        {(table.tableProperties && Object.keys(table.tableProperties).length > 0) || table.policies ? (
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
              Properties & Policies
            </h2>

            {/* Policies Section */}
            {table.policies && (
              <div style={{ marginBottom: '1.5rem' }}>
                <h3 style={{
                  fontSize: '1rem',
                  fontWeight: '600',
                  marginBottom: '0.75rem',
                  color: '#374151',
                  paddingBottom: '0.5rem',
                  borderBottom: '2px solid #e5e7eb'
                }}>
                  Policies
                </h3>
                <div style={{ 
                  display: 'grid', 
                  gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
                  gap: '1rem',
                  marginTop: '1rem'
                }}>
                  <CompactDetailRow 
                    label="Sharing Enabled" 
                    value={
                      <span style={{
                        padding: '0.25rem 0.75rem',
                        backgroundColor: table.policies.sharingEnabled ? '#d1fae5' : '#fee2e2',
                        color: table.policies.sharingEnabled ? '#065f46' : '#991b1b',
                        borderRadius: '9999px',
                        fontSize: '0.875rem',
                        fontWeight: '500'
                      }}>
                        {table.policies.sharingEnabled ? 'Yes' : 'No'}
                      </span>
                    } 
                  />
                  {table.policies.retention && (
                    <CompactDetailRow label="Retention" value={JSON.stringify(table.policies.retention)} />
                  )}
                  {table.policies.replication && (
                    <CompactDetailRow label="Replication" value={JSON.stringify(table.policies.replication)} />
                  )}
                  {table.policies.history && (
                    <CompactDetailRow label="History" value={JSON.stringify(table.policies.history)} />
                  )}
                </div>
              </div>
            )}

            {/* Table Properties */}
            {table.tableProperties && Object.keys(table.tableProperties).length > 0 && (
              <div>
                <h3 style={{
                  fontSize: '1rem',
                  fontWeight: '600',
                  marginBottom: '0.75rem',
                  color: '#374151',
                  paddingBottom: '0.5rem',
                  borderBottom: '2px solid #e5e7eb'
                }}>
                  Table Properties ({Object.keys(table.tableProperties).length})
                </h3>
                <div style={{ overflowX: 'auto', maxHeight: '400px', overflowY: 'auto', marginTop: '1rem' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                    <thead style={{ position: 'sticky', top: 0, backgroundColor: '#f9fafb', zIndex: 1 }}>
                      <tr>
                        <th style={{ 
                          padding: '0.75rem', 
                          textAlign: 'left', 
                          fontWeight: '600', 
                          color: '#374151',
                          borderBottom: '2px solid #e5e7eb'
                        }}>
                          Property
                        </th>
                        <th style={{ 
                          padding: '0.75rem', 
                          textAlign: 'left', 
                          fontWeight: '600', 
                          color: '#374151',
                          borderBottom: '2px solid #e5e7eb'
                        }}>
                          Value
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(table.tableProperties).map(([key, value]) => (
                        <tr key={key} style={{ borderBottom: '1px solid #e5e7eb' }}>
                          <td style={{ 
                            padding: '0.75rem', 
                            fontFamily: 'monospace', 
                            fontSize: '0.875rem',
                            color: '#374151',
                            fontWeight: '500',
                            maxWidth: '300px'
                          }}>
                            {key}
                          </td>
                          <td style={{ 
                            padding: '0.75rem', 
                            fontFamily: 'monospace', 
                            fontSize: '0.875rem',
                            color: '#6b7280',
                            wordBreak: 'break-word'
                          }}>
                            {value}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        ) : null}
      </div>
    </main>
  );
}

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: '200px 1fr',
      gap: '1rem',
      paddingBottom: '1rem',
      borderBottom: '1px solid #e5e7eb'
    }}>
      <dt style={{
        fontWeight: '600',
        color: '#374151',
        fontSize: '0.875rem',
        textTransform: 'uppercase',
        letterSpacing: '0.05em'
      }}>
        {label}
      </dt>
      <dd style={{
        color: '#1f2937',
        fontSize: '1rem',
        margin: 0
      }}>
        {value}
      </dd>
    </div>
  );
}

function CompactDetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
      paddingBottom: '0.75rem',
      borderBottom: '1px solid #f3f4f6'
    }}>
      <dt style={{
        fontWeight: '500',
        color: '#6b7280',
        fontSize: '0.875rem'
      }}>
        {label}
      </dt>
      <dd style={{
        color: '#1f2937',
        fontSize: '0.875rem',
        margin: 0,
        textAlign: 'right',
        maxWidth: '60%',
        wordBreak: 'break-word'
      }}>
        {value}
      </dd>
    </div>
  );
}

export default function TableDetailPage() {
  return (
    <Suspense fallback={
      <div style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontFamily: 'system-ui, -apple-system, sans-serif'
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: '50px',
            height: '50px',
            border: '4px solid #e5e7eb',
            borderTop: '4px solid #3b82f6',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite',
            margin: '0 auto 1rem'
          }}></div>
          <p style={{ color: '#6b7280' }}>Loading table details...</p>
        </div>
      </div>
    }>
      <TableDetailContent />
    </Suspense>
  );
}
