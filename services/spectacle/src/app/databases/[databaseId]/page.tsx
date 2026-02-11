'use client';

import { useParams, useRouter } from 'next/navigation';
import { useEffect, useState } from 'react';
import Link from 'next/link';

interface Table {
  tableId: string;
  databaseId: string;
  clusterId?: string;
  tableUri: string;
  tableUUID?: string;
  tableLocation?: string;
  tableVersion?: string;
  tableType: string;
  tableCreator?: string;
  creationTime?: number;
  lastModifiedTime?: number;
}

interface AclPolicy {
  principal: string;
  role: string;
  operation: string;
}

export default function DatabasePage() {
  const params = useParams();
  const router = useRouter();
  const databaseId = params.databaseId as string;

  const [tables, setTables] = useState<Table[]>([]);
  const [aclPolicies, setAclPolicies] = useState<AclPolicy[]>([]);
  const [loading, setLoading] = useState(true);
  const [aclLoading, setAclLoading] = useState(true);
  const [error, setError] = useState('');
  const [aclError, setAclError] = useState('');

  useEffect(() => {
    fetchTables();
    fetchAclPolicies();
  }, [databaseId]);

  const fetchTables = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/tables/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to fetch tables');
      }

      const data = await response.json();
      const baseTables = data.results || [];
      
      // Fetch metadata for all tables in parallel to get UUID and timestamps
      const metadataPromises = baseTables.map(async (table: any) => {
        try {
          const metaResponse = await fetch('/api/tables/metadata', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
              databaseId: table.databaseId, 
              tableId: table.tableId 
            }),
          });
          
          if (metaResponse.ok) {
            const metadata = await metaResponse.json();
            // Parse the currentMetadata JSON to extract UUID and timestamps
            if (metadata.currentMetadata) {
              try {
                const parsed = JSON.parse(metadata.currentMetadata);
                return {
                  ...table,
                  tableUUID: parsed['table-uuid'] || table.tableId,
                  lastModifiedTime: parsed['last-updated-ms'],
                  creationTime: parsed['last-updated-ms'], // Iceberg doesn't have creation time, use last-updated
                };
              } catch (e) {
                console.error('Error parsing metadata for', table.tableId, e);
              }
            }
          }
        } catch (e) {
          console.error('Error fetching metadata for', table.tableId, e);
        }
        return table;
      });
      
      const enrichedTables = await Promise.all(metadataPromises);
      setTables(enrichedTables);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load tables');
      console.error('Tables error:', err);
    } finally {
      setLoading(false);
    }
  };

  const fetchAclPolicies = async () => {
    setAclLoading(true);
    setAclError('');

    try {
      const response = await fetch('/api/databases/acl-policies', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to fetch ACL policies');
      }

      const data = await response.json();
      setAclPolicies(data.results || []);
    } catch (err) {
      setAclError(err instanceof Error ? err.message : 'Failed to load ACL policies');
      console.error('ACL error:', err);
    } finally {
      setAclLoading(false);
    }
  };

  const formatDate = (timestamp: number | undefined) => {
    if (timestamp === undefined || timestamp === null) {
      return 'Not available';
    }
    if (timestamp === 0) {
      return 'Not set';
    }
    // If timestamp is in seconds (less than year 2000 in milliseconds), convert to milliseconds
    const timestampMs = timestamp < 10000000000 ? timestamp * 1000 : timestamp;
    return new Date(timestampMs).toLocaleString();
  };

  return (
    <main style={{
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif',
      backgroundColor: '#f9fafb'
    }}>
      <div style={{ maxWidth: '1400px', margin: '0 auto' }}>
        {/* Breadcrumb Navigation */}
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem',
          marginBottom: '2rem',
          padding: '0.75rem 1rem',
          backgroundColor: 'white',
          borderRadius: '8px',
          boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
          fontSize: '0.875rem'
        }}>
          <button
            onClick={() => router.push('/')}
            style={{
              padding: '0.25rem 0.5rem',
              backgroundColor: 'transparent',
              color: '#3b82f6',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '0.875rem',
              fontWeight: '500',
              transition: 'background-color 0.2s'
            }}
            onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#eff6ff'}
            onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
          >
            🏠 Home
          </button>
          <span style={{ color: '#d1d5db' }}>/</span>
          <span style={{
            color: '#374151',
            fontWeight: '600',
            fontFamily: 'monospace',
            padding: '0.25rem 0.5rem',
            backgroundColor: '#f3f4f6',
            borderRadius: '4px'
          }}>
            {databaseId}
          </span>
        </div>

        {/* Header */}
        <div style={{ marginBottom: '2rem' }}>

          <h1 style={{
            fontSize: '2rem',
            fontWeight: '700',
            color: '#111827',
            marginBottom: '0.5rem'
          }}>
            Database: {databaseId}
          </h1>

          <div style={{
            fontSize: '1rem',
            color: '#6b7280'
          }}>
            {tables.length} {tables.length === 1 ? 'table' : 'tables'}
          </div>
        </div>

        {/* Two Column Layout */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'minmax(0, 3fr) minmax(0, 2fr)',
          gap: '1.5rem',
          marginBottom: '1.5rem'
        }}>
          {/* Tables List - Left Column */}
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
              Tables
            </h2>

            {loading && (
              <div style={{ textAlign: 'center', padding: '2rem', color: '#6b7280' }}>
                Loading tables...
              </div>
            )}

            {error && (
              <div style={{
                backgroundColor: '#fef2f2',
                color: '#991b1b',
                padding: '1rem',
                borderRadius: '6px',
                marginBottom: '1rem'
              }}>
                {error}
              </div>
            )}

            {!loading && !error && tables.length === 0 && (
              <div style={{
                textAlign: 'center',
                padding: '2rem',
                color: '#6b7280'
              }}>
                No tables found in this database
              </div>
            )}

            {!loading && tables.length > 0 && (
              <div style={{
                display: 'flex',
                flexDirection: 'column',
                gap: '0.75rem',
                maxHeight: '600px',
                overflowY: 'auto'
              }}>
                {tables.map((table) => (
                  <Link
                    key={table.tableId}
                    href={`/tables/${table.databaseId}/${table.tableId}`}
                    style={{
                      textDecoration: 'none',
                      display: 'block'
                    }}
                  >
                    <div style={{
                      padding: '1rem',
                      border: '1px solid #e5e7eb',
                      borderRadius: '6px',
                      backgroundColor: '#f9fafb',
                      cursor: 'pointer',
                      transition: 'all 0.2s',
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.borderColor = '#3b82f6';
                      e.currentTarget.style.backgroundColor = '#eff6ff';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.borderColor = '#e5e7eb';
                      e.currentTarget.style.backgroundColor = '#f9fafb';
                    }}>
                      <div style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        marginBottom: '0.5rem'
                      }}>
                        <div style={{
                          fontSize: '1rem',
                          fontWeight: '600',
                          color: '#1f2937'
                        }}>
                          {table.tableId}
                        </div>
                        <span style={{
                          padding: '0.125rem 0.5rem',
                          backgroundColor: '#dbeafe',
                          color: '#1e40af',
                          borderRadius: '9999px',
                          fontSize: '0.75rem',
                          fontWeight: '500'
                        }}>
                          {table.tableType}
                        </span>
                      </div>

                      <div style={{
                        fontSize: '0.875rem',
                        color: '#6b7280',
                        marginTop: '0.5rem'
                      }}>
                        UUID: {table.tableUUID || 'Loading...'}
                      </div>

                      <div style={{
                        fontSize: '0.875rem',
                        color: '#6b7280',
                        marginTop: '0.25rem'
                      }}>
                        Last Updated: {formatDate(table.lastModifiedTime)}
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </div>

          {/* Database ACL Policies - Right Column */}
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
              Database Permissions
            </h2>

            {aclLoading && (
              <div style={{ textAlign: 'center', padding: '2rem', color: '#6b7280' }}>
                Loading permissions...
              </div>
            )}

            {aclError && (
              <div style={{
                backgroundColor: '#fef2f2',
                color: '#991b1b',
                padding: '1rem',
                borderRadius: '6px',
                marginBottom: '1rem'
              }}>
                {aclError}
              </div>
            )}

            {!aclLoading && !aclError && aclPolicies.length === 0 && (
              <div style={{
                backgroundColor: '#fef9c3',
                color: '#854d0e',
                padding: '1rem',
                borderRadius: '6px'
              }}>
                No ACL policies configured for this database
              </div>
            )}

            {!aclLoading && aclPolicies.length > 0 && (
              <div style={{
                maxHeight: '600px',
                overflowY: 'auto'
              }}>
                <table style={{
                  width: '100%',
                  borderCollapse: 'collapse'
                }}>
                  <thead style={{
                    position: 'sticky',
                    top: 0,
                    backgroundColor: '#f9fafb',
                    zIndex: 1
                  }}>
                    <tr>
                      <th style={{
                        padding: '0.75rem',
                        textAlign: 'left',
                        fontWeight: '600',
                        fontSize: '0.875rem',
                        color: '#374151',
                        borderBottom: '2px solid #e5e7eb'
                      }}>
                        Principal
                      </th>
                      <th style={{
                        padding: '0.75rem',
                        textAlign: 'left',
                        fontWeight: '600',
                        fontSize: '0.875rem',
                        color: '#374151',
                        borderBottom: '2px solid #e5e7eb'
                      }}>
                        Role
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {aclPolicies.map((policy, index) => (
                      <tr key={index} style={{
                        borderBottom: '1px solid #f3f4f6'
                      }}>
                        <td style={{
                          padding: '0.75rem',
                          fontSize: '0.875rem',
                          color: '#374151',
                          fontFamily: 'monospace'
                        }}>
                          {policy.principal}
                        </td>
                        <td style={{
                          padding: '0.75rem',
                          fontSize: '0.875rem'
                        }}>
                          <span style={{
                            padding: '0.25rem 0.75rem',
                            backgroundColor: '#dbeafe',
                            color: '#1e40af',
                            borderRadius: '9999px',
                            fontSize: '0.75rem',
                            fontWeight: '500'
                          }}>
                            {policy.role}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}
