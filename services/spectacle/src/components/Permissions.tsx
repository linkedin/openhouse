'use client';

import { useState, useEffect } from 'react';

interface AclPolicy {
  principal: string;
  role: string;
  expirationEpochTimeSeconds?: number;
  properties?: Record<string, string>;
}

interface PermissionsProps {
  databaseId: string;
  tableId: string;
}

export default function Permissions({ databaseId, tableId }: PermissionsProps) {
  const [policies, setPolicies] = useState<AclPolicy[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchPermissions();
  }, [databaseId, tableId]);

  const fetchPermissions = async () => {
    setLoading(true);
    setError('');

    try {
      const response = await fetch('/api/tables/permissions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId, tableId }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to fetch ACL policies');
      }

      const data = await response.json();
      setPolicies(data.results || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      console.error('Permissions fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (epochSeconds?: number) => {
    if (!epochSeconds) return 'Never';
    return new Date(epochSeconds * 1000).toLocaleString();
  };

  return (
    <div
      style={{
        backgroundColor: 'white',
        padding: '1.5rem',
        borderRadius: '8px',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        marginBottom: '1.5rem',
      }}
    >
      <h2
        style={{
          fontSize: '1.25rem',
          fontWeight: '600',
          marginBottom: '1rem',
          color: '#1f2937',
        }}
      >
        Permissions (ACL Policies)
      </h2>

      {loading && (
        <div style={{ textAlign: 'center', padding: '2rem', color: '#6b7280' }}>
          Loading permissions...
        </div>
      )}

      {error && (
        <div
          style={{
            backgroundColor: '#fee2e2',
            color: '#991b1b',
            padding: '1rem',
            borderRadius: '6px',
            marginBottom: '1rem',
          }}
        >
          <strong>Error:</strong> {error}
        </div>
      )}

      {!loading && !error && policies.length === 0 && (
        <div
          style={{
            textAlign: 'center',
            padding: '2rem',
            color: '#6b7280',
            backgroundColor: '#f9fafb',
            borderRadius: '6px',
          }}
        >
          No ACL policies found for this table.
        </div>
      )}

      {!loading && !error && policies.length > 0 && (
        <>
          {policies.length > 20 && (
            <div
              style={{
                fontSize: '0.875rem',
                color: '#6b7280',
                marginBottom: '0.75rem',
                padding: '0.5rem',
                backgroundColor: '#f9fafb',
                borderRadius: '6px',
              }}
            >
              Showing {policies.length} permissions (scrollable)
            </div>
          )}
          <div
            style={{
              overflowX: 'auto',
              overflowY: policies.length > 20 ? 'scroll' : 'visible',
              maxHeight: policies.length > 20 ? '600px' : 'none',
              border: policies.length > 20 ? '1px solid #e5e7eb' : 'none',
              borderRadius: '6px',
            }}
            className="custom-scrollbar"
          >
            <table
              style={{
                width: '100%',
                borderCollapse: 'collapse',
              }}
            >
              <thead>
                <tr
                  style={{
                    backgroundColor: '#f9fafb',
                    borderBottom: '2px solid #e5e7eb',
                    position: policies.length > 20 ? 'sticky' : 'static',
                    top: 0,
                    zIndex: 1,
                  }}
                >
                <th
                  style={{
                    padding: '0.75rem',
                    textAlign: 'left',
                    fontWeight: '600',
                    color: '#374151',
                    fontSize: '0.875rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.05em',
                  }}
                >
                  Principal
                </th>
                <th
                  style={{
                    padding: '0.75rem',
                    textAlign: 'left',
                    fontWeight: '600',
                    color: '#374151',
                    fontSize: '0.875rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.05em',
                  }}
                >
                  Role
                </th>
                <th
                  style={{
                    padding: '0.75rem',
                    textAlign: 'left',
                    fontWeight: '600',
                    color: '#374151',
                    fontSize: '0.875rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.05em',
                  }}
                >
                  Expiration
                </th>
                <th
                  style={{
                    padding: '0.75rem',
                    textAlign: 'left',
                    fontWeight: '600',
                    color: '#374151',
                    fontSize: '0.875rem',
                    textTransform: 'uppercase',
                    letterSpacing: '0.05em',
                  }}
                >
                  Properties
                </th>
              </tr>
            </thead>
            <tbody>
              {policies.map((policy, index) => (
                <tr
                  key={index}
                  style={{
                    borderBottom: '1px solid #e5e7eb',
                  }}
                >
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: '0.875rem',
                      color: '#374151',
                      fontFamily: 'monospace',
                    }}
                  >
                    {policy.principal}
                  </td>
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: '0.875rem',
                    }}
                  >
                    <span
                      style={{
                        padding: '0.25rem 0.75rem',
                        backgroundColor:
                          policy.role === 'TABLE_ADMIN'
                            ? '#dbeafe'
                            : policy.role === 'TABLE_EDITOR'
                            ? '#d1fae5'
                            : policy.role === 'TABLE_VIEWER'
                            ? '#fef3c7'
                            : '#f3f4f6',
                        color:
                          policy.role === 'TABLE_ADMIN'
                            ? '#1e40af'
                            : policy.role === 'TABLE_EDITOR'
                            ? '#065f46'
                            : policy.role === 'TABLE_VIEWER'
                            ? '#92400e'
                            : '#374151',
                        borderRadius: '9999px',
                        fontSize: '0.75rem',
                        fontWeight: '500',
                        textTransform: 'uppercase',
                      }}
                    >
                      {policy.role}
                    </span>
                  </td>
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: '0.875rem',
                      color: '#6b7280',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {formatDate(policy.expirationEpochTimeSeconds)}
                  </td>
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: '0.875rem',
                      color: '#6b7280',
                      fontFamily: 'monospace',
                    }}
                  >
                    {policy.properties && Object.keys(policy.properties).length > 0 ? (
                      <pre
                        style={{
                          margin: 0,
                          fontSize: '0.75rem',
                          maxWidth: '300px',
                          overflow: 'auto',
                        }}
                      >
                        {JSON.stringify(policy.properties, null, 2)}
                      </pre>
                    ) : (
                      <span style={{ color: '#9ca3af' }}>None</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        </>
      )}
    </div>
  );
}
