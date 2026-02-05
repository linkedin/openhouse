'use client';

import { useState, useEffect } from 'react';
import { fonts, fontSizes, fontWeights, colors } from '@/lib/theme';

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
          fontSize: fontSizes.xl,
          fontWeight: fontWeights.semibold,
          marginBottom: '1rem',
          color: colors.text.primary,
        }}
      >
        Permissions (ACL Policies)
      </h2>

      {loading && (
        <div style={{ textAlign: 'center', padding: '2rem', color: colors.text.muted }}>
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
            color: colors.text.muted,
            backgroundColor: colors.background.page,
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
                fontSize: fontSizes.sm,
                color: colors.text.muted,
                marginBottom: '0.75rem',
                padding: '0.5rem',
                backgroundColor: colors.background.page,
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
                    fontWeight: fontWeights.semibold,
                    color: colors.text.secondary,
                    fontSize: fontSizes.xs,
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
                    fontWeight: fontWeights.semibold,
                    color: colors.text.secondary,
                    fontSize: fontSizes.xs,
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
                    fontWeight: fontWeights.semibold,
                    color: colors.text.secondary,
                    fontSize: fontSizes.xs,
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
                    fontWeight: fontWeights.semibold,
                    color: colors.text.secondary,
                    fontSize: fontSizes.xs,
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
                      fontSize: fontSizes.sm,
                      color: colors.text.secondary,
                      fontFamily: fonts.mono,
                    }}
                  >
                    {policy.principal}
                  </td>
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: fontSizes.sm,
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
                            : colors.background.subtle,
                        color:
                          policy.role === 'TABLE_ADMIN'
                            ? '#1e40af'
                            : policy.role === 'TABLE_EDITOR'
                            ? '#065f46'
                            : policy.role === 'TABLE_VIEWER'
                            ? '#92400e'
                            : colors.text.secondary,
                        borderRadius: '9999px',
                        fontSize: fontSizes.xs,
                        fontWeight: fontWeights.medium,
                        textTransform: 'uppercase',
                      }}
                    >
                      {policy.role}
                    </span>
                  </td>
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: fontSizes.sm,
                      color: colors.text.muted,
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {formatDate(policy.expirationEpochTimeSeconds)}
                  </td>
                  <td
                    style={{
                      padding: '0.75rem',
                      fontSize: fontSizes.sm,
                      color: colors.text.muted,
                      fontFamily: fonts.mono,
                    }}
                  >
                    {policy.properties && Object.keys(policy.properties).length > 0 ? (
                      <pre
                        style={{
                          margin: 0,
                          fontSize: fontSizes.xs,
                          maxWidth: '300px',
                          overflow: 'auto',
                        }}
                      >
                        {JSON.stringify(policy.properties, null, 2)}
                      </pre>
                    ) : (
                      <span style={{ color: colors.text.disabled }}>None</span>
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
