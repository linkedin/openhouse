'use client';

import { useEffect, useState } from 'react';
import { computeMetadataDiff, MetadataDiff } from '@/lib/metadataDiff';

interface MetadataDiffModalProps {
  isOpen: boolean;
  onClose: () => void;
  databaseId: string;
  tableId: string;
  snapshotId: number | string;
}

export default function MetadataDiffModal({
  isOpen,
  onClose,
  databaseId,
  tableId,
  snapshotId,
}: MetadataDiffModalProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [diff, setDiff] = useState<MetadataDiff | null>(null);
  const [diffData, setDiffData] = useState<any>(null);

  useEffect(() => {
    if (isOpen && snapshotId) {
      fetchDiff();
    }
  }, [isOpen, snapshotId]);

  const fetchDiff = async () => {
    setLoading(true);
    setError('');
    setDiff(null);

    try {
      console.log('Fetching diff for:', { databaseId, tableId, snapshotId, type: typeof snapshotId });
      
      // Keep snapshotId as string to preserve precision for large numbers
      const cleanSnapshotId = String(snapshotId);
      console.log('Clean snapshot ID (as string):', cleanSnapshotId);
      
      const response = await fetch(
        `/api/tables/metadata-diff?databaseId=${encodeURIComponent(databaseId)}&tableId=${encodeURIComponent(tableId)}&snapshotId=${cleanSnapshotId}`
      );

      console.log('Response status:', response.status);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
        console.error('Error response:', errorData);
        const errorMessage = [
          `API Error: ${response.status}`,
          errorData.error,
          errorData.details,
          errorData.backendUrl ? `Backend URL: ${errorData.backendUrl}` : null
        ].filter(Boolean).join('\n');
        throw new Error(errorMessage);
      }

      const data = await response.json();
      console.log('Received data:', data);
      setDiffData(data);

      // Compute the semantic diff
      const computedDiff = computeMetadataDiff(data.currentMetadata, data.previousMetadata);
      console.log('Computed diff:', computedDiff);
      setDiff(computedDiff);
    } catch (err) {
      console.error('Fetch error:', err);
      setError(err instanceof Error ? err.message : 'Failed to load metadata diff');
    } finally {
      setLoading(false);
    }
  };

  if (!isOpen) return null;

  const formatTimestamp = (ts: number | null) => {
    if (!ts) return 'N/A';
    return new Date(ts).toLocaleString();
  };

  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
        padding: '2rem',
      }}
      onClick={onClose}
    >
      <div
        style={{
          backgroundColor: 'white',
          borderRadius: '12px',
          maxWidth: '1200px',
          width: '100%',
          maxHeight: '90vh',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
          boxShadow: '0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            padding: '1.5rem',
            borderBottom: '1px solid #e5e7eb',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <h2 style={{ fontSize: '1.5rem', fontWeight: '600', color: '#111827', margin: 0 }}>
            Metadata Diff - Snapshot {snapshotId}
          </h2>
          <button
            onClick={onClose}
            style={{
              padding: '0.5rem',
              backgroundColor: 'transparent',
              border: 'none',
              cursor: 'pointer',
              fontSize: '1.5rem',
              color: '#6b7280',
              lineHeight: 1,
            }}
          >
            ×
          </button>
        </div>

        {/* Content */}
        <div style={{ flex: 1, overflow: 'auto', padding: '1.5rem' }}>
          {loading && (
            <div style={{ textAlign: 'center', padding: '3rem', color: '#6b7280' }}>
              Loading metadata diff...
            </div>
          )}

          {error && (
            <div
              style={{
                backgroundColor: '#fee2e2',
                color: '#991b1b',
                padding: '1rem',
                borderRadius: '6px',
              }}
            >
              <div style={{ fontWeight: '600', marginBottom: '0.5rem' }}>Error</div>
              <pre style={{ 
                whiteSpace: 'pre-wrap', 
                wordBreak: 'break-word',
                fontFamily: 'monospace',
                fontSize: '0.875rem',
                margin: 0
              }}>
                {error}
              </pre>
            </div>
          )}

          {diff && diffData && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
              {/* Version Info */}
              <div
                style={{
                  padding: '1rem',
                  backgroundColor: '#f9fafb',
                  borderRadius: '8px',
                  border: '1px solid #e5e7eb',
                }}
              >
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                  <div>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem' }}>
                      Current Snapshot
                    </div>
                    <div style={{ fontFamily: 'monospace', fontSize: '0.875rem', fontWeight: '600' }}>
                      {diffData.currentSnapshotId}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginTop: '0.25rem' }}>
                      {formatTimestamp(diffData.currentTimestamp)}
                    </div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem' }}>
                      Previous Snapshot
                    </div>
                    <div style={{ fontFamily: 'monospace', fontSize: '0.875rem', fontWeight: '600' }}>
                      {diffData.isFirstCommit ? 'N/A (First Commit)' : diffData.previousSnapshotId}
                    </div>
                    {!diffData.isFirstCommit && (
                      <div style={{ fontSize: '0.75rem', color: '#6b7280', marginTop: '0.25rem' }}>
                        {formatTimestamp(diffData.previousTimestamp)}
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Summary Statistics */}
              {!diffData.isFirstCommit && (
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                  gap: '1rem'
                }}>
                  {/* Schema Changes Summary */}
                  <div style={{
                    padding: '1rem',
                    backgroundColor: diff.schemaChanges.addedFields.length > 0 || diff.schemaChanges.removedFields.length > 0 || diff.schemaChanges.modifiedFields.length > 0 ? '#f0fdf4' : '#f9fafb',
                    borderRadius: '8px',
                    border: `1px solid ${diff.schemaChanges.addedFields.length > 0 || diff.schemaChanges.removedFields.length > 0 || diff.schemaChanges.modifiedFields.length > 0 ? '#86efac' : '#e5e7eb'}`,
                  }}>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.5rem', fontWeight: '500' }}>
                      Schema Changes
                    </div>
                    <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#111827' }}>
                      {diff.schemaChanges.addedFields.length + diff.schemaChanges.removedFields.length + diff.schemaChanges.modifiedFields.length}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginTop: '0.25rem' }}>
                      {diff.schemaChanges.addedFields.length > 0 && `+${diff.schemaChanges.addedFields.length} added `}
                      {diff.schemaChanges.removedFields.length > 0 && `-${diff.schemaChanges.removedFields.length} removed `}
                      {diff.schemaChanges.modifiedFields.length > 0 && `~${diff.schemaChanges.modifiedFields.length} modified`}
                    </div>
                  </div>

                  {/* Partition Spec Changes */}
                  <div style={{
                    padding: '1rem',
                    backgroundColor: diff.partitionSpecChanges.changed ? '#fef3c7' : '#f9fafb',
                    borderRadius: '8px',
                    border: `1px solid ${diff.partitionSpecChanges.changed ? '#fde047' : '#e5e7eb'}`,
                  }}>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.5rem', fontWeight: '500' }}>
                      Partition Spec
                    </div>
                    <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#111827' }}>
                      {diff.partitionSpecChanges.changed ? 'Changed' : 'Unchanged'}
                    </div>
                    {diff.partitionSpecChanges.changed && (
                      <div style={{ fontSize: '0.75rem', color: '#6b7280', marginTop: '0.25rem' }}>
                        Spec ID: {diff.partitionSpecChanges.previousSpecId} → {diff.partitionSpecChanges.currentSpecId}
                      </div>
                    )}
                  </div>

                  {/* Property Changes */}
                  <div style={{
                    padding: '1rem',
                    backgroundColor: Object.keys(diff.propertyChanges.added).length > 0 || Object.keys(diff.propertyChanges.removed).length > 0 || Object.keys(diff.propertyChanges.modified).length > 0 ? '#dbeafe' : '#f9fafb',
                    borderRadius: '8px',
                    border: `1px solid ${Object.keys(diff.propertyChanges.added).length > 0 || Object.keys(diff.propertyChanges.removed).length > 0 || Object.keys(diff.propertyChanges.modified).length > 0 ? '#93c5fd' : '#e5e7eb'}`,
                  }}>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.5rem', fontWeight: '500' }}>
                      Property Changes
                    </div>
                    <div style={{ fontSize: '1.5rem', fontWeight: '700', color: '#111827' }}>
                      {Object.keys(diff.propertyChanges.added).length + Object.keys(diff.propertyChanges.removed).length + Object.keys(diff.propertyChanges.modified).length}
                    </div>
                    <div style={{ fontSize: '0.75rem', color: '#6b7280', marginTop: '0.25rem' }}>
                      {Object.keys(diff.propertyChanges.added).length > 0 && `+${Object.keys(diff.propertyChanges.added).length} added `}
                      {Object.keys(diff.propertyChanges.removed).length > 0 && `-${Object.keys(diff.propertyChanges.removed).length} removed `}
                      {Object.keys(diff.propertyChanges.modified).length > 0 && `~${Object.keys(diff.propertyChanges.modified).length} modified`}
                    </div>
                  </div>
                </div>
              )}

              {diffData.isFirstCommit && (
                <div
                  style={{
                    padding: '1rem',
                    backgroundColor: '#dbeafe',
                    color: '#1e40af',
                    borderRadius: '8px',
                    textAlign: 'center',
                  }}
                >
                  This is the first commit - no previous metadata to compare
                </div>
              )}

              {/* Schema Changes */}
              {(diff.schemaChanges.addedFields.length > 0 ||
                diff.schemaChanges.removedFields.length > 0 ||
                diff.schemaChanges.modifiedFields.length > 0) && (
                <DiffSection title="Schema Changes">
                  {diff.schemaChanges.addedFields.length > 0 && (
                    <ChangeGroup title="Added Fields" type="added">
                      {diff.schemaChanges.addedFields.map((field) => (
                        <FieldItem key={field.id} field={field} />
                      ))}
                    </ChangeGroup>
                  )}

                  {diff.schemaChanges.removedFields.length > 0 && (
                    <ChangeGroup title="Removed Fields" type="removed">
                      {diff.schemaChanges.removedFields.map((field) => (
                        <FieldItem key={field.id} field={field} />
                      ))}
                    </ChangeGroup>
                  )}

                  {diff.schemaChanges.modifiedFields.length > 0 && (
                    <ChangeGroup title="Modified Fields" type="modified">
                      {diff.schemaChanges.modifiedFields.map((mod) => (
                        <div key={mod.field.id} style={{ marginBottom: '0.5rem' }}>
                          <FieldItem field={mod.field} />
                          <div style={{ marginLeft: '1rem', marginTop: '0.25rem' }}>
                            {mod.changes.map((change, idx) => (
                              <div
                                key={idx}
                                style={{
                                  fontSize: '0.75rem',
                                  color: '#6b7280',
                                  fontFamily: 'monospace',
                                }}
                              >
                                {change}
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </ChangeGroup>
                  )}
                </DiffSection>
              )}

              {/* Partition Spec Changes */}
              {diff.partitionSpecChanges.changed && (
                <DiffSection title="Partition Specification Changes">
                  <div
                    style={{
                      padding: '1rem',
                      backgroundColor: '#fef3c7',
                      borderRadius: '6px',
                      border: '1px solid #fde68a',
                    }}
                  >
                    <div style={{ fontSize: '0.875rem', color: '#92400e', marginBottom: '0.5rem' }}>
                      Partition spec changed from ID {diff.partitionSpecChanges.previousSpecId} to{' '}
                      {diff.partitionSpecChanges.currentSpecId}
                    </div>
                    <details>
                      <summary style={{ cursor: 'pointer', fontSize: '0.875rem', color: '#92400e' }}>
                        View Details
                      </summary>
                      <pre
                        style={{
                          fontSize: '0.75rem',
                          marginTop: '0.5rem',
                          padding: '0.5rem',
                          backgroundColor: 'white',
                          borderRadius: '4px',
                          overflow: 'auto',
                        }}
                      >
                        {JSON.stringify(diff.partitionSpecChanges, null, 2)}
                      </pre>
                    </details>
                  </div>
                </DiffSection>
              )}

              {/* Property Changes */}
              {(Object.keys(diff.propertyChanges.added).length > 0 ||
                Object.keys(diff.propertyChanges.removed).length > 0 ||
                Object.keys(diff.propertyChanges.modified).length > 0) && (
                <DiffSection title="Property Changes">
                  {Object.keys(diff.propertyChanges.added).length > 0 && (
                    <ChangeGroup title="Added Properties" type="added">
                      {Object.entries(diff.propertyChanges.added).map(([key, value]) => (
                        <PropertyItem key={key} name={key} value={value} />
                      ))}
                    </ChangeGroup>
                  )}

                  {Object.keys(diff.propertyChanges.removed).length > 0 && (
                    <ChangeGroup title="Removed Properties" type="removed">
                      {Object.entries(diff.propertyChanges.removed).map(([key, value]) => (
                        <PropertyItem key={key} name={key} value={value} />
                      ))}
                    </ChangeGroup>
                  )}

                  {Object.keys(diff.propertyChanges.modified).length > 0 && (
                    <ChangeGroup title="Modified Properties" type="modified">
                      {Object.entries(diff.propertyChanges.modified).map(([key, change]) => (
                        <div key={key} style={{ marginBottom: '0.75rem' }}>
                          <div
                            style={{
                              fontSize: '0.875rem',
                              fontWeight: '600',
                              fontFamily: 'monospace',
                              color: '#92400e',
                              marginBottom: '0.5rem',
                            }}
                          >
                            {key}
                          </div>
                          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                            <div
                              style={{
                                padding: '0.5rem',
                                backgroundColor: '#fee2e2',
                                color: '#991b1b',
                                borderRadius: '4px',
                                fontSize: '0.75rem',
                                fontFamily: 'monospace',
                                wordBreak: 'break-all',
                              }}
                            >
                              <span style={{ fontWeight: '600' }}>Old:</span> {change.oldValue}
                            </div>
                            <div
                              style={{
                                padding: '0.5rem',
                                backgroundColor: '#d1fae5',
                                color: '#065f46',
                                borderRadius: '4px',
                                fontSize: '0.75rem',
                                fontFamily: 'monospace',
                                wordBreak: 'break-all',
                              }}
                            >
                              <span style={{ fontWeight: '600' }}>New:</span> {change.newValue}
                            </div>
                          </div>
                        </div>
                      ))}
                    </ChangeGroup>
                  )}
                </DiffSection>
              )}

              {/* Snapshot Pointer Change */}
              {diff.snapshotChanges.snapshotIdChanged && (
                <DiffSection title="Snapshot Pointer">
                  <div
                    style={{
                      padding: '1rem',
                      backgroundColor: '#dbeafe',
                      borderRadius: '6px',
                      border: '1px solid #bfdbfe',
                    }}
                  >
                    <div style={{ fontSize: '0.875rem', color: '#1e40af' }}>
                      Current snapshot changed from {diff.snapshotChanges.previousSnapshotId} to{' '}
                      {diff.snapshotChanges.currentSnapshotId}
                    </div>
                  </div>
                </DiffSection>
              )}

              {/* No Changes */}
              {!diffData.isFirstCommit &&
                diff.schemaChanges.addedFields.length === 0 &&
                diff.schemaChanges.removedFields.length === 0 &&
                diff.schemaChanges.modifiedFields.length === 0 &&
                !diff.partitionSpecChanges.changed &&
                Object.keys(diff.propertyChanges.added).length === 0 &&
                Object.keys(diff.propertyChanges.removed).length === 0 &&
                Object.keys(diff.propertyChanges.modified).length === 0 &&
                !diff.snapshotChanges.snapshotIdChanged && (
                  <div
                    style={{
                      padding: '2rem',
                      textAlign: 'center',
                      color: '#6b7280',
                      fontSize: '0.875rem',
                    }}
                  >
                    No semantic changes detected between these snapshots
                  </div>
                )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function DiffSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h3
        style={{
          fontSize: '1rem',
          fontWeight: '600',
          marginBottom: '0.75rem',
          color: '#374151',
        }}
      >
        {title}
      </h3>
      {children}
    </div>
  );
}

function ChangeGroup({
  title,
  type,
  children,
}: {
  title: string;
  type: 'added' | 'removed' | 'modified';
  children: React.ReactNode;
}) {
  const colors = {
    added: { bg: '#d1fae5', text: '#065f46' },
    removed: { bg: '#fee2e2', text: '#991b1b' },
    modified: { bg: '#fef3c7', text: '#92400e' },
  };

  return (
    <div style={{ marginBottom: '1rem' }}>
      <div
        style={{
          fontSize: '0.875rem',
          fontWeight: '500',
          color: colors[type].text,
          marginBottom: '0.5rem',
        }}
      >
        {title}
      </div>
      <div
        style={{
          padding: '0.75rem',
          backgroundColor: colors[type].bg,
          borderRadius: '6px',
        }}
      >
        {children}
      </div>
    </div>
  );
}

function FieldItem({ field }: { field: any }) {
  return (
    <div
      style={{
        fontSize: '0.875rem',
        fontFamily: 'monospace',
        marginBottom: '0.25rem',
      }}
    >
      <span style={{ fontWeight: '600' }}>{field.name}</span>
      <span style={{ color: '#6b7280' }}> ({field.type})</span>
      {field.required && <span style={{ color: '#991b1b', marginLeft: '0.5rem' }}>*required</span>}
    </div>
  );
}

function PropertyItem({ name, value }: { name: string; value: string }) {
  return (
    <div
      style={{
        fontSize: '0.875rem',
        fontFamily: 'monospace',
        marginBottom: '0.25rem',
        wordBreak: 'break-all',
      }}
    >
      <span style={{ fontWeight: '600' }}>{name}:</span> {value}
    </div>
  );
}
