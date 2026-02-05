'use client';

import { useParams, useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useState, Suspense } from 'react';
import { Table } from '@/types/table';
import Maintenance from '@/components/Maintenance';
import Permissions from '@/components/Permissions';
import MetadataDiffModal from '@/components/MetadataDiffModal';
import DataPreview from '@/components/DataPreview';

interface IcebergMetadata {
  tableId: string;
  databaseId: string;
  currentMetadata: string;
  metadataLog: string | null;
  metadataHistory: Array<{
    version: number;
    file: string;
    timestamp: number;
    location: string;
  }> | null;
  metadataLocation: string;
  snapshots: string | null;
  partitions: string | null;
  currentSnapshotId: number | string | null;
}

interface SnapshotCardProps {
  snapshotId: number | string;
  operation: string;
  timestamp: string;
  summary: Record<string, string>;
  isCurrent: boolean;
}

function SnapshotCard({ snapshotId, operation, timestamp, summary, isCurrent }: SnapshotCardProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Defensive checks
  if (snapshotId === undefined || snapshotId === null) {
    return null;
  }

  const formatValue = (value: string | number) => {
    if (value === 'N/A' || value === undefined || value === null) return 'N/A';
    return Number(value).toLocaleString();
  };

  const formatBytes = (bytes: string | number) => {
    if (bytes === 'N/A' || bytes === undefined || bytes === null) return 'N/A';
    const num = Number(bytes);
    if (num === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(num) / Math.log(k));
    return Math.round((num / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  // Extract metadata fields from summary
  const sparkAppId = summary['spark.app.id'] || summary['spark-app-id'] || 'N/A';
  const addedRecords = summary['added-records'] || 'N/A';
  const addedFiles = summary['added-data-files'] || 'N/A';
  const addedFilesSize = summary['added-files-size'] || 'N/A';
  const changedPartitionCount = summary['changed-partition-count'] || 'N/A';
  const totalRecords = summary['total-records'] || 'N/A';
  const totalFiles = summary['total-data-files'] || 'N/A';
  const totalFilesSize = summary['total-files-size'] || 'N/A';
  const totalDeleteFiles = summary['total-delete-files'] || 'N/A';
  const totalPositionDeletes = summary['total-position-deletes'] || 'N/A';
  const totalEqualityDeletes = summary['total-equality-deletes'] || 'N/A';

  const MetricItem = ({ label, value }: { label: string; value: string }) => (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0.5rem 0', borderBottom: '1px solid #f3f4f6' }}>
      <span style={{ fontSize: '0.875rem', color: '#6b7280' }}>{label}</span>
      <span style={{ fontSize: '0.875rem', fontWeight: '500', color: '#374151', fontFamily: 'monospace' }}>{value}</span>
    </div>
  );

  return (
    <div style={{
      border: isCurrent ? '2px solid #3b82f6' : '1px solid #e5e7eb',
      borderRadius: '8px',
      backgroundColor: isCurrent ? '#eff6ff' : 'white',
      transition: 'all 0.2s ease-in-out',
      boxShadow: isCurrent ? '0 4px 6px -1px rgba(59, 130, 246, 0.1), 0 2px 4px -1px rgba(59, 130, 246, 0.06)' : 'none'
    }}>
      {/* Card Header - Always Visible */}
      <div
        onClick={() => setIsExpanded(!isExpanded)}
        style={{
          padding: '0.7rem',
          cursor: 'pointer',
          display: 'grid',
          gridTemplateColumns: '1fr 110px 180px 30px',
          gap: '0.5rem',
          alignItems: 'center',
          backgroundColor: isCurrent ? '#dbeafe' : '#f9fafb',
          borderRadius: isExpanded ? '8px 8px 0 0' : '8px',
          minHeight: '39px'
        }}
      >
        {/* Left: Snapshot ID with CURRENT badge */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          <span style={{
            fontFamily: 'monospace',
            fontSize: '0.875rem',
            fontWeight: '600',
            color: isCurrent ? '#1e40af' : '#374151'
          }}>
            {snapshotId || 'Unknown ID'}
          </span>
          {isCurrent && (
            <span style={{
              padding: '0.125rem 0.5rem',
              backgroundColor: '#3b82f6',
              color: 'white',
              borderRadius: '9999px',
              fontSize: '0.75rem',
              fontWeight: '500',
              whiteSpace: 'nowrap'
            }}>
              CURRENT
            </span>
          )}
        </div>

        {/* Operation Badge - Fixed Width */}
        <span style={{
          padding: '0.25rem 0.75rem',
          backgroundColor: operation === 'append' ? '#d1fae5' : operation === 'overwrite' ? '#fef3c7' : '#e5e7eb',
          color: operation === 'append' ? '#065f46' : operation === 'overwrite' ? '#92400e' : '#374151',
          borderRadius: '9999px',
          fontSize: '0.75rem',
          fontWeight: '500',
          textTransform: 'uppercase',
          minWidth: '90px',
          maxWidth: '130px',
          textAlign: 'center',
          whiteSpace: 'nowrap',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          display: 'inline-block'
        }}>
          {operation || 'unknown'}
        </span>

        {/* Timestamp */}
        <span style={{
          fontSize: '0.875rem',
          color: '#6b7280',
          whiteSpace: 'nowrap',
          overflow: 'hidden',
          textOverflow: 'ellipsis'
        }}>
          {timestamp || 'Unknown time'}
        </span>

        {/* Expand Arrow */}
        <span style={{
          fontSize: '0.875rem',
          color: '#9ca3af',
          transform: isExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
          transition: 'transform 0.2s',
          display: 'flex',
          justifyContent: 'center',
          width: '20px'
        }}>
          ▼
        </span>
      </div>

      {/* Expandable Content */}
      {isExpanded && (
        <div style={{
          padding: '1.25rem',
          maxHeight: '500px',
          overflowY: 'auto',
          backgroundColor: 'white',
          borderTop: '1px solid #e5e7eb',
          borderRadius: '0 0 8px 8px'
        }}>
          {/* Data Changes Section */}
          <div style={{ marginBottom: '1.5rem' }}>
            <h4 style={{ fontSize: '0.875rem', fontWeight: '600', color: '#374151', marginBottom: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Data Changes
            </h4>
            <MetricItem label="Added Records" value={formatValue(addedRecords)} />
            <MetricItem label="Added Data Files" value={formatValue(addedFiles)} />
            <MetricItem label="Added Files Size" value={formatBytes(addedFilesSize)} />
            <MetricItem label="Changed Partition Count" value={formatValue(changedPartitionCount)} />
          </div>

          {/* Total Statistics Section */}
          <div style={{ marginBottom: '1.5rem' }}>
            <h4 style={{ fontSize: '0.875rem', fontWeight: '600', color: '#374151', marginBottom: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Total Statistics
            </h4>
            <MetricItem label="Total Records" value={formatValue(totalRecords)} />
            <MetricItem label="Total Data Files" value={formatValue(totalFiles)} />
            <MetricItem label="Total Files Size" value={formatBytes(totalFilesSize)} />
          </div>

          {/* Delete Operations Section */}
          <div style={{ marginBottom: '1.5rem' }}>
            <h4 style={{ fontSize: '0.875rem', fontWeight: '600', color: '#374151', marginBottom: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Delete Operations
            </h4>
            <MetricItem label="Total Delete Files" value={formatValue(totalDeleteFiles)} />
            <MetricItem label="Total Position Deletes" value={formatValue(totalPositionDeletes)} />
            <MetricItem label="Total Equality Deletes" value={formatValue(totalEqualityDeletes)} />
          </div>

          {/* Execution Details Section */}
          <div>
            <h4 style={{ fontSize: '0.875rem', fontWeight: '600', color: '#374151', marginBottom: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
              Execution Details
            </h4>
            <MetricItem label="Spark App ID" value={sparkAppId} />
          </div>
        </div>
      )}
    </div>
  );
}

interface SchemaViewerProps {
  currentSchema: string;
  metadata: IcebergMetadata;
}

function SchemaViewer({ currentSchema, metadata }: SchemaViewerProps) {
  const [currentSchemaIndex, setCurrentSchemaIndex] = useState(0);
  const [schemas, setSchemas] = useState<any[]>([]);

  useEffect(() => {
    try {
      const metadataJson = JSON.parse(metadata.currentMetadata);
      const schemasArray = metadataJson.schemas || [];

      // Sort schemas by schema-id in descending order (newest first)
      const sortedSchemas = [...schemasArray].sort((a, b) => (b['schema-id'] || 0) - (a['schema-id'] || 0));
      setSchemas(sortedSchemas);
    } catch (e) {
      console.error('Error parsing schemas:', e);
    }
  }, [metadata]);

  const getFieldChanges = (currentSchema: any, previousSchema: any | null) => {
    if (!previousSchema) return { newFieldIds: new Set<number>() };

    const currentFields = currentSchema.fields || [];
    const previousFields = previousSchema.fields || [];
    const previousFieldIds = new Set(previousFields.map((f: any) => f.id));

    const newFieldIds = new Set<number>(
      currentFields
        .filter((f: any) => !previousFieldIds.has(f.id))
        .map((f: any) => f.id as number)
    );

    return { newFieldIds };
  };

  const highlightSchemaJSON = (schema: any, newFieldIds: Set<number>) => {
    const schemaString = JSON.stringify(schema, null, 2);
    const lines = schemaString.split('\n');

    // Track which lines belong to new fields
    const highlightedLines = new Set<number>();
    let currentFieldId: number | null = null;
    let braceDepth = 0;
    let inFieldObject = false;
    let fieldStartLine = -1;

    // First pass: identify which lines belong to new fields
    lines.forEach((line, index) => {
      // Check if this line contains a field ID
      const idMatch = line.match(/"id":\s*(\d+)/);
      if (idMatch) {
        const fieldId = parseInt(idMatch[1]);
        if (newFieldIds.has(fieldId)) {
          currentFieldId = fieldId;
          inFieldObject = true;
          fieldStartLine = index;
          // Find the line where this field object starts (the opening brace)
          for (let i = index - 1; i >= 0; i--) {
            if (lines[i].trim().endsWith('{')) {
              fieldStartLine = i;
              break;
            }
          }
          braceDepth = 0;
        }
      }

      // Track brace depth to know when field object ends
      if (inFieldObject) {
        const openBraces = (line.match(/{/g) || []).length;
        const closeBraces = (line.match(/}/g) || []).length;
        braceDepth += openBraces - closeBraces;

        // Mark all lines from field start to current as highlighted
        for (let i = fieldStartLine; i <= index; i++) {
          highlightedLines.add(i);
        }

        // When braces are balanced, we've exited the field object
        if (braceDepth <= 0 && line.includes('}')) {
          inFieldObject = false;
          currentFieldId = null;
        }
      }
    });

    return lines.map((line, index) => {
      const isHighlighted = highlightedLines.has(index);

      return (
        <div key={index} style={{
          backgroundColor: isHighlighted ? '#dcfce7' : 'transparent',
          borderLeft: isHighlighted ? '4px solid #22c55e' : 'none',
          paddingLeft: isHighlighted ? '0.5rem' : '0'
        }}>
          {line}
        </div>
      );
    });
  };

  const handlePrevious = () => {
    if (currentSchemaIndex < schemas.length - 1) {
      setCurrentSchemaIndex(currentSchemaIndex + 1);
    }
  };

  const handleNext = () => {
    if (currentSchemaIndex > 0) {
      setCurrentSchemaIndex(currentSchemaIndex - 1);
    }
  };

  if (schemas.length === 0) {
    return (
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
        {JSON.stringify(JSON.parse(currentSchema), null, 2)}
      </pre>
    );
  }

  const currentDisplaySchema = schemas[currentSchemaIndex];
  const previousDisplaySchema = currentSchemaIndex < schemas.length - 1 ? schemas[currentSchemaIndex + 1] : null;
  const { newFieldIds } = getFieldChanges(currentDisplaySchema, previousDisplaySchema);

  return (
    <div>
      {/* Navigation Controls */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
        <button
          onClick={handlePrevious}
          disabled={currentSchemaIndex >= schemas.length - 1}
          style={{
            padding: '0.5rem 1rem',
            backgroundColor: currentSchemaIndex >= schemas.length - 1 ? '#e5e7eb' : '#3b82f6',
            color: currentSchemaIndex >= schemas.length - 1 ? '#9ca3af' : 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: currentSchemaIndex >= schemas.length - 1 ? 'not-allowed' : 'pointer',
            fontSize: '0.875rem',
            fontWeight: '500'
          }}
        >
          ← Previous
        </button>

        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '0.875rem', fontWeight: '600', color: '#374151' }}>
            Schema Version {currentDisplaySchema['schema-id']}
            <span style={{ fontWeight: 'normal', color: '#6b7280' }}>
              {' '}({currentSchemaIndex + 1} of {schemas.length})
            </span>
          </div>
          {newFieldIds.size > 0 && (
            <div style={{
              marginTop: '0.25rem',
              fontSize: '0.75rem',
              color: '#22c55e',
              fontWeight: '500'
            }}>
              {newFieldIds.size} New Field{newFieldIds.size !== 1 ? 's' : ''} Added
            </div>
          )}
        </div>

        <button
          onClick={handleNext}
          disabled={currentSchemaIndex === 0}
          style={{
            padding: '0.5rem 1rem',
            backgroundColor: currentSchemaIndex === 0 ? '#e5e7eb' : '#3b82f6',
            color: currentSchemaIndex === 0 ? '#9ca3af' : 'white',
            border: 'none',
            borderRadius: '6px',
            cursor: currentSchemaIndex === 0 ? 'not-allowed' : 'pointer',
            fontSize: '0.875rem',
            fontWeight: '500'
          }}
        >
          Next →
        </button>
      </div>

      {/* Schema Display */}
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
        {highlightSchemaJSON(currentDisplaySchema, newFieldIds)}
      </pre>
    </div>
  );
}

function TableDetailContent() {
  const params = useParams();
  const router = useRouter();
  const searchParams = useSearchParams();
  const databaseId = params.databaseId as string;
  const tableId = params.tableId as string;
  const searchDatabaseId = searchParams.get('db') || databaseId;
  
  const [table, setTable] = useState<Table | null>(null);
  const [icebergMetadata, setIcebergMetadata] = useState<IcebergMetadata | null>(null);
  const [loading, setLoading] = useState(true);
  const [metadataLoading, setMetadataLoading] = useState(false);
  const [error, setError] = useState('');
  const [metadataError, setMetadataError] = useState('');
  const [diffModalOpen, setDiffModalOpen] = useState(false);
  const [selectedMetadataFile, setSelectedMetadataFile] = useState<string | null>(null);

  const handleViewDiff = (metadataFile: string) => {
    setSelectedMetadataFile(metadataFile);
    setDiffModalOpen(true);
  };

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
      
      // Fetch Iceberg metadata after table details load
      fetchIcebergMetadata();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const fetchIcebergMetadata = async () => {
    setMetadataLoading(true);
    setMetadataError('');

    try {
      const response = await fetch('/api/tables/metadata', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId, tableId }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to fetch Iceberg metadata');
      }

      // Backend now sends all Long values as strings, so we can safely parse
      const data = await response.json();
      console.log('Fetched metadata with currentSnapshotId:', data.currentSnapshotId, typeof data.currentSnapshotId);
      setIcebergMetadata(data);
    } catch (err) {
      setMetadataError(err instanceof Error ? err.message : 'Failed to load Iceberg metadata');
      console.error('Iceberg metadata error:', err);
    } finally {
      setMetadataLoading(false);
    }
  };

  const formatDate = (timestamp: number | string) => {
    // Convert string to number if needed
    const ts = typeof timestamp === 'string' ? parseInt(timestamp) : timestamp;
    // If timestamp is in seconds (less than year 2000 in milliseconds), convert to milliseconds
    const timestampMs = ts < 10000000000 ? ts * 1000 : ts;
    return new Date(timestampMs).toLocaleString();
  };

  const removeNullValues = (obj: any): any => {
    if (typeof obj !== 'object' || obj === null) return obj;
    return Object.fromEntries(
      Object.entries(obj).filter(([_, value]) => value !== null)
    );
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

  // Check if this is partial data (Iceberg metadata unavailable)
  const isPartialData = (table as any)._partial === true;

  return (
    <main style={{
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif',
      backgroundColor: '#f9fafb'
    }}>
      <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
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
          <button
            onClick={() => router.push(`/databases/${encodeURIComponent(databaseId)}`)}
            style={{
              padding: '0.25rem 0.5rem',
              backgroundColor: 'transparent',
              color: '#3b82f6',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '0.875rem',
              fontWeight: '500',
              transition: 'background-color 0.2s',
              fontFamily: 'monospace'
            }}
            onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#eff6ff'}
            onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
          >
            {databaseId}
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
            {tableId}
          </span>
        </div>

        {/* Warning Banner for Partial Data */}
        {isPartialData && (
          <div style={{
            backgroundColor: '#fef3c7',
            border: '1px solid #fbbf24',
            borderRadius: '8px',
            padding: '1rem',
            marginBottom: '2rem',
            display: 'flex',
            alignItems: 'center',
            gap: '0.75rem'
          }}>
            <span style={{ fontSize: '1.5rem' }}>⚠️</span>
            <div>
              <div style={{ fontWeight: '600', color: '#92400e', marginBottom: '0.25rem' }}>
                Limited Table Information
              </div>
              <div style={{ fontSize: '0.875rem', color: '#78350f' }}>
                {(table as any)._error || 'Iceberg metadata could not be loaded for this table. Showing basic information only.'}
              </div>
            </div>
          </div>
        )}

        {/* Header */}
        <div style={{
          backgroundColor: 'white',
          padding: '2rem',
          borderRadius: '8px',
          boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
          marginBottom: '2rem'
        }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2rem' }}>
            {/* Database Name */}
            <div>
              <div style={{ marginBottom: '0.5rem' }}>
                <span style={{
                  fontSize: '0.875rem',
                  fontWeight: '600',
                  color: '#6b7280',
                  textTransform: 'uppercase',
                  letterSpacing: '0.05em'
                }}>
                  Database Name
                </span>
              </div>
              <h2 style={{
                fontSize: '1.5rem',
                fontWeight: 'bold',
                color: '#1f2937',
                margin: 0
              }}>
                {table.databaseId}
              </h2>
            </div>

            {/* Table Name */}
            <div>
              <div style={{ marginBottom: '0.5rem' }}>
                <span style={{
                  fontSize: '0.875rem',
                  fontWeight: '600',
                  color: '#6b7280',
                  textTransform: 'uppercase',
                  letterSpacing: '0.05em'
                }}>
                  Table Name
                </span>
              </div>
              <h1 style={{
                fontSize: '1.5rem',
                fontWeight: 'bold',
                color: '#1f2937',
                margin: 0
              }}>
                {table.tableId}
              </h1>
            </div>
          </div>
        </div>

        {/* Two Column Layout: Basic Info (2/5) and Iceberg Metadata (3/5) */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'minmax(0, 2fr) minmax(0, 3fr)',
          gap: '1.5rem',
          marginBottom: '1.5rem'
        }}>
          {/* Basic Information - Left Column */}
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
                  {table.tableType || 'Unknown'}
                </span>
              } />
              <CompactDetailRow label="Creator" value={table.tableCreator || 'N/A'} />
              <CompactDetailRow label="Created" value={table.creationTime ? formatDate(table.creationTime) : 'N/A'} />
              <CompactDetailRow label="Last Modified" value={table.lastModifiedTime ? formatDate(table.lastModifiedTime) : 'N/A'} />
              
              {/* Table URI */}
              {table.tableUri && (
                <div style={{ marginTop: '0.5rem' }}>
                  <h3 style={{
                    fontSize: '0.875rem',
                    fontWeight: '600',
                    marginBottom: '0.5rem',
                    color: '#6b7280'
                  }}>
                    Table URI
                  </h3>
                  <div style={{
                    backgroundColor: '#f9fafb',
                    padding: '0.75rem',
                    borderRadius: '6px',
                    fontFamily: 'monospace',
                    fontSize: '0.75rem',
                    wordBreak: 'break-all',
                    color: '#374151'
                  }}>
                    {table.tableUri}
                  </div>
                </div>
              )}

              {/* Storage Location */}
              {table.tableLocation && (
                <div style={{ marginTop: '0.5rem' }}>
                  <h3 style={{
                    fontSize: '0.875rem',
                    fontWeight: '600',
                    marginBottom: '0.5rem',
                    color: '#6b7280'
                  }}>
                    Storage Location
                  </h3>
                  <div style={{
                    backgroundColor: '#f9fafb',
                    padding: '0.75rem',
                    borderRadius: '6px',
                    fontFamily: 'monospace',
                    fontSize: '0.75rem',
                    wordBreak: 'break-all',
                    color: '#374151'
                  }}>
                    {table.tableLocation}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Iceberg Metadata - Right Column */}
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
              Iceberg Metadata
            </h2>

            {metadataLoading && (
              <div style={{ textAlign: 'center', padding: '2rem', color: '#6b7280' }}>
                Loading Iceberg metadata...
              </div>
            )}

            {metadataError && (
              <div style={{
                backgroundColor: '#fef2f2',
                color: '#991b1b',
                padding: '1rem',
                borderRadius: '6px',
                marginBottom: '1rem'
              }}>
                {metadataError}
              </div>
            )}

            {icebergMetadata && !metadataLoading && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
                {/* Current Snapshot ID */}
                {icebergMetadata.currentSnapshotId && (() => {
                  // Find the current snapshot to get its summary data
                  let currentSnapshotSummary = null;
                  try {
                    if (icebergMetadata.snapshots) {
                      const snapshots = JSON.parse(icebergMetadata.snapshots);
                      const currentSnapshot = snapshots.find((s: any) =>
                        s['snapshot-id'] === icebergMetadata.currentSnapshotId
                      );
                      if (currentSnapshot && currentSnapshot.summary) {
                        currentSnapshotSummary = currentSnapshot.summary;
                      }
                    }
                  } catch (e) {
                    console.error('Error parsing current snapshot:', e);
                  }

                  const formatValue = (value: string | number) => {
                    if (value === 'N/A' || value === undefined || value === null) return 'N/A';
                    return Number(value).toLocaleString();
                  };

                  const formatBytes = (bytes: string | number) => {
                    if (bytes === 'N/A' || bytes === undefined || bytes === null) return 'N/A';
                    const num = Number(bytes);
                    if (num === 0) return '0 B';
                    const k = 1024;
                    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
                    const i = Math.floor(Math.log(num) / Math.log(k));
                    return Math.round((num / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
                  };

                  const totalRecords = currentSnapshotSummary?.['total-records'] || 'N/A';
                  const totalFiles = currentSnapshotSummary?.['total-data-files'] || 'N/A';
                  const totalSize = currentSnapshotSummary?.['total-files-size'] || 'N/A';

                  return (
                    <div>
                      <h3 style={{
                        fontSize: '1rem',
                        fontWeight: '600',
                        marginBottom: '0.75rem',
                        color: '#374151'
                      }}>
                        Current Snapshot
                      </h3>
                      <div style={{
                        backgroundColor: '#f9fafb',
                        padding: '0.75rem',
                        borderRadius: '6px',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '0.5rem'
                      }}>
                        <div style={{
                          fontFamily: 'monospace',
                          fontSize: '0.875rem',
                          color: '#374151',
                          fontWeight: '600'
                        }}>
                          {icebergMetadata.currentSnapshotId}
                        </div>
                        {currentSnapshotSummary && (
                          <div style={{
                            display: 'grid',
                            gridTemplateColumns: 'repeat(3, 1fr)',
                            gap: '0.75rem',
                            marginTop: '0.25rem',
                            paddingTop: '0.5rem',
                            borderTop: '1px solid #e5e7eb'
                          }}>
                            <div>
                              <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem' }}>Records</div>
                              <div style={{ fontFamily: 'monospace', fontSize: '0.875rem', color: '#374151', fontWeight: '500' }}>
                                {formatValue(totalRecords)}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem' }}>Files</div>
                              <div style={{ fontFamily: 'monospace', fontSize: '0.875rem', color: '#374151', fontWeight: '500' }}>
                                {formatValue(totalFiles)}
                              </div>
                            </div>
                            <div>
                              <div style={{ fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem' }}>Size</div>
                              <div style={{ fontFamily: 'monospace', fontSize: '0.875rem', color: '#374151', fontWeight: '500' }}>
                                {formatBytes(totalSize)}
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })()}

                {/* Snapshots */}
                {(() => {
                  // Check if snapshots data exists
                  if (!icebergMetadata.snapshots) {
                    console.warn('No snapshots data available in metadata');
                    return (
                      <div>
                        <h3 style={{
                          fontSize: '1rem',
                          fontWeight: '600',
                          marginBottom: '0.75rem',
                          color: '#374151'
                        }}>
                          Snapshots
                        </h3>
                        <div style={{
                          backgroundColor: '#fef9c3',
                          color: '#854d0e',
                          padding: '1rem',
                          borderRadius: '6px',
                          border: '1px solid #fde047'
                        }}>
                          No snapshot data available for this table.
                        </div>
                      </div>
                    );
                  }

                  try {
                    // Backend now sends all Long values as strings, so we can safely parse
                    const snapshots = JSON.parse(icebergMetadata.snapshots);
                    console.log('Parsed snapshots:', snapshots);
                    console.log('Current snapshot ID from backend:', icebergMetadata.currentSnapshotId, typeof icebergMetadata.currentSnapshotId);

                    // Sort by timestamp (convert string timestamps to numbers for comparison)
                    const sortedSnapshots = snapshots.sort((a: any, b: any) => {
                      const timeA = typeof a['timestamp-ms'] === 'string' ? parseInt(a['timestamp-ms']) : a['timestamp-ms'];
                      const timeB = typeof b['timestamp-ms'] === 'string' ? parseInt(b['timestamp-ms']) : b['timestamp-ms'];
                      return timeB - timeA;
                    });
                    if (sortedSnapshots.length > 0) {
                      console.log('Most recent snapshot by timestamp:', sortedSnapshots[0]['snapshot-id'], typeof sortedSnapshots[0]['snapshot-id']);
                    }

                    // Check if snapshots array is empty
                    if (!Array.isArray(snapshots) || snapshots.length === 0) {
                      console.warn('Snapshots array is empty');
                      return (
                        <div>
                          <h3 style={{
                            fontSize: '1rem',
                            fontWeight: '600',
                            marginBottom: '0.75rem',
                            color: '#374151'
                          }}>
                            Snapshots
                          </h3>
                          <div style={{
                            backgroundColor: '#fef9c3',
                            color: '#854d0e',
                            padding: '1rem',
                            borderRadius: '6px',
                            border: '1px solid #fde047'
                          }}>
                            This table has no snapshots yet.
                          </div>
                        </div>
                      );
                    }

                    return (
                      <div>
                        <h3 style={{
                          fontSize: '1rem',
                          fontWeight: '600',
                          marginBottom: '0.75rem',
                          color: '#374151'
                        }}>
                          Snapshots ({snapshots.length})
                        </h3>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem', maxHeight: '600px', overflowY: 'auto' }}>
                          {sortedSnapshots
                            .map((snapshot: any, index: number) => {
                              // Extract operation from various possible locations
                              const operation = snapshot.operation
                                || snapshot.summary?.operation
                                || (snapshot.summary && Object.keys(snapshot.summary).length > 0 ? 'append' : 'unknown');

                              const isCurrent = snapshot['snapshot-id'] === icebergMetadata.currentSnapshotId;

                              console.log(`Snapshot ${snapshot['snapshot-id']}: isCurrent=${isCurrent} (current=${icebergMetadata.currentSnapshotId})`);

                              return (
                                <SnapshotCard
                                  key={snapshot['snapshot-id'] || index}
                                  snapshotId={snapshot['snapshot-id']}
                                  operation={operation}
                                  timestamp={formatDate(snapshot['timestamp-ms'])}
                                  summary={snapshot.summary || {}}
                                  isCurrent={isCurrent}
                                />
                              );
                            })}
                        </div>
                      </div>
                    );
                  } catch (e) {
                    console.error('Error parsing snapshots:', e);
                    return (
                      <div>
                        <h3 style={{
                          fontSize: '1rem',
                          fontWeight: '600',
                          marginBottom: '0.75rem',
                          color: '#374151'
                        }}>
                          Snapshots
                        </h3>
                        <div style={{
                          backgroundColor: '#fef2f2',
                          color: '#991b1b',
                          padding: '1rem',
                          borderRadius: '6px',
                          border: '1px solid #fca5a5'
                        }}>
                          <div style={{ fontWeight: '600', marginBottom: '0.5rem' }}>Failed to parse snapshots data</div>
                          <div style={{ fontSize: '0.875rem' }}>
                            Error: {e instanceof Error ? e.message : 'Unknown error'}
                          </div>
                          <div style={{ fontSize: '0.875rem', marginTop: '0.5rem', opacity: 0.8 }}>
                            Check browser console for more details.
                          </div>
                        </div>
                      </div>
                    );
                  }
                })()}

                {/* Metadata History */}
                {(() => {
                  try {
                    const metadataLog = icebergMetadata.metadataLog ? JSON.parse(icebergMetadata.metadataLog) : [];

                    if (metadataLog.length > 0) {
                      return (
                        <div>
                          <h3 style={{
                            fontSize: '1rem',
                            fontWeight: '600',
                            marginBottom: '0.75rem',
                            color: '#374151'
                          }}>
                            Metadata History ({metadataLog.length})
                          </h3>
                          <div style={{ overflowX: 'auto', maxHeight: '400px', overflowY: 'auto' }}>
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
                                    Timestamp
                                  </th>
                                  <th style={{
                                    padding: '0.75rem',
                                    textAlign: 'left',
                                    fontWeight: '600',
                                    color: '#374151',
                                    borderBottom: '2px solid #e5e7eb'
                                  }}>
                                    Metadata File
                                  </th>
                                  <th style={{
                                    padding: '0.75rem',
                                    textAlign: 'center',
                                    fontWeight: '600',
                                    color: '#374151',
                                    borderBottom: '2px solid #e5e7eb',
                                    width: '100px'
                                  }}>
                                    Actions
                                  </th>
                                </tr>
                              </thead>
                              <tbody>
                                {[...metadataLog].reverse().map((entry: any, index: number) => (
                                  <tr key={index} style={{ borderBottom: '1px solid #e5e7eb' }}>
                                    <td style={{
                                      padding: '0.75rem',
                                      fontSize: '0.875rem',
                                      color: '#6b7280',
                                      whiteSpace: 'nowrap'
                                    }}>
                                      {formatDate(entry['timestamp-ms'])}
                                    </td>
                                    <td style={{
                                      padding: '0.75rem',
                                      fontFamily: 'monospace',
                                      fontSize: '0.75rem',
                                      color: '#6b7280',
                                      wordBreak: 'break-all'
                                    }}>
                                      {entry['metadata-file']}
                                    </td>
                                    <td style={{
                                      padding: '0.75rem',
                                      textAlign: 'center'
                                    }}>
                                      <button
                                        onClick={() => handleViewDiff(entry['metadata-file'])}
                                        style={{
                                          padding: '0.375rem 0.75rem',
                                          backgroundColor: '#eff6ff',
                                          color: '#3b82f6',
                                          border: '1px solid #dbeafe',
                                          borderRadius: '6px',
                                          fontSize: '0.75rem',
                                          fontWeight: '500',
                                          cursor: 'pointer',
                                          transition: 'all 0.2s',
                                          whiteSpace: 'nowrap'
                                        }}
                                        onMouseEnter={(e) => {
                                          e.currentTarget.style.backgroundColor = '#dbeafe';
                                          e.currentTarget.style.borderColor = '#93c5fd';
                                          e.currentTarget.style.color = '#2563eb';
                                        }}
                                        onMouseLeave={(e) => {
                                          e.currentTarget.style.backgroundColor = '#eff6ff';
                                          e.currentTarget.style.borderColor = '#dbeafe';
                                          e.currentTarget.style.color = '#3b82f6';
                                        }}
                                      >
                                        View Diff
                                      </button>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      );
                    }
                  } catch (e) {
                    console.error('Error parsing metadata log:', e);
                  }
                  return null;
                })()}

                {/* Partitions */}
                {icebergMetadata.partitions && (
                  <div>
                    <h3 style={{
                      fontSize: '1rem',
                      fontWeight: '600',
                      marginBottom: '0.75rem',
                      color: '#374151'
                    }}>
                      Partition Specs
                    </h3>
                    <pre style={{
                      backgroundColor: '#f9fafb',
                      padding: '1rem',
                      borderRadius: '6px',
                      fontFamily: 'monospace',
                      fontSize: '0.875rem',
                      overflow: 'auto',
                      maxHeight: '300px',
                      color: '#374151',
                      margin: 0
                    }}>
                      {JSON.stringify(JSON.parse(icebergMetadata.partitions), null, 2)}
                    </pre>
                  </div>
                )}

                {/* Full Metadata JSON (Collapsible) */}
                <details style={{ marginTop: '1rem' }}>
                  <summary style={{
                    cursor: 'pointer',
                    fontWeight: '600',
                    color: '#374151',
                    padding: '0.75rem',
                    backgroundColor: '#f9fafb',
                    borderRadius: '6px',
                    userSelect: 'none'
                  }}>
                    View Full Metadata JSON
                  </summary>
                  <pre style={{
                    backgroundColor: '#f9fafb',
                    padding: '1rem',
                    borderRadius: '6px',
                    fontFamily: 'monospace',
                    fontSize: '0.75rem',
                    overflow: 'auto',
                    maxHeight: '500px',
                    color: '#374151',
                    margin: '0.5rem 0 0 0'
                  }}>
                    {JSON.stringify(JSON.parse(icebergMetadata.currentMetadata), (key, value) =>
                      typeof value === 'bigint' ? value.toString() : value
                    , 2)}
                  </pre>
                </details>
              </div>
            )}
          </div>
        </div>

        {/* Schema - Full Width */}
        {table.schema && icebergMetadata && (
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
            <SchemaViewer currentSchema={table.schema} metadata={icebergMetadata} />
          </div>
        )}

        {/* Data Preview - Full Width */}
        <DataPreview databaseId={databaseId} tableId={tableId} />

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
                <div style={{ display: 'grid', gap: '1rem', marginTop: '1rem' }}>
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
                    <CompactDetailRow label="Partition Retention" value={JSON.stringify(removeNullValues(table.policies.retention))} />
                  )}
                  {table.policies.replication && (
                    <CompactDetailRow label="Replication" value={JSON.stringify(removeNullValues(table.policies.replication))} />
                  )}
                  {table.policies.history && (
                    <CompactDetailRow label="Data Versions Retention" value={JSON.stringify(removeNullValues(table.policies.history))} />
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

        {/* Maintenance Operations */}
        <Maintenance databaseId={databaseId} tableId={tableId} table={table} />

        {/* Permissions (ACL Policies) */}
        <Permissions databaseId={databaseId} tableId={tableId} />
      </div>

      {/* Metadata Diff Modal */}
      {selectedMetadataFile && (
        <MetadataDiffModal
          isOpen={diffModalOpen}
          onClose={() => setDiffModalOpen(false)}
          databaseId={databaseId}
          tableId={tableId}
          metadataFile={selectedMetadataFile}
        />
      )}
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
