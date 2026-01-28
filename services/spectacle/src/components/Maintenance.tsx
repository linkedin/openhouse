'use client';

import { useState } from 'react';

interface MaintenanceJob {
  type: string;
  args: string[];
}

interface MaintenanceProps {
  databaseId: string;
  tableId: string;
}

const MAINTENANCE_JOBS: MaintenanceJob[] = [
  { type: 'NO_OP', args: [] },
  { type: 'SQL_TEST', args: [] },
  { type: 'RETENTION', args: ['--backupDir', '.backup'] },
  { type: 'DATA_COMPACTION', args: [] },
  { type: 'SNAPSHOTS_EXPIRATION', args: [] },
  { type: 'ORPHAN_FILES_DELETION', args: ['--backupDir', '.backup'] },
  { type: 'ORPHAN_DIRECTORY_DELETION', args: ['--trashDir', '.trash'] },
  { type: 'TABLE_STATS_COLLECTION', args: [] },
  { type: 'DATA_LAYOUT_STRATEGY_GENERATION', args: [] },
  { type: 'DATA_LAYOUT_STRATEGY_EXECUTION', args: [] },
];

export default function Maintenance({ databaseId, tableId }: MaintenanceProps) {
  const fqtn = `${databaseId}.${tableId}`;

  // Jobs that don't need tableName argument
  const NO_TABLE_JOBS = ['NO_OP', 'SQL_TEST'];
  // Jobs that need tableDirectoryPath instead of tableName
  const DIRECTORY_JOBS = ['ORPHAN_DIRECTORY_DELETION'];

  const [jobRequests, setJobRequests] = useState<{ [key: string]: string }>(
    MAINTENANCE_JOBS.reduce((acc, job) => {
      let args: string[];

      if (NO_TABLE_JOBS.includes(job.type)) {
        // Jobs that don't need table argument at all
        args = [...job.args];
      } else if (DIRECTORY_JOBS.includes(job.type)) {
        // Jobs that need tableDirectoryPath instead
        // For now, we'll leave this empty as it's not table-specific
        args = [...job.args];
      } else {
        // Most jobs need --tableName with fqtn
        args = ['--tableName', fqtn, ...job.args];
      }

      const defaultRequest = {
        jobName: `${job.type.toLowerCase()}_${tableId}`,
        clusterId: 'LocalHadoopCluster',
        jobConf: {
          jobType: job.type,
          args: args,
        },
      };
      acc[job.type] = JSON.stringify(defaultRequest, null, 2);
      return acc;
    }, {} as { [key: string]: string })
  );
  const [loading, setLoading] = useState<{ [key: string]: boolean }>({});
  const [results, setResults] = useState<{ [key: string]: { success: boolean; message: string } }>({});

  const handleRequestChange = (jobType: string, value: string) => {
    setJobRequests((prev) => ({ ...prev, [jobType]: value }));
  };

  const handleTriggerJob = async (jobType: string) => {
    setLoading((prev) => ({ ...prev, [jobType]: true }));
    setResults((prev) => ({ ...prev, [jobType]: { success: false, message: '' } }));

    try {
      const requestBody = JSON.parse(jobRequests[jobType]);

      const response = await fetch('/api/jobs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to submit job');
      }

      const data = await response.json();
      setResults((prev) => ({
        ...prev,
        [jobType]: {
          success: true,
          message: `Job submitted successfully! Job ID: ${data.jobId || 'N/A'}`,
        },
      }));
    } catch (err) {
      setResults((prev) => ({
        ...prev,
        [jobType]: {
          success: false,
          message: err instanceof Error ? err.message : 'An error occurred',
        },
      }));
    } finally {
      setLoading((prev) => ({ ...prev, [jobType]: false }));
    }
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
        Maintenance Operations
      </h2>
      <p
        style={{
          color: '#6b7280',
          fontSize: '0.875rem',
          marginBottom: '1.5rem',
        }}
      >
        Trigger maintenance jobs for this table. Edit the request body as needed before submitting.
      </p>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {MAINTENANCE_JOBS.map((job) => (
          <div
            key={job.type}
            style={{
              border: '1px solid #e5e7eb',
              borderRadius: '6px',
              padding: '1rem',
            }}
          >
            {/* Operation Type and Trigger Button */}
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '0.75rem',
              }}
            >
              <h3
                style={{
                  fontSize: '0.875rem',
                  fontWeight: '600',
                  color: '#374151',
                }}
              >
                {job.type}
              </h3>

              {/* Trigger Button */}
              <button
                onClick={() => handleTriggerJob(job.type)}
                disabled={loading[job.type]}
                style={{
                  padding: '0.5rem 1rem',
                  backgroundColor: loading[job.type] ? '#9ca3af' : '#3b82f6',
                  color: 'white',
                  border: 'none',
                  borderRadius: '6px',
                  cursor: loading[job.type] ? 'not-allowed' : 'pointer',
                  fontSize: '0.875rem',
                  fontWeight: '500',
                  whiteSpace: 'nowrap',
                }}
              >
                {loading[job.type] ? 'Submitting...' : 'Trigger'}
              </button>
            </div>

            {/* Request Body Editor */}
            <textarea
              value={jobRequests[job.type]}
              onChange={(e) => handleRequestChange(job.type, e.target.value)}
              style={{
                width: '100%',
                minHeight: '120px',
                padding: '0.5rem',
                fontFamily: 'monospace',
                fontSize: '0.75rem',
                border: '1px solid #d1d5db',
                borderRadius: '4px',
                resize: 'vertical',
                color: '#374151',
                backgroundColor: '#f9fafb',
              }}
              spellCheck={false}
            />

            {/* Result Message */}
            {results[job.type] && (
              <div
                style={{
                  marginTop: '0.75rem',
                  padding: '0.75rem',
                  borderRadius: '4px',
                  backgroundColor: results[job.type].success ? '#d1fae5' : '#fee2e2',
                  color: results[job.type].success ? '#065f46' : '#991b1b',
                  fontSize: '0.875rem',
                }}
              >
                {results[job.type].message}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
