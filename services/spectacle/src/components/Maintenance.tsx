'use client';

import { useState, useEffect, useRef } from 'react';
import { Table } from '@/types/table';

interface MaintenanceJob {
  type: string;
  args: string[];
}

interface MaintenanceProps {
  databaseId: string;
  tableId: string;
  table: Table | null;
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

export default function Maintenance({ databaseId, tableId, table }: MaintenanceProps) {
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
      } else if (job.type === 'RETENTION' && table?.policies?.retention) {
        // Populate RETENTION args from policies
        const retention = table.policies.retention;
        args = ['--tableName', fqtn];

        if (retention.columnPattern?.columnName) {
          args.push('--columnName', retention.columnPattern.columnName);
        }
        if (retention.columnPattern?.pattern) {
          args.push('--columnPattern', retention.columnPattern.pattern);
        }
        if (retention.granularity) {
          args.push('--granularity', retention.granularity.toLowerCase());
        }
        if (retention.count) {
          args.push('--count', String(retention.count));
        }
        args.push(...job.args); // Add --backupDir
      } else if (job.type === 'SNAPSHOTS_EXPIRATION' && table?.policies?.history) {
        // Populate SNAPSHOTS_EXPIRATION args from policies
        const history = table.policies.history;
        args = ['--tableName', fqtn];

        if (history.maxAge) {
          args.push('--maxAge', String(history.maxAge));
        }
        if (history.granularity) {
          args.push('--granularity', history.granularity.toLowerCase());
        }
        if (history.versions) {
          args.push('--versions', String(history.versions));
        }
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
  const [activeTab, setActiveTab] = useState<string>(MAINTENANCE_JOBS[0].type);
  const [loading, setLoading] = useState<{ [key: string]: boolean }>({});
  const [results, setResults] = useState<{ [key: string]: { success: boolean; message: string } }>({});
  const [jobIds, setJobIds] = useState<{ [key: string]: string }>({});
  const [jobStates, setJobStates] = useState<{ [key: string]: string }>({});
  const [recentJobs, setRecentJobs] = useState<{ [key: string]: any[] }>({});
  const pollIntervalRefs = useRef<{ [key: string]: NodeJS.Timeout }>({});

  const handleRequestChange = (jobType: string, value: string) => {
    setJobRequests((prev) => ({ ...prev, [jobType]: value }));
  };

  const fetchRecentJobs = async (jobType: string) => {
    try {
      const jobNamePrefix = `${jobType.toLowerCase()}_${tableId}`;
      const response = await fetch(`/api/jobs?jobNamePrefix=${encodeURIComponent(jobNamePrefix)}&limit=10`);

      if (!response.ok) {
        return;
      }

      const data = await response.json();
      setRecentJobs((prev) => ({ ...prev, [jobType]: data.results || [] }));
    } catch (err) {
      // Silently handle errors
    }
  };

  const TERMINAL_STATES = ['CANCELLED', 'FAILED', 'SUCCEEDED'];

  const pollJobStatus = async (jobType: string, jobId: string) => {
    try {
      const response = await fetch(`/api/jobs/${jobId}`, {
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-cache',
        },
      });

      if (!response.ok) {
        throw new Error('Failed to fetch job status');
      }

      const data = await response.json();
      const state = data.state;

      setJobStates((prev) => ({ ...prev, [jobType]: state }));

      // Stop polling if terminal state is reached
      if (TERMINAL_STATES.includes(state)) {
        if (pollIntervalRefs.current[jobType]) {
          clearInterval(pollIntervalRefs.current[jobType]);
          delete pollIntervalRefs.current[jobType];
        }
        setLoading((prev) => ({ ...prev, [jobType]: false }));

        // Update result message with final status
        const success = state === 'SUCCEEDED';
        setResults((prev) => ({
          ...prev,
          [jobType]: {
            success,
            message: `Job ${state.toLowerCase()}! Job ID: ${jobId}`,
          },
        }));

        // Refresh recent jobs list
        fetchRecentJobs(jobType);
      }
    } catch (err) {
      // Silently handle polling errors
    }
  };

  // Cleanup polling intervals on unmount
  useEffect(() => {
    return () => {
      Object.values(pollIntervalRefs.current).forEach((interval) => {
        clearInterval(interval);
      });
    };
  }, []);

  // Fetch recent jobs when active tab changes
  useEffect(() => {
    fetchRecentJobs(activeTab);
  }, [activeTab, tableId]);

  const handleTriggerJob = async (jobType: string) => {
    // Clear any existing polling interval for this job type
    if (pollIntervalRefs.current[jobType]) {
      clearInterval(pollIntervalRefs.current[jobType]);
      delete pollIntervalRefs.current[jobType];
    }

    setLoading((prev) => ({ ...prev, [jobType]: true }));
    setResults((prev) => ({ ...prev, [jobType]: { success: false, message: '' } }));
    setJobStates((prev) => ({ ...prev, [jobType]: 'SUBMITTING' }));

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
      const jobId = data.jobId;

      setJobIds((prev) => ({ ...prev, [jobType]: jobId }));
      setJobStates((prev) => ({ ...prev, [jobType]: data.state || 'QUEUED' }));

      // Refresh recent jobs list immediately with the response
      fetchRecentJobs(jobType);

      // Start polling for job status every 2 seconds
      pollIntervalRefs.current[jobType] = setInterval(() => {
        pollJobStatus(jobType, jobId);
      }, 2000);

      // Poll immediately once
      pollJobStatus(jobType, jobId);

      setResults((prev) => ({
        ...prev,
        [jobType]: {
          success: true,
          message: `Job submitted! Job ID: ${jobId}`,
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
      setLoading((prev) => ({ ...prev, [jobType]: false }));
      setJobStates((prev) => {
        const newState = { ...prev };
        delete newState[jobType];
        return newState;
      });
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

      {/* Tab Navigation */}
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '0.5rem',
          marginBottom: '1.5rem',
          borderBottom: '2px solid #e5e7eb',
          paddingBottom: '0.5rem',
        }}
      >
        {MAINTENANCE_JOBS.map((job) => (
          <button
            key={job.type}
            onClick={() => setActiveTab(job.type)}
            style={{
              padding: '0.5rem 1rem',
              backgroundColor: activeTab === job.type ? '#3b82f6' : 'transparent',
              color: activeTab === job.type ? 'white' : '#6b7280',
              border: activeTab === job.type ? 'none' : '1px solid #e5e7eb',
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '0.875rem',
              fontWeight: '500',
              transition: 'all 0.2s',
            }}
            onMouseEnter={(e) => {
              if (activeTab !== job.type) {
                e.currentTarget.style.backgroundColor = '#f3f4f6';
              }
            }}
            onMouseLeave={(e) => {
              if (activeTab !== job.type) {
                e.currentTarget.style.backgroundColor = 'transparent';
              }
            }}
          >
            {job.type}
          </button>
        ))}
      </div>

      {/* Active Tab Content */}
      {MAINTENANCE_JOBS.filter((job) => job.type === activeTab).map((job) => (
        <div key={job.type}>
          <p
            style={{
              color: '#6b7280',
              fontSize: '0.875rem',
              marginBottom: '1rem',
            }}
          >
            Edit the request body as needed before submitting the job.
          </p>

          {/* Request Body Editor */}
          <textarea
            value={jobRequests[job.type]}
            onChange={(e) => handleRequestChange(job.type, e.target.value)}
            style={{
              width: '100%',
              minHeight: '200px',
              padding: '0.75rem',
              fontFamily: 'monospace',
              fontSize: '0.75rem',
              border: '1px solid #d1d5db',
              borderRadius: '6px',
              resize: 'vertical',
              color: '#374151',
              backgroundColor: '#f9fafb',
              marginBottom: '1rem',
              boxSizing: 'border-box',
            }}
            spellCheck={false}
          />

          {/* Trigger Button */}
          <button
            onClick={() => handleTriggerJob(job.type)}
            style={{
              padding: '0.625rem 1.5rem',
              backgroundColor: '#3b82f6',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '0.875rem',
              fontWeight: '600',
              marginBottom: '1rem',
            }}
          >
            Trigger Job
          </button>

          {/* Result Message */}
          {results[job.type] && (
            <div
              style={{
                padding: '0.75rem',
                borderRadius: '6px',
                backgroundColor: results[job.type].success ? '#d1fae5' : '#fee2e2',
                color: results[job.type].success ? '#065f46' : '#991b1b',
                fontSize: '0.875rem',
                marginBottom: '1rem',
              }}
            >
              {results[job.type].message}
            </div>
          )}

          {/* Recent Jobs */}
          {recentJobs[job.type] && recentJobs[job.type].length > 0 && (
            <div
              style={{
                marginTop: '1.5rem',
                borderTop: '1px solid #e5e7eb',
                paddingTop: '1rem',
              }}
            >
              <h3
                style={{
                  fontSize: '1rem',
                  fontWeight: '600',
                  marginBottom: '0.75rem',
                  color: '#374151',
                }}
              >
                Recent Jobs
              </h3>
              <div style={{ overflowX: 'auto' }}>
                <table
                  style={{
                    width: '100%',
                    fontSize: '0.75rem',
                    borderCollapse: 'collapse',
                  }}
                >
                  <thead>
                    <tr
                      style={{
                        backgroundColor: '#f9fafb',
                        borderBottom: '1px solid #e5e7eb',
                      }}
                    >
                      <th style={{ padding: '0.5rem', textAlign: 'left', color: '#6b7280' }}>
                        Job ID
                      </th>
                      <th style={{ padding: '0.5rem', textAlign: 'left', color: '#6b7280' }}>
                        State
                      </th>
                      <th style={{ padding: '0.5rem', textAlign: 'left', color: '#6b7280' }}>
                        Created
                      </th>
                      <th style={{ padding: '0.5rem', textAlign: 'left', color: '#6b7280' }}>
                        Duration
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentJobs[job.type].map((recentJob: any) => {
                      const duration =
                        recentJob.finishTimeMs && recentJob.startTimeMs
                          ? `${((recentJob.finishTimeMs - recentJob.startTimeMs) / 1000).toFixed(1)}s`
                          : 'N/A';
                      const createdDate = new Date(recentJob.creationTimeMs);
                      const stateColor =
                        recentJob.state === 'SUCCEEDED'
                          ? '#10b981'
                          : recentJob.state === 'FAILED'
                          ? '#ef4444'
                          : recentJob.state === 'CANCELLED'
                          ? '#f59e0b'
                          : '#6b7280';

                      return (
                        <tr
                          key={recentJob.jobId}
                          style={{
                            borderBottom: '1px solid #f3f4f6',
                          }}
                        >
                          <td
                            style={{
                              padding: '0.5rem',
                              color: '#374151',
                              fontFamily: 'monospace',
                              fontSize: '0.7rem',
                              maxWidth: '300px',
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              whiteSpace: 'nowrap',
                            }}
                            title={recentJob.jobId}
                          >
                            {recentJob.jobId}
                          </td>
                          <td
                            style={{
                              padding: '0.5rem',
                              color: stateColor,
                              fontWeight: '600',
                            }}
                          >
                            {recentJob.state}
                          </td>
                          <td
                            style={{
                              padding: '0.5rem',
                              color: '#6b7280',
                            }}
                          >
                            {createdDate.toLocaleString()}
                          </td>
                          <td
                            style={{
                              padding: '0.5rem',
                              color: '#6b7280',
                            }}
                          >
                            {duration}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
