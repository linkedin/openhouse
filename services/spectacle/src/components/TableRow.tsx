import { Table } from '@/types/table';
import { useRouter } from 'next/navigation';

interface TableRowProps {
  table: Table;
  index: number;
}

export default function TableRow({ table, index }: TableRowProps) {
  const router = useRouter();

  // Check if this is partial data (Iceberg metadata unavailable)
  const isPartialData = (table as any)._partial === true;

  const formatDate = (timestamp: number | undefined) => {
    if (!timestamp || timestamp === 0) {
      return 'Not available';
    }
    // If timestamp is in seconds (less than year 2000 in milliseconds), convert to milliseconds
    const timestampMs = timestamp < 10000000000 ? timestamp * 1000 : timestamp;
    return new Date(timestampMs).toLocaleString();
  };

  const handleRowClick = () => {
    // Store current database ID in URL for back navigation
    const currentUrl = new URL(window.location.href);
    const databaseId = currentUrl.searchParams.get('db') || table.databaseId;

    router.push(`/tables/${encodeURIComponent(table.databaseId)}/${encodeURIComponent(table.tableId)}?db=${encodeURIComponent(databaseId)}`);
  };

  return (
    <tr
      key={`${table.databaseId}-${table.tableId}-${index}`}
      onClick={handleRowClick}
      style={{
        borderBottom: '1px solid #e5e7eb',
        transition: 'background-color 0.2s',
        cursor: 'pointer',
        backgroundColor: isPartialData ? '#fef3c7' : 'white'
      }}
      onMouseEnter={(e) => e.currentTarget.style.backgroundColor = isPartialData ? '#fde68a' : '#f9fafb'}
      onMouseLeave={(e) => e.currentTarget.style.backgroundColor = isPartialData ? '#fef3c7' : 'white'}
    >
      <td style={{ padding: '0.75rem 1.5rem', color: '#1f2937', fontWeight: '500' }}>
        {table.tableId}
        {isPartialData && (
          <span style={{
            marginLeft: '0.5rem',
            padding: '0.125rem 0.5rem',
            backgroundColor: '#fbbf24',
            color: '#78350f',
            borderRadius: '9999px',
            fontSize: '0.75rem',
            fontWeight: '500'
          }}>
            Limited Info
          </span>
        )}
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280' }}>
        {table.databaseId}
      </td>
      <td style={{ padding: '0.75rem 1.5rem' }}>
        <span style={{
          padding: '0.25rem 0.75rem',
          backgroundColor: isPartialData ? '#e5e7eb' : '#dbeafe',
          color: isPartialData ? '#6b7280' : '#1e40af',
          borderRadius: '9999px',
          fontSize: '0.875rem',
          fontWeight: '500'
        }}>
          {table.tableType || 'Unknown'}
        </span>
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280' }}>
        {table.tableCreator || 'N/A'}
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280', fontSize: '0.875rem' }}>
        {formatDate(table.lastModifiedTime)}
      </td>
    </tr>
  );
}
