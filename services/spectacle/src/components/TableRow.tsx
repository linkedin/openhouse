import { Table } from '@/types/table';
import { useRouter } from 'next/navigation';

interface TableRowProps {
  table: Table;
  index: number;
}

export default function TableRow({ table, index }: TableRowProps) {
  const router = useRouter();
  
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
        cursor: 'pointer'
      }}
      onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f9fafb'}
      onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'white'}
    >
      <td style={{ padding: '0.75rem 1.5rem', color: '#1f2937', fontWeight: '500' }}>
        {table.tableId}
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280' }}>
        {table.databaseId}
      </td>
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
      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280' }}>
        {table.tableCreator || 'N/A'}
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: '#6b7280', fontSize: '0.875rem' }}>
        {formatDate(table.lastModifiedTime)}
      </td>
    </tr>
  );
}
