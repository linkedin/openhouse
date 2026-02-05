import { Table } from '@/types/table';
import { useRouter } from 'next/navigation';
import { fontSizes, fontWeights, colors } from '@/lib/theme';

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

  const handleDatabaseClick = (e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent row click
    router.push(`/databases/${encodeURIComponent(table.databaseId)}`);
  };

  return (
    <tr
      key={`${table.databaseId}-${table.tableId}-${index}`}
      onClick={handleRowClick}
      style={{
        borderBottom: `1px solid ${colors.border.default}`,
        transition: 'background-color 0.2s',
        cursor: 'pointer',
        backgroundColor: isPartialData ? '#fef3c7' : 'white'
      }}
      onMouseEnter={(e) => e.currentTarget.style.backgroundColor = isPartialData ? '#fde68a' : colors.background.page}
      onMouseLeave={(e) => e.currentTarget.style.backgroundColor = isPartialData ? '#fef3c7' : 'white'}
    >
      <td style={{ padding: '0.75rem 1.5rem', color: colors.text.primary, fontWeight: fontWeights.medium, fontSize: fontSizes.sm }}>
        {table.tableId}
        {isPartialData && (
          <span style={{
            marginLeft: '0.5rem',
            padding: '0.125rem 0.5rem',
            backgroundColor: '#fbbf24',
            color: '#78350f',
            borderRadius: '9999px',
            fontSize: fontSizes.xs,
            fontWeight: fontWeights.medium
          }}>
            Limited Info
          </span>
        )}
      </td>
      <td style={{ padding: '0.75rem 1.5rem', fontSize: fontSizes.sm }}>
        <span
          onClick={handleDatabaseClick}
          style={{
            color: colors.accent.primary,
            cursor: 'pointer',
            textDecoration: 'underline',
            textDecorationColor: 'transparent',
            transition: 'text-decoration-color 0.2s'
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.textDecorationColor = colors.accent.primary;
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.textDecorationColor = 'transparent';
          }}
        >
          {table.databaseId}
        </span>
      </td>
      <td style={{ padding: '0.75rem 1.5rem' }}>
        <span style={{
          padding: '0.25rem 0.75rem',
          backgroundColor: isPartialData ? colors.border.default : '#dbeafe',
          color: isPartialData ? colors.text.muted : '#1e40af',
          borderRadius: '9999px',
          fontSize: fontSizes.sm,
          fontWeight: fontWeights.medium
        }}>
          {table.tableType || 'Unknown'}
        </span>
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: colors.text.muted, fontSize: fontSizes.sm }}>
        {table.tableCreator || 'N/A'}
      </td>
      <td style={{ padding: '0.75rem 1.5rem', color: colors.text.muted, fontSize: fontSizes.sm }}>
        {formatDate(table.lastModifiedTime)}
      </td>
    </tr>
  );
}
