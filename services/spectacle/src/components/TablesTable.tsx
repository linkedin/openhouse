import { Table } from '@/types/table';
import TableRow from './TableRow';
import { fontSizes, fontWeights, colors } from '@/lib/theme';

interface TablesTableProps {
  tables: Table[];
}

export default function TablesTable({ tables }: TablesTableProps) {
  if (tables.length === 0) return null;

  return (
    <div style={{
      backgroundColor: 'white',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      overflow: 'hidden'
    }}>
      <div style={{
        padding: '1rem 1.5rem',
        borderBottom: `1px solid ${colors.border.default}`,
        backgroundColor: colors.background.page
      }}>
        <h2 style={{ fontSize: fontSizes.xl, fontWeight: fontWeights.semibold, margin: 0 }}>
          Found {tables.length} table{tables.length !== 1 ? 's' : ''}
        </h2>
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ backgroundColor: colors.background.page }}>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: fontWeights.semibold, color: colors.text.secondary, fontSize: fontSizes.sm }}>
                Table ID
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: fontWeights.semibold, color: colors.text.secondary, fontSize: fontSizes.sm }}>
                Database ID
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: fontWeights.semibold, color: colors.text.secondary, fontSize: fontSizes.sm }}>
                Type
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: fontWeights.semibold, color: colors.text.secondary, fontSize: fontSizes.sm }}>
                Creator
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: fontWeights.semibold, color: colors.text.secondary, fontSize: fontSizes.sm }}>
                Last Modified
              </th>
            </tr>
          </thead>
          <tbody>
            {tables.map((table, index) => (
              <TableRow key={`${table.databaseId}-${table.tableId}-${index}`} table={table} index={index} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
