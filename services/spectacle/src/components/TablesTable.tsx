import { Table } from '@/types/table';
import TableRow from './TableRow';

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
        borderBottom: '1px solid #e5e7eb',
        backgroundColor: '#f9fafb'
      }}>
        <h2 style={{ fontSize: '1.25rem', fontWeight: '600', margin: 0 }}>
          Found {tables.length} table{tables.length !== 1 ? 's' : ''}
        </h2>
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ backgroundColor: '#f9fafb' }}>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>
                Table ID
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>
                Database ID
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>
                Type
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>
                Creator
              </th>
              <th style={{ padding: '0.75rem 1.5rem', textAlign: 'left', fontWeight: '600', color: '#374151' }}>
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
