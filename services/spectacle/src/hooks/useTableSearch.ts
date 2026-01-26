import { useState } from 'react';
import { Table } from '@/types/table';

export function useTableSearch() {
  const [databaseId, setDatabaseId] = useState('');
  const [tables, setTables] = useState<Table[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchFilter, setSearchFilter] = useState('');

  const searchTables = async () => {
    if (!databaseId.trim()) {
      setError('Please enter a database ID');
      return;
    }

    setLoading(true);
    setError('');

    try {
      // First, get the list of tables
      const searchResponse = await fetch('/api/tables/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId }),
      });

      if (!searchResponse.ok) {
        const errorData = await searchResponse.json();
        throw new Error(errorData.error || `Failed to fetch tables: ${searchResponse.status}`);
      }

      const searchData = await searchResponse.json();
      const tableList = searchData.results || [];

      // Then, fetch full details for each table in parallel
      const detailPromises = tableList.map(async (table: Table) => {
        try {
          const detailResponse = await fetch('/api/tables/details', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({ 
              databaseId: table.databaseId, 
              tableId: table.tableId 
            }),
          });

          if (detailResponse.ok) {
            return await detailResponse.json();
          }
          // If detail fetch fails, return the original table data
          return table;
        } catch {
          // If detail fetch fails, return the original table data
          return table;
        }
      });

      const tablesWithDetails = await Promise.all(detailPromises);
      setTables(tablesWithDetails);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  const filteredTables = tables.filter(table =>
    table.tableId.toLowerCase().includes(searchFilter.toLowerCase()) ||
    table.databaseId.toLowerCase().includes(searchFilter.toLowerCase())
  );

  return {
    databaseId,
    setDatabaseId,
    tables,
    loading,
    error,
    searchFilter,
    setSearchFilter,
    searchTables,
    filteredTables,
  };
}
