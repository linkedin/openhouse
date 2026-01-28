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
      setError('Please enter a database ID or database.table format');
      return;
    }

    setLoading(true);
    setError('');

    try {
      // Parse input - check if it's in "database.table" format
      let searchDatabaseId = databaseId.trim();
      let specificTableId: string | null = null;

      if (databaseId.includes('.')) {
        const parts = databaseId.split('.');
        if (parts.length === 2) {
          searchDatabaseId = parts[0];
          specificTableId = parts[1];
        }
      }

      // First, get the list of tables
      const searchResponse = await fetch('/api/tables/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ databaseId: searchDatabaseId }),
      });

      if (!searchResponse.ok) {
        const errorData = await searchResponse.json();
        const status = searchResponse.status;

        if (status === 404) {
          throw new Error(`Database "${searchDatabaseId}" not found`);
        } else if (status === 400) {
          throw new Error(`Invalid database ID format: "${searchDatabaseId}". Please enter a valid database ID or use database.table format.`);
        } else {
          throw new Error(errorData.error || `Failed to fetch tables (${status})`);
        }
      }

      const searchData = await searchResponse.json();
      const tableList = searchData.results || [];

      // If searching for a specific table, filter the list
      if (specificTableId) {
        const matchingTable = tableList.find(
          (table: Table) => table.tableId.toLowerCase() === specificTableId.toLowerCase()
        );

        if (!matchingTable) {
          throw new Error(`Table "${specificTableId}" not found in database "${searchDatabaseId}"`);
        }

        // Fetch details for the specific table
        try {
          const detailResponse = await fetch('/api/tables/details', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              databaseId: searchDatabaseId,
              tableId: specificTableId
            }),
          });

          if (detailResponse.ok) {
            const tableDetails = await detailResponse.json();
            setTables([tableDetails]);
            setSearchFilter(''); // Clear filter to show the single result
          } else {
            setTables([matchingTable]);
          }
        } catch {
          setTables([matchingTable]);
        }
      } else {
        // Fetch full details for all tables in parallel
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
      }
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
