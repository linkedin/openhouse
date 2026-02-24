import { useState } from 'react';
import { Table } from '@/types/table';

export function useTableSearch() {
  const [databaseId, setDatabaseId] = useState('');
  const [tables, setTables] = useState<Table[]>([]);
  const [allTablesList, setAllTablesList] = useState<Table[]>([]);
  const [currentBatchIndex, setCurrentBatchIndex] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState('');
  const [searchFilter, setSearchFilter] = useState('');

  const fetchTableDetails = async (tableList: Table[]): Promise<Table[]> => {
    const batchPromises = tableList.map(async (table: Table) => {
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
        return table;
      } catch {
        return table;
      }
    });

    return await Promise.all(batchPromises);
  };

  const loadMoreTables = async () => {
    if (!hasMore || loadingMore) return;

    setLoadingMore(true);
    try {
      const BATCH_SIZE = 20;
      const nextBatchIndex = currentBatchIndex + 1;
      const startIdx = nextBatchIndex * BATCH_SIZE;
      const endIdx = startIdx + BATCH_SIZE;

      const nextBatch = allTablesList.slice(startIdx, endIdx);
      if (nextBatch.length === 0) {
        setHasMore(false);
        return;
      }

      const tablesWithDetails = await fetchTableDetails(nextBatch);
      setTables(prev => [...prev, ...tablesWithDetails]);
      setCurrentBatchIndex(nextBatchIndex);
      setHasMore(endIdx < allTablesList.length);
    } catch (err) {
      console.error('Failed to load more tables:', err);
    } finally {
      setLoadingMore(false);
    }
  };

  const searchTables = async (overrideDatabaseId?: string) => {
    const dbIdToSearch = overrideDatabaseId !== undefined ? overrideDatabaseId : databaseId;

    if (!dbIdToSearch.trim()) {
      setError('Please enter a database ID or database.table format');
      return;
    }

    setLoading(true);
    setError('');
    setTables([]);
    setAllTablesList([]);
    setCurrentBatchIndex(0);
    setHasMore(false);

    try {
      // Parse input - check if it's in "database.table" format
      let searchDatabaseId = dbIdToSearch.trim();
      let specificTableId: string | null = null;

      if (dbIdToSearch.includes('.')) {
        const parts = dbIdToSearch.split('.');
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
        const tableIdToFind = specificTableId.toLowerCase();
        const matchingTable = tableList.find(
          (table: Table) => table.tableId.toLowerCase() === tableIdToFind
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
        // Store the full list and load first batch
        const BATCH_SIZE = 20;
        setAllTablesList(tableList);
        setCurrentBatchIndex(0);
        setHasMore(tableList.length > BATCH_SIZE);

        // Load first batch
        const firstBatch = tableList.slice(0, BATCH_SIZE);
        const tablesWithDetails = await fetchTableDetails(firstBatch);
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
    loadingMore,
    hasMore,
    error,
    searchFilter,
    setSearchFilter,
    searchTables,
    loadMoreTables,
    filteredTables,
  };
}
