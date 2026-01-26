'use client';

import { useEffect, Suspense } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { useTableSearch } from '@/hooks/useTableSearch';
import PageHeader from '@/components/PageHeader';
import SearchBar from '@/components/SearchBar';
import TableFilter from '@/components/TableFilter';
import ErrorMessage from '@/components/ErrorMessage';
import TablesTable from '@/components/TablesTable';
import EmptyState from '@/components/EmptyState';

function HomeContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const {
    databaseId,
    setDatabaseId,
    tables,
    loading,
    error,
    searchFilter,
    setSearchFilter,
    searchTables,
    filteredTables,
  } = useTableSearch();

  // Restore search from URL on mount
  useEffect(() => {
    const dbFromUrl = searchParams.get('db');
    if (dbFromUrl && dbFromUrl !== databaseId) {
      setDatabaseId(dbFromUrl);
    }
  }, [searchParams]);

  // Auto-search when database ID is restored from URL
  useEffect(() => {
    const dbFromUrl = searchParams.get('db');
    if (dbFromUrl && databaseId === dbFromUrl && tables.length === 0 && !loading) {
      searchTables();
    }
  }, [databaseId]);

  // Update URL when database ID changes
  const handleDatabaseIdChange = (value: string) => {
    setDatabaseId(value);
    if (value) {
      router.replace(`/?db=${encodeURIComponent(value)}`, { scroll: false });
    } else {
      router.replace('/', { scroll: false });
    }
  };

  return (
    <main style={{
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif',
      backgroundColor: '#f9fafb'
    }}>
      <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
        <PageHeader />
        
        <SearchBar
          databaseId={databaseId}
          onDatabaseIdChange={handleDatabaseIdChange}
          onSearch={searchTables}
          loading={loading}
        />
        
        {tables.length > 0 && (
          <TableFilter
            searchFilter={searchFilter}
            onFilterChange={setSearchFilter}
          />
        )}

        <ErrorMessage message={error} />

        <TablesTable tables={filteredTables} />

        {!loading && tables.length === 0 && !error && <EmptyState />}
      </div>
    </main>
  );
}

export default function Home() {
  return (
    <Suspense fallback={
      <div style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontFamily: 'system-ui, -apple-system, sans-serif'
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: '50px',
            height: '50px',
            border: '4px solid #e5e7eb',
            borderTop: '4px solid #3b82f6',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite',
            margin: '0 auto 1rem'
          }}></div>
          <p style={{ color: '#6b7280' }}>Loading...</p>
        </div>
      </div>
    }>
      <HomeContent />
    </Suspense>
  );
}
