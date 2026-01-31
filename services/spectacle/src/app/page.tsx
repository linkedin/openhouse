'use client';

import { Suspense, useEffect, useRef } from 'react';
import { useTableSearch } from '@/hooks/useTableSearch';
import PageHeader from '@/components/PageHeader';
import SearchBar from '@/components/SearchBar';
import TableFilter from '@/components/TableFilter';
import ErrorMessage from '@/components/ErrorMessage';
import TablesTable from '@/components/TablesTable';
import EmptyState from '@/components/EmptyState';

function HomeContent() {
  const {
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
  } = useTableSearch();

  const observerTarget = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loadingMore) {
          loadMoreTables();
        }
      },
      { threshold: 0.1 }
    );

    const currentTarget = observerTarget.current;
    if (currentTarget) {
      observer.observe(currentTarget);
    }

    return () => {
      if (currentTarget) {
        observer.unobserve(currentTarget);
      }
    };
  }, [hasMore, loadingMore, loadMoreTables]);

  const handleDatabaseIdChange = (value: string) => {
    setDatabaseId(value);
  };

  return (
    <main style={{
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif',
      backgroundColor: '#f9fafb',
      position: 'relative',
      overflow: 'hidden'
    }}>
      {/* Iceberg Background Overlay */}
      <div style={{
        position: 'absolute',
        top: '400px',
        left: '50%',
        transform: 'translateX(-50%)',
        width: '600px',
        height: '600px',
        backgroundImage: 'url(/iceberg_bg.png)',
        backgroundSize: 'contain',
        backgroundRepeat: 'no-repeat',
        backgroundPosition: 'center',
        opacity: 0.08,
        pointerEvents: 'none',
        zIndex: 0
      }} />

      <div style={{ maxWidth: '1200px', margin: '0 auto', position: 'relative', zIndex: 1 }}>
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

        {/* Infinite scroll observer target */}
        {tables.length > 0 && (
          <div ref={observerTarget} style={{ height: '20px', margin: '1rem 0' }}>
            {loadingMore && (
              <div style={{ textAlign: 'center', padding: '1rem', color: '#6b7280' }}>
                <div style={{
                  width: '30px',
                  height: '30px',
                  border: '3px solid #e5e7eb',
                  borderTop: '3px solid #3b82f6',
                  borderRadius: '50%',
                  animation: 'spin 1s linear infinite',
                  margin: '0 auto'
                }}></div>
                <p style={{ marginTop: '0.5rem', fontSize: '0.875rem' }}>Loading more tables...</p>
              </div>
            )}
            {!hasMore && tables.length > 20 && (
              <div style={{
                textAlign: 'center',
                padding: '1rem',
                color: '#6b7280',
                fontSize: '0.875rem'
              }}>
                All tables loaded ({tables.length} total)
              </div>
            )}
          </div>
        )}
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
