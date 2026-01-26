interface SearchBarProps {
  databaseId: string;
  onDatabaseIdChange: (value: string) => void;
  onSearch: () => void;
  loading: boolean;
}

export default function SearchBar({ 
  databaseId, 
  onDatabaseIdChange, 
  onSearch, 
  loading 
}: SearchBarProps) {
  return (
    <div style={{
      backgroundColor: 'white',
      padding: '1.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      marginBottom: '2rem'
    }}>
      <div style={{ display: 'flex', gap: '1rem' }}>
        <input
          type="text"
          placeholder="Enter Database ID"
          value={databaseId}
          onChange={(e) => onDatabaseIdChange(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && onSearch()}
          style={{
            flex: 1,
            padding: '0.75rem',
            border: '1px solid #d1d5db',
            borderRadius: '6px',
            fontSize: '1rem',
            outline: 'none'
          }}
        />
        <button
          onClick={onSearch}
          disabled={loading}
          style={{
            padding: '0.75rem 2rem',
            backgroundColor: loading ? '#9ca3af' : '#3b82f6',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            fontSize: '1rem',
            fontWeight: '500',
            cursor: loading ? 'not-allowed' : 'pointer',
            transition: 'background-color 0.2s'
          }}
        >
          {loading ? 'Searching...' : 'Search'}
        </button>
      </div>
    </div>
  );
}
