import { useState, useEffect, useRef } from 'react';

interface SearchBarProps {
  databaseId: string;
  onDatabaseIdChange: (value: string) => void;
  onSearch: () => void;
  loading: boolean;
}

interface Database {
  databaseId: string;
}

interface TableSuggestion {
  databaseId: string;
  tableId: string;
}

export default function SearchBar({
  databaseId,
  onDatabaseIdChange,
  onSearch,
  loading
}: SearchBarProps) {
  const [databases, setDatabases] = useState<Database[]>([]);
  const [tableSuggestions, setTableSuggestions] = useState<TableSuggestion[]>([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [loadingSuggestions, setLoadingSuggestions] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetchDatabases();

    // Close suggestions when clicking outside
    const handleClickOutside = (event: MouseEvent) => {
      if (suggestionsRef.current && !suggestionsRef.current.contains(event.target as Node) &&
          inputRef.current && !inputRef.current.contains(event.target as Node)) {
        setShowSuggestions(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  useEffect(() => {
    // Fetch table suggestions when user types a database name
    if (databaseId.trim() && !databaseId.includes('.')) {
      const matchingDb = databases.find(db =>
        db.databaseId.toLowerCase() === databaseId.toLowerCase()
      );
      if (matchingDb) {
        fetchTableSuggestions(matchingDb.databaseId);
      }
    }
  }, [databaseId, databases]);

  const fetchDatabases = async () => {
    try {
      const response = await fetch('/api/databases');
      if (response.ok) {
        const data = await response.json();
        setDatabases(data.results || []);
      }
    } catch (error) {
      console.error('Failed to fetch databases:', error);
    }
  };

  const fetchTableSuggestions = async (dbId: string) => {
    setLoadingSuggestions(true);
    try {
      const response = await fetch('/api/tables/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ databaseId: dbId }),
      });

      if (response.ok) {
        const data = await response.json();
        const tables = (data.results || []).slice(0, 5); // Limit to 5 suggestions
        setTableSuggestions(tables);
      }
    } catch (error) {
      console.error('Failed to fetch table suggestions:', error);
    } finally {
      setLoadingSuggestions(false);
    }
  };

  const handleInputChange = (value: string) => {
    onDatabaseIdChange(value);
    setShowSuggestions(true);
  };

  const handleSuggestionClick = (suggestion: string) => {
    onDatabaseIdChange(suggestion);
    setShowSuggestions(false);
    // Automatically trigger search after selection
    setTimeout(() => onSearch(), 100);
  };

  const getFilteredSuggestions = () => {
    const input = databaseId.toLowerCase().trim();
    if (!input) return [];

    const suggestions: Array<{ value: string; label: string; type: 'database' | 'table' }> = [];

    // Add matching databases
    databases
      .filter(db => db.databaseId.toLowerCase().includes(input))
      .slice(0, 5)
      .forEach(db => {
        suggestions.push({
          value: db.databaseId,
          label: db.databaseId,
          type: 'database'
        });
      });

    // Add matching tables from current database if it's an exact match
    if (tableSuggestions.length > 0) {
      tableSuggestions
        .filter(table =>
          table.tableId.toLowerCase().includes(input) ||
          `${table.databaseId}.${table.tableId}`.toLowerCase().includes(input)
        )
        .forEach(table => {
          suggestions.push({
            value: `${table.databaseId}.${table.tableId}`,
            label: `${table.databaseId}.${table.tableId}`,
            type: 'table'
          });
        });
    }

    return suggestions;
  };

  const suggestions = getFilteredSuggestions();

  return (
    <div style={{
      backgroundColor: 'white',
      padding: '1.5rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      marginBottom: '2rem',
      position: 'relative'
    }}>
      <div style={{ display: 'flex', gap: '1rem' }}>
        <div style={{ flex: 1, position: 'relative' }}>
          <input
            ref={inputRef}
            type="text"
            placeholder="Enter Database ID or database.table"
            value={databaseId}
            onChange={(e) => handleInputChange(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && onSearch()}
            onFocus={() => setShowSuggestions(true)}
            style={{
              width: '100%',
              padding: '0.75rem',
              border: '1px solid #d1d5db',
              borderRadius: '6px',
              fontSize: '1rem',
              outline: 'none'
            }}
          />

          {/* Suggestions Dropdown */}
          {showSuggestions && suggestions.length > 0 && (
            <div
              ref={suggestionsRef}
              style={{
                position: 'absolute',
                top: '100%',
                left: 0,
                right: 0,
                marginTop: '0.25rem',
                backgroundColor: 'white',
                border: '1px solid #d1d5db',
                borderRadius: '6px',
                boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
                maxHeight: '300px',
                overflowY: 'auto',
                zIndex: 10
              }}
            >
              {suggestions.map((suggestion, index) => (
                <div
                  key={index}
                  onClick={() => handleSuggestionClick(suggestion.value)}
                  style={{
                    padding: '0.75rem',
                    cursor: 'pointer',
                    borderBottom: index < suggestions.length - 1 ? '1px solid #f3f4f6' : 'none',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    transition: 'background-color 0.2s'
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.backgroundColor = '#f9fafb';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.backgroundColor = 'white';
                  }}
                >
                  <span style={{
                    fontSize: '0.875rem',
                    fontFamily: 'monospace',
                    color: '#374151'
                  }}>
                    {suggestion.label}
                  </span>
                  <span style={{
                    fontSize: '0.75rem',
                    padding: '0.125rem 0.5rem',
                    backgroundColor: suggestion.type === 'database' ? '#dbeafe' : '#dcfce7',
                    color: suggestion.type === 'database' ? '#1e40af' : '#065f46',
                    borderRadius: '9999px',
                    fontWeight: '500'
                  }}>
                    {suggestion.type === 'database' ? 'DATABASE' : 'TABLE'}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

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
