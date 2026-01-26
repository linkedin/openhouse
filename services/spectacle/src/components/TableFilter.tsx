interface TableFilterProps {
  searchFilter: string;
  onFilterChange: (value: string) => void;
}

export default function TableFilter({ searchFilter, onFilterChange }: TableFilterProps) {
  return (
    <input
      type="text"
      placeholder="Filter tables..."
      value={searchFilter}
      onChange={(e) => onFilterChange(e.target.value)}
      style={{
        width: '100%',
        padding: '0.75rem',
        border: '1px solid #d1d5db',
        borderRadius: '6px',
        fontSize: '1rem',
        outline: 'none',
        marginTop: '1rem'
      }}
    />
  );
}
