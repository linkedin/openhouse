export default function EmptyState() {
  return (
    <div style={{
      backgroundColor: 'white',
      padding: '3rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      textAlign: 'center',
      color: '#6b7280'
    }}>
      <p style={{ fontSize: '1.125rem', marginBottom: '0.5rem' }}>No tables found</p>
      <p style={{ fontSize: '0.875rem' }}>Enter a database ID and click Search to get started</p>
    </div>
  );
}
