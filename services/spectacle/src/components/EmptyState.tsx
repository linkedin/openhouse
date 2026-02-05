import { fontSizes, colors } from '@/lib/theme';

export default function EmptyState() {
  return (
    <div style={{
      backgroundColor: 'white',
      padding: '3rem',
      borderRadius: '8px',
      boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
      textAlign: 'center',
      color: colors.text.muted
    }}>
      <p style={{ fontSize: fontSizes.lg, marginBottom: '0.5rem' }}>No tables found</p>
      <p style={{ fontSize: fontSizes.sm }}>Enter a database ID and click Search to get started</p>
    </div>
  );
}
