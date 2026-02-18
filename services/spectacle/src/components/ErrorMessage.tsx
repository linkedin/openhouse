interface ErrorMessageProps {
  message: string;
}

export default function ErrorMessage({ message }: ErrorMessageProps) {
  if (!message) return null;

  return (
    <div style={{
      backgroundColor: '#fee2e2',
      color: '#991b1b',
      padding: '1rem',
      borderRadius: '6px',
      marginBottom: '1rem'
    }}>
      <strong>Error:</strong> {message}
    </div>
  );
}
