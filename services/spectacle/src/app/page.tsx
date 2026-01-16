export default function Home() {
  return (
    <main style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      minHeight: '100vh',
      padding: '2rem',
      fontFamily: 'system-ui, -apple-system, sans-serif'
    }}>
      <h1 style={{
        fontSize: '3rem',
        fontWeight: 'bold',
        marginBottom: '1rem',
        background: 'linear-gradient(to right, #3b82f6, #8b5cf6)',
        WebkitBackgroundClip: 'text',
        WebkitTextFillColor: 'transparent',
        backgroundClip: 'text'
      }}>
        Hello World
      </h1>
      <p style={{
        fontSize: '1.25rem',
        color: '#6b7280',
        textAlign: 'center'
      }}>
        Welcome to OpenHouse Spectacle
      </p>
    </main>
  )
}
