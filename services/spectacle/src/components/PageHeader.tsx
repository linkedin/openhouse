import Image from 'next/image';

export default function PageHeader() {
  return (
    <div style={{ textAlign: 'center', marginBottom: '2rem' }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '0.5rem',
        marginBottom: '0.5rem'
      }}>
        <Image
          src="/logo.png"
          alt="OpenHouse Logo"
          width={60}
          height={60}
          style={{ objectFit: 'contain' }}
        />
        <h1 style={{
          fontSize: '2.5rem',
          fontWeight: 'bold',
          margin: 0,
          background: 'linear-gradient(to right, #4e8cc9, #8c78d6)',
          WebkitBackgroundClip: 'text',
          WebkitTextFillColor: 'transparent',
          backgroundClip: 'text'
        }}>
          OpenHouse Spectacle
        </h1>
      </div>
      <p style={{
        fontSize: '1rem',
        color: '#6b7280',
        margin: 0
      }}>
        Search and explore OpenHouse tables!
      </p>
    </div>
  );
}
