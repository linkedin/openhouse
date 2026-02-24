import Image from 'next/image';
import { fontSizes, fontWeights, colors } from '@/lib/theme';

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
          fontSize: fontSizes['4xl'],
          fontWeight: fontWeights.bold,
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
        fontSize: fontSizes.base,
        color: colors.text.muted,
        margin: 0
      }}>
        Search and explore OpenHouse tables!
      </p>
    </div>
  );
}
