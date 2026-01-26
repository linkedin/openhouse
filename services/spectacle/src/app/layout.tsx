import type { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'OpenHouse Spectacle',
  description: 'OpenHouse Spectacle Application',
  icons: {
    icon: '/iceberg.ico',
    shortcut: '/iceberg.ico',
    apple: '/apple-touch-icon.png',
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <head>
        <style>{`
          @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
          }
        `}</style>
      </head>
      <body style={{ margin: 0, padding: 0 }}>{children}</body>
    </html>
  )
}
