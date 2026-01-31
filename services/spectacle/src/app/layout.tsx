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

          /* Custom scrollbar styling */
          .custom-scrollbar::-webkit-scrollbar {
            width: 12px;
          }

          .custom-scrollbar::-webkit-scrollbar-track {
            background: #f3f4f6;
            border-radius: 6px;
          }

          .custom-scrollbar::-webkit-scrollbar-thumb {
            background: #9ca3af;
            border-radius: 6px;
            border: 2px solid #f3f4f6;
          }

          .custom-scrollbar::-webkit-scrollbar-thumb:hover {
            background: #6b7280;
          }
        `}</style>
      </head>
      <body style={{ margin: 0, padding: 0 }}>{children}</body>
    </html>
  )
}
