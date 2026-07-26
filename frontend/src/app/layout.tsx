import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'PulseWatch — Real-time Anomaly Detection',
  description:
    'A real-time streaming analytics platform that detects anomalies in IoT sensor data using Z-score, CUSUM, and Isolation Forest algorithms.',
  keywords: ['anomaly detection', 'real-time', 'IoT', 'streaming', 'kafka', 'spark'],
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full">{children}</body>
    </html>
  );
}
