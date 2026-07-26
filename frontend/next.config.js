/** @type {import('next').NextConfig} */
const nextConfig = {
  // Omit output: 'standalone' so Next.js builds the standard .next directory for Vercel
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
    NEXT_PUBLIC_WS_URL:  process.env.NEXT_PUBLIC_WS_URL  || 'ws://localhost:8000',
  },
};

module.exports = nextConfig;
