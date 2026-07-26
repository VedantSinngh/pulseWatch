/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
      },
      colors: {
        // Anomaly accent — used ONLY for anomaly highlights
        anomaly: {
          DEFAULT: '#ef4444',  // red-500
          muted: '#fca5a5',    // red-300 (for subtle highlights)
          bg: '#fef2f2',       // red-50 (background tint)
        },
      },
      animation: {
        'slide-in': 'slide-in 0.25s ease-out',
        'pulse-dot': 'pulse-dot 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
      keyframes: {
        'slide-in': {
          '0%': { opacity: '0', transform: 'translateY(-6px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        'pulse-dot': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.3' },
        },
      },
    },
  },
  plugins: [],
};
