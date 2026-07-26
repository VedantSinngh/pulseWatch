'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import StatsStrip from './components/StatsStrip';
import EntitySelector from './components/EntitySelector';
import TimeSeriesChart from './components/TimeSeriesChart';
import AlertFeed from './components/AlertFeed';

const WS_URL = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000';
const MAX_ALERTS = 50;

interface AnomalyEvent {
  sensor_id: string;
  timestamp: string;
  temperature: number;
  vibration: number;
  pressure: number;
  anomaly_types: string[];
  severity?: string;
  if_score?: number;
  [key: string]: unknown;
}

export default function DashboardPage() {
  const [selectedEntity, setSelectedEntity] = useState('sensor-001');
  const [alerts, setAlerts] = useState<AnomalyEvent[]>([]);
  const [liveEvents, setLiveEvents] = useState<Array<Record<string, unknown>>>([]);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── WebSocket connection with auto-reconnect ──────────────────────────────
  const connectWs = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(`${WS_URL}/ws/anomalies`);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('[PulseWatch] WebSocket connected');
    };

    ws.onmessage = (evt) => {
      try {
        const event: AnomalyEvent = JSON.parse(evt.data as string);
        setAlerts((prev) => [event, ...prev].slice(0, MAX_ALERTS));
        setLiveEvents((prev) => [event, ...prev].slice(0, 500));
      } catch {
        // Ignore malformed messages
      }
    };

    ws.onerror = () => {
      console.warn('[PulseWatch] WebSocket error — will reconnect');
    };

    ws.onclose = () => {
      console.warn('[PulseWatch] WebSocket closed — reconnecting in 3s');
      reconnectTimerRef.current = setTimeout(connectWs, 3000);
    };
  }, []);

  useEffect(() => {
    connectWs();
    // Heartbeat to keep connection alive
    const heartbeat = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send('ping');
      }
    }, 30_000);
    return () => {
      clearInterval(heartbeat);
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      wsRef.current?.close();
    };
  }, [connectWs]);

  // Count anomalies in last hour from alerts
  const anomalyCountLastHour = alerts.filter((a) => {
    const ts = new Date(a.timestamp).getTime();
    return Date.now() - ts < 60 * 60 * 1000;
  }).length;

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100">
      {/* ── Topbar ─────────────────────────────────────────────────────────── */}
      <header className="border-b border-zinc-900">
        <div className="max-w-screen-xl mx-auto px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            {/* Logo mark */}
            <span className="inline-flex items-center justify-center w-6 h-6 rounded bg-anomaly/10">
              <span className="block w-2 h-2 rounded-full bg-anomaly animate-pulse-dot" aria-hidden="true" />
            </span>
            <span className="text-sm font-semibold text-zinc-100 tracking-tight">
              PulseWatch
            </span>
          </div>
          <EntitySelector value={selectedEntity} onChange={setSelectedEntity} />
        </div>
      </header>

      {/* ── Main content ───────────────────────────────────────────────────── */}
      <main className="max-w-screen-xl mx-auto px-6 py-8">
        {/* Stats strip */}
        <section aria-label="System statistics">
          <StatsStrip
            selectedEntity={selectedEntity}
            anomalyCount={anomalyCountLastHour}
          />
        </section>

        {/* Divider */}
        <div className="border-t border-zinc-900 my-6" aria-hidden="true" />

        {/* Page heading */}
        <div className="mb-6">
          <h1 className="text-lg font-semibold text-zinc-100 leading-tight">
            {selectedEntity}
          </h1>
          <p className="text-sm text-zinc-500 mt-0.5">
            Temperature · Vibration · Pressure — last 15 minutes
          </p>
        </div>

        {/* ── Chart + Alert feed ────────────────────────────────────────────── */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 lg:gap-12">
          {/* Chart — 2/3 width */}
          <section
            className="lg:col-span-2"
            aria-label="Sensor time series chart"
          >
            <h2 className="text-xs text-zinc-500 uppercase tracking-widest font-medium mb-4">
              Temperature (°C)
            </h2>
            <TimeSeriesChart entityId={selectedEntity} liveEvents={liveEvents} />
          </section>

          {/* Alert feed — 1/3 width */}
          <section
            className="lg:col-span-1 min-h-[280px] lg:max-h-[380px]"
            aria-label="Live anomaly alert feed"
          >
            <AlertFeed alerts={alerts} />
          </section>
        </div>

        {/* ── Footer note ──────────────────────────────────────────────────── */}
        <footer className="mt-16 pb-8 text-center">
          <p className="text-xs text-zinc-800">
            Anomalies detected by Z-score · CUSUM · Isolation Forest
          </p>
        </footer>
      </main>
    </div>
  );
}
