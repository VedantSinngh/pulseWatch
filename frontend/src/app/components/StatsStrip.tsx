'use client';

import { useEffect, useState } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface HealthStatus {
  status: 'ok' | 'degraded' | 'down' | 'loading';
  opensearch?: { status: string; latency_ms?: number };
  kafka?: { status: string; latency_ms?: number };
}

interface StatsStripProps {
  selectedEntity: string;
  anomalyCount: number;
}

function StatusDot({ status }: { status: string }) {
  const color =
    status === 'ok' ? 'bg-emerald-500' :
    status === 'degraded' ? 'bg-yellow-500' :
    status === 'loading' ? 'bg-zinc-600' :
    'bg-anomaly';

  return (
    <span
      className={`inline-block w-1.5 h-1.5 rounded-full ${color} ${
        status === 'ok' ? 'animate-pulse-dot' : ''
      }`}
      aria-hidden="true"
    />
  );
}

export default function StatsStrip({ selectedEntity, anomalyCount }: StatsStripProps) {
  const [health, setHealth] = useState<HealthStatus>({ status: 'loading' });
  const [eventRate, setEventRate] = useState<number | null>(null);

  // Poll health every 15 seconds
  useEffect(() => {
    async function fetchHealth() {
      try {
        const res = await fetch(`${API_URL}/health`, { cache: 'no-store' });
        if (!res.ok) throw new Error('health endpoint error');
        const data = await res.json();
        setHealth(data);
      } catch {
        setHealth({ status: 'down' });
      }
    }
    fetchHealth();
    const id = setInterval(fetchHealth, 15_000);
    return () => clearInterval(id);
  }, []);

  // Estimate event rate from stats endpoint (events per minute)
  useEffect(() => {
    async function fetchStats() {
      try {
        const res = await fetch(`${API_URL}/stats/${selectedEntity}`, {
          cache: 'no-store',
        });
        if (!res.ok) { setEventRate(null); return; }
        // Event rate: approximate from anomaly count trends
        // We show a static "5 sensors × 2/s" = ~10 events/s for now,
        // since OpenSearch doesn't provide a direct events/s counter cheaply.
        setEventRate(10);
      } catch {
        setEventRate(null);
      }
    }
    fetchStats();
  }, [selectedEntity]);

  const overallOk =
    health.status === 'ok' &&
    health.opensearch?.status === 'ok' &&
    health.kafka?.status === 'ok';

  return (
    <div className="flex flex-wrap items-center gap-x-8 gap-y-2 py-4">
      {/* Event rate */}
      <div>
        <p className="text-xs text-zinc-500 uppercase tracking-widest font-medium mb-0.5">
          Event Rate
        </p>
        <p className="text-2xl font-semibold text-zinc-100 tabular-nums leading-none">
          {eventRate !== null ? `~${eventRate}` : '—'}
          <span className="text-sm font-normal text-zinc-500 ml-1">/s</span>
        </p>
      </div>

      <div className="w-px h-8 bg-zinc-800 hidden sm:block" aria-hidden="true" />

      {/* Anomaly count */}
      <div>
        <p className="text-xs text-zinc-500 uppercase tracking-widest font-medium mb-0.5">
          Anomalies (1h)
        </p>
        <p className={`text-2xl font-semibold tabular-nums leading-none ${
          anomalyCount > 0 ? 'text-anomaly' : 'text-zinc-100'
        }`}>
          {anomalyCount}
        </p>
      </div>

      <div className="w-px h-8 bg-zinc-800 hidden sm:block" aria-hidden="true" />

      {/* System status */}
      <div>
        <p className="text-xs text-zinc-500 uppercase tracking-widest font-medium mb-0.5">
          System
        </p>
        <div className="flex items-center gap-2">
          <StatusDot status={health.status} />
          <span className="text-sm font-medium text-zinc-300">
            {health.status === 'loading' ? 'Checking...' :
             overallOk ? 'All systems operational' :
             health.status === 'degraded' ? 'Degraded' : 'Unreachable'}
          </span>
        </div>
        {!overallOk && health.status !== 'loading' && (
          <div className="flex items-center gap-3 mt-1">
            <span className="text-xs text-zinc-600 flex items-center gap-1">
              <StatusDot status={health.kafka?.status || 'down'} />
              Kafka
            </span>
            <span className="text-xs text-zinc-600 flex items-center gap-1">
              <StatusDot status={health.opensearch?.status || 'down'} />
              OpenSearch
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
