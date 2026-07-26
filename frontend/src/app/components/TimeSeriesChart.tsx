'use client';

import { useEffect, useRef, useState, useCallback } from 'react';
import {
  ComposedChart,
  Line,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

interface DataPoint {
  time: string;       // formatted label
  timestamp: number;  // epoch ms for sorting
  temperature: number;
  mean: number | null;
  upperBand: number | null;
  lowerBand: number | null;
  anomaly: number | null;  // same as temperature when anomalous, else null
}

interface TimeSeriesChartProps {
  entityId: string;
  liveEvents: Array<Record<string, unknown>>;
}

// Format a timestamp for the X axis
function fmtTime(isoStr: string): string {
  const d = new Date(isoStr);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// Custom tooltip
function CustomTooltip({ active, payload, label }: {
  active?: boolean;
  payload?: Array<{ value: number; name: string; color: string }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded p-3 text-xs space-y-1 shadow-lg">
      <p className="text-zinc-400 mb-1">{label}</p>
      {payload.map((entry) => (
        entry.value != null && (
          <p key={entry.name} style={{ color: entry.color }}>
            {entry.name}: <span className="font-semibold tabular-nums">{Number(entry.value).toFixed(2)}</span>
          </p>
        )
      ))}
    </div>
  );
}

const MAX_POINTS = 200;

export default function TimeSeriesChart({ entityId, liveEvents }: TimeSeriesChartProps) {
  const [data, setData] = useState<DataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // ── Fetch historical data when entity changes ──────────────────────────────
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    async function fetchHistory() {
      try {
        const since = new Date(Date.now() - 15 * 60 * 1000).toISOString();
        const res = await fetch(
          `${API_URL}/events?entity_id=${entityId}&since=${since}&limit=300`,
          { cache: 'no-store' }
        );
        if (!res.ok) throw new Error(`API error ${res.status}`);
        const json = await res.json();

        if (cancelled) return;

        // Build rolling mean from history data
        const events = (json.events || []) as Array<Record<string, unknown>>;
        const points = buildPoints(events);
        setData(points.slice(-MAX_POINTS));
        setLoading(false);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : 'Failed to load data');
        setLoading(false);
      }
    }

    fetchHistory();
    return () => { cancelled = true; };
  }, [entityId]);

  // ── Append live events from WebSocket ─────────────────────────────────────
  useEffect(() => {
    if (!liveEvents.length) return;
    const incoming = liveEvents.filter(
      (e) => e.sensor_id === entityId
    );
    if (!incoming.length) return;

    setData((prev) => {
      const next = [...prev, ...buildPoints(incoming)];
      return next.slice(-MAX_POINTS);
    });
  }, [liveEvents, entityId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-600 text-sm">
        Loading sensor history…
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-600 text-sm">
        <span className="text-anomaly mr-1">⚠</span> {error}
      </div>
    );
  }

  if (!data.length) {
    return (
      <div className="flex items-center justify-center h-64 text-zinc-600 text-sm">
        No data yet — waiting for events…
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center gap-4 mb-4 text-xs text-zinc-500">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-4 h-px bg-zinc-400" />
          Temperature
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-4 h-px bg-zinc-700" style={{ borderTop: '1px dashed #52525b' }} />
          Rolling mean
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-2 h-2 rounded-full bg-anomaly" />
          Anomaly
        </span>
      </div>

      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={data} margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#27272a"
            vertical={false}
          />
          <XAxis
            dataKey="time"
            tick={{ fill: '#71717a', fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fill: '#71717a', fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            width={40}
            domain={['auto', 'auto']}
          />
          <Tooltip content={<CustomTooltip />} />

          {/* Rolling mean band (upper) */}
          <Line
            type="monotone"
            dataKey="upperBand"
            stroke="#3f3f46"
            strokeDasharray="4 2"
            dot={false}
            activeDot={false}
            name="Upper band"
            strokeWidth={1}
          />
          {/* Rolling mean */}
          <Line
            type="monotone"
            dataKey="mean"
            stroke="#52525b"
            strokeDasharray="4 2"
            dot={false}
            strokeWidth={1}
            name="Mean"
          />
          {/* Rolling mean band (lower) */}
          <Line
            type="monotone"
            dataKey="lowerBand"
            stroke="#3f3f46"
            strokeDasharray="4 2"
            dot={false}
            activeDot={false}
            name="Lower band"
            strokeWidth={1}
          />
          {/* Raw temperature */}
          <Line
            type="monotone"
            dataKey="temperature"
            stroke="#a1a1aa"
            dot={false}
            strokeWidth={1.5}
            name="Temperature (°C)"
          />
          {/* Anomaly scatter points */}
          <Scatter
            dataKey="anomaly"
            fill="#ef4444"
            name="Anomaly"
            shape={(props: any) => {
              const { cx, cy } = props || {};
              if (cy == null) return <g />;
              return (
                <circle
                  cx={cx}
                  cy={cy}
                  r={4}
                  fill="#ef4444"
                  stroke="#fca5a5"
                  strokeWidth={1.5}
                  opacity={0.9}
                />
              );
            }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Build DataPoint array from raw event records ───────────────────────────
function buildPoints(events: Array<Record<string, unknown>>): DataPoint[] {
  // Compute rolling mean over a window of 20 points
  const WINDOW = 20;
  const sorted = [...events].sort((a, b) =>
    new Date(a.timestamp as string).getTime() - new Date(b.timestamp as string).getTime()
  );

  return sorted.map((e, i) => {
    const temp = Number(e.temperature);
    const isAnomaly = Boolean(e.is_anomaly);
    const ts = e.timestamp as string;

    // Rolling mean and std from recent window
    const windowStart = Math.max(0, i - WINDOW + 1);
    const window = sorted.slice(windowStart, i + 1).map((w) => Number(w.temperature));
    const mean = window.reduce((s, v) => s + v, 0) / window.length;
    const variance = window.reduce((s, v) => s + (v - mean) ** 2, 0) / window.length;
    const std = Math.sqrt(variance);

    return {
      time: fmtTime(ts),
      timestamp: new Date(ts).getTime(),
      temperature: temp,
      mean: Number(mean.toFixed(3)),
      upperBand: Number((mean + std).toFixed(3)),
      lowerBand: Number((mean - std).toFixed(3)),
      anomaly: isAnomaly ? temp : null,
    };
  });
}
