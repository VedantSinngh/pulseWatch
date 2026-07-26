'use client';

import { useEffect, useRef } from 'react';

interface AnomalyEvent {
  sensor_id: string;
  timestamp: string;
  temperature: number;
  vibration: number;
  pressure: number;
  anomaly_types: string[];
  severity?: string;
  if_score?: number;
}

interface AlertFeedProps {
  alerts: AnomalyEvent[];
}

function fmtTimestamp(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  } catch {
    return iso;
  }
}

function AnomalyTypePill({ type }: { type: string }) {
  const labels: Record<string, string> = {
    zscore: 'Z-score',
    cusum: 'CUSUM',
    isolation_forest: 'Isolation Forest',
  };
  return (
    <span className="text-xs text-anomaly/70 font-medium">
      {labels[type] || type}
    </span>
  );
}

export default function AlertFeed({ alerts }: AlertFeedProps) {
  const feedRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to top when new alerts arrive (newest first)
  useEffect(() => {
    if (feedRef.current) {
      feedRef.current.scrollTop = 0;
    }
  }, [alerts.length]);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xs text-zinc-500 uppercase tracking-widest font-medium">
          Live Alerts
        </h2>
        {alerts.length > 0 && (
          <span className="text-xs text-anomaly font-semibold tabular-nums">
            {alerts.length}
          </span>
        )}
      </div>

      <div
        ref={feedRef}
        className="flex-1 overflow-y-auto space-y-px"
        aria-label="Live anomaly alert feed"
        aria-live="polite"
        aria-atomic="false"
      >
        {alerts.length === 0 ? (
          <p className="text-sm text-zinc-700 py-8 text-center">
            No anomalies detected yet
          </p>
        ) : (
          alerts.map((alert, idx) => (
            <div
              key={`${alert.timestamp}-${idx}`}
              className="animate-slide-in py-3 border-b border-zinc-900 last:border-0"
            >
              {/* Top row: sensor + time */}
              <div className="flex items-baseline justify-between mb-1">
                <span className="text-sm font-semibold text-anomaly">
                  {alert.sensor_id}
                </span>
                <span className="text-xs text-zinc-600 tabular-nums">
                  {fmtTimestamp(alert.timestamp)}
                </span>
              </div>

              {/* Values */}
              <div className="flex items-center gap-3 mb-1.5">
                <span className="text-xs text-zinc-400 tabular-nums">
                  {alert.temperature?.toFixed(1)}°C
                </span>
                <span className="text-zinc-800" aria-hidden="true">·</span>
                <span className="text-xs text-zinc-400 tabular-nums">
                  {alert.vibration?.toFixed(4)} g
                </span>
                <span className="text-zinc-800" aria-hidden="true">·</span>
                <span className="text-xs text-zinc-400 tabular-nums">
                  {alert.pressure?.toFixed(1)} kPa
                </span>
              </div>

              {/* Anomaly types */}
              <div className="flex flex-wrap gap-2">
                {(alert.anomaly_types || []).map((t) => (
                  <AnomalyTypePill key={t} type={t} />
                ))}
                {alert.severity && (
                  <span
                    className={`text-xs font-medium ${
                      alert.severity === 'high' ? 'text-anomaly' : 'text-yellow-600'
                    }`}
                  >
                    {alert.severity}
                  </span>
                )}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
