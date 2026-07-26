'use client';

const SENSORS = ['sensor-001', 'sensor-002', 'sensor-003', 'sensor-004', 'sensor-005'];

interface EntitySelectorProps {
  value: string;
  onChange: (id: string) => void;
}

export default function EntitySelector({ value, onChange }: EntitySelectorProps) {
  return (
    <div className="flex items-center gap-3">
      <label
        htmlFor="entity-select"
        className="text-xs text-zinc-500 uppercase tracking-widest font-medium whitespace-nowrap"
      >
        Sensor
      </label>
      <div className="relative">
        <select
          id="entity-select"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className={`
            appearance-none bg-zinc-900 border border-zinc-800 text-zinc-100
            text-sm font-medium rounded px-3 py-1.5 pr-7
            focus:outline-none focus:border-zinc-600
            cursor-pointer transition-colors hover:border-zinc-700
          `}
          aria-label="Select sensor to display"
        >
          {SENSORS.map((id) => (
            <option key={id} value={id}>
              {id}
            </option>
          ))}
        </select>
        {/* Custom dropdown arrow */}
        <span
          className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 text-zinc-500"
          aria-hidden="true"
        >
          <svg width="10" height="6" viewBox="0 0 10 6" fill="none">
            <path d="M1 1L5 5L9 1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
          </svg>
        </span>
      </div>
    </div>
  );
}
