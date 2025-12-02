'use client';

interface MetricsCardProps {
  title: string;
  value: string | number;
  unit?: string;
  description?: string;
  trend?: 'up' | 'down' | 'neutral';
  trendValue?: string;
}

export default function MetricsCard({ title, value, unit, description, trend, trendValue }: MetricsCardProps) {
  const trendConfig = {
    up: {
      color: 'text-green-400',
      bg: 'bg-green-500/10',
      icon: (
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
        </svg>
      ),
    },
    down: {
      color: 'text-red-400',
      bg: 'bg-red-500/10',
      icon: (
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
        </svg>
      ),
    },
    neutral: {
      color: 'text-slate-400',
      bg: 'bg-slate-500/10',
      icon: (
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14" />
        </svg>
      ),
    },
  };

  const trendStyle = trend ? trendConfig[trend] : null;

  return (
    <div className="relative bg-slate-800/50 rounded-xl p-6 border border-slate-700/50 card-hover group overflow-hidden">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <h3 className="text-sm font-medium text-slate-400 uppercase tracking-wide">{title}</h3>
        {trendStyle && (
          <div className={`flex items-center space-x-1 px-2 py-1 rounded-lg ${trendStyle.bg}`}>
            <span className={trendStyle.color}>{trendStyle.icon}</span>
            {trendValue && <span className={`text-xs font-medium ${trendStyle.color}`}>{trendValue}</span>}
          </div>
        )}
      </div>

      {/* Value */}
      <div className="flex items-baseline">
        <p className="text-3xl font-bold text-white group-hover:text-blue-400 transition-colors">
          {value}
        </p>
        {unit && (
          <span className="text-lg text-slate-400 ml-2">{unit}</span>
        )}
      </div>

      {/* Description */}
      {description && (
        <p className="mt-3 text-sm text-slate-500">{description}</p>
      )}

      {/* Decorative gradient line */}
      <div className="absolute bottom-0 left-0 right-0 h-1 bg-gradient-to-r from-blue-500/0 via-blue-500/50 to-purple-500/0 opacity-0 group-hover:opacity-100 transition-opacity rounded-b-xl"></div>
    </div>
  );
}
