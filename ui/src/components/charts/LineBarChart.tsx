// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'

const SERIES_COLOURS = [
  '#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#f97316',
]

interface ColFormat { type: string; unit?: string | null }

interface Props {
  chartType: 'line' | 'bar'
  xKey: string
  yKeys: string[]
  rows: unknown[][]
  columns: string[]
  height?: number
  formats?: Record<string, ColFormat>
  onClickPoint?: (pointData: Record<string, unknown>) => void
}

function formatTooltipValue(value: unknown, fmt?: ColFormat): string {
  if (value == null) return '—'
  const n = typeof value === 'number' ? value : parseFloat(String(value))
  if (isNaN(n)) return String(value)
  if (!fmt) return n.toFixed(2)
  if (fmt.type === 'duration') {
    const hours = n
    if (hours >= 24) return `${(hours / 24).toFixed(2)}d`
    if (hours >= 1) return `${hours.toFixed(2)}h`
    if (hours >= 1 / 60) return `${(hours * 60).toFixed(2)}m`
    return `${Math.round(hours * 3600)}s`
  }
  if (fmt.type === 'percent') return `${(n * 100).toFixed(2)}%`
  return n.toFixed(2)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomTooltip({ active, payload, label, formats, yKeys: _yKeys }: any) {
  if (!active || !payload?.length) return null
  return (
    <div style={{ fontSize: 12, borderRadius: 6, border: '1px solid #e5e7eb', background: '#fff', padding: '8px 12px' }}>
      <p style={{ marginBottom: 4, fontWeight: 500, color: '#374151' }}>{label}</p>
      {payload.map((entry, i: number) => (
        <p key={i} style={{ margin: '2px 0', color: entry.color }}>
          {entry.name}: {formatTooltipValue(entry.value, formats?.[entry.dataKey])}
        </p>
      ))}
    </div>
  )
}

export function LineBarChart({ chartType, xKey, yKeys, rows, columns, height = 320, formats, onClickPoint }: Props) {
  // Transform rows + columns array into recharts object array
  const data = rows.map((row) => {
    const obj: Record<string, unknown> = {}
    columns.forEach((col, i) => { obj[col] = row[i] })
    return obj
  })

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleChartClick = onClickPoint ? (payload: any) => {
    if (payload?.activePayload?.[0]?.payload) {
      onClickPoint(payload.activePayload[0].payload as Record<string, unknown>)
    }
  } : undefined

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleDotClick = onClickPoint ? (_event: any, data: any) => {
    if (data?.payload) onClickPoint(data.payload as Record<string, unknown>)
  } : undefined

  const commonProps = {
    data,
    margin: { top: 8, right: 16, left: 0, bottom: 4 },
    onClick: handleChartClick,
  }

  const cursorStyle = onClickPoint ? 'cursor-pointer' : ''

  const tooltipContent = formats
    ? <CustomTooltip formats={formats} yKeys={yKeys} />
    : undefined

  return (
    <div className={cursorStyle}>
      <ResponsiveContainer width="100%" height={height}>
        {chartType === 'bar' ? (
          <BarChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
            <XAxis dataKey={xKey} tick={{ fontSize: 11, fill: '#6b7280' }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} axisLine={false} tickLine={false} width={48} />
            <Tooltip
              content={tooltipContent}
              contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e5e7eb' }}
              cursor={{ fill: onClickPoint ? '#f3f4f6' : '#f9fafb' }}
            />
            {yKeys.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
            {yKeys.map((key, i) => (
              <Bar key={key} dataKey={key} fill={SERIES_COLOURS[i % SERIES_COLOURS.length]} radius={[3, 3, 0, 0]}
                onClick={onClickPoint ? (barData) => onClickPoint(barData as unknown as Record<string, unknown>) : undefined}
                cursor={onClickPoint ? 'pointer' : undefined}
              />
            ))}
          </BarChart>
        ) : (
          <LineChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
            <XAxis dataKey={xKey} tick={{ fontSize: 11, fill: '#6b7280' }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} axisLine={false} tickLine={false} width={48} />
            <Tooltip
              content={tooltipContent}
              contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e5e7eb' }}
            />
            {yKeys.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
            {yKeys.map((key, i) => (
              <Line
                key={key}
                type="monotone"
                dataKey={key}
                stroke={SERIES_COLOURS[i % SERIES_COLOURS.length]}
                strokeWidth={2}
                dot={false}
                activeDot={handleDotClick ? { r: 5, cursor: 'pointer', onClick: handleDotClick } : { r: 4 }}
              />
            ))}
          </LineChart>
        )}
      </ResponsiveContainer>
    </div>
  )
}
