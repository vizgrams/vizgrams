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
  '#ec4899', '#14b8a6', '#a855f7', '#84cc16', '#3b82f6', '#f43f5e', '#94a3b8',
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
  /** Column whose values become stacked series (long → wide pivot).
   *  When set, the chart pivots: each unique value of this column becomes a
   *  separate Bar stacked under the same xKey. yKeys[0] supplies the value. */
  groupBy?: string
  /** Bar stacking mode: 'absolute' stacks raw values; 'percent' normalises
   *  each x-bar to sum to 100%. Only meaningful with chartType='bar'. */
  stack?: 'absolute' | 'percent'
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
      {payload.map((entry: { color: string; name: string; value: unknown; dataKey: string }, i: number) => (
        <p key={i} style={{ margin: '2px 0', color: entry.color }}>
          {entry.name}: {formatTooltipValue(entry.value, formats?.[entry.dataKey])}
        </p>
      ))}
    </div>
  )
}

export function LineBarChart({ chartType, xKey, yKeys, rows, columns, height = 320, formats, onClickPoint, groupBy, stack }: Props) {
  // Transform rows + columns array into recharts object array
  const longData = rows.map((row) => {
    const obj: Record<string, unknown> = {}
    columns.forEach((col, i) => { obj[col] = row[i] })
    return obj
  })

  // Long → wide pivot when groupBy is set: rows of (x, group, value) become
  // one row per x with one column per unique group. yKeys[0] supplies the
  // value column. Effective series keys are the unique group values, in
  // first-seen order for stable colour assignment.
  let data = longData
  let effectiveYKeys = yKeys
  if (groupBy && yKeys.length > 0) {
    const valueKey = yKeys[0]
    const allSeries = new Set<string>()
    const wide: Record<string, Record<string, unknown>> = {}
    const xOrder: string[] = []
    for (const row of longData) {
      const x = String(row[xKey])
      const series = String(row[groupBy] ?? '(unset)')
      allSeries.add(series)
      if (!wide[x]) { wide[x] = { [xKey]: row[xKey] }; xOrder.push(x) }
      wide[x][series] = row[valueKey]
    }
    // Order series by the LATEST x-bar's values (descending) so the largest
    // current segment is drawn at the bottom (Recharts stacks first→bottom).
    // Ties break alphabetically for stability across re-renders.
    const latestX = xOrder[xOrder.length - 1]
    const latestRow = latestX ? wide[latestX] : {}
    const toNum = (v: unknown) => {
      if (v == null) return 0
      const n = typeof v === 'number' ? v : parseFloat(String(v))
      return isNaN(n) ? 0 : n
    }
    effectiveYKeys = [...allSeries].sort((a, b) => {
      const diff = toNum(latestRow[b]) - toNum(latestRow[a])
      return diff !== 0 ? diff : a.localeCompare(b)
    })
    data = Object.values(wide)

    // Normalise to percentages if requested — each x-row's series values are
    // scaled so they sum to 100. Empty/missing values stay 0.
    if (stack === 'percent') {
      data = data.map((row) => {
        const total = effectiveYKeys.reduce((s, k) => {
          const v = row[k]
          const n = typeof v === 'number' ? v : parseFloat(String(v ?? 0))
          return s + (isNaN(n) ? 0 : n)
        }, 0)
        if (total === 0) return row
        const out = { ...row }
        for (const k of effectiveYKeys) {
          const v = out[k]
          const n = typeof v === 'number' ? v : parseFloat(String(v ?? 0))
          out[k] = isNaN(n) ? 0 : n / total
        }
        return out
      })
    }
  }
  const stacked = !!groupBy
  const stackId = stacked ? 'stack' : undefined

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
            <YAxis
              tick={{ fontSize: 11, fill: '#6b7280' }} axisLine={false} tickLine={false} width={48}
              tickFormatter={stack === 'percent' ? (v) => `${Math.round(Number(v) * 100)}%` : undefined}
              domain={stack === 'percent' ? [0, 1] : undefined}
            />
            <Tooltip
              content={tooltipContent}
              contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e5e7eb' }}
              cursor={{ fill: onClickPoint ? '#f3f4f6' : '#f9fafb' }}
            />
            {effectiveYKeys.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
            {effectiveYKeys.map((key, i) => (
              <Bar key={key} dataKey={key} stackId={stackId}
                fill={SERIES_COLOURS[i % SERIES_COLOURS.length]}
                radius={stacked && i < effectiveYKeys.length - 1 ? 0 : [3, 3, 0, 0]}
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
            {effectiveYKeys.length > 1 && <Legend wrapperStyle={{ fontSize: 12 }} />}
            {effectiveYKeys.map((key, i) => (
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
