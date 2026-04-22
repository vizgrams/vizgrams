// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { ResponsiveCalendar } from '@nivo/calendar'

// GitHub-style green colour scheme
const GITHUB_COLORS = ['#ebedf0', '#9be9a8', '#40c463', '#30a14e', '#216e39']

const COLOR_SCHEMES: Record<string, string[]> = {
  github: GITHUB_COLORS,
  sequential: ['#f0f9ff', '#bae6fd', '#7dd3fc', '#38bdf8', '#0284c7'],
  diverging: ['#fde68a', '#fbbf24', '#f59e0b', '#d97706', '#92400e'],
}

interface CalendarData {
  day: string
  value: number
}

interface Props {
  rows: unknown[][]
  columns: string[]
  dateKey: string
  valueKey: string
  groupByKey?: string
  colorScheme?: string
  label?: string
  weeks?: number
  height?: number
  onClickPoint?: (pointData: Record<string, unknown>) => void
}

const YEAR_SPACING = 32

function toDateRange(weeks: number): { from: string; to: string; yearCount: number } {
  const to = new Date()
  const from = new Date()
  from.setDate(from.getDate() - weeks * 7)

  // Snap from to Jan 1 of the start year so the grid is complete
  const fromYear = from.getFullYear()
  const toYear = to.getFullYear()
  const yearCount = toYear - fromYear + 1

  return {
    from: `${fromYear}-01-01`,
    to: to.toISOString().slice(0, 10),
    yearCount,
  }
}

function scaledHeight(baseHeight: number, yearCount: number): number {
  return baseHeight * yearCount + YEAR_SPACING * Math.max(yearCount - 1, 0)
}

function CalendarGrid({
  data,
  from,
  to,
  colors,
  height,
  label,
  group,
  onClickPoint,
}: {
  data: CalendarData[]
  from: string
  to: string
  colors: string[]
  height: number
  label: string
  group?: string
  onClickPoint?: (pointData: Record<string, unknown>) => void
}) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const handleClick = onClickPoint ? (datum: any) => {
    const pointData: Record<string, unknown> = { day: datum.day, value: datum.value }
    if (group != null) pointData.group = group
    onClickPoint(pointData)
  } : undefined

  return (
    <div style={{ height }} className={onClickPoint ? 'cursor-pointer' : ''}>
      <ResponsiveCalendar
        data={data}
        from={from}
        to={to}
        emptyColor={colors[0]}
        colors={colors.slice(1)}
        margin={{ top: 8, right: 16, bottom: 8, left: 32 }}
        yearSpacing={YEAR_SPACING}
        monthBorderColor="#ffffff"
        dayBorderWidth={2}
        dayBorderColor="#ffffff"
        onClick={handleClick}
        tooltip={({ day, value: v }) => (
          <div className="bg-white border border-border rounded px-2 py-1 text-xs shadow-sm">
            <span className="font-medium">{day}</span>: {v} {label}
          </div>
        )}
      />
    </div>
  )
}

export function CalendarHeatmapChart({
  rows,
  columns,
  dateKey,
  valueKey,
  groupByKey,
  colorScheme = 'github',
  label = 'contributions',
  weeks = 52,
  height = 180,
  onClickPoint,
}: Props) {
  const dateIdx = columns.indexOf(dateKey)
  const valueIdx = columns.indexOf(valueKey)
  const groupIdx = groupByKey ? columns.indexOf(groupByKey) : -1

  const colors = COLOR_SCHEMES[colorScheme] ?? GITHUB_COLORS
  const { from, to, yearCount } = toDateRange(weeks)
  const totalHeight = scaledHeight(height, yearCount)

  if (groupIdx >= 0) {
    const groups = new Map<string, CalendarData[]>()
    for (const row of rows) {
      const day = String(row[dateIdx])
      const value = Number(row[valueIdx]) || 0
      const group = String(row[groupIdx])
      if (!groups.has(group)) groups.set(group, [])
      groups.get(group)!.push({ day, value })
    }

    return (
      <div className="space-y-4">
        {Array.from(groups.entries()).map(([group, data]) => (
          <div key={group}>
            <p className="text-xs font-medium text-muted-foreground mb-1">{group}</p>
            <CalendarGrid
              data={data}
              from={from}
              to={to}
              colors={colors}
              height={totalHeight}
              label={label}
              group={group}
              onClickPoint={onClickPoint}
            />
          </div>
        ))}
      </div>
    )
  }

  const data: CalendarData[] = rows.map((row) => ({
    day: String(row[dateIdx]),
    value: Number(row[valueIdx]) || 0,
  }))

  return (
    <CalendarGrid
      data={data}
      from={from}
      to={to}
      colors={colors}
      height={totalHeight}
      label={label}
      onClickPoint={onClickPoint}
    />
  )
}
