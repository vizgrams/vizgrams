// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ViewContent — the canonical view renderer (Epic 20 VG-237).
 *
 * Takes a ``ViewResult`` (rows + columns + visualization spec) and
 * dispatches to the right chart / table / metric / map renderer with
 * drilldown handlers wired in. Used by every surface that renders a
 * view — ViewsPage, EntitiesPage (via row links), AppPage, chat.
 *
 * ``onNavigate`` is the seam: callers decide what happens on a
 * drilldown click. Pages and chat both hand the resulting ``DrillFrame``
 * off to react-router via ``frameToUrl``.
 */

import { useState } from 'react'
import {
  BarChart2, ChevronDown, ChevronRight, ChevronUp, ChevronsUpDown, LayoutDashboard,
} from 'lucide-react'

import type { ViewResult } from '@/api/client'
import { Card } from '@/components/Layout'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { CalendarHeatmapChart } from '@/components/charts/CalendarHeatmapChart'
import { MapChart } from '@/components/charts/MapChart'
import { cn, formatValue as _formatValue } from '@/lib/utils'
import {
  type DrillFrame,
  type ViewDrilldownConfig,
  resolveMarkerAction,
  resolvePointDrilldown,
  resolveViewDrilldown,
} from '@/components/view/drilldown'

function formatValue(value: string | number | null, fmt?: { type: string; unit?: string | null }): string {
  if (value == null) return '—'
  return _formatValue(value, fmt as Parameters<typeof _formatValue>[1])
}

export function ViewContent({
  result,
  rowDrilldown,
  appDrilldown,
  paramValues,
  onNavigate,
}: {
  result: ViewResult
  rowDrilldown?: ViewDrilldownConfig
  appDrilldown?: ViewDrilldownConfig
  paramValues: Record<string, string>
  onNavigate: (frame: DrillFrame) => void
}) {
  const [sort, setSort] = useState<{ col: string; dir: 'asc' | 'desc' } | null>(null)

  const viz = result.visualization as Record<string, unknown>
  const pointDrilldown = viz.point_drilldown as ViewDrilldownConfig | undefined

  const handleClickPoint = pointDrilldown
    ? (pointData: Record<string, unknown>) => {
        const frame = resolvePointDrilldown(pointDrilldown, pointData, paramValues)
        if (frame) onNavigate(frame)
      }
    : undefined

  if (result.type === 'metric') {
    const measureCol = result.measure
    const colIdx = measureCol ? result.columns.indexOf(measureCol) : -1
    const value = colIdx >= 0 && result.rows.length > 0 ? result.rows[0][colIdx] : null
    const suffix = viz.suffix as string | undefined
    return (
      <Card className="inline-flex flex-col items-start gap-1 px-8 py-6">
        <span className="text-4xl font-semibold tabular-nums">
          {value !== null ? String(value) : '—'}
          {suffix && <span className="ml-2 text-xl font-normal text-muted-foreground">{suffix}</span>}
        </span>
        {measureCol && <span className="text-sm text-muted-foreground">{measureCol}</span>}
      </Card>
    )
  }

  if (result.type === 'table') {
    const columns = (viz.columns as string[] | undefined) ?? result.columns
    const colIndices = columns.map((c) => result.columns.indexOf(c)).filter((i) => i >= 0)
    const displayCols = colIndices.map((i) => result.columns[i])
    const isDrillable = !!rowDrilldown
    const hasAppDrill = !!appDrilldown
    const appDrillTitle = appDrilldown?.label
      ?? (appDrilldown?.app ? `Open ${appDrilldown.app}` : undefined)

    const sortedRows = sort ? [...result.rows].sort((a, b) => {
      const idx = result.columns.indexOf(sort.col)
      if (idx < 0) return 0
      const av = a[idx], bv = b[idx]
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      const cmp = typeof av === 'number' && typeof bv === 'number'
        ? av - bv
        : String(av).localeCompare(String(bv))
      return sort.dir === 'asc' ? cmp : -cmp
    }) : result.rows

    function toggleSort(col: string) {
      setSort(prev =>
        prev?.col !== col ? { col, dir: 'asc' }
        : prev.dir === 'asc' ? { col, dir: 'desc' }
        : null
      )
    }

    return (
      <Card className="p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                {displayCols.map((col) => (
                  <th key={col} className="px-4 py-2.5 text-left font-medium whitespace-nowrap">
                    <button
                      onClick={() => toggleSort(col)}
                      className={cn(
                        'flex items-center gap-1 transition-colors hover:text-foreground',
                        sort?.col === col ? 'text-foreground' : 'text-muted-foreground',
                      )}
                    >
                      {col}
                      {sort?.col === col
                        ? sort.dir === 'asc'
                          ? <ChevronUp className="h-3 w-3" />
                          : <ChevronDown className="h-3 w-3" />
                        : <ChevronsUpDown className="h-3 w-3 opacity-30" />
                      }
                    </button>
                  </th>
                ))}
                {hasAppDrill && <th className="w-6" />}
                {isDrillable && <th className="w-6" />}
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row, i) => (
                <tr
                  key={i}
                  onClick={isDrillable ? () => {
                    const frame = resolveViewDrilldown(rowDrilldown!, row, result.columns, paramValues)
                    if (frame) onNavigate(frame)
                  } : undefined}
                  className={cn(
                    'border-b last:border-0 transition-colors',
                    isDrillable ? 'cursor-pointer hover:bg-primary/5 group' : 'hover:bg-muted/30',
                  )}
                >
                  {colIndices.map((ci, j) => {
                    const col = result.columns[ci]
                    const fmt = result.formats?.[col]
                    const val = row[ci]
                    return (
                      <td key={j} className="px-4 py-2.5 tabular-nums text-muted-foreground whitespace-nowrap">
                        {val != null ? formatValue(val, fmt) : <span className="opacity-30">—</span>}
                      </td>
                    )
                  })}
                  {hasAppDrill && (
                    <td className="pr-2">
                      <button
                        type="button"
                        title={appDrillTitle}
                        onClick={(e) => {
                          e.stopPropagation()
                          const frame = resolveViewDrilldown(appDrilldown!, row, result.columns, paramValues)
                          if (frame) onNavigate(frame)
                        }}
                        className="p-1 rounded text-muted-foreground/40 hover:text-foreground hover:bg-muted/60 transition-colors"
                      >
                        <LayoutDashboard className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  )}
                  {isDrillable && (
                    <td className="pr-3">
                      <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/30 group-hover:text-muted-foreground transition-colors" />
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    )
  }

  if (result.type === 'chart') {
    const chartType = viz.chart_type as string | undefined
    if (chartType === 'calendar_heatmap') {
      return (
        <Card className="p-4">
          <CalendarHeatmapChart
            rows={result.rows} columns={result.columns}
            dateKey={viz.date as string} valueKey={viz.value as string}
            groupByKey={viz.group_by as string | undefined}
            colorScheme={viz.color_scheme as string | undefined}
            label={viz.label as string | undefined}
            weeks={viz.weeks as number | undefined} height={180}
            onClickPoint={handleClickPoint}
          />
        </Card>
      )
    }
    if (chartType === 'line' || chartType === 'bar') {
      return (
        <Card className="p-4">
          <LineBarChart
            chartType={chartType} xKey={viz.x as string}
            yKeys={(viz.y as string[]) ?? []}
            rows={result.rows} columns={result.columns} height={320}
            formats={result.formats ?? undefined}
            onClickPoint={handleClickPoint}
            groupBy={viz.group_by as string | undefined}
            stack={viz.stack as 'absolute' | 'percent' | undefined}
          />
        </Card>
      )
    }
  }

  if (result.type === 'map') {
    const markerActionConfigs = (viz.marker_actions as ViewDrilldownConfig[] | undefined) ?? []
    return (
      <Card className="p-0 overflow-hidden">
        <MapChart
          rows={result.rows} columns={result.columns}
          latKey={viz.lat as string} lonKey={viz.lon as string}
          labelKey={viz.label as string | undefined}
          tooltipKeys={viz.popup as string[] | undefined}
          sizeKey={viz.size as string | undefined}
          zoom={viz.zoom as number | undefined}
          centerLat={viz.center_lat as number | undefined}
          centerLon={(viz.center_lon ?? viz.center_long) as number | undefined}
          markerActions={markerActionConfigs.map((a) => ({ label: a.label ?? '' }))}
          onMarkerAction={(i, rowDict) => {
            const cfg = markerActionConfigs[i]
            if (!cfg) return
            const frame = resolveMarkerAction(cfg, rowDict, paramValues)
            if (frame) onNavigate(frame)
          }}
          height={480}
        />
      </Card>
    )
  }

  // Fallback
  return (
    <Card className="flex items-center gap-3 bg-amber-50 border-amber-200 text-amber-800 text-sm">
      <BarChart2 className="h-4 w-4 shrink-0" />
      <span>Unsupported view type <code className="font-mono">{result.type}</code></span>
    </Card>
  )
}
