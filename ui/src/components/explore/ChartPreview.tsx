// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChartPreview — renders a saved view's chart inline (Epic 27 VG-300).
 *
 * Used by /explore Overview + Charts tabs in place of the old dashed
 * placeholder. Fetches the view's data on mount and dispatches to the
 * same chart components ViewContent uses (line/bar via Recharts, map,
 * calendar heatmap). KPI / metric views are not handled here — KpiCard
 * renders the scalar directly (VG-301).
 *
 * Click handlers are intentionally absent — chart drill-down on cards
 * lands in VG-302 (a side drawer with the full chart). The preview is
 * purely visual; the parent <ChartCardEl> wraps it in the clickable
 * shell.
 */

import { useEffect, useState } from 'react'

import type { ViewResult } from '@/api/client'
import { CalendarHeatmapChart } from '@/components/charts/CalendarHeatmapChart'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { useModel } from '@/context/ModelContext'

interface Props {
  viewName: string
  height?: number
}

export function ChartPreview({ viewName, height = 120 }: Props) {
  const { api } = useModel()
  const [result, setResult] = useState<ViewResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.executeView(viewName, 500)
      .then((r) => { if (!cancelled) setResult(r) })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, viewName])

  if (loading) {
    return <PreviewShell height={height}><span className="text-[10px] text-muted-foreground/60">Loading…</span></PreviewShell>
  }
  if (error || !result) {
    return <PreviewShell height={height}><span className="text-[10px] text-red-600/70">{error ?? 'No data'}</span></PreviewShell>
  }

  const viz = result.visualization as Record<string, unknown>
  const chartType = viz.chart_type as string | undefined

  if (result.type === 'chart' && (chartType === 'line' || chartType === 'bar')) {
    return (
      <PreviewShell height={height} bare>
        <LineBarChart
          chartType={chartType}
          xKey={viz.x as string}
          yKeys={(viz.y as string[]) ?? []}
          rows={result.rows}
          columns={result.columns}
          height={height}
          formats={result.formats ?? undefined}
          groupBy={viz.group_by as string | undefined}
          stack={viz.stack as 'absolute' | 'percent' | undefined}
        />
      </PreviewShell>
    )
  }

  if (result.type === 'chart' && chartType === 'calendar_heatmap') {
    return (
      <PreviewShell height={height} bare>
        <CalendarHeatmapChart
          rows={result.rows}
          columns={result.columns}
          dateKey={viz.date as string}
          valueKey={viz.value as string}
          groupByKey={viz.group_by as string | undefined}
          colorScheme={viz.color_scheme as string | undefined}
          label={viz.label as string | undefined}
          weeks={viz.weeks as number | undefined}
          height={height}
        />
      </PreviewShell>
    )
  }

  if (result.type === 'table') {
    // Compact preview — first 3 rows, first 3 cols. The full table opens
    // in the chart detail drawer (VG-302).
    const cols = result.columns.slice(0, 3)
    const rows = result.rows.slice(0, 3)
    return (
      <PreviewShell height={height}>
        <table className="w-full text-[10px] tabular-nums">
          <thead>
            <tr className="text-muted-foreground/70">
              {cols.map((c) => <th key={c} className="text-left font-normal px-1 pb-1">{c}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                {cols.map((_, j) => (
                  <td key={j} className="px-1 py-0.5 text-muted-foreground truncate max-w-[6rem]">
                    {r[j] != null ? String(r[j]) : '—'}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </PreviewShell>
    )
  }

  // Fallback — unsupported types (map needs a wider canvas; metric goes
  // through KpiCard; unknown chart_type just falls through). Show the
  // type label so the card still conveys what's there.
  return (
    <PreviewShell height={height}>
      <span className="text-[10px] text-muted-foreground/60 font-mono">{chartType ?? result.type}</span>
    </PreviewShell>
  )
}

function PreviewShell({
  height, bare, children,
}: { height: number; bare?: boolean; children: React.ReactNode }) {
  return (
    <div
      className={
        bare
          ? 'w-full overflow-hidden'
          : 'w-full rounded bg-muted/40 border border-dashed flex items-center justify-center overflow-hidden'
      }
      style={{ height }}
    >
      {children}
    </div>
  )
}
