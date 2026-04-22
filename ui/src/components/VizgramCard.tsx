// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useState } from 'react'
import { formatDistanceToNow } from 'date-fns'
import { Heart, Bookmark } from 'lucide-react'
import type { VizgramSummary } from '@/api/client'
import { engageVizgram } from '@/api/client'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { CalendarHeatmapChart } from '@/components/charts/CalendarHeatmapChart'

// ---------------------------------------------------------------------------
// Mini table — shows up to 5 rows from the snapshot
// ---------------------------------------------------------------------------

function MiniTable({
  columns,
  rows,
  vizColumns,
}: {
  columns: string[]
  rows: (string | number | null)[][]
  vizColumns?: string[]
}) {
  const displayCols = vizColumns?.length
    ? vizColumns.map((c) => columns.indexOf(c)).filter((i) => i >= 0)
    : columns.map((_, i) => i)
  const visibleRows = rows.slice(0, 5)

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b bg-muted/50">
            {displayCols.map((ci) => (
              <th key={ci} className="px-3 py-1.5 text-left font-medium text-muted-foreground whitespace-nowrap">
                {columns[ci]}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row, i) => (
            <tr key={i} className="border-b last:border-0">
              {displayCols.map((ci, j) => (
                <td key={j} className="px-3 py-1.5 tabular-nums text-muted-foreground whitespace-nowrap">
                  {row[ci] != null ? String(row[ci]) : <span className="opacity-30">—</span>}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 5 && (
        <p className="px-3 py-1 text-[10px] text-muted-foreground/60">
          +{rows.length - 5} more rows
        </p>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Metric display
// ---------------------------------------------------------------------------

function MiniMetric({
  columns,
  rows,
  measureCol,
  suffix,
}: {
  columns: string[]
  rows: (string | number | null)[][]
  measureCol?: string
  suffix?: string
}) {
  const idx = measureCol ? columns.indexOf(measureCol) : -1
  const value = idx >= 0 && rows.length > 0 ? rows[0][idx] : null
  return (
    <div className="flex flex-col items-start gap-0.5 px-2 py-4">
      <span className="text-3xl font-semibold tabular-nums">
        {value != null ? String(value) : '—'}
        {suffix && <span className="ml-1.5 text-lg font-normal text-muted-foreground">{suffix}</span>}
      </span>
      {measureCol && <span className="text-xs text-muted-foreground">{measureCol}</span>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Visualization switcher
// ---------------------------------------------------------------------------

function VizgramViz({ vizgram }: { vizgram: VizgramSummary }) {
  const { chart_config, data_snapshot } = vizgram
  const rows = data_snapshot ?? []
  const columns = chart_config.columns ?? []
  const viz = chart_config.visualization

  if (chart_config.type === 'table') {
    return (
      <MiniTable
        columns={columns}
        rows={rows}
        vizColumns={viz.columns as string[] | undefined}
      />
    )
  }

  if (chart_config.type === 'metric') {
    return (
      <MiniMetric
        columns={columns}
        rows={rows}
        measureCol={viz.measure as string | undefined}
        suffix={viz.suffix as string | undefined}
      />
    )
  }

  if (chart_config.type === 'chart') {
    const chartType = viz.chart_type as string | undefined
    if (chartType === 'calendar_heatmap') {
      return (
        <CalendarHeatmapChart
          rows={rows}
          columns={columns}
          dateKey={viz.date as string}
          valueKey={viz.value as string}
          groupByKey={viz.group_by as string | undefined}
          colorScheme={viz.color_scheme as string | undefined}
          label={viz.label as string | undefined}
          weeks={viz.weeks as number | undefined}
          height={140}
        />
      )
    }
    if (chartType === 'line' || chartType === 'bar') {
      return (
        <LineBarChart
          chartType={chartType}
          xKey={viz.x as string}
          yKeys={(viz.y as string[]) ?? []}
          rows={rows}
          columns={columns}
          height={220}
        />
      )
    }
  }

  return (
    <div className="flex items-center justify-center h-20 text-xs text-muted-foreground/50">
      {chart_config.type}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Engage button — like or save with optimistic toggle
// ---------------------------------------------------------------------------

function EngageButton({
  icon: Icon,
  active,
  count,
  activeClass,
  onClick,
}: {
  icon: React.ElementType
  active: boolean
  count: number
  activeClass: string
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1 text-xs transition-colors ${
        active
          ? activeClass
          : 'text-muted-foreground/50 hover:text-muted-foreground'
      }`}
    >
      <Icon className={`h-3.5 w-3.5 ${active ? 'fill-current' : ''}`} />
      {count > 0 && <span className="tabular-nums">{count}</span>}
    </button>
  )
}

// ---------------------------------------------------------------------------
// VizgramCard
// ---------------------------------------------------------------------------

export function VizgramCard({ vizgram }: { vizgram: VizgramSummary }) {
  const relativeTime = formatDistanceToNow(new Date(vizgram.published_at), { addSuffix: true })

  const [liked, setLiked] = useState(vizgram.viewer_liked)
  const [saved, setSaved] = useState(vizgram.viewer_saved)
  const [likeCount, setLikeCount] = useState(vizgram.like_count)
  const [saveCount, setSaveCount] = useState(vizgram.save_count)

  const handleEngage = (type: 'like' | 'save') => {
    // Optimistic update
    if (type === 'like') {
      const next = !liked
      setLiked(next)
      setLikeCount((n) => n + (next ? 1 : -1))
    } else {
      const next = !saved
      setSaved(next)
      setSaveCount((n) => n + (next ? 1 : -1))
    }

    engageVizgram(vizgram.id, type).then((res) => {
      // Reconcile with server response
      setLiked(res.liked)
      setSaved(res.saved)
      setLikeCount(res.like_count)
      setSaveCount(res.save_count)
    }).catch(() => {
      // Revert on failure
      if (type === 'like') {
        setLiked(liked)
        setLikeCount(likeCount)
      } else {
        setSaved(saved)
        setSaveCount(saveCount)
      }
    })
  }

  return (
    <div className="rounded-lg border bg-card overflow-hidden flex flex-col">
      {/* Visualization */}
      <div className="border-b bg-muted/20 min-h-[80px]">
        <VizgramViz vizgram={vizgram} />
      </div>

      {/* Content */}
      <div className="p-4 flex flex-col gap-2 flex-1">
        <h3 className="text-sm font-semibold leading-snug">{vizgram.title}</h3>
        {vizgram.caption && (
          <p className="text-xs text-muted-foreground leading-relaxed line-clamp-3">
            {vizgram.caption}
          </p>
        )}

        {/* Footer: metadata + engagement */}
        <div className="flex items-center justify-between mt-auto pt-1">
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground/70 flex-wrap">
            <span className="font-mono">{vizgram.query_ref}</span>
            <span>·</span>
            <span>{vizgram.dataset_ref}</span>
            <span>·</span>
            <span>{relativeTime}</span>
            <span>·</span>
            <span>{vizgram.author_display_name ?? vizgram.author_id}</span>
          </div>
          <div className="flex items-center gap-3 ml-3 shrink-0">
            <EngageButton
              icon={Heart}
              active={liked}
              count={likeCount}
              activeClass="text-rose-500"
              onClick={() => handleEngage('like')}
            />
            <EngageButton
              icon={Bookmark}
              active={saved}
              count={saveCount}
              activeClass="text-blue-500"
              onClick={() => handleEngage('save')}
            />
          </div>
        </div>
      </div>
    </div>
  )
}
