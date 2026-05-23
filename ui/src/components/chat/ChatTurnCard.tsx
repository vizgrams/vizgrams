// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChatTurnCard — renders one assistant turn (caption + chart + meta).
 *
 * Chart selection follows the backend's chart_type:
 *   bar / line → LineBarChart (existing component, recharts-based)
 *   table      → plain HTML table
 *   kpi        → single big number
 *   scatter    → table fallback for v1 (recharts scatter wiring is a follow-up)
 */

import { useState } from 'react'
import { AlertCircle, ChevronDown, ChevronUp, Code, FileCode } from 'lucide-react'

import type { ChatResponse } from '@/api/client'
import { Card } from '@/components/Layout'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { cn } from '@/lib/utils'

interface Props {
  response: ChatResponse
}

type SourceTab = 'query_yaml' | 'view_yaml' | 'sql'

export function ChatTurnCard({ response }: Props) {
  const [openTab, setOpenTab] = useState<SourceTab | null>(null)

  if (!response.success) {
    return (
      <Card>
        <div className="flex items-start gap-2 text-sm text-destructive">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">Couldn't answer that</div>
            <div className="text-xs text-muted-foreground mt-1">{response.error || 'Unknown failure.'}</div>
          </div>
        </div>
      </Card>
    )
  }

  return (
    <Card className="space-y-3">
      {response.content && (
        <p className="text-sm leading-relaxed">{response.content}</p>
      )}

      <ChartDisplay response={response} />

      {response.truncated && (
        <p className="text-xs text-muted-foreground">
          Showing first {response.rows.length} of {response.row_count.toLocaleString()} rows.
        </p>
      )}

      <SourceToggle response={response} openTab={openTab} setOpenTab={setOpenTab} />
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Source viewer — Query YAML / View YAML / SQL tabs.
// The YAMLs are the canonical artifacts (validated against the same schemas
// the existing query / view endpoints use). SQL is shown for debugging.
// ---------------------------------------------------------------------------

interface SourceToggleProps {
  response: ChatResponse
  openTab: SourceTab | null
  setOpenTab: (tab: SourceTab | null) => void
}

function SourceToggle({ response, openTab, setOpenTab }: SourceToggleProps) {
  const tabs: { key: SourceTab; label: string; content: string | null; icon: JSX.Element }[] = [
    { key: 'query_yaml', label: 'Query YAML', content: response.query_yaml, icon: <FileCode className="h-3 w-3" /> },
    { key: 'view_yaml', label: 'View YAML', content: response.view_yaml, icon: <FileCode className="h-3 w-3" /> },
    { key: 'sql', label: 'SQL', content: response.sql, icon: <Code className="h-3 w-3" /> },
  ]
  const available = tabs.filter((t) => t.content)
  if (available.length === 0) return null

  function toggle(key: SourceTab) {
    setOpenTab(openTab === key ? null : key)
  }

  const current = available.find((t) => t.key === openTab)
  return (
    <div className="border-t pt-2">
      <div className="flex items-center gap-3 text-xs text-muted-foreground">
        {available.map((t) => (
          <button
            key={t.key}
            type="button"
            onClick={() => toggle(t.key)}
            className={cn(
              'flex items-center gap-1 hover:text-foreground transition-colors',
              openTab === t.key && 'text-foreground font-medium',
            )}
          >
            {t.icon}
            {t.label}
            {openTab === t.key
              ? <ChevronUp className="h-3 w-3" />
              : <ChevronDown className="h-3 w-3" />}
          </button>
        ))}
      </div>
      {current && (
        <pre className="mt-2 text-xs bg-muted rounded p-3 overflow-x-auto whitespace-pre-wrap">
          {current.content}
        </pre>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart switch
// ---------------------------------------------------------------------------

function ChartDisplay({ response }: { response: ChatResponse }) {
  const { chart_type, columns, rows, x_field, y_field } = response

  if (!rows.length) {
    return (
      <div className="text-sm text-muted-foreground italic">No rows returned.</div>
    )
  }

  if (chart_type === 'kpi') {
    return <KpiTile response={response} />
  }

  if (chart_type === 'bar' || chart_type === 'line') {
    // Fall back to first non-x column if the LLM didn't pick y_field.
    const yKey = y_field || columns.find((c) => c !== x_field) || columns[0]
    const xKey = x_field || columns[0]
    if (!xKey || !yKey) {
      return <DataTable response={response} />
    }
    return (
      <LineBarChart
        chartType={chart_type}
        xKey={xKey}
        yKeys={[yKey]}
        rows={rows}
        columns={columns}
        height={280}
      />
    )
  }

  // table / scatter / null → fall back to a plain data table.
  return <DataTable response={response} />
}

// ---------------------------------------------------------------------------
// KPI: pick the first numeric column from the first row.
// ---------------------------------------------------------------------------

function KpiTile({ response }: { response: ChatResponse }) {
  const { columns, rows, y_field } = response
  const idx = y_field
    ? columns.indexOf(y_field)
    : columns.findIndex((_, i) => typeof rows[0][i] === 'number')
  const value = idx >= 0 ? rows[0][idx] : rows[0][0]
  const label = idx >= 0 ? columns[idx] : columns[0]
  return (
    <div className="py-4 text-center">
      <div className="text-4xl font-semibold tabular-nums">{formatValue(value)}</div>
      <div className="text-xs text-muted-foreground mt-1 uppercase tracking-wide">{label}</div>
    </div>
  )
}

function formatValue(v: unknown): string {
  if (v == null) return '—'
  if (typeof v === 'number') return v.toLocaleString()
  return String(v)
}

// ---------------------------------------------------------------------------
// DataTable: simple HTML table for table chart or fallback.
// ---------------------------------------------------------------------------

function DataTable({ response }: { response: ChatResponse }) {
  const { columns, rows } = response
  const visibleRows = rows.slice(0, 20)
  return (
    <div className="overflow-x-auto border rounded">
      <table className="w-full text-xs">
        <thead className="bg-muted">
          <tr>
            {columns.map((c) => (
              <th key={c} className="text-left px-3 py-1.5 font-medium">{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row, i) => (
            <tr key={i} className={cn(i % 2 === 1 && 'bg-muted/30')}>
              {row.map((cell, j) => (
                <td key={j} className="px-3 py-1.5 tabular-nums">
                  {formatValue(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > visibleRows.length && (
        <div className="text-xs text-muted-foreground px-3 py-1.5 border-t">
          + {rows.length - visibleRows.length} more rows
        </div>
      )}
    </div>
  )
}
