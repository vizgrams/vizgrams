// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * NewChartDrawer — author a new chart from /explore (Epic 27 VG-304).
 *
 * Replaces the dead "+ New chart" → /views?root= link. Minimal form: pick
 * a query rooted on the current entity, name the chart, pick a chart
 * type, and edit the generated YAML template before saving via
 * api.saveView. Matches the spirit of the existing /views startNewView
 * flow (prompt + template) but in a proper drawer with query selection
 * scoped to the entity.
 */

import { useEffect, useState } from 'react'
import { X } from 'lucide-react'

import type { QuerySummary } from '@/api/client'
import { useModel } from '@/context/ModelContext'

const NAME_RE = /^[a-z][a-z0-9_]*$/

type ChartType = 'line' | 'bar' | 'kpi' | 'table'

interface Props {
  entity: string
  onClose: () => void
  onCreated?: (viewName: string) => void
}

export function NewChartDrawer({ entity, onClose, onCreated }: Props) {
  const { api } = useModel()
  const [queries, setQueries] = useState<QuerySummary[]>([])
  const [loadingQueries, setLoadingQueries] = useState(true)

  const [name, setName] = useState('')
  const [queryName, setQueryName] = useState('')
  const [chartType, setChartType] = useState<ChartType>('bar')
  const [yaml, setYaml] = useState('')
  const [yamlDirty, setYamlDirty] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Load queries rooted on this entity. The /query endpoint returns all
  // queries; we filter client-side by `root` so the user only sees
  // relevant choices.
  useEffect(() => {
    let cancelled = false
    setLoadingQueries(true)
    api.listQueries()
      .then((qs) => {
        if (cancelled) return
        const filtered = qs.filter((q) => q.root === entity)
        setQueries(filtered)
        if (filtered.length > 0 && !queryName) setQueryName(filtered[0].name)
      })
      .catch(() => { if (!cancelled) setQueries([]) })
      .finally(() => { if (!cancelled) setLoadingQueries(false) })
    return () => { cancelled = true }
    // queryName only initialised once — don't re-run when it changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, entity])

  // Regenerate the YAML template whenever name/query/type changes — but
  // only if the user hasn't started editing it themselves.
  useEffect(() => {
    if (yamlDirty) return
    setYaml(buildTemplate({ name, queryName, chartType }))
  }, [name, queryName, chartType, yamlDirty])

  async function save() {
    setError(null)
    if (!NAME_RE.test(name)) {
      setError('Name must be lowercase letters / digits / underscores, starting with a letter.')
      return
    }
    if (!queryName) {
      setError('Pick a query first.')
      return
    }
    setSaving(true)
    try {
      await api.saveView(name, yaml)
      onCreated?.(name)
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[36rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70">{entity}</div>
            <h2 className="text-base font-semibold tracking-tight">New chart</h2>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
          <Field label="Name">
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="snake_case_name"
              className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
            />
          </Field>

          <Field label={`Query rooted on ${entity}`}>
            {loadingQueries
              ? <p className="text-xs text-muted-foreground/60">Loading…</p>
              : queries.length === 0
                ? <p className="text-xs text-muted-foreground/60">No queries rooted on {entity}. Author a query first.</p>
                : (
                  <select
                    value={queryName}
                    onChange={(e) => setQueryName(e.target.value)}
                    className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
                  >
                    {queries.map((q) => (
                      <option key={q.name} value={q.name}>{q.name}</option>
                    ))}
                  </select>
                )}
          </Field>

          <Field label="Chart type">
            <div className="flex gap-1.5">
              {(['bar', 'line', 'kpi', 'table'] as ChartType[]).map((t) => (
                <button
                  key={t}
                  onClick={() => setChartType(t)}
                  className={
                    'text-xs px-2.5 py-1 rounded border ' +
                    (chartType === t ? 'bg-foreground text-background' : 'bg-card text-muted-foreground hover:text-foreground')
                  }
                >
                  {t}
                </button>
              ))}
            </div>
          </Field>

          <Field label="YAML">
            <textarea
              value={yaml}
              onChange={(e) => { setYaml(e.target.value); setYamlDirty(true) }}
              rows={12}
              className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono resize-y"
            />
            {yamlDirty && (
              <button
                onClick={() => { setYamlDirty(false) }}
                className="mt-1 text-[10px] text-muted-foreground hover:text-foreground"
              >
                Reset to template
              </button>
            )}
          </Field>

          {error && <p className="text-xs text-red-600">{error}</p>}
        </div>

        <div className="flex justify-end gap-2 px-5 py-3 border-t">
          <button
            onClick={onClose}
            className="text-xs text-muted-foreground hover:text-foreground px-3 py-1.5"
          >
            Cancel
          </button>
          <button
            onClick={save}
            disabled={saving || queries.length === 0}
            className="text-xs px-3 py-1.5 rounded border bg-foreground text-background hover:bg-foreground/90 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {saving ? 'Saving…' : 'Create chart'}
          </button>
        </div>
      </div>
    </>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70 mb-1.5">{label}</div>
      {children}
    </div>
  )
}

export function buildTemplate({
  name, queryName, chartType,
}: { name: string; queryName: string; chartType: ChartType }): string {
  const safeName = name || '<chart_name>'
  const safeQuery = queryName || '<query_name>'
  if (chartType === 'kpi') {
    return [
      `name: ${safeName}`,
      'type: metric',
      `query: ${safeQuery}`,
      'measure: <measure_name>',
      'visualization:',
      '  suffix: ""',
      '',
    ].join('\n')
  }
  if (chartType === 'table') {
    return [
      `name: ${safeName}`,
      'type: table',
      `query: ${safeQuery}`,
      'visualization:',
      '  columns:',
      '    - <column_name>',
      '',
    ].join('\n')
  }
  // bar / line
  return [
    `name: ${safeName}`,
    'type: chart',
    `query: ${safeQuery}`,
    'visualization:',
    `  chart_type: ${chartType}`,
    '  x: <x_column>',
    '  y:',
    '    - <y_column>',
    '',
  ].join('\n')
}
