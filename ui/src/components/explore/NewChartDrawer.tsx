// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * NewChartDrawer — unified chart authoring (Epic 27 VG-304 + follow-up).
 *
 * Authors the query + visualization that compose a chart in a single
 * drawer. Both YAML files are saved atomically via api.saveChart, which
 * rolls back the query write if the view fails to validate.
 *
 * Mental model: the user creates ONE thing — a chart. Internally that's
 * still a query.yaml + a view.yaml sharing the same name. Power users
 * who want query reuse can still author standalone queries via the
 * regular endpoints; this drawer is the friendly default.
 *
 * "Start from existing query" pre-populates the query pane from an
 * existing artifact so reuse is one click away without forcing the user
 * to think about it.
 */

import { useEffect, useMemo, useState } from 'react'
import { X } from 'lucide-react'

import type { QuerySummary } from '@/api/client'
import { useModel } from '@/context/ModelContext'

const NAME_RE = /^[a-z][a-z0-9_]*$/

type ChartType = 'line' | 'bar' | 'kpi' | 'table'

interface Props {
  entity: string
  onClose: () => void
  onCreated?: (chartName: string) => void
}

export function NewChartDrawer({ entity, onClose, onCreated }: Props) {
  const { api } = useModel()
  const [queries, setQueries] = useState<QuerySummary[]>([])

  const [name, setName] = useState('')
  const [chartType, setChartType] = useState<ChartType>('bar')
  const [queryYaml, setQueryYaml] = useState('')
  const [viewYaml, setViewYaml] = useState('')
  const [queryDirty, setQueryDirty] = useState(false)
  const [viewDirty, setViewDirty] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // List existing queries for the "Start from" picker. Filtered to those
  // rooted on the current entity so the user only sees relevant ones.
  useEffect(() => {
    let cancelled = false
    api.listQueries()
      .then((qs) => { if (!cancelled) setQueries(qs.filter((q) => q.root === entity)) })
      .catch(() => { if (!cancelled) setQueries([]) })
    return () => { cancelled = true }
  }, [api, entity])

  // Regenerate templates when name / type / entity changes — unless the
  // user has typed into the pane (the dirty flags). Each pane tracks its
  // own dirty bit so editing one doesn't freeze the other.
  const defaultQuery = useMemo(
    () => buildQueryTemplate({ name, entity }),
    [name, entity],
  )
  const defaultView = useMemo(
    () => buildViewTemplate({ name, chartType }),
    [name, chartType],
  )
  useEffect(() => { if (!queryDirty) setQueryYaml(defaultQuery) }, [defaultQuery, queryDirty])
  useEffect(() => { if (!viewDirty) setViewYaml(defaultView) }, [defaultView, viewDirty])

  function loadExistingQuery(qName: string) {
    if (!qName) return
    api.getQuery(qName)
      .then((q) => {
        if (q.raw_yaml) {
          setQueryYaml(q.raw_yaml)
          setQueryDirty(true)
        }
      })
      .catch(() => { /* silent — picker keeps current yaml */ })
  }

  async function save() {
    setError(null)
    if (!NAME_RE.test(name)) {
      setError('Name must be lowercase letters / digits / underscores, starting with a letter.')
      return
    }
    setSaving(true)
    try {
      await api.saveChart(name, queryYaml, viewYaml)
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
      <div className="fixed top-0 right-0 bottom-0 w-[42rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70">{entity}</div>
            <h2 className="text-base font-semibold tracking-tight">New chart</h2>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          <Field label="Name">
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="snake_case_name"
              className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
            />
            <p className="text-[10px] text-muted-foreground/60 mt-1">
              Used as the name of both the query and the chart that consume it.
            </p>
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

          {queries.length > 0 && (
            <Field label="Start from existing query (optional)">
              <select
                onChange={(e) => loadExistingQuery(e.target.value)}
                defaultValue=""
                className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
              >
                <option value="">— author a new query —</option>
                {queries.map((q) => (
                  <option key={q.name} value={q.name}>{q.name}</option>
                ))}
              </select>
            </Field>
          )}

          <Field label="Query (YAML)">
            <textarea
              value={queryYaml}
              onChange={(e) => { setQueryYaml(e.target.value); setQueryDirty(true) }}
              rows={10}
              className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono resize-y"
            />
            {queryDirty && (
              <button
                onClick={() => setQueryDirty(false)}
                className="mt-1 text-[10px] text-muted-foreground hover:text-foreground"
              >
                Reset query to template
              </button>
            )}
          </Field>

          <Field label="Visualization (YAML)">
            <textarea
              value={viewYaml}
              onChange={(e) => { setViewYaml(e.target.value); setViewDirty(true) }}
              rows={10}
              className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono resize-y"
            />
            {viewDirty && (
              <button
                onClick={() => setViewDirty(false)}
                className="mt-1 text-[10px] text-muted-foreground hover:text-foreground"
              >
                Reset visualization to template
              </button>
            )}
          </Field>

          {error && <p className="text-xs text-red-600 whitespace-pre-wrap">{error}</p>}
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
            disabled={saving}
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

export function buildQueryTemplate({ name, entity }: { name: string; entity: string }): string {
  const safeName = name || '<query_name>'
  return [
    `name: ${safeName}`,
    `entity: ${entity}`,
    'attributes:',
    '  - <attribute_name>',
    'measures:',
    '  - count(*)',
    '',
  ].join('\n')
}

export function buildViewTemplate({
  name, chartType,
}: { name: string; chartType: ChartType }): string {
  const safeName = name || '<chart_name>'
  if (chartType === 'kpi') {
    return [
      `name: ${safeName}`,
      'type: metric',
      `query: ${safeName}`,
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
      `query: ${safeName}`,
      'visualization:',
      '  columns:',
      '    - <column_name>',
      '',
    ].join('\n')
  }
  return [
    `name: ${safeName}`,
    'type: chart',
    `query: ${safeName}`,
    'visualization:',
    `  chart_type: ${chartType}`,
    '  x: <x_column>',
    '  y:',
    '    - <y_column>',
    '',
  ].join('\n')
}
