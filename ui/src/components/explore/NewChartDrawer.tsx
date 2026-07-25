// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * NewChartDrawer — single-editor chart authoring.
 *
 * A chart is one YAML file: query fields (root, attributes, where, …)
 * live at the top level, visualization + type live under the
 * ``visualization`` block. The server splits this into a query + view
 * pair on save (see ``api/services/chart_service.py``).
 *
 * "Start from existing query" fetches the query YAML and inlines its
 * fields into the current chart YAML — one click to reuse a shared
 * query without leaving the single-file mental model.
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
  const [chartYaml, setChartYaml] = useState('')
  const [dirty, setDirty] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    api.listQueries()
      .then((qs) => { if (!cancelled) setQueries(qs.filter((q) => q.root === entity)) })
      .catch(() => { if (!cancelled) setQueries([]) })
    return () => { cancelled = true }
  }, [api, entity])

  // Regenerate the template unless the user has typed into the editor.
  // Once dirty, name/type changes stop clobbering their work — the
  // ``Reset to template`` button is the escape hatch.
  const defaultYaml = useMemo(
    () => buildChartTemplate({ name, entity, chartType }),
    [name, entity, chartType],
  )
  useEffect(() => { if (!dirty) setChartYaml(defaultYaml) }, [defaultYaml, dirty])

  function loadExistingQuery(qName: string) {
    if (!qName) return
    api.getQuery(qName)
      .then((q) => {
        if (q.raw_yaml) {
          // Inline the existing query fields under the current chart's
          // visualization block. Renaming the ``name:`` field to the
          // chart's new name is deliberate — the on-disk artifact keeps
          // its identity, but the *chart* the user is authoring is the
          // one they typed a name for.
          setChartYaml(inlineQueryIntoChart(q.raw_yaml, name, chartType))
          setDirty(true)
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
      await api.saveChart(name, chartYaml)
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

          <Field label="Chart (YAML)">
            <textarea
              value={chartYaml}
              onChange={(e) => { setChartYaml(e.target.value); setDirty(true) }}
              rows={22}
              className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono resize-y"
            />
            {dirty && (
              <button
                onClick={() => setDirty(false)}
                className="mt-1 text-[10px] text-muted-foreground hover:text-foreground"
              >
                Reset to template
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

// ---------------------------------------------------------------------------
// Templates — one chart YAML, query fields at top level + visualization block
// ---------------------------------------------------------------------------

export function buildChartTemplate({
  name, entity, chartType,
}: { name: string; entity: string; chartType: ChartType }): string {
  const safeName = name || '<chart_name>'
  const commonQuery = [
    `name: ${safeName}`,
    `entity: ${entity}`,
    'attributes:',
    '  - <attribute_name>',
    'measures:',
    '  - count(*)',
    '',
  ]
  if (chartType === 'kpi') {
    return [
      ...commonQuery,
      'type: metric',
      'measure: <measure_name>',
      'visualization:',
      '  suffix: ""',
      '',
    ].join('\n')
  }
  if (chartType === 'table') {
    return [
      ...commonQuery,
      'type: table',
      'visualization:',
      '  columns:',
      '    - <column_name>',
      '',
    ].join('\n')
  }
  return [
    ...commonQuery,
    'type: chart',
    'visualization:',
    `  chart_type: ${chartType}`,
    '  x: <x_column>',
    '  y:',
    '    - <y_column>',
    '',
  ].join('\n')
}

// Inline an existing query YAML into a fresh chart template. String-level
// composition rather than YAML parsing so we don't pull in js-yaml or
// disturb the user's field ordering; the ``name:`` line is retargeted to
// the chart being authored.
function inlineQueryIntoChart(queryYaml: string, chartName: string, chartType: ChartType): string {
  const retargeted = queryYaml.replace(
    /^name:\s*[a-zA-Z0-9_]+\s*$/m,
    `name: ${chartName || '<chart_name>'}`,
  )
  const trimmed = retargeted.trimEnd()
  const vizBlock = buildVisualizationBlock(chartType)
  return `${trimmed}\n${vizBlock}\n`
}

function buildVisualizationBlock(chartType: ChartType): string {
  if (chartType === 'kpi') {
    return [
      'type: metric',
      'measure: <measure_name>',
      'visualization:',
      '  suffix: ""',
    ].join('\n')
  }
  if (chartType === 'table') {
    return [
      'type: table',
      'visualization:',
      '  columns:',
      '    - <column_name>',
    ].join('\n')
  }
  return [
    'type: chart',
    'visualization:',
    `  chart_type: ${chartType}`,
    '  x: <x_column>',
    '  y:',
    '    - <y_column>',
  ].join('\n')
}
