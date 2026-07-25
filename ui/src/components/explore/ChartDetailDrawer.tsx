// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChartDetailDrawer — full-chart side panel on /explore (Epic 27 VG-302
 * + edit follow-up).
 *
 * Two modes:
 *   - Preview (default): renders the chart via the canonical ViewContent.
 *     Drilldowns inside the drawer push a DrilldownOverlay on top.
 *   - Edit: single YAML editor showing the composed chart (query fields
 *     at the top level + visualization block). Save calls api.saveChart
 *     which server-side splits back into the query + view pair (with
 *     query rollback on view validation failure). Switching back to
 *     Preview refetches.
 *
 * The chart-as-one-thing UX is the point: users don't have to think
 * "edit query, then edit view." Members proposing changes is deferred
 * to a follow-up — for now Edit is an admin-only direct-save.
 */

import { useEffect, useMemo, useState } from 'react'
import { Eye, Pencil, X } from 'lucide-react'

import type { ViewDetail, ViewResult } from '@/api/client'
import { DrilldownOverlay } from '@/components/explore/DrilldownOverlay'
import { ViewContent } from '@/components/view/ViewContent'
import { ViewParamBar } from '@/components/view/ViewParamBar'
import type { DrillFrame, ViewDrilldownConfig } from '@/components/view/drilldown'
import { useModel } from '@/context/ModelContext'

interface Props {
  viewName: string
  onClose: () => void
}

type Mode = 'preview' | 'edit'

export function ChartDetailDrawer({ viewName, onClose }: Props) {
  const { api } = useModel()
  const [mode, setMode] = useState<Mode>('preview')
  const [detail, setDetail] = useState<ViewDetail | null>(null)
  const [result, setResult] = useState<ViewResult | null>(null)
  const [chartYaml, setChartYaml] = useState<string>('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [executing, setExecuting] = useState(false)
  const [refreshTick, setRefreshTick] = useState(0)
  const [nested, setNested] = useState<DrillFrame | null>(null)

  // Param values the user is editing — start empty, the ViewParamBar's
  // placeholders show the defaults from the view. On Apply we re-execute
  // with these values. Reset whenever we switch to a different view.
  const [paramValues, setParamValues] = useState<Record<string, string>>({})
  useEffect(() => { setParamValues({}) }, [viewName])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    Promise.all([
      api.getView(viewName),
      api.executeView(viewName, 1000, 0, paramValues),
      // Composed chart YAML for the Edit tab. 404 is recoverable (the
      // view exists but has no query behind it) — edit shows blank so
      // the user can paste one in.
      api.getChart(viewName).catch(() => null),
    ])
      .then(([d, r, c]) => {
        if (cancelled) return
        setDetail(d)
        setResult(r)
        setChartYaml(c?.chart_yaml ?? '')
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
    // paramValues changes go through applyParams(), not this effect — the
    // initial load uses whatever empty/default state is current.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, viewName, refreshTick])

  // Re-execute with new param values (Apply button in ViewParamBar).
  // Keeps the detail + query already fetched — just refreshes the data.
  async function applyParams() {
    setExecuting(true)
    setError(null)
    try {
      const r = await api.executeView(viewName, 1000, 0, paramValues)
      setResult(r)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to execute')
    } finally {
      setExecuting(false)
    }
  }

  // Row-click vs. app drilldown — both come from the view's viz spec.
  // The row drilldown drives the click-on-row navigation (typically to a
  // detail view); the app drilldown surfaces a secondary affordance per row
  // that hops to a related application. Previously these were collapsed
  // into a single slot via ??, which silently dropped app_drilldown when
  // row_drilldown was also present (the dora_clt_by_team → team_health case).
  const rowDrilldown = useMemo<ViewDrilldownConfig | undefined>(() => {
    const viz = result?.visualization as Record<string, unknown> | undefined
    return viz?.row_drilldown as ViewDrilldownConfig | undefined
  }, [result])
  const appDrilldown = useMemo<ViewDrilldownConfig | undefined>(() => {
    const viz = result?.visualization as Record<string, unknown> | undefined
    return viz?.app_drilldown as ViewDrilldownConfig | undefined
  }, [result])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[44rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div className="min-w-0">
            <h2 className="text-base font-semibold tracking-tight">{viewName}</h2>
            {detail?.query && (
              <p className="text-[11px] text-muted-foreground/70 mt-0.5 font-mono truncate">
                query: {detail.query}
              </p>
            )}
          </div>
          <div className="flex items-center gap-1 shrink-0">
            <button
              onClick={() => setMode((m) => (m === 'preview' ? 'edit' : 'preview'))}
              title={mode === 'preview' ? 'Edit' : 'Preview'}
              className="text-xs inline-flex items-center gap-1 px-2 py-1 rounded border bg-card text-muted-foreground hover:text-foreground"
            >
              {mode === 'preview' ? <><Pencil className="h-3 w-3" /> Edit</> : <><Eye className="h-3 w-3" /> Preview</>}
            </button>
            <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-1">
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
          {loading && <p className="text-xs text-muted-foreground">Loading…</p>}
          {error && <p className="text-xs text-red-600">{error}</p>}

          {!loading && !error && mode === 'preview' && result && (
            <>
              {detail?.params && detail.params.length > 0 && (
                <ViewParamBar
                  params={detail.params}
                  values={paramValues}
                  onChange={setParamValues}
                  onApply={applyParams}
                />
              )}
              {executing && <p className="text-xs text-muted-foreground">Updating…</p>}
              <ViewContent
                result={result}
                rowDrilldown={rowDrilldown}
                appDrilldown={appDrilldown}
                paramValues={paramValues}
                onNavigate={(frame) => setNested(frame)}
              />
            </>
          )}

          {!loading && !error && mode === 'edit' && (
            <EditPanel
              viewName={viewName}
              initialChartYaml={chartYaml}
              onSaved={() => {
                setMode('preview')
                setRefreshTick((t) => t + 1)
              }}
            />
          )}
        </div>
      </div>
      {nested && (
        <DrilldownOverlay frame={nested} onClose={() => setNested(null)} />
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// Edit panel — single YAML textarea for the composed chart. Server splits
// into query + view on save (see api/services/chart_service.py).
// ---------------------------------------------------------------------------

function EditPanel({
  viewName, initialChartYaml, onSaved,
}: {
  viewName: string
  initialChartYaml: string
  onSaved: () => void
}) {
  const { api } = useModel()
  const [chartYaml, setChartYaml] = useState(initialChartYaml)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => { setChartYaml(initialChartYaml) }, [initialChartYaml])

  async function save() {
    setSaving(true)
    setError(null)
    try {
      await api.saveChart(viewName, chartYaml)
      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70 mb-1.5">
          Chart (YAML)
        </div>
        <textarea
          value={chartYaml}
          onChange={(e) => setChartYaml(e.target.value)}
          rows={24}
          className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono resize-y"
          placeholder="No chart YAML found — paste one here to create it."
        />
      </div>

      {error && <p className="text-xs text-red-600 whitespace-pre-wrap">{error}</p>}

      <div className="flex justify-end gap-2">
        <button
          onClick={save}
          disabled={saving}
          className="text-xs px-3 py-1.5 rounded border bg-foreground text-background hover:bg-foreground/90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {saving ? 'Saving…' : 'Save chart'}
        </button>
      </div>
    </div>
  )
}
