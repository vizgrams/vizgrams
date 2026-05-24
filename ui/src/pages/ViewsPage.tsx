// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ViewsPage — saved-views surface.
 *
 *   /views               left rail with the list of views, empty right pane
 *   /views/:name         left rail + the named view rendered on the right
 *   /views/:name?p=v     same, with the view params prefilled from the URL
 *
 * Drilldown clicks bubble up through ``ViewContent`` and resolve to a
 * router URL via ``frameToUrl`` — this page never holds a stack; browser
 * history is the stack.
 */

import { useCallback, useEffect, useState } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import {
  Activity, Hash,
  Loader2, Play, Plus, Save, Table, Upload,
} from 'lucide-react'

import type { ViewSummary, ViewResult } from '@/api/client'
import { publishVizgram, previewCaption } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { useRole } from '@/context/RoleContext'
import { cn } from '@/lib/utils'
import { ErrorMessage, Spinner } from '@/components/Layout'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { EditMode } from '@/pages/explore/EditShell'
import { YamlEditor } from '@/components/YamlEditor'
import {
  type DrillFrame,
  type ViewDrilldownConfig,
  frameToUrl,
} from '@/components/view/drilldown'
import { ViewContent } from '@/components/view/ViewContent'
import { ViewParamBar } from '@/components/view/ViewParamBar'
import {
  LibraryFilter,
  filterByLibrary,
  type LibraryFilterValue,
} from '@/components/library/LibraryFilter'

// ---------------------------------------------------------------------------
// Type icons / colours (left rail)
// ---------------------------------------------------------------------------

const TYPE_ICONS: Record<string, React.ReactNode> = {
  chart: <Activity className="h-3.5 w-3.5" />,
  table: <Table className="h-3.5 w-3.5" />,
  metric: <Hash className="h-3.5 w-3.5" />,
  map: <span className="h-3.5 w-3.5 inline-flex items-center justify-center text-[9px]">🗺</span>,
}
const TYPE_COLOURS: Record<string, string> = {
  chart: 'bg-violet-50 text-violet-700 border-violet-200',
  table: 'bg-blue-50 text-blue-700 border-blue-200',
  metric: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  map: 'bg-teal-50 text-teal-700 border-teal-200',
}

// Reserved-name validation matching the backend slug rules.
const NAME_RE = /^[a-z][a-z0-9_]*$/

// ---------------------------------------------------------------------------
// View result frame — renders a single saved view + param bar + edit + publish
// ---------------------------------------------------------------------------

function ViewResultFrame({
  name,
  initialParams,
  onNavigate,
  onParamsApplied,
}: {
  name: string
  initialParams: Record<string, string>
  onNavigate: (frame: DrillFrame) => void
  onParamsApplied?: (params: Record<string, string>) => void
}) {
  const { api, model } = useModel()
  const { role } = useRole()
  const [result, setResult] = useState<ViewResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [paramValues, setParamValues] = useState<Record<string, string>>(initialParams)
  const [yamlContent, setYamlContent] = useState('')
  const [savedYaml, setSavedYaml] = useState('')
  const [editMode, setEditMode] = useState<EditMode>('yaml')
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])
  const [viewType, setViewType] = useState<string>('')
  const [publishOpen, setPublishOpen] = useState(false)
  const [publishTitle, setPublishTitle] = useState('')
  const [publishCaption, setPublishCaption] = useState('')
  const [captionLoading, setCaptionLoading] = useState(false)
  const [captionUnavailable, setCaptionUnavailable] = useState(false)
  const [publishing, setPublishing] = useState(false)
  const [publishError, setPublishError] = useState<string | null>(null)

  const runView = useCallback(async (params: Record<string, string>, type = viewType) => {
    setError(null); setLoading(true)
    try {
      const limit = type === 'map' ? 10000 : 1000
      const r = await api.executeView(name, limit, 0, params)
      setResult(r)
      if (Object.keys(params).length === 0 && r.params?.length) {
        const defaults: Record<string, string> = {}
        for (const p of r.params) {
          if (p.default != null) defaults[p.name] = p.default
        }
        setParamValues((prev) => ({ ...defaults, ...prev }))
      }
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [api, name, viewType])

  useEffect(() => {
    api.getView(name).then((d) => {
      const yaml = d.raw_yaml ?? ''
      setYamlContent(yaml); setSavedYaml(yaml)
      setViewType(d.type)
      runView(paramValues, d.type)
    }).catch(() => { runView(paramValues) })
    setValidStatus('pending')
    api.validateView(name)
      .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
      .catch(() => setValidStatus('idle'))
    // Intentionally fires on name change only: paramValues changes per keystroke
    // and runView identity tracks viewType — both would cause spurious re-fetches.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name])

  const handleSaveYaml = useCallback(async () => {
    if (saving) return
    setSaving(true); setValidErrors([])
    try {
      const updated = await api.saveView(name, yamlContent)
      const yaml = updated.raw_yaml ?? yamlContent
      setSavedYaml(yaml)
      await runView(paramValues)
      setValidStatus('pending')
      api.validateView(name)
        .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
        .catch(() => setValidStatus('idle'))
    } catch (e) {
      setValidStatus('invalid')
      setValidErrors([{ path: '', message: String(e) }])
    } finally {
      setSaving(false)
    }
  }, [api, name, yamlContent, paramValues, saving, runView])

  const buildPayload = useCallback(() => {
    if (!result) return null
    const MAX_ROWS = result.type === 'table' ? 50 : 500
    return {
      model,
      query_ref: name,
      title: publishTitle.trim(),
      slice_config: { parameters: paramValues, snapshot_at: new Date().toISOString() },
      chart_config: { type: result.type, visualization: result.visualization, columns: result.columns },
      data_snapshot: result.rows.slice(0, MAX_ROWS),
    }
  }, [model, name, result, publishTitle, paramValues])

  const openPublishDialog = useCallback(async () => {
    if (!result) return
    setPublishTitle(name)
    setPublishCaption('')
    setCaptionUnavailable(false)
    setPublishError(null)
    setPublishOpen(true)
    setCaptionLoading(true)
    try {
      const MAX_ROWS = result.type === 'table' ? 50 : 500
      const payload = {
        model,
        query_ref: name,
        title: name,
        slice_config: { parameters: paramValues, snapshot_at: new Date().toISOString() },
        chart_config: { type: result.type, visualization: result.visualization, columns: result.columns },
        data_snapshot: result.rows.slice(0, MAX_ROWS),
      }
      const res = await previewCaption(payload)
      if (res.caption) {
        setPublishCaption(res.caption)
      } else {
        setCaptionUnavailable(true)
      }
    } catch {
      setCaptionUnavailable(true)
    } finally {
      setCaptionLoading(false)
    }
  }, [model, name, result, paramValues])

  const handlePublish = useCallback(async () => {
    const payload = buildPayload()
    if (!payload || !payload.title) return
    setPublishing(true); setPublishError(null)
    try {
      await publishVizgram({ ...payload, caption: publishCaption.trim() || undefined })
      setPublishOpen(false)
    } catch (e) {
      setPublishError(String(e))
    } finally {
      setPublishing(false)
    }
  }, [buildPayload, publishCaption])

  const viz = result ? (result.visualization as Record<string, unknown>) : {}
  const rowDrilldown = (viz.app_drilldown ?? viz.row_drilldown) as ViewDrilldownConfig | undefined
  const dirty = yamlContent !== savedYaml

  return (
    <div className="space-y-4">
      {/* Toolbar */}
      <div className="flex items-center gap-2">
        <h1 className="text-lg font-semibold flex-1">{name}</h1>
        {result && (
          <span className="text-xs text-muted-foreground">
            {result.row_count.toLocaleString()} row{result.row_count !== 1 ? 's' : ''}
            {result.truncated && ' (truncated)'}
            {' · '}{result.duration_ms}ms
          </span>
        )}
        {dirty && !saving && <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />}
        <button disabled={!dirty || saving} onClick={handleSaveYaml}
          className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
          {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
          {saving ? 'Saving…' : 'Save'}
        </button>
        <button disabled={loading} onClick={() => { runView(paramValues); onParamsApplied?.(paramValues) }}
          className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
          {loading ? 'Running…' : 'Run'}
          <span className="text-[10px] opacity-60 ml-0.5 hidden sm:inline">⌘↵</span>
        </button>
        {result && (role === 'admin' || role === 'creator') && (
          <button onClick={openPublishDialog}
            className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors">
            <Upload className="h-3.5 w-3.5" />
            Publish
          </button>
        )}
      </div>

      <EditSection
        defaultOpen={false}
        mode={editMode}
        onModeChange={setEditMode}
        isDirty={dirty}
        validStatus={validStatus}
        builderContent={<p className="text-sm text-muted-foreground">Visual builder coming soon.</p>}
        yamlContent={
          <YamlEditor
            name={`${name}.yaml`}
            historyKey={{ type: 'view', name }}
            content={yamlContent}
            savedContent={savedYaml}
            onChange={setYamlContent}
            onSave={handleSaveYaml}
            hideHeader
            hideSaveButton
          />
        }
        historyKey={{ type: 'view', name }}
        onRestoreVersion={(content) => setYamlContent(content)}
        validErrors={validErrors}
      />

      <ViewParamBar
        params={result?.params ?? []}
        values={paramValues}
        onChange={setParamValues}
        onApply={() => { runView(paramValues); onParamsApplied?.(paramValues) }}
      />

      {loading && <Spinner />}
      {error && <ErrorMessage message={error} />}

      {result && !loading && (
        <ViewContent
          result={result}
          rowDrilldown={rowDrilldown}
          paramValues={paramValues}
          onNavigate={onNavigate}
        />
      )}

      {publishOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
          onClick={() => setPublishOpen(false)}
        >
          <div
            className="bg-background rounded-lg border shadow-lg p-6 w-full max-w-md space-y-4"
            onClick={(e) => e.stopPropagation()}
          >
            <h2 className="text-base font-semibold">Publish vizgram</h2>

            <div className="space-y-1.5">
              <label className="text-xs font-medium text-muted-foreground">Title</label>
              <input
                type="text"
                value={publishTitle}
                onChange={(e) => setPublishTitle(e.target.value)}
                autoFocus
                className="w-full h-8 rounded border bg-background px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </div>

            <div className="space-y-1.5">
              <div className="flex items-center gap-2">
                <label className="text-xs font-medium text-muted-foreground">Caption</label>
                {captionLoading && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
                {captionUnavailable && !captionLoading && (
                  <span className="text-xs text-muted-foreground/60">AI unavailable — write your own</span>
                )}
              </div>
              <textarea
                value={publishCaption}
                onChange={(e) => setPublishCaption(e.target.value)}
                placeholder={captionLoading ? 'Generating…' : 'Add a caption (optional)'}
                disabled={captionLoading}
                rows={3}
                className="w-full rounded border bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-ring resize-none disabled:opacity-50"
              />
            </div>

            {publishError && <p className="text-xs text-destructive">{publishError}</p>}

            <div className="flex justify-end gap-2">
              <button
                onClick={() => setPublishOpen(false)}
                className="border rounded-md px-3 py-1.5 text-xs hover:bg-muted transition-colors"
              >
                Cancel
              </button>
              <button
                disabled={publishing || !publishTitle.trim() || captionLoading}
                onClick={handlePublish}
                className="bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40"
              >
                {publishing ? 'Publishing…' : 'Publish'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page shell — sidebar + right pane
// ---------------------------------------------------------------------------

export function ViewsPage() {
  const { model, api } = useModel()
  const { role, userId } = useRole()
  const navigate = useNavigate()
  const { name } = useParams<{ name?: string }>()
  const [searchParams, setSearchParams] = useSearchParams()

  const canCreate = role === 'admin' || role === 'creator'

  const [views, setViews] = useState<ViewSummary[]>([])
  const [filter, setFilter] = useState<LibraryFilterValue>('certified')

  // Reload the views list whenever the model changes.
  useEffect(() => {
    setViews([])
    api.listViews().then(setViews).catch(() => {})
    // api identity changes per-render — depend on the model string only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model])

  // When loading completes, if "Certified" is the active filter but nothing
  // is certified yet, fall through to "All" so the user isn't staring at an
  // empty list. Better UX than asking them to discover the toggle.
  useEffect(() => {
    if (filter !== 'certified') return
    if (views.length === 0) return
    if (!views.some((v) => v.is_certified)) setFilter('all')
    // Intentionally only on first views payload — the user can re-pick later.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [views.length > 0])

  const filteredViews = filterByLibrary(views, filter, userId)

  const params = Object.fromEntries(searchParams)

  const handleNavigate = useCallback((frame: DrillFrame) => {
    navigate(frameToUrl(frame))
  }, [navigate])

  // Run/Enter on the param bar updates the URL search params so the state
  // is shareable and survives a refresh.
  const handleParamsApplied = useCallback((next: Record<string, string>) => {
    const sp = new URLSearchParams()
    for (const [k, v] of Object.entries(next)) {
      if (v) sp.set(k, v)
    }
    setSearchParams(sp, { replace: true })
  }, [setSearchParams])

  async function startNewView() {
    const newName = window.prompt('New view name (lowercase, underscores only):')?.trim()
    if (!newName) return
    if (!NAME_RE.test(newName)) {
      alert(`Invalid name "${newName}". Use lowercase letters, digits, and underscores; must start with a letter.`)
      return
    }
    if (views.some((v) => v.name === newName)) {
      alert(`A view named "${newName}" already exists.`)
      return
    }
    const template = `name: ${newName}\ntype: chart\nquery: query_name\nvisualization:\n  chart_type: bar\n  x: x_column\n  y:\n    - y_column\n`
    try {
      await api.saveView(newName, template)
      const list = await api.listViews()
      setViews(list)
      navigate(`/views/${encodeURIComponent(newName)}`)
    } catch (e) {
      alert(`Failed to create view: ${String(e)}`)
    }
  }

  async function startNewApp() {
    const newName = window.prompt('New app name (lowercase, underscores only):')?.trim()
    if (!newName) return
    if (!NAME_RE.test(newName)) {
      alert(`Invalid name "${newName}". Use lowercase letters, digits, and underscores; must start with a letter.`)
      return
    }
    const template = `name: ${newName}\nviews: []\nlayout: []\nparams: []\n`
    try {
      await api.saveApplication(newName, template)
      navigate(`/apps/${encodeURIComponent(newName)}`)
    } catch (e) {
      alert(`Failed to create app: ${String(e)}`)
    }
  }

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      <aside className="w-52 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="flex-1 overflow-y-auto py-2">
          {canCreate && (
            <div className="px-2 pb-2 mb-1 border-b flex gap-1">
              <button
                onClick={startNewView}
                className="flex-1 flex items-center justify-center gap-1 text-xs text-muted-foreground hover:text-foreground hover:bg-muted rounded px-2 py-1.5 transition-colors"
                title="Create a new view"
              >
                <Plus className="h-3 w-3" /> View
              </button>
              <button
                onClick={startNewApp}
                className="flex-1 flex items-center justify-center gap-1 text-xs text-muted-foreground hover:text-foreground hover:bg-muted rounded px-2 py-1.5 transition-colors"
                title="Create a new app"
              >
                <Plus className="h-3 w-3" /> App
              </button>
            </div>
          )}
          <div className="px-2 pb-2 mb-1 border-b">
            <LibraryFilter
              value={filter}
              onChange={setFilter}
              currentUserId={userId}
              matchCount={filteredViews.length}
              totalCount={views.length}
            />
          </div>
          {views.length === 0
            ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">Loading…</p>
            : filteredViews.length === 0
            ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">
                No views match this filter.
              </p>
            : filteredViews.map((v) => {
                const active = name === v.name
                return (
                  <button
                    key={v.name}
                    onClick={() => navigate(`/views/${encodeURIComponent(v.name)}`)}
                    title={v.name}
                    className={cn(
                      'w-full text-left px-4 py-2 flex items-center gap-2.5 transition-colors',
                      active ? 'bg-primary/8 text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground',
                    )}
                  >
                    <span className={cn('shrink-0 inline-flex items-center rounded border p-0.5', TYPE_COLOURS[v.type] ?? 'bg-muted text-muted-foreground border-border')}>
                      {TYPE_ICONS[v.type]}
                    </span>
                    <span className="text-xs font-mono leading-snug line-clamp-2 break-all">{v.name}</span>
                  </button>
                )
              })
          }
        </div>
      </aside>

      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        <div className="flex-1 overflow-y-auto px-6 py-6">
          {name ? (
            <ViewResultFrame
              key={name}
              name={name}
              initialParams={params}
              onNavigate={handleNavigate}
              onParamsApplied={handleParamsApplied}
            />
          ) : (
            <div className="flex flex-col items-center justify-center h-48 text-center gap-2">
              <p className="text-muted-foreground text-sm">Select a view from the list to start exploring.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
