// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useCallback, useEffect, useState } from 'react'
import { Loader2, Plus, Save, Play, Upload } from 'lucide-react'
import { useModel } from '@/context/ModelContext'
import { useRole } from '@/context/RoleContext'
import { Spinner, ErrorMessage } from '@/components/Layout'
import { YamlEditor } from '@/components/YamlEditor'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { ViewSummary, ViewDetail, ViewResult } from '@/api/client'
import { publishVizgram, previewCaption } from '@/api/client'
import { MapChart } from '@/components/charts/MapChart'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { cn } from '@/lib/utils'

export function ViewsPage() {
  const { api, model } = useModel()
  const { role } = useRole()
  const [views, setViews] = useState<ViewSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedName, setSelectedName] = useState<string | null>(null)
  const [detail, setDetail] = useState<ViewDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [editorContent, setEditorContent] = useState('')
  const [savedContent, setSavedContent] = useState('')
  const [saving, setSaving] = useState(false)
  const [editMode, setEditMode] = useState<'builder' | 'yaml'>('yaml')
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])
  const [isNewMode, setIsNewMode] = useState(false)
  const [viewRefresh, setViewRefresh] = useState(0)
  const [running, setRunning] = useState(false)
  const [runResult, setRunResult] = useState<ViewResult | null>(null)
  const [publishOpen, setPublishOpen] = useState(false)
  const [publishTitle, setPublishTitle] = useState('')
  const [publishCaption, setPublishCaption] = useState('')
  const [captionLoading, setCaptionLoading] = useState(false)
  const [publishing, setPublishing] = useState(false)
  const [publishError, setPublishError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    api.listViews()
      .then((list) => { setViews(list); setLoading(false) })
      .catch((e) => { setError(String(e)); setLoading(false) })
  }, [api, model, viewRefresh])

  useEffect(() => {
    if (!selectedName) return
    setDetailLoading(true)
    setValidStatus('idle')
    setValidErrors([])
    api.getView(selectedName)
      .then((d) => {
        setDetail(d)
        const yaml = d.raw_yaml ?? ''
        setEditorContent(yaml)
        setSavedContent(yaml)
        setDetailLoading(false)
      })
      .catch(() => setDetailLoading(false))
  }, [selectedName, api])

  useEffect(() => {
    if (!selectedName) return
    setValidStatus('pending')
    api.validateView(selectedName)
      .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
      .catch(() => setValidStatus('idle'))
  }, [selectedName, api])

  function startNew() {
    setSelectedName(null)
    setDetail(null)
    setIsNewMode(true)
    const template = `name: new_view\ntype: chart\nquery: query_name\nvisualization:\n  chart_type: bar\n`
    setEditorContent(template)
    setSavedContent('')
    setValidStatus('idle')
    setValidErrors([])
  }

  async function handleSave() {
    const saveName = isNewMode
      ? (editorContent.match(/^name:\s*(\S+)/m)?.[1] ?? 'new_view')
      : selectedName
    if (!saveName || saving) return
    setSaving(true); setValidErrors([])
    try {
      const updated = await api.saveView(saveName, editorContent)
      const yaml = updated.raw_yaml ?? editorContent
      setSavedContent(yaml)
      setDetail(updated)
      setSelectedName(saveName)
      setIsNewMode(false)
      setViewRefresh(c => c + 1)
      setValidStatus('pending')
      api.validateView(saveName)
        .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
        .catch(() => setValidStatus('idle'))
    } catch (e) {
      const msg = String(e)
      setValidStatus('invalid')
      setValidErrors([{ path: '', message: msg }])
    } finally {
      setSaving(false)
    }
  }

  async function handleRun() {
    if (!selectedName || running) return
    setRunning(true)
    try {
      const limit = detail?.type === 'map' ? 10000 : 1000
      const result = await api.executeView(selectedName, limit)
      setRunResult(result)
    } catch (e) {
      console.error(e)
    } finally {
      setRunning(false)
    }
  }

  const openPublishDialog = useCallback(async () => {
    if (!selectedName || !runResult) return
    setPublishOpen(true)
    setPublishTitle(selectedName)
    setPublishCaption('')
    setPublishError(null)
    setCaptionLoading(true)
    try {
      const res = await previewCaption({
        model,
        query_ref: selectedName,
        title: selectedName,
        slice_config: {},
        chart_config: runResult.visualization ?? {},
        data_snapshot: runResult.rows.slice(0, 50),
      })
      setPublishCaption(res.caption ?? '')
    } catch { /* caption is optional */ }
    finally { setCaptionLoading(false) }
  }, [model, selectedName, runResult])

  async function handlePublish() {
    if (!selectedName || !runResult || publishing) return
    setPublishing(true)
    setPublishError(null)
    try {
      await publishVizgram({
        model,
        query_ref: selectedName,
        title: publishTitle.trim() || selectedName,
        caption: publishCaption || undefined,
        slice_config: {},
        chart_config: runResult.visualization ?? {},
        data_snapshot: runResult.rows.slice(0, 200),
      })
      setPublishOpen(false)
    } catch (e) {
      setPublishError(e instanceof Error ? e.message : 'Publish failed')
    } finally {
      setPublishing(false)
    }
  }

  if (loading) return <Spinner />
  if (error) return <ErrorMessage message={error} />

  const dirty = editorContent !== savedContent

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left: view list */}
      <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="px-3 py-3 border-b flex items-center justify-between shrink-0">
          <span className="text-sm font-semibold">Views</span>
          <button onClick={startNew} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
            <Plus className="h-3.5 w-3.5" /> New
          </button>
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {isNewMode && (
            <div className="w-full text-left px-4 py-2.5 border-b border-border/30 bg-muted">
              <div className="text-sm font-medium text-foreground/50 italic flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-orange-400 shrink-0" />
                {editorContent.match(/^name:\s*(\S+)/m)?.[1] ?? 'new_view'}
              </div>
            </div>
          )}
          {views.map((v) => (
            <button
              key={v.name}
              onClick={() => { setSelectedName(v.name); setIsNewMode(false) }}
              title={v.name}
              className={cn(
                'w-full text-left px-4 py-2 text-sm transition-colors',
                selectedName === v.name
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground',
              )}
            >
              <div className="line-clamp-2 break-all">{v.name}</div>
              {v.query && (
                <div className="text-[10px] font-mono text-muted-foreground/60 mt-0.5">
                  {v.type} &middot; {v.query}
                </div>
              )}
            </button>
          ))}
          {views.length === 0 && !isNewMode && (
            <p className="px-4 py-6 text-xs text-muted-foreground text-center">No views found</p>
          )}
        </div>
      </aside>

      {/* Right: detail + editor */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {!selectedName && !isNewMode ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select a view to edit
          </div>
        ) : (
          <>
            {/* Toolbar */}
            <div className="shrink-0 border-b px-6 py-3 flex items-center gap-2">
              <h1 className="text-lg font-semibold flex-1">{isNewMode ? (editorContent.match(/^name:\s*(\S+)/m)?.[1] ?? 'new_view') : selectedName}</h1>
              {dirty && !saving && <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />}
              <button disabled={(!dirty && !isNewMode) || saving} onClick={handleSave}
                className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
                {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
                {saving ? 'Saving...' : 'Save'}
              </button>
              {!isNewMode && selectedName && (
                <>
                  <button disabled={running} onClick={handleRun}
                    className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
                    {running ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                    {running ? 'Running...' : 'Run'}
                  </button>
                  {runResult && (role === 'admin' || role === 'creator') && (
                    <button onClick={openPublishDialog}
                      className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors">
                      <Upload className="h-3.5 w-3.5" />
                      Publish
                    </button>
                  )}
                </>
              )}
            </div>

            {/* Stats strip */}
            {detail && (
              <div className="shrink-0 border-b px-6 py-2.5 flex items-center gap-6 text-xs text-muted-foreground bg-muted/30">
                <span>type: <strong className="text-foreground">{detail.type}</strong></span>
                <span>query: <strong className="text-foreground">{detail.query}</strong></span>
                {detail.measure && <span>measure: <strong className="text-foreground">{detail.measure}</strong></span>}
              </div>
            )}

            {/* Collapsibles */}
            <div className="flex-1 overflow-y-auto px-6 py-4 space-y-3">
              <EditSection
                defaultOpen={false}
                mode={editMode}
                onModeChange={setEditMode}
                isDirty={dirty}
                validStatus={validStatus}
                builderContent={<p className="text-sm text-muted-foreground">Visual builder coming soon.</p>}
                yamlContent={
                  <YamlEditor
                    name={`${(selectedName ?? 'new_view')}.yaml`}
                    historyKey={{ type: 'view', name: selectedName ?? 'new_view' }}
                    content={editorContent}
                    savedContent={savedContent}
                    onChange={setEditorContent}
                    onSave={handleSave}
                    loading={detailLoading}
                    hideHeader
                    hideSaveButton
                  />
                }
                historyKey={{ type: 'view', name: selectedName ?? 'new_view' }}
                onRestoreVersion={(content) => setEditorContent(content)}
                validErrors={validErrors}
              />

              {/* Results — type-aware rendering */}
              {runResult && <ViewResultPanel result={runResult} />}
            </div>
          </>
        )}
      </div>

      {/* Publish dialog */}
      {publishOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-background border rounded-xl shadow-xl w-full max-w-md mx-4 p-5 space-y-4">
            <h2 className="text-lg font-semibold">Publish vizgram</h2>
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Title</label>
              <input value={publishTitle} onChange={e => setPublishTitle(e.target.value)}
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring" />
            </div>
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Caption {captionLoading && <span className="text-muted-foreground/50">(generating...)</span>}</label>
              <textarea value={publishCaption} onChange={e => setPublishCaption(e.target.value)} rows={4}
                className="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring resize-none" />
            </div>
            {publishError && <p className="text-xs text-red-500">{publishError}</p>}
            <div className="flex justify-end gap-2 pt-1">
              <button onClick={() => setPublishOpen(false)} className="px-3 py-1.5 text-sm border rounded hover:bg-muted transition-colors">Cancel</button>
              <button onClick={handlePublish} disabled={publishing || !publishTitle.trim()}
                className="px-3 py-1.5 text-sm bg-primary text-primary-foreground rounded hover:opacity-90 transition-opacity disabled:opacity-40">
                {publishing ? 'Publishing...' : 'Publish'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Result panel — renders map, chart, or table based on view type
// ---------------------------------------------------------------------------

function ViewResultPanel({ result }: { result: ViewResult }) {
  const viz = (result.visualization ?? {}) as Record<string, unknown>

  const header = (
    <div className="px-4 py-2 bg-muted/30 border-b flex items-center justify-between">
      <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
        Results ({result.type})
      </span>
      <span className="text-xs text-muted-foreground">
        {result.total_row_count.toLocaleString()} rows
        {result.truncated && ` (showing ${result.row_count.toLocaleString()})`}
        <span className="ml-2 opacity-50">{result.duration_ms}ms</span>
      </span>
    </div>
  )

  // Map view
  if (result.type === 'map' && viz.lat && viz.lon) {
    return (
      <div className="border rounded-lg overflow-hidden">
        {header}
        <MapChart
          rows={result.rows}
          columns={result.columns}
          latKey={viz.lat as string}
          lonKey={(viz.lon ?? viz.center_long) as string}
          labelKey={viz.label as string | undefined}
          tooltipKeys={viz.popup as string[] | undefined}
          sizeKey={viz.size as string | undefined}
          zoom={viz.zoom as number | undefined}
          centerLat={viz.center_lat as number | undefined}
          centerLon={(viz.center_lon ?? viz.center_long) as number | undefined}
          height={480}
        />
      </div>
    )
  }

  // Chart view (line/bar)
  if (result.type === 'chart' && viz.chart_type && viz.chart_type !== 'calendar_heatmap') {
    return (
      <div className="border rounded-lg overflow-hidden">
        {header}
        <div className="p-4">
          <LineBarChart
            rows={result.rows}
            columns={result.columns}
            xKey={viz.x as string}
            yKeys={Array.isArray(viz.y) ? viz.y as string[] : [viz.y as string]}
            chartType={viz.chart_type as 'bar' | 'line'}
            formats={result.formats}
            height={360}
          />
        </div>
      </div>
    )
  }

  // Table view (default)
  return (
    <div className="border rounded-lg overflow-hidden">
      {header}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-muted/20">
              {result.columns.map(c => (
                <th key={c} className="text-left px-3 py-2 font-medium text-xs text-muted-foreground whitespace-nowrap">{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, i) => (
              <tr key={i} className="border-b last:border-0 hover:bg-muted/20 transition-colors">
                {row.map((val, j) => (
                  <td key={j} className="px-3 py-2 text-sm tabular-nums whitespace-nowrap">
                    {val == null ? <span className="italic opacity-40">null</span> : String(val)}
                  </td>
                ))}
              </tr>
            ))}
            {result.rows.length === 0 && (
              <tr><td colSpan={result.columns.length} className="px-3 py-6 text-center text-sm text-muted-foreground">No results</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
