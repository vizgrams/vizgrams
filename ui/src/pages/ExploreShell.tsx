// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ExploreShell — unified exploration surface combining Views and Entity browsing.
 *
 * Left panel: scrollable list of Views + Entities (both sections).
 * Right panel: frame renderer driven by useDrillStack.
 * Breadcrumb: derived from the stack, consistent across view and entity frames.
 *
 * Drilldown: clicking within any frame pushes onto the shared stack.
 * Sidebar clicks reset the stack to a fresh frame.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  Activity, BarChart2, ChevronDown, ChevronRight, ChevronUp, ChevronsUpDown, Hash,
  Layers, LayoutGrid, Loader2, Play, Save, SlidersHorizontal, Table, Upload,
} from 'lucide-react'
import type { ViewSummary, ViewResult, ParamDef, EntitySummary, ApplicationSummary } from '@/api/client'
import { publishVizgram, previewCaption } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { useRole } from '@/context/RoleContext'
import { cn, formatValue as _formatValue } from '@/lib/utils'
import { Card, ErrorMessage, Spinner } from '@/components/Layout'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { CalendarHeatmapChart } from '@/components/charts/CalendarHeatmapChart'
import { MapChart } from '@/components/charts/MapChart'
import { useDrillStack, frameLabel } from '@/hooks/useDrillStack'
import type { DrillFrame } from '@/hooks/useDrillStack'
import { EntityDetailFrame } from '@/pages/explore/EntityDetailFrame'
import { EntityListFrame } from '@/pages/explore/EntityListFrame'
import { AppFrame } from '@/pages/explore/AppFrame'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { EditMode } from '@/pages/explore/EditShell'
import { YamlEditor } from '@/components/YamlEditor'

// ---------------------------------------------------------------------------
// Type icons / colours (shared with old ViewsPage)
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

// ---------------------------------------------------------------------------
// Drilldown resolution (view rows → next frame)
// ---------------------------------------------------------------------------

interface ViewDrilldownConfig {
  label?: string
  app?: string
  view?: string
  entity?: string
  id_column?: string
  params?: Record<string, string>
}

function resolveRowParams(
  tpl: Record<string, string>,
  row: (string | number | null)[],
  columns: string[],
  sourceParams: Record<string, string>,
): Record<string, string> {
  const resolved: Record<string, string> = { ...sourceParams }
  for (const [key, template] of Object.entries(tpl)) {
    if (typeof template === 'string' && template.startsWith('row.')) {
      const colName = template.slice(4)
      const idx = columns.indexOf(colName)
      resolved[key] = idx >= 0 && row[idx] != null ? String(row[idx]) : ''
    } else {
      resolved[key] = String(template)
    }
  }
  return resolved
}

function resolveViewDrilldown(
  config: ViewDrilldownConfig,
  row: (string | number | null)[],
  columns: string[],
  sourceParams: Record<string, string>,
): DrillFrame | null {
  if (config.entity && config.id_column) {
    const idx = columns.indexOf(config.id_column)
    const id = idx >= 0 && row[idx] != null ? String(row[idx]) : null
    if (!id) return null
    return { kind: 'entity-detail', entity: config.entity, id }
  }
  if (config.app) {
    const params = resolveRowParams(config.params ?? {}, row, columns, sourceParams)
    return { kind: 'app', name: config.app, params }
  }
  if (config.view) {
    const params = resolveRowParams(config.params ?? {}, row, columns, sourceParams)
    return { kind: 'view', name: config.view, params }
  }
  return null
}

function resolveMarkerAction(
  config: ViewDrilldownConfig,
  rowDict: Record<string, unknown>,
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolveParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const resolved: Record<string, string> = { ...sourceParams }
    for (const [key, template] of Object.entries(tpl)) {
      if (typeof template === 'string' && template.startsWith('row.')) {
        const col = template.slice(4)
        const val = rowDict[col]
        resolved[key] = val != null ? String(val) : ''
      } else {
        resolved[key] = String(template)
      }
    }
    return resolved
  }

  if (config.entity && config.id_column) {
    const val = rowDict[config.id_column]
    const id = val != null ? String(val) : null
    if (!id) return null
    return { kind: 'entity-detail', entity: config.entity, id }
  }
  if (config.app) return { kind: 'app', name: config.app, params: resolveParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolveParams(config.params) }
  return null
}

function resolvePointDrilldown(
  config: ViewDrilldownConfig,
  pointData: Record<string, unknown>,
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolvePointParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const resolved: Record<string, string> = { ...sourceParams }
    for (const [key, template] of Object.entries(tpl)) {
      if (typeof template === 'string' && template.startsWith('point.')) {
        const col = template.slice(6)
        const val = pointData[col]
        resolved[key] = val != null ? String(val) : ''
      } else {
        resolved[key] = String(template)
      }
    }
    return resolved
  }

  if (config.entity && config.id_column) {
    const val = pointData[config.id_column]
    const id = val != null ? String(val) : null
    if (!id) return null
    return { kind: 'entity-detail', entity: config.entity, id }
  }
  if (config.app) return { kind: 'app', name: config.app, params: resolvePointParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolvePointParams(config.params) }
  return null
}

// ---------------------------------------------------------------------------
// Duration formatter
// ---------------------------------------------------------------------------

function formatValue(value: string | number | null, fmt?: { type: string; unit?: string | null }): string {
  if (value == null) return '—'
  return _formatValue(value, fmt as Parameters<typeof _formatValue>[1])
}

// ---------------------------------------------------------------------------
// View result frame — renders a single view execution + param bar
// ---------------------------------------------------------------------------

function ViewResultFrame({
  name,
  initialParams,
  onNavigate,
}: {
  name: string
  initialParams: Record<string, string>
  onNavigate: (frame: DrillFrame) => void
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
  }, [api, name])

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
    setCaptionUnavailable(false)
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
        <button disabled={loading} onClick={() => runView(paramValues)}
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

      {/* Edit section — Builder + YAML unified, collapsed by default */}
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

      {/* Param bar */}
      {(result?.params ?? []).length > 0 && (
        <div className="flex items-end gap-3 flex-wrap rounded-lg border bg-muted/30 px-4 py-3">
          <SlidersHorizontal className="h-4 w-4 text-muted-foreground shrink-0 mt-1" />
          {(result!.params as ParamDef[]).map((p) => (
            <div key={p.name} className="flex flex-col gap-1 min-w-[140px]">
              <label className="text-xs text-muted-foreground font-medium">
                {p.label ?? p.name}
                {p.optional && <span className="ml-1 text-muted-foreground/60">(optional)</span>}
              </label>
              <input
                type="text"
                value={paramValues[p.name] ?? ''}
                placeholder={p.optional ? 'all' : (p.default ?? '')}
                onChange={(e) => setParamValues((prev) => ({ ...prev, [p.name]: e.target.value }))}
                onKeyDown={(e) => e.key === 'Enter' && runView(paramValues)}
                className="h-7 rounded border bg-background px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring w-full"
              />
            </div>
          ))}
        </div>
      )}

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

function ViewContent({
  result,
  rowDrilldown,
  paramValues,
  onNavigate,
}: {
  result: ViewResult
  rowDrilldown?: ViewDrilldownConfig
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

// ---------------------------------------------------------------------------
// Main shell
// ---------------------------------------------------------------------------

export function ExploreShell() {
  const { model, api } = useModel()
  const { stack, current, push, navigateTo, reset } = useDrillStack(model)
  const [searchParams, setSearchParams] = useSearchParams()

  const [views, setViews] = useState<ViewSummary[]>([])
  const [entities, setEntities] = useState<EntitySummary[]>([])
  const [apps, setApps] = useState<ApplicationSummary[]>([])

  // Track the previous model value so we can distinguish a real model switch
  // from the initial mount. We must NOT clear URL params on initial mount — the
  // user may have navigated directly to /explore?app=name from another page and
  // we need those params to survive so the search-params effect below can open
  // the correct frame. Clearing only makes sense when the model actually changes
  // from one value to another (stale params from the old model must be dropped).
  // Using prevModelRef (rather than an "isFirstRun" bool) is StrictMode-safe:
  // the simulated remount sees the same model value and therefore isModelChange
  // stays false on the second invocation.
  const prevModelRef = useRef<string | null>(null)

  // Load sidebar data on model change; also clear URL params that belong to the
  // previous model so the searchParams effect below doesn't re-apply a stale app.
  useEffect(() => {
    const isModelChange = prevModelRef.current !== null && prevModelRef.current !== model
    prevModelRef.current = model
    setViews([]); setEntities([]); setApps([])
    api.listViews().then(setViews).catch(() => {})
    api.listEntities().then(setEntities).catch(() => {})
    api.listApplications().then(setApps).catch(() => {})
    if (isModelChange && (searchParams.has('app') || searchParams.has('section'))) {
      setSearchParams({}, { replace: true })
    }
  }, [model])

  // React to primary sidebar navigation params (?app=name, ?section=entities).
  // Guard the app case: only restore the frame if the app actually exists in the
  // current model — prevents a stale ?app= param from targeting the wrong model.
  // Also skip the reset when the current frame is already this app — prevents a
  // feedback loop where the sync effect (below) updates ?app= and this effect
  // then resets the frame with empty params, losing drilldown context (e.g. team_name).
  useEffect(() => {
    const app = searchParams.get('app')
    const section = searchParams.get('section')
    if (app) {
      if (apps.some((a) => a.name === app)) {
        if (current?.kind !== 'app' || current.name !== app) {
          reset({ kind: 'app', name: app, params: {} })
        }
      }
    } else if (section === 'entities' && entities.length > 0) {
      reset({ kind: 'entity-list', entity: entities[0].name })
    }
  }, [searchParams.toString(), entities.length, apps.length])

  // Sync ?app= search param with current app frame so the Layout NavItem
  // stays highlighted correctly when the frame is initialized from the URL hash.
  useEffect(() => {
    if (current?.kind === 'app' && searchParams.get('app') !== current.name) {
      setSearchParams({ app: current.name }, { replace: true })
    }
  }, [current?.kind === 'app' ? current.name : null])

  function selectView(name: string) {
    reset({ kind: 'view', name, params: {} })
  }

  function selectEntity(entity: string) {
    reset({ kind: 'entity-list', entity })
  }

  function selectApp(name: string) {
    reset({ kind: 'app', name, params: {} })
  }

  const handleNavigate = useCallback((frame: DrillFrame) => {
    push(frame)
  }, [push])

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">

      {/* ── Left panel — context-sensitive secondary nav ── */}
      {searchParams.get('app') == null && current?.kind !== 'app' && (
        <aside className="w-52 shrink-0 border-r flex flex-col overflow-hidden bg-card">
          <div className="flex-1 overflow-y-auto py-2">

            {/* Entity Explorer mode */}
            {searchParams.get('section') === 'entities' && (
              entities.length === 0
                ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">Loading…</p>
                : entities.map((e) => {
                    const active = stack.length > 0 && stack[0].kind === 'entity-list' && (stack[0] as { entity: string }).entity === e.name
                    return (
                      <button
                        key={e.name}
                        onClick={() => selectEntity(e.name)}
                        title={e.name}
                        className={cn(
                          'w-full text-left px-4 py-2 transition-colors',
                          active ? 'bg-primary/8 text-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground',
                        )}
                      >
                        <div className="text-xs line-clamp-1 break-all">{e.name}</div>
                        {e.row_count != null && (
                          <div className="text-[10px] text-muted-foreground/60 mt-0.5 tabular-nums">
                            {e.row_count.toLocaleString()} rows
                          </div>
                        )}
                      </button>
                    )
                  })
            )}

            {/* Views mode (default — no section or app param) */}
            {searchParams.get('section') == null && (
              views.length === 0
                ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">Loading…</p>
                : views.map((v) => {
                    const active = stack.length > 0 && stack[0].kind === 'view' && stack[0].name === v.name
                    return (
                      <button
                        key={v.name}
                        onClick={() => selectView(v.name)}
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
            )}

          </div>
        </aside>
      )}

      {/* ── Right panel ── */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">

        {/* Breadcrumb */}
        {stack.length > 0 && (
          <nav className="shrink-0 px-6 py-2.5 border-b flex items-center gap-1 flex-wrap text-xs">
            {stack.map((frame, i) => (
              <span key={i} className="flex items-center gap-1">
                {i > 0 && <ChevronRight className="h-3 w-3 text-muted-foreground/40 shrink-0" />}
                <button
                  onClick={() => navigateTo(i)}
                  className={cn(
                    'font-mono transition-colors',
                    i === stack.length - 1
                      ? 'text-foreground font-medium cursor-default'
                      : 'text-muted-foreground hover:text-foreground',
                  )}
                >
                  {frameLabel(frame)}
                </button>
              </span>
            ))}
          </nav>
        )}

        {/* Frame content */}
        <div className="flex-1 overflow-y-auto px-6 py-6">
          {!current ? (
            <div className="flex flex-col items-center justify-center h-48 text-center gap-2">
              <p className="text-muted-foreground text-sm">Select an app, view, or entity to start exploring.</p>
            </div>
          ) : current.kind === 'app' ? (
            <AppFrame
              key={`${current.name}-${JSON.stringify(current.params)}`}
              name={current.name}
              initialParams={current.params}
              onNavigate={handleNavigate}
            />
          ) : current.kind === 'view' ? (
            <ViewResultFrame
              key={`${current.name}-${JSON.stringify(current.params)}`}
              name={current.name}
              initialParams={current.params}
              onNavigate={handleNavigate}
            />
          ) : current.kind === 'entity-list' ? (
            <EntityListFrame
              key={current.entity}
              entity={current.entity}
              onNavigate={handleNavigate}
            />
          ) : (
            <EntityDetailFrame
              key={`${current.entity}-${current.id}`}
              entity={current.entity}
              id={current.id}
              onNavigate={handleNavigate}
            />
          )}
        </div>
      </div>
    </div>
  )
}
