// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * AppFrame — props-driven app canvas used inside ExploreShell.
 *
 * Renders an application's views in their layout. Every table row that carries
 * a drilldown config (app_drilldown or row_drilldown) calls onNavigate so the
 * parent stack gets the next frame — keeping the breadcrumb unified across
 * apps, views, and entity details.
 */
import { useCallback, useEffect, useState } from 'react'
import { Activity, AlertCircle, ChevronRight, Hash, Loader2, Map, Play, Save, SlidersHorizontal, Table } from 'lucide-react'
import type { ApplicationDetail, ViewResult, ParamDef } from '@/api/client'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { EditMode } from '@/pages/explore/EditShell'
import { YamlEditor } from '@/components/YamlEditor'
import { useModel } from '@/context/ModelContext'
import { cn, formatValue as _formatValue } from '@/lib/utils'
import { Card, Spinner, ErrorMessage } from '@/components/Layout'
import { LineBarChart } from '@/components/charts/LineBarChart'
import { CalendarHeatmapChart } from '@/components/charts/CalendarHeatmapChart'
import { MapChart } from '@/components/charts/MapChart'
import type { DrillFrame } from '@/hooks/useDrillStack'

// ---------------------------------------------------------------------------
// Drilldown resolution — unified for app / view / entity targets
// ---------------------------------------------------------------------------

interface DrilldownConfig {
  label?: string
  app?: string
  view?: string
  entity?: string
  id_column?: string
  params?: Record<string, string>
}

function resolveDrilldown(
  config: DrilldownConfig,
  row: (string | number | null)[],
  columns: string[],
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolveParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const out: Record<string, string> = { ...sourceParams }
    for (const [k, v] of Object.entries(tpl)) {
      if (typeof v === 'string' && v.startsWith('row.')) {
        const idx = columns.indexOf(v.slice(4))
        out[k] = idx >= 0 && row[idx] != null ? String(row[idx]) : ''
      } else {
        out[k] = String(v)
      }
    }
    return out
  }

  if (config.app) return { kind: 'app', name: config.app, params: resolveParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolveParams(config.params) }
  if (config.entity && config.id_column) {
    const idx = columns.indexOf(config.id_column)
    const id = idx >= 0 && row[idx] != null ? String(row[idx]) : null
    if (id) return { kind: 'entity-detail', entity: config.entity, id }
  }
  return null
}

function resolveMarkerAction(
  config: DrilldownConfig,
  rowDict: Record<string, unknown>,
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolveParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const out: Record<string, string> = { ...sourceParams }
    for (const [k, v] of Object.entries(tpl)) {
      if (typeof v === 'string' && v.startsWith('row.')) {
        const val = rowDict[v.slice(4)]
        out[k] = val != null ? String(val) : ''
      } else {
        out[k] = String(v)
      }
    }
    return out
  }

  if (config.app) return { kind: 'app', name: config.app, params: resolveParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolveParams(config.params) }
  if (config.entity && config.id_column) {
    const val = rowDict[config.id_column]
    const id = val != null ? String(val) : null
    if (id) return { kind: 'entity-detail', entity: config.entity, id }
  }
  return null
}

function resolvePointDrilldown(
  config: DrilldownConfig,
  pointData: Record<string, unknown>,
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolveParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const out: Record<string, string> = { ...sourceParams }
    for (const [k, v] of Object.entries(tpl)) {
      if (typeof v === 'string' && v.startsWith('point.')) {
        const val = pointData[v.slice(6)]
        out[k] = val != null ? String(val) : ''
      } else {
        out[k] = String(v)
      }
    }
    return out
  }

  if (config.app) return { kind: 'app', name: config.app, params: resolveParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolveParams(config.params) }
  if (config.entity && config.id_column) {
    const val = pointData[config.id_column]
    const id = val != null ? String(val) : null
    if (id) return { kind: 'entity-detail', entity: config.entity, id }
  }
  return null
}

// ---------------------------------------------------------------------------
// Duration / number formatter (shared pattern across the app)
// ---------------------------------------------------------------------------

function formatValue(value: string | number | null, fmt?: { type: string; unit?: string | null }): string {
  if (value == null) return '—'
  return _formatValue(value, fmt as Parameters<typeof _formatValue>[1])
}

// ---------------------------------------------------------------------------
// Individual view card content
// ---------------------------------------------------------------------------

const TYPE_ICONS: Record<string, React.ReactNode> = {
  chart: <Activity className="h-3 w-3" />,
  table: <Table className="h-3 w-3" />,
  metric: <Hash className="h-3 w-3" />,
  map: <Map className="h-3 w-3" />,
}
const TYPE_COLOURS: Record<string, string> = {
  chart: 'bg-violet-50 text-violet-700 border-violet-200',
  table: 'bg-blue-50 text-blue-700 border-blue-200',
  metric: 'bg-emerald-50 text-emerald-700 border-emerald-200',
  map: 'bg-teal-50 text-teal-700 border-teal-200',
}

function ViewCardContent({
  result,
  paramValues,
  onNavigate,
}: {
  result: ViewResult
  paramValues: Record<string, string>
  onNavigate: (frame: DrillFrame) => void
}) {
  const viz = result.visualization as Record<string, unknown>
  // Prefer app_drilldown when in app context; fall back to row_drilldown
  const drillConfig = (viz.app_drilldown ?? viz.row_drilldown) as DrilldownConfig | undefined
  const pointDrillConfig = viz.point_drilldown as DrilldownConfig | undefined

  const handleClickPoint = pointDrillConfig
    ? (pointData: Record<string, unknown>) => {
        const frame = resolvePointDrilldown(pointDrillConfig, pointData, paramValues)
        if (frame) onNavigate(frame)
      }
    : undefined

  if (result.type === 'metric') {
    const measureCol = result.measure
    const colIdx = measureCol ? result.columns.indexOf(measureCol) : -1
    const value = colIdx >= 0 && result.rows.length > 0 ? result.rows[0][colIdx] : null
    const suffix = viz.suffix as string | undefined
    return (
      <div className="flex flex-col">
        <span className="text-3xl font-semibold tabular-nums">
          {value !== null ? String(value) : '—'}
        </span>
        {suffix && <span className="text-sm text-muted-foreground mt-1">{suffix}</span>}
        {measureCol && <span className="text-xs text-muted-foreground/60 mt-0.5">{measureCol}</span>}
      </div>
    )
  }

  if (result.type === 'table') {
    const cols = (viz.columns as string[] | undefined) ?? result.columns
    const colIndices = cols.map((c) => result.columns.indexOf(c)).filter((i) => i >= 0)
    const displayCols = colIndices.map((i) => result.columns[i])
    const previewRows = result.rows.slice(0, 10)
    const isDrillable = !!drillConfig

    return (
      <div className="overflow-x-auto -mx-4 px-4">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b">
              {displayCols.map((col) => (
                <th key={col} className="pb-1.5 text-left font-medium text-muted-foreground pr-4 whitespace-nowrap">{col}</th>
              ))}
              {isDrillable && <th className="w-4" />}
            </tr>
          </thead>
          <tbody>
            {previewRows.map((row, i) => (
              <tr
                key={i}
                onClick={isDrillable ? () => {
                  const frame = resolveDrilldown(drillConfig!, row, result.columns, paramValues)
                  if (frame) onNavigate(frame)
                } : undefined}
                className={cn(
                  'border-b last:border-0 transition-colors',
                  isDrillable ? 'cursor-pointer hover:bg-primary/5 group' : '',
                )}
              >
                {colIndices.map((ci, j) => {
                  const col = result.columns[ci]
                  const fmt = result.formats?.[col]
                  const val = row[ci]
                  return (
                    <td key={j} className="py-1.5 pr-4 tabular-nums text-muted-foreground whitespace-nowrap">
                      {val != null ? formatValue(val, fmt) : <span className="opacity-30">—</span>}
                    </td>
                  )
                })}
                {isDrillable && (
                  <td className="py-1.5">
                    <ChevronRight className="h-3 w-3 text-muted-foreground/30 group-hover:text-muted-foreground transition-colors" />
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
        {result.total_row_count > previewRows.length && (
          <p className="text-xs text-muted-foreground mt-2">Showing {previewRows.length} of {result.total_row_count.toLocaleString()} rows</p>
        )}
      </div>
    )
  }

  // Charts
  const chartType = viz.chart_type as string | undefined
  if (chartType === 'calendar_heatmap') {
    return (
      <CalendarHeatmapChart
        rows={result.rows} columns={result.columns}
        dateKey={viz.date as string} valueKey={viz.value as string}
        groupByKey={viz.group_by as string | undefined}
        colorScheme={viz.color_scheme as string | undefined}
        label={viz.label as string | undefined}
        weeks={viz.weeks as number | undefined} height={160}
        onClickPoint={handleClickPoint}
      />
    )
  }
  if (chartType === 'line' || chartType === 'bar') {
    return (
      <LineBarChart
        chartType={chartType} xKey={viz.x as string}
        yKeys={(viz.y as string[]) ?? []}
        rows={result.rows} columns={result.columns} height={220}
        formats={result.formats ?? undefined}
        onClickPoint={handleClickPoint}
      />
    )
  }

  if (result.type === 'map') {
    const markerActionConfigs = (viz.marker_actions as DrilldownConfig[] | undefined) ?? []
    return (
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
        height={300}
      />
    )
  }

  return (
    <div className="flex items-center gap-2 text-sm text-muted-foreground">
      <Activity className="h-4 w-4 shrink-0" />
      <span>{chartType ?? 'chart'} · {result.row_count.toLocaleString()} rows · {result.duration_ms}ms</span>
    </div>
  )
}

function ViewCard({
  name,
  result,
  paramValues,
  onNavigate,
}: {
  name: string
  result: ViewResult | Error | undefined
  paramValues: Record<string, string>
  onNavigate: (frame: DrillFrame) => void
}) {
  const isLoading = result === undefined
  const isError = result instanceof Error

  return (
    <Card className="p-0 overflow-hidden flex flex-col">
      <div className="flex items-center gap-2 px-4 py-2.5 border-b bg-muted/30">
        <button
          onClick={() => onNavigate({ kind: 'view', name, params: paramValues })}
          className="text-sm font-medium font-mono flex-1 truncate text-left hover:text-primary transition-colors"
          title={`Open view: ${name}`}
        >
          {name}
        </button>
        {!isLoading && !isError && (
          <span className={cn(
            'inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[10px] font-medium shrink-0',
            TYPE_COLOURS[(result as ViewResult).type] ?? 'bg-muted text-muted-foreground border-border',
          )}>
            {TYPE_ICONS[(result as ViewResult).type]}
            {(result as ViewResult).type}
          </span>
        )}
      </div>
      <div className="flex-1 p-4 min-h-[100px]">
        {isLoading && (
          <div className="flex items-center gap-2 text-muted-foreground text-sm">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading…
          </div>
        )}
        {isError && (
          <div className="flex items-start gap-2 text-red-600 text-sm">
            <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" /> {result.message}
          </div>
        )}
        {!isLoading && !isError && (
          <ViewCardContent result={result as ViewResult} paramValues={paramValues} onNavigate={onNavigate} />
        )}
      </div>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Main frame
// ---------------------------------------------------------------------------

export function AppFrame({
  name,
  initialParams,
  onNavigate,
}: {
  name: string
  initialParams: Record<string, string>
  onNavigate: (frame: DrillFrame) => void
}) {
  const { api } = useModel()
  const [detail, setDetail] = useState<ApplicationDetail | null>(null)
  const [viewResults, setViewResults] = useState<Record<string, ViewResult | Error>>({})
  const [paramValues, setParamValues] = useState<Record<string, string>>(initialParams)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [yamlContent, setYamlContent] = useState('')
  const [savedYaml, setSavedYaml] = useState('')
  const [editMode, setEditMode] = useState<EditMode>('yaml')
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])

  const executeViews = useCallback(async (appDetail: ApplicationDetail, params: Record<string, string>) => {
    setViewResults({})
    await Promise.all(appDetail.views.map(async (viewName) => {
      try {
        let r = await api.executeView(viewName, 1000, 0, params)
        if (r.type === 'map' && r.truncated) {
          r = await api.executeView(viewName, 10000, 0, params)
        }
        setViewResults((prev) => ({ ...prev, [viewName]: r }))
      } catch (e) {
        setViewResults((prev) => ({ ...prev, [viewName]: e instanceof Error ? e : new Error(String(e)) }))
      }
    }))
  }, [api])

  const handleSaveYaml = useCallback(async () => {
    if (saving) return
    setSaving(true); setValidErrors([])
    try {
      const updated = await api.saveApplication(name, yamlContent)
      const yaml = updated.raw_yaml ?? yamlContent
      setSavedYaml(yaml)
      setDetail(updated)
      executeViews(updated, paramValues)
      setValidStatus('pending')
      api.validateApplication(name)
        .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
        .catch(() => setValidStatus('idle'))
    } catch (e) {
      setValidStatus('invalid')
      setValidErrors([{ path: '', message: String(e) }])
    } finally {
      setSaving(false)
    }
  }, [api, name, yamlContent, paramValues, saving, executeViews])

  useEffect(() => {
    setLoading(true); setError(null)
    api.getApplication(name)
      .then((appDetail) => {
        setDetail(appDetail)
        const yaml = appDetail.raw_yaml ?? ''
        setYamlContent(yaml); setSavedYaml(yaml)
        const defaults: Record<string, string> = {}
        for (const p of appDetail.params ?? []) {
          if (p.default != null) defaults[p.name] = p.default
        }
        const params = { ...defaults, ...initialParams }
        setParamValues(params)
        executeViews(appDetail, params)
        setValidStatus('pending')
        api.validateApplication(name)
          .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
          .catch(() => setValidStatus('idle'))
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [name])

  if (loading) return <Spinner />
  if (error) return <ErrorMessage message={error} />
  if (!detail) return null

  const rows: string[][] = detail.layout.length > 0
    ? detail.layout.map((r) => r.row)
    : detail.views.map((v) => [v])

  const dirty = yamlContent !== savedYaml

  return (
    <div className="space-y-4">
      {/* Toolbar */}
      <div className="flex items-center gap-2">
        <h1 className="text-lg font-semibold flex-1">{detail.name}</h1>
        {dirty && !saving && <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />}
        <button disabled={!dirty || saving} onClick={handleSaveYaml}
          className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
          {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
          {saving ? 'Saving…' : 'Save'}
        </button>
        <button disabled={loading} onClick={() => executeViews(detail, paramValues)}
          className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
          {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
          {loading ? 'Running…' : 'Run'}
          <span className="text-[10px] opacity-60 ml-0.5 hidden sm:inline">⌘↵</span>
        </button>
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
            historyKey={{ type: 'application', name }}
            content={yamlContent}
            savedContent={savedYaml}
            onChange={setYamlContent}
            onSave={handleSaveYaml}
            hideHeader
            hideSaveButton
          />
        }
        historyKey={{ type: 'application', name }}
        onRestoreVersion={(content) => setYamlContent(content)}
        validErrors={validErrors}
      />

      {/* Param bar */}
      {(detail.params ?? []).length > 0 && (
        <div className="flex items-end gap-3 flex-wrap rounded-lg border bg-muted/30 px-4 py-3">
          <SlidersHorizontal className="h-4 w-4 text-muted-foreground shrink-0 mt-1" />
          {(detail.params as ParamDef[]).map((p) => (
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
                onKeyDown={(e) => e.key === 'Enter' && executeViews(detail, paramValues)}
                className="h-7 rounded border bg-background px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring w-full"
              />
            </div>
          ))}
        </div>
      )}

      {/* Layout grid */}
      {rows.map((row, i) => (
        <div key={i} className={cn('grid gap-4', row.length === 1 ? 'grid-cols-1' : `grid-cols-${Math.min(row.length, 3)}`)}>
          {row.map((viewName) => (
            <ViewCard
              key={viewName}
              name={viewName}
              result={viewResults[viewName]}
              paramValues={paramValues}
              onNavigate={onNavigate}
            />
          ))}
        </div>
      ))}
    </div>
  )
}
