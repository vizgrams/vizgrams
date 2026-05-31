// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ExplorePage — entity-first catalog browser (Epic 26 VG-291).
 *
 * Read-only in this phase: editors (Schema, mapper sub-groups, extractor
 * drawer) are deferred to VG-293+. The pencil affordances render but are
 * disabled so the shape is visible.
 *
 * Routing: selected entity + active tab persist in the querystring
 * (`/explore?entity=PullRequest&tab=charts`) so links share / refresh
 * survive cleanly.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  Activity as ActivityIcon, ArrowUpRight, BarChart3, Database,
  Download, GitCommit, Hash, History, Layers, LineChart,
  Link2, Pencil, Plus, RotateCcw, Shuffle, Sparkles, Table2, Wrench, X,
} from 'lucide-react'

import type {
  ActivityEvent, ActivityFeed, ChartSummary, EntityDetail, EntitySummary,
  PipelineSummary, Proposal, ProposalKind,
} from '@/api/client'
import { ChartDetailDrawer } from '@/components/explore/ChartDetailDrawer'
import { ChartPreview } from '@/components/explore/ChartPreview'
import { NewChartDrawer } from '@/components/explore/NewChartDrawer'
import { RecordDetailDrawer } from '@/components/explore/RecordDetailDrawer'
import { SchemaAddPanel } from '@/components/explore/SchemaAddPanel'
import { GovernedYamlEditor } from '@/components/proposals/GovernedYamlEditor'
import { ProposalCard } from '@/components/proposals/ProposalCard'
import { ProposeChangeForm } from '@/components/proposals/ProposeChangeForm'
import { useModel } from '@/context/ModelContext'
import { useRole } from '@/context/RoleContext'
import { cn, formatValue } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Page shell
// ---------------------------------------------------------------------------

type Tab = 'overview' | 'records' | 'charts' | 'schema' | 'pipeline' | 'activity'

const TABS: { id: Tab; label: string; icon: React.ReactNode }[] = [
  { id: 'overview', label: 'Overview', icon: <ActivityIcon className="h-3.5 w-3.5" /> },
  { id: 'records',  label: 'Records',  icon: <Table2 className="h-3.5 w-3.5" /> },
  { id: 'charts',   label: 'Charts',   icon: <BarChart3 className="h-3.5 w-3.5" /> },
  { id: 'schema',   label: 'Schema',   icon: <Database className="h-3.5 w-3.5" /> },
  { id: 'pipeline', label: 'Pipeline', icon: <Shuffle className="h-3.5 w-3.5" /> },
  { id: 'activity', label: 'Activity', icon: <History className="h-3.5 w-3.5" /> },
]

function isTab(value: string | null): value is Tab {
  return TABS.some((t) => t.id === value)
}

export function ExplorePage() {
  const { api } = useModel()
  const [entities, setEntities] = useState<EntitySummary[]>([])
  const [loadingEntities, setLoadingEntities] = useState(true)

  const [searchParams, setSearchParams] = useSearchParams()
  const entityParam = searchParams.get('entity') ?? ''
  const tabParam = searchParams.get('tab')
  const tab: Tab = isTab(tabParam) ? tabParam : 'overview'

  useEffect(() => {
    let cancelled = false
    setLoadingEntities(true)
    api.listEntities()
      .then((result) => { if (!cancelled) setEntities(result) })
      .catch(() => { if (!cancelled) setEntities([]) })
      .finally(() => { if (!cancelled) setLoadingEntities(false) })
    return () => { cancelled = true }
  }, [api])

  const selected = useMemo<EntitySummary | null>(() => {
    if (entityParam) {
      return entities.find((e) => e.name === entityParam) ?? null
    }
    return entities[0] ?? null
  }, [entities, entityParam])

  // Sync URL whenever the entity changes — keep tab if same entity selected,
  // reset to overview when entity changes.
  const selectEntity = useCallback((name: string) => {
    setSearchParams({ entity: name, tab: 'overview' })
  }, [setSearchParams])

  const selectTab = useCallback((next: Tab) => {
    if (!selected) return
    setSearchParams({ entity: selected.name, tab: next })
  }, [selected, setSearchParams])

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      <EntitySidebar
        entities={entities}
        loading={loadingEntities}
        selected={selected}
        onSelect={selectEntity}
      />
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {selected ? (
          <div className="flex-1 overflow-y-auto">
            <EntityHeader entity={selected} />
            <Tabs current={tab} onChange={selectTab} />
            <div className="px-8 py-6">
              {tab === 'overview' && <OverviewTab entity={selected} onSeeAll={() => selectTab('records')} />}
              {tab === 'records'  && <RecordsTab  entity={selected} />}
              {tab === 'charts'   && <ChartsTab   entity={selected} />}
              {tab === 'schema'   && <SchemaTab   entity={selected} />}
              {tab === 'pipeline' && <PipelineTab entity={selected} />}
              {tab === 'activity' && <ActivityTab entity={selected} />}
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-center h-full">
            {loadingEntities
              ? <p className="text-xs text-muted-foreground">Loading…</p>
              : <p className="text-sm text-muted-foreground">No entities in this model yet.</p>
            }
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sidebar
// ---------------------------------------------------------------------------

function EntitySidebar({
  entities, loading, selected, onSelect,
}: {
  entities: EntitySummary[]
  loading: boolean
  selected: EntitySummary | null
  onSelect: (name: string) => void
}) {
  return (
    <aside className="w-52 shrink-0 border-r flex flex-col overflow-hidden bg-card">
      <div className="px-4 pt-4 pb-2 text-[10px] uppercase tracking-wider text-muted-foreground/70">
        Entities
      </div>
      <div className="flex-1 overflow-y-auto pb-2">
        {loading && entities.length === 0
          ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">Loading…</p>
          : entities.length === 0
          ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">No entities</p>
          : entities.map((e) => {
              const active = selected?.name === e.name
              return (
                <button
                  key={e.name}
                  onClick={() => onSelect(e.name)}
                  className={cn(
                    'w-full text-left px-4 py-2 flex items-center justify-between transition-colors',
                    active
                      ? 'bg-primary/8 text-foreground'
                      : 'text-muted-foreground hover:bg-muted hover:text-foreground',
                  )}
                >
                  <span className="text-xs">{e.name}</span>
                  {e.row_count != null && (
                    <span className="text-[10px] text-muted-foreground/60 tabular-nums">
                      {e.row_count.toLocaleString()}
                    </span>
                  )}
                </button>
              )
            })
        }
      </div>
    </aside>
  )
}

// ---------------------------------------------------------------------------
// Header + tabs
// ---------------------------------------------------------------------------

function EntityHeader({ entity }: { entity: EntitySummary }) {
  // VG-304 — "+ New chart" opens an in-shell drawer with a query picker
  // + chart-type template instead of jumping to the dead /views page.
  const [newOpen, setNewOpen] = useState(false)
  return (
    <div className="px-8 pt-6 pb-4 flex items-start justify-between gap-6 border-b">
      <div className="min-w-0">
        <h1 className="text-xl font-semibold tracking-tight">{entity.name}</h1>
        <div className="mt-2 flex gap-4 text-[11px] text-muted-foreground/70 tabular-nums">
          {entity.row_count != null && <span>{entity.row_count.toLocaleString()} records</span>}
          {entity.row_count != null && <span>·</span>}
          <span>{entity.attribute_count} attributes</span>
          <span>·</span>
          <span>{entity.relation_count} relations</span>
          <span>·</span>
          <span>{entity.feature_count} computed</span>
        </div>
      </div>
      <button
        onClick={() => setNewOpen(true)}
        title={`Author a new chart rooted on ${entity.name}`}
        className="shrink-0 inline-flex items-center gap-1.5 text-xs px-3 py-1.5 rounded border bg-card hover:bg-muted transition-colors"
      >
        <Plus className="h-3.5 w-3.5" /> New chart
      </button>
      {newOpen && (
        <NewChartDrawer entity={entity.name} onClose={() => setNewOpen(false)} />
      )}
    </div>
  )
}

function Tabs({ current, onChange }: { current: Tab; onChange: (t: Tab) => void }) {
  return (
    <div className="px-8 border-b bg-card/50 sticky top-0 z-10">
      <div className="flex gap-1">
        {TABS.map((t) => {
          const active = current === t.id
          return (
            <button
              key={t.id}
              onClick={() => onChange(t.id)}
              className={cn(
                'inline-flex items-center gap-1.5 px-3 py-2.5 text-xs transition-colors border-b-2 -mb-px',
                active ? 'border-foreground text-foreground'
                       : 'border-transparent text-muted-foreground hover:text-foreground',
              )}
            >
              {t.icon}
              {t.label}
            </button>
          )
        })}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Overview — KPIs (chart_type='kpi') + featured charts (first 2 non-kpi)
// ---------------------------------------------------------------------------

function OverviewTab({ entity, onSeeAll }: { entity: EntitySummary; onSeeAll: () => void }) {
  const { api } = useModel()
  const [charts, setCharts] = useState<ChartSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.listEntityCharts(entity.name)
      .then((result) => { if (!cancelled) setCharts(result) })
      .catch(() => { if (!cancelled) setCharts([]) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, entity.name])

  if (loading) return <Loading />
  const kpis = charts.filter((c) => c.chart_type === 'kpi').slice(0, 3)
  const featured = charts.filter((c) => c.chart_type !== 'kpi').slice(0, 2)

  return (
    <div className="space-y-6 max-w-5xl">
      {kpis.length > 0 && (
        <div className="grid grid-cols-3 gap-3">
          {kpis.map((k) => <KpiCard key={k.name} chart={k} />)}
        </div>
      )}
      {featured.length > 0 && (
        <div className="grid grid-cols-2 gap-3">
          {featured.map((c) => <ChartCardEl key={c.name} card={c} large />)}
        </div>
      )}
      {charts.length === 0 && (
        <EmptyState label={`No charts yet for ${entity.name}.`} />
      )}
      {charts.length > 0 && (
        <button
          onClick={onSeeAll}
          className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-1"
        >
          See all records →
        </button>
      )}
    </div>
  )
}

function KpiCard({ chart }: { chart: ChartSummary }) {
  const { api } = useModel()
  const [value, setValue] = useState<string | number | null>(null)
  const [suffix, setSuffix] = useState<string | null>(null)
  const [state, setState] = useState<'loading' | 'ok' | 'error'>('loading')

  // VG-301 — execute the view and pull the scalar. Matches ViewContent's
  // metric path: result.measure names the value column, value lives at
  // rows[0][colIdx].
  useEffect(() => {
    let cancelled = false
    setState('loading')
    api.executeView(chart.name, 1)
      .then((r) => {
        if (cancelled) return
        const colIdx = r.measure ? r.columns.indexOf(r.measure) : -1
        const v = colIdx >= 0 && r.rows.length > 0 ? r.rows[0][colIdx] : null
        const fmt = r.measure ? r.formats?.[r.measure] : undefined
        setValue(v != null ? formatValue(v, fmt) : null)
        const viz = r.visualization as { suffix?: string }
        setSuffix(viz?.suffix ?? null)
        setState('ok')
      })
      .catch(() => { if (!cancelled) setState('error') })
    return () => { cancelled = true }
  }, [api, chart.name])

  return (
    <div className="rounded border bg-card p-4">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70">{chart.name}</div>
      <div className="text-2xl font-semibold tabular-nums mt-1">
        {state === 'loading' ? <span className="text-muted-foreground/40">…</span>
          : state === 'error' || value == null ? <span className="text-muted-foreground/40">—</span>
          : <>{value}{suffix && <span className="ml-1 text-sm font-normal text-muted-foreground">{suffix}</span>}</>
        }
      </div>
      <div className="text-[10px] text-muted-foreground/60 mt-1 font-mono">{chart.query}</div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Records — embeds the existing EntityListFrame (rich record browser)
// ---------------------------------------------------------------------------

// VG-303 — DrillFrame contract from EntityListFrame. Mirrors
// components/view/drilldown.ts but reproduced here to avoid coupling
// the lazy import to that module's full type.
type DrillTarget =
  | { kind: 'entity-detail'; entity: string; id: string }
  | { kind: 'entity-list'; entity: string }
  | { kind: string; [k: string]: unknown }

function RecordsTab({ entity }: { entity: EntitySummary }) {
  // Lazy import keeps the bundle small + avoids circular deps when this
  // page is unmounted. Falls back to a placeholder if the frame's data
  // contract changes underneath us.
  const [Frame, setFrame] = useState<React.ComponentType<{ entity: string; onNavigate: (f: unknown) => void }> | null>(null)
  // VG-303: row click → record detail drawer.
  const [openRecord, setOpenRecord] = useState<{ entity: string; id: string } | null>(null)

  useEffect(() => {
    import('@/pages/explore/EntityListFrame')
      .then((m) => setFrame(() => m.EntityListFrame as never))
      .catch(() => setFrame(null))
  }, [])
  if (!Frame) return <Loading />
  return (
    <div className="-mx-2">
      <Frame
        entity={entity.name}
        onNavigate={(f: unknown) => {
          const frame = f as DrillTarget
          if (frame.kind === 'entity-detail') {
            setOpenRecord({ entity: frame.entity as string, id: frame.id as string })
          }
        }}
      />
      {openRecord && (
        <RecordDetailDrawer
          entity={openRecord.entity}
          id={openRecord.id}
          onClose={() => setOpenRecord(null)}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Charts — every chart rooted on this entity
// ---------------------------------------------------------------------------

function ChartsTab({ entity }: { entity: EntitySummary }) {
  const { api } = useModel()
  const [charts, setCharts] = useState<ChartSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.listEntityCharts(entity.name)
      .then((result) => { if (!cancelled) setCharts(result) })
      .catch(() => { if (!cancelled) setCharts([]) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, entity.name])

  if (loading) return <Loading />
  if (charts.length === 0) return <EmptyState label={`No charts yet for ${entity.name}.`} />
  return (
    <div className="grid grid-cols-3 gap-3 max-w-5xl">
      {charts.map((c) => <ChartCardEl key={c.name} card={c} />)}
    </div>
  )
}

const KIND_ICON: Record<string, React.ReactNode> = {
  bar:   <BarChart3 className="h-3.5 w-3.5" />,
  line:  <LineChart className="h-3.5 w-3.5" />,
  kpi:   <Hash className="h-3.5 w-3.5" />,
  table: <Table2 className="h-3.5 w-3.5" />,
}

function ChartCardEl({ card, large = false }: { card: ChartSummary; large?: boolean }) {
  // VG-302 — click opens an in-shell side drawer instead of navigating
  // away to /views/:name (which is a redirect to /explore now anyway).
  const [open, setOpen] = useState(false)
  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className={cn(
          'group rounded border bg-card hover:border-foreground/30 transition-colors block w-full text-left',
          large ? 'p-4' : 'p-3',
        )}
      >
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <div className={cn('font-medium leading-snug', large ? 'text-sm' : 'text-xs')}>{card.name}</div>
            <div className="text-[10px] text-muted-foreground/70 mt-0.5 font-mono truncate">{card.query}</div>
          </div>
          <span className="shrink-0 inline-flex items-center rounded border bg-muted/40 p-1 text-muted-foreground">
            {KIND_ICON[card.chart_type] ?? <BarChart3 className="h-3.5 w-3.5" />}
          </span>
        </div>
        <div className="mt-3">
          <ChartPreview viewName={card.name} height={large ? 128 : 80} />
        </div>
        <div className="mt-2 flex items-center justify-between text-[10px] text-muted-foreground/60">
          <span className="font-mono">{card.chart_type}</span>
          <ArrowUpRight className="h-3 w-3 opacity-0 group-hover:opacity-100 transition-opacity" />
        </div>
      </button>
      {open && <ChartDetailDrawer viewName={card.name} onClose={() => setOpen(false)} />}
    </>
  )
}

// ---------------------------------------------------------------------------
// Schema — read-only attributes / relations / computed features
// ---------------------------------------------------------------------------

function SchemaTab({ entity }: { entity: EntitySummary }) {
  const { api } = useModel()
  const [detail, setDetail] = useState<EntityDetail | null>(null)
  const [loading, setLoading] = useState(true)
  // VG-305: bumped after a successful Add Computed save so the schema
  // refetches and the new feature appears in the list.
  const [refreshTick, setRefreshTick] = useState(0)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.getEntity(entity.name)
      .then((result) => { if (!cancelled) setDetail(result) })
      .catch(() => { if (!cancelled) setDetail(null) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, entity.name, refreshTick])

  if (loading) return <Loading />
  if (!detail) return <EmptyState label="Could not load schema." />

  return (
    <div className="grid grid-cols-3 gap-6 max-w-5xl">
      <SchemaList icon={<Hash className="h-3.5 w-3.5" />} title="Attributes">
        {detail.attributes.length === 0
          ? <EmptyRow label="No attributes." />
          : detail.attributes.map((a) => (
              <ReadOnlyRow
                key={a.name}
                primary={a.name}
                secondary={a.type}
                governed
                governedKind="attribute"
                entityName={entity.name}
              />
            ))
        }
        {/* VG-306 — propose a new attribute (admins apply on approval) */}
        <SchemaAddPanel entity={entity.name} kind="attribute" />
      </SchemaList>
      <SchemaList icon={<Link2 className="h-3.5 w-3.5" />} title="Relations">
        {detail.relations.length === 0
          ? <EmptyRow label="No relations." />
          : detail.relations.map((r) => (
              <ReadOnlyRow
                key={r.name ?? r.target}
                primary={r.name ?? r.target}
                secondary={r.cardinality}
                tertiary={`→ ${r.target}`}
                governed
                governedKind="relation"
                entityName={entity.name}
              />
            ))
        }
        {/* VG-306 — propose a new relation */}
        <SchemaAddPanel entity={entity.name} kind="relation" />
      </SchemaList>
      <SchemaList icon={<Sparkles className="h-3.5 w-3.5" />} title="Computed">
        {detail.features.length === 0
          ? <EmptyRow label="No computed features." />
          : detail.features.map((f) => (
              <ReadOnlyRow key={f.feature_id} primary={f.name} secondary={f.expr} mono />
            ))
        }
        {/* VG-293 Describe-it + VG-305 save → refresh schema on success */}
        <ComputedAddPanel entity={entity.name} onSaved={() => setRefreshTick((t) => t + 1)} />
      </SchemaList>
    </div>
  )
}

// Compact open/close + form for Add Computed. Describe-it (VG-293)
// generates name + expr from a natural-language prompt; Save (VG-305)
// constructs the feature YAML and PUTs it via saveFeatureYaml.
function ComputedAddPanel({ entity, onSaved }: { entity: string; onSaved?: () => void }) {
  const { api } = useModel()
  const [open, setOpen] = useState(false)
  const [prompt, setPrompt] = useState('')
  const [name, setName] = useState('')
  const [expr, setExpr] = useState('')
  const [dataType, setDataType] = useState<'FLOAT' | 'INTEGER' | 'STRING'>('FLOAT')
  const [generating, setGenerating] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function generate() {
    if (!prompt.trim()) return
    setGenerating(true)
    setError(null)
    try {
      const r = await api.describeComputed(entity, prompt)
      setName(r.name)
      setExpr(r.expr)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Generation failed')
    } finally {
      setGenerating(false)
    }
  }

  function reset() {
    setOpen(false)
    setPrompt(''); setName(''); setExpr('')
    setDataType('FLOAT')
    setError(null)
  }

  async function save() {
    setError(null)
    if (!/^[a-z][a-z0-9_]*$/.test(name)) {
      setError('Name must be lowercase letters / digits / underscores, starting with a letter.')
      return
    }
    if (!expr.trim()) {
      setError('Expression is required.')
      return
    }
    setSaving(true)
    try {
      const entitySnake = entity.replace(/(?<=[a-z0-9])([A-Z])/g, '_$1').toLowerCase()
      const featureId = `${entitySnake}.${name}`
      const yaml = [
        `feature_id: ${featureId}`,
        `name: ${name}`,
        `entity_type: ${entity}`,
        `entity_key: ${entitySnake}_key`,
        `data_type: ${dataType}`,
        'materialization_mode: dynamic',
        `expr: ${JSON.stringify(expr)}`,
        '',
      ].join('\n')
      await api.saveFeatureYaml(featureId, yaml)
      onSaved?.()
      reset()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="mt-3 inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
      >
        <Plus className="h-3 w-3" /> Add computed
      </button>
    )
  }
  return (
    <div className="mt-3 rounded border bg-card p-3 space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">Add computed feature</div>
        <button onClick={reset} className="text-muted-foreground hover:text-foreground">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      <div className="rounded border bg-muted/30 p-2.5 space-y-2">
        <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-muted-foreground/70">
          <Sparkles className="h-3 w-3" />
          Describe it
        </div>
        <input
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="e.g. lead time in hours from created to merged"
          className="w-full text-xs bg-background border rounded px-2 py-1.5 placeholder:text-muted-foreground/50"
        />
        <button
          onClick={generate}
          disabled={generating || !prompt.trim()}
          className="text-xs px-2.5 py-1 rounded border bg-card hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
        >
          <Sparkles className="h-3 w-3" />
          {generating ? 'Generating…' : 'Generate'}
        </button>
      </div>

      <div className="space-y-2">
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="name (snake_case)"
          className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
        />
        <textarea
          value={expr}
          onChange={(e) => setExpr(e.target.value)}
          placeholder="expression"
          rows={2}
          className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono resize-y"
        />
        <div className="flex items-center gap-1.5">
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground/70">Type</label>
          <select
            value={dataType}
            onChange={(e) => setDataType(e.target.value as 'FLOAT' | 'INTEGER' | 'STRING')}
            className="text-xs bg-background border rounded px-2 py-1 font-mono"
          >
            <option value="FLOAT">FLOAT</option>
            <option value="INTEGER">INTEGER</option>
            <option value="STRING">STRING</option>
          </select>
        </div>
      </div>

      {error && <p className="text-[11px] text-red-600">{error}</p>}

      <div className="flex justify-end gap-2 pt-1">
        <button
          onClick={reset}
          className="text-xs text-muted-foreground hover:text-foreground px-2 py-1"
        >
          Cancel
        </button>
        <button
          onClick={save}
          disabled={saving}
          className="text-xs px-3 py-1 rounded border bg-foreground text-background hover:bg-foreground/90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {saving ? 'Saving…' : 'Save'}
        </button>
      </div>
    </div>
  )
}

function ReadOnlyRow({
  primary, secondary, tertiary, mono,
  governed, governedKind, entityName,
}: {
  primary: string
  secondary?: string
  tertiary?: string
  mono?: boolean
  // VG-296: governed rows open a propose-change form instead of a
  // direct edit. Computed rows are excluded — they're authored
  // directly via the ComputedAddPanel.
  governed?: boolean
  governedKind?: ProposalKind
  entityName?: string
}) {
  const [proposeOpen, setProposeOpen] = useState(false)
  return (
    <>
      <div className="group flex items-baseline justify-between gap-2 py-1.5 text-xs border-b last:border-b-0">
        <div className="min-w-0">
          <div className="font-mono">{primary}</div>
          {tertiary && <div className="text-[10px] text-muted-foreground/60 mt-0.5">{tertiary}</div>}
          {mono && secondary && (
            <div className="font-mono text-[10px] text-muted-foreground/70 mt-0.5 break-all">{secondary}</div>
          )}
        </div>
        <div className="flex items-center gap-1 shrink-0">
          {!mono && secondary && <span className="text-[10px] text-muted-foreground/70 mr-1">{secondary}</span>}
          {governed && governedKind ? (
            <button
              onClick={() => setProposeOpen((o) => !o)}
              title="Propose change"
              className="opacity-0 group-hover:opacity-100 transition-opacity text-muted-foreground hover:text-foreground p-1"
            >
              <Pencil className="h-3 w-3" />
            </button>
          ) : (
            <button
              disabled
              title="Editing lands in a follow-up"
              className="text-muted-foreground/30 p-1 cursor-not-allowed"
            >
              <Pencil className="h-3 w-3" />
            </button>
          )}
        </div>
      </div>
      {proposeOpen && governed && governedKind && (
        <ProposeChangeForm
          artifactKind={governedKind}
          artifactName={primary}
          entityName={entityName ?? null}
          current={`${secondary ?? ''}${tertiary ? ` ${tertiary}` : ''}`.trim()}
          onClose={() => setProposeOpen(false)}
        />
      )}
    </>
  )
}

function SchemaList({
  icon, title, children,
}: { icon: React.ReactNode; title: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-muted-foreground/70 pb-2 mb-2 border-b">
        {icon}
        {title}
      </div>
      <div>{children}</div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Pipeline — lineage chips + read-only mapper sub-groups + drawer
// ---------------------------------------------------------------------------

// Pipeline tab — single end-to-end lineage view.
//
// Sources fan into the mapper (left column), sub-groups fan out of the
// mapper into the entity (right column). Every chip is the entry point
// to edit the thing it represents:
//   - Tool: read-only (configured globally; not editable per-entity)
//   - Extractor: opens the governed YAML editor
//   - Raw: read-only (it's just data)
//   - Mapper: opens whole-mapper YAML editor (add/remove sub-groups etc.)
//   - Sub-group: opens sub-group-focused editor inside the mapper YAML
//   - Entity: not editable here — Schema tab is the editor for entities
function PipelineTab({ entity }: { entity: EntitySummary }) {
  const { api } = useModel()
  // Suppress unused-import warning while keeping useRole available for
  // any per-role gating we add to chips later.
  void useRole
  const [pipeline, setPipeline] = useState<PipelineSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [openExtractor, setOpenExtractor] = useState<string | null>(null)
  const [editingSubGroup, setEditingSubGroup] = useState<string | null>(null)
  const [editingMapper, setEditingMapper] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.getEntityPipeline(entity.name)
      .then((result) => { if (!cancelled) setPipeline(result) })
      .catch(() => { if (!cancelled) setPipeline(null) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, entity.name])

  if (loading) return <Loading />
  if (!pipeline || !pipeline.mapper) return <EmptyState label={`No pipeline configured for ${entity.name}.`} />

  const mapper = pipeline.mapper
  const sources = pipeline.sources
  const subgroups = mapper.groups
  const headerNote = [
    sources.length > 1 && `${sources.length} sources join`,
    subgroups.length > 1 && `${subgroups.length} sub-groups merge`,
  ].filter(Boolean).join(' · ')

  return (
    <div className="space-y-4 max-w-5xl">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70">
        Lineage
        {headerNote && (
          <span className="ml-2 text-muted-foreground/60 normal-case tracking-normal">
            · {headerNote}
          </span>
        )}
      </div>

      <div className="rounded border bg-card p-5 overflow-x-auto">
        <div className="flex items-stretch gap-4 min-w-max">
          {/* Sources column — one row per raw table feeding the mapper */}
          <div className="flex flex-col gap-2 justify-center">
            {sources.length === 0
              ? <EmptyRow label="No sources declared in the mapper." />
              : sources.map((src, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <LineageChip
                      icon={<Wrench className="h-3 w-3" />}
                      kind="Tool"
                      name={src.tool ?? '(not in catalog)'}
                      muted={!src.tool}
                    />
                    <LineageArrow />
                    <LineageChip
                      icon={<Download className="h-3 w-3" />}
                      kind="Extractor"
                      name={src.extractor ?? '(not in catalog)'}
                      muted={!src.extractor}
                      onClick={src.extractor ? () => setOpenExtractor(src.extractor) : undefined}
                    />
                    <LineageArrow />
                    <LineageChip icon={<Database className="h-3 w-3" />} kind="Raw" name={src.raw_table} mono />
                  </div>
                ))
            }
          </div>

          {/* Sources → Mapper converger */}
          {sources.length > 1
            ? <Converger count={sources.length} />
            : <LineageArrow />
          }

          {/* Mapper chip — clicking edits the whole mapper YAML */}
          <div className="flex items-center">
            <LineageChip
              icon={<Shuffle className="h-3 w-3" />}
              kind="Mapper"
              name={mapper.name}
              highlight
              onClick={() => setEditingMapper(true)}
            />
          </div>

          {/* Mapper → Sub-groups diverger (only when multiple sub-groups) */}
          {subgroups.length > 1
            ? <Diverger count={subgroups.length} />
            : <LineageArrow />
          }

          {/* Sub-groups column — one chip per sub-group, each clickable */}
          <div className="flex flex-col gap-2 justify-center">
            {subgroups.length === 0 ? (
              // Single-target mapper — there's no sub-group, the mapper
              // writes directly. Render a placeholder so the lane closes.
              <LineageChip
                icon={<Shuffle className="h-3 w-3" />}
                kind="Sub-group"
                name="(direct)"
                muted
              />
            ) : subgroups.map((g) => (
              <LineageChip
                key={g.name}
                icon={<Shuffle className="h-3 w-3" />}
                kind="Sub-group"
                name={g.name}
                mono
                onClick={() => setEditingSubGroup(g.name)}
              />
            ))}
          </div>

          {/* Sub-groups → Entity converger (mirror of the source one) */}
          {subgroups.length > 1
            ? <Converger count={subgroups.length} />
            : <LineageArrow />
          }

          {/* Entity */}
          <div className="flex items-center">
            <LineageChip
              icon={<Layers className="h-3 w-3" />}
              kind="Entity"
              name={entity.name}
              active
            />
          </div>
        </div>
      </div>

      <p className="text-[11px] text-muted-foreground/60">
        Click any chip to edit. The mapper chip opens the whole-mapper YAML
        (use this to add or remove sub-groups, or change sources).
      </p>

      {/* Drawers for in-place edits */}
      {openExtractor && (
        <ExtractorDrawer
          name={openExtractor}
          entityName={entity.name}
          onClose={() => setOpenExtractor(null)}
        />
      )}
      {editingMapper && (
        <MapperEditorDrawer
          mapperName={mapper.name}
          entityName={entity.name}
          onClose={() => setEditingMapper(false)}
        />
      )}
      {editingSubGroup && (
        <SubGroupEditorDrawer
          groupName={editingSubGroup}
          mapperName={mapper.name}
          entityName={entity.name}
          onClose={() => setEditingSubGroup(null)}
        />
      )}
    </div>
  )
}

// Lineage geometry — kept in one place so chips, columns and the SVG
// connectors all agree. Bumping CHIP_PX (e.g. to fit longer labels)
// keeps the converger / diverger paths centred on chip midpoints.
const CHIP_PX = 56
const ROW_GAP_PX = 8

function laneHeight(count: number): number {
  if (count <= 0) return CHIP_PX
  return count * CHIP_PX + (count - 1) * ROW_GAP_PX
}

function chipMidY(i: number): number {
  return CHIP_PX / 2 + i * (CHIP_PX + ROW_GAP_PX)
}

// Sub-groups diverger — visual mirror of Converger but spreads one
// input across N outputs.
//
// self-center is critical: the parent row uses items-stretch, but this
// container has an explicit height. Without self-center it'd hug the
// top of the parent while the chip column next to it is justify-center
// — the SVG paths would land above the actual chip midpoints.
function Diverger({ count }: { count: number }) {
  const height = laneHeight(count)
  const midY = height / 2
  return (
    <div className="shrink-0 self-center flex items-center" style={{ height }}>
      <svg width="24" height={height} viewBox={`0 0 24 ${height}`} className="text-muted-foreground/40">
        {Array.from({ length: count }).map((_, i) => {
          const y = chipMidY(i)
          return (
            <path
              key={i}
              d={`M0 ${midY} L12 ${midY} L12 ${y} L24 ${y}`}
              fill="none"
              stroke="currentColor"
              strokeWidth="1"
            />
          )
        })}
      </svg>
    </div>
  )
}

function LineageChip({
  icon, kind, name, mono, highlight, active, muted, onClick,
}: {
  icon: React.ReactNode
  kind: string
  name: string
  mono?: boolean
  highlight?: boolean
  active?: boolean
  // Visually de-emphasise — used when the chip stands in for missing
  // data (e.g. raw table has no extractor in the catalog).
  muted?: boolean
  onClick?: () => void
}) {
  // Fixed height matches CHIP_PX so the Converger / Diverger SVG paths
  // land on chip centres regardless of label length. Truncate long
  // names with ellipsis rather than letting the chip grow.
  return (
    <button
      onClick={onClick}
      disabled={!onClick}
      style={{ height: CHIP_PX }}
      className={cn(
        'shrink-0 inline-flex flex-col items-start justify-center px-3 rounded border transition-colors text-left w-44',
        active     ? 'border-foreground/40 bg-primary/8'
        : highlight ? 'border-foreground/20 bg-card'
        : muted     ? 'border-dashed bg-muted/30'
        :             'bg-card',
        onClick && 'hover:bg-muted cursor-pointer',
        !onClick && 'cursor-default',
      )}
    >
      <div className="flex items-center gap-1 text-[9px] uppercase tracking-wider text-muted-foreground/70">
        {icon}
        {kind}
      </div>
      <div className={cn(
        'text-xs mt-0.5 truncate w-full',
        mono && 'font-mono',
        muted && 'text-muted-foreground/60 italic',
      )} title={name}>
        {name}
      </div>
    </button>
  )
}

function LineageArrow() {
  // Self-center so we line up with the chip / converger midpoints
  // (parent uses items-stretch so a bare span would float to the top).
  return (
    <span className="shrink-0 self-center text-muted-foreground/40 select-none">
      →
    </span>
  )
}

function Converger({ count }: { count: number }) {
  const height = laneHeight(count)
  const midY = height / 2
  return (
    // self-center keeps the SVG aligned with the chip column on the
    // left even when the parent row stretches to a taller lane
    // (e.g. sources=2 and sub-groups=4 → parent height = laneHeight(4),
    // this converger is laneHeight(2)).
    <div className="shrink-0 self-center flex items-center" style={{ height }}>
      <svg width="24" height={height} viewBox={`0 0 24 ${height}`} className="text-muted-foreground/40">
        {Array.from({ length: count }).map((_, i) => {
          const y = chipMidY(i)
          return (
            <path
              key={i}
              d={`M0 ${y} L12 ${y} L12 ${midY} L24 ${midY}`}
              fill="none"
              stroke="currentColor"
              strokeWidth="1"
            />
          )
        })}
      </svg>
    </div>
  )
}

// VG-297 — Extractor drawer with editable YAML.
// Admin → direct PUT /tool/{name}/extract.
// Member → POST /proposals with artifact_kind=extractor (after_yaml).
function ExtractorDrawer({
  name, entityName, onClose,
}: { name: string; entityName: string; onClose: () => void }) {
  const { api } = useModel()
  const [content, setContent] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.getExtractor(name)
      .then((r) => { if (!cancelled) setContent(r.raw_yaml ?? '') })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Load failed') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, name])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[40rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div>
            <div className="flex items-center gap-2">
              <Download className="h-4 w-4 text-muted-foreground" />
              <h2 className="text-base font-semibold tracking-tight">{name}</h2>
            </div>
            <p className="text-xs text-muted-foreground mt-0.5">
              Extractor · shared infrastructure
            </p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-4">
          {loading
            ? <Loading />
            : error
            ? <p className="text-xs text-red-600">{error}</p>
            : (
              <GovernedYamlEditor
                title={`Extractor: ${name}`}
                initialContent={content ?? ''}
                onDirectSave={async (yaml) => { await api.saveExtractor(name, yaml) }}
                onProposeChange={async (yaml, reason) => {
                  await api.createProposal({
                    artifact_kind: 'extractor',
                    artifact_name: name,
                    entity_name: entityName,
                    reason,
                    before_yaml: content ?? '',
                    after_yaml: yaml,
                  })
                }}
                onClose={onClose}
              />
            )
          }
        </div>
      </div>
    </>
  )
}

// MapperEditorDrawer — whole-mapper YAML edit. Use this to add/remove
// sub-groups or change the mapper's sources + targets. Admin → direct
// PUT mapper; member → propose with artifact_kind=mapper.
function MapperEditorDrawer({
  mapperName, entityName, onClose,
}: {
  mapperName: string
  entityName: string
  onClose: () => void
}) {
  const { api } = useModel()
  const [content, setContent] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.getMapper(mapperName)
      .then((r) => { if (!cancelled) setContent(r.raw_yaml ?? '') })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Load failed') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, mapperName])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[40rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div>
            <div className="flex items-center gap-2">
              <Shuffle className="h-4 w-4 text-muted-foreground" />
              <h2 className="text-base font-semibold tracking-tight">{mapperName}</h2>
            </div>
            <p className="text-xs text-muted-foreground mt-0.5">
              Mapper · whole YAML (add/remove sub-groups here)
            </p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-4">
          {loading
            ? <Loading />
            : error
            ? <p className="text-xs text-red-600">{error}</p>
            : (
              <GovernedYamlEditor
                title={`Mapper: ${mapperName}`}
                initialContent={content ?? ''}
                onDirectSave={async (yaml) => { await api.saveMapper(mapperName, yaml) }}
                onProposeChange={async (yaml, reason) => {
                  await api.createProposal({
                    artifact_kind: 'mapper',
                    artifact_name: mapperName,
                    entity_name: entityName,
                    reason,
                    before_yaml: content ?? '',
                    after_yaml: yaml,
                  })
                }}
                onClose={onClose}
              />
            )
          }
        </div>
      </div>
    </>
  )
}

// SubGroupEditorDrawer — same persistence story (saves the whole mapper)
// but the proposal records WHICH sub-group was edited via
// artifact_kind=sub_group + artifact_name=<group name>. Pulled into a
// side drawer for consistency with the extractor + mapper editors.
function SubGroupEditorDrawer({
  groupName, mapperName, entityName, onClose,
}: {
  groupName: string
  mapperName: string
  entityName: string
  onClose: () => void
}) {
  const { api } = useModel()
  const [content, setContent] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.getMapper(mapperName)
      .then((r) => { if (!cancelled) setContent(r.raw_yaml ?? '') })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Load failed') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, mapperName])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[40rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div>
            <div className="flex items-center gap-2">
              <Shuffle className="h-4 w-4 text-muted-foreground" />
              <h2 className="text-base font-semibold tracking-tight font-mono">{groupName}</h2>
            </div>
            <p className="text-xs text-muted-foreground mt-0.5">
              Sub-group inside mapper <span className="font-mono">{mapperName}</span>
            </p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-4">
          {loading
            ? <Loading />
            : error
            ? <p className="text-xs text-red-600">{error}</p>
            : (
              <GovernedYamlEditor
                title={`Sub-group: ${groupName} (inside mapper ${mapperName})`}
                initialContent={content ?? ''}
                onDirectSave={async (yaml) => { await api.saveMapper(mapperName, yaml) }}
                onProposeChange={async (yaml, reason) => {
                  await api.createProposal({
                    artifact_kind: 'sub_group',
                    artifact_name: groupName,
                    entity_name: entityName,
                    reason,
                    before_yaml: content ?? '',
                    after_yaml: yaml,
                  })
                }}
                onClose={onClose}
              />
            )
          }
        </div>
      </div>
    </>
  )
}

// ---------------------------------------------------------------------------
// Activity — chronological feed with ontology-bump clustering
// ---------------------------------------------------------------------------

const ACTION_ICON: Record<string, React.ReactNode> = {
  created:  <Plus className="h-3 w-3" />,
  updated:  <Pencil className="h-3 w-3" />,
  deleted:  <X className="h-3 w-3" />,
  restored: <RotateCcw className="h-3 w-3" />,
  ran:      <ActivityIcon className="h-3 w-3" />,
}

function ActivityTab({ entity }: { entity: EntitySummary }) {
  const { api } = useModel()
  const [feed, setFeed] = useState<ActivityFeed | null>(null)
  const [pending, setPending] = useState<Proposal[]>([])
  const [loading, setLoading] = useState(true)
  // Bump to refetch after a proposal is decided.
  const [refreshTick, setRefreshTick] = useState(0)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    Promise.all([
      api.getEntityActivity(entity.name).catch(() => ({ events: [], has_more: false })),
      api.listProposals({ entity: entity.name, status: 'pending' }).catch(() => [] as Proposal[]),
    ])
      .then(([f, p]) => {
        if (cancelled) return
        setFeed(f)
        setPending(p)
      })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, entity.name, refreshTick])

  if (loading) return <Loading />

  const hasFeed = feed && feed.events.length > 0
  const hasPending = pending.length > 0
  if (!hasFeed && !hasPending) {
    return <EmptyState label={`No activity yet for ${entity.name}.`} />
  }
  const groups = hasFeed ? groupActivity(feed!.events) : []

  return (
    <div className="max-w-3xl space-y-5">
      {hasPending && (
        <section>
          <h3 className="text-[10px] uppercase tracking-wider text-amber-700 dark:text-amber-400 mb-2">
            Pending changes · {pending.length} awaiting review
          </h3>
          <div className="space-y-3">
            {pending.map((p) => (
              <ProposalCard
                key={p.id}
                proposal={p}
                onDecided={() => setRefreshTick((t) => t + 1)}
              />
            ))}
          </div>
        </section>
      )}
      {groups.length > 0 && (
        <section>
          {hasPending && (
            <h3 className="text-[10px] uppercase tracking-wider text-muted-foreground/70 mb-2">History</h3>
          )}
          <div className="space-y-3">
            {groups.map((g, i) =>
              g.kind === 'ontology'
                ? <OntologyBumpCard key={i} version={g.version} events={g.events} />
                : <ArtifactEventCard key={i} event={g.event} />
            )}
          </div>
        </section>
      )}
    </div>
  )
}

type ActivityGroup =
  | { kind: 'ontology'; version: string; events: ActivityEvent[] }
  | { kind: 'artifact'; event: ActivityEvent }

export function groupActivity(events: ActivityEvent[]): ActivityGroup[] {
  const out: ActivityGroup[] = []
  for (const e of events) {
    const last = out[out.length - 1]
    if (e.ontology_version && last?.kind === 'ontology' && last.version === e.ontology_version) {
      last.events.push(e)
    } else if (e.ontology_version) {
      out.push({ kind: 'ontology', version: e.ontology_version, events: [e] })
    } else {
      out.push({ kind: 'artifact', event: e })
    }
  }
  return out
}

function OntologyBumpCard({ version, events }: { version: string; events: ActivityEvent[] }) {
  const actor = events[0].actor ?? 'someone'
  const when = events[0].created_at
  return (
    <div className="rounded border bg-card">
      <div className="flex items-center justify-between gap-3 px-3 py-2 border-b">
        <div className="flex items-center gap-2 min-w-0">
          <span className="shrink-0 inline-flex items-center gap-1 text-[10px] uppercase tracking-wider rounded px-1.5 py-0.5 border border-foreground/30 bg-muted/60 text-foreground/80">
            <Database className="h-2.5 w-2.5" />
            ontology
            <span className="font-mono normal-case tracking-normal">{version}</span>
          </span>
          <span className="text-xs truncate">
            <span className="font-medium">{actor}</span>{' '}
            <span className="text-muted-foreground">
              changed {events.length} {events.length === 1 ? 'thing' : 'things'}
            </span>
          </span>
        </div>
        <span className="text-[10px] text-muted-foreground/60 shrink-0">{formatWhen(when)}</span>
      </div>
      <ul>
        {events.map((e, i) => (
          <li
            key={i}
            className={cn(
              'flex items-baseline justify-between gap-2 px-3 py-2 text-xs',
              i !== 0 && 'border-t',
            )}
          >
            <div className="flex items-baseline gap-2 min-w-0">
              <span className="shrink-0 text-muted-foreground/60 inline-flex items-center">
                {ACTION_ICON[e.action]}
              </span>
              <span className="text-muted-foreground">{e.action}</span>{' '}
              <span className="text-muted-foreground/60">{e.object_kind}</span>{' '}
              <span className="font-mono">{e.object_name}</span>
            </div>
          </li>
        ))}
      </ul>
    </div>
  )
}

function ArtifactEventCard({ event: e }: { event: ActivityEvent }) {
  return (
    <div className="flex items-start gap-3 rounded border bg-card px-3 py-2.5">
      <span className="shrink-0 mt-0.5 rounded-full border bg-background p-1.5 text-muted-foreground">
        {ACTION_ICON[e.action]}
      </span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-0.5 flex-wrap">
          <span className="shrink-0 inline-flex items-center gap-1 text-[10px] uppercase tracking-wider rounded px-1.5 py-0.5 border border-border bg-card text-muted-foreground/70">
            {e.object_kind}
          </span>
          <span className="text-xs">
            {e.actor && <><span className="font-medium">{e.actor}</span>{' '}</>}
            <span className="text-muted-foreground">{e.action}</span>{' '}
            <span className="font-mono">{e.object_name}</span>
            {e.note && <span className="text-muted-foreground/70"> · {e.note}</span>}
          </span>
        </div>
        <div className="text-[10px] text-muted-foreground/60">{formatWhen(e.created_at)}</div>
      </div>
      <div className="flex items-center gap-1 shrink-0">
        <button
          disabled
          title="Version view lands in a follow-up"
          className="text-[10px] text-muted-foreground/30 inline-flex items-center gap-0.5 px-2 py-1 cursor-not-allowed"
        >
          <GitCommit className="h-3 w-3" /> view
        </button>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Shared bits
// ---------------------------------------------------------------------------

function Loading() {
  return <p className="text-xs text-muted-foreground">Loading…</p>
}

function EmptyState({ label }: { label: string }) {
  return (
    <div className="rounded border border-dashed bg-card/40 px-6 py-12 text-center max-w-3xl">
      <p className="text-sm text-muted-foreground">{label}</p>
    </div>
  )
}

function EmptyRow({ label }: { label: string }) {
  return <p className="text-xs text-muted-foreground/60 py-1.5">{label}</p>
}

// Convert an ISO timestamp to a short relative label. Falls back to the
// raw string if parsing fails so we never block the page on a bad date.
export function formatWhen(iso: string): string {
  const then = new Date(iso).getTime()
  if (Number.isNaN(then)) return iso
  const delta = Math.max(0, Date.now() - then) / 1000
  if (delta < 60) return 'just now'
  if (delta < 3600) return `${Math.floor(delta / 60)}m ago`
  if (delta < 86400) return `${Math.floor(delta / 3600)}h ago`
  if (delta < 604800) return `${Math.floor(delta / 86400)}d ago`
  return new Date(iso).toLocaleDateString()
}
