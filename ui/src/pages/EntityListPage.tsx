// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useState, useEffect } from 'react'
import { Link, useParams } from 'react-router-dom'
import { ChevronUp, ChevronDown, ChevronsUpDown, RefreshCw, Play, ChevronRight } from 'lucide-react'
import { useApi } from '@/hooks/useApi'
import { useJobPoller } from '@/hooks/useJobPoller'
import { useModel } from '@/context/ModelContext'
import { Badge, Card, ErrorMessage, Spinner } from '@/components/Layout'
import { JobStatusPanel } from '@/components/JobStatusPanel'
import { cn } from '@/lib/utils'
import type { AttributeOut, FeatureOut } from '@/api/client'

const PAGE_SIZE = 100

// ---------------------------------------------------------------------------
// Column selector panel
// ---------------------------------------------------------------------------

interface ColItem {
  id: string          // attr name or feature_id
  label: string
  isFeature: boolean
  description?: string | null
}

function ColumnSelectorPanel({
  attributes,
  features,
  selected,
  onApply,
}: {
  attributes: AttributeOut[]
  features: FeatureOut[]
  selected: string[]
  onApply: (cols: string[]) => void
}) {
  const [open, setOpen] = useState(false)
  const [draft, setDraft] = useState<Set<string>>(() => new Set(selected))

  // Sync draft when selected changes externally (e.g. entity changes)
  useEffect(() => { setDraft(new Set(selected)) }, [selected.join(',')])

  const items: ColItem[] = [
    ...attributes.map((a) => ({ id: a.name, label: a.name, isFeature: false, description: null })),
    ...features.map((f) => ({ id: f.feature_id, label: f.name, isFeature: true, description: f.description })),
  ]

  function toggle(id: string) {
    setDraft((prev) => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  function handleApply() {
    const ordered = items.filter((i) => draft.has(i.id)).map((i) => i.id)
    onApply(ordered)
  }

  return (
    <div className="border border-border rounded-md mb-4 overflow-hidden">
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center justify-between px-4 py-2.5 bg-muted/30 hover:bg-muted/60 transition-colors text-left"
      >
        <span className="text-sm font-medium flex items-center gap-2">
          Attributes &amp; Features
          <span className="text-xs text-muted-foreground font-normal">
            {selected.length} selected
          </span>
        </span>
        <ChevronRight className={cn('h-4 w-4 text-muted-foreground transition-transform', open && 'rotate-90')} />
      </button>

      {open && (
        <div className="px-4 py-3 space-y-3">
          <div className="flex flex-wrap gap-2 max-h-64 overflow-y-auto">
            {items.map((item) => {
              const on = draft.has(item.id)
              return (
                <button
                  key={item.id}
                  onClick={() => toggle(item.id)}
                  title={item.description ?? undefined}
                  className={cn(
                    'flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium border transition-colors',
                    on
                      ? 'bg-primary text-primary-foreground border-primary'
                      : 'bg-transparent text-muted-foreground border-border hover:border-foreground/40 hover:text-foreground',
                  )}
                >
                  {item.isFeature && (
                    <span className={cn(
                      'text-[9px] font-bold border rounded px-0.5 leading-none',
                      on ? 'border-primary-foreground/50 text-primary-foreground/80' : 'border-primary/30 text-primary/60',
                    )}>
                      ƒ
                    </span>
                  )}
                  {item.label}
                </button>
              )
            })}
          </div>
          <div className="flex items-center gap-2 pt-1 border-t border-border">
            <button
              onClick={handleApply}
              className="text-xs px-3 py-1.5 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              Apply
            </button>
            <button
              onClick={() => { setDraft(new Set(selected)); setOpen(false) }}
              className="text-xs px-3 py-1.5 border rounded-md hover:bg-muted transition-colors"
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

type SortDir = 'ASC' | 'DESC'
interface SortState { column: string; direction: SortDir }

function SortIcon({ column, sort }: { column: string; sort: SortState | null }) {
  if (sort?.column !== column) return <ChevronsUpDown className="inline h-3 w-3 ml-1 opacity-30" />
  return sort.direction === 'ASC'
    ? <ChevronUp className="inline h-3 w-3 ml-1" />
    : <ChevronDown className="inline h-3 w-3 ml-1" />
}

export function EntityListPage() {
  const { entity = '' } = useParams<{ entity: string }>()
  const { api, model } = useModel()
  const [offset, setOffset] = useState(0)
  const [filters, setFilters] = useState<string>('')
  const [pendingFilter, setPendingFilter] = useState<string>('')
  const [sort, setSort] = useState<SortState | null>(null)
  const { state: jobState, start: startJob, reset: resetJob } = useJobPoller()
  const [sortInitialised, setSortInitialised] = useState(false)
  // selectedCols drives the table; null = not yet initialised from schema
  const [selectedCols, setSelectedCols] = useState<string[] | null>(null)

  // Load entity schema
  const schemaState = useApi(() => api.getEntity(entity), [model, entity])

  // Initialise sort and selectedCols from schema once loaded
  if (schemaState.status === 'ok' && !sortInitialised) {
    const schema = schemaState.data
    const first = schema.display_order[0]
    setSort(first ? { column: first.column, direction: first.direction.toUpperCase() as 'ASC' | 'DESC' } : null)
    setSortInitialised(true)

    if (selectedCols === null) {
      const pk = schema.attributes.find((a) => a.semantic === 'PRIMARY_KEY')?.name
        ?? schema.attributes[0]?.name
      const list = schema.display_list.length > 0
        ? schema.display_list
        : schema.attributes.slice(0, 6).map((a) => a.name)
      setSelectedCols([pk, ...list.filter((c) => c !== pk)])
    }
  }
  if (schemaState.status === 'loading' && sortInitialised) {
    setSortInitialised(false)
    setSelectedCols(null)
  }

  const schema = schemaState.status === 'ok' ? schemaState.data : null
  const features = schema?.features ?? []
  const featureIds = new Set(features.map((f) => f.feature_id))

  // Attribute columns (non-feature) actually present in selectedCols
  const attrCols = (selectedCols ?? []).filter((c) => !featureIds.has(c))
  const featureCols = (selectedCols ?? []).filter((c) => featureIds.has(c))

  const pk = schema?.attributes.find((a) => a.semantic === 'PRIMARY_KEY')?.name
    ?? schema?.attributes[0]?.name

  // Inline query — only attribute columns (features fetched separately)
  const queryState = useApi(() => {
    if (schemaState.status !== 'ok' || !attrCols.length) return Promise.resolve(null)
    const where = filters.trim() ? [filters.trim()] : []
    const order_by = sort ? [{ column: sort.column, direction: sort.direction }] : []
    return api.executeInline(
      { entity, detail: true, attributes: attrCols, where, order_by },
      PAGE_SIZE,
      offset,
    )
  }, [model, entity, offset, filters, sort, attrCols.join(','), schemaState.status])

  // Fetch feature values when feature columns are selected
  const featureValuesState = useApi(
    () => featureCols.length > 0 ? api.getEntityFeatureValues(entity) : Promise.resolve(null),
    [model, entity, featureCols.join(',')],
  )
  const featureValues = featureValuesState.status === 'ok' ? featureValuesState.data : null

  if (schemaState.status === 'loading') return <Spinner />
  if (schemaState.status === 'error') return <ErrorMessage message={schemaState.error} />

  const displayCols = selectedCols ?? attrCols

  function toggleSort(col: string) {
    if (featureIds.has(col)) return // can't sort by feature columns
    setOffset(0)
    setSort((prev) => {
      if (prev?.column !== col) return { column: col, direction: 'ASC' }
      if (prev.direction === 'ASC') return { column: col, direction: 'DESC' }
      return null
    })
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">{entity}</h1>
        <div className="flex items-center gap-3">
          {queryState.status === 'ok' && queryState.data && (
            <Badge className="text-muted-foreground">
              {queryState.data.total_row_count.toLocaleString()} rows
            </Badge>
          )}
          <button
            disabled={jobState.phase === 'running'}
            onClick={async () => {
              resetJob()
              const job = await api.runMapper(entity)
              startJob(job.job_id, 'Run Mapper')
            }}
            className="flex items-center gap-1.5 border rounded-md px-3 py-1.5 text-sm hover:bg-muted transition-colors disabled:opacity-50"
          >
            <Play className={cn('h-3.5 w-3.5', jobState.phase === 'running' && jobState.job.operation === 'map' && 'animate-pulse')} />
            Run Mapper
          </button>
          <button
            disabled={jobState.phase === 'running'}
            onClick={async () => {
              resetJob()
              const job = await api.rematerializeEntity(entity)
              startJob(job.job_id, 'Rematerialize')
            }}
            className="flex items-center gap-1.5 border rounded-md px-3 py-1.5 text-sm hover:bg-muted transition-colors disabled:opacity-50"
          >
            <RefreshCw className={cn('h-3.5 w-3.5', jobState.phase === 'running' && jobState.job.operation === 'materialize' && 'animate-spin')} />
            Rematerialize
          </button>
        </div>
      </div>

      <JobStatusPanel state={jobState} onDismiss={resetJob} />

      {/* Column selector */}
      {schema && selectedCols && (
        <ColumnSelectorPanel
          attributes={schema.attributes}
          features={features}
          selected={selectedCols}
          onApply={(cols) => { setSelectedCols(cols); setOffset(0) }}
        />
      )}

      {/* Filter bar */}
      <div className="flex gap-2 mb-4">
        <input
          className="flex-1 border rounded-md px-3 py-1.5 text-sm font-mono focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder={`Filter — e.g. status == 'OPEN'`}
          value={pendingFilter}
          onChange={(e) => setPendingFilter(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') { setFilters(pendingFilter); setOffset(0) }
          }}
        />
        <button
          className="border rounded-md px-3 py-1.5 text-sm hover:bg-muted transition-colors"
          onClick={() => { setFilters(pendingFilter); setOffset(0) }}
        >
          Apply
        </button>
        {filters && (
          <button
            className="border rounded-md px-3 py-1.5 text-sm text-muted-foreground hover:bg-muted transition-colors"
            onClick={() => { setFilters(''); setPendingFilter(''); setOffset(0) }}
          >
            Clear
          </button>
        )}
      </div>

      {queryState.status === 'loading' && <Spinner />}
      {queryState.status === 'error' && <ErrorMessage message={queryState.error} />}

      {queryState.status === 'ok' && queryState.data && (() => {
        const result = queryState.data
        const pkIdx = result.columns.indexOf(pk ?? '')

        return (
          <>
            <Card className="overflow-x-auto p-0">
              <table className="w-full text-sm whitespace-nowrap">
                <thead>
                  <tr className="border-b bg-muted/50">
                    {displayCols.map((col) => {
                      const isFeat = featureIds.has(col)
                      const feat = isFeat ? features.find((f) => f.feature_id === col) : null
                      return (
                        <th key={col} className="text-left px-3 py-2.5 font-medium">
                          <button
                            onClick={() => toggleSort(col)}
                            disabled={isFeat}
                            className={cn(
                              'flex items-center gap-0.5 transition-colors',
                              isFeat ? 'cursor-default' : 'hover:text-foreground',
                              sort?.column === col ? 'text-foreground' : 'text-muted-foreground',
                            )}
                          >
                            {isFeat && (
                              <span className="text-[10px] font-semibold text-primary/60 border border-primary/30 rounded px-0.5 leading-none mr-0.5">
                                ƒ
                              </span>
                            )}
                            <span title={feat?.description ?? undefined}>{feat ? feat.name : col}</span>
                            {!isFeat && <SortIcon column={col} sort={sort} />}
                          </button>
                        </th>
                      )
                    })}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row, i) => {
                    const id = pkIdx >= 0 ? String(row[pkIdx] ?? '') : ''
                    return (
                      <tr key={i} className="border-b last:border-0 hover:bg-muted/30 transition-colors">
                        {displayCols.map((col) => {
                          if (featureIds.has(col)) {
                            const fv = featureValues?.[id]?.[col]
                            return (
                              <td key={col} className="px-3 py-2 text-muted-foreground">
                                {fv != null ? String(fv) : <span className="opacity-30">—</span>}
                              </td>
                            )
                          }
                          const colIdx = result.columns.indexOf(col)
                          const val = colIdx >= 0 ? row[colIdx] : null
                          return (
                            <td key={col} className="px-3 py-2">
                              {col === pk && id
                                ? <Link to={`/explore/${entity}/${encodeURIComponent(id)}`} className="text-primary hover:underline font-mono text-xs">{String(val ?? '')}</Link>
                                : <span className="text-muted-foreground">{val != null ? String(val) : '—'}</span>
                              }
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                  {result.rows.length === 0 && (
                    <tr><td colSpan={displayCols.length} className="px-3 py-8 text-center text-muted-foreground">No results</td></tr>
                  )}
                </tbody>
              </table>
            </Card>

            {/* Pagination */}
            <div className="mt-3 flex items-center justify-between text-sm text-muted-foreground">
              <span>
                {offset + 1}–{Math.min(offset + result.row_count, result.total_row_count)} of {result.total_row_count.toLocaleString()}
                {sort && <span className="ml-2 text-xs">sorted by {sort.column} {sort.direction}</span>}
              </span>
              <div className="flex gap-2">
                <button
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                  className="border rounded px-2.5 py-1 disabled:opacity-40 hover:bg-muted transition-colors"
                >
                  ← Prev
                </button>
                <button
                  disabled={!result.truncated}
                  onClick={() => setOffset(offset + PAGE_SIZE)}
                  className="border rounded px-2.5 py-1 disabled:opacity-40 hover:bg-muted transition-colors"
                >
                  Next →
                </button>
              </div>
            </div>
          </>
        )
      })()}
    </div>
  )
}
