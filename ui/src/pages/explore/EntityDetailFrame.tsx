// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * EntityDetailFrame — props-driven version of EntityDetailPage.
 * Used inside ExploreShell where navigation is handled by useDrillStack.
 */
import { useState } from 'react'
import type { RelationshipStub, FeatureOut } from '@/api/client'
import { useApi } from '@/hooks/useApi'
import { useModel } from '@/context/ModelContext'
import { Badge, Card, ErrorMessage, Spinner } from '@/components/Layout'
import { ChevronRight, Check, X, Pencil, ExternalLink } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { DrillFrame } from '@/hooks/useDrillStack'

// ---------------------------------------------------------------------------
// Feature inline editor (unchanged from EntityDetailPage)
// ---------------------------------------------------------------------------

function FeatureValueCell({
  entity,
  feature,
  featureValue,
}: {
  entity: string
  feature: FeatureOut
  featureValue: { value: string | number | null; computed_at: string | null } | undefined
}) {
  const { api } = useModel()
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState(feature.expr)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSave = async () => {
    setSaving(true); setError(null)
    try { await api.updateFeatureExpr(entity, feature.feature_id, draft); setEditing(false) }
    catch (e) { setError(e instanceof Error ? e.message : 'Save failed') }
    finally { setSaving(false) }
  }

  if (editing) {
    return (
      <div className="flex flex-col gap-1">
        <input
          className="text-xs font-mono border border-border rounded px-2 py-1 bg-background w-full"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') handleSave(); if (e.key === 'Escape') { setDraft(feature.expr); setEditing(false) } }}
          autoFocus
          disabled={saving}
        />
        {error && <span className="text-xs text-destructive">{error}</span>}
        <div className="flex gap-1">
          <button onClick={handleSave} disabled={saving} className="flex items-center gap-1 text-xs text-green-600 hover:text-green-700 disabled:opacity-50">
            <Check className="h-3 w-3" /> Save
          </button>
          <button onClick={() => { setDraft(feature.expr); setEditing(false) }} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
            <X className="h-3 w-3" /> Cancel
          </button>
        </div>
      </div>
    )
  }

  if (featureValue !== undefined) {
    return (
      <div className="flex items-center gap-1 group">
        <span className="text-sm font-mono">
          {featureValue.value === null
            ? <span className="italic text-muted-foreground opacity-40">null</span>
            : String(featureValue.value)}
        </span>
        <button onClick={() => setEditing(true)} className="opacity-0 group-hover:opacity-60 hover:!opacity-100 transition-opacity" title={`Edit expression: ${feature.expr}`}>
          <Pencil className="h-3 w-3" />
        </button>
      </div>
    )
  }

  return (
    <div className="flex items-center gap-1 group">
      <span className="text-sm text-muted-foreground opacity-30 cursor-default" title={`Not yet computed. Expression: ${feature.expr}`}>—</span>
      <button onClick={() => setEditing(true)} className="opacity-0 group-hover:opacity-60 hover:!opacity-100 transition-opacity" title={`Edit expression: ${feature.expr}`}>
        <Pencil className="h-3 w-3" />
      </button>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Related records table — uses onNavigate instead of Link
// ---------------------------------------------------------------------------

function RelatedTable({
  result,
  targetEntity,
  onNavigate,
}: {
  result: import('@/api/client').RelatedResult
  targetEntity: string
  onNavigate: (frame: DrillFrame) => void
}) {
  const pkIdx = result.target_pk ? result.columns.indexOf(result.target_pk) : -1

  if (result.rows.length === 0) {
    return <p className="text-sm text-muted-foreground">No related records found.</p>
  }

  return (
    <div className="space-y-2">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border">
              {result.columns.map((col) => (
                <th key={col} className="text-left px-2 py-1 font-medium text-muted-foreground text-xs">{col}</th>
              ))}
              <th className="w-8" />
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, i) => {
              const pkVal = pkIdx >= 0 ? String(row[pkIdx] ?? '') : null
              return (
                <tr key={i} className="border-b border-border/50 hover:bg-muted/30">
                  {row.map((cell, j) => (
                    <td key={j} className="px-2 py-1.5 font-mono text-xs text-muted-foreground max-w-48 truncate">
                      {cell === null ? <span className="italic opacity-40">null</span> : String(cell)}
                    </td>
                  ))}
                  <td className="px-2 py-1.5">
                    {pkVal && (
                      <button
                        onClick={() => onNavigate({ kind: 'entity-detail', entity: targetEntity, id: pkVal })}
                        className="text-primary hover:text-primary/80 transition-colors"
                        title={`Open ${targetEntity} · ${pkVal}`}
                      >
                        <ExternalLink className="h-3 w-3" />
                      </button>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {result.truncated && (
        <p className="text-xs text-muted-foreground">
          Showing {result.rows.length} of {result.total_row_count} records
        </p>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Relationship panel
// ---------------------------------------------------------------------------

function RelationshipRow({
  entity,
  id,
  name,
  stub,
  onNavigate,
}: {
  entity: string
  id: string
  name: string
  stub: RelationshipStub
  onNavigate: (frame: DrillFrame) => void
}) {
  const { api } = useModel()
  const [open, setOpen] = useState(false)

  const relState = useApi(
    () => (open ? api.getRelated(entity, id, name) : Promise.resolve(null)),
    [open, entity, id, name],
  )

  const isManyToOne = stub.cardinality === 'MANY_TO_ONE' || stub.cardinality === 'ONE_TO_ONE'

  return (
    <div className="border border-border rounded-md overflow-hidden">
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center justify-between px-4 py-3 bg-muted/40 hover:bg-muted/70 transition-colors text-left"
      >
        <div className="flex items-center gap-2">
          <span className="font-medium text-sm">{name}</span>
          <Badge className="text-xs">{stub.cardinality}</Badge>
          <span className="text-xs text-muted-foreground">→ {stub.target}</span>
        </div>
        <div className="flex items-center gap-2">
          {isManyToOne && 'id' in stub && stub.id != null && (
            <span className="text-xs text-muted-foreground font-mono">{stub.id}</span>
          )}
          {!isManyToOne && 'count' in stub && (
            <span className="text-xs text-muted-foreground">{stub.count ?? '?'} records</span>
          )}
          <ChevronRight className={cn('h-4 w-4 text-muted-foreground transition-transform', open && 'rotate-90')} />
        </div>
      </button>

      {open && (
        <div className="p-4">
          {relState.status === 'loading' && <Spinner />}
          {relState.status === 'error' && <ErrorMessage message={relState.error} />}
          {relState.status === 'ok' && relState.data && (
            <RelatedTable result={relState.data} targetEntity={stub.target} onNavigate={onNavigate} />
          )}
          {relState.status === 'ok' && !relState.data && (
            <p className="text-sm text-muted-foreground">No data</p>
          )}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main frame
// ---------------------------------------------------------------------------

export function EntityDetailFrame({
  entity,
  id,
  onNavigate,
}: {
  entity: string
  id: string
  onNavigate: (frame: DrillFrame) => void
}) {
  const { api, model } = useModel()
  const recordState = useApi(() => api.getEntityRecord(entity, id), [model, entity, id])
  const schemaState = useApi(() => api.getEntity(entity), [model, entity])

  if (recordState.status === 'loading') return <Spinner />
  if (recordState.status === 'error') return <ErrorMessage message={recordState.error} />

  const record = recordState.data
  const schema = schemaState.status === 'ok' ? schemaState.data : null
  const displayDetail = schema?.display_detail ?? []
  const features: FeatureOut[] = schema?.features ?? []

  const orderedKeys = displayDetail.length > 0
    ? [...displayDetail.filter((k) => k in record.properties), ...Object.keys(record.properties).filter((k) => !displayDetail.includes(k))]
    : Object.keys(record.properties)

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <h1 className="text-2xl font-semibold">{entity}</h1>
        <span className="font-mono text-muted-foreground text-sm">{id}</span>
      </div>

      <Card>
        <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-4">Properties</h2>
        <dl className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-3">
          {orderedKeys.map((key) => {
            const val = record.properties[key]
            return (
              <div key={key} className="flex flex-col gap-0.5">
                <dt className="text-xs text-muted-foreground font-medium">{key}</dt>
                <dd className="text-sm font-mono">
                  {val === null ? <span className="italic text-muted-foreground opacity-40">null</span> : String(val)}
                </dd>
              </div>
            )
          })}
          {features.map((feature) => {
            const fv = record.feature_values?.[feature.feature_id]
            return (
              <div key={feature.feature_id} className="flex flex-col gap-0.5">
                <dt className="text-xs text-muted-foreground font-medium flex items-center gap-1">
                  {feature.name}
                  <span className="text-[10px] font-semibold text-primary/60 border border-primary/30 rounded px-0.5 leading-none" title={feature.description ?? `Feature: ${feature.expr}`}>ƒ</span>
                </dt>
                <dd>
                  <FeatureValueCell entity={entity} feature={feature} featureValue={fv} />
                </dd>
              </div>
            )
          })}
        </dl>
      </Card>

      {Object.keys(record.relationships).length > 0 && (
        <div className="space-y-2">
          <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">Relationships</h2>
          {Object.entries(record.relationships).map(([name, stub]) => (
            <RelationshipRow key={name} entity={entity} id={id} name={name} stub={stub} onNavigate={onNavigate} />
          ))}
        </div>
      )}
    </div>
  )
}
