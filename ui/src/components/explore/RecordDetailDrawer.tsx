// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * RecordDetailDrawer — entity record detail panel on /explore (Epic 27 VG-303).
 *
 * Opens when a row in the Records tab (EntityListFrame) is clicked. Shows
 * the record's attributes, relations (with click-through to navigate), and
 * computed feature values. Reuses api.getEntityRecord which already powers
 * the legacy /entities/:entity/:id surface.
 */

import { useEffect, useState } from 'react'
import { Hash, Link2, Sparkles, X } from 'lucide-react'

import type { EntityRecord, RelationshipStub } from '@/api/client'
import { useModel } from '@/context/ModelContext'

interface Props {
  entity: string
  id: string
  onClose: () => void
  onNavigateRelated?: (entity: string) => void
}

export function RecordDetailDrawer({ entity, id, onClose, onNavigateRelated }: Props) {
  const { api } = useModel()
  const [record, setRecord] = useState<EntityRecord | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    api.getEntityRecord(entity, id)
      .then((r) => { if (!cancelled) setRecord(r) })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, entity, id])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[36rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div className="min-w-0">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70">{entity}</div>
            <h2 className="text-base font-semibold tracking-tight font-mono truncate">{id}</h2>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground shrink-0">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
          {loading && <p className="text-xs text-muted-foreground">Loading…</p>}
          {error && <p className="text-xs text-red-600">{error}</p>}
          {!loading && !error && record && (
            <>
              <Section icon={<Hash className="h-3.5 w-3.5" />} title="Attributes">
                {Object.keys(record.properties).length === 0
                  ? <Empty label="No attributes." />
                  : Object.entries(record.properties).map(([k, v]) => (
                      <DefRow key={k} label={k} value={v} mono />
                    ))
                }
              </Section>

              <Section icon={<Link2 className="h-3.5 w-3.5" />} title="Relations">
                {Object.keys(record.relationships).length === 0
                  ? <Empty label="No relations." />
                  : Object.entries(record.relationships).map(([name, rel]) => (
                      <RelationRow
                        key={name}
                        name={name}
                        stub={rel}
                        onNavigate={onNavigateRelated}
                      />
                    ))
                }
              </Section>

              <Section icon={<Sparkles className="h-3.5 w-3.5" />} title="Computed">
                {Object.keys(record.feature_values).length === 0
                  ? <Empty label="No computed features." />
                  : Object.entries(record.feature_values).map(([k, fv]) => (
                      <DefRow
                        key={k}
                        label={k}
                        value={fv.value}
                        secondary={fv.computed_at ? `computed ${fv.computed_at}` : undefined}
                        mono
                      />
                    ))
                }
              </Section>
            </>
          )}
        </div>
      </div>
    </>
  )
}

function Section({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-muted-foreground/70 pb-2 mb-2 border-b">
        {icon}
        {title}
      </div>
      <div className="space-y-1">{children}</div>
    </div>
  )
}

function DefRow({ label, value, secondary, mono }: {
  label: string
  value: string | number | null
  secondary?: string
  mono?: boolean
}) {
  return (
    <div className="grid grid-cols-[10rem_1fr] gap-3 py-1 text-xs border-b border-border/40 last:border-b-0">
      <div className="font-mono text-muted-foreground/80 truncate">{label}</div>
      <div className="min-w-0">
        <div className={mono ? 'font-mono break-all' : 'break-words'}>
          {value == null ? <span className="text-muted-foreground/40">—</span> : String(value)}
        </div>
        {secondary && <div className="text-[10px] text-muted-foreground/60 mt-0.5">{secondary}</div>}
      </div>
    </div>
  )
}

function RelationRow({
  name, stub, onNavigate,
}: {
  name: string
  stub: RelationshipStub
  onNavigate?: (entity: string) => void
}) {
  // M2O / 1:1 → single target with an id (or null). 1:M / M:M → just a count.
  const isSingleton = stub.cardinality === 'MANY_TO_ONE' || stub.cardinality === 'ONE_TO_ONE'
  return (
    <div className="grid grid-cols-[10rem_1fr] gap-3 py-1 text-xs border-b border-border/40 last:border-b-0">
      <div className="font-mono text-muted-foreground/80 truncate">{name}</div>
      <div className="min-w-0 flex items-center justify-between gap-2">
        <span className="text-muted-foreground">
          {isSingleton
            ? ('id' in stub && stub.id ? `→ ${stub.target} · ${stub.id}` : `→ ${stub.target}`)
            : `${'count' in stub && stub.count != null ? stub.count : '?'} ${stub.target}`}
        </span>
        {onNavigate && (
          <button
            onClick={() => onNavigate(stub.target)}
            title={`Switch to ${stub.target}`}
            className="text-[10px] text-muted-foreground/70 hover:text-foreground"
          >
            open →
          </button>
        )}
      </div>
    </div>
  )
}

function Empty({ label }: { label: string }) {
  return <div className="text-xs text-muted-foreground/50 italic py-1">{label}</div>
}
