// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * DrilldownOverlay — render a DrillFrame in a side drawer instead of
 * navigating away from the current surface.
 *
 * Background: the legacy frameToUrl flow targets /views/:name and
 * /entities/:e/:id, both of which now redirect to /explore (Epic 26
 * VG-298). Bar / table / map drilldowns inside an app or inside the
 * /explore ChartDetailDrawer would therefore lose context. This overlay
 * keeps users where they are and renders the target inline.
 *
 * Frame handling:
 *   view          → fetch + render via ViewContent
 *   entity-detail → record drawer (attributes / relations / computed)
 *   entity-list   → fetch entities + show the EntityListFrame browser
 *   app           → fall through to a router navigate (true context switch)
 *
 * Nested drilldowns push another overlay on top — the user can close one
 * level at a time without losing the parent context.
 */

import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { X } from 'lucide-react'

import type { EntitySummary, ViewDetail, ViewResult } from '@/api/client'
import { RecordDetailDrawer } from '@/components/explore/RecordDetailDrawer'
import { ViewContent } from '@/components/view/ViewContent'
import { frameToUrl, type DrillFrame } from '@/components/view/drilldown'
import { useModel } from '@/context/ModelContext'

interface Props {
  frame: DrillFrame
  onClose: () => void
}

export function DrilldownOverlay({ frame, onClose }: Props) {
  const navigate = useNavigate()
  // App frames are a true context switch — render nothing here and let the
  // caller handle the navigation. The effect runs once on mount.
  useEffect(() => {
    if (frame.kind === 'app') {
      navigate(frameToUrl(frame))
      onClose()
    }
    // app frame is fire-and-forget; other kinds render inline below
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  if (frame.kind === 'app') return null
  if (frame.kind === 'entity-detail') {
    return (
      <RecordDetailDrawer
        entity={frame.entity}
        id={frame.id}
        onClose={onClose}
      />
    )
  }
  if (frame.kind === 'entity-list') {
    return <EntityListOverlay entity={frame.entity} onClose={onClose} />
  }
  return <ViewOverlay name={frame.name} params={frame.params} onClose={onClose} />
}

// ---------------------------------------------------------------------------
// View overlay — same shape as ChartDetailDrawer but standalone so nested
// drilldowns can push another overlay on top.
// ---------------------------------------------------------------------------

function ViewOverlay({
  name, params, onClose,
}: { name: string; params: Record<string, string>; onClose: () => void }) {
  const { api } = useModel()
  const [detail, setDetail] = useState<ViewDetail | null>(null)
  const [result, setResult] = useState<ViewResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [nested, setNested] = useState<DrillFrame | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    Promise.all([api.getView(name), api.executeView(name, 1000, 0, params)])
      .then(([d, r]) => {
        if (cancelled) return
        setDetail(d)
        setResult(r)
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, name, params])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[44rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div className="min-w-0">
            <h2 className="text-base font-semibold tracking-tight">{name}</h2>
            {detail?.query && (
              <p className="text-[11px] text-muted-foreground/70 mt-0.5 font-mono truncate">
                query: {detail.query}
              </p>
            )}
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground shrink-0">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
          {loading && <p className="text-xs text-muted-foreground">Loading…</p>}
          {error && <p className="text-xs text-red-600">{error}</p>}
          {!loading && !error && result && (
            <ViewContent
              result={result}
              rowDrilldown={undefined}
              paramValues={params}
              onNavigate={(f) => setNested(f)}
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
// Entity list overlay — wraps the existing EntityListFrame (lazy-loaded the
// same way ExplorePage's Records tab does, so this overlay doesn't drag
// the frame into every bundle that touches drilldowns).
// ---------------------------------------------------------------------------

function EntityListOverlay({ entity, onClose }: { entity: string; onClose: () => void }) {
  const [Frame, setFrame] = useState<React.ComponentType<{ entity: string; onNavigate: (f: unknown) => void }> | null>(null)
  const [nested, setNested] = useState<DrillFrame | null>(null)

  useEffect(() => {
    import('@/pages/explore/EntityListFrame')
      .then((m) => setFrame(() => m.EntityListFrame as never))
      .catch(() => setFrame(null))
  }, [])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[56rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div className="min-w-0">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70">Records</div>
            <h2 className="text-base font-semibold tracking-tight">{entity}</h2>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground shrink-0">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-2 py-3">
          {Frame
            ? <Frame entity={entity} onNavigate={(f) => setNested(f as DrillFrame)} />
            : <p className="text-xs text-muted-foreground px-3 py-2">Loading…</p>
          }
        </div>
      </div>
      {nested && (
        <DrilldownOverlay frame={nested} onClose={() => setNested(null)} />
      )}
    </>
  )
}

// Re-export so consumers don't have to know about the entity-summary shape
// just to keep TypeScript happy with EntityListFrame's lazy import.
export type { EntitySummary }
