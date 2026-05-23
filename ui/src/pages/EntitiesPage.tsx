// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * EntitiesPage — entity-data browser.
 *
 *   /entities                      left rail with all entities, empty right pane
 *   /entities/:entity              entity-list (a page of rows for one entity)
 *   /entities/:entity/:id          entity-detail (one record + relationships)
 *
 * Drilldown clicks bubble up through ``EntityListFrame`` / ``EntityDetailFrame``
 * as ``DrillFrame`` values and resolve to router URLs via ``frameToUrl``.
 */

import { useCallback, useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'

import type { EntitySummary } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { cn } from '@/lib/utils'
import { EntityListFrame } from '@/pages/explore/EntityListFrame'
import { EntityDetailFrame } from '@/pages/explore/EntityDetailFrame'
import { type DrillFrame, frameToUrl } from '@/components/view/drilldown'

export function EntitiesPage() {
  const { model, api } = useModel()
  const navigate = useNavigate()
  const { entity, id } = useParams<{ entity?: string; id?: string }>()

  const [entities, setEntities] = useState<EntitySummary[]>([])

  useEffect(() => {
    setEntities([])
    api.listEntities().then(setEntities).catch(() => {})
    // api identity changes per-render — depend on the model string only.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model])

  const handleNavigate = useCallback((frame: DrillFrame) => {
    navigate(frameToUrl(frame))
  }, [navigate])

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      <aside className="w-52 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="flex-1 overflow-y-auto py-2">
          {entities.length === 0
            ? <p className="px-4 py-6 text-xs text-muted-foreground text-center">Loading…</p>
            : entities.map((e) => {
                const active = entity === e.name
                return (
                  <button
                    key={e.name}
                    onClick={() => navigate(`/entities/${encodeURIComponent(e.name)}`)}
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
          }
        </div>
      </aside>

      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        <div className="flex-1 overflow-y-auto px-6 py-6">
          {!entity ? (
            <div className="flex flex-col items-center justify-center h-48 text-center gap-2">
              <p className="text-muted-foreground text-sm">Select an entity from the list to start exploring.</p>
            </div>
          ) : id ? (
            <EntityDetailFrame key={`${entity}-${id}`} entity={entity} id={id} onNavigate={handleNavigate} />
          ) : (
            <EntityListFrame key={entity} entity={entity} onNavigate={handleNavigate} />
          )}
        </div>
      </div>
    </div>
  )
}
