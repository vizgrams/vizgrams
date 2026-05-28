// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChartDetailDrawer — full-chart side panel on /explore (Epic 27 VG-302).
 *
 * Replaces the dead /views/:name link from ChartCardEl. Fetches the view
 * + its result and hands them to the canonical ViewContent renderer so
 * the in-shell experience matches /views without needing to navigate
 * away. Drilldown clicks update the URL the same way the standalone
 * /views page does.
 */

import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { X } from 'lucide-react'

import type { ViewDetail, ViewResult } from '@/api/client'
import { ViewContent } from '@/components/view/ViewContent'
import { frameToUrl } from '@/components/view/drilldown'
import { useModel } from '@/context/ModelContext'

interface Props {
  viewName: string
  onClose: () => void
}

export function ChartDetailDrawer({ viewName, onClose }: Props) {
  const { api } = useModel()
  const navigate = useNavigate()
  const [detail, setDetail] = useState<ViewDetail | null>(null)
  const [result, setResult] = useState<ViewResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    Promise.all([api.getView(viewName), api.executeView(viewName, 1000)])
      .then(([d, r]) => {
        if (cancelled) return
        setDetail(d)
        setResult(r)
      })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, viewName])

  return (
    <>
      <div onClick={onClose} className="fixed inset-0 bg-black/30 z-40" aria-hidden />
      <div className="fixed top-0 right-0 bottom-0 w-[44rem] max-w-[95vw] bg-card border-l z-50 flex flex-col shadow-xl">
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-3 border-b">
          <div className="min-w-0">
            <h2 className="text-base font-semibold tracking-tight">{viewName}</h2>
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
              paramValues={{}}
              onNavigate={(frame) => {
                // Close the drawer when navigating away — react-router will
                // unmount /explore anyway, but clearing local state avoids
                // a flash if we ever stay in-page.
                onClose()
                navigate(frameToUrl(frame))
              }}
            />
          )}
        </div>
      </div>
    </>
  )
}
