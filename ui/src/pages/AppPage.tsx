// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * AppPage — single application, full-bleed.
 *
 *   /apps/:name              the app with default params
 *   /apps/:name?p=v          params prefilled from the URL
 *
 * Drilldowns open an in-shell DrilldownOverlay rather than navigating
 * away — keeps the user inside the app context (which is the point of
 * an "app" surface). True app→app frames still navigate.
 */

import { useCallback, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'

import { DrilldownOverlay } from '@/components/explore/DrilldownOverlay'
import { AppFrame } from '@/pages/explore/AppFrame'
import { type DrillFrame } from '@/components/view/drilldown'

export function AppPage() {
  const { name } = useParams<{ name: string }>()
  const [searchParams, setSearchParams] = useSearchParams()
  const [drilldown, setDrilldown] = useState<DrillFrame | null>(null)

  const handleNavigate = useCallback((frame: DrillFrame) => {
    setDrilldown(frame)
  }, [])

  const handleParamsApplied = useCallback((next: Record<string, string>) => {
    const sp = new URLSearchParams()
    for (const [k, v] of Object.entries(next)) {
      if (v) sp.set(k, v)
    }
    setSearchParams(sp, { replace: true })
  }, [setSearchParams])

  if (!name) return null

  return (
    <div className="px-6 py-6">
      <AppFrame
        key={name}
        name={name}
        initialParams={Object.fromEntries(searchParams)}
        onNavigate={handleNavigate}
        onParamsApplied={handleParamsApplied}
      />
      {drilldown && (
        <DrilldownOverlay frame={drilldown} onClose={() => setDrilldown(null)} />
      )}
    </div>
  )
}
