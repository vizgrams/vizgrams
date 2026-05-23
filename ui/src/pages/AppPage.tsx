// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * AppPage — single application, full-bleed.
 *
 *   /apps/:name              the app with default params
 *   /apps/:name?p=v          params prefilled from the URL
 *
 * Drilldown clicks resolve to router URLs via ``frameToUrl``.
 */

import { useCallback } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { AppFrame } from '@/pages/explore/AppFrame'
import { type DrillFrame, frameToUrl } from '@/components/view/drilldown'

export function AppPage() {
  const { name } = useParams<{ name: string }>()
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()

  const handleNavigate = useCallback((frame: DrillFrame) => {
    navigate(frameToUrl(frame))
  }, [navigate])

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
    </div>
  )
}
