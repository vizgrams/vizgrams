// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChatViewCard — renders one chat-turn response as a View (Epic 20 VG-237).
 *
 * Every chat response is either:
 *   - ``saved_view``: a reference to an existing saved view → execute by name
 *   - ``inline_view``: a transient view YAML (+ optional transient query YAML)
 *                      → execute via the inline-view endpoint
 *
 * Either way the actual chart / table / metric / map rendering goes through
 * the same ``ViewContent`` component every other surface uses. Charts,
 * drilldowns, formatters, sorts — uniform across the product without
 * chat-specific code.
 *
 * Drilldown clicks navigate the user into ``/views``, ``/entities`` or
 * ``/apps`` so they land on the right surface. The chat itself doesn't
 * carry a drill stack — browser back/forward returns here.
 */

import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AlertCircle, ChevronDown, ChevronUp, Code, FileCode, Loader2, Wand2 } from 'lucide-react'

import type { ChatResponse, ChatTraceStep, ViewResult } from '@/api/client'
import { Card } from '@/components/Layout'
import { ViewContent } from '@/components/view/ViewContent'
import { ViewParamBar } from '@/components/view/ViewParamBar'
import { useModel } from '@/context/ModelContext'
import { frameToUrl, type DrillFrame } from '@/components/view/drilldown'
import { cn } from '@/lib/utils'

interface Props {
  response: ChatResponse
}

type SourceTab = 'query_yaml' | 'view_yaml' | 'sql' | 'trace'

export function ChatViewCard({ response }: Props) {
  const [openTab, setOpenTab] = useState<SourceTab | null>(null)

  if (!response.success) {
    return (
      <Card>
        <div className="flex items-start gap-2 text-sm text-destructive">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">Couldn't answer that</div>
            <div className="text-xs text-muted-foreground mt-1">
              {response.error || 'Unknown failure.'}
            </div>
          </div>
        </div>
        {response.trace.length > 0 && (
          <div className="mt-3">
            <SourceToggle response={response} openTab={openTab} setOpenTab={setOpenTab} />
          </div>
        )}
      </Card>
    )
  }

  return (
    <div className="space-y-2">
      <ChatViewBody response={response} />
      <SourceToggle response={response} openTab={openTab} setOpenTab={setOpenTab} />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Body: execute the view (saved or inline) and render via ViewContent.
// ---------------------------------------------------------------------------

function ChatViewBody({ response }: { response: ChatResponse }) {
  const { api } = useModel()
  const navigate = useNavigate()
  const [result, setResult] = useState<ViewResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  // The values the user is editing in the param bar. Seeded from the chat
  // response (the params the LLM picked); the schema (label/default/optional)
  // arrives in ``result.params`` after the first execute and is rendered by
  // ``ViewParamBar``.
  const initialParams = (response.saved_view?.params || response.inline_view?.params) ?? {}
  const [paramValues, setParamValues] = useState<Record<string, string>>(initialParams)

  const runWithParams = useCallback(async (values: Record<string, string>) => {
    setLoading(true); setError(null)
    try {
      const r = response.saved_view
        ? await api.executeView(response.saved_view.name, 1000, 0, values)
        : response.inline_view
        ? await api.executeViewInline(
            response.inline_view.view_yaml,
            response.inline_view.query_yaml,
            values,
          )
        : null
      if (!r) {
        setError('No view in response')
        return
      }
      setResult(r)
      // Saved views with no values supplied default each param from its
      // schema — surface those in the bar so the user sees what's being
      // applied instead of empty inputs.
      if (Object.keys(values).length === 0 && r.params?.length) {
        const defaults: Record<string, string> = {}
        for (const p of r.params) {
          if (p.default != null) defaults[p.name] = p.default
        }
        setParamValues((prev) => ({ ...defaults, ...prev }))
      }
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [api, response.saved_view, response.inline_view])

  useEffect(() => {
    runWithParams(initialParams)
    // initialParams is derived from response.* which is in runWithParams's
    // deps; we only want this to fire when the response itself changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runWithParams])

  // Clicking a drilldown target navigates into the appropriate surface
  // (/views, /entities, /apps). ``frameToUrl`` is the single source of
  // truth for that mapping — using it here keeps chat drilldowns
  // indistinguishable from in-surface drilldowns.
  const handleNavigate = (frame: DrillFrame) => {
    navigate(frameToUrl(frame))
  }

  if (error) {
    return (
      <Card>
        <div className="flex items-start gap-2 text-sm text-destructive">
          <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <div className="font-medium">View execution failed</div>
            <div className="text-xs text-muted-foreground mt-1">{error}</div>
          </div>
        </div>
      </Card>
    )
  }
  if (!result) {
    return (
      <Card>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading view…
        </div>
      </Card>
    )
  }

  const viz = (result.visualization as Record<string, unknown>) || {}
  const rowDrilldown = (viz.row_drilldown ?? viz.app_drilldown) as Parameters<typeof ViewContent>[0]['rowDrilldown']

  return (
    <div className="space-y-3">
      <ViewParamBar
        params={result.params ?? []}
        values={paramValues}
        onChange={setParamValues}
        onApply={() => runWithParams(paramValues)}
      />
      {loading && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Updating…
        </div>
      )}
      <ViewContent
        result={result}
        rowDrilldown={rowDrilldown}
        paramValues={paramValues}
        onNavigate={handleNavigate}
      />
    </div>
  )
}

// ---------------------------------------------------------------------------
// Source viewer — Query YAML / View YAML / SQL / Tool calls (VG-239).
// Same UX as before, just relocated to live alongside the new card.
// ---------------------------------------------------------------------------

interface SourceToggleProps {
  response: ChatResponse
  openTab: SourceTab | null
  setOpenTab: (tab: SourceTab | null) => void
}

interface TabSpec {
  key: SourceTab
  label: string
  available: boolean
  icon: React.ReactNode
}

function SourceToggle({ response, openTab, setOpenTab }: SourceToggleProps) {
  const tabs: TabSpec[] = [
    { key: 'query_yaml', label: 'Query YAML', available: !!response.query_yaml, icon: <FileCode className="h-3 w-3" /> },
    { key: 'view_yaml', label: 'View YAML', available: !!response.view_yaml, icon: <FileCode className="h-3 w-3" /> },
    { key: 'sql', label: 'SQL', available: !!response.sql, icon: <Code className="h-3 w-3" /> },
    {
      key: 'trace',
      label: `Tool calls (${response.trace.length})`,
      available: response.trace.length > 0,
      icon: <Wand2 className="h-3 w-3" />,
    },
  ]
  const available = tabs.filter((t) => t.available)
  if (available.length === 0) return null

  function toggle(key: SourceTab) {
    setOpenTab(openTab === key ? null : key)
  }

  const current = available.find((t) => t.key === openTab)

  return (
    <div className="pt-1">
      <div className="flex items-center gap-3 text-xs text-muted-foreground flex-wrap">
        {available.map((t) => (
          <button
            key={t.key}
            type="button"
            onClick={() => toggle(t.key)}
            className={cn(
              'flex items-center gap-1 hover:text-foreground transition-colors',
              openTab === t.key && 'text-foreground font-medium',
            )}
          >
            {t.icon}
            {t.label}
            {openTab === t.key
              ? <ChevronUp className="h-3 w-3" />
              : <ChevronDown className="h-3 w-3" />}
          </button>
        ))}
      </div>
      {current && (
        <div className="mt-2">
          {current.key === 'trace' ? (
            <TraceView trace={response.trace} />
          ) : (
            <pre className="text-xs bg-muted rounded p-3 overflow-x-auto whitespace-pre-wrap">
              {current.key === 'query_yaml' ? response.query_yaml :
               current.key === 'view_yaml' ? response.view_yaml :
               response.sql}
            </pre>
          )}
        </div>
      )}
    </div>
  )
}

function TraceView({ trace }: { trace: ChatTraceStep[] }) {
  const [expanded, setExpanded] = useState<number | null>(null)
  return (
    <div className="space-y-1.5">
      {trace.map((step, i) => {
        const isOpen = expanded === i
        return (
          <div key={i} className="border rounded text-xs">
            <button
              type="button"
              onClick={() => setExpanded(isOpen ? null : i)}
              className="w-full flex items-center gap-2 px-2 py-1.5 hover:bg-muted/40 transition-colors text-left"
            >
              <span className={cn(
                'inline-flex h-4 w-4 items-center justify-center rounded-full text-[10px] font-bold',
                step.success
                  ? 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400'
                  : 'bg-destructive/15 text-destructive',
              )}>
                {step.success ? '✓' : '✗'}
              </span>
              <code className="font-mono font-medium shrink-0">{step.name}</code>
              <span className="text-muted-foreground flex-1 truncate">{step.summary}</span>
              {isOpen
                ? <ChevronUp className="h-3 w-3 shrink-0" />
                : <ChevronDown className="h-3 w-3 shrink-0" />}
            </button>
            {isOpen && (
              <div className="border-t p-2 bg-muted/20 space-y-2">
                <div>
                  <div className="text-muted-foreground mb-1 uppercase tracking-wide text-[10px]">
                    Arguments
                  </div>
                  <pre className="bg-background rounded p-2 overflow-x-auto whitespace-pre-wrap">
                    {JSON.stringify(step.arguments, null, 2)}
                  </pre>
                </div>
                {Object.keys(step.payload).length > 0 && (
                  <div>
                    <div className="text-muted-foreground mb-1 uppercase tracking-wide text-[10px]">
                      Result
                    </div>
                    <pre className="bg-background rounded p-2 overflow-x-auto whitespace-pre-wrap">
                      {JSON.stringify(step.payload, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
