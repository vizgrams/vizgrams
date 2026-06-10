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

import { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  AlertCircle, Check, ChevronDown, ChevronUp, Code, Copy, ExternalLink,
  FileCode, Loader2, Upload, Wand2,
} from 'lucide-react'

import type { ChatResponse, ChatTraceStep, ViewResult } from '@/api/client'
import { previewCaption } from '@/api/client'
import { Card } from '@/components/Layout'
import { ViewContent } from '@/components/view/ViewContent'
import { ViewParamBar } from '@/components/view/ViewParamBar'
import { useModel } from '@/context/ModelContext'
import { useRole } from '@/context/RoleContext'
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
  const { api, model } = useModel()
  const { role } = useRole()
  const canPublish = role === 'admin' || role === 'member'
  const navigate = useNavigate()
  const [result, setResult] = useState<ViewResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [publishOpen, setPublishOpen] = useState(false)
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
  const rowDrilldown = viz.row_drilldown as Parameters<typeof ViewContent>[0]['rowDrilldown']
  const appDrilldown = viz.app_drilldown as Parameters<typeof ViewContent>[0]['appDrilldown']

  return (
    <div className="space-y-3">
      <div className="flex items-start gap-2">
        <div className="flex-1 min-w-0">
          <ViewParamBar
            params={result.params ?? []}
            values={paramValues}
            onChange={setParamValues}
            onApply={() => runWithParams(paramValues)}
          />
        </div>
        {canPublish && (
          <button
            onClick={() => setPublishOpen(true)}
            className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors shrink-0"
            title="Publish this answer as a vizgram"
          >
            <Upload className="h-3.5 w-3.5" />
            Publish
          </button>
        )}
      </div>
      {loading && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Updating…
        </div>
      )}
      <ViewContent
        result={result}
        rowDrilldown={rowDrilldown}
        appDrilldown={appDrilldown}
        paramValues={paramValues}
        onNavigate={handleNavigate}
      />
      {publishOpen && (
        <PublishDialog
          response={response}
          result={result}
          paramValues={paramValues}
          modelId={model}
          onClose={() => setPublishOpen(false)}
        />
      )}
    </div>
  )
}


// ---------------------------------------------------------------------------
// Publish dialog (Epic 21 — VG-240 + VG-241)
//
// Mirrors the existing /views publish dialog so the UX is identical for
// the user. The one extra step happens server-side: if the chat turn is
// inline (path B / C), the backend saves view + optional query as
// artifacts (created_via='chat', uncertified) before creating the
// vizgram. The link we hand back goes to /views/<name> so the user
// shares live data, not a static snapshot.
// ---------------------------------------------------------------------------

function PublishDialog({
  response, result, paramValues, modelId, onClose,
}: {
  response: ChatResponse
  result: ViewResult
  paramValues: Record<string, string>
  modelId: string
  onClose: () => void
}) {
  const { api } = useModel()
  // Default title: the short factual title the LLM produced for this turn
  // (set by present_view's ``title`` arg, or the saved view's name for
  // path A). Falls back to a placeholder only when the auto-present
  // safety net produced the turn (LLM forgot present_view entirely).
  const defaultTitle = response.title || response.saved_view?.name || 'Untitled chat answer'
  const [title, setTitle] = useState(defaultTitle)
  const [caption, setCaption] = useState('')
  const [captionLoading, setCaptionLoading] = useState(true)
  const [captionUnavailable, setCaptionUnavailable] = useState(false)
  const [publishing, setPublishing] = useState(false)
  const [publishError, setPublishError] = useState<string | null>(null)
  const [published, setPublished] = useState<{ view_name: string; vizgram_id: string } | null>(null)
  const [copied, setCopied] = useState(false)
  const titleRef = useRef<HTMLInputElement>(null)

  // Seed the AI caption on open. Same endpoint the /views publish
  // dialog uses, so cached captions on the same data hash get reused.
  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const MAX_ROWS = result.type === 'table' ? 50 : 500
        const res = await previewCaption({
          model: modelId,
          query_ref: result.query || defaultTitle,
          title: defaultTitle,
          slice_config: { parameters: paramValues, snapshot_at: new Date().toISOString() },
          chart_config: {
            type: result.type,
            visualization: result.visualization,
            columns: result.columns,
          },
          data_snapshot: result.rows.slice(0, MAX_ROWS),
        })
        if (cancelled) return
        if (res.caption) setCaption(res.caption)
        else setCaptionUnavailable(true)
      } catch {
        if (!cancelled) setCaptionUnavailable(true)
      } finally {
        if (!cancelled) setCaptionLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
    // We deliberately don't re-run when the user edits the title —
    // captions are seeded once from the data, not the title.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  async function handlePublish() {
    if (!title.trim() || publishing) return
    setPublishing(true); setPublishError(null)
    try {
      const out = await api.chatPublish({
        title: title.trim(),
        caption: caption.trim() || null,
        saved_view: response.saved_view ?? null,
        inline_view: response.inline_view ?? null,
        params: paramValues,
        // VG-283: when the chat turn was persisted (server returned a
        // turn_id), pass it through so the publish endpoint can attach
        // the produced artifacts back to the originating turn.
        turn_id: response.turn_id ?? null,
      })
      setPublished({ view_name: out.view_name, vizgram_id: out.vizgram_id })
    } catch (e) {
      setPublishError(e instanceof Error ? e.message : String(e))
    } finally {
      setPublishing(false)
    }
  }

  const shareUrl = published
    ? `${window.location.origin}/views/${encodeURIComponent(published.view_name)}`
    : ''

  async function copyLink() {
    if (!shareUrl) return
    try {
      await navigator.clipboard.writeText(shareUrl)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard API can fail in non-secure contexts; the user can
      // copy from the input box manually.
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40"
      onClick={onClose}
    >
      <div
        className="bg-background rounded-lg border shadow-lg p-6 w-full max-w-md space-y-4"
        onClick={(e) => e.stopPropagation()}
      >
        {published ? (
          <>
            <h2 className="text-base font-semibold flex items-center gap-2">
              <Check className="h-4 w-4 text-emerald-500" />
              Published
            </h2>
            <p className="text-xs text-muted-foreground">
              Anyone with this link can open the view with live data.
            </p>
            <div className="flex items-center gap-2">
              <input
                readOnly
                value={shareUrl}
                onFocus={(e) => e.target.select()}
                className="flex-1 h-8 rounded border bg-muted/30 px-2 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-ring"
              />
              <button
                onClick={copyLink}
                className="flex items-center gap-1.5 border rounded-md px-2.5 h-8 text-xs hover:bg-muted transition-colors"
                title="Copy link"
              >
                {copied ? <Check className="h-3.5 w-3.5 text-emerald-500" /> : <Copy className="h-3.5 w-3.5" />}
                {copied ? 'Copied' : 'Copy'}
              </button>
            </div>
            <div className="flex justify-between items-center pt-1">
              <a
                href={`/views/${encodeURIComponent(published.view_name)}`}
                className="text-xs text-primary hover:underline flex items-center gap-1"
              >
                Open view
                <ExternalLink className="h-3 w-3" />
              </a>
              <button
                onClick={onClose}
                className="bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity"
              >
                Done
              </button>
            </div>
          </>
        ) : (
          <>
            <h2 className="text-base font-semibold">Publish vizgram</h2>

            <div className="space-y-1.5">
              <label className="text-xs font-medium text-muted-foreground">Title</label>
              <input
                ref={titleRef}
                type="text"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                autoFocus
                onFocus={(e) => e.target.select()}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !captionLoading && title.trim()) handlePublish()
                }}
                className="w-full h-8 rounded border bg-background px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </div>

            <div className="space-y-1.5">
              <div className="flex items-center gap-2">
                <label className="text-xs font-medium text-muted-foreground">Caption</label>
                {captionLoading && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
                {captionUnavailable && !captionLoading && (
                  <span className="text-xs text-muted-foreground/60">AI unavailable — write your own</span>
                )}
              </div>
              <textarea
                value={caption}
                onChange={(e) => setCaption(e.target.value)}
                placeholder={captionLoading ? 'Generating…' : 'Add a caption (optional)'}
                disabled={captionLoading}
                rows={3}
                className="w-full rounded border bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-ring resize-none disabled:opacity-50"
              />
            </div>

            {publishError && <p className="text-xs text-destructive">{publishError}</p>}

            <div className="flex justify-end gap-2">
              <button
                onClick={onClose}
                className="border rounded-md px-3 py-1.5 text-xs hover:bg-muted transition-colors"
              >
                Cancel
              </button>
              <button
                disabled={publishing || !title.trim() || captionLoading}
                onClick={handlePublish}
                className="bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40"
              >
                {publishing ? 'Publishing…' : 'Publish'}
              </button>
            </div>
          </>
        )}
      </div>
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
