// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChatPage — assistant-ui + AG-UI (Phase 2 of chat-first framing).
 *
 * Wraps the app in AssistantRuntimeProvider bound to an HttpAgent
 * pointing at /api/v1/model/{model}/chat/stream (AG-UI protocol). The
 * server-side loop (chat_turn) emits AG-UI events; two makeAssistantToolUI
 * components handle the chart-card render for the terminal tools
 * (present_view / run_saved_view) — the tool's TOOL_CALL_RESULT carries
 * the view payload as JSON.
 *
 * This is the minimum viable swap: text streaming works, chart cards
 * render as JSON blocks (a follow-up wires the real ChatViewCard so
 * drilldowns / publish / params flow through). Session history sidebar
 * is deferred — Phase 3 threads it through assistant-ui's threadList
 * adapter against the existing /chat/sessions API.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AlertCircle, Loader2 } from 'lucide-react'
import { HttpAgent } from '@ag-ui/client'
import {
  AssistantRuntimeProvider,
  ThreadPrimitive,
  MessagePrimitive,
  ComposerPrimitive,
  makeAssistantToolUI,
  useMessage,
  type ToolCallMessagePartProps,
} from '@assistant-ui/react'
import { useAgUiRuntime } from '@assistant-ui/react-ag-ui'

import type { ViewResult } from '@/api/client'
import { ViewContent } from '@/components/view/ViewContent'
import { ViewParamBar } from '@/components/view/ViewParamBar'
import { frameToUrl, type DrillFrame, type ViewDrilldownConfig } from '@/components/view/drilldown'
import { useModel } from '@/context/ModelContext'

// Dev auth: the API expects X-Dev-User in local dev. Vite's dev server
// proxies /api → :8000, so a relative URL is enough.
const DEV_USER = 'oliver.fenton@iaggbs.com'

function ChatRuntimeProvider({
  model,
  children,
}: {
  model: string
  children: React.ReactNode
}) {
  const agent = useMemo(
    () =>
      new HttpAgent({
        url: `/api/v1/model/${encodeURIComponent(model)}/chat/stream`,
        headers: { 'X-Dev-User': DEV_USER },
      }),
    [model],
  )
  const runtime = useAgUiRuntime({ agent })
  return (
    <AssistantRuntimeProvider runtime={runtime}>
      {children}
    </AssistantRuntimeProvider>
  )
}

// ---------------------------------------------------------------------------
// Tool UI — the chart card. The backend embeds a view payload in the
// terminal tool's TOOL_CALL_RESULT as JSON; we parse and render.
// ---------------------------------------------------------------------------

type ViewPayload =
  | { kind: 'saved_view'; payload: { name: string; params?: Record<string, string> } }
  | { kind: 'inline_view'; payload: { view_yaml: string; query_yaml?: string | null; params?: Record<string, string> } }

// Assistant-ui's tool renderer may hand ``result`` as string OR object
// OR null/undefined during the "call in flight, no result yet" phase.
// Handle all three so a re-render race doesn't leave the user staring
// at a placeholder forever.
function ChartCardRender({ result }: ToolCallMessagePartProps<unknown, unknown>) {
  const parsed = useMemo<ViewPayload | null>(() => {
    if (result == null) return null
    if (typeof result === 'object') return result as ViewPayload
    if (typeof result !== 'string') return null
    try {
      return JSON.parse(result) as ViewPayload
    } catch {
      return null
    }
  }, [result])

  if (!parsed) {
    const hint = result == null
      ? '(waiting for tool result)'
      : `(unparseable: ${typeof result === 'string' ? result.slice(0, 80) : typeof result})`
    return (
      <div className="rounded-md border bg-muted/30 p-3 my-2 text-xs text-muted-foreground font-mono">
        {hint}
      </div>
    )
  }
  return <ChatChartCard view={parsed} />
}

// Execute the view (saved or inline) and render via ViewContent — the
// same component every other surface uses. Drilldown navigates out of
// chat into /explore. Params editable via ViewParamBar.
function ChatChartCard({ view }: { view: ViewPayload }) {
  const { api } = useModel()
  const navigate = useNavigate()
  const [result, setResult] = useState<ViewResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const initialParams = view.payload.params ?? {}
  const [paramValues, setParamValues] = useState<Record<string, string>>(initialParams)

  const run = useCallback(async (values: Record<string, string>) => {
    setLoading(true); setError(null)
    try {
      const r = view.kind === 'saved_view'
        ? await api.executeView(view.payload.name, 1000, 0, values)
        : await api.executeViewInline(
            view.payload.view_yaml,
            view.payload.query_yaml ?? undefined,
            values,
          )
      setResult(r)
      // Surface schema defaults in the param bar on first render so the
      // user sees what's actually being applied.
      if (Object.keys(values).length === 0 && r.params?.length) {
        const defaults: Record<string, string> = {}
        for (const p of r.params) if (p.default != null) defaults[p.name] = p.default
        setParamValues((prev) => ({ ...defaults, ...prev }))
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [api, view])

  useEffect(() => { run(initialParams) /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [run])

  if (loading && !result) {
    return (
      <div className="rounded-md border bg-card p-3 my-2 flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Loading view…
      </div>
    )
  }
  if (error || !result) {
    return (
      <div className="rounded-md border border-red-200 bg-red-50 p-3 my-2 flex items-start gap-2 text-sm text-red-800">
        <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
        <div>{error ?? 'View execution failed.'}</div>
      </div>
    )
  }

  const viz = (result.visualization as Record<string, unknown>) || {}
  const rowDrilldown = viz.row_drilldown as ViewDrilldownConfig | undefined
  const appDrilldown = viz.app_drilldown as ViewDrilldownConfig | undefined

  return (
    <div className="rounded-md border bg-card p-4 my-2 space-y-3">
      {result.params && result.params.length > 0 && (
        <ViewParamBar
          params={result.params}
          values={paramValues}
          onChange={setParamValues}
          onApply={() => run(paramValues)}
        />
      )}
      <ViewContent
        result={result}
        rowDrilldown={rowDrilldown}
        appDrilldown={appDrilldown}
        paramValues={paramValues}
        onNavigate={(frame: DrillFrame) => navigate(frameToUrl(frame))}
      />
    </div>
  )
}

const PresentViewUI = makeAssistantToolUI({
  toolName: 'present_view',
  render: ChartCardRender,
  display: 'standalone',
})

const RunSavedViewUI = makeAssistantToolUI({
  toolName: 'run_saved_view',
  render: ChartCardRender,
  display: 'standalone',
})

// ---------------------------------------------------------------------------
// Thread — hand-rolled from primitives (no shadcn scaffold). Enough for
// the spike; Phase 3 can adopt the shadcn Thread if we decide it's worth
// the styling overhead.
// ---------------------------------------------------------------------------

function Thread() {
  return (
    <ThreadPrimitive.Root className="flex-1 flex flex-col min-h-0">
      <ThreadPrimitive.Viewport className="flex-1 overflow-y-auto px-6 py-6">
        <ThreadPrimitive.Empty>
          <div className="text-sm text-muted-foreground text-center py-12">
            Ask a question about your data to get started.
          </div>
        </ThreadPrimitive.Empty>

        <ThreadPrimitive.Messages
          components={{
            UserMessage,
            AssistantMessage,
          }}
        />
      </ThreadPrimitive.Viewport>

      <div className="border-t px-6 py-3">
        <Composer />
      </div>
    </ThreadPrimitive.Root>
  )
}

function UserMessage() {
  return (
    <MessagePrimitive.Root className="max-w-3xl mx-auto my-4 flex justify-end">
      <div className="rounded-lg bg-foreground text-background px-3 py-2 text-sm max-w-[80%]">
        <MessagePrimitive.Parts />
      </div>
    </MessagePrimitive.Root>
  )
}

function AssistantMessage() {
  return (
    <MessagePrimitive.Root className="max-w-3xl mx-auto my-4">
      <div className="text-sm text-foreground">
        <MessagePrimitive.Parts />
        <MessageErrorBanner />
      </div>
    </MessagePrimitive.Root>
  )
}

// RUN_ERROR events from the backend land on the current message as
// ``status.type === "incomplete" && reason === "error"`` with an
// ``error`` payload carrying the human-friendly message. Without this,
// the thread silently freezes — no text, no explanation of why the
// turn failed (out-of-credit, auth, provider down, etc.).
function MessageErrorBanner() {
  const message = useMessage((m) => m)
  const status = message?.status
  if (!status || status.type !== 'incomplete' || status.reason !== 'error') {
    return null
  }
  const err = status.error
  const text = typeof err === 'string'
    ? err
    : typeof err === 'object' && err !== null && 'message' in err
      ? String((err as Record<string, unknown>).message)
      : "The assistant couldn't complete this turn."
  return (
    <div className="mt-2 flex items-start gap-2 rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800">
      <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
      <div>{text}</div>
    </div>
  )
}

function Composer() {
  return (
    <ComposerPrimitive.Root className="max-w-3xl mx-auto flex items-end gap-2">
      <ComposerPrimitive.Input
        placeholder="Ask about your data…"
        className="flex-1 resize-none rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-foreground/20 min-h-[44px] max-h-40"
      />
      <ComposerPrimitive.Send className="rounded-md bg-foreground text-background text-sm px-4 py-2 hover:bg-foreground/90 disabled:opacity-50">
        Send
      </ComposerPrimitive.Send>
    </ComposerPrimitive.Root>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default function ChatPage() {
  const { model } = useModel()
  const chatEnabled = useChatEnabled()

  if (!model) return null
  if (chatEnabled === false) return <ChatDisabledEmpty />
  // While the capability probe is in flight, render nothing rather
  // than the enabled page — flashing the composer only to disable it
  // on the response would be worse than a brief blank.
  if (chatEnabled === null) return null

  return (
    <ChatRuntimeProvider model={model}>
      {/* The tool-UI registrations mount here so the runtime knows how
          to render matching tool-call parts before any message arrives. */}
      <PresentViewUI />
      <RunSavedViewUI />
      <div className="flex flex-col h-full">
        <Thread />
      </div>
    </ChatRuntimeProvider>
  )
}

// Probes GET /api/v1/config for the ``chat_enabled`` flag. Returns
// ``null`` while loading (so the caller can render a blank state
// instead of flashing UI). Failures fall back to ``true`` — a broken
// config endpoint shouldn't disable chat.
function useChatEnabled(): boolean | null {
  const [enabled, setEnabled] = useState<boolean | null>(null)
  useEffect(() => {
    let cancelled = false
    fetch('/api/v1/config')
      .then((r) => (r.ok ? r.json() : { chat_enabled: true }))
      .then((d) => { if (!cancelled) setEnabled(d.chat_enabled !== false) })
      .catch(() => { if (!cancelled) setEnabled(true) })
    return () => { cancelled = true }
  }, [])
  return enabled
}

function ChatDisabledEmpty() {
  return (
    <div className="flex-1 flex items-center justify-center p-8">
      <div className="max-w-md text-center space-y-2">
        <h2 className="text-lg font-semibold">Chat is disabled</h2>
        <p className="text-sm text-muted-foreground">
          The assistant surface is turned off on this deployment. Explore
          data via saved views and applications, or ask an admin to
          enable an LLM provider.
        </p>
      </div>
    </div>
  )
}
