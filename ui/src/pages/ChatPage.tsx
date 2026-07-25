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

import { useMemo } from 'react'
import { HttpAgent } from '@ag-ui/client'
import {
  AssistantRuntimeProvider,
  ThreadPrimitive,
  MessagePrimitive,
  ComposerPrimitive,
  makeAssistantToolUI,
  type ToolCallMessagePartProps,
} from '@assistant-ui/react'
import { useAgUiRuntime } from '@assistant-ui/react-ag-ui'

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

type ViewPayload = {
  kind: 'inline_view' | 'saved_view'
  payload: Record<string, unknown>
}

function ChartCardRender({ result }: ToolCallMessagePartProps<unknown, string>) {
  const parsed = useMemo<ViewPayload | null>(() => {
    if (typeof result !== 'string') return null
    try {
      return JSON.parse(result) as ViewPayload
    } catch {
      return null
    }
  }, [result])

  if (!parsed) {
    return (
      <div className="rounded-md border bg-muted/30 p-3 my-2 text-xs text-muted-foreground font-mono">
        (rendering pending)
      </div>
    )
  }

  // Spike placeholder — Phase 3 replaces this with the real ChatViewCard
  // (chart, params, drilldowns, publish). Rendering the payload as JSON
  // proves the makeAssistantToolUI wire works end-to-end.
  return (
    <div className="rounded-md border bg-card p-4 my-2">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2">
        {parsed.kind === 'saved_view' ? 'saved view' : 'inline view'}
      </div>
      <pre className="text-xs overflow-x-auto bg-muted/50 p-3 rounded font-mono">
        {JSON.stringify(parsed.payload, null, 2)}
      </pre>
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
      </div>
    </MessagePrimitive.Root>
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
  if (!model) return null
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
