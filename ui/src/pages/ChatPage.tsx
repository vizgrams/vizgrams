// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChatPage — natural-language data exploration (Epic 19 VG-206).
 *
 * Ask a question of the current model; the LLM authors a query, runs it,
 * picks a chart, and writes a caption. Follow-up questions drill in via
 * the previous turn's conversation context.
 *
 * State is ephemeral (Zustand-style local state only) — refresh clears
 * the chat. Persistence comes in a later phase.
 */

import { useEffect, useRef, useState } from 'react'
import { Loader2, Send } from 'lucide-react'

import type { ChatHistoryTurn, ChatResponse } from '@/api/client'
import { Card } from '@/components/Layout'
import { ChatTurnCard } from '@/components/chat/ChatTurnCard'
import { useModel } from '@/context/ModelContext'

interface AssistantTurn {
  role: 'assistant'
  response: ChatResponse
}

interface UserTurn {
  role: 'user'
  content: string
}

type Turn = UserTurn | AssistantTurn

export default function ChatPage() {
  const { api, model } = useModel()
  const [turns, setTurns] = useState<Turn[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [globalError, setGlobalError] = useState<string | null>(null)
  const scrollRef = useRef<HTMLDivElement | null>(null)

  // Reset the conversation when the model changes — a chat about one
  // model's data makes no sense if the user switches mid-stream.
  useEffect(() => {
    setTurns([])
    setInput('')
    setGlobalError(null)
  }, [model])

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [turns, busy])

  function buildHistoryForApi(): ChatHistoryTurn[] {
    return turns.map((t) =>
      t.role === 'user'
        ? { role: 'user', content: t.content }
        : { role: 'assistant', content: t.response.content || '' },
    )
  }

  async function handleSend() {
    const message = input.trim()
    if (!message || busy) return
    setGlobalError(null)
    setBusy(true)
    const history = buildHistoryForApi()
    setTurns((prev) => [...prev, { role: 'user', content: message }])
    setInput('')

    try {
      const response = await api.chatTurn(message, history)
      setTurns((prev) => [...prev, { role: 'assistant', response }])
    } catch (err) {
      // 503 (LLM unavailable) and other transport-level failures land here.
      const detail = err instanceof Error ? err.message : String(err)
      setGlobalError(detail)
    } finally {
      setBusy(false)
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  function handleClear() {
    setTurns([])
    setGlobalError(null)
  }

  return (
    <div className="flex flex-col h-full -mx-6 -my-6">
      {/* Header */}
      <div className="border-b px-6 py-3 flex items-center justify-between bg-card">
        <div>
          <h1 className="text-lg font-semibold">Explore</h1>
          <p className="text-xs text-muted-foreground">
            Ask questions in plain English; we'll author a query, run it, and chart the result.
          </p>
        </div>
        {turns.length > 0 && (
          <button
            type="button"
            onClick={handleClear}
            className="text-xs text-muted-foreground hover:text-foreground"
          >
            Clear conversation
          </button>
        )}
      </div>

      {/* Message stream */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {turns.length === 0 && !busy && (
          <EmptyState model={model} />
        )}

        {turns.map((t, i) =>
          t.role === 'user' ? (
            <UserBubble key={i} content={t.content} />
          ) : (
            <ChatTurnCard key={i} response={t.response} />
          ),
        )}

        {busy && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Authoring query…
          </div>
        )}

        {globalError && (
          <Card>
            <div className="text-sm text-destructive">
              <strong>Request failed:</strong> {globalError}
            </div>
          </Card>
        )}
      </div>

      {/* Input */}
      <div className="border-t px-6 py-3 bg-card">
        <div className="flex gap-2 items-end max-w-4xl mx-auto">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={busy}
            placeholder={
              turns.length === 0
                ? `Ask anything about ${model}…  (Shift+Enter for newline)`
                : 'Ask a follow-up, or drill into the last result…'
            }
            rows={Math.min(4, Math.max(1, input.split('\n').length))}
            className="flex-1 resize-none rounded-md border bg-background px-3 py-2 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
          />
          <button
            type="button"
            onClick={handleSend}
            disabled={busy || !input.trim()}
            className="rounded-md bg-primary text-primary-foreground px-3 py-2 text-sm font-medium disabled:opacity-50 hover:bg-primary/90"
            aria-label="Send"
          >
            <Send className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function UserBubble({ content }: { content: string }) {
  return (
    <div className="flex justify-end">
      <div className="max-w-2xl bg-primary text-primary-foreground rounded-2xl px-4 py-2 text-sm whitespace-pre-wrap">
        {content}
      </div>
    </div>
  )
}

function EmptyState({ model }: { model: string }) {
  return (
    <div className="max-w-2xl mx-auto py-12 text-center space-y-3">
      <p className="text-base font-medium">Ask anything about the <code className="text-primary">{model}</code> data.</p>
      <p className="text-sm text-muted-foreground">Try:</p>
      <ul className="text-sm text-muted-foreground space-y-1.5">
        <li>"How many pull requests are in the system?"</li>
        <li>"Show me the top 10 PR authors by count"</li>
        <li>"Throughput per month over the last year"</li>
      </ul>
    </div>
  )
}
