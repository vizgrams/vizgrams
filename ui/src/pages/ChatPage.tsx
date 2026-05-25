// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChatPage — natural-language data exploration (Epic 19 VG-206; persisted
 * across sessions in Epic 25 VG-282).
 *
 * Sidebar = the user's past chat sessions (server-backed via
 * /api/v1/model/{m}/chat/sessions). Right pane = the active conversation.
 * Selecting a session re-hydrates the transcript from the server.
 *
 * Persistence sources (in priority order):
 *   1. Server (the source of truth — survives across tabs / devices)
 *   2. sessionStorage of the current session id only (so a tab refresh
 *      mid-conversation lands you back where you were without an extra
 *      click on the sidebar)
 *
 * The full turn list used to live in sessionStorage too; that was a
 * pre-VG-280 fallback. Now the server has it, so we just store the id.
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { Loader2, Send } from 'lucide-react'

import type {
  ChatHistoryTurn, ChatResponse, ChatTurnPersisted,
  EntitySummary, ViewSummary,
} from '@/api/client'
import { Card } from '@/components/Layout'
import { ChatViewCard } from '@/components/chat/ChatViewCard'
import { ChatHistorySidebar } from '@/components/chat/ChatHistorySidebar'
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

// Per-model sessionStorage key — only stores the active session id now
// (server holds the actual turn data). Lets a tab refresh land you back
// in the right session without a sidebar click.
function activeSessionKey(model: string) {
  return `vizgrams:chat:active_session:${model}`
}

export default function ChatPage() {
  const { api, model } = useModel()
  const [turns, setTurns] = useState<Turn[]>([])
  const [sessionId, setSessionId] = useState<string | null>(() => {
    return model ? sessionStorage.getItem(activeSessionKey(model)) : null
  })
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [globalError, setGlobalError] = useState<string | null>(null)
  const [suggestions, setSuggestions] = useState<string[]>([])
  // Bumped after each turn so the sidebar re-fetches and the just-touched
  // session bubbles to the top of the list.
  const [sidebarRefreshKey, setSidebarRefreshKey] = useState(0)
  const scrollRef = useRef<HTMLDivElement | null>(null)

  // Load the active session's turns whenever the session id changes
  // (sidebar click, tab refresh with stored id, model switch reading
  // the model's last active session).
  useEffect(() => {
    if (!sessionId) {
      setTurns([])
      return
    }
    let cancelled = false
    api.chatSessions.get(sessionId).then((detail) => {
      if (cancelled) return
      setTurns(hydrateTurns(detail.turns))
    }).catch(() => {
      // Stale id (deleted on server, or different user) — silently fall
      // through to a fresh chat. The persistTurn fallback in the server
      // will give us a new session id on the next send.
      if (!cancelled) {
        setSessionId(null)
        setTurns([])
      }
    })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId])

  // Switch models → load THAT model's last-active session if there is one,
  // otherwise show empty state.
  useEffect(() => {
    if (!model) return
    const stored = sessionStorage.getItem(activeSessionKey(model))
    setSessionId(stored)
    setInput('')
    setGlobalError(null)
  }, [model])

  // Persist the active session id whenever it changes (per-model key).
  useEffect(() => {
    if (!model) return
    if (sessionId) sessionStorage.setItem(activeSessionKey(model), sessionId)
    else sessionStorage.removeItem(activeSessionKey(model))
  }, [model, sessionId])

  // Empty-state prompt chips — derived from the model's saved views.
  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const views = await api.listViews()
        if (cancelled) return
        if (views.length > 0) {
          setSuggestions(viewsToPrompts(views))
          return
        }
        const entities = await api.listEntities()
        if (cancelled) return
        setSuggestions(entitiesToPrompts(entities))
      } catch {
        if (!cancelled) setSuggestions([])
      }
    }
    load()
    return () => { cancelled = true }
  }, [model, api])

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [turns, busy])

  function buildHistoryForApi(): ChatHistoryTurn[] {
    return turns.map((t) => {
      if (t.role === 'user') {
        return { role: 'user' as const, content: t.content }
      }
      // No caption in the response (VG-237). Synthesise a short
      // assistant memory so follow-ups ("now break that by team") have
      // an anchor.
      const memory = t.response.saved_view
        ? `(showed saved view: ${t.response.saved_view.name})`
        : t.response.inline_view
        ? `(generated a view from your last question)`
        : `(no view returned)`
      return { role: 'assistant' as const, content: memory }
    })
  }

  const handleSend = useCallback(async (messageOverride?: string) => {
    const message = (messageOverride ?? input).trim()
    if (!message || busy) return
    setGlobalError(null)
    setBusy(true)
    const history = buildHistoryForApi()
    setTurns((prev) => [...prev, { role: 'user', content: message }])
    setInput('')

    try {
      const response = await api.chatTurn(message, history, sessionId)
      setTurns((prev) => [...prev, { role: 'assistant', response }])
      // Server may have created a fresh session (first turn) or returned
      // the one we passed in. Either way: thread the id back through.
      if (response.session_id && response.session_id !== sessionId) {
        setSessionId(response.session_id)
      }
      // Bump the sidebar so it re-fetches with the just-touched session
      // at the top.
      setSidebarRefreshKey((k) => k + 1)
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err)
      setGlobalError(detail)
    } finally {
      setBusy(false)
    }
    // turns is intentionally not a dep — we rebuild history from
    // current turns each call, but stale closure on `turns` would only
    // affect the in-flight message which we just appended above.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, busy, input, sessionId])

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  function handleNewChat() {
    // Drop session id → empty turns. Next send creates a fresh server
    // session and we'll get the id back on the response.
    setSessionId(null)
    setTurns([])
    setGlobalError(null)
    setInput('')
  }

  function handleSelectSession(id: string) {
    if (id === sessionId) return
    setSessionId(id)
    setGlobalError(null)
  }

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      <ChatHistorySidebar
        currentSessionId={sessionId}
        onSelect={handleSelectSession}
        onNewChat={handleNewChat}
        refreshKey={sidebarRefreshKey}
      />

      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {/* Header */}
        <div className="border-b px-6 py-3 flex items-center justify-between bg-card shrink-0">
          <div>
            <h1 className="text-lg font-semibold">Chat</h1>
            <p className="text-xs text-muted-foreground">
              Ask questions in plain English; we'll author a query, run it, and chart the result.
            </p>
          </div>
        </div>

        {/* Message stream */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
          {turns.length === 0 && !busy && (
            <EmptyState
              model={model}
              suggestions={suggestions}
              onSelect={(prompt) => handleSend(prompt)}
            />
          )}

          {turns.map((t, i) =>
            t.role === 'user' ? (
              <UserBubble key={i} content={t.content} />
            ) : (
              <ChatViewCard key={i} response={t.response} />
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
        <div className="border-t px-6 py-3 bg-card shrink-0">
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
              onClick={() => handleSend()}
              disabled={busy || !input.trim()}
              className="rounded-md bg-primary text-primary-foreground px-3 py-2 text-sm font-medium disabled:opacity-50 hover:bg-primary/90"
              aria-label="Send"
            >
              <Send className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Re-hydrate server-side turn rows into the local Turn shape used by the
// renderer. User rows become UserTurn; assistant rows wrap the stored
// ChatResponse JSON.
// ---------------------------------------------------------------------------

function hydrateTurns(persisted: ChatTurnPersisted[]): Turn[] {
  const out: Turn[] = []
  for (const t of persisted) {
    if (t.role === 'user') {
      out.push({ role: 'user', content: t.content ?? '' })
    } else if (t.role === 'assistant' && t.response) {
      out.push({ role: 'assistant', response: t.response })
    }
    // Assistant turns without response_json (very old / mid-failed) are
    // dropped — nothing to render. Rare edge case.
  }
  return out
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

interface EmptyStateProps {
  model: string
  suggestions: string[]
  onSelect: (prompt: string) => void
}

function EmptyState({ model, suggestions, onSelect }: EmptyStateProps) {
  return (
    <div className="max-w-2xl mx-auto py-12 text-center space-y-4">
      <p className="text-base font-medium">
        Ask anything about the <code className="text-primary">{model}</code> data.
      </p>
      {suggestions.length > 0 && (
        <>
          <p className="text-sm text-muted-foreground">Try:</p>
          <div className="flex flex-wrap gap-2 justify-center">
            {suggestions.map((prompt) => (
              <button
                key={prompt}
                type="button"
                onClick={() => onSelect(prompt)}
                className="text-sm px-3 py-1.5 rounded-full border bg-card hover:bg-muted/50 hover:border-primary/40 transition-colors"
              >
                {prompt}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Suggestion derivation
// ---------------------------------------------------------------------------

const SUGGESTION_CAP = 5

function viewsToPrompts(views: ViewSummary[]): string[] {
  return views
    .slice(0, SUGGESTION_CAP)
    .map((v) => `Show me ${snakeToWords(v.name)}`)
}

function entitiesToPrompts(entities: EntitySummary[]): string[] {
  const out: string[] = []
  if (entities[0]) {
    out.push(`How many ${pluraliseLastWord(pascalToWords(entities[0].name))} are there?`)
  }
  if (entities[1]) {
    out.push(`Show me top ${pluraliseLastWord(pascalToWords(entities[1].name))} by count`)
  }
  if (entities[2]) {
    out.push(`Tell me about ${pascalToWords(entities[2].name)}`)
  }
  return out
}

function snakeToWords(s: string): string {
  return s.replace(/_/g, ' ').toLowerCase()
}

function pascalToWords(s: string): string {
  return s
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .toLowerCase()
}

function pluraliseLastWord(phrase: string): string {
  const words = phrase.split(/\s+/)
  if (words.length === 0) return phrase
  const last = words[words.length - 1]
  if (last.endsWith('y') && !/[aeiou]y$/.test(last)) {
    words[words.length - 1] = last.slice(0, -1) + 'ies'
  } else if (!last.endsWith('s')) {
    words[words.length - 1] = last + 's'
  }
  return words.join(' ')
}
