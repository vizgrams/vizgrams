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

import type { ChatHistoryTurn, ChatResponse, EntitySummary, ViewSummary } from '@/api/client'
import { Card } from '@/components/Layout'
import { ChatViewCard } from '@/components/chat/ChatViewCard'
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

// Per-model sessionStorage key. Session-scoped so the chat survives
// navigation into /explore (drilldown round-trips) and back, but is cleared
// when the tab closes — chat state isn't meant to outlive the browser session.
function storageKey(model: string) {
  return `vizgrams:chat:turns:${model}`
}

function loadTurns(model: string): Turn[] {
  if (!model) return []
  try {
    const raw = sessionStorage.getItem(storageKey(model))
    if (!raw) return []
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? (parsed as Turn[]) : []
  } catch {
    return []
  }
}

export default function ChatPage() {
  const { api, model } = useModel()
  const [turns, setTurns] = useState<Turn[]>(() => loadTurns(model))
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [globalError, setGlobalError] = useState<string | null>(null)
  const [suggestions, setSuggestions] = useState<string[]>([])
  const scrollRef = useRef<HTMLDivElement | null>(null)

  // Switching models loads that model's chat (or starts fresh). Each model
  // gets its own session-scoped buffer so a chat about openflights doesn't
  // leak into iagai.
  useEffect(() => {
    setTurns(loadTurns(model))
    setInput('')
    setGlobalError(null)
  }, [model])

  // Persist turns whenever they change. Skipping when empty keeps the empty
  // state clean (no stray storage entries for models never chatted with).
  useEffect(() => {
    if (!model) return
    if (turns.length === 0) {
      sessionStorage.removeItem(storageKey(model))
    } else {
      try {
        sessionStorage.setItem(storageKey(model), JSON.stringify(turns))
      } catch {
        // Quota or serialization issues — degrade silently; chat still works
        // in-memory, you just lose round-trip persistence for this turn.
      }
    }
  }, [model, turns])

  // Build empty-state suggestion prompts from the model's saved views.
  // Falls back to entity-based prompts when the model has no views yet.
  // Per-model so the chips read naturally on openflights / crypto / etc.
  // instead of hard-coded PR-throughput examples.
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
        // Suggestions are a nice-to-have; failures shouldn't break the page.
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

  async function handleSend(messageOverride?: string) {
    const message = (messageOverride ?? input).trim()
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
// Suggestion derivation — turn the model's catalog into natural prompts
//
// Views are preferred (they're user-facing and well-named). Falls back to
// entities when the model has none yet. The prompts read as if a user
// typed them, so clicking a chip behaves indistinguishably from typing.
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
  // PullRequest → "pull request"; DORAMetric → "dora metric"; URL → "url"
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
