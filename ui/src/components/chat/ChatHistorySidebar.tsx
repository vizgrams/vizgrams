// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ChatHistorySidebar — list of past chat sessions on /chat (VG-282).
 *
 * Owner-scoped + per-model (the backend already filters); click a
 * session to resume, hover for rename / delete actions. Auto-refresh
 * after each new turn so the active session bubbles to the top.
 */

import { useEffect, useState } from 'react'
import { Loader2, MoreVertical, Pencil, Plus, Trash2 } from 'lucide-react'
import { formatDistanceToNow } from 'date-fns'

import type { ChatSessionSummary } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { cn } from '@/lib/utils'

interface Props {
  // Which session is currently open in the right pane. Null = fresh chat
  // not yet persisted (no turns sent), or the current session got deleted.
  currentSessionId: string | null
  onSelect: (sessionId: string) => void
  onNewChat: () => void
  // Bumped by the parent after each turn so the sidebar refreshes its
  // ordering (the just-touched session should be on top).
  refreshKey: number
}

export function ChatHistorySidebar({
  currentSessionId, onSelect, onNewChat, refreshKey,
}: Props) {
  const { api, model } = useModel()
  const [sessions, setSessions] = useState<ChatSessionSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  // Per-row local state for the rename inline editor + delete confirmation.
  const [renamingId, setRenamingId] = useState<string | null>(null)
  const [renameDraft, setRenameDraft] = useState('')

  useEffect(() => {
    let cancelled = false
    setLoading(true); setError(null)
    api.chatSessions.list()
      .then((rows) => { if (!cancelled) setSessions(rows) })
      .catch((e) => { if (!cancelled) setError(String(e)) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
    // Model change → different per-model history; refreshKey bump → re-fetch
    // after a turn lands so ordering + new sessions appear.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, refreshKey])

  async function handleDelete(id: string) {
    // Don't ask for confirmation — soft-delete + can re-open from URL if
    // we surface that later. For now, deletion is undoable only via the
    // backend (ended_at flag, not a hard delete).
    const before = sessions
    setSessions((prev) => prev.filter((s) => s.id !== id))
    try {
      await api.chatSessions.delete(id)
      if (id === currentSessionId) onNewChat()
    } catch {
      // Revert on failure — the session is still there server-side.
      setSessions(before)
    }
  }

  async function handleRename(id: string) {
    const title = renameDraft.trim()
    if (!title) { setRenamingId(null); return }
    try {
      const updated = await api.chatSessions.rename(id, title)
      setSessions((prev) => prev.map((s) => s.id === id ? updated : s))
    } catch {
      // Silent failure — fall through to closing the editor without changes
    } finally {
      setRenamingId(null)
      setRenameDraft('')
    }
  }

  return (
    <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
      <div className="px-3 py-3 border-b shrink-0">
        <button
          type="button"
          onClick={onNewChat}
          className="w-full flex items-center justify-center gap-1.5 text-xs font-medium border rounded-md py-1.5 hover:bg-muted transition-colors"
        >
          <Plus className="h-3.5 w-3.5" />
          New chat
        </button>
      </div>

      <div className="flex-1 overflow-y-auto py-1">
        {loading && (
          <div className="px-3 py-4 flex items-center gap-2 text-xs text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" />
            Loading…
          </div>
        )}
        {error && (
          <p className="px-3 py-2 text-xs text-destructive">{error}</p>
        )}
        {!loading && !error && sessions.length === 0 && (
          <p className="px-3 py-4 text-xs text-muted-foreground">
            No past chats yet. Start one with the input on the right.
          </p>
        )}
        {sessions.map((s) => {
          const active = s.id === currentSessionId
          const isRenaming = renamingId === s.id
          return (
            <div
              key={s.id}
              className={cn(
                'group relative border-b border-border/30 last:border-0',
                active && 'bg-muted',
              )}
            >
              {isRenaming ? (
                <div className="px-3 py-2">
                  <input
                    autoFocus
                    value={renameDraft}
                    onChange={(e) => setRenameDraft(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') handleRename(s.id)
                      if (e.key === 'Escape') { setRenamingId(null); setRenameDraft('') }
                    }}
                    onBlur={() => handleRename(s.id)}
                    className="w-full text-xs rounded border bg-background px-2 py-1 focus:outline-none focus:ring-1 focus:ring-ring"
                  />
                </div>
              ) : (
                <button
                  type="button"
                  onClick={() => onSelect(s.id)}
                  title={s.title ?? 'Untitled'}
                  className={cn(
                    'w-full text-left px-3 py-2 transition-colors',
                    active
                      ? 'text-foreground'
                      : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground',
                  )}
                >
                  <div className="text-xs line-clamp-2 break-words pr-6">
                    {s.title || <span className="italic opacity-60">Untitled</span>}
                  </div>
                  <div className="text-[10px] text-muted-foreground/60 mt-0.5">
                    {formatDistanceToNow(new Date(s.updated_at), { addSuffix: true })}
                  </div>
                </button>
              )}
              {!isRenaming && (
                <RowActions
                  onRename={() => { setRenamingId(s.id); setRenameDraft(s.title ?? '') }}
                  onDelete={() => handleDelete(s.id)}
                />
              )}
            </div>
          )
        })}
      </div>
    </aside>
  )
}

// Hover-revealed menu on each session row. Kept small + click-anywhere
// to close instead of a real popover library — fewer deps for what's
// really just two buttons.
function RowActions({ onRename, onDelete }: { onRename: () => void; onDelete: () => void }) {
  const [open, setOpen] = useState(false)
  useEffect(() => {
    if (!open) return
    const close = () => setOpen(false)
    document.addEventListener('click', close)
    return () => document.removeEventListener('click', close)
  }, [open])

  return (
    <div className="absolute right-2 top-2 opacity-0 group-hover:opacity-100 transition-opacity">
      <button
        type="button"
        onClick={(e) => { e.stopPropagation(); setOpen((o) => !o) }}
        className="rounded p-1 hover:bg-muted text-muted-foreground"
        title="More actions"
      >
        <MoreVertical className="h-3 w-3" />
      </button>
      {open && (
        <div
          className="absolute right-0 top-6 bg-popover border rounded-md shadow-md py-1 z-10 min-w-[100px]"
          onClick={(e) => e.stopPropagation()}
        >
          <button
            type="button"
            onClick={() => { setOpen(false); onRename() }}
            className="w-full text-left px-2.5 py-1 text-xs hover:bg-muted flex items-center gap-1.5"
          >
            <Pencil className="h-3 w-3" /> Rename
          </button>
          <button
            type="button"
            onClick={() => { setOpen(false); onDelete() }}
            className="w-full text-left px-2.5 py-1 text-xs text-destructive hover:bg-muted flex items-center gap-1.5"
          >
            <Trash2 className="h-3 w-3" /> Delete
          </button>
        </div>
      )}
    </div>
  )
}
