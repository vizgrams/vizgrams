// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * NotificationBell — shows unresolved proposal notifications for the
 * current user (Epic 26 VG-296). Lives in the sidebar bottom utilities.
 *
 * Poll cadence: count refreshes every 30s, full list lazily on click.
 * Each item links to the proposal's home (the entity's Activity tab so
 * the user lands directly on the pending changes section).
 */

import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { Bell } from 'lucide-react'

import { countMyNotifications, listMyNotifications, type NotificationOut } from '@/api/client'
import { cn } from '@/lib/utils'

const POLL_INTERVAL_MS = 30_000

interface Props {
  collapsed: boolean
  dark?: boolean
}

export function NotificationBell({ collapsed, dark }: Props) {
  const [count, setCount] = useState(0)
  const [open, setOpen] = useState(false)
  const [items, setItems] = useState<NotificationOut[]>([])
  const [loading, setLoading] = useState(false)
  const popRef = useRef<HTMLDivElement>(null)

  // Poll the badge count on a slow cadence. Failures are silent — bell
  // is a non-critical surface and we don't want auth blips to spam.
  useEffect(() => {
    let cancelled = false
    async function refresh() {
      try {
        const r = await countMyNotifications()
        if (!cancelled) setCount(r.count)
      } catch {
        if (!cancelled) setCount(0)
      }
    }
    refresh()
    const t = setInterval(refresh, POLL_INTERVAL_MS)
    return () => { cancelled = true; clearInterval(t) }
  }, [])

  // Click-outside to close. The popover is fixed-positioned so we
  // anchor by the button below.
  useEffect(() => {
    if (!open) return
    function onClick(e: MouseEvent) {
      if (!popRef.current?.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onClick)
    return () => document.removeEventListener('mousedown', onClick)
  }, [open])

  async function toggle() {
    if (open) {
      setOpen(false)
      return
    }
    setOpen(true)
    setLoading(true)
    try {
      const rows = await listMyNotifications()
      setItems(rows)
    } catch {
      setItems([])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="relative" ref={popRef}>
      <button
        onClick={toggle}
        title={count > 0 ? `${count} pending change${count === 1 ? '' : 's'}` : 'No pending changes'}
        className={cn(
          'flex items-center rounded-md min-w-0 transition-colors w-full',
          collapsed ? 'justify-center p-2' : 'gap-2 px-2 py-1.5',
          dark
            ? 'text-white/55 hover:bg-white/[0.07] hover:text-white/90'
            : 'text-muted-foreground hover:bg-muted hover:text-foreground',
        )}
      >
        <div className="relative shrink-0">
          <Bell className="h-3.5 w-3.5" />
          {count > 0 && (
            <span
              className={cn(
                'absolute -top-1.5 -right-1.5 text-[9px] font-semibold rounded-full px-1 min-w-[14px] h-[14px] flex items-center justify-center',
                'bg-amber-500 text-white',
              )}
            >
              {count > 99 ? '99+' : count}
            </span>
          )}
        </div>
        {!collapsed && <span className="truncate text-xs">Notifications</span>}
      </button>

      {open && (
        <div
          className={cn(
            'fixed z-50 w-80 rounded-md border bg-card shadow-xl p-2',
            'bottom-16 left-2',
          )}
        >
          <div className="flex items-center justify-between px-2 py-1.5 border-b mb-1">
            <span className="text-xs font-medium">Pending changes</span>
            <span className="text-[10px] text-muted-foreground/70">{count}</span>
          </div>
          {loading
            ? <p className="text-xs text-muted-foreground p-3 text-center">Loading…</p>
            : items.length === 0
            ? <p className="text-xs text-muted-foreground p-3 text-center">No pending changes for you.</p>
            : (
              <ul className="max-h-80 overflow-y-auto">
                {items.map((n) => (
                  <li key={n.id} className="border-b last:border-b-0">
                    <Link
                      to={proposalLink(n)}
                      onClick={() => setOpen(false)}
                      className="block p-2 hover:bg-muted/40 transition-colors"
                    >
                      <div className="text-[10px] uppercase tracking-wider text-amber-700 dark:text-amber-400">
                        proposed · {n.artifact_kind?.replace('_', ' ') ?? '—'}
                      </div>
                      <div className="text-xs mt-0.5">
                        <span className="font-medium">{n.proposed_by ?? 'someone'}</span>{' '}
                        <span className="text-muted-foreground">→</span>{' '}
                        <span className="font-mono">{n.artifact_name ?? '—'}</span>
                      </div>
                      {n.reason && (
                        <div className="text-[10px] text-muted-foreground/70 mt-0.5 line-clamp-2">"{n.reason}"</div>
                      )}
                    </Link>
                  </li>
                ))}
              </ul>
            )
          }
        </div>
      )}
    </div>
  )
}

// Where does clicking a notification take you? The entity's Activity
// tab — pending section sits at the top so the reviewer lands right
// where they need to be. Falls back to /explore for cross-entity
// proposals (e.g. extractor changes).
function proposalLink(n: NotificationOut): string {
  if (n.entity_name) {
    return `/explore?entity=${encodeURIComponent(n.entity_name)}&tab=activity`
  }
  return '/explore?tab=activity'
}
