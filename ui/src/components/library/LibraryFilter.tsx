// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * LibraryFilter — three-chip selector for the library list pages.
 *
 *   Certified (default) — only artifacts marked is_certified=true
 *   All                 — everything
 *   Mine                — artifacts whose created_by === current user
 *
 * Used on /views, /queries, /features. Defaults to Certified so the
 * library stays clean: chat-spawned drafts and other users' experiments
 * don't pollute the catalog until someone vets them.
 *
 * "Mine" relies on VG-252 (created_by stamped on each save) and
 * VG-260's /me.user_id surface; falls back to disabled when the user
 * isn't authenticated.
 */

import { cn } from '@/lib/utils'
import type { LibraryFields } from '@/api/client'

export type LibraryFilterValue = 'certified' | 'all' | 'mine'

interface Props {
  value: LibraryFilterValue
  onChange: (v: LibraryFilterValue) => void
  // null when the user isn't authenticated yet — disables "Mine".
  currentUserId: string | null
  // For status display (e.g. "12 of 30"). Optional.
  matchCount?: number
  totalCount?: number
}

export function LibraryFilter({ value, onChange, currentUserId, matchCount, totalCount }: Props) {
  const chips: { id: LibraryFilterValue; label: string; disabled?: boolean; title?: string }[] = [
    { id: 'certified', label: 'Certified' },
    { id: 'all',       label: 'All' },
    {
      id: 'mine',
      label: 'Mine',
      disabled: !currentUserId,
      title: currentUserId ? undefined : 'Sign in to see your own artifacts',
    },
  ]

  return (
    <div className="flex items-center gap-1.5">
      <div className="inline-flex rounded-md border bg-card text-xs overflow-hidden">
        {chips.map((c) => (
          <button
            key={c.id}
            type="button"
            onClick={() => !c.disabled && onChange(c.id)}
            disabled={c.disabled}
            title={c.title}
            className={cn(
              'px-2.5 py-1 transition-colors border-r last:border-r-0',
              value === c.id
                ? 'bg-primary text-primary-foreground font-medium'
                : 'text-muted-foreground hover:text-foreground hover:bg-muted/60',
              c.disabled && 'opacity-40 cursor-not-allowed hover:bg-transparent hover:text-muted-foreground',
            )}
          >
            {c.label}
          </button>
        ))}
      </div>
      {matchCount != null && totalCount != null && matchCount !== totalCount && (
        <span className="text-[10px] text-muted-foreground tabular-nums">
          {matchCount} of {totalCount}
        </span>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Filter predicate — pure, easy to test.
// ---------------------------------------------------------------------------

/** Apply the library filter to an artifact list. */
export function filterByLibrary<T extends LibraryFields>(
  items: T[],
  filter: LibraryFilterValue,
  currentUserId: string | null,
): T[] {
  switch (filter) {
    case 'certified':
      return items.filter((i) => i.is_certified === true)
    case 'mine':
      // No user → no matches. Better than silently showing everyone's
      // work (which is what an empty-string compare would produce).
      if (!currentUserId) return []
      return items.filter((i) => i.created_by === currentUserId)
    case 'all':
    default:
      return items
  }
}
