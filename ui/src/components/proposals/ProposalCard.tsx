// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ProposalCard — visualizes one pending proposal with side-by-side diff +
 * approve/reject actions (Epic 26 VG-296). Used by the "Pending changes"
 * section at the top of the Activity tab.
 */

import { useState } from 'react'
import { Pencil, X } from 'lucide-react'

import { approveProposal, rejectProposal, type Proposal } from '@/api/client'

interface Props {
  proposal: Proposal
  onDecided?: () => void  // parent refetches the pending list
}

export function ProposalCard({ proposal: p, onDecided }: Props) {
  const [busy, setBusy] = useState<'approve' | 'reject' | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [rejectingComment, setRejectingComment] = useState<string | null>(null)

  async function approve() {
    setBusy('approve')
    setError(null)
    try {
      await approveProposal(p.id)
      onDecided?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Approve failed')
    } finally {
      setBusy(null)
    }
  }

  async function reject() {
    if (rejectingComment === null) {
      // First click reveals the comment field; second click submits.
      setRejectingComment('')
      return
    }
    if (!rejectingComment.trim()) {
      setError('A reason is required to reject')
      return
    }
    setBusy('reject')
    setError(null)
    try {
      await rejectProposal(p.id, rejectingComment.trim())
      onDecided?.()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Reject failed')
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="rounded border border-amber-500/40 bg-amber-50/30 dark:bg-amber-950/10">
      <div className="flex items-center justify-between gap-3 px-3 py-2 border-b border-amber-500/30">
        <div className="flex items-center gap-2 min-w-0">
          <span className="shrink-0 inline-flex items-center gap-1 text-[10px] uppercase tracking-wider rounded px-1.5 py-0.5 border border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-400">
            <Pencil className="h-2.5 w-2.5" />
            proposed · {p.artifact_kind.replace('_', ' ')}
          </span>
          <span className="text-xs truncate">
            <span className="font-medium">{p.proposed_by}</span>{' '}
            <span className="text-muted-foreground">wants to change</span>{' '}
            <span className="font-mono">{p.artifact_name}</span>
          </span>
        </div>
        <span className="text-[10px] text-muted-foreground/70 shrink-0">{p.created_at}</span>
      </div>

      <div className="px-3 py-3 space-y-2">
        <div className="text-xs italic text-muted-foreground/90 leading-snug">
          "{p.reason}"
        </div>

        <div className="grid grid-cols-2 gap-2 mt-2">
          <div>
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70 mb-1">Current</div>
            <div className="text-xs rounded border bg-card px-2 py-1.5 font-mono text-muted-foreground/80 line-through decoration-amber-500/50 break-all">
              {p.before_yaml || '—'}
            </div>
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70 mb-1">Proposed</div>
            <div className="text-xs rounded border bg-card px-2 py-1.5 font-mono text-amber-700 dark:text-amber-400 break-all">
              {p.after_yaml || '—'}
            </div>
          </div>
        </div>

        <div className="text-[10px] text-muted-foreground/70">
          Reviewers: {p.notified_to.length === 0
            ? <span className="italic">none</span>
            : p.notified_to.map((r, i) => (
                <span key={i}>
                  <span className="font-medium">{r}</span>
                  {i < p.notified_to.length - 1 && ' · '}
                </span>
              ))
          }
        </div>

        {rejectingComment !== null && (
          <textarea
            value={rejectingComment}
            onChange={(e) => setRejectingComment(e.target.value)}
            placeholder="reason for rejecting (required)"
            rows={2}
            className="w-full text-xs bg-background border rounded px-2.5 py-2 placeholder:text-muted-foreground/50"
          />
        )}

        {error && <p className="text-[11px] text-red-600">{error}</p>}

        <div className="flex items-center justify-end gap-1 pt-1">
          {rejectingComment !== null && (
            <button
              onClick={() => { setRejectingComment(null); setError(null) }}
              className="text-[11px] text-muted-foreground hover:text-foreground px-2 py-1"
            >
              cancel
            </button>
          )}
          <button
            onClick={reject}
            disabled={busy !== null}
            className="text-xs text-muted-foreground hover:text-foreground inline-flex items-center gap-0.5 px-2 py-1 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <X className="h-3 w-3" /> {busy === 'reject' ? 'Rejecting…' : rejectingComment !== null ? 'Submit rejection' : 'Reject'}
          </button>
          {rejectingComment === null && (
            <button
              onClick={approve}
              disabled={busy !== null}
              className="text-xs px-3 py-1 rounded border border-amber-500/40 bg-amber-500 text-white hover:bg-amber-600 inline-flex items-center gap-1 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              ✓ {busy === 'approve' ? 'Approving…' : 'Approve'}
            </button>
          )}
        </div>
      </div>
    </div>
  )
}
