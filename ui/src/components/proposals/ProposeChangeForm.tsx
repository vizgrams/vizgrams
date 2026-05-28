// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ProposeChangeForm — inline form for members proposing a change to a
 * governed artifact (Epic 26 VG-296).
 *
 * Shape: Current ↔ Proposed side-by-side + required Reason field +
 * "Reviewers" line showing who'll get notified. Submits to
 * POST /api/v1/model/{m}/proposals.
 *
 * The "owner + admins" recipient list is computed server-side from the
 * artifact's last-touched-by; we display "owner + admins" as a soft
 * preview without doing the resolution ourselves.
 */

import { useState } from 'react'
import { Pencil, X } from 'lucide-react'

import type { ProposalCreate, ProposalKind } from '@/api/client'
import { useModel } from '@/context/ModelContext'

interface Props {
  artifactKind: ProposalKind
  artifactName: string
  entityName?: string | null
  current: string
  onClose: () => void
  onSubmitted?: () => void  // refresh-trigger for parent
}

export function ProposeChangeForm({
  artifactKind, artifactName, entityName, current, onClose, onSubmitted,
}: Props) {
  const { api } = useModel()
  const [proposed, setProposed] = useState(current)
  const [reason, setReason] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit() {
    if (!reason.trim()) {
      setError('Reason is required')
      return
    }
    setSubmitting(true)
    setError(null)
    const body: ProposalCreate = {
      artifact_kind: artifactKind,
      artifact_name: artifactName,
      entity_name: entityName ?? null,
      reason,
      before_yaml: current,
      after_yaml: proposed,
    }
    try {
      await api.createProposal(body)
      onSubmitted?.()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Submit failed')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="my-2 rounded border border-amber-500/40 bg-amber-50/30 dark:bg-amber-950/10 p-3 space-y-3">
      <div className="flex items-center justify-between">
        <div className="inline-flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-amber-700 dark:text-amber-400">
          <Pencil className="h-3 w-3" /> Propose change to {artifactName}
        </div>
        <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground/70 block mb-1">Current</label>
          <div className="text-xs rounded border bg-card px-2.5 py-2 font-mono text-muted-foreground/80 break-all">
            {current || '—'}
          </div>
        </div>
        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground/70 block mb-1">Proposed</label>
          <input
            value={proposed}
            onChange={(e) => setProposed(e.target.value)}
            className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono"
          />
        </div>
      </div>

      <div>
        <label className="text-[10px] uppercase tracking-wider text-muted-foreground/70 block mb-1">
          Reason <span className="text-amber-700 dark:text-amber-400 normal-case tracking-normal">· required</span>
        </label>
        <textarea
          value={reason}
          onChange={(e) => setReason(e.target.value)}
          rows={2}
          placeholder="e.g. add a draft state so in-progress PRs aren't conflated with open ones"
          className="w-full text-xs bg-background border rounded px-2.5 py-2 placeholder:text-muted-foreground/50"
        />
      </div>

      <div className="text-[10px] text-muted-foreground/70">
        Reviewers: <span className="font-medium">owner</span> · <span className="font-medium">admins</span>
      </div>

      {error && <p className="text-[11px] text-red-600">{error}</p>}

      <div className="flex justify-end gap-2">
        <button
          onClick={onClose}
          className="text-xs text-muted-foreground hover:text-foreground px-3 py-1.5"
        >
          Cancel
        </button>
        <button
          onClick={submit}
          disabled={submitting}
          className="text-xs px-3 py-1.5 rounded border border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-400 hover:bg-amber-500/20 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {submitting ? 'Submitting…' : 'Propose change'}
        </button>
      </div>
    </div>
  )
}
