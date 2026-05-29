// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * SchemaAddPanel — propose a new attribute or relation (Epic 27 VG-306).
 *
 * Attributes and relations live inside the entity YAML rather than as
 * standalone artifacts, so "adding" one means proposing a new entry to
 * the entity. This panel matches the shape of ComputedAddPanel (compact
 * open/close + form) but submits a proposal via api.createProposal
 * instead of writing directly — admins approve before applying.
 *
 * Computed features have their own dedicated panel since they're
 * directly authored (not gated by proposals).
 */

import { useState } from 'react'
import { Plus, X } from 'lucide-react'

import type { ProposalCreate, ProposalKind } from '@/api/client'
import { useModel } from '@/context/ModelContext'

interface Props {
  entity: string
  // Either 'attribute' or 'relation' — keeps the surface generic so a
  // single component handles both.
  kind: Extract<ProposalKind, 'attribute' | 'relation'>
  onProposed?: () => void
}

const LABELS = {
  attribute: { add: '+ Add attribute', title: 'Propose new attribute', defPlaceholder: 'STRING / INTEGER / FLOAT / enum<a,b>' },
  relation:  { add: '+ Add relation',  title: 'Propose new relation',  defPlaceholder: 'MANY_TO_ONE → Target / ONE_TO_MANY → Target' },
} as const

export function SchemaAddPanel({ entity, kind, onProposed }: Props) {
  const { api } = useModel()
  const labels = LABELS[kind]
  const [open, setOpen] = useState(false)
  const [name, setName] = useState('')
  const [definition, setDefinition] = useState('')
  const [reason, setReason] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function reset() {
    setOpen(false)
    setName(''); setDefinition(''); setReason('')
    setError(null)
  }

  async function submit() {
    setError(null)
    if (!/^[a-z][a-z0-9_]*$/.test(name)) {
      setError('Name must be lowercase letters / digits / underscores, starting with a letter.')
      return
    }
    if (!definition.trim()) {
      setError('Definition is required.')
      return
    }
    if (!reason.trim()) {
      setError('Reason is required.')
      return
    }
    setSubmitting(true)
    try {
      const body: ProposalCreate = {
        artifact_kind: kind,
        artifact_name: name,
        entity_name: entity,
        reason,
        before_yaml: '',
        after_yaml: `${name}: ${definition}`,
      }
      await api.createProposal(body)
      onProposed?.()
      reset()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Submit failed')
    } finally {
      setSubmitting(false)
    }
  }

  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="mt-3 inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
      >
        <Plus className="h-3 w-3" /> {labels.add.replace(/^\+ /, '')}
      </button>
    )
  }

  return (
    <div className="mt-3 rounded border border-amber-500/40 bg-amber-50/30 dark:bg-amber-950/10 p-3 space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium text-amber-700 dark:text-amber-400">{labels.title}</div>
        <button onClick={reset} className="text-muted-foreground hover:text-foreground">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      <input
        value={name}
        onChange={(e) => setName(e.target.value)}
        placeholder="name (snake_case)"
        className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
      />
      <input
        value={definition}
        onChange={(e) => setDefinition(e.target.value)}
        placeholder={labels.defPlaceholder}
        className="w-full text-xs bg-background border rounded px-2 py-1.5 font-mono"
      />
      <textarea
        value={reason}
        onChange={(e) => setReason(e.target.value)}
        rows={2}
        placeholder="reason (required) — why is this needed?"
        className="w-full text-xs bg-background border rounded px-2 py-1.5 placeholder:text-muted-foreground/50"
      />

      <div className="text-[10px] text-muted-foreground/70">
        Reviewers: <span className="font-medium">owner</span> · <span className="font-medium">admins</span>
      </div>

      {error && <p className="text-[11px] text-red-600">{error}</p>}

      <div className="flex justify-end gap-2 pt-1">
        <button
          onClick={reset}
          className="text-xs text-muted-foreground hover:text-foreground px-2 py-1"
        >
          Cancel
        </button>
        <button
          onClick={submit}
          disabled={submitting}
          className="text-xs px-3 py-1 rounded border border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-400 hover:bg-amber-500/20 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {submitting ? 'Submitting…' : 'Propose'}
        </button>
      </div>
    </div>
  )
}
