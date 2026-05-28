// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * GovernedYamlEditor — shared YAML editor for governed surfaces
 * (mappers, extractors) (Epic 26 VG-297).
 *
 * Authorization is role-conditional inside the component so callers
 * don't have to gate twice:
 *   - admin → directly calls ``onDirectSave`` (PUT /mapper or
 *     /tool/{t}/extract). Mutation lands immediately.
 *   - member → opens an inline reason prompt and calls
 *     ``onProposeChange`` (POST /proposals with the YAML as
 *     ``after_yaml``). Decision goes through the VG-295 workflow.
 *
 * Errors stay in the editor so the user can fix + retry without
 * losing their edit.
 */

import { useEffect, useState } from 'react'
import { Pencil, X } from 'lucide-react'

import { useRole } from '@/context/RoleContext'

interface Props {
  title: string
  initialContent: string
  // Hooks the caller wires to the right API call. Both return a
  // Promise so we can disable the button while in-flight.
  onDirectSave: (content: string) => Promise<void>
  onProposeChange: (content: string, reason: string) => Promise<void>
  onClose: () => void
}

export function GovernedYamlEditor({
  title, initialContent, onDirectSave, onProposeChange, onClose,
}: Props) {
  const { role } = useRole()
  const isAdmin = role === 'admin'

  const [content, setContent] = useState(initialContent)
  const [reason, setReason] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // If the caller passes new content (e.g. after a refetch), pick it up.
  useEffect(() => { setContent(initialContent) }, [initialContent])

  async function save() {
    setBusy(true)
    setError(null)
    try {
      if (isAdmin) {
        await onDirectSave(content)
      } else {
        if (!reason.trim()) {
          setError('A reason is required when proposing a change')
          setBusy(false)
          return
        }
        await onProposeChange(content, reason.trim())
      }
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="rounded border bg-card p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">{title}</div>
        <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      <textarea
        value={content}
        onChange={(e) => setContent(e.target.value)}
        rows={14}
        className="w-full text-xs bg-background border rounded px-2.5 py-2 font-mono resize-y"
      />

      {!isAdmin && (
        <div className="rounded border border-amber-500/40 bg-amber-50/30 dark:bg-amber-950/10 p-2.5 space-y-2">
          <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-amber-700 dark:text-amber-400">
            <Pencil className="h-3 w-3" />
            Propose change — admin or owner must approve
          </div>
          <textarea
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            rows={2}
            placeholder="Reason for this change (required)"
            className="w-full text-xs bg-background border rounded px-2.5 py-2 placeholder:text-muted-foreground/50"
          />
        </div>
      )}

      {error && <p className="text-[11px] text-red-600">{error}</p>}

      <div className="flex justify-end gap-2">
        <button
          onClick={onClose}
          className="text-xs text-muted-foreground hover:text-foreground px-3 py-1.5"
        >
          Cancel
        </button>
        <button
          onClick={save}
          disabled={busy}
          className={
            isAdmin
              ? 'text-xs px-3 py-1.5 rounded border bg-foreground text-background hover:bg-foreground/90 disabled:opacity-50 disabled:cursor-not-allowed'
              : 'text-xs px-3 py-1.5 rounded border border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-400 hover:bg-amber-500/20 disabled:opacity-50 disabled:cursor-not-allowed'
          }
        >
          {busy ? 'Saving…' : isAdmin ? 'Save' : 'Propose change'}
        </button>
      </div>
    </div>
  )
}
