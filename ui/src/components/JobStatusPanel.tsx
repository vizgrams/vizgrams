// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { CheckCircle, XCircle, Loader } from 'lucide-react'
import type { JobState } from '@/hooks/useJobPoller'
import { cn } from '@/lib/utils'

interface Props {
  state: JobState
  onDismiss: () => void
}

export function JobStatusPanel({ state, onDismiss }: Props) {
  if (state.phase === 'idle') return null

  const { job } = state
  const done = state.phase === 'done'
  const ok = done && job.status === 'completed'
  const failed = done && job.status === 'failed'

  return (
    <div
      className={cn(
        'flex items-start gap-3 rounded-md border px-4 py-3 text-sm',
        !done && 'border-border bg-muted/40',
        ok && 'border-green-200 bg-green-50 text-green-800',
        failed && 'border-red-200 bg-red-50 text-red-800',
      )}
    >
      <span className="mt-0.5 shrink-0">
        {!done && <Loader className="h-4 w-4 animate-spin text-muted-foreground" />}
        {ok && <CheckCircle className="h-4 w-4 text-green-600" />}
        {failed && <XCircle className="h-4 w-4 text-red-600" />}
      </span>

      <div className="flex-1 min-w-0">
        {!done && <p className="text-muted-foreground">{state.label}…</p>}
        {ok && <p className="font-medium">{state.label} completed successfully.</p>}
        {failed && (
          <>
            <p className="font-medium">{state.label} failed.</p>
            {job.error && <p className="mt-1 text-xs font-mono opacity-80">{job.error}</p>}
          </>
        )}
        {job.progress.length > 0 && !done && (
          <p className="mt-0.5 text-xs text-muted-foreground">{job.progress.at(-1)}</p>
        )}
      </div>

      {done && (
        <button
          onClick={onDismiss}
          className="text-xs opacity-60 hover:opacity-100 shrink-0"
        >
          Dismiss
        </button>
      )}
    </div>
  )
}
