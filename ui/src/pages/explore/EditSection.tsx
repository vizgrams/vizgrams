// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useState, useEffect } from 'react'
import { ChevronDown } from 'lucide-react'
import { EditShell } from './EditShell'
import type { EditShellProps } from './EditShell'
import { StatusBadge } from '@/components/StatusBadge'
import type { ValidStatus } from '@/components/StatusBadge'
import { cn } from '@/lib/utils'

export type { ValidStatus }

export interface EditSectionProps extends Omit<EditShellProps, 'noBorder'> {
  /** Open by default — pass true for new items, false (default) for existing */
  defaultOpen?: boolean
  /** Validation status shown as a badge in the section header */
  validStatus?: ValidStatus
}

export function EditSection({ defaultOpen = false, validStatus = 'idle', ...shellProps }: EditSectionProps) {
  const [open, setOpen] = useState(defaultOpen)
  const errorCount = (shellProps.validErrors ?? []).length

  // When defaultOpen is true (new mode), force the section open
  useEffect(() => {
    if (defaultOpen) setOpen(true)
  }, [defaultOpen])

  const alwaysOpen = defaultOpen

  return (
    <div className="border rounded-lg overflow-hidden">
      {!alwaysOpen && (
        <button
          onClick={() => setOpen(o => !o)}
          className={cn(
            'w-full flex items-center justify-between px-3 py-2 bg-muted/30 hover:bg-muted/50 transition-colors',
            open && 'border-b',
          )}
        >
          <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Edit</span>
          <div className="flex items-center gap-2">
            <StatusBadge status={validStatus} errorCount={errorCount} />
            <ChevronDown className={cn('h-3.5 w-3.5 text-muted-foreground transition-transform', !open && '-rotate-90')} />
          </div>
        </button>
      )}
      {(alwaysOpen || open) && <EditShell noBorder {...shellProps} />}
    </div>
  )
}
