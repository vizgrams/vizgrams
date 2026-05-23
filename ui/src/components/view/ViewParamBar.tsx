// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ViewParamBar — the slim input row for a view's declared parameters.
 *
 * The same widget shows up everywhere a view renders: ``ViewResultFrame``
 * (saved views in /views), ``ChatViewCard`` (chat-rendered views, so
 * follow-ups can refine the answer without re-asking), and apps that
 * embed a view with its own param schema.
 *
 * Apply happens on Enter — the parent decides what to do (typically
 * re-execute and sync to the URL).
 */

import { SlidersHorizontal } from 'lucide-react'
import type { ParamDef } from '@/api/client'

interface Props {
  params: ParamDef[]
  values: Record<string, string>
  onChange: (values: Record<string, string>) => void
  onApply: () => void
}

export function ViewParamBar({ params, values, onChange, onApply }: Props) {
  if (params.length === 0) return null
  return (
    <div className="flex items-end gap-3 flex-wrap rounded-lg border bg-muted/30 px-4 py-3">
      <SlidersHorizontal className="h-4 w-4 text-muted-foreground shrink-0 mt-1" />
      {params.map((p) => (
        <div key={p.name} className="flex flex-col gap-1 min-w-[140px]">
          <label className="text-xs text-muted-foreground font-medium">
            {p.label ?? p.name}
            {p.optional && <span className="ml-1 text-muted-foreground/60">(optional)</span>}
          </label>
          <input
            type="text"
            value={values[p.name] ?? ''}
            placeholder={p.optional ? 'all' : (p.default ?? '')}
            onChange={(e) => onChange({ ...values, [p.name]: e.target.value })}
            onKeyDown={(e) => { if (e.key === 'Enter') onApply() }}
            className="h-7 rounded border bg-background px-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring w-full"
          />
        </div>
      ))}
    </div>
  )
}
