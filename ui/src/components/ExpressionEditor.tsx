// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useState, useEffect, useRef, useCallback } from 'react'
import { Check, X, ChevronDown, ChevronUp, BookOpen, Play, Loader2 } from 'lucide-react'
import { useModel } from '@/context/ModelContext'
import type { ExprMode, FunctionDoc } from '@/api/client'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Debounce hook
// ---------------------------------------------------------------------------

function useDebounce<T>(value: T, ms: number): T {
  const [debouncedValue, setDebouncedValue] = useState(value)
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedValue(value), ms)
    return () => clearTimeout(timer)
  }, [value, ms])
  return debouncedValue
}

// ---------------------------------------------------------------------------
// Function browser popover
// ---------------------------------------------------------------------------

function FunctionBrowser({
  mode,
  onInsert,
  onClose,
}: {
  mode: ExprMode
  onInsert: (snippet: string) => void
  onClose: () => void
}) {
  const { api } = useModel()
  const [docs, setDocs] = useState<FunctionDoc[]>([])
  const [search, setSearch] = useState('')

  useEffect(() => {
    api.listFunctions(mode).then(setDocs).catch(() => {})
  }, [mode])

  const filtered = docs.filter(
    (f) =>
      f.name.includes(search.toLowerCase()) ||
      f.description.toLowerCase().includes(search.toLowerCase()),
  )

  const grouped = filtered.reduce<Record<string, FunctionDoc[]>>((acc, f) => {
    ;(acc[f.category] ??= []).push(f)
    return acc
  }, {})

  return (
    <div className="absolute right-0 top-8 z-50 w-80 bg-popover border border-border rounded-lg shadow-lg overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-muted/40">
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Functions</span>
        <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      <div className="px-2 py-1.5 border-b border-border">
        <input
          className="w-full text-xs px-2 py-1 rounded border border-border bg-background focus:outline-none focus:ring-1 focus:ring-ring"
          placeholder="Search…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          autoFocus
        />
      </div>
      <div className="max-h-72 overflow-y-auto">
        {Object.entries(grouped).map(([cat, fns]) => (
          <div key={cat}>
            <div className="px-3 py-1 text-[10px] font-semibold text-muted-foreground uppercase tracking-wide bg-muted/30 sticky top-0">
              {cat}
            </div>
            {fns.map((f) => (
              <button
                key={f.name}
                onClick={() => { onInsert(f.example); onClose() }}
                className="w-full text-left px-3 py-2 hover:bg-muted/50 transition-colors"
              >
                <div className="text-xs font-mono font-medium">{f.signature}</div>
                <div className="text-[11px] text-muted-foreground mt-0.5">{f.description}</div>
              </button>
            ))}
          </div>
        ))}
        {filtered.length === 0 && (
          <p className="px-3 py-4 text-xs text-muted-foreground text-center">No functions found</p>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// ExpressionEditor
// ---------------------------------------------------------------------------

export interface ExpressionEditorProps {
  entity: string
  mode: ExprMode
  value: string
  onChange: (v: string) => void
  placeholder?: string
  rows?: number
  /** Called when validation runs — useful for parent to know valid state */
  onValidation?: (result: { valid: boolean; compiled_sql: string | null }) => void
  /** Suppress the built-in Preview button (use when the parent owns the preview action) */
  hidePreviewButton?: boolean
}

export function ExpressionEditor({
  entity,
  mode,
  value,
  onChange,
  placeholder,
  rows = 3,
  onValidation,
  hidePreviewButton = false,
}: ExpressionEditorProps) {
  const { api } = useModel()
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const [validationState, setValidationState] = useState<
    | { status: 'idle' }
    | { status: 'validating' }
    | { status: 'valid'; sql: string }
    | { status: 'invalid'; errors: string[] }
  >({ status: 'idle' })

  const [showSql, setShowSql] = useState(false)
  const [showBrowser, setShowBrowser] = useState(false)

  const [previewState, setPreviewState] = useState<
    | { status: 'idle' }
    | { status: 'loading' }
    | { status: 'ok'; results: { entity_id: string; value: string | number | null }[] }
    | { status: 'error'; message: string }
  >({ status: 'idle' })

  const debouncedValue = useDebounce(value, 500)

  // Auto-validate on change
  useEffect(() => {
    if (!debouncedValue.trim() || !entity) {
      setValidationState({ status: 'idle' })
      return
    }
    setValidationState({ status: 'validating' })
    api
      .validateExpression(entity, debouncedValue, mode)
      .then((result) => {
        if (result.valid) {
          setValidationState({ status: 'valid', sql: result.compiled_sql ?? '' })
        } else {
          setValidationState({ status: 'invalid', errors: result.errors.map((e) => e.message) })
        }
        onValidation?.({ valid: result.valid, compiled_sql: result.compiled_sql })
      })
      .catch(() => setValidationState({ status: 'idle' }))
  }, [debouncedValue, entity, mode])

  const handlePreview = useCallback(async () => {
    if (!value.trim() || !entity) return
    setPreviewState({ status: 'loading' })
    try {
      const result = await api.previewExpression(entity, value)
      setPreviewState({ status: 'ok', results: result.results })
    } catch (e) {
      setPreviewState({ status: 'error', message: e instanceof Error ? e.message : 'Preview failed' })
    }
  }, [value, entity, api])

  function insertAtCursor(snippet: string) {
    const el = textareaRef.current
    if (!el) { onChange(value + snippet); return }
    const start = el.selectionStart
    const end = el.selectionEnd
    const next = value.slice(0, start) + snippet + value.slice(end)
    onChange(next)
    // Restore cursor after React re-render
    requestAnimationFrame(() => {
      el.selectionStart = el.selectionEnd = start + snippet.length
      el.focus()
    })
  }

  const isValid = validationState.status === 'valid'
  const isInvalid = validationState.status === 'invalid'
  const isValidating = validationState.status === 'validating'

  return (
    <div className="space-y-1.5">
      {/* Textarea + function browser button */}
      <div className="relative">
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          rows={rows}
          placeholder={placeholder ?? 'Enter expression…'}
          className={cn(
            'w-full font-mono text-sm px-3 py-2 rounded-md border bg-background resize-none focus:outline-none focus:ring-1 focus:ring-ring transition-colors',
            isValid && 'border-green-500/60',
            isInvalid && 'border-destructive',
            !isValid && !isInvalid && 'border-border',
          )}
        />
        <button
          type="button"
          onClick={() => setShowBrowser((o) => !o)}
          className="absolute top-2 right-2 text-xs text-muted-foreground hover:text-foreground transition-colors font-mono"
          title="Function browser"
        >
          <BookOpen className="h-3.5 w-3.5" />
        </button>
        {showBrowser && (
          <FunctionBrowser
            mode={mode}
            onInsert={insertAtCursor}
            onClose={() => setShowBrowser(false)}
          />
        )}
      </div>

      {/* Status bar */}
      <div className="flex items-center justify-between min-h-[20px]">
        <div className="flex items-center gap-2">
          {isValidating && (
            <span className="flex items-center gap-1 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" /> Validating…
            </span>
          )}
          {isValid && (
            <span className="flex items-center text-green-600">
              <Check className="h-3.5 w-3.5" />
            </span>
          )}
          {isInvalid && validationState.errors.map((err, i) => (
            <span key={i} className="flex items-center gap-1 text-xs text-destructive">
              <X className="h-3.5 w-3.5 shrink-0" /> {err}
            </span>
          ))}
        </div>

        <div className="flex items-center gap-2">
          {/* Preview button */}
          {mode === 'feature' && !hidePreviewButton && (
            <button
              type="button"
              onClick={handlePreview}
              disabled={!isValid || previewState.status === 'loading'}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-40 transition-colors"
            >
              {previewState.status === 'loading'
                ? <Loader2 className="h-3 w-3 animate-spin" />
                : <Play className="h-3 w-3" />}
              Preview
            </button>
          )}

          {/* SQL toggle */}
          {isValid && (
            <button
              type="button"
              onClick={() => setShowSql((o) => !o)}
              className="flex items-center gap-0.5 text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              SQL {showSql ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
            </button>
          )}
        </div>
      </div>

      {/* Preview results */}
      {previewState.status === 'ok' && (
        <div className="border border-border rounded overflow-hidden text-xs">
          <table className="w-full">
            <thead>
              <tr className="bg-muted/40 border-b border-border">
                <th className="text-left px-2.5 py-1.5 font-medium text-muted-foreground">Entity ID</th>
                <th className="text-right px-2.5 py-1.5 font-medium text-muted-foreground">Value</th>
              </tr>
            </thead>
            <tbody>
              {previewState.results.map((r) => (
                <tr key={r.entity_id} className="border-b border-border/50 last:border-0">
                  <td className="px-2.5 py-1.5 font-mono text-muted-foreground truncate max-w-48">{r.entity_id}</td>
                  <td className="px-2.5 py-1.5 font-mono text-right font-medium">
                    {r.value === null ? <span className="italic opacity-40">null</span> : String(r.value)}
                  </td>
                </tr>
              ))}
              {previewState.results.length === 0 && (
                <tr><td colSpan={2} className="px-2.5 py-2 text-center text-muted-foreground italic">No results</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
      {previewState.status === 'error' && (
        <div className="text-xs text-destructive bg-destructive/10 border border-destructive/30 rounded px-2.5 py-1.5">
          {previewState.message}
        </div>
      )}

      {/* Compiled SQL */}
      {showSql && isValid && (
        <pre className="text-xs font-mono bg-muted/40 border border-border rounded px-2.5 py-2 overflow-x-auto whitespace-pre-wrap text-muted-foreground">
          {validationState.status === 'valid' ? validationState.sql : ''}
        </pre>
      )}
    </div>
  )
}
