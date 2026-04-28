// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useCallback, useEffect, useRef, useState, useMemo } from 'react'
import {
  Play, Save, Plus, ChevronDown, ChevronRight,
  Download, X, Code2, Sparkles, ArrowUpDown, ArrowUp, ArrowDown,
  Edit2, Send, Check, Loader2,
} from 'lucide-react'
import type { QueryDetail, QueryResult, QuerySummary, ValidationResult, EntityDetail, EntitySummary, ExprMode } from '@/api/client'
import { useApi } from '@/hooks/useApi'
import { useModel } from '@/context/ModelContext'
import { Card, Spinner, ErrorMessage } from '@/components/Layout'
import { YamlEditor } from '@/components/YamlEditor'
import type { YamlEditorProps } from '@/components/YamlEditor'
import type { EditMode } from '@/pages/explore/EditShell'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import { cn, formatValue as _formatValue } from '@/lib/utils'
import {
  type AttributeRow, type MeasureRow, type FilterRow, type OrderRow, type QueryDraft,
  makeId, detailToDraft, draftToYaml, SAMPLE, applyNumberPattern,
} from '@/lib/queryUtils'

// ---------------------------------------------------------------------------
// Draft types (re-exported from queryUtils)
// ---------------------------------------------------------------------------

function emptyDraft(): QueryDraft {
  return { name: '', description: '', root: '', attributes: [], measures: [], filters: [], order: [], params: [] }
}

// ---------------------------------------------------------------------------
// Inline validation hook + icon
// ---------------------------------------------------------------------------

function useInlineValidation(entity: string, expr: string, mode: ExprMode) {
  const { api } = useModel()
  const [status, setStatus] = useState<'idle' | 'validating' | 'valid' | 'invalid'>('idle')
  useEffect(() => {
    if (!expr.trim() || !entity) { setStatus('idle'); return }
    setStatus('validating')
    const timer = setTimeout(() => {
      api.validateExpression(entity, expr, mode)
        .then(r => setStatus(r.valid ? 'valid' : 'invalid'))
        .catch(() => setStatus('idle'))
    }, 500)
    return () => clearTimeout(timer)
  }, [expr, entity, mode])
  return status
}

function ValidationIcon({ status }: { status: 'idle' | 'validating' | 'valid' | 'invalid' }) {
  if (status === 'idle') return <span className="w-3.5 shrink-0" />
  if (status === 'validating') return <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-muted-foreground" />
  if (status === 'valid') return <Check className="h-3.5 w-3.5 shrink-0 text-green-600" />
  return <X className="h-3.5 w-3.5 shrink-0 text-red-500" />
}

// ---------------------------------------------------------------------------
// Path autocomplete
// ---------------------------------------------------------------------------

interface PathSuggestion {
  fullPath: string; label: string; type: 'attribute' | 'relation'
  attrType?: string; targetEntity?: string
}

function computeSuggestions(path: string, rootEntity: string, entities: Record<string, EntityDetail>): PathSuggestion[] {
  if (!rootEntity || !entities[rootEntity]) return []
  const segments = path.split('.')
  const typing = segments[segments.length - 1].toLowerCase()
  const parentSegments = segments.slice(0, -1)
  const prefix = parentSegments.length ? parentSegments.join('.') + '.' : ''
  let currentEntity = rootEntity
  for (const seg of parentSegments) {
    const ent = entities[currentEntity]
    if (!ent) return []
    const rel = ent.relations.find(r => r.name.toLowerCase() === seg.toLowerCase())
    if (!rel?.target || !entities[rel.target]) return []
    currentEntity = rel.target
  }
  const ent = entities[currentEntity]
  if (!ent) return []
  const results: PathSuggestion[] = []
  for (const attr of ent.attributes)
    if (!typing || attr.name.toLowerCase().includes(typing))
      results.push({ fullPath: prefix + attr.name, label: attr.name, type: 'attribute', attrType: attr.type })
  for (const rel of ent.relations) {
    if (!rel.target || !entities[rel.target]) continue
    if (!typing || rel.name.toLowerCase().includes(typing))
      results.push({ fullPath: prefix + rel.name + '.', label: rel.name, type: 'relation', targetEntity: rel.target })
  }
  return results.slice(0, 24)
}

function PathAutocomplete({ value, onChange, rootEntity, entities, placeholder = 'field.path…' }: {
  value: string; onChange: (v: string) => void
  rootEntity: string; entities: Record<string, EntityDetail>; placeholder?: string
}) {
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)
  const suggestions = useMemo(() => computeSuggestions(value, rootEntity, entities), [value, rootEntity, entities])
  useEffect(() => { setActiveIdx(0) }, [value])
  useEffect(() => {
    function handle(e: MouseEvent) {
      if (!inputRef.current?.contains(e.target as Node) && !listRef.current?.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handle)
    return () => document.removeEventListener('mousedown', handle)
  }, [])
  function select(s: PathSuggestion) {
    onChange(s.fullPath)
    if (s.type === 'attribute') { setOpen(false); inputRef.current?.blur() }
    else setTimeout(() => inputRef.current?.focus(), 0)
  }
  function onKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'ArrowDown') { if (!open) { setOpen(true); e.preventDefault(); return } setActiveIdx(i => Math.min(i + 1, suggestions.length - 1)); e.preventDefault() }
    else if (e.key === 'ArrowUp') { setActiveIdx(i => Math.max(i - 1, 0)); e.preventDefault() }
    else if ((e.key === 'Enter' || e.key === 'Tab') && open && suggestions[activeIdx]) { select(suggestions[activeIdx]); e.preventDefault() }
    else if (e.key === 'Escape') setOpen(false)
  }
  return (
    <div className="relative flex-1 min-w-0">
      <input ref={inputRef} value={value}
        onChange={e => { onChange(e.target.value); setOpen(true) }}
        onFocus={() => setOpen(true)} onKeyDown={onKeyDown} placeholder={placeholder}
        className="w-full rounded-md border bg-background px-2.5 py-1.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-ring"
        autoComplete="off" spellCheck={false} />
      {open && suggestions.length > 0 && (
        <div ref={listRef} className="absolute z-50 mt-0.5 left-0 min-w-[220px] rounded-md border bg-card shadow-lg max-h-52 overflow-y-auto">
          {suggestions.map((s, i) => (
            <button key={s.fullPath} onMouseDown={e => { e.preventDefault(); select(s) }}
              className={cn('w-full text-left px-2.5 py-1.5 flex items-center gap-2 text-xs transition-colors', i === activeIdx ? 'bg-muted' : 'hover:bg-muted/60')}>
              <span className={cn('shrink-0 w-3 text-center font-bold', s.type === 'relation' ? 'text-blue-500' : 'text-muted-foreground/30')}>{s.type === 'relation' ? '→' : '·'}</span>
              <span className="font-mono">{s.label}</span>
              <span className="ml-auto font-mono text-[10px] text-muted-foreground/50">{s.type === 'attribute' ? s.attrType : s.targetEntity}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Collapsible section
// ---------------------------------------------------------------------------

function Section({ title, count, onAdd, defaultOpen = true, children }: {
  title: string; count: number; onAdd: () => void; defaultOpen?: boolean; children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="border rounded-lg overflow-hidden">
      <div className="flex items-center bg-muted/30 px-3 py-2 gap-2">
        <button onClick={() => setOpen(o => !o)} className="flex items-center gap-1.5 flex-1 min-w-0 text-left">
          {open ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />}
          <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">{title}</span>
          {count > 0 && <span className="text-xs text-muted-foreground/60 ml-0.5">({count})</span>}
        </button>
        <button onClick={onAdd} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors shrink-0">
          <Plus className="h-3 w-3" /> Add
        </button>
      </div>
      {open && <div className="p-3">{children}</div>}
    </div>
  )
}


// ---------------------------------------------------------------------------
// Sortable results table
// ---------------------------------------------------------------------------

function formatValue(value: string | number | null, fmt?: { type: string; unit?: string | null }): string {
  if (value == null) return '—'
  return _formatValue(value, fmt as Parameters<typeof _formatValue>[1])
}

function downloadCsv(result: QueryResult, name: string) {
  const rows = [result.columns, ...result.rows.map(r => r.map(v => v ?? ''))]
  const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n')
  const a = document.createElement('a')
  a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }))
  a.download = `${name}.csv`; a.click()
}

function ResultsTable({ result }: { result: QueryResult }) {
  const [sortCol, setSortCol] = useState<string | null>(null)
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [showSql, setShowSql] = useState(false)

  const sortedRows = useMemo(() => {
    if (!sortCol) return result.rows
    const idx = result.columns.indexOf(sortCol)
    if (idx < 0) return result.rows
    return [...result.rows].sort((a, b) => {
      const av = a[idx], bv = b[idx]
      if (av == null && bv == null) return 0
      if (av == null) return sortDir === 'asc' ? 1 : -1
      if (bv == null) return sortDir === 'asc' ? -1 : 1
      if (typeof av === 'number' && typeof bv === 'number') return sortDir === 'asc' ? av - bv : bv - av
      return sortDir === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av))
    })
  }, [result.rows, sortCol, sortDir])

  function toggleSort(col: string) {
    if (sortCol === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortCol(col); setSortDir('asc') }
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground font-medium">
          {result.total_row_count.toLocaleString()} rows
          {result.truncated && ` (showing ${result.row_count.toLocaleString()})`}
          <span className="ml-2 opacity-50">{result.duration_ms}ms</span>
        </span>
        <div className="flex items-center gap-2">
          <button onClick={() => setShowSql(s => !s)} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
            <Code2 className="h-3 w-3" /> SQL
          </button>
          <button onClick={() => downloadCsv(result, result.query)} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
            <Download className="h-3 w-3" /> CSV
          </button>
        </div>
      </div>
      {showSql && <pre className="rounded-md border bg-muted/30 px-3 py-2 text-xs font-mono text-muted-foreground overflow-x-auto whitespace-pre-wrap">{result.sql}</pre>}
      <Card className="p-0 overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-muted/30">
              {result.columns.map(c => (
                <th key={c} className="text-left px-3 py-2 font-medium text-xs whitespace-nowrap">
                  <button onClick={() => toggleSort(c)} className="flex items-center gap-1 hover:text-foreground text-muted-foreground transition-colors group">
                    {c}
                    {sortCol === c
                      ? sortDir === 'asc' ? <ArrowUp className="h-3 w-3 text-foreground" /> : <ArrowDown className="h-3 w-3 text-foreground" />
                      : <ArrowUpDown className="h-3 w-3 opacity-0 group-hover:opacity-40 transition-opacity" />}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row, i) => (
              <tr key={i} className="border-b last:border-0 hover:bg-muted/20 transition-colors">
                {row.map((val, j) => (
                  <td key={j} className="px-3 py-2 text-sm tabular-nums whitespace-nowrap">
                    {formatValue(val, result.formats[result.columns[j]])}
                  </td>
                ))}
              </tr>
            ))}
            {result.rows.length === 0 && (
              <tr><td colSpan={result.columns.length} className="px-3 py-6 text-center text-sm text-muted-foreground">No results</td></tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
  )
}

// ---------------------------------------------------------------------------
// AI panel
// ---------------------------------------------------------------------------

interface ChatMessage { id: string; role: 'user' | 'assistant'; content: string; error?: boolean }

const AI_EXAMPLE_PROMPTS = [
  'Calculate change lead time by team',
  'Filter to only show merged PRs from this year',
  'Group results by week instead of month',
  'Add a count of distinct authors per team',
]

function AiPanel({ draft: _draft, onApply: _onApply, disabled, open, onToggle: _onToggle }: {
  draft: QueryDraft
  onApply: (patch: Partial<QueryDraft>) => void
  disabled: boolean
  open: boolean
  onToggle: () => void
}) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight
  }, [messages])

  async function send() {
    const text = input.trim()
    if (!text || loading) return
    setInput('')
    setMessages(m => [...m, { id: makeId(), role: 'user', content: text }])
    setLoading(true)
    try {
      // Stub — replace with real API call when endpoint is ready:
      // const result = await api.aiSuggest(text, draft)
      // onApply(result.changes)
      // setMessages(m => [...m, { id: makeId(), role: 'assistant', content: result.message }])
      await new Promise(r => setTimeout(r, 700))
      setMessages(m => [...m, {
        id: makeId(), role: 'assistant',
        content: 'AI query building is coming soon. Your prompt has been noted and will be applied once the endpoint is ready.',
      }])
    } catch (e) {
      setMessages(m => [...m, { id: makeId(), role: 'assistant', content: `Error: ${String(e)}`, error: true }])
    } finally {
      setLoading(false)
    }
  }

  if (!open) return null

  return (
    <div className="w-72 shrink-0 border-l flex flex-col bg-card">
      {/* Header */}
      <div className="px-4 py-3 border-b shrink-0">
        <div className="flex items-center gap-2">
          <Sparkles className="h-3.5 w-3.5 text-purple-500" />
          <span className="text-sm font-semibold">AI Assistant</span>
          <span className="ml-auto text-[10px] bg-purple-100 text-purple-600 dark:bg-purple-900/40 dark:text-purple-400 rounded-full px-1.5 py-0.5 font-medium">Soon</span>
        </div>
        <p className="text-xs text-muted-foreground mt-1 leading-snug">
          Iterate on your query in plain English. Each message refines the current state.
        </p>
      </div>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-3 flex flex-col gap-2 min-h-0">
        {messages.length === 0 ? (
          <div className="flex flex-col gap-1.5 py-2">
            <p className="text-[11px] text-muted-foreground/60 mb-1 px-1">Try asking:</p>
            {AI_EXAMPLE_PROMPTS.map(p => (
              <button key={p} onClick={() => setInput(p)} disabled={disabled}
                className="text-xs text-left rounded-md border bg-muted/20 px-3 py-2 hover:bg-muted/50 transition-colors text-muted-foreground disabled:opacity-40">
                "{p}"
              </button>
            ))}
          </div>
        ) : (
          messages.map(m => (
            <div key={m.id} className={cn('flex', m.role === 'user' ? 'justify-end' : 'justify-start')}>
              <div className={cn('rounded-xl px-3 py-2 text-xs leading-relaxed max-w-[90%]',
                m.role === 'user'
                  ? 'bg-purple-600 text-white rounded-br-sm'
                  : m.error
                    ? 'bg-red-50 text-red-700 border border-red-200 rounded-bl-sm'
                    : 'bg-muted text-foreground rounded-bl-sm')}>
                {m.content}
              </div>
            </div>
          ))
        )}
        {loading && (
          <div className="flex justify-start">
            <div className="bg-muted rounded-xl rounded-bl-sm px-3 py-2">
              <span className="flex gap-1">
                {[0, 1, 2].map(i => <span key={i} className="w-1.5 h-1.5 rounded-full bg-muted-foreground/40 animate-bounce" style={{ animationDelay: `${i * 0.15}s` }} />)}
              </span>
            </div>
          </div>
        )}
      </div>

      {/* Input */}
      <div className="p-3 border-t shrink-0">
        <div className="flex gap-2 items-end">
          <textarea
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() } }}
            placeholder={disabled ? 'Select an entity first…' : 'Describe a change to your query…'}
            disabled={disabled || loading}
            rows={2}
            className="flex-1 rounded-md border bg-background px-2.5 py-1.5 text-xs resize-none focus:outline-none focus:ring-1 focus:ring-purple-400 disabled:opacity-50 min-h-0"
          />
          <button onClick={send} disabled={disabled || !input.trim() || loading}
            className="shrink-0 rounded-md bg-purple-600 text-white p-2 hover:bg-purple-700 transition-colors disabled:opacity-40">
            <Send className="h-3.5 w-3.5" />
          </button>
        </div>
        <p className="text-[10px] text-muted-foreground/40 mt-1.5">Enter to send · Shift+Enter for new line</p>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Number pattern helper
// ---------------------------------------------------------------------------

const NUMBER_PRESETS: { pattern: string; label: string }[] = [
  { pattern: '0',       label: 'integer' },
  { pattern: '0,0',     label: 'thousands' },
  { pattern: '0.0',     label: '1 decimal' },
  { pattern: '0,0.0',   label: '1 dec + sep' },
  { pattern: '0,0.00',  label: '2 dec + sep' },
  { pattern: '0a',      label: 'abbreviated' },
  { pattern: '0.0a',    label: 'abbrev + dec' },
]

function NumberPatternSelect({ value, onChange }: { value: string; onChange: (p: string) => void }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      className="rounded-md border bg-background px-2 py-1 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-ring">
      <option value="">— none —</option>
      {NUMBER_PRESETS.map(p => (
        <option key={p.pattern} value={p.pattern}>
          {applyNumberPattern(SAMPLE, p.pattern)}
        </option>
      ))}
    </select>
  )
}

// ---------------------------------------------------------------------------
// AttrRow / MeasureRow (extracted so hooks can run per-row)
// ---------------------------------------------------------------------------

function AttrRow({ a, rootEntity, entities, onUpdate, onRemove }: {
  a: AttributeRow; rootEntity: string; entities: Record<string, EntityDetail>
  onUpdate: (patch: Partial<AttributeRow>) => void; onRemove: () => void
}) {
  const validStatus = useInlineValidation(rootEntity, a.expr, 'feature')
  return (
    <div className="flex items-center gap-2">
      <input value={a.alias} onChange={e => onUpdate({ alias: e.target.value })} placeholder="alias"
        className="w-24 shrink-0 rounded-md border bg-background px-2.5 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring" />
      <PathAutocomplete value={a.expr} onChange={v => onUpdate({ expr: v })} rootEntity={rootEntity} entities={entities} placeholder="field or format_time(field, 'YYYY-MM')" />
      <ValidationIcon status={validStatus} />
      <button onClick={onRemove} className="shrink-0 text-muted-foreground/30 hover:text-red-500 transition-colors"><X className="h-3.5 w-3.5" /></button>
    </div>
  )
}

const MEASURE_FORMAT_TYPES = ['', 'number', 'percent', 'duration']

function MeasureRow({ m, rootEntity, entities, onUpdate, onUpdateS, onRemove }: {
  m: MeasureRow; rootEntity: string; entities: Record<string, EntityDetail>
  onUpdate: (patch: Partial<MeasureRow>) => void
  onUpdateS: (patch: Partial<MeasureRow>) => void
  onRemove: () => void
}) {
  const measureExpr = m.rawExpr ?? (m.field.trim() ? `${m.agg}(${m.field})` : m.agg === 'count' ? 'count(*)' : '')
  const validStatus = useInlineValidation(rootEntity, measureExpr, 'measure')

  if (m.rawExpr) {
    return (
      <div className="flex items-center gap-2">
        <span className="w-24 shrink-0 rounded-md border bg-muted px-2.5 py-1.5 text-xs font-mono text-muted-foreground truncate" title={m.name}>{m.name || '—'}</span>
        <span className="flex-1 flex items-center gap-1.5 rounded-md border bg-muted px-2.5 py-1.5 text-xs font-mono text-muted-foreground overflow-hidden">
          <Code2 className="h-3 w-3 shrink-0 opacity-50" />
          <span className="truncate" title={m.rawExpr}>{m.rawExpr}</span>
        </span>
        <ValidationIcon status={validStatus} />
        <button onClick={onRemove} className="shrink-0 text-muted-foreground/30 hover:text-red-500 transition-colors"><X className="h-3.5 w-3.5" /></button>
        <span className="shrink-0 text-[10px] text-muted-foreground/25 font-mono leading-none">fmt</span>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center gap-2">
        <input value={m.name} onChange={e => onUpdate({ name: e.target.value })} placeholder="name"
          className="w-24 shrink-0 rounded-md border bg-background px-2.5 py-1.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-ring" />
        <select value={m.agg} onChange={e => onUpdateS({ agg: e.target.value })}
          className="w-28 shrink-0 rounded-md border bg-background px-2.5 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
          {AGGREGATIONS.map(a => <option key={a} value={a}>{a}</option>)}
        </select>
        <PathAutocomplete value={m.field} onChange={v => onUpdate({ field: v })} rootEntity={rootEntity} entities={entities} placeholder={m.agg === 'count' ? 'field… (optional)' : 'field…'} />
        <ValidationIcon status={validStatus} />
        <button onClick={onRemove} className="shrink-0 text-muted-foreground/30 hover:text-red-500 transition-colors"><X className="h-3.5 w-3.5" /></button>
        <button onClick={() => onUpdateS({ showFormat: !m.showFormat })}
          className="shrink-0 text-[10px] text-muted-foreground/50 hover:text-muted-foreground transition-colors font-mono leading-none">
          fmt
        </button>
      </div>
      {m.showFormat && (
        <div className="flex items-center gap-2 ml-[212px]">
          <select value={m.formatType} onChange={e => onUpdateS({ formatType: e.target.value })}
            className="w-24 rounded-md border bg-background px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
            {MEASURE_FORMAT_TYPES.map(t => <option key={t} value={t}>{t || 'type…'}</option>)}
          </select>
          {m.formatType === 'number' && (
            <><span className="text-xs text-muted-foreground shrink-0">pattern</span>
            <NumberPatternSelect value={m.formatPattern} onChange={p => onUpdateS({ formatPattern: p })} /></>
          )}
          {m.formatType === 'duration' && (
            <><span className="text-xs text-muted-foreground">unit</span>
              <select value={m.formatUnit} onChange={e => onUpdateS({ formatUnit: e.target.value })}
                className="w-24 rounded-md border bg-background px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
                <option value="">—</option><option value="hours">hours</option><option value="days">days</option><option value="minutes">minutes</option>
              </select></>
          )}
        </div>
      )}
    </div>
  )
}

function FilterRow({ f, rootEntity, onUpdate, onRemove }: {
  f: FilterRow; rootEntity: string
  onUpdate: (patch: Partial<FilterRow>) => void; onRemove: () => void
}) {
  const validStatus = useInlineValidation(rootEntity, f.expr, 'filter')
  return (
    <div className="flex items-center gap-2">
      <input value={f.expr} onChange={e => onUpdate({ expr: e.target.value })} placeholder="e.g. status = 'active' or merged_at >= now() - 4w"
        className="flex-1 rounded-md border bg-background px-2.5 py-1.5 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-ring" />
      <ValidationIcon status={validStatus} />
      <button onClick={onRemove} className="shrink-0 text-muted-foreground/30 hover:text-red-500 transition-colors"><X className="h-3.5 w-3.5" /></button>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Form (centre panel)
// ---------------------------------------------------------------------------

const AGGREGATIONS = ['count', 'sum', 'avg', 'min', 'max', 'count_distinct']

function QueryForm({
  draft, onChange, onChangeStructural, entityDetails, entityNames,
  validStatus, validErrors, saving, onSave, onRun, running, result,
  autoFocusName, aiOpen, onToggleAi, yamlEditorProps,
  editMode, onEditModeChange, isDirty, onRestoreVersion,
}: {
  draft: QueryDraft
  onChange: (d: QueryDraft) => void
  onChangeStructural: (d: QueryDraft) => void
  entityDetails: Record<string, EntityDetail>; entityNames: string[]
  validStatus: ValidStatus; validErrors: ValidationResult['errors']
  saving: boolean
  onSave: () => void; onRun: () => void; running: boolean
  result: QueryResult | null
  autoFocusName?: boolean
  aiOpen: boolean; onToggleAi: () => void
  yamlEditorProps: Omit<YamlEditorProps, 'headerSlot'> | null
  editMode: EditMode
  onEditModeChange: (mode: EditMode) => void
  isDirty: boolean
  onRestoreVersion?: (content: string) => void
}) {
  const [nameEditing, setNameEditing] = useState(!!autoFocusName)

  const set = <K extends keyof QueryDraft>(k: K, v: QueryDraft[K]) => onChange({ ...draft, [k]: v })
  const setS = <K extends keyof QueryDraft>(k: K, v: QueryDraft[K]) => onChangeStructural({ ...draft, [k]: v })

  // Attributes
  function addAttr() { setS('attributes', [...draft.attributes, { id: makeId(), alias: '', expr: '' }]) }
  function removeAttr(id: string) { setS('attributes', draft.attributes.filter(a => a.id !== id)) }
  function updateAttr(id: string, p: Partial<AttributeRow>) { onChange({ ...draft, attributes: draft.attributes.map(a => a.id === id ? { ...a, ...p } : a) }) }

  // Measures
  function addMeasure() { setS('measures', [...draft.measures, { id: makeId(), name: '', agg: 'count', field: '', formatType: '', formatPattern: '', formatUnit: '', showFormat: false }]) }
  function removeMeasure(id: string) { setS('measures', draft.measures.filter(m => m.id !== id)) }
  function updateMeasure(id: string, p: Partial<MeasureRow>) { onChange({ ...draft, measures: draft.measures.map(m => m.id === id ? { ...m, ...p } : m) }) }
  function updateMeasureS(id: string, p: Partial<MeasureRow>) { onChangeStructural({ ...draft, measures: draft.measures.map(m => m.id === id ? { ...m, ...p } : m) }) }

  // Filters
  function addFilter() { setS('filters', [...draft.filters, { id: makeId(), expr: '' }]) }
  function removeFilter(id: string) { setS('filters', draft.filters.filter(f => f.id !== id)) }

  // Order
  function addOrder() {
    const first = [...draft.attributes.map(a => a.alias || a.expr.split('.').pop() || ''), ...draft.measures.map(m => m.name)].find(Boolean) || ''
    setS('order', [...draft.order, { id: makeId(), field: first, direction: 'desc' }])
  }
  function removeOrder(id: string) { setS('order', draft.order.filter(o => o.id !== id)) }
  function updateOrder(id: string, p: Partial<OrderRow>) { onChangeStructural({ ...draft, order: draft.order.map(o => o.id === id ? { ...o, ...p } : o) }) }

  const availableOrderFields = [
    ...draft.attributes.map(a => a.alias || a.expr.split('.').pop() || a.expr).filter(Boolean),
    ...draft.measures.map(m => m.name).filter(Boolean),
  ]

  return (
    <div className="flex flex-col gap-4">
      {/* Query header */}
      <div className="flex items-start gap-3 pb-4 border-b shrink-0">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            {nameEditing ? (
              <input autoFocus value={draft.name}
                onChange={e => set('name', e.target.value.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, ''))}
                onBlur={() => setNameEditing(false)}
                onKeyDown={e => { if (e.key === 'Enter') setNameEditing(false) }}
                className="text-xl font-semibold bg-background border rounded-md px-2 py-0.5 focus:outline-none focus:ring-1 focus:ring-ring max-w-xs" />
            ) : (
              <button onClick={() => setNameEditing(true)} className="flex items-center gap-2 group text-left">
                <h1 className="text-xl font-semibold">{draft.name || <span className="text-muted-foreground">unnamed_query</span>}</h1>
                <Edit2 className="h-3.5 w-3.5 text-muted-foreground opacity-0 group-hover:opacity-60 transition-opacity" />
              </button>
            )}
          </div>
          <input value={draft.description} onChange={e => set('description', e.target.value)}
            placeholder="Add a description…"
            className="w-full text-sm text-muted-foreground bg-transparent border-none outline-none focus:ring-0 placeholder:text-muted-foreground/30 mb-2" />
        </div>

        <div className="flex items-center gap-1.5 shrink-0 pt-1">
          <button disabled={saving} onClick={onSave}
            className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
            <Save className="h-3.5 w-3.5" /> {saving ? 'Saving…' : 'Save'}
          </button>
          <button disabled={running} onClick={onRun}
            className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
            <Play className="h-3.5 w-3.5" /> {running ? 'Running…' : 'Run'}
            <span className="text-[10px] opacity-60 ml-0.5 hidden sm:inline">⌘↵</span>
          </button>
          <button onClick={onToggleAi} title={aiOpen ? 'Hide AI' : 'AI Assistant'}
            className={cn('border rounded-md p-1.5 transition-colors', aiOpen ? 'bg-purple-100 text-purple-600 border-purple-200 hover:bg-purple-200' : 'text-muted-foreground hover:bg-muted hover:text-foreground')}>
            <Sparkles className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Edit sections + results */}
      <div className="flex flex-col gap-3 pb-4">

        {/* EditShell: builder + yaml modes — collapsible */}
        <EditSection
          defaultOpen={!!autoFocusName}
          mode={editMode}
          onModeChange={onEditModeChange}
          isDirty={isDirty}
          validStatus={validStatus}
          historyKey={yamlEditorProps?.historyKey}
          onRestoreVersion={onRestoreVersion}
          builderContent={
            <div className="flex flex-col gap-3">
              {/* Entity selector */}
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground">Entity</span>
                <select value={draft.root}
                  onChange={e => onChangeStructural({ ...draft, root: e.target.value, attributes: [], measures: [], filters: [], order: [] })}
                  className="rounded-md border bg-background px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
                  <option value="">Select entity…</option>
                  {entityNames.map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </div>
              {!draft.root && (
                <p className="text-xs text-muted-foreground/60 text-center py-2">Select an entity to start building.</p>
              )}
              <div className={cn('flex flex-col gap-3', !draft.root && 'opacity-40 pointer-events-none')}>
                <Section title="Attributes" count={draft.attributes.length} onAdd={addAttr}>
                  {draft.attributes.length === 0
                    ? <p className="text-xs text-muted-foreground/50 py-1">No attributes — add at least one field to include in results.</p>
                    : <div className="flex flex-col gap-2">
                        {draft.attributes.map(a => (
                          <AttrRow key={a.id} a={a} rootEntity={draft.root} entities={entityDetails}
                            onUpdate={p => updateAttr(a.id, p)} onRemove={() => removeAttr(a.id)} />
                        ))}
                      </div>
                  }
                </Section>

                <Section title="Measures" count={draft.measures.length} onAdd={addMeasure}>
                  {draft.measures.length === 0
                    ? <p className="text-xs text-muted-foreground/50 py-1">No measures defined.</p>
                    : <div className="flex flex-col gap-2">
                        {draft.measures.map(m => (
                          <MeasureRow key={m.id} m={m} rootEntity={draft.root} entities={entityDetails}
                            onUpdate={p => updateMeasure(m.id, p)} onUpdateS={p => updateMeasureS(m.id, p)} onRemove={() => removeMeasure(m.id)} />
                        ))}
                      </div>
                  }
                </Section>

                <Section title="Filters" count={draft.filters.length} onAdd={addFilter} defaultOpen={draft.filters.length > 0}>
                  {draft.filters.length === 0
                    ? <p className="text-xs text-muted-foreground/50 py-1">No filters — all records included.</p>
                    : <div className="flex flex-col gap-2">
                        {draft.filters.map(f => (
                          <FilterRow key={f.id} f={f} rootEntity={draft.root}
                            onUpdate={p => onChange({ ...draft, filters: draft.filters.map(x => x.id === f.id ? { ...x, ...p } : x) })}
                            onRemove={() => removeFilter(f.id)} />
                        ))}
                      </div>
                  }
                </Section>

                <Section title="Order" count={draft.order.length} onAdd={addOrder} defaultOpen={draft.order.length > 0}>
                  {draft.order.length === 0
                    ? <p className="text-xs text-muted-foreground/50 py-1">No ordering defined.</p>
                    : <div className="flex flex-col gap-2">
                        {draft.order.map(o => (
                          <div key={o.id} className="flex items-center gap-2">
                            <select value={o.field} onChange={e => updateOrder(o.id, { field: e.target.value })}
                              className="flex-1 rounded-md border bg-background px-2.5 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
                              {availableOrderFields.length === 0 && <option value="">—</option>}
                              {availableOrderFields.map(f => <option key={f} value={f}>{f}</option>)}
                            </select>
                            <select value={o.direction} onChange={e => updateOrder(o.id, { direction: e.target.value as 'asc' | 'desc' })}
                              className="w-20 shrink-0 rounded-md border bg-background px-2.5 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
                              <option value="asc">asc</option><option value="desc">desc</option>
                            </select>
                            <button onClick={() => removeOrder(o.id)} className="shrink-0 text-muted-foreground/30 hover:text-red-500 transition-colors"><X className="h-3.5 w-3.5" /></button>
                          </div>
                        ))}
                      </div>
                  }
                </Section>
              </div>
            </div>
          }
          yamlContent={
            yamlEditorProps ? (
              <YamlEditor
                {...yamlEditorProps}
                hideHeader
                hideSaveButton
              />
            ) : null
          }
          validErrors={validErrors}
        />

        {/* Results */}
        {result && <ResultsTable result={result} />}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function QueriesPage() {
  const { api, model } = useModel()

  const [selected, setSelected] = useState<string | null>(null)
  const [isNewMode, setIsNewMode] = useState(false)
  const [aiOpen, setAiOpen] = useState(false)
  const [draft, setDraft] = useState<QueryDraft>(emptyDraft())
  const [editMode, setEditMode] = useState<EditMode>('yaml')

  const [savedYaml, setSavedYaml] = useState('')
  const [yamlEditorContent, setYamlEditorContent] = useState('')

  const [result, setResult] = useState<QueryResult | null>(null)
  const [running, setRunning] = useState(false)
  const [saving, setSaving] = useState(false)
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<ValidationResult['errors']>([])
  const [entityDetails, setEntityDetails] = useState<Record<string, EntityDetail>>({})
  const [search, setSearch] = useState('')
  const [queryRefresh, setQueryRefresh] = useState(0)

  const queriesState = useApi(() => api.listQueries(), [model, queryRefresh])
  const entitiesState = useApi(() => api.listEntities(), [model])
  const detailState = useApi(
    () => selected ? api.getQuery(selected) : Promise.resolve(null),
    [model, selected, queryRefresh],
  )

  // Load query into draft when selected
  useEffect(() => {
    if (detailState.status === 'ok' && detailState.data) {
      const d = detailToDraft(detailState.data as QueryDetail & { description?: string })
      setDraft(d)
      const raw = (detailState.data as QueryDetail & { raw_yaml?: string }).raw_yaml || draftToYaml(d)
      setSavedYaml(raw)
      setYamlEditorContent(raw)
      setValidStatus('idle'); setValidErrors([]); setResult(null)
    }
  }, [detailState.status, selected])

  // Select first query on load
  useEffect(() => {
    if (queriesState.status === 'ok' && (queriesState.data as QuerySummary[]).length > 0 && selected === null && !isNewMode)
      setSelected((queriesState.data as QuerySummary[])[0].name)
  }, [queriesState.status, selected, isNewMode])

  // Reset entity cache on model change
  useEffect(() => { setEntityDetails({}) }, [model])

  async function ensureEntityDetails() {
    if (Object.keys(entityDetails).length > 0) return
    try {
      const summaries = await api.listEntities()
      const details = await Promise.all(summaries.map((s) => api.getEntity(s.name)))
      const map: Record<string, EntityDetail> = {}
      for (const d of details) map[d.name] = d
      setEntityDetails(map)
    } catch { /* autocomplete degrades gracefully */ }
  }

  // Debounced real-time validation — always validate yamlEditorContent, not
  // draftToYaml(draft), so plain-string attributes and other content not
  // representable by the builder form are not silently dropped.
  useEffect(() => {
    if (!draft.name || !draft.root) { setValidStatus('idle'); return }
    setValidStatus('pending')
    const t = setTimeout(async () => {
      try {
        const r = await api.validateInline(draft.name, yamlEditorContent)
        setValidStatus(r.valid ? 'valid' : 'invalid')
        setValidErrors(r.errors)
      } catch { setValidStatus('idle') }
    }, 800)
    return () => clearTimeout(t)
  }, [draft, yamlEditorContent])

  useEffect(() => { if (draft.root) ensureEntityDetails() }, [draft.root])

  const handleRun = useCallback(async () => {
    if (!draft.root) return
    setRunning(true)
    try {
      const name = draft.name || selected || 'query'
      setResult(await api.executeInlineYaml(name, yamlEditorContent))
    }
    catch (e) { console.error(e) }
    finally { setRunning(false) }
  }, [api, draft, selected, yamlEditorContent])

  // Keyboard shortcuts
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const mod = e.metaKey || e.ctrlKey
      if (mod && e.key === 'Enter') { e.preventDefault(); handleRun() }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [handleRun])

  // isDirty: yamlEditorContent is the canonical current YAML in both modes.
  // Builder changes sync into yamlEditorContent via handleBuilderChange below.
  const isDirty = yamlEditorContent !== savedYaml

  // When the user edits in builder mode, sync yamlEditorContent so that
  // isDirty, the debounced validator, and handleSave all see the same YAML.
  function handleBuilderChange(newDraft: QueryDraft) {
    setDraft(newDraft)
    setYamlEditorContent(draftToYaml(newDraft))
  }

  async function handleSave() {
    setSaving(true)
    setValidErrors([])
    try {
      const yaml = yamlEditorContent
      const yamlName = yaml.match(/^name:\s*(\S+)/m)?.[1]
      const name = draft.name || yamlName || selected || 'new_query'
      // Validate before saving
      const validation = await api.validateInline(name, yaml)
      if (!validation.valid) {
        setValidStatus('invalid')
        setValidErrors(validation.errors)
        return
      }
      await api.saveQuery(name, yaml)
      setSavedYaml(yaml)
      setYamlEditorContent(yaml)
      setSelected(name)
      setIsNewMode(false)
      setQueryRefresh(c => c + 1)
    } catch (e) {
      setValidErrors([{ path: '', message: String(e) }])
      setValidStatus('invalid')
    } finally {
      setSaving(false)
    }
  }

  function handleEditModeChange(next: EditMode) {
    if (isDirty && !isNewMode) return // blocked
    setEditMode(next)
    if (next === 'yaml') {
      setYamlEditorContent(savedYaml)
    }
    // Sync yaml → builder: isDirty=false means yamlContent===savedYaml===draftToYaml(draft)
  }

  function startNew() {
    setSelected(null)
    setIsNewMode(true)
    setDraft(emptyDraft())
    setEditMode('yaml')
    const template = `name: new_query\nroot: entity_name\nattributes:\n  - path: field_name\nmeasures:\n  - name: count\n    expr: "count(*)"\n`
    setYamlEditorContent(template)
    setSavedYaml(''); setResult(null); setValidStatus('idle'); setValidErrors([])
    ensureEntityDetails()
  }

  // Apply AI suggestion (will be wired when endpoint is ready)
  function handleAiApply(patch: Partial<QueryDraft>) {
    const next = { ...draft, ...patch }
    setDraft(next)
  }

  const queries = (queriesState.status === 'ok' ? (queriesState.data as QuerySummary[]) : [])
    .filter(q => !search || q.name.toLowerCase().includes(search.toLowerCase()))
  const entityNames = entitiesState.status === 'ok' ? (entitiesState.data as EntitySummary[]).map(e => e.name) : []

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left sidebar — query list */}
      <aside className="w-60 shrink-0 border-r flex flex-col bg-card overflow-hidden">
        <div className="px-3 py-3 border-b flex items-center justify-between shrink-0">
          <span className="text-sm font-semibold">Queries</span>
          <button onClick={startNew} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
            <Plus className="h-3.5 w-3.5" /> New
          </button>
        </div>
        <div className="px-3 py-2 border-b shrink-0">
          <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search…"
            className="w-full rounded-md border bg-muted/40 px-2.5 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring" />
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {queriesState.status === 'loading' && <div className="px-3 py-4"><Spinner /></div>}
          {queriesState.status === 'error' && <div className="px-3 py-2"><ErrorMessage message={queriesState.error} /></div>}
          {isNewMode && (
            <div className="w-full text-left px-3 py-2.5 border-b border-border/30 bg-muted">
              <div className="text-sm font-medium text-foreground/50 italic flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-orange-400 shrink-0" />
                {draft.name || 'unnamed_query'}
              </div>
            </div>
          )}
          {queries.map(q => (
            <button key={q.name} title={q.name} onClick={() => { setSelected(q.name); setIsNewMode(false) }}
              className={cn('w-full text-left px-3 py-2.5 transition-colors hover:bg-muted/50 border-b border-border/30 last:border-0', selected === q.name && 'bg-muted')}>
              <div className={cn('text-sm line-clamp-2 break-all flex items-center gap-1', selected === q.name ? 'font-medium text-foreground' : 'text-foreground/80')}>
                {q.name}
                {isDirty && selected === q.name && <span className="w-1.5 h-1.5 rounded-full bg-orange-400 shrink-0" />}
              </div>
              {q.root && <div className="text-xs text-muted-foreground/60 mt-0.5">{q.root}</div>}
            </button>
          ))}
          {queries.length === 0 && queriesState.status === 'ok' && (
            <p className="px-3 py-4 text-xs text-muted-foreground">{search ? 'No matches' : 'No saved queries'}</p>
          )}
        </div>
      </aside>

      {/* Centre — query form */}
      <div className="flex-1 overflow-hidden min-w-0 flex flex-col">
        {!selected && !isNewMode ? (
          <div className="flex flex-col items-center justify-center h-full text-center gap-3 px-6">
            <p className="text-sm text-muted-foreground">Select a query or create a new one.</p>
            <button onClick={startNew} className="flex items-center gap-1.5 border rounded-md px-3 py-1.5 text-sm hover:bg-muted transition-colors">
              <Plus className="h-4 w-4" /> New query
            </button>
          </div>
        ) : (
          <div className="flex-1 overflow-y-auto px-6 py-5">
            {detailState.status === 'loading' && !draft.name && (
              <div className="py-5"><Spinner /></div>
            )}
            <QueryForm
              draft={draft}
              onChange={handleBuilderChange}
              onChangeStructural={handleBuilderChange}
              entityDetails={entityDetails}
              entityNames={entityNames}
              validStatus={validStatus}
              validErrors={validErrors}
              saving={saving}
              onSave={handleSave}
              onRun={handleRun}
              running={running}
              result={result}
              autoFocusName={isNewMode}
              aiOpen={aiOpen}
              onToggleAi={() => setAiOpen(o => !o)}
              editMode={editMode}
              onEditModeChange={handleEditModeChange}
              isDirty={isDirty}
              onRestoreVersion={(content) => setYamlEditorContent(content)}
              yamlEditorProps={(selected || isNewMode) ? {
                name: `${selected ?? 'new_query'}.yaml`,
                historyKey: { type: 'query' as const, name: selected ?? '__new__' },
                content: yamlEditorContent,
                savedContent: savedYaml,
                onChange: setYamlEditorContent,
                onSave: isNewMode ? handleSave : async () => {
                  await api.saveQuery(selected!, yamlEditorContent)
                  setSavedYaml(yamlEditorContent)
                  setQueryRefresh(k => k + 1)
                },
              } : null}
            />
          </div>
        )}
      </div>

      {/* Right — AI assistant */}
      <AiPanel key={selected ?? '__new__'} draft={draft} onApply={handleAiApply} disabled={!draft.root} open={aiOpen} onToggle={() => setAiOpen(o => !o)} />
    </div>
  )
}
