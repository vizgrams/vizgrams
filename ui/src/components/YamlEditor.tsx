// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useRef, useState } from 'react'
import { Save, Loader2, Check, Play, X, History, RotateCcw, ChevronRight } from 'lucide-react'
import { useModel } from '@/context/ModelContext'
import type { VersionSummary, VersionDetail } from '@/api/client'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Version history panel
// ---------------------------------------------------------------------------

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  const m = Math.floor(diff / 60000)
  if (m < 1) return 'just now'
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  const d = Math.floor(h / 24)
  return `${d}d ago`
}

export function VersionPanel({
  artifactType,
  artifactName,
  onRestore,
  onClose,
}: {
  artifactType: string
  artifactName: string
  onRestore: (content: string) => void
  onClose: () => void
}) {
  const { api } = useModel()
  const [versions, setVersions] = useState<VersionSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [selected, setSelected] = useState<VersionDetail | null>(null)
  const [loadingDetail, setLoadingDetail] = useState(false)

  useEffect(() => {
    setLoading(true)
    setSelected(null)
    api.listVersions(artifactType, artifactName)
      .then(setVersions)
      .catch(() => setVersions([]))
      .finally(() => setLoading(false))
  }, [artifactType, artifactName])

  function selectVersion(v: VersionSummary) {
    setLoadingDetail(true)
    api.getVersion(artifactType, artifactName, v.id)
      .then(setSelected)
      .catch(() => {})
      .finally(() => setLoadingDetail(false))
  }

  return (
    <div className="flex flex-col h-full border-l bg-card min-w-0">
      {/* Panel header */}
      <div className="flex items-center gap-2 px-3 py-2 border-b shrink-0">
        <History className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="text-xs font-medium flex-1">Version history</span>
        <button onClick={onClose} className="text-muted-foreground hover:text-foreground transition-colors">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {selected ? (
        /* Detail view */
        <div className="flex flex-col h-full min-h-0">
          <div className="flex items-center gap-2 px-3 py-2 border-b shrink-0">
            <button onClick={() => setSelected(null)}
              className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1 transition-colors">
              <ChevronRight className="h-3 w-3 rotate-180" /> Back
            </button>
            <span className="text-xs text-muted-foreground ml-auto">
              v{selected.version_num} · {timeAgo(selected.created_at)}
            </span>
          </div>
          <pre className="flex-1 min-h-0 overflow-auto text-xs font-mono px-3 py-2 text-muted-foreground leading-relaxed whitespace-pre-wrap">
            {selected.content}
          </pre>
          <div className="shrink-0 px-3 py-2 border-t">
            <button
              onClick={() => { onRestore(selected.content); onClose() }}
              className="w-full flex items-center justify-center gap-1.5 text-xs px-2.5 py-1.5 rounded border border-border hover:bg-muted transition-colors"
            >
              <RotateCcw className="h-3 w-3" /> Restore this version
            </button>
          </div>
        </div>
      ) : (
        /* List view */
        <div className="flex-1 min-h-0 overflow-y-auto">
          {loading && (
            <div className="flex items-center justify-center py-8 text-muted-foreground gap-2">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              <span className="text-xs">Loading…</span>
            </div>
          )}
          {!loading && versions.length === 0 && (
            <p className="text-xs text-muted-foreground text-center py-8 px-4">
              No versions saved yet.<br />Save this file to create v1.
            </p>
          )}
          {!loading && versions.map(v => (
            <button
              key={v.id}
              onClick={() => selectVersion(v)}
              className="w-full text-left px-3 py-2 hover:bg-muted/60 border-b border-border/40 transition-colors"
            >
              <div className="flex items-center gap-2">
                <span className="text-xs font-mono text-muted-foreground shrink-0">v{v.version_num}</span>
                {v.is_current === 1 && (
                  <span className="text-[10px] px-1 py-0.5 rounded bg-green-500/10 text-green-700 font-medium shrink-0">current</span>
                )}
                <span className="text-xs text-muted-foreground ml-auto shrink-0">{timeAgo(v.created_at)}</span>
              </div>
              {v.message && (
                <p className="text-xs text-foreground mt-0.5 truncate">{v.message}</p>
              )}
            </button>
          ))}
        </div>
      )}
      {loadingDetail && (
        <div className="absolute inset-0 flex items-center justify-center bg-background/60">
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// YamlEditor
// ---------------------------------------------------------------------------

export interface YamlEditorProps {
  /** Filename or label shown in the header */
  name: string
  content: string
  /** Reference content used to detect dirty state */
  savedContent: string
  onChange: (v: string) => void
  onSave: () => Promise<void>
  /** Optional run action — disabled when there are unsaved changes */
  onRun?: () => Promise<void>
  runLabel?: string
  loading?: boolean
  placeholder?: string
  /** Extra content rendered in the header (replaces filename) */
  headerSlot?: React.ReactNode
  /** If set, enables the version history panel */
  historyKey?: { type: string; name: string }
  /** Hide the built-in Save button (parent owns saving) */
  hideSaveButton?: boolean
  /** Hide the entire header row (name, save, run, history buttons) */
  hideHeader?: boolean
}

type RunState = 'idle' | 'running' | 'done' | 'error'

export function YamlEditor({
  name,
  content,
  savedContent,
  onChange,
  onSave,
  onRun,
  runLabel = 'Run',
  loading = false,
  placeholder,
  headerSlot,
  historyKey,
  hideSaveButton = false,
  hideHeader = false,
}: YamlEditorProps) {
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)
  const [runState, setRunState] = useState<RunState>('idle')
  const [runError, setRunError] = useState<string | null>(null)
  const [showHistory, setShowHistory] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const dirty = content !== savedContent

  // Reset run state and close history when selection changes
  useEffect(() => {
    setRunState('idle')
    setRunError(null)
    setShowHistory(false)
  }, [name])

  async function handleSave() {
    if (!dirty || saving) return
    setSaving(true)
    setSaveError(null)
    setSaved(false)
    try {
      await onSave()
      setSaved(true)
      setTimeout(() => setSaved(false), 2000)
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  async function handleRun() {
    if (!onRun || dirty || runState === 'running') return
    setRunState('running')
    setRunError(null)
    try {
      await onRun()
      setRunState('done')
      setTimeout(() => setRunState('idle'), 3000)
    } catch (e) {
      setRunState('error')
      setRunError(e instanceof Error ? e.message : 'Run failed')
      setTimeout(() => setRunState('idle'), 4000)
    }
  }

  useEffect(() => {
    function handler(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault()
        handleSave()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [dirty, saving, content])

  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Header */}
      {!hideHeader && <div className="flex items-center gap-3 px-4 py-2 border-b shrink-0 bg-card">
        {headerSlot ?? (
          <span className="text-xs font-mono text-muted-foreground flex-1 truncate">{name}</span>
        )}

        <div className="flex items-center gap-1.5 shrink-0 ml-auto">
          {/* History button */}
          {historyKey && (
            <button
              onClick={() => setShowHistory(o => !o)}
              title="Version history"
              className={cn(
                'p-1 rounded transition-colors',
                showHistory
                  ? 'text-foreground bg-muted'
                  : 'text-muted-foreground hover:text-foreground',
              )}
            >
              <History className="h-3.5 w-3.5" />
            </button>
          )}

          {/* Run button */}
          {onRun && (
            <button
              onClick={handleRun}
              disabled={dirty || runState === 'running'}
              title={dirty ? 'Save before running' : runLabel}
              className={cn(
                'flex items-center gap-1.5 text-xs px-2.5 py-1 rounded border transition-colors',
                dirty || runState === 'running'
                  ? 'border-transparent text-muted-foreground/40 cursor-default'
                  : runState === 'done'
                    ? 'border-green-500/40 text-green-600'
                    : runState === 'error'
                      ? 'border-red-500/40 text-red-500'
                      : 'border-border text-foreground hover:bg-muted',
              )}
            >
              {runState === 'running' ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : runState === 'done' ? (
                <Check className="h-3 w-3" />
              ) : runState === 'error' ? (
                <X className="h-3 w-3" />
              ) : (
                <Play className="h-3 w-3" />
              )}
              {runState === 'running' ? 'Running…' : runState === 'done' ? 'Done' : runState === 'error' ? 'Failed' : runLabel}
            </button>
          )}

          {/* Dirty indicator */}
          {!hideSaveButton && dirty && !saving && (
            <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />
          )}

          {/* Save button */}
          {!hideSaveButton && (
            <button
              onClick={handleSave}
              disabled={!dirty || saving}
              className={cn(
                'flex items-center gap-1.5 text-xs px-2.5 py-1 rounded border transition-colors',
                dirty && !saving
                  ? 'border-border text-foreground hover:bg-muted'
                  : 'border-transparent text-muted-foreground/40 cursor-default',
              )}
            >
              {saving ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : saved ? (
                <Check className="h-3 w-3 text-green-500" />
              ) : (
                <Save className="h-3 w-3" />
              )}
              {saving ? 'Saving…' : saved ? 'Saved' : 'Save'}
            </button>
          )}
        </div>
      </div>}

      {/* Body: editor + optional history panel side by side */}
      <div className="flex flex-1 min-h-0">
        {/* Editor */}
        <div className="flex flex-col flex-1 min-w-0 min-h-0">
          <div className="flex-1 min-h-[480px] relative">
            {loading ? (
              <div className="flex items-center justify-center h-full text-muted-foreground text-sm gap-2">
                <Loader2 className="h-4 w-4 animate-spin" /> Loading…
              </div>
            ) : (
              <textarea
                ref={textareaRef}
                value={content}
                onChange={(e) => onChange(e.target.value)}
                placeholder={placeholder ?? '# YAML content…'}
                spellCheck={false}
                className="w-full h-full resize-none font-mono text-xs leading-relaxed px-4 py-3 bg-background focus:outline-none"
              />
            )}
          </div>

          {/* Error footer */}
          {(saveError || runError) && (
            <div className="shrink-0 px-4 py-2 border-t text-xs text-red-500 bg-red-500/5 font-mono">
              {saveError || runError}
            </div>
          )}
        </div>

        {/* History panel */}
        {showHistory && historyKey && (
          <div className="relative w-64 shrink-0 flex flex-col min-h-0">
            <VersionPanel
              artifactType={historyKey.type}
              artifactName={historyKey.name}
              onRestore={(c) => { onChange(c) }}
              onClose={() => setShowHistory(false)}
            />
          </div>
        )}
      </div>
    </div>
  )
}
