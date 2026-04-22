// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { Code2, Wrench, History, ChevronRight, RotateCcw, Loader2 } from 'lucide-react'
import { useState, useEffect, useRef } from 'react'
import { useModel } from '@/context/ModelContext'
import type { VersionSummary, VersionDetail } from '@/api/client'
import { cn } from '@/lib/utils'

export type EditMode = 'builder' | 'yaml'

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  const m = Math.floor(diff / 60000)
  if (m < 1) return 'just now'
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

export interface EditShellProps {
  /** Current mode. Optional — omit when there is no builder (YAML-only). */
  mode?: EditMode
  /** Called when the user switches mode. Optional when no builder. */
  onModeChange?: (mode: EditMode) => void
  /** When true, switching modes is blocked */
  isDirty: boolean
  /** When omitted the mode toggle is hidden and only YAML is shown */
  builderContent?: React.ReactNode
  yamlContent: React.ReactNode
  /** Validation errors shown below the content */
  validErrors?: { path: string; message: string }[]
  /** Suppress outer border/rounded — use when embedded in a parent container that already provides a border */
  noBorder?: boolean
  /** If set, enables version history panel in YAML mode */
  historyKey?: { type: string; name: string }
  /** Called when user restores a version */
  onRestoreVersion?: (content: string) => void
}

export function EditShell({
  mode,
  onModeChange,
  isDirty,
  builderContent,
  yamlContent,
  validErrors = [],
  noBorder = false,
  historyKey,
  onRestoreVersion,
}: EditShellProps) {
  const hasBuilder = builderContent != null
  const effectiveMode: EditMode = hasBuilder ? (mode ?? 'yaml') : 'yaml'
  const { api } = useModel()

  const [versionOpen, setVersionOpen] = useState(false)
  const [versions, setVersions] = useState<VersionSummary[]>([])
  const [loadingVersions, setLoadingVersions] = useState(false)
  const [selectedVersion, setSelectedVersion] = useState<VersionDetail | null>(null)
  const [loadingDetail, setLoadingDetail] = useState(false)

  // Refs for scroll sync
  const yamlWrapperRef = useRef<HTMLDivElement>(null)
  const versionContentRef = useRef<HTMLDivElement>(null)

  function handleModeClick(next: EditMode) {
    if (next === effectiveMode || isDirty) return
    onModeChange?.(next)
  }

  // Close version panel when leaving YAML mode
  useEffect(() => {
    if (effectiveMode !== 'yaml') {
      setVersionOpen(false)
      setSelectedVersion(null)
    }
  }, [effectiveMode])

  // Load version list when panel opens
  useEffect(() => {
    if (!versionOpen || !historyKey) return
    setLoadingVersions(true)
    setSelectedVersion(null)
    api.listVersions(historyKey.type, historyKey.name)
      .then(setVersions)
      .catch(() => setVersions([]))
      .finally(() => setLoadingVersions(false))
  }, [versionOpen, historyKey?.type, historyKey?.name])

  // Scroll sync: left textarea → right version content
  useEffect(() => {
    if (!selectedVersion) return
    const textarea = yamlWrapperRef.current?.querySelector('textarea')
    const target = versionContentRef.current
    if (!textarea || !target) return
    function handler() { target!.scrollTop = textarea!.scrollTop }
    textarea.addEventListener('scroll', handler, { passive: true })
    return () => textarea.removeEventListener('scroll', handler)
  }, [selectedVersion])

  function selectVersion(v: VersionSummary) {
    if (!historyKey) return
    setLoadingDetail(true)
    api.getVersion(historyKey.type, historyKey.name, v.id)
      .then(setSelectedVersion)
      .catch(() => {})
      .finally(() => setLoadingDetail(false))
  }

  function closeVersionPanel() {
    setVersionOpen(false)
    setSelectedVersion(null)
  }

  const showVersionPanel = versionOpen && effectiveMode === 'yaml' && !!historyKey

  return (
    <div className={cn('overflow-hidden flex flex-col', !noBorder && 'border rounded-lg')}>
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 border-b bg-muted/30 shrink-0">
        {/* Mode toggle — only shown when a builder exists */}
        {hasBuilder && (
          <div className="flex items-center gap-0.5 rounded-md border bg-muted/40 p-0.5 shrink-0">
            {(['builder', 'yaml'] as EditMode[]).map((m) => {
              const isActive = effectiveMode === m
              const blocked = !isActive && isDirty
              const label = m === 'builder' ? 'Builder' : 'YAML'
              const Icon = m === 'builder' ? Wrench : Code2
              return (
                <button
                  key={m}
                  onClick={() => handleModeClick(m)}
                  disabled={blocked}
                  title={blocked ? `Save changes before switching to ${label}` : label}
                  className={cn(
                    'flex items-center gap-1.5 px-2.5 py-1 rounded text-xs transition-colors',
                    isActive
                      ? 'bg-background text-foreground font-medium shadow-sm'
                      : 'text-muted-foreground hover:text-foreground',
                    blocked && 'opacity-40 cursor-not-allowed hover:text-muted-foreground',
                  )}
                >
                  <Icon className="h-3 w-3" />
                  {label}
                </button>
              )
            })}
          </div>
        )}

        {/* Version controls — always right-aligned */}
        {historyKey && effectiveMode === 'yaml' && (
          <div className="ml-auto flex items-center gap-2 shrink-0">
            {versionOpen && selectedVersion && (
              <>
                <button
                  onClick={() => setSelectedVersion(null)}
                  className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground border rounded px-2 py-0.5 transition-colors"
                >
                  <ChevronRight className="h-3 w-3 rotate-180" />Back
                </button>
                <span className="text-xs text-muted-foreground/70">
                  v{selectedVersion.version_num} · {timeAgo(selectedVersion.created_at)}
                </span>
                <button
                  onClick={() => { onRestoreVersion?.(selectedVersion.content); closeVersionPanel() }}
                  className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground border rounded px-2 py-0.5 transition-colors"
                >
                  <RotateCcw className="h-3 w-3" />Restore
                </button>
              </>
            )}
            <button
              onClick={versionOpen ? closeVersionPanel : () => setVersionOpen(true)}
              title={versionOpen ? 'Close version history' : 'Version history'}
              className={cn('p-1 rounded transition-colors', versionOpen ? 'text-foreground bg-muted' : 'text-muted-foreground hover:text-foreground')}
            >
              <History className="h-3.5 w-3.5" />
            </button>
          </div>
        )}
      </div>

      {/* Content */}
      {effectiveMode === 'builder' ? (
        <div className="p-4">
          {builderContent}
        </div>
      ) : (
        <div className="flex min-h-[480px]">
          {/* Left: editable YAML — always present */}
          <div
            ref={yamlWrapperRef}
            className={cn('flex-1 min-w-0', showVersionPanel && 'overflow-hidden')}
          >
            {yamlContent}
          </div>

          {/* Right: version panel */}
          {showVersionPanel && (
            <div className="w-1/2 shrink-0 border-l flex flex-col overflow-hidden">
              {!selectedVersion ? (
                /* Version list — its own scroll */
                <div className="flex-1 overflow-y-auto min-h-0">
                  {loadingVersions && (
                    <div className="flex items-center justify-center py-8 gap-2 text-muted-foreground">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      <span className="text-xs">Loading…</span>
                    </div>
                  )}
                  {!loadingVersions && versions.length === 0 && (
                    <p className="text-xs text-muted-foreground text-center py-8 px-4">No versions saved yet.</p>
                  )}
                  {!loadingVersions && versions.map(v => (
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
                      {v.message && <p className="text-xs text-foreground mt-0.5 truncate">{v.message}</p>}
                    </button>
                  ))}
                  {loadingDetail && (
                    <div className="flex items-center justify-center py-4 gap-2 text-muted-foreground">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    </div>
                  )}
                </div>
              ) : (
                /* Version content — no scrollbar, synced to left textarea */
                <div
                  ref={versionContentRef}
                  className="flex-1 overflow-y-hidden min-h-0"
                >
                  <pre className="font-mono text-xs leading-relaxed px-4 py-3 whitespace-pre-wrap break-all m-0 bg-transparent">
                    {selectedVersion.content}
                  </pre>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Validation error footer */}
      {validErrors.length > 0 && (
        <div className="border-t px-4 py-3 bg-red-50 dark:bg-red-950/20 shrink-0 space-y-0.5">
          {validErrors.map((e, i) => (
            <div key={i} className="text-xs font-mono text-red-600 dark:text-red-400">
              {e.path ? `${e.path}: ` : ''}{e.message}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
