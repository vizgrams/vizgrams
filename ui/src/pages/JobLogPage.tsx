// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { CheckCircle, XCircle, Loader, Clock, ChevronDown, ChevronRight, X, RotateCcw } from 'lucide-react'
import type { JobOut } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { Card, ErrorMessage, Spinner } from '@/components/Layout'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const OP_LABELS: Record<string, string> = {
  materialize:   'Rematerialize',
  reconcile_all: 'Rematerialize All',
  extract:       'Extract',
  map:           'Map',
  reconcile:     'Reconcile Features',
}

function opLabel(op: string) {
  return OP_LABELS[op] ?? op.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

function duration(job: JobOut): string {
  if (!job.completed_at) return '—'
  const ms = new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`
}

function relativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime()
  if (diff < 60_000) return 'just now'
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return new Date(iso).toLocaleDateString()
}

function subject(job: JobOut): string {
  if (job.entity && job.entity !== '*') return job.entity
  if (job.extractor) return job.extractor
  if (job.task) return job.task
  return '—'
}

const RUNNING = new Set(['running', 'cancelling'])
// ---------------------------------------------------------------------------
// Status badge
// ---------------------------------------------------------------------------

function StatusBadge({ status }: { status: JobOut['status'] }) {
  return (
    <span className={cn(
      'inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium',
      status === 'completed' && 'bg-green-100 text-green-700',
      status === 'failed'    && 'bg-red-100 text-red-700',
      status === 'cancelled' && 'bg-muted text-muted-foreground',
      RUNNING.has(status)    && 'bg-blue-100 text-blue-700',
    )}>
      {status === 'completed' && <CheckCircle className="h-3 w-3" />}
      {status === 'failed'    && <XCircle className="h-3 w-3" />}
      {RUNNING.has(status)    && <Loader className="h-3 w-3 animate-spin" />}
      {status === 'cancelled' && <Clock className="h-3 w-3" />}
      {status}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

function JobRow({
  job, onCancel, onRerun, defaultExpanded = false,
}: {
  job: JobOut
  onCancel: (job: JobOut) => Promise<void>
  onRerun: (job: JobOut) => Promise<void>
  defaultExpanded?: boolean
}) {
  // Rows used to be clickable only when detail existed. That left running
  // jobs with no progress yet looking dead. Always expandable now; we
  // render a "no progress reported yet" hint when the lists are empty.
  const [expanded, setExpanded] = useState(defaultExpanded)
  const [busy, setBusy] = useState<'cancel' | 'rerun' | null>(null)
  const isRunning = RUNNING.has(job.status)
  const isTerminal = !isRunning  // completed, failed, cancelled
  const canRerun = isTerminal && !!(job.extractor || job.entity)

  async function handleCancel(e: React.MouseEvent) {
    e.stopPropagation()
    if (!window.confirm(`Cancel ${opLabel(job.operation)} (${subject(job)})?`)) return
    setBusy('cancel')
    try { await onCancel(job) } finally { setBusy(null) }
  }

  async function handleRerun(e: React.MouseEvent) {
    e.stopPropagation()
    setBusy('rerun')
    try { await onRerun(job) } finally { setBusy(null) }
  }

  return (
    <>
      <tr
        className="border-b last:border-0 transition-colors cursor-pointer hover:bg-muted/30"
        onClick={() => setExpanded((x) => !x)}
      >
        <td className="px-4 py-3 w-6">
          {expanded
            ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
            : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />}
        </td>
        <td className="px-4 py-3 text-sm font-medium">{opLabel(job.operation)}</td>
        <td className="px-4 py-3 text-sm text-muted-foreground font-mono">{subject(job)}</td>
        <td className="px-4 py-3"><StatusBadge status={job.status} /></td>
        <td className="px-4 py-3 text-sm text-muted-foreground tabular-nums">{duration(job)}</td>
        <td className="px-4 py-3 text-sm text-muted-foreground">{relativeTime(job.started_at)}</td>
        <td className="px-4 py-3 text-right whitespace-nowrap">
          {isRunning && (
            <button
              onClick={handleCancel}
              disabled={busy !== null || job.status === 'cancelling'}
              className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-red-600 disabled:opacity-50 disabled:cursor-not-allowed"
              title="Cancel this job"
            >
              <X className="h-3 w-3" />
              {busy === 'cancel' ? 'Cancelling…' : 'Cancel'}
            </button>
          )}
          {canRerun && (
            <button
              onClick={handleRerun}
              disabled={busy !== null}
              className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-50 disabled:cursor-not-allowed"
              title="Run the same operation again"
            >
              <RotateCcw className="h-3 w-3" />
              {busy === 'rerun' ? 'Starting…' : 'Rerun'}
            </button>
          )}
        </td>
      </tr>
      {expanded && (
        <tr className="border-b bg-muted/20">
          <td colSpan={7} className="px-8 py-3 space-y-2">
            {job.error && (
              <p className="text-sm text-red-700 font-mono whitespace-pre-wrap">{job.error}</p>
            )}
            {job.warnings.map((w, i) => (
              <p key={i} className="text-sm text-yellow-700">{w}</p>
            ))}
            {job.progress.length > 0 ? (
              <ul className="text-xs text-muted-foreground font-mono space-y-0.5 max-h-72 overflow-y-auto">
                {job.progress.map((p, i) => <li key={i}>{p}</li>)}
              </ul>
            ) : !job.error ? (
              <p className="text-xs text-muted-foreground italic">
                {isRunning
                  ? 'No progress reported yet — auto-refreshing.'
                  : 'No progress details were recorded for this job.'}
              </p>
            ) : null}
          </td>
        </tr>
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// Filter tabs
// ---------------------------------------------------------------------------

type Filter = 'all' | 'running' | 'failed'
const FILTERS: { key: Filter; label: string }[] = [
  { key: 'all',     label: 'All' },
  { key: 'running', label: 'Running' },
  { key: 'failed',  label: 'Failed' },
]

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

const POLL_MS = 4000
const LIMIT = 50

export function JobLogPage() {
  const { api, model } = useModel()
  const [filter, setFilter] = useState<Filter>('all')
  const [jobs, setJobs] = useState<JobOut[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  async function load() {
    try {
      const params = { limit: LIMIT, ...(filter === 'running' ? { status: 'running' } : filter === 'failed' ? { status: 'failed' } : {}) }
      const data = await api.listJobs(params)
      setJobs(data)
      setError(null)
    } catch (e) {
      setError(String(e))
    }
  }

  // Cancel a running job. The batch service flips it to 'cancelling';
  // the next poll picks up the final 'cancelled' state.
  async function cancel(job: JobOut) {
    setActionError(null)
    try {
      await api.cancelJob(job.job_id)
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e))
    }
  }

  // Rerun a terminal job by re-issuing the same operation. The batch
  // service uses the sentinel '__all__' as the entity for "run all
  // mappers / materialize every entity" jobs — dispatching those to
  // the per-entity endpoints would 422 since '__all__' violates the
  // entity-name regex. Route them to the *-all endpoints instead.
  async function rerun(job: JobOut) {
    setActionError(null)
    const isAll = job.entity === '__all__'
    try {
      if (job.operation === 'extract' && job.extractor) {
        await api.runExtractor(job.extractor, job.task ?? undefined)
      } else if (job.operation === 'map' && isAll) {
        await api.runAllMappers()
      } else if (job.operation === 'map' && job.entity) {
        await api.runMapper(job.entity)
      } else if (job.operation === 'reconcile_all' || (job.operation === 'materialize' && isAll)) {
        await api.reconcileAll()
      } else if (job.operation === 'materialize' && job.entity) {
        await api.rematerializeEntity(job.entity)
      } else {
        throw new Error(`Don't know how to rerun operation "${job.operation}".`)
      }
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e))
    }
  }

  // Reload when model or filter changes
  useEffect(() => {
    setJobs(null)
    load()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, filter])

  // Auto-refresh while any running jobs present
  useEffect(() => {
    const hasRunning = jobs?.some((j) => RUNNING.has(j.status)) ?? false
    if (!hasRunning) return
    const id = setInterval(load, POLL_MS)
    return () => clearInterval(id)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobs])

  const runningCount = jobs?.filter((j) => RUNNING.has(j.status)).length ?? 0

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Jobs</h1>
        {runningCount > 0 && (
          <span className="text-sm text-muted-foreground flex items-center gap-1.5">
            <Loader className="h-3.5 w-3.5 animate-spin" />
            {runningCount} running
          </span>
        )}
      </div>

      {/* Filter tabs */}
      <div className="flex gap-1 border-b">
        {FILTERS.map(({ key, label }) => (
          <button
            key={key}
            onClick={() => setFilter(key)}
            className={cn(
              'px-4 py-2 text-sm -mb-px border-b-2 transition-colors',
              filter === key
                ? 'border-foreground text-foreground font-medium'
                : 'border-transparent text-muted-foreground hover:text-foreground',
            )}
          >
            {label}
          </button>
        ))}
      </div>

      {error && <ErrorMessage message={error} />}
      {actionError && <ErrorMessage message={actionError} />}

      {!jobs && !error && <Spinner />}

      {jobs && (
        <Card className="p-0 overflow-hidden">
          {jobs.length === 0 ? (
            <p className="px-4 py-10 text-center text-sm text-muted-foreground">No jobs found.</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="w-6 px-4 py-2.5" />
                  <th className="text-left px-4 py-2.5 font-medium">Operation</th>
                  <th className="text-left px-4 py-2.5 font-medium">Subject</th>
                  <th className="text-left px-4 py-2.5 font-medium">Status</th>
                  <th className="text-left px-4 py-2.5 font-medium">Duration</th>
                  <th className="text-left px-4 py-2.5 font-medium">Started</th>
                  <th className="text-right px-4 py-2.5 font-medium w-32">Actions</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <JobRow
                    key={job.job_id}
                    job={job}
                    onCancel={cancel}
                    onRerun={rerun}
                    // Auto-expand running jobs so the progress tail is
                    // the default view when someone opens the page.
                    defaultExpanded={RUNNING.has(job.status)}
                  />
                ))}
              </tbody>
            </table>
          )}
        </Card>
      )}
    </div>
  )
}
