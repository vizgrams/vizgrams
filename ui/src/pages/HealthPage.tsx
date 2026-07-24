// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { CheckCircle, AlertTriangle, XCircle, PlayCircle, ShieldAlert } from 'lucide-react'
import type { HealthReport, HealthTarget } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { Card, ErrorMessage, Spinner } from '@/components/Layout'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SECTION_LABELS: Record<string, string> = {
  extract:     'Extract',
  map:         'Map',
  materialize: 'Materialize',
  reconcile:   'Reconcile',
}

const SECTION_HINTS: Record<string, string> = {
  extract:     'Per-tool. Cron-driven; failures cap at N before scheduler stops firing.',
  map:         'One wave per model. Runs when any scheduled mapper is due.',
  materialize: 'One wave per model. Rebuilds entity tables and materialised features.',
  reconcile:   'Manual only. Recomputes feature values against the current entity tables.',
}

// Freshness heuristics. Backend returns raw signals; the page decides colour.
// Anything past these thresholds gets nudged to warning/critical even if the
// last attempt technically "completed" — a "successful" reconcile 34 days ago
// is still the DORA-null-cliff situation.
const STALE_DAYS_MANUAL_WARN = 7
const STALE_DAYS_MANUAL_CRIT = 30
const STALE_DAYS_SCHEDULED_WARN = 2
const STALE_DAYS_SCHEDULED_CRIT = 7

type Level = 'ok' | 'warn' | 'crit'

function ageDays(iso: string | null): number | null {
  if (!iso) return null
  return (Date.now() - new Date(iso).getTime()) / 86_400_000
}

function isScheduled(t: HealthTarget): boolean {
  // A row is "scheduled" if it has its own cron OR is a wave row with
  // scheduled children (backend marks wave cron as the literal "wave").
  return !!t.cron || (t.scheduled_children?.length ?? 0) > 0
}

function targetLevel(t: HealthTarget): Level {
  if (t.cap_hit) return 'crit'
  if (t.last_attempt_status === 'failed') return 'warn'
  const days = ageDays(t.last_success)
  if (days == null) return 'warn'  // never ran
  if (isScheduled(t)) {
    if (days >= STALE_DAYS_SCHEDULED_CRIT) return 'crit'
    if (days >= STALE_DAYS_SCHEDULED_WARN) return 'warn'
  } else {
    if (days >= STALE_DAYS_MANUAL_CRIT) return 'crit'
    if (days >= STALE_DAYS_MANUAL_WARN) return 'warn'
  }
  return 'ok'
}

function sectionLevel(targets: HealthTarget[]): Level {
  const lvls = targets.map(targetLevel)
  if (lvls.includes('crit')) return 'crit'
  if (lvls.includes('warn')) return 'warn'
  return 'ok'
}

function relativeTime(iso: string | null): string {
  if (!iso) return 'never'
  const diff = Date.now() - new Date(iso).getTime()
  if (diff < 0) {
    const ahead = -diff
    if (ahead < 60_000) return 'in <1m'
    if (ahead < 3_600_000) return `in ${Math.round(ahead / 60_000)}m`
    if (ahead < 86_400_000) return `in ${Math.round(ahead / 3_600_000)}h`
    return `in ${Math.round(ahead / 86_400_000)}d`
  }
  if (diff < 60_000) return 'just now'
  if (diff < 3_600_000) return `${Math.round(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.round(diff / 3_600_000)}h ago`
  return `${Math.round(diff / 86_400_000)}d ago`
}

// ---------------------------------------------------------------------------
// Status badge
// ---------------------------------------------------------------------------

function LevelBadge({ level, capHit }: { level: Level; capHit: boolean }) {
  if (capHit) {
    return (
      <span className="inline-flex items-center gap-1 rounded-full bg-red-100 text-red-700 px-2 py-0.5 text-xs font-medium">
        <ShieldAlert className="h-3 w-3" /> capped
      </span>
    )
  }
  const cls = level === 'ok'
    ? 'bg-green-100 text-green-700'
    : level === 'warn'
      ? 'bg-yellow-100 text-yellow-800'
      : 'bg-red-100 text-red-700'
  const label = level === 'ok' ? 'healthy' : level === 'warn' ? 'stale' : 'critical'
  const Icon = level === 'ok' ? CheckCircle : level === 'warn' ? AlertTriangle : XCircle
  return (
    <span className={cn(
      'inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium',
      cls,
    )}>
      <Icon className="h-3 w-3" />
      {label}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

type TriggerFn = (op: string, target: HealthTarget) => Promise<void>

function TargetRow({
  operation, target, onTrigger, busyKey,
}: {
  operation: string
  target: HealthTarget
  onTrigger: TriggerFn
  busyKey: string | null
}) {
  const level = targetLevel(target)
  const key = `${operation}:${target.name}`
  const busy = busyKey === key
  const label = target.name === '__all__' ? 'all' : target.name
  return (
    <tr className="border-b last:border-0 hover:bg-muted/30">
      <td className="px-4 py-2.5 text-sm font-mono">{label}</td>
      <td className="px-4 py-2.5 text-sm text-muted-foreground">
        {target.cron === 'wave'
          ? <span className="italic">wave ({target.scheduled_children?.length ?? 0})</span>
          : target.cron
            ? <span className="font-mono text-xs">{target.cron}</span>
            : <span className="italic">manual</span>}
      </td>
      <td className="px-4 py-2.5 text-sm text-muted-foreground tabular-nums">
        {relativeTime(target.last_success)}
      </td>
      <td className="px-4 py-2.5 text-sm text-muted-foreground tabular-nums">
        {target.next_run ? relativeTime(target.next_run) : '—'}
      </td>
      <td className="px-4 py-2.5 text-sm text-muted-foreground tabular-nums">
        {target.failures_since_success > 0
          ? <span className={target.cap_hit ? 'text-red-700 font-medium' : 'text-yellow-700'}>
              {target.failures_since_success} / {target.failure_cap}
            </span>
          : <span className="text-muted-foreground/60">0</span>}
      </td>
      <td className="px-4 py-2.5"><LevelBadge level={level} capHit={target.cap_hit} /></td>
      <td className="px-4 py-2.5 text-right">
        <button
          onClick={() => onTrigger(operation, target)}
          disabled={busy}
          className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground disabled:opacity-50"
          title={target.cap_hit
            ? 'Run manually to reset the failure-cap counter'
            : 'Run now'}
        >
          <PlayCircle className="h-3.5 w-3.5" />
          {busy ? 'Starting…' : target.cap_hit ? 'Reset & run' : 'Run now'}
        </button>
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

const POLL_MS = 15_000

export function HealthPage() {
  const { api, model } = useModel()
  const [health, setHealth] = useState<HealthReport | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [busyKey, setBusyKey] = useState<string | null>(null)

  async function load() {
    try {
      const data = await api.getHealth()
      setHealth(data)
      setError(null)
    } catch (e) {
      setError(String(e))
    }
  }

  async function trigger(op: string, t: HealthTarget) {
    setActionError(null)
    setBusyKey(`${op}:${t.name}`)
    try {
      if (op === 'extract') {
        await api.runExtractor(t.name)
      } else if (op === 'map') {
        await api.runAllMappers()
      } else if (op === 'materialize') {
        await api.reconcileAll()
      } else if (op === 'reconcile') {
        await api.reconcileFeatures()
      } else {
        throw new Error(`Don't know how to trigger operation "${op}".`)
      }
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusyKey(null)
    }
  }

  useEffect(() => {
    setHealth(null)
    load()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model])

  useEffect(() => {
    const id = setInterval(load, POLL_MS)
    return () => clearInterval(id)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model])

  const cappedCount = health?.sections
    .flatMap((s) => s.targets)
    .filter((t) => t.cap_hit).length ?? 0

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">Health</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Per-model pipeline status. Compare last-success against schedule to spot silent stalls.
          </p>
        </div>
        <Link
          to="/jobs"
          className="text-sm text-muted-foreground hover:text-foreground underline underline-offset-4"
        >
          View job log →
        </Link>
      </div>

      {cappedCount > 0 && (
        <div className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800 flex items-start gap-2">
          <ShieldAlert className="h-4 w-4 mt-0.5 shrink-0" />
          <div>
            <span className="font-medium">{cappedCount} target{cappedCount > 1 ? 's are' : ' is'} blocked</span>
            {' — the scheduler stopped firing after 5+ consecutive failed runs. '}
            Click <span className="font-mono">Reset &amp; run</span> on each red row to trigger a manual run;
            once it succeeds, the counter resets and the schedule fires again.
          </div>
        </div>
      )}

      {error && <ErrorMessage message={error} />}
      {actionError && <ErrorMessage message={actionError} />}

      {!health && !error && <Spinner />}

      {health?.sections.map((section) => {
        const level = sectionLevel(section.targets)
        const children = section.targets.flatMap((t) => t.scheduled_children ?? [])
        return (
          <Card key={section.operation} className="p-0 overflow-hidden">
            <div className="flex items-baseline justify-between px-4 py-3 border-b bg-muted/30">
              <div className="flex items-baseline gap-3">
                <h2 className="text-base font-semibold">{SECTION_LABELS[section.operation] ?? section.operation}</h2>
                <LevelBadge level={level} capHit={false} />
              </div>
              <p className="text-xs text-muted-foreground max-w-md text-right">
                {SECTION_HINTS[section.operation]}
              </p>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/50">
                  <th className="text-left px-4 py-2 font-medium">Target</th>
                  <th className="text-left px-4 py-2 font-medium">Cron</th>
                  <th className="text-left px-4 py-2 font-medium">Last success</th>
                  <th className="text-left px-4 py-2 font-medium">Next run</th>
                  <th className="text-left px-4 py-2 font-medium">Failures / cap</th>
                  <th className="text-left px-4 py-2 font-medium">Status</th>
                  <th className="text-right px-4 py-2 font-medium w-32">Action</th>
                </tr>
              </thead>
              <tbody>
                {section.targets.map((t) => (
                  <TargetRow
                    key={t.name}
                    operation={section.operation}
                    target={t}
                    onTrigger={trigger}
                    busyKey={busyKey}
                  />
                ))}
              </tbody>
            </table>
            {children.length > 0 && (
              <div className="px-4 py-2 text-xs text-muted-foreground border-t bg-muted/10">
                <span className="font-medium">Scheduled: </span>
                <span className="font-mono">{children.join(', ')}</span>
              </div>
            )}
          </Card>
        )
      })}
    </div>
  )
}
