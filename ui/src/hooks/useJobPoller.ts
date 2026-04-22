// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useRef, useState } from 'react'
import type { JobOut } from '@/api/client'
import { useModel } from '@/context/ModelContext'

const POLL_INTERVAL_MS = 1500
const TERMINAL = new Set(['completed', 'failed', 'cancelled'])

export type JobState =
  | { phase: 'idle' }
  | { phase: 'running'; job: JobOut; label: string }
  | { phase: 'done'; job: JobOut; label: string }

export function useJobPoller() {
  const { api } = useModel()
  const [state, setState] = useState<JobState>({ phase: 'idle' })
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const jobIdRef = useRef<string | null>(null)

  function clear() {
    if (timerRef.current) clearTimeout(timerRef.current)
    timerRef.current = null
  }

  async function start(jobId: string, label: string) {
    clear()
    jobIdRef.current = jobId
    const job = await api.getJob(jobId)
    setState({ phase: TERMINAL.has(job.status) ? 'done' : 'running', job, label })
    if (!TERMINAL.has(job.status)) schedule(jobId, label)
  }

  function schedule(jobId: string, label: string) {
    timerRef.current = setTimeout(async () => {
      if (jobIdRef.current !== jobId) return
      try {
        const job = await api.getJob(jobId)
        const done = TERMINAL.has(job.status)
        setState({ phase: done ? 'done' : 'running', job, label })
        if (!done) schedule(jobId, label)
      } catch {
        // network blip — keep polling
        schedule(jobId, label)
      }
    }, POLL_INTERVAL_MS)
  }

  function reset() {
    clear()
    jobIdRef.current = null
    setState({ phase: 'idle' })
  }

  useEffect(() => () => clear(), [])

  return { state, start, reset }
}
