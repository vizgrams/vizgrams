// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

type ColumnFormat = { type: string; pattern: string | null; unit: string | null }

/** Format a query result value using its column format descriptor. */
export function formatValue(value: unknown, fmt: ColumnFormat | undefined): string {
  if (!fmt) return String(value)
  if (typeof value !== 'number') return String(value)
  if (fmt.type === 'duration') {
    const h = value
    if ((fmt.unit === 'hours' || !fmt.unit) && h >= 24) return `${(h / 24).toFixed(1)}d`
    if ((fmt.unit === 'hours' || !fmt.unit) && h >= 1)  return `${h.toFixed(1)}h`
    if ((fmt.unit === 'hours' || !fmt.unit) && h >= 1 / 60) return `${Math.round(h * 60)}m`
    return `${Math.round(h * 3600)}s`
  }
  if (fmt.type === 'percent') return `${(value * 100).toFixed(1)}%`
  if (fmt.type === 'number') return value.toLocaleString()
  return String(value)
}

/** Poll a job until completed/failed. Throws on failure. */
export async function pollJob(
  getJob: (id: string) => Promise<{ status: string; error: string | null }>,
  jobId: string,
  intervalMs = 2000,
): Promise<void> {
  while (true) {
    await new Promise((r) => setTimeout(r, intervalMs))
    const job = await getJob(jobId)
    if (job.status === 'completed') return
    if (job.status === 'failed' || job.status === 'cancelled') {
      throw new Error(job.error ?? `Job ${job.status}`)
    }
  }
}
