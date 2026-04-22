// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { cn } from '@/lib/utils'

export type ValidStatus = 'idle' | 'pending' | 'valid' | 'invalid'

export function StatusBadge({ status, errorCount }: { status: ValidStatus; errorCount: number }) {
  if (status === 'idle') return null
  return (
    <span className={cn('inline-flex items-center gap-1.5 text-xs rounded-full px-2 py-0.5 font-medium', {
      'bg-yellow-100 text-yellow-700': status === 'pending',
      'bg-green-100 text-green-700': status === 'valid',
      'bg-red-100 text-red-700': status === 'invalid',
    })}>
      <span className={cn('w-1.5 h-1.5 rounded-full', {
        'bg-yellow-500 animate-pulse': status === 'pending',
        'bg-green-500': status === 'valid',
        'bg-red-500': status === 'invalid',
      })} />
      {status === 'pending' && 'Checking…'}
      {status === 'valid' && 'Valid'}
      {status === 'invalid' && `${errorCount} error${errorCount !== 1 ? 's' : ''}`}
    </span>
  )
}
