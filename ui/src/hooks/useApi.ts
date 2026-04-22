// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'

export type ApiState<T> =
  | { status: 'loading' }
  | { status: 'error'; error: string }
  | { status: 'ok'; data: T }

export function useApi<T>(fn: () => Promise<T>, deps: unknown[] = []): ApiState<T> {
  const [state, setState] = useState<ApiState<T>>({ status: 'loading' })

  useEffect(() => {
    let cancelled = false
    setState({ status: 'loading' })
    fn()
      .then((data) => { if (!cancelled) setState({ status: 'ok', data }) })
      .catch((err: unknown) => {
        if (!cancelled) setState({ status: 'error', error: String(err) })
      })
    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)

  return state
}
