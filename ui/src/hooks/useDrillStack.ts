// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useCallback, useEffect, useRef, useState } from 'react'

// ---------------------------------------------------------------------------
// Frame type — discriminated union covering all navigable surfaces
// ---------------------------------------------------------------------------

export type DrillFrame =
  | { kind: 'view'; name: string; params: Record<string, string> }
  | { kind: 'entity-list'; entity: string }
  | { kind: 'entity-detail'; entity: string; id: string }
  | { kind: 'app'; name: string; params: Record<string, string> }

export function frameLabel(frame: DrillFrame): string {
  switch (frame.kind) {
    case 'app':
    case 'view': {
      const vals = Object.values(frame.params).filter(Boolean)
      return vals.length ? `${frame.name} · ${vals.join(', ')}` : frame.name
    }
    case 'entity-list':
      return frame.entity
    case 'entity-detail':
      return frame.id
  }
}

// ---------------------------------------------------------------------------
// URL encoding / decoding
// ---------------------------------------------------------------------------

export function encodeFrame(frame: DrillFrame): string {
  switch (frame.kind) {
    case 'view': {
      const qs = new URLSearchParams(frame.params).toString()
      return `#view/${encodeURIComponent(frame.name)}${qs ? '?' + qs : ''}`
    }
    case 'app': {
      const qs = new URLSearchParams(frame.params).toString()
      return `#app/${encodeURIComponent(frame.name)}${qs ? '?' + qs : ''}`
    }
    case 'entity-list':
      return `#entities/${encodeURIComponent(frame.entity)}`
    case 'entity-detail':
      return `#entity/${encodeURIComponent(frame.entity)}/${encodeURIComponent(frame.id)}`
  }
}

export function decodeHash(hash: string): DrillFrame | null {
  if (!hash || hash === '#') return null
  const raw = hash.startsWith('#') ? hash.slice(1) : hash
  const qIdx = raw.indexOf('?')
  const path = qIdx >= 0 ? raw.slice(0, qIdx) : raw
  const search = qIdx >= 0 ? raw.slice(qIdx + 1) : ''
  const parts = path.split('/').filter(Boolean).map(decodeURIComponent)
  const params = search ? Object.fromEntries(new URLSearchParams(search)) : {}

  if (parts[0] === 'view' && parts[1]) {
    return { kind: 'view', name: parts[1], params }
  }
  if (parts[0] === 'app' && parts[1]) {
    return { kind: 'app', name: parts[1], params }
  }
  if (parts[0] === 'entities' && parts[1]) {
    return { kind: 'entity-list', entity: parts[1] }
  }
  if (parts[0] === 'entity' && parts[1] && parts[2]) {
    return { kind: 'entity-detail', entity: parts[1], id: parts[2] }
  }
  return null
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useDrillStack(modelKey: string) {
  // Store the model key alongside the stack so a stale stack from a previous
  // model is never exposed. When modelKey changes the derived activeStack is
  // immediately [] — no setState-during-render gymnastics needed.
  const [state, setState] = useState<{ modelKey: string; stack: DrillFrame[] }>(() => {
    const frame = decodeHash(window.location.hash)
    return { modelKey, stack: frame ? [frame] : [] }
  })

  // Ref mirror — lets callbacks access current stack without stale closure deps
  const stackRef = useRef(state.stack)
  // Suppress the popstate handler when we triggered history.go ourselves
  const navigatingRef = useRef(false)

  // Keep ref in sync with state
  useEffect(() => { stackRef.current = state.stack }, [state.stack])

  // Clear stack when model changes — strip URL state so ExploreShell doesn't
  // try to reload a frame that belongs to the previous model.
  useEffect(() => {
    if (state.modelKey === modelKey) return
    stackRef.current = []
    history.replaceState(null, '', window.location.pathname)
    setState({ modelKey, stack: [] })
  }, [modelKey, state.modelKey])

  // The stack to expose: empty whenever the stored modelKey doesn't match the
  // current prop — this short-circuits stale frames between the model change
  // and the effect above committing.
  const effectiveStack = state.modelKey === modelKey ? state.stack : []

  const setStack = useCallback((newStack: DrillFrame[]) => {
    stackRef.current = newStack
    setState((s) => ({ ...s, stack: newStack }))
  }, [])

  // Restore stack on browser back / forward
  useEffect(() => {
    function onPopstate(e: PopStateEvent) {
      if (navigatingRef.current) return
      const ev = e.state as { stack?: DrillFrame[] } | null
      const newStack = ev?.stack ?? (() => {
        const f = decodeHash(window.location.hash)
        return f ? [f] : []
      })()
      setStack(newStack)
    }
    window.addEventListener('popstate', onPopstate)
    return () => window.removeEventListener('popstate', onPopstate)
  }, [setStack])

  /** Push a new frame — adds a browser history entry */
  const push = useCallback((frame: DrillFrame) => {
    const next = [...stackRef.current, frame]
    history.pushState({ stack: next }, '', encodeFrame(frame))
    setStack(next)
  }, [setStack])

  /** Navigate back to a specific depth. -1 = clear stack. */
  const navigateTo = useCallback((idx: number) => {
    const s = stackRef.current
    const newStack = idx < 0 ? [] : s.slice(0, idx + 1)
    const stepsBack = s.length - 1 - (idx < 0 ? -1 : idx)

    setStack(newStack)

    if (stepsBack > 0) {
      navigatingRef.current = true
      history.go(-stepsBack)
      setTimeout(() => { navigatingRef.current = false }, 100)
    } else {
      const frame = newStack[newStack.length - 1]
      const url = frame ? encodeFrame(frame) : window.location.pathname + window.location.search
      history.replaceState({ stack: newStack }, '', url)
    }
  }, [setStack])

  /** Replace the entire stack with a single frame (sidebar navigation) */
  const reset = useCallback((frame: DrillFrame) => {
    const newStack = [frame]
    history.replaceState({ stack: newStack }, '', encodeFrame(frame))
    setStack(newStack)
  }, [setStack])

  const current = effectiveStack[effectiveStack.length - 1] ?? null

  return { stack: effectiveStack, current, push, navigateTo, reset }
}
