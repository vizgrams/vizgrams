// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import { makeApi, listModels } from '@/api/client'

const STORAGE_KEY = 'explore_model'

interface ModelContextValue {
  model: string
  setModel: (model: string) => void
  api: ReturnType<typeof makeApi>
}

const ModelContext = createContext<ModelContextValue | null>(null)

export function ModelProvider({ children }: { children: ReactNode }) {
  const [model, setModelState] = useState<string>(() => localStorage.getItem(STORAGE_KEY) ?? '')
  const [ready, setReady] = useState(false)

  useEffect(() => {
    // Always validate on load — the saved model may have been deleted or renamed.
    // Falls back to the first available model if the saved one no longer exists.
    listModels()
      .then(models => {
        const saved = localStorage.getItem(STORAGE_KEY)
        const exists = models.some(m => m.name === saved)
        if (!exists && models.length > 0) setModel(models[0].name)
      })
      .catch(() => {})
      .finally(() => setReady(true))
  }, [])

  function setModel(m: string) {
    localStorage.setItem(STORAGE_KEY, m)
    setModelState(m)
  }

  if (!ready) return null

  return (
    <ModelContext.Provider value={{ model, setModel, api: makeApi(model) }}>
      {children}
    </ModelContext.Provider>
  )
}

export function useModel() {
  const ctx = useContext(ModelContext)
  if (!ctx) throw new Error('useModel must be used inside ModelProvider')
  return ctx
}
