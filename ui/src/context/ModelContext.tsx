// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import { makeApi, listModels } from '@/api/client'

interface ModelContextValue {
  model: string
  api: ReturnType<typeof makeApi>
}

const ModelContext = createContext<ModelContextValue | null>(null)

export function ModelProvider({ children }: { children: ReactNode }) {
  const [model, setModel] = useState<string>('')
  const [ready, setReady] = useState(false)

  useEffect(() => {
    listModels()
      .then(models => {
        const active = models.find(m => m.is_active)?.name ?? models[0]?.name ?? ''
        setModel(active)
      })
      .catch(() => {})
      .finally(() => setReady(true))
  }, [])

  if (!ready) return null

  return (
    <ModelContext.Provider value={{ model, api: makeApi(model) }}>
      {children}
    </ModelContext.Provider>
  )
}

export function useModel() {
  const ctx = useContext(ModelContext)
  if (!ctx) throw new Error('useModel must be used inside ModelProvider')
  return ctx
}
