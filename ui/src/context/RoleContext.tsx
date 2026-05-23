// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { createContext, useContext, useEffect, useState } from 'react'
import { getMe } from '@/api/client'
import type { PlatformRole, MeResponse } from '@/api/client'

interface RoleState {
  email: string | null
  userId: string | null
  role: PlatformRole
  loading: boolean
}

const RoleContext = createContext<RoleState>({
  email: null,
  userId: null,
  role: 'viewer',
  loading: true,
})

export function RoleProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<RoleState>({
    email: null, userId: null, role: 'viewer', loading: true,
  })

  useEffect(() => {
    getMe()
      .then((me: MeResponse) => setState({
        email: me.email, userId: me.user_id, role: me.role ?? 'viewer', loading: false,
      }))
      .catch(() => setState({ email: null, userId: null, role: 'viewer', loading: false }))
  }, [])

  return <RoleContext.Provider value={state}>{children}</RoleContext.Provider>
}

export function useRole(): RoleState {
  return useContext(RoleContext)
}
