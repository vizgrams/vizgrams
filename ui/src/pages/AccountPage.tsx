// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { LogOut, Shield, User } from 'lucide-react'
import { getMe } from '@/api/client'

type Me = { email: string | null; is_system_admin: boolean }

export function AccountPage() {
  const [me, setMe] = useState<Me | null>(null)

  useEffect(() => {
    getMe().then(setMe).catch(() => {})
  }, [])

  const initial = me?.email?.[0]?.toUpperCase() ?? '?'

  return (
    <div className="max-w-sm mx-auto py-12 px-4">
      <h1 className="text-xl font-semibold mb-6">Account</h1>

      <div className="rounded-lg border bg-card shadow-sm p-6 space-y-5">
        {/* Avatar + identity */}
        <div className="flex items-center gap-4">
          <div className="h-12 w-12 rounded-full bg-primary/10 flex items-center justify-center shrink-0">
            {me ? (
              <span className="text-lg font-semibold text-primary">{initial}</span>
            ) : (
              <User className="h-5 w-5 text-muted-foreground" />
            )}
          </div>
          <div className="min-w-0">
            <p className="font-medium truncate">{me?.email ?? '—'}</p>
            {me?.is_system_admin && (
              <span className="inline-flex items-center gap-1 text-xs text-amber-600 font-medium mt-0.5">
                <Shield className="h-3 w-3" />
                System Administrator
              </span>
            )}
          </div>
        </div>

        <hr />

        {/* Sign out */}
        <a
          href="/oauth2/sign_out"
          className="inline-flex items-center gap-2 text-sm text-destructive hover:text-destructive/80 transition-colors"
        >
          <LogOut className="h-4 w-4" />
          Sign out
        </a>
      </div>
    </div>
  )
}
