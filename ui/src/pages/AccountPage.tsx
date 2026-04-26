// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { LogOut, Shield, Pencil, Star, Eye } from 'lucide-react'
import { getMe, type MeResponse, type PlatformRole } from '@/api/client'

const ROLE_CONFIG: Record<PlatformRole, { label: string; icon: React.ElementType; className: string }> = {
  admin: {
    label: 'Administrator',
    icon: Shield,
    className: 'text-amber-600 bg-amber-50 border-amber-200',
  },
  creator: {
    label: 'Creator',
    icon: Pencil,
    className: 'text-blue-600 bg-blue-50 border-blue-200',
  },
  viewer: {
    label: 'Viewer',
    icon: Eye,
    className: 'text-muted-foreground bg-muted border-border',
  },
}

const PROVIDER_LABELS: Record<string, string> = {
  auth0: 'Auth0',
  google: 'Google',
  dev: 'Local dev',
}

function RoleBadge({ role }: { role: PlatformRole }) {
  const { label, icon: Icon, className } = ROLE_CONFIG[role]
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs font-medium ${className}`}>
      <Icon className="h-3 w-3" />
      {label}
    </span>
  )
}

export function AccountPage() {
  const [me, setMe] = useState<MeResponse | null>(null)

  useEffect(() => {
    getMe().then(setMe).catch(() => {})
  }, [])

  const name = me?.display_name ?? me?.email ?? '—'
  const initial = name[0]?.toUpperCase() ?? '?'
  const providerLabel = me?.provider ? (PROVIDER_LABELS[me.provider] ?? me.provider) : null

  return (
    <div className="max-w-sm mx-auto py-12 px-4">
      <h1 className="text-xl font-semibold mb-6">Account</h1>

      <div className="rounded-lg border bg-card shadow-sm divide-y">

        {/* Identity */}
        <div className="p-6 flex items-center gap-4">
          <div className="h-14 w-14 rounded-full bg-primary/10 flex items-center justify-center shrink-0">
            <span className="text-xl font-semibold text-primary">{initial}</span>
          </div>
          <div className="min-w-0 space-y-1">
            <p className="font-semibold text-base truncate">{name}</p>
            {me?.email && me.display_name && (
              <p className="text-sm text-muted-foreground truncate">{me.email}</p>
            )}
            {me && <RoleBadge role={me.role} />}
          </div>
        </div>

        {/* Details */}
        {providerLabel && (
          <div className="px-6 py-4">
            <dl className="space-y-2 text-sm">
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Signed in via</dt>
                <dd className="font-medium">{providerLabel}</dd>
              </div>
            </dl>
          </div>
        )}

        {/* Notifications placeholder */}
        <div className="px-6 py-4">
          <p className="text-sm font-medium mb-1">Notifications</p>
          <p className="text-xs text-muted-foreground">Email preferences coming soon.</p>
        </div>

        {/* Sign out */}
        <div className="px-6 py-4">
          <a
            href="/oauth2/sign_out"
            className="inline-flex items-center gap-2 text-sm text-destructive hover:text-destructive/80 transition-colors"
          >
            <LogOut className="h-4 w-4" />
            Sign out
          </a>
        </div>
      </div>
    </div>
  )
}
