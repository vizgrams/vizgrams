// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { LogOut, Shield, Pencil, Eye, Bell, UserRound } from 'lucide-react'
import { getMe, type MeResponse, type PlatformRole } from '@/api/client'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Role config — only shown for admin and creator
// ---------------------------------------------------------------------------

const ROLE_CONFIG: Record<Exclude<PlatformRole, 'viewer'>, { label: string; icon: React.ElementType; className: string }> = {
  admin: {
    label: 'Administrator',
    icon: Shield,
    className: 'text-amber-700 bg-amber-50 border-amber-200',
  },
  creator: {
    label: 'Creator',
    icon: Pencil,
    className: 'text-blue-700 bg-blue-50 border-blue-200',
  },
}

const PROVIDER_LABELS: Record<string, string> = {
  auth0: 'Auth0',
  google: 'Google',
  dev: 'Local dev',
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function SectionCard({ title, icon: Icon, children }: { title: string; icon?: React.ElementType; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border bg-card shadow-sm">
      <div className="px-5 py-4 border-b flex items-center gap-2">
        {Icon && <Icon className="h-4 w-4 text-muted-foreground" />}
        <h2 className="text-sm font-semibold">{title}</h2>
      </div>
      <div className="px-5 py-4">{children}</div>
    </div>
  )
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between py-2 text-sm">
      <span className="text-muted-foreground w-32 shrink-0">{label}</span>
      <span className="text-foreground font-medium text-right">{children}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function AccountPage() {
  const [me, setMe] = useState<MeResponse | null>(null)

  useEffect(() => {
    getMe().then(setMe).catch(() => {})
  }, [])

  const name = me?.display_name ?? me?.email ?? '—'
  const initial = name[0]?.toUpperCase() ?? '?'
  const roleConfig = me?.role && me.role !== 'viewer' ? ROLE_CONFIG[me.role] : null
  const providerLabel = me?.provider ? (PROVIDER_LABELS[me.provider] ?? me.provider) : '—'

  // Soft sign-out: clears the oauth2-proxy cookie, Auth0 session stays alive
  // so the next visit is a single click.
  const softLogoutUrl = '/oauth2/sign_out'

  // Hard sign-out: cookie cleared AND Auth0 session terminated. Constructed
  // via the rd= param so oauth2-proxy clears its cookie first, then redirects
  // to the IdP logout page. Only available when the API provides the URL
  // (set via VZ_HARD_LOGOUT_URL env var in production).
  const hardLogoutUrl = me?.hard_logout_url
    ? `/oauth2/sign_out?rd=${encodeURIComponent(me.hard_logout_url)}`
    : null

  return (
    <div className="max-w-2xl mx-auto py-8 px-4 space-y-5">

      {/* Page title */}
      <h1 className="text-xl font-semibold">Account</h1>

      {/* ── Profile hero ───────────────────────────────────────────────── */}
      <div className="rounded-lg border bg-card shadow-sm p-6">
        <div className="flex items-start gap-5">
          {/* Avatar */}
          <div className="h-16 w-16 rounded-full bg-primary/10 flex items-center justify-center shrink-0 border border-primary/20">
            <span className="text-2xl font-semibold text-primary">{initial}</span>
          </div>

          {/* Identity */}
          <div className="flex-1 min-w-0 space-y-1.5">
            <p className="text-lg font-semibold leading-tight">{name}</p>
            {me?.email && me.display_name && (
              <p className="text-sm text-muted-foreground">{me.email}</p>
            )}
            {roleConfig && (
              <span className={cn(
                'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full border text-xs font-medium',
                roleConfig.className,
              )}>
                <roleConfig.icon className="h-3 w-3" />
                {roleConfig.label}
              </span>
            )}
          </div>

          {/* Edit placeholder */}
          <button
            disabled
            className="shrink-0 text-xs text-muted-foreground/50 border border-dashed rounded-md px-3 py-1.5 cursor-not-allowed select-none"
            title="Coming soon"
          >
            Edit profile
          </button>
        </div>
      </div>

      {/* ── Two-column info / preferences ──────────────────────────────── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">

        <SectionCard title="Account" icon={UserRound}>
          <div className="divide-y">
            <Row label="Signed in via">{providerLabel}</Row>
          </div>
        </SectionCard>

        <SectionCard title="Notifications" icon={Bell}>
          <p className="text-sm text-muted-foreground py-2">
            Email preferences coming soon.
          </p>
        </SectionCard>

      </div>

      {/* ── Session ────────────────────────────────────────────────────── */}
      <SectionCard title="Session" icon={LogOut}>
        <div className="flex flex-wrap items-center gap-x-6 gap-y-3">
          <a
            href={softLogoutUrl}
            className="inline-flex items-center gap-2 text-sm text-destructive hover:text-destructive/80 transition-colors"
          >
            <LogOut className="h-4 w-4" />
            Sign out
          </a>
          {hardLogoutUrl && (
            <>
              <span className="text-border hidden sm:block">|</span>
              <a
                href={hardLogoutUrl}
                className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-destructive transition-colors"
              >
                <Eye className="h-4 w-4" />
                Sign out of all devices
              </a>
            </>
          )}
        </div>
        <p className="text-xs text-muted-foreground/60 mt-3">
          "Sign out" clears this browser session. "Sign out of all devices" also
          terminates your identity provider session, requiring a full re-login next time.
        </p>
      </SectionCard>

    </div>
  )
}
