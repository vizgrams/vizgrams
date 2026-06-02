// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { LogOut, Shield, Pencil, Bell, UserRound } from 'lucide-react'
import { getMe, type MeResponse, type PlatformRole } from '@/api/client'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Role config — only shown for admin and member (VG-292: creator collapsed into member).
// ---------------------------------------------------------------------------

const ROLE_CONFIG: Record<Exclude<PlatformRole, 'viewer'>, { label: string; icon: React.ElementType; className: string }> = {
  admin: {
    label: 'Administrator',
    icon: Shield,
    className: 'text-amber-700 bg-amber-50 border-amber-200',
  },
  member: {
    label: 'Member',
    icon: Pencil,
    className: 'text-blue-700 bg-blue-50 border-blue-200',
  },
}

const PROVIDER_LABELS: Record<string, string> = {
  auth0: 'Auth0',
  google: 'Google',
  entra: 'Microsoft Entra ID',
  okta: 'Okta',
  dex: 'Dex',
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

  // Sign-out has to terminate BOTH the oauth2-proxy cookie AND the IdP
  // (Auth0) session — otherwise oauth2-proxy bounces straight back to
  // Auth0, sees a live SSO session, and silently re-authenticates the
  // same user. Indistinguishable from "the button didn't work".
  //
  // We construct it as a soft sign-out URL with rd= pointing at the IdP
  // logout endpoint, so oauth2-proxy clears its cookie *first* and then
  // redirects to terminate the IdP session. Falls back to soft-only if
  // the API hasn't supplied a hard-logout URL (e.g. local dev with no
  // VZ_HARD_LOGOUT_URL).
  const logoutUrl = me?.hard_logout_url
    ? `/oauth2/sign_out?rd=${encodeURIComponent(me.hard_logout_url)}`
    : '/oauth2/sign_out'

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
        <a
          href={logoutUrl}
          className="inline-flex items-center gap-2 text-sm text-destructive hover:text-destructive/80 transition-colors"
        >
          <LogOut className="h-4 w-4" />
          Sign out
        </a>
        <p className="text-xs text-muted-foreground/60 mt-3">
          Signs you out of vizgrams and your identity provider — you'll need to
          re-enter your credentials next time you visit.
        </p>
      </SectionCard>

    </div>
  )
}
