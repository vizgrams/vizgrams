// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import {
  ChevronDown, PanelLeftClose, PanelLeftOpen,
  Download, Shuffle, Sparkles,
  Layers, BarChart2, Share2,
  Compass, LayoutGrid,
  Clock, Settings, User, Database, Rss, Bookmark, Box,
} from 'lucide-react'
import { listModels } from '@/api/client'
import type { ModelSummary, ApplicationSummary } from '@/api/client'
import { useModel } from '@/context/ModelContext'
import { useRole } from '@/context/RoleContext'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// Digital Twin icon — two overlapping squares (solid = physical, dashed = digital)
// ---------------------------------------------------------------------------

function DigitalTwinIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 20 20"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-hidden="true"
    >
      {/* Digital twin — back square, dashed */}
      <rect x="7" y="2" width="11" height="11" rx="2" stroke="currentColor" strokeWidth="1.5" strokeDasharray="2.5 1.5" opacity="0.5" />
      {/* Physical — front square, solid */}
      <rect x="2" y="7" width="11" height="11" rx="2" stroke="currentColor" strokeWidth="1.5" fill="currentColor" fillOpacity="0.12" />
    </svg>
  )
}

interface LayoutProps { children: React.ReactNode }

export function Layout({ children }: LayoutProps) {
  const { pathname } = useLocation()
  const isGraph = pathname.startsWith('/graph')
  const [collapsed, setCollapsed] = useState(false)

  return (
    <div className="flex h-screen bg-background overflow-hidden">
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed((c) => !c)} />
      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <Breadcrumbs pathname={pathname} />
        <main className={cn(
          'flex-1',
          isGraph ? 'overflow-hidden' : 'overflow-y-auto px-6 py-6',
        )}>
          {children}
        </main>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sidebar
// ---------------------------------------------------------------------------

interface SidebarProps {
  collapsed: boolean
  onToggle: () => void
}

function Sidebar({ collapsed, onToggle }: SidebarProps) {
  const { model, api } = useModel()
  const { email: userEmail, role } = useRole()
  const [apps, setApps] = useState<ApplicationSummary[]>([])

  useEffect(() => {
    api.listApplications().then(setApps).catch(() => {})
  }, [model])

  return (
    <aside className={cn(
      'shrink-0 flex flex-col overflow-hidden transition-all duration-200 bg-[#2d3748]',
      collapsed ? 'w-12' : 'w-52',
    )}>
      {/* Header */}
      <div className={cn(
        'border-b border-white/10 flex items-center shrink-0',
        collapsed ? 'h-12 justify-center' : 'px-3 py-3 gap-2',
      )}>
        {!collapsed && (
          <span className="flex-1 flex items-center">
            <DigitalTwinIcon className="h-5 w-5 text-white/80" />
          </span>
        )}
        <button
          onClick={onToggle}
          className="text-white/35 hover:text-white/70 transition-colors shrink-0"
          title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {collapsed
            ? <PanelLeftOpen className="h-4 w-4" />
            : <PanelLeftClose className="h-4 w-4" />}
        </button>
      </div>

      {/* Model switcher */}
      {!collapsed && (
        <div className="px-3 py-2 border-b border-white/10">
          <ModelSwitcher dark />
        </div>
      )}
      {collapsed && (
        <div className="flex justify-center py-2 border-b border-white/10">
          <ModelSwitcherIcon dark />
        </div>
      )}

      {/* Scrollable nav */}
      <nav className={cn('flex-1 overflow-y-auto py-3 space-y-4', collapsed ? 'px-1.5' : 'px-2')}>

        {role === 'admin' && (
          <NavSection label="Admin" collapsed={collapsed} dark>
            <NavItem to="/admin/models" matchPrefix="/admin/models" icon={<Box className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Models</NavItem>
            <NavItem to="/tools" icon={<Download className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Extractors</NavItem>
            <NavItem to="/mappers" icon={<Shuffle className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Mappers</NavItem>
            <NavItem to="/entities" icon={<Layers className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Ontology</NavItem>
            <NavItem to="/jobs" icon={<Clock className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Jobs</NavItem>
          </NavSection>
        )}

        {(role === 'admin' || role === 'creator') && (
          <NavSection label="Creator" collapsed={collapsed} dark>
            <NavItem to="/features" icon={<Sparkles className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Features</NavItem>
            <NavItem to="/queries" icon={<BarChart2 className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Query Builder</NavItem>
            <NavItem to="/explore" matchExact icon={<Compass className="h-3.5 w-3.5" />} collapsed={collapsed} dark>View Builder</NavItem>
            <NavItem to="/graph" icon={<Share2 className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Graph</NavItem>
          </NavSection>
        )}

        <NavSection label="User" collapsed={collapsed} dark>
          <NavItem to="/feed" icon={<Rss className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Feed</NavItem>
          <NavItem to="/saved" icon={<Bookmark className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Saved</NavItem>
          {apps.map((a) => (
            <NavItem key={a.name} to={`/explore?app=${encodeURIComponent(a.name)}`} matchSearch={`app=${encodeURIComponent(a.name)}`} icon={<LayoutGrid className="h-3.5 w-3.5" />} collapsed={collapsed} dark>{a.name}</NavItem>
          ))}
          <NavItem to="/explore?section=entities" matchSearch="section=entities" icon={<Layers className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Entity Explorer</NavItem>
        </NavSection>

      </nav>

      {/* Bottom utilities */}
      <div className={cn('border-t border-white/10 py-2 space-y-0.5', collapsed ? 'px-1.5' : 'px-2')}>
        <NavItem disabled icon={<Settings className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Settings</NavItem>
        {userEmail ? (
          <Link
            to="/account"
            className={cn(
              'flex items-center rounded-md text-sm text-white/55 hover:bg-white/[0.07] hover:text-white/90 transition-colors min-w-0',
              collapsed ? 'justify-center p-2' : 'gap-2 px-2 py-1.5',
            )}
            title={collapsed ? userEmail : undefined}
          >
            {collapsed ? (
              <span className="h-5 w-5 rounded-full bg-white/15 flex items-center justify-center text-[10px] font-semibold text-white/70 shrink-0">
                {userEmail[0].toUpperCase()}
              </span>
            ) : (
              <>
                <User className="h-3.5 w-3.5 shrink-0" />
                <span className="truncate text-xs">{userEmail}</span>
              </>
            )}
          </Link>
        ) : (
          <NavItem disabled icon={<User className="h-3.5 w-3.5" />} collapsed={collapsed} dark>Account</NavItem>
        )}
      </div>
    </aside>
  )
}

// ---------------------------------------------------------------------------
// Nav primitives
// ---------------------------------------------------------------------------

function NavSection({ label, collapsed, dark, children }: { label: string; collapsed: boolean; dark?: boolean; children: React.ReactNode }) {
  return (
    <div>
      {!collapsed && (
        <p className={cn(
          'px-2 mb-1 text-[10px] font-semibold uppercase tracking-widest',
          dark ? 'text-white/30' : 'text-muted-foreground/50',
        )}>
          {label}
        </p>
      )}
      {collapsed && <div className={cn('mb-1 mx-auto h-px', dark ? 'bg-white/10' : 'bg-border/50')} />}
      <div className="space-y-0.5">{children}</div>
    </div>
  )
}

interface NavItemProps {
  to?: string
  icon?: React.ReactNode
  disabled?: boolean
  collapsed?: boolean
  dark?: boolean
  matchExact?: boolean
  matchPrefix?: string
  matchSearch?: string   // if set, location.search must include this string to be active
  neverActive?: boolean
  children: React.ReactNode
}

function NavItem({ to, icon, disabled, collapsed, dark, matchExact, matchPrefix, matchSearch, neverActive, children }: NavItemProps) {
  const { pathname, search } = useLocation()

  const basePath = to?.split('?')[0] ?? ''
  const pathMatch = !neverActive && !disabled && to != null && (
    matchPrefix
      ? pathname.startsWith(matchPrefix)
      : matchExact
        ? pathname === basePath
        : pathname === basePath || pathname.startsWith(basePath + '/')
  )
  const active = pathMatch && (
    matchSearch
      ? search.includes(matchSearch)
      : matchExact
        ? search === '' || search === '?'   // exact items only active when no search params
        : true
  )

  const label = typeof children === 'string' ? children : undefined

  const base = cn(
    'flex items-center rounded-md text-sm w-full transition-colors',
    collapsed ? 'justify-center p-2' : 'gap-2 px-2 py-1.5',
  )

  if (disabled || !to) {
    return (
      <span
        className={cn(base, dark ? 'text-white/20 cursor-default select-none' : 'text-muted-foreground/35 cursor-default select-none')}
        title={collapsed ? label : undefined}
      >
        {icon}
        {!collapsed && children}
      </span>
    )
  }

  return (
    <Link
      to={to}
      title={collapsed ? label : undefined}
      className={cn(
        base,
        dark
          ? active
            ? 'bg-white/10 text-white font-medium'
            : 'text-white/55 hover:bg-white/[0.07] hover:text-white/90'
          : active
            ? 'bg-primary/10 text-primary font-medium'
            : 'text-muted-foreground hover:bg-muted hover:text-foreground',
      )}
    >
      {icon}
      {!collapsed && children}
    </Link>
  )
}

// ---------------------------------------------------------------------------
// Breadcrumbs (only shown for explore drill-down)
// ---------------------------------------------------------------------------

function Breadcrumbs({ pathname }: { pathname: string }) {
  if (!pathname.startsWith('/explore/')) return null
  const parts = pathname.split('/').filter(Boolean)

  const crumbs = [
    { href: '/explore', label: 'Entities' },
    ...(parts[1] ? [{ href: `/explore/${parts[1]}`, label: parts[1] }] : []),
    ...(parts[2] ? [{ href: `/explore/${parts[1]}/${parts[2]}`, label: decodeURIComponent(parts[2]) }] : []),
  ]

  return (
    <nav className="px-6 py-2 border-b text-sm text-muted-foreground flex gap-1.5 items-center shrink-0">
      {crumbs.map((c, i) => (
        <span key={c.href} className="flex items-center gap-1.5">
          {i > 0 && <span className="opacity-40">/</span>}
          {i < crumbs.length - 1
            ? <Link to={c.href} className="hover:text-foreground transition-colors">{c.label}</Link>
            : <span className="text-foreground font-medium">{c.label}</span>
          }
        </span>
      ))}
    </nav>
  )
}

// ---------------------------------------------------------------------------
// Model switcher (full)
// ---------------------------------------------------------------------------

function ModelSwitcher({ dark }: { dark?: boolean }) {
  const { model, setModel } = useModel()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [models, setModels] = useState<ModelSummary[]>([])
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    listModels().then(setModels).catch(() => {})
  }, [])

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  function select(name: string) {
    setOpen(false)
    if (name !== model) { setModel(name); navigate('/explore') }
  }

  const current = models.find((m) => m.name === model)
  const label = current?.display_name || model

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        className={cn(
          'flex items-center gap-1.5 w-full text-xs font-mono px-2 py-1 rounded transition-colors',
          dark ? 'bg-white/10 text-white/75 hover:bg-white/[0.15]' : 'bg-muted hover:bg-muted/70',
        )}
      >
        <span className="truncate flex-1 text-left">{label}</span>
        <ChevronDown className={cn('h-3 w-3 shrink-0 transition-transform', open && 'rotate-180')} />
      </button>
      {open && (
        <div className="absolute left-0 top-full mt-1 z-50 min-w-44 rounded-md border bg-card shadow-md py-1">
          {models.map((m) => (
            <button
              key={m.name}
              onClick={() => select(m.name)}
              className={cn(
                'w-full text-left px-3 py-2 text-sm hover:bg-muted transition-colors',
                m.name === model ? 'font-medium text-foreground' : 'text-muted-foreground',
              )}
            >
              {m.display_name || m.name}
              <span className="ml-2 font-mono text-xs opacity-50">{m.name}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Model switcher (icon-only, collapsed sidebar)
// ---------------------------------------------------------------------------

function ModelSwitcherIcon({ dark }: { dark?: boolean }) {
  const { model, setModel } = useModel()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [models, setModels] = useState<ModelSummary[]>([])
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    listModels().then(setModels).catch(() => {})
  }, [])

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  function select(name: string) {
    setOpen(false)
    if (name !== model) { setModel(name); navigate('/explore') }
  }

  const current = models.find((m) => m.name === model)
  const label = current?.display_name || model

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        title={label}
        className={cn(dark ? 'text-white/40 hover:text-white/80' : 'text-muted-foreground hover:text-foreground', 'transition-colors p-1')}
      >
        <Database className="h-3.5 w-3.5" />
      </button>
      {open && (
        <div className="absolute left-full top-0 ml-2 z-50 min-w-44 rounded-md border bg-card shadow-md py-1">
          {models.map((m) => (
            <button
              key={m.name}
              onClick={() => select(m.name)}
              className={cn(
                'w-full text-left px-3 py-2 text-sm hover:bg-muted transition-colors',
                m.name === model ? 'font-medium text-foreground' : 'text-muted-foreground',
              )}
            >
              {m.display_name || m.name}
              <span className="ml-2 font-mono text-xs opacity-50">{m.name}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Reusable primitives
// ---------------------------------------------------------------------------

export function Card({ className, children }: { className?: string; children: React.ReactNode }) {
  return (
    <div className={cn('rounded-lg border bg-card text-card-foreground shadow-sm p-4', className)}>
      {children}
    </div>
  )
}

export function Badge({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <span className={cn('inline-flex items-center rounded-md border px-2 py-0.5 text-xs font-medium', className)}>
      {children}
    </span>
  )
}

export function Spinner() {
  return (
    <div className="flex items-center justify-center py-16 text-muted-foreground text-sm gap-2">
      <span className="animate-spin">⟳</span> Loading…
    </div>
  )
}

export function ErrorMessage({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
      {message}
    </div>
  )
}
