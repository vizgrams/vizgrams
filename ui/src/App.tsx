// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { BrowserRouter, Routes, Route, Navigate, useParams } from 'react-router-dom'
import { ShieldOff } from 'lucide-react'
import { ModelProvider } from '@/context/ModelContext'
import { RoleProvider } from '@/context/RoleContext'
import { useRole } from '@/context/RoleContext'
import type { PlatformRole } from '@/api/client'
import { Layout } from '@/components/Layout'
import { JobLogPage } from '@/pages/JobLogPage'
import { ToolsPage } from '@/pages/ToolsPage'
import { AppPage } from '@/pages/AppPage'
import ChatPage from '@/pages/ChatPage'
import { AccountPage } from '@/pages/AccountPage'
import { FeedPage } from '@/pages/FeedPage'
import { SavedPage } from '@/pages/SavedPage'
import { ExplorePage } from '@/pages/ExplorePage'
import { ModelsPage } from '@/pages/admin/ModelsPage'

// ---------------------------------------------------------------------------
// VG-298 — legacy routes redirect to /explore. Page files are retained for
// one release (soak period) so an emergency revert is just a route swap.
// ---------------------------------------------------------------------------

function EntityRedirect() {
  const { entity } = useParams()
  return <Navigate to={entity ? `/explore?entity=${encodeURIComponent(entity)}` : '/explore'} replace />
}

// ---------------------------------------------------------------------------
// Route guard — renders children only if the user meets minRole, else 403.
// Shows nothing while the role is still loading to avoid flash.
// ---------------------------------------------------------------------------

function ProtectedRoute({ minRole, children }: { minRole: PlatformRole; children: React.ReactNode }) {
  const { role, loading } = useRole()
  if (loading) return null
  // Epic 26 VG-292: two-role hierarchy admin > member > viewer.
  // 'member' satisfies any minRole except 'admin'; 'viewer' is the
  // unauthenticated default and satisfies nothing above viewer.
  const allowed =
    minRole === 'admin'  ? role === 'admin' :
    minRole === 'member' ? role === 'admin' || role === 'member' :
    true
  if (!allowed) return <AccessDenied requiredRole={minRole} />
  return <>{children}</>
}

function AccessDenied({ requiredRole }: { requiredRole: PlatformRole }) {
  return (
    <div className="flex flex-col items-center justify-center py-24 text-center gap-4">
      <ShieldOff className="h-10 w-10 text-muted-foreground/30" />
      <h1 className="text-xl font-semibold text-foreground">Access denied</h1>
      <p className="text-sm text-muted-foreground max-w-sm">
        This page requires <span className="font-medium">{requiredRole}</span> access.
        Contact your administrator if you need access.
      </p>
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <RoleProvider>
      <ModelProvider>
        <Layout>
          <Routes>
            <Route path="/" element={<Navigate to="/feed" replace />} />
            <Route path="/dashboard" element={<Navigate to="/feed" replace />} />

            {/* User — open to all */}
            <Route path="/feed" element={<FeedPage />} />
            <Route path="/saved" element={<SavedPage />} />
            <Route path="/explore" element={<ExplorePage />} />
            <Route path="/chat" element={<ProtectedRoute minRole="member"><ChatPage /></ProtectedRoute>} />
            <Route path="/apps/:name" element={<AppPage />} />
            <Route path="/account" element={<AccountPage />} />

            {/* Admin — admin only */}
            <Route path="/admin/models" element={<ProtectedRoute minRole="admin"><ModelsPage /></ProtectedRoute>} />
            <Route path="/tools" element={<ProtectedRoute minRole="admin"><ToolsPage /></ProtectedRoute>} />
            <Route path="/jobs" element={<ProtectedRoute minRole="admin"><JobLogPage /></ProtectedRoute>} />

            {/* VG-298 — legacy routes redirect to /explore */}
            <Route path="/views" element={<Navigate to="/explore" replace />} />
            <Route path="/views/:name" element={<Navigate to="/explore" replace />} />
            <Route path="/entities" element={<Navigate to="/explore" replace />} />
            <Route path="/entities/:entity" element={<EntityRedirect />} />
            <Route path="/entities/:entity/:id" element={<EntityRedirect />} />
            <Route path="/queries" element={<Navigate to="/explore" replace />} />
            <Route path="/features" element={<Navigate to="/explore" replace />} />
            <Route path="/graph" element={<Navigate to="/explore" replace />} />
            <Route path="/mappers" element={<Navigate to="/explore" replace />} />
            <Route path="/ontology" element={<Navigate to="/explore" replace />} />
          </Routes>
        </Layout>
      </ModelProvider>
      </RoleProvider>
    </BrowserRouter>
  )
}
