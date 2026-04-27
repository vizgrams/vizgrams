// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { ShieldOff } from 'lucide-react'
import { ModelProvider } from '@/context/ModelContext'
import { RoleProvider } from '@/context/RoleContext'
import { useRole } from '@/context/RoleContext'
import type { PlatformRole } from '@/api/client'
import { Layout } from '@/components/Layout'
import { EntityListPage } from '@/pages/EntityListPage'
import { EntityDetailPage } from '@/pages/EntityDetailPage'
import { EntitiesPage } from '@/pages/EntitiesPage'
import { JobLogPage } from '@/pages/JobLogPage'
import { ToolsPage } from '@/pages/ToolsPage'
import { MappersPage } from '@/pages/MappersPage'
import { GraphPage } from '@/pages/GraphPage'
import { QueriesPage } from '@/pages/QueriesPage'
import { FeaturesPage } from '@/pages/FeaturesPage'
import { ExploreShell } from '@/pages/ExploreShell'
import { AccountPage } from '@/pages/AccountPage'
import { FeedPage } from '@/pages/FeedPage'
import { SavedPage } from '@/pages/SavedPage'
import { ViewsPage } from '@/pages/ViewsPage'
import { ModelsPage } from '@/pages/admin/ModelsPage'

// ---------------------------------------------------------------------------
// Route guard — renders children only if the user meets minRole, else 403.
// Shows nothing while the role is still loading to avoid flash.
// ---------------------------------------------------------------------------

function ProtectedRoute({ minRole, children }: { minRole: PlatformRole; children: React.ReactNode }) {
  const { role, loading } = useRole()
  if (loading) return null
  const allowed =
    minRole === 'admin' ? role === 'admin' :
    minRole === 'creator' ? role === 'admin' || role === 'creator' :
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
            <Route path="/explore" element={<ExploreShell />} />
            <Route path="/explore/:entity" element={<EntityListPage />} />
            <Route path="/explore/:entity/:id" element={<EntityDetailPage />} />
            <Route path="/account" element={<AccountPage />} />

            {/* Creator — creator+ */}
            <Route path="/features" element={<ProtectedRoute minRole="creator"><FeaturesPage /></ProtectedRoute>} />
            <Route path="/queries" element={<ProtectedRoute minRole="creator"><QueriesPage /></ProtectedRoute>} />
            <Route path="/views" element={<ProtectedRoute minRole="creator"><ViewsPage /></ProtectedRoute>} />
            <Route path="/graph" element={<ProtectedRoute minRole="creator"><GraphPage /></ProtectedRoute>} />

            {/* Admin — admin only */}
            <Route path="/admin/models" element={<ProtectedRoute minRole="admin"><ModelsPage /></ProtectedRoute>} />
            <Route path="/tools" element={<ProtectedRoute minRole="admin"><ToolsPage /></ProtectedRoute>} />
            <Route path="/mappers" element={<ProtectedRoute minRole="admin"><MappersPage /></ProtectedRoute>} />
            <Route path="/entities" element={<ProtectedRoute minRole="admin"><EntitiesPage /></ProtectedRoute>} />
            <Route path="/jobs" element={<ProtectedRoute minRole="admin"><JobLogPage /></ProtectedRoute>} />

            {/* Legacy redirects */}
            <Route path="/applications" element={<Navigate to="/explore" replace />} />
          </Routes>
        </Layout>
      </ModelProvider>
      </RoleProvider>
    </BrowserRouter>
  )
}
