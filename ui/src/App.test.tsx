// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * VG-298 legacy-route redirect tests.
 *
 * The deprecated surfaces (Views, Queries, Entity Explorer, Features,
 * Mappers, Ontology, Graph) collapse into /explore. Bookmarked URLs and
 * shared links must keep working — every old path redirects, and for the
 * `/entities/:entity[/:id]` shape the entity is preserved as
 * `?entity=`. The pages themselves stay on disk for a soak period so a
 * revert is just a route table swap.
 */

import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route, Navigate, useParams, useLocation } from 'react-router-dom'

// Avoid pulling in the heavy app shell during routing tests — re-declare
// the same redirect routes the prod App uses. If these get out of sync,
// the App.tsx review catches it; the goal here is to lock the redirect
// contract, not to wire up the whole tree.
function EntityRedirect() {
  const { entity } = useParams()
  return <Navigate to={entity ? `/explore?entity=${encodeURIComponent(entity)}` : '/explore'} replace />
}

function LocationProbe() {
  const loc = useLocation()
  return <div data-testid="location">{loc.pathname + loc.search}</div>
}

function renderRoutes(initial: string) {
  return render(
    <MemoryRouter initialEntries={[initial]}>
      <Routes>
        <Route path="/explore" element={<LocationProbe />} />
        <Route path="/views" element={<Navigate to="/explore" replace />} />
        <Route path="/views/:name" element={<Navigate to="/explore" replace />} />
        <Route path="/entities" element={<Navigate to="/explore" replace />} />
        <Route path="/entities/:entity" element={<EntityRedirect />} />
        <Route path="/entities/:entity/:id" element={<EntityRedirect />} />
        <Route path="/queries" element={<Navigate to="/explore" replace />} />
        <Route path="/features" element={<Navigate to="/explore" replace />} />
        <Route path="/graph" element={<Navigate to="/explore" replace />} />
        <Route path="/ontology" element={<Navigate to="/explore" replace />} />
      </Routes>
    </MemoryRouter>,
  )
}

describe('VG-298 — legacy route redirects', () => {
  // VG-308: /mappers restored as a real admin route, removed from redirect list.
  it.each([
    ['/views', '/explore'],
    ['/views/my-chart', '/explore'],
    ['/entities', '/explore'],
    ['/queries', '/explore'],
    ['/features', '/explore'],
    ['/graph', '/explore'],
    ['/ontology', '/explore'],
  ])('redirects %s → %s', async (from, to) => {
    renderRoutes(from)
    await waitFor(() => expect(screen.getByTestId('location')).toHaveTextContent(to))
  })

  it('preserves entity when redirecting /entities/:entity', async () => {
    renderRoutes('/entities/PullRequest')
    await waitFor(() =>
      expect(screen.getByTestId('location')).toHaveTextContent('/explore?entity=PullRequest'),
    )
  })

  it('preserves entity (drops row id) when redirecting /entities/:entity/:id', async () => {
    renderRoutes('/entities/PullRequest/abc-123')
    await waitFor(() =>
      expect(screen.getByTestId('location')).toHaveTextContent('/explore?entity=PullRequest'),
    )
  })

  it('url-encodes the entity name', async () => {
    renderRoutes('/entities/My%20Entity')
    await waitFor(() =>
      expect(screen.getByTestId('location')).toHaveTextContent('/explore?entity=My%20Entity'),
    )
  })
})

// ---------------------------------------------------------------------------
// Sidebar smoke — every entry the user sees must point at a still-routed
// path. Catches regressions where someone removes a route but leaves the
// link, or vice versa.
// ---------------------------------------------------------------------------

vi.mock('@/api/client', async (orig) => {
  const original: object = await orig()
  return {
    ...original,
    countMyNotifications: vi.fn(async () => 0),
    listMyNotifications: vi.fn(async () => []),
  }
})

vi.mock('@/context/ModelContext', () => ({
  ModelProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useModel: () => ({
    model: 'demo',
    api: { listApplications: vi.fn(async () => []) },
  }),
}))

vi.mock('@/context/RoleContext', () => ({
  RoleProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useRole: () => ({
    email: 'u@example.com', userId: 'u1', role: 'admin', loading: false,
  }),
}))

describe('VG-298 — sidebar smoke', () => {
  it('renders no link to the deprecated surfaces', async () => {
    // Import lazily so the mocks above apply.
    const { Layout } = await import('@/components/Layout')
    render(
      <MemoryRouter>
        <Layout><div /></Layout>
      </MemoryRouter>,
    )
    // Wait for the role-gated admin section to render.
    await screen.findByText('Models')

    // None of the removed labels should be on the page. VG-307 removed
    // Extractors as well — per-entity Pipeline tab + /tools deep-link from
    // PipelineTab covers the admin's needs.
    for (const label of ['Views', 'Entity Explorer', 'Mappers', 'Ontology', 'Features', 'Query Builder', 'Graph', 'Extractors']) {
      expect(screen.queryByText(label)).not.toBeInTheDocument()
    }
    // Explore + the kept admin entries are still there.
    expect(screen.getByText('Explore')).toBeInTheDocument()
    expect(screen.getByText('Jobs')).toBeInTheDocument()
  })
})
