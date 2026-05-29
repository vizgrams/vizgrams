// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

import { DrilldownOverlay } from './DrilldownOverlay'

const getView = vi.fn()
const executeView = vi.fn()
const getEntityRecord = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { getView, executeView, getEntityRecord } }),
}))

vi.mock('@/components/view/ViewContent', () => ({
  ViewContent: ({ result, onNavigate }: { result: { type: string }; onNavigate: (f: unknown) => void }) => (
    <div data-testid="view-content">
      <span>type:{result.type}</span>
      <button onClick={() => onNavigate({ kind: 'entity-detail', entity: 'Person', id: 'p1' })}>
        drill-to-record
      </button>
    </div>
  ),
}))

vi.mock('@/pages/explore/EntityListFrame', () => ({
  EntityListFrame: ({ entity }: { entity: string }) => (
    <div data-testid="entity-list-frame">records for {entity}</div>
  ),
}))

const navigate = vi.fn()
vi.mock('react-router-dom', async (orig) => {
  const actual = (await orig()) as object
  return { ...actual, useNavigate: () => navigate }
})

function makeView() {
  return {
    name: 'x', type: 'chart', columns: [], rows: [], visualization: {},
    formats: {}, params: [],
  }
}

describe('DrilldownOverlay', () => {
  it('view frame: renders ViewContent in a drawer', async () => {
    getView.mockClear().mockResolvedValue({ name: 'detail', query: 'q', type: 'chart' })
    executeView.mockClear().mockResolvedValue(makeView())
    render(
      <MemoryRouter>
        <DrilldownOverlay
          frame={{ kind: 'view', name: 'detail', params: { p: '1' } }}
          onClose={() => {}}
        />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByTestId('view-content')).toHaveTextContent('type:chart'))
    expect(executeView).toHaveBeenCalledWith('detail', 1000, 0, { p: '1' })
  })

  it('view frame: drilldown inside view pushes a nested overlay', async () => {
    getView.mockClear().mockResolvedValue({ name: 'd', query: 'q', type: 'chart' })
    executeView.mockClear().mockResolvedValue(makeView())
    getEntityRecord.mockClear().mockResolvedValue({
      entity: 'Person', id: 'p1',
      properties: { name: 'Alice' },
      relationships: {}, feature_values: {},
    })
    render(
      <MemoryRouter>
        <DrilldownOverlay
          frame={{ kind: 'view', name: 'd', params: {} }}
          onClose={() => {}}
        />
      </MemoryRouter>,
    )
    await screen.findByText('drill-to-record')
    fireEvent.click(screen.getByText('drill-to-record'))
    // Record drawer now mounted on top — its "Alice" property text appears.
    await waitFor(() => expect(screen.getByText('Alice')).toBeInTheDocument())
    expect(getEntityRecord).toHaveBeenCalledWith('Person', 'p1')
  })

  it('entity-detail frame: opens RecordDetailDrawer directly', async () => {
    getEntityRecord.mockClear().mockResolvedValue({
      entity: 'PullRequest', id: 'pr1',
      properties: { title: 'Add the thing' },
      relationships: {}, feature_values: {},
    })
    render(
      <MemoryRouter>
        <DrilldownOverlay
          frame={{ kind: 'entity-detail', entity: 'PullRequest', id: 'pr1' }}
          onClose={() => {}}
        />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByText('Add the thing')).toBeInTheDocument())
  })

  it('entity-list frame: renders the lazy EntityListFrame', async () => {
    render(
      <MemoryRouter>
        <DrilldownOverlay
          frame={{ kind: 'entity-list', entity: 'PullRequest' }}
          onClose={() => {}}
        />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByTestId('entity-list-frame')).toHaveTextContent('records for PullRequest'))
  })

  it('app frame: navigates rather than rendering inline', async () => {
    navigate.mockClear()
    const onClose = vi.fn()
    render(
      <MemoryRouter>
        <DrilldownOverlay
          frame={{ kind: 'app', name: 'dashboard', params: { team: 'a' } }}
          onClose={onClose}
        />
      </MemoryRouter>,
    )
    await waitFor(() => expect(navigate).toHaveBeenCalledWith('/apps/dashboard?team=a'))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })
})
