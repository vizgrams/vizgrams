// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

import { ChartDetailDrawer } from './ChartDetailDrawer'

const getView = vi.fn()
const executeView = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { getView, executeView } }),
}))

// ViewContent pulls in Recharts; replace it with a stub that just exposes
// the result type so we can confirm wiring without exercising the renderer.
vi.mock('@/components/view/ViewContent', () => ({
  ViewContent: ({ result }: { result: { type: string } }) => (
    <div data-testid="view-content">type:{result.type}</div>
  ),
}))

describe('ChartDetailDrawer', () => {
  it('fetches view + result and renders ViewContent', async () => {
    getView.mockClear().mockResolvedValue({ name: 'x', query: 'q', type: 'chart' })
    executeView.mockClear().mockResolvedValue({ name: 'x', type: 'chart', columns: [], rows: [], visualization: {} })
    render(
      <MemoryRouter>
        <ChartDetailDrawer viewName="x" onClose={() => {}} />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByTestId('view-content')).toHaveTextContent('type:chart'))
    expect(getView).toHaveBeenCalledWith('x')
    expect(executeView).toHaveBeenCalledWith('x', 1000)
  })

  it('shows error inline when fetch rejects', async () => {
    getView.mockClear().mockRejectedValue(new Error('not found'))
    executeView.mockClear().mockRejectedValue(new Error('not found'))
    render(
      <MemoryRouter>
        <ChartDetailDrawer viewName="missing" onClose={() => {}} />
      </MemoryRouter>,
    )
    await waitFor(() => expect(screen.getByText(/not found/)).toBeInTheDocument())
  })

  it('close button calls onClose', async () => {
    getView.mockClear().mockResolvedValue({ name: 'x', query: 'q', type: 'chart' })
    executeView.mockClear().mockResolvedValue({ name: 'x', type: 'chart', columns: [], rows: [], visualization: {} })
    const onClose = vi.fn()
    render(
      <MemoryRouter>
        <ChartDetailDrawer viewName="x" onClose={onClose} />
      </MemoryRouter>,
    )
    await screen.findByTestId('view-content')
    // Two close affordances exist — the backdrop and the X button. Click
    // the X explicitly.
    const xButton = screen.getAllByRole('button').find((b) => b.querySelector('svg')?.classList.contains('lucide-x'))
    fireEvent.click(xButton!)
    expect(onClose).toHaveBeenCalled()
  })
})
