// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

import { ChartDetailDrawer } from './ChartDetailDrawer'

const getView = vi.fn()
const executeView = vi.fn()
const getQuery = vi.fn()
const saveChart = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({
    model: 'demo',
    api: { getView, executeView, getQuery, saveChart },
  }),
}))

// ViewContent pulls in Recharts; replace it with a stub.
vi.mock('@/components/view/ViewContent', () => ({
  ViewContent: ({ result }: { result: { type: string } }) => (
    <div data-testid="view-content">type:{result.type}</div>
  ),
}))

// DrilldownOverlay isn't relevant to these tests but it's mounted
// conditionally so we don't need to mock unless drilldowns fire.

function setupHappy() {
  getView.mockClear().mockResolvedValue({
    name: 'x', query: 'x', type: 'chart',
    raw_yaml: 'name: x\ntype: chart\nquery: x\n',
  })
  executeView.mockClear().mockResolvedValue({
    name: 'x', type: 'chart', columns: [], rows: [], visualization: {},
  })
  getQuery.mockClear().mockResolvedValue({
    name: 'x', raw_yaml: 'name: x\nentity: Widget\n',
  })
  saveChart.mockClear().mockResolvedValue({ query: {}, view: {} })
}

describe('ChartDetailDrawer', () => {
  it('preview mode fetches view + result and renders ViewContent', async () => {
    setupHappy()
    render(<MemoryRouter><ChartDetailDrawer viewName="x" onClose={() => {}} /></MemoryRouter>)
    await waitFor(() => expect(screen.getByTestId('view-content')).toHaveTextContent('type:chart'))
    expect(getView).toHaveBeenCalledWith('x')
    expect(executeView).toHaveBeenCalledWith('x', 1000, 0, {})
    expect(getQuery).toHaveBeenCalledWith('x')
  })

  it('clicking Edit switches to the two-pane editor pre-filled', async () => {
    setupHappy()
    render(<MemoryRouter><ChartDetailDrawer viewName="x" onClose={() => {}} /></MemoryRouter>)
    await screen.findByTestId('view-content')
    fireEvent.click(screen.getByRole('button', { name: /edit/i }))
    // The Edit toggle flips the button label to "Preview" — wait for that.
    await screen.findByRole('button', { name: /preview/i })
    const textboxes = screen.getAllByRole('textbox') as HTMLTextAreaElement[]
    const queryTextarea = textboxes.find((t) => t.value.includes('entity: Widget'))
    const viewTextarea = textboxes.find((t) => t.value.includes('type: chart'))
    expect(queryTextarea).toBeDefined()
    expect(viewTextarea).toBeDefined()
  })

  it('Save in edit mode calls saveChart with both yamls then returns to preview', async () => {
    setupHappy()
    render(<MemoryRouter><ChartDetailDrawer viewName="x" onClose={() => {}} /></MemoryRouter>)
    await screen.findByTestId('view-content')
    fireEvent.click(screen.getByRole('button', { name: /edit/i }))
    fireEvent.click(await screen.findByRole('button', { name: /save chart/i }))
    await waitFor(() => expect(saveChart).toHaveBeenCalledTimes(1))
    expect(saveChart.mock.calls[0][0]).toBe('x')
    expect(saveChart.mock.calls[0][1]).toContain('entity: Widget')
    expect(saveChart.mock.calls[0][2]).toContain('type: chart')
    // Refetches after save → ViewContent visible again
    await waitFor(() => expect(screen.getByTestId('view-content')).toBeInTheDocument())
  })

  it('Save error keeps the editor open with the error message visible', async () => {
    setupHappy()
    saveChart.mockClear().mockRejectedValue(new Error('422: invalid query'))
    render(<MemoryRouter><ChartDetailDrawer viewName="x" onClose={() => {}} /></MemoryRouter>)
    await screen.findByTestId('view-content')
    fireEvent.click(screen.getByRole('button', { name: /edit/i }))
    fireEvent.click(await screen.findByRole('button', { name: /save chart/i }))
    expect(await screen.findByText(/invalid query/i)).toBeInTheDocument()
    // Still on edit mode — Save button still rendered.
    expect(screen.getByRole('button', { name: /save chart/i })).toBeInTheDocument()
  })

  it('shows error inline when fetch rejects', async () => {
    getView.mockClear().mockRejectedValue(new Error('not found'))
    executeView.mockClear().mockRejectedValue(new Error('not found'))
    getQuery.mockClear()
    render(<MemoryRouter><ChartDetailDrawer viewName="missing" onClose={() => {}} /></MemoryRouter>)
    await waitFor(() => expect(screen.getByText(/not found/)).toBeInTheDocument())
  })

  it('close button calls onClose', async () => {
    setupHappy()
    const onClose = vi.fn()
    render(<MemoryRouter><ChartDetailDrawer viewName="x" onClose={onClose} /></MemoryRouter>)
    await screen.findByTestId('view-content')
    const xButton = screen.getAllByRole('button').find((b) => b.querySelector('svg')?.classList.contains('lucide-x'))
    fireEvent.click(xButton!)
    expect(onClose).toHaveBeenCalled()
  })
})
