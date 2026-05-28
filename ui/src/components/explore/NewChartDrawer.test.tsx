// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import type { QuerySummary } from '@/api/client'
import { NewChartDrawer, buildTemplate } from './NewChartDrawer'

const listQueries = vi.fn()
const saveView = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { listQueries, saveView } }),
}))

function querySummary(name: string, root: string | null = 'PullRequest'): QuerySummary {
  return {
    name, root, measure_count: 1, group_by_count: 1,
  } as QuerySummary
}

describe('buildTemplate', () => {
  it('produces a bar chart template with chart_type=bar', () => {
    const t = buildTemplate({ name: 'x', queryName: 'q', chartType: 'bar' })
    expect(t).toContain('type: chart')
    expect(t).toContain('chart_type: bar')
    expect(t).toContain('query: q')
  })
  it('produces a metric template for kpi', () => {
    const t = buildTemplate({ name: 'x', queryName: 'q', chartType: 'kpi' })
    expect(t).toContain('type: metric')
    expect(t).toContain('measure:')
  })
  it('produces a table template', () => {
    const t = buildTemplate({ name: 'x', queryName: 'q', chartType: 'table' })
    expect(t).toContain('type: table')
    expect(t).toContain('columns:')
  })
})

describe('NewChartDrawer', () => {
  it('lists only queries rooted on the current entity', async () => {
    listQueries.mockClear().mockResolvedValue([
      querySummary('prs_open', 'PullRequest'),
      querySummary('teams_size', 'Team'),
      querySummary('prs_merged', 'PullRequest'),
    ])
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await waitFor(() => expect(screen.getByRole('option', { name: 'prs_open' })).toBeInTheDocument())
    expect(screen.getByRole('option', { name: 'prs_merged' })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'teams_size' })).not.toBeInTheDocument()
  })

  it('shows guidance when no queries exist for the entity', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('teams_size', 'Team')])
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await waitFor(() => expect(screen.getByText(/No queries rooted on PullRequest/)).toBeInTheDocument())
    // Save button disabled with no queries available.
    expect(screen.getByRole('button', { name: /create chart/i })).toBeDisabled()
  })

  it('rejects invalid names inline before calling saveView', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('q')])
    saveView.mockClear()
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await screen.findByRole('option', { name: 'q' })
    fireEvent.change(screen.getByPlaceholderText(/snake_case_name/i), { target: { value: 'BadName!' } })
    fireEvent.click(screen.getByRole('button', { name: /create chart/i }))
    expect(await screen.findByText(/lowercase letters/i)).toBeInTheDocument()
    expect(saveView).not.toHaveBeenCalled()
  })

  it('saves with the generated yaml then calls onCreated + onClose', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('prs_open')])
    saveView.mockClear().mockResolvedValue({ name: 'new_chart' })
    const onCreated = vi.fn()
    const onClose = vi.fn()
    render(<NewChartDrawer entity="PullRequest" onClose={onClose} onCreated={onCreated} />)
    await screen.findByRole('option', { name: 'prs_open' })
    fireEvent.change(screen.getByPlaceholderText(/snake_case_name/i), { target: { value: 'new_chart' } })
    fireEvent.click(screen.getByRole('button', { name: /create chart/i }))
    await waitFor(() => expect(saveView).toHaveBeenCalledTimes(1))
    expect(saveView.mock.calls[0][0]).toBe('new_chart')
    expect(saveView.mock.calls[0][1]).toContain('query: prs_open')
    expect(saveView.mock.calls[0][1]).toContain('chart_type: bar')
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith('new_chart'))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('shows server error inline and keeps drawer open', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('q')])
    saveView.mockClear().mockRejectedValue(new Error('400: invalid yaml'))
    const onClose = vi.fn()
    render(<NewChartDrawer entity="PullRequest" onClose={onClose} />)
    await screen.findByRole('option', { name: 'q' })
    fireEvent.change(screen.getByPlaceholderText(/snake_case_name/i), { target: { value: 'works' } })
    fireEvent.click(screen.getByRole('button', { name: /create chart/i }))
    expect(await screen.findByText(/invalid yaml/i)).toBeInTheDocument()
    expect(onClose).not.toHaveBeenCalled()
  })

  it('changing chart_type updates the template', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('q')])
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await screen.findByRole('option', { name: 'q' })
    // Default is bar — switch to kpi and confirm the YAML textarea reflects it.
    fireEvent.click(screen.getByRole('button', { name: 'kpi' }))
    // textareas are role=textbox; the only one that grows is the YAML editor.
    const textboxes = screen.getAllByRole('textbox') as HTMLTextAreaElement[]
    const yaml = textboxes.find((t) => t.tagName === 'TEXTAREA')
    expect(yaml).toBeDefined()
    expect(yaml!.value).toContain('type: metric')
  })
})
