// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import type { QuerySummary } from '@/api/client'
import { NewChartDrawer, buildChartTemplate } from './NewChartDrawer'

const listQueries = vi.fn()
const getQuery = vi.fn()
const saveChart = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { listQueries, getQuery, saveChart } }),
}))

function querySummary(name: string, root: string | null = 'PullRequest'): QuerySummary {
  return { name, root, measure_count: 1, group_by_count: 1 } as QuerySummary
}

describe('buildChartTemplate', () => {
  it('produces a self-contained bar chart template', () => {
    const t = buildChartTemplate({ name: 'x', entity: 'PullRequest', chartType: 'bar' })
    expect(t).toContain('entity: PullRequest')
    expect(t).toContain('attributes:')
    expect(t).toContain('type: chart')
    expect(t).toContain('chart_type: bar')
    // No self-referencing ``query: x`` — that field is a split-time
    // implementation detail, not something the user should see.
    expect(t).not.toContain('query: x')
  })

  it('produces a metric template for kpi', () => {
    const t = buildChartTemplate({ name: 'x', entity: 'PullRequest', chartType: 'kpi' })
    expect(t).toContain('type: metric')
    expect(t).toContain('measure:')
  })

  it('produces a table template', () => {
    const t = buildChartTemplate({ name: 'x', entity: 'PullRequest', chartType: 'table' })
    expect(t).toContain('type: table')
    expect(t).toContain('columns:')
  })
})

describe('NewChartDrawer', () => {
  it('shows the "Start from existing query" picker only when entity-rooted queries exist', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('prs_open')])
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await waitFor(() => expect(screen.getByText(/Start from existing query/i)).toBeInTheDocument())
  })

  it('hides the picker when no queries are rooted on the entity', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('teams_size', 'Team')])
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await waitFor(() => expect(listQueries).toHaveBeenCalled())
    expect(screen.queryByText(/Start from existing query/i)).not.toBeInTheDocument()
  })

  it('rejects invalid names inline before calling saveChart', async () => {
    listQueries.mockClear().mockResolvedValue([])
    saveChart.mockClear()
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    fireEvent.change(screen.getByPlaceholderText(/snake_case_name/i),
                     { target: { value: 'BadName!' } })
    fireEvent.click(screen.getByRole('button', { name: /create chart/i }))
    expect(await screen.findByText(/lowercase letters/i)).toBeInTheDocument()
    expect(saveChart).not.toHaveBeenCalled()
  })

  it('saves the composed chart YAML then onCreated + onClose', async () => {
    listQueries.mockClear().mockResolvedValue([])
    saveChart.mockClear().mockResolvedValue({ query: {}, view: {}, chart_yaml: '' })
    const onCreated = vi.fn()
    const onClose = vi.fn()
    render(<NewChartDrawer entity="PullRequest" onClose={onClose} onCreated={onCreated} />)
    fireEvent.change(screen.getByPlaceholderText(/snake_case_name/i),
                     { target: { value: 'new_chart' } })
    fireEvent.click(screen.getByRole('button', { name: /create chart/i }))
    await waitFor(() => expect(saveChart).toHaveBeenCalledTimes(1))
    const [name, yaml] = saveChart.mock.calls[0]
    expect(name).toBe('new_chart')
    // Single YAML string containing both query and viz fields — the
    // load-bearing property that proves the two-file → one-file collapse.
    expect(yaml).toContain('entity: PullRequest')
    expect(yaml).toContain('chart_type: bar')
    expect(yaml).toContain('visualization:')
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith('new_chart'))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('shows server error inline and keeps drawer open', async () => {
    listQueries.mockClear().mockResolvedValue([])
    saveChart.mockClear().mockRejectedValue(new Error('400: invalid yaml'))
    const onClose = vi.fn()
    render(<NewChartDrawer entity="PullRequest" onClose={onClose} />)
    fireEvent.change(screen.getByPlaceholderText(/snake_case_name/i),
                     { target: { value: 'works' } })
    fireEvent.click(screen.getByRole('button', { name: /create chart/i }))
    expect(await screen.findByText(/invalid yaml/i)).toBeInTheDocument()
    expect(onClose).not.toHaveBeenCalled()
  })

  it('inlines an existing query when picked', async () => {
    listQueries.mockClear().mockResolvedValue([querySummary('prs_open')])
    getQuery.mockClear().mockResolvedValue({
      name: 'prs_open', raw_yaml: 'name: prs_open\nentity: PullRequest\nattributes:\n  - id\n',
    })
    render(<NewChartDrawer entity="PullRequest" onClose={() => {}} />)
    await screen.findByText(/Start from existing query/i)
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'prs_open' } })
    await waitFor(() => expect(getQuery).toHaveBeenCalledWith('prs_open'))
    // Single editor now shows both the loaded query fields AND the viz
    // template appended below.
    const textareas = screen.getAllByRole('textbox') as HTMLTextAreaElement[]
    const chartTextarea = textareas.find((t) => t.value.includes('- id'))
    expect(chartTextarea).toBeDefined()
    expect(chartTextarea!.value).toContain('visualization:')
  })
})
