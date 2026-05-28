// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'

import type { ViewResult } from '@/api/client'
import { ChartPreview } from './ChartPreview'

const executeView = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { executeView } }),
}))

// LineBarChart pulls in Recharts which renders to SVG and is awkward in jsdom.
// Stub it so we can assert the dispatch path without worrying about pixel
// math — the chart library itself is exercised by /views tests.
vi.mock('@/components/charts/LineBarChart', () => ({
  LineBarChart: ({ chartType, xKey }: { chartType: string; xKey: string }) => (
    <div data-testid="linebar-chart">{chartType}:{xKey}</div>
  ),
}))
vi.mock('@/components/charts/CalendarHeatmapChart', () => ({
  CalendarHeatmapChart: ({ dateKey }: { dateKey: string }) => (
    <div data-testid="heatmap">{dateKey}</div>
  ),
}))

function makeLineResult(): ViewResult {
  return {
    name: 'prs_by_day',
    type: 'chart',
    measure: null,
    columns: ['day', 'count'],
    rows: [['2026-01-01', 3], ['2026-01-02', 5]],
    visualization: { chart_type: 'line', x: 'day', y: ['count'] },
    formats: {},
    params: [],
  } as unknown as ViewResult
}

describe('ChartPreview', () => {
  it('renders LineBarChart for chart_type=line', async () => {
    executeView.mockClear().mockResolvedValue(makeLineResult())
    render(<ChartPreview viewName="prs_by_day" />)
    await waitFor(() => expect(screen.getByTestId('linebar-chart')).toHaveTextContent('line:day'))
    expect(executeView).toHaveBeenCalledWith('prs_by_day', 500)
  })

  it('renders CalendarHeatmapChart for chart_type=calendar_heatmap', async () => {
    executeView.mockClear().mockResolvedValue({
      name: 'activity', type: 'chart', columns: ['day', 'n'], rows: [],
      visualization: { chart_type: 'calendar_heatmap', date: 'day', value: 'n' },
      formats: {}, params: [], measure: null,
    } as unknown as ViewResult)
    render(<ChartPreview viewName="activity" />)
    await waitFor(() => expect(screen.getByTestId('heatmap')).toHaveTextContent('day'))
  })

  it('renders a compact table for type=table', async () => {
    executeView.mockClear().mockResolvedValue({
      name: 'top10', type: 'table',
      columns: ['name', 'count'],
      rows: [['alice', 10], ['bob', 7]],
      visualization: {}, formats: {}, params: [], measure: null,
    } as unknown as ViewResult)
    render(<ChartPreview viewName="top10" />)
    await waitFor(() => expect(screen.getByText('alice')).toBeInTheDocument())
    expect(screen.getByText('bob')).toBeInTheDocument()
  })

  it('shows a fallback for unknown chart_type', async () => {
    executeView.mockClear().mockResolvedValue({
      name: 'weird', type: 'chart',
      columns: [], rows: [],
      visualization: { chart_type: 'sankey' },
      formats: {}, params: [], measure: null,
    } as unknown as ViewResult)
    render(<ChartPreview viewName="weird" />)
    await waitFor(() => expect(screen.getByText('sankey')).toBeInTheDocument())
  })

  it('shows an error inline when executeView rejects', async () => {
    executeView.mockClear().mockRejectedValue(new Error('boom'))
    render(<ChartPreview viewName="broken" />)
    await waitFor(() => expect(screen.getByText('boom')).toBeInTheDocument())
  })
})
