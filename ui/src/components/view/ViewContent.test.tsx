// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

import { ViewContent } from './ViewContent'
import type { ViewResult } from '@/api/client'

const tableResult: ViewResult = {
  name: 'dora_clt_by_team',
  type: 'table',
  columns: ['team', 'avg_clt'],
  rows: [['platform', 12], ['frontend', 9]],
  visualization: {
    columns: ['team', 'avg_clt'],
  } as Record<string, unknown>,
  row_count: 2,
  total_row_count: 2,
  duration_ms: 1,
  truncated: false,
  measure: null,
  formats: null,
  params: [],
}

describe('ViewContent table drilldowns', () => {
  it('renders an app-drilldown button per row when appDrilldown is configured', () => {
    const onNavigate = vi.fn()
    render(
      <ViewContent
        result={tableResult}
        rowDrilldown={{ view: 'pr_clt_detail', params: { team_name: 'row.team' } }}
        appDrilldown={{ app: 'team_health', params: { team_name: 'row.team' } }}
        paramValues={{}}
        onNavigate={onNavigate}
      />,
    )
    const buttons = screen.getAllByTitle('Open team_health')
    expect(buttons).toHaveLength(2)
  })

  it('clicking the app-drilldown button navigates to the app and does not fire row drilldown', () => {
    const onNavigate = vi.fn()
    render(
      <ViewContent
        result={tableResult}
        rowDrilldown={{ view: 'pr_clt_detail', params: { team_name: 'row.team' } }}
        appDrilldown={{ app: 'team_health', params: { team_name: 'row.team' } }}
        paramValues={{}}
        onNavigate={onNavigate}
      />,
    )
    fireEvent.click(screen.getAllByTitle('Open team_health')[0])
    expect(onNavigate).toHaveBeenCalledTimes(1)
    expect(onNavigate).toHaveBeenCalledWith({
      kind: 'app',
      name: 'team_health',
      params: { team_name: 'platform' },
    })
  })

  it('row click still fires row drilldown independently of the app button', () => {
    const onNavigate = vi.fn()
    render(
      <ViewContent
        result={tableResult}
        rowDrilldown={{ view: 'pr_clt_detail', params: { team_name: 'row.team' } }}
        appDrilldown={{ app: 'team_health', params: { team_name: 'row.team' } }}
        paramValues={{}}
        onNavigate={onNavigate}
      />,
    )
    fireEvent.click(screen.getByText('frontend'))
    expect(onNavigate).toHaveBeenCalledWith({
      kind: 'view',
      name: 'pr_clt_detail',
      params: { team_name: 'frontend' },
    })
  })

  it('omits the app-drilldown column when only rowDrilldown is configured', () => {
    const onNavigate = vi.fn()
    render(
      <ViewContent
        result={tableResult}
        rowDrilldown={{ view: 'pr_clt_detail', params: { team_name: 'row.team' } }}
        paramValues={{}}
        onNavigate={onNavigate}
      />,
    )
    expect(screen.queryByTitle(/Open /)).toBeNull()
  })
})
