// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * ExplorePage component tests (Epic 26 VG-291).
 *
 * Pin down the load → render path for each tab, the URL-state contract
 * (entity + tab persist in querystring), and the groupActivity helper
 * since it drives the ontology-bump clustering on the Activity tab.
 */

import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'

import type {
  ActivityEvent, ActivityFeed, ChartSummary, EntityDetail, EntitySummary,
  PipelineSummary,
} from '@/api/client'
import { ExplorePage, formatWhen, groupActivity } from './ExplorePage'

// ---------------------------------------------------------------------------
// Fake API + Model mock
// ---------------------------------------------------------------------------

type FakeApi = {
  listEntities: () => Promise<EntitySummary[]>
  getEntity: (e: string) => Promise<EntityDetail>
  listEntityCharts: (e: string) => Promise<ChartSummary[]>
  getEntityPipeline: (e: string) => Promise<PipelineSummary | null>
  getEntityActivity: (e: string) => Promise<ActivityFeed>
  listProposals: (params?: object) => Promise<unknown[]>
  executeView: (name: string, limit?: number) => Promise<unknown>
  getView: (name: string) => Promise<unknown>
  listQueries: () => Promise<unknown[]>
  saveView: (name: string, content: string) => Promise<unknown>
}

let fakeApi: FakeApi

vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: fakeApi }),
}))

// EntityListFrame is dynamically imported by the Records tab; stub it
// so the lazy import resolves to a known harmless component.
vi.mock('@/pages/explore/EntityListFrame', () => ({
  EntityListFrame: ({ entity }: { entity: string }) => (
    <div data-testid="entity-list-frame">records for {entity}</div>
  ),
}))

// ChartPreview hits api.executeView; stub it here so the chart cards
// render a known marker instead of trying to lay out a real chart.
vi.mock('@/components/explore/ChartPreview', () => ({
  ChartPreview: ({ viewName }: { viewName: string }) => (
    <div data-testid={`chart-preview-${viewName}`}>preview {viewName}</div>
  ),
}))

// ChartDetailDrawer pulls in ViewContent (and Recharts); stub it here so
// the click-to-open behavior is observable without rendering real charts.
vi.mock('@/components/explore/ChartDetailDrawer', () => ({
  ChartDetailDrawer: ({ viewName, onClose }: { viewName: string; onClose: () => void }) => (
    <div data-testid={`chart-detail-${viewName}`}>
      detail {viewName}
      <button onClick={onClose}>close-detail</button>
    </div>
  ),
}))

vi.mock('@/components/explore/NewChartDrawer', () => ({
  NewChartDrawer: ({ entity, onClose }: { entity: string; onClose: () => void }) => (
    <div data-testid={`new-chart-${entity}`}>
      new chart for {entity}
      <button onClick={onClose}>close-new</button>
    </div>
  ),
}))

function makeApi(overrides: Partial<FakeApi> = {}): FakeApi {
  return {
    listEntities: vi.fn(async () => [WIDGET, GADGET]),
    getEntity: vi.fn(async (name: string) => entityDetail(name)),
    listEntityCharts: vi.fn(async () => []),
    getEntityPipeline: vi.fn(async () => null),
    getEntityActivity: vi.fn(async () => ({ events: [], has_more: false })),
    listProposals: vi.fn(async () => []),
    executeView: vi.fn(async () => ({})),
    getView: vi.fn(async () => ({})),
    listQueries: vi.fn(async () => []),
    saveView: vi.fn(async () => ({})),
    ...overrides,
  }
}

const WIDGET: EntitySummary = {
  name: 'Widget', table_name: 'widget',
  attribute_count: 3, relation_count: 1, feature_count: 2,
  row_count: 247, table_exists: true,
}
const GADGET: EntitySummary = {
  name: 'Gadget', table_name: 'gadget',
  attribute_count: 1, relation_count: 0, feature_count: 0,
  row_count: 0, table_exists: true,
}

function entityDetail(name: string): EntityDetail {
  return {
    name,
    table_name: name.toLowerCase(),
    attributes: [{ name: 'id', type: 'STRING', semantic: 'PRIMARY_KEY' }],
    relations: [],
    features: [],
    database: { present: true, row_count: 0, last_updated_at: null },
    display_list: [], display_detail: [], display_order: [], raw_yaml: '',
  }
}

function renderAt(url: string) {
  return render(
    <MemoryRouter initialEntries={[url]}>
      <Routes>
        <Route path="/explore" element={<ExplorePage />} />
      </Routes>
    </MemoryRouter>,
  )
}

// ---------------------------------------------------------------------------
// Sidebar + URL state
// ---------------------------------------------------------------------------

describe('ExplorePage sidebar + routing', () => {
  it('lists entities in the sidebar after load', async () => {
    fakeApi = makeApi()
    renderAt('/explore')
    // Widget appears twice (sidebar entry + header h1) once the selection
    // settles. Gadget is sidebar-only since it's not the selected entity.
    await waitFor(() => {
      expect(screen.getAllByText('Widget').length).toBeGreaterThanOrEqual(1)
    })
    expect(screen.getByText('Gadget')).toBeInTheDocument()
  })

  it('shows the entity from the URL querystring on first render', async () => {
    fakeApi = makeApi()
    renderAt('/explore?entity=Gadget&tab=charts')
    // Gadget should be the *selected* one (header shows its name as h1)
    await waitFor(() => {
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toHaveTextContent('Gadget')
    })
  })

  it('falls back to the first entity when no entity in URL', async () => {
    fakeApi = makeApi()
    renderAt('/explore')
    await waitFor(() => {
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Widget')
    })
  })

  it('selecting an entity in the sidebar updates the heading', async () => {
    fakeApi = makeApi()
    renderAt('/explore')
    await screen.findByText('Gadget')
    fireEvent.click(screen.getByText('Gadget'))
    await waitFor(() => {
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Gadget')
    })
  })
})

// ---------------------------------------------------------------------------
// Each tab loads its data
// ---------------------------------------------------------------------------

describe('ExplorePage tabs', () => {
  it('Charts tab fetches charts and renders them', async () => {
    const charts: ChartSummary[] = [
      { name: 'pr_trend', type: 'chart', query: 'q1', chart_type: 'line' },
      { name: 'pr_count', type: 'chart', query: 'q2', chart_type: 'bar' },
    ]
    fakeApi = makeApi({ listEntityCharts: vi.fn(async () => charts) })
    renderAt('/explore?entity=Widget&tab=charts')
    expect(await screen.findByText('pr_trend')).toBeInTheDocument()
    expect(screen.getByText('pr_count')).toBeInTheDocument()
  })

  it('Clicking a chart card opens the detail drawer (VG-302)', async () => {
    const charts: ChartSummary[] = [
      { name: 'pr_trend', type: 'chart', query: 'q1', chart_type: 'line' },
    ]
    fakeApi = makeApi({ listEntityCharts: vi.fn(async () => charts) })
    renderAt('/explore?entity=Widget&tab=charts')
    const cardLabel = await screen.findByText('pr_trend')
    // Find the enclosing card button — both label and chart_type live inside.
    const card = cardLabel.closest('button')
    expect(card).not.toBeNull()
    fireEvent.click(card!)
    expect(await screen.findByTestId('chart-detail-pr_trend')).toBeInTheDocument()
  })

  it('Charts tab shows empty state when no charts', async () => {
    fakeApi = makeApi()
    renderAt('/explore?entity=Widget&tab=charts')
    expect(await screen.findByText(/No charts yet for Widget/)).toBeInTheDocument()
  })

  it('Overview KPI cards execute their view and render the scalar (VG-301)', async () => {
    const charts: ChartSummary[] = [
      { name: 'open_prs', type: 'metric', query: 'q_open', chart_type: 'kpi' },
    ]
    fakeApi = makeApi({
      listEntityCharts: vi.fn(async () => charts),
      executeView: vi.fn(async (name: string) => ({
        name, type: 'metric',
        measure: 'n',
        columns: ['n'],
        rows: [[42]],
        visualization: { suffix: 'open' },
        formats: { n: { type: 'number', pattern: null, unit: null } },
        params: [],
      })),
    })
    renderAt('/explore?entity=Widget&tab=overview')
    // Wait for the KPI to resolve.
    expect(await screen.findByText('42')).toBeInTheDocument()
    expect(screen.getByText('open')).toBeInTheDocument()
    expect(fakeApi.executeView).toHaveBeenCalledWith('open_prs', 1)
  })

  it('Overview KPI cards show em-dash when no value comes back (VG-301)', async () => {
    const charts: ChartSummary[] = [
      { name: 'empty_kpi', type: 'metric', query: 'q', chart_type: 'kpi' },
    ]
    fakeApi = makeApi({
      listEntityCharts: vi.fn(async () => charts),
      executeView: vi.fn(async () => ({
        name: 'empty_kpi', type: 'metric',
        measure: 'n', columns: ['n'], rows: [],
        visualization: {}, formats: {}, params: [],
      })),
    })
    renderAt('/explore?entity=Widget&tab=overview')
    // Two em-dashes possible (one in label area, one as KPI value); we just
    // need to confirm the KPI didn't crash and rendered the empty marker.
    await waitFor(() => expect(screen.getAllByText('—').length).toBeGreaterThanOrEqual(1))
  })

  it('Schema tab fetches detail and shows attributes', async () => {
    fakeApi = makeApi({
      getEntity: vi.fn(async (name) => ({
        ...entityDetail(name),
        attributes: [
          { name: 'state', type: 'enum', semantic: 'STATUS' },
          { name: 'merged_at', type: 'timestamp', semantic: 'TIMESTAMP' },
        ],
      })),
    })
    renderAt('/explore?entity=Widget&tab=schema')
    expect(await screen.findByText('state')).toBeInTheDocument()
    expect(screen.getByText('merged_at')).toBeInTheDocument()
  })

  it('Schema tab attribute pencil opens the Propose change form (VG-296)', async () => {
    fakeApi = makeApi({
      getEntity: vi.fn(async (name) => ({
        ...entityDetail(name),
        attributes: [{ name: 'state', type: 'enum', semantic: 'STATUS' }],
      })),
    })
    renderAt('/explore?entity=Widget&tab=schema')
    await screen.findByText('state')
    // Attribute rows are governed → pencil exists and is enabled with
    // "Propose change" tooltip (replaces the VG-291 read-only state).
    const pencil = screen.getByRole('button', { name: /Propose change/i })
    expect(pencil).toBeEnabled()
  })

  it('Pipeline tab renders lineage chips when pipeline exists', async () => {
    const pipeline: PipelineSummary = {
      entity: 'Widget',
      sources: [{ tool: 'github', extractor: 'gh_widgets', raw_table: 'raw_widgets' }],
      mapper: { name: 'widget_mapper', groups: [] },
    }
    fakeApi = makeApi({ getEntityPipeline: vi.fn(async () => pipeline) })
    renderAt('/explore?entity=Widget&tab=pipeline')
    expect(await screen.findByText('github')).toBeInTheDocument()
    expect(screen.getByText('gh_widgets')).toBeInTheDocument()
    expect(screen.getByText('raw_widgets')).toBeInTheDocument()
    expect(screen.getByText('widget_mapper')).toBeInTheDocument()
  })

  it('Pipeline tab shows empty state when no mapper', async () => {
    fakeApi = makeApi({ getEntityPipeline: vi.fn(async () => null) })
    renderAt('/explore?entity=Widget&tab=pipeline')
    expect(await screen.findByText(/No pipeline configured for Widget/)).toBeInTheDocument()
  })

  it('Pipeline tab surfaces multiple sources stacked', async () => {
    const pipeline: PipelineSummary = {
      entity: 'PullRequest',
      sources: [
        { tool: 'github', extractor: 'gh_pulls', raw_table: 'raw_pulls' },
        { tool: 'github', extractor: 'gh_users', raw_table: 'raw_users' },
      ],
      mapper: { name: 'pr_mapper', groups: [] },
    }
    fakeApi = makeApi({ getEntityPipeline: vi.fn(async () => pipeline) })
    renderAt('/explore?entity=Widget&tab=pipeline')
    expect(await screen.findByText('raw_pulls')).toBeInTheDocument()
    expect(screen.getByText('raw_users')).toBeInTheDocument()
  })

  it('Pipeline tab lists mapper sub-groups when present', async () => {
    const pipeline: PipelineSummary = {
      entity: 'Contribution',
      sources: [{ tool: 'github', extractor: 'gh', raw_table: 'raw' }],
      mapper: { name: 'contributions', groups: [
        { name: 'authors' }, { name: 'reviews' }, { name: 'commits' },
      ]},
    }
    fakeApi = makeApi({ getEntityPipeline: vi.fn(async () => pipeline) })
    renderAt('/explore?entity=Widget&tab=pipeline')
    expect(await screen.findByText('authors')).toBeInTheDocument()
    expect(screen.getByText('reviews')).toBeInTheDocument()
    expect(screen.getByText('commits')).toBeInTheDocument()
  })

  it('Activity tab renders ontology bump cards + artifact events', async () => {
    const events: ActivityEvent[] = [
      { actor: 'alice', action: 'updated', object_kind: 'chart',
        object_name: 'pr_throughput', created_at: '2026-05-26T10:00:00Z',
        note: 'v3 → v4', ontology_version: null },
      { actor: 'bob', action: 'created', object_kind: 'attribute',
        object_name: 'churn', created_at: '2026-05-25T10:00:00Z',
        note: null, ontology_version: 'v17 → v18' },
      { actor: 'bob', action: 'created', object_kind: 'computed',
        object_name: 'churn_score', created_at: '2026-05-25T10:00:00Z',
        note: null, ontology_version: 'v17 → v18' },
    ]
    fakeApi = makeApi({
      getEntityActivity: vi.fn(async () => ({ events, has_more: false })),
    })
    renderAt('/explore?entity=Widget&tab=activity')
    // Ontology bump: two events cluster under one card with "changed 2 things"
    expect(await screen.findByText(/changed 2 things/i)).toBeInTheDocument()
    // Both row-level changes show as list items inside the card
    expect(screen.getByText('churn')).toBeInTheDocument()
    expect(screen.getByText('churn_score')).toBeInTheDocument()
    // Artifact event renders separately
    expect(screen.getByText('pr_throughput')).toBeInTheDocument()
  })

  it('Records tab embeds the existing EntityListFrame', async () => {
    fakeApi = makeApi()
    renderAt('/explore?entity=Widget&tab=records')
    await waitFor(() => {
      expect(screen.getByTestId('entity-list-frame')).toHaveTextContent('records for Widget')
    })
  })
})

// ---------------------------------------------------------------------------
// Tab switching — clicks should re-render the right tab
// ---------------------------------------------------------------------------

describe('ExplorePage tab navigation', () => {
  it('clicking a tab swaps the visible content', async () => {
    const charts: ChartSummary[] = [
      { name: 'only_chart', type: 'chart', query: 'q1', chart_type: 'bar' },
    ]
    fakeApi = makeApi({ listEntityCharts: vi.fn(async () => charts) })
    renderAt('/explore?entity=Widget&tab=overview')
    // Overview kicks off — wait for it to settle
    await waitFor(() => expect(fakeApi.listEntityCharts).toHaveBeenCalled())
    // Click the Charts tab
    fireEvent.click(screen.getByRole('button', { name: /Charts/i }))
    expect(await screen.findByText('only_chart')).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// Pure-function helpers
// ---------------------------------------------------------------------------

describe('groupActivity', () => {
  it('clusters consecutive ontology events with the same version', () => {
    const events: ActivityEvent[] = [
      { actor: 'a', action: 'updated', object_kind: 'attribute', object_name: 'x',
        created_at: 't1', note: null, ontology_version: 'v1 → v2' },
      { actor: 'a', action: 'created', object_kind: 'relation', object_name: 'y',
        created_at: 't1', note: null, ontology_version: 'v1 → v2' },
    ]
    const groups = groupActivity(events)
    expect(groups).toHaveLength(1)
    expect(groups[0].kind).toBe('ontology')
    if (groups[0].kind === 'ontology') {
      expect(groups[0].events).toHaveLength(2)
    }
  })

  it('does not merge ontology events across different versions', () => {
    const events: ActivityEvent[] = [
      { actor: 'a', action: 'updated', object_kind: 'attribute', object_name: 'x',
        created_at: 't1', note: null, ontology_version: 'v2 → v3' },
      { actor: 'a', action: 'created', object_kind: 'attribute', object_name: 'y',
        created_at: 't2', note: null, ontology_version: 'v1 → v2' },
    ]
    expect(groupActivity(events)).toHaveLength(2)
  })

  it('keeps artifact events as their own single-event groups', () => {
    const events: ActivityEvent[] = [
      { actor: 'a', action: 'updated', object_kind: 'chart', object_name: 'c1',
        created_at: 't1', note: null, ontology_version: null },
      { actor: 'a', action: 'updated', object_kind: 'mapper', object_name: 'm1',
        created_at: 't2', note: null, ontology_version: null },
    ]
    const groups = groupActivity(events)
    expect(groups).toHaveLength(2)
    expect(groups.every((g) => g.kind === 'artifact')).toBe(true)
  })

  it('interleaving ontology + artifact events produces alternating groups', () => {
    const events: ActivityEvent[] = [
      { actor: 'a', action: 'updated', object_kind: 'attribute', object_name: 'x',
        created_at: 't1', note: null, ontology_version: 'v1 → v2' },
      { actor: 'b', action: 'updated', object_kind: 'chart', object_name: 'c1',
        created_at: 't2', note: null, ontology_version: null },
      { actor: 'c', action: 'created', object_kind: 'relation', object_name: 'r1',
        created_at: 't3', note: null, ontology_version: 'v2 → v3' },
    ]
    const groups = groupActivity(events)
    expect(groups.map((g) => g.kind)).toEqual(['ontology', 'artifact', 'ontology'])
  })
})

describe('formatWhen', () => {
  it('returns "just now" for very recent timestamps', () => {
    const now = new Date(Date.now() - 5_000).toISOString()
    expect(formatWhen(now)).toBe('just now')
  })

  it('returns Nm ago for sub-hour deltas', () => {
    const fiveMinAgo = new Date(Date.now() - 5 * 60_000).toISOString()
    expect(formatWhen(fiveMinAgo)).toBe('5m ago')
  })

  it('returns Nh ago for sub-day deltas', () => {
    const threeHourAgo = new Date(Date.now() - 3 * 3600_000).toISOString()
    expect(formatWhen(threeHourAgo)).toBe('3h ago')
  })

  it('falls back to raw string on malformed input', () => {
    expect(formatWhen('not-a-date')).toBe('not-a-date')
  })
})
