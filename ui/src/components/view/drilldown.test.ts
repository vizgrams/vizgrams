// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest'

import { frameToUrl, resolveViewDrilldown } from './drilldown'
import type { DrillFrame, ViewDrilldownConfig } from './drilldown'

// ---------------------------------------------------------------------------
// frameToUrl — URL serialisation for every DrillFrame kind.
//
// Why: this is the single source of truth for drilldown URLs across the
// product (chat, explorer, app, entity detail). A regression here breaks
// every cross-surface click silently.
// ---------------------------------------------------------------------------

describe('frameToUrl', () => {
  it('serialises a view frame with no params', () => {
    const frame: DrillFrame = { kind: 'view', name: 'dora_clt_by_team', params: {} }
    expect(frameToUrl(frame)).toBe('/views/dora_clt_by_team')
  })

  it('serialises a view frame with params as a query string', () => {
    const frame: DrillFrame = {
      kind: 'view', name: 'dora_clt_by_team',
      params: { team: 'lovelace', weeks: '12' },
    }
    const url = frameToUrl(frame)
    expect(url).toMatch(/^\/views\/dora_clt_by_team\?/)
    // URLSearchParams may pick its own order; assert by parsing.
    const qs = new URLSearchParams(url.split('?')[1])
    expect(qs.get('team')).toBe('lovelace')
    expect(qs.get('weeks')).toBe('12')
  })

  it('serialises an app frame the same way as a view (different prefix)', () => {
    const frame: DrillFrame = {
      kind: 'app', name: 'dora_dashboard', params: { team: 'lovelace' },
    }
    expect(frameToUrl(frame)).toBe('/apps/dora_dashboard?team=lovelace')
  })

  it('serialises an entity-list frame', () => {
    const frame: DrillFrame = { kind: 'entity-list', entity: 'PullRequest' }
    expect(frameToUrl(frame)).toBe('/entities/PullRequest')
  })

  it('serialises an entity-detail frame', () => {
    const frame: DrillFrame = {
      kind: 'entity-detail', entity: 'PullRequest', id: '01HXYZ',
    }
    expect(frameToUrl(frame)).toBe('/entities/PullRequest/01HXYZ')
  })

  it('URL-encodes names + ids with unsafe characters', () => {
    expect(frameToUrl({ kind: 'view', name: 'foo bar', params: {} }))
      .toBe('/views/foo%20bar')
    expect(frameToUrl({
      kind: 'entity-detail', entity: 'PullRequest', id: 'has/slash',
    })).toBe('/entities/PullRequest/has%2Fslash')
  })
})

// ---------------------------------------------------------------------------
// resolveViewDrilldown — table-row drilldown resolution.
//
// One canonical case per target kind; the other two resolvers
// (resolveMarkerAction, resolvePointDrilldown) share the same shape so
// covering this one is sufficient to lock the contract in.
// ---------------------------------------------------------------------------

describe('resolveViewDrilldown', () => {
  const COLUMNS = ['week', 'team', 'pr_count', 'team_id']
  const ROW = ['2026-w20', 'lovelace', 42, 'team-abc']

  it('resolves entity targets via id_column', () => {
    const config: ViewDrilldownConfig = {
      entity: 'Team', id_column: 'team_id',
    }
    const frame = resolveViewDrilldown(config, ROW, COLUMNS, {})
    expect(frame).toEqual({ kind: 'entity-detail', entity: 'Team', id: 'team-abc' })
  })

  it('returns null when id_column value is missing on the row', () => {
    const config: ViewDrilldownConfig = {
      entity: 'Team', id_column: 'missing_column',
    }
    expect(resolveViewDrilldown(config, ROW, COLUMNS, {})).toBeNull()
  })

  it('resolves view targets with row.<col> template params', () => {
    const config: ViewDrilldownConfig = {
      view: 'team_detail',
      params: { team_name: 'row.team' },
    }
    const frame = resolveViewDrilldown(config, ROW, COLUMNS, {})
    expect(frame).toEqual({
      kind: 'view', name: 'team_detail', params: { team_name: 'lovelace' },
    })
  })

  it('merges source params with row-derived params (row wins)', () => {
    const config: ViewDrilldownConfig = {
      view: 'team_detail',
      params: { team_name: 'row.team' },
    }
    const frame = resolveViewDrilldown(
      config, ROW, COLUMNS,
      { team_name: 'old', extra: 'preserved' },
    )
    expect(frame).toEqual({
      kind: 'view',
      name: 'team_detail',
      params: { team_name: 'lovelace', extra: 'preserved' },
    })
  })

  it('returns null when no drilldown target is specified', () => {
    expect(resolveViewDrilldown({}, ROW, COLUMNS, {})).toBeNull()
  })
})
