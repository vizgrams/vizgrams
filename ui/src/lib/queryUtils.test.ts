// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, it, expect } from 'vitest'
import { draftToYaml, detailToDraft, applyNumberPattern, SAMPLE } from './queryUtils'
import type { QueryDraft, MeasureRow, OrderRow } from './queryUtils'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function emptyDraft(): QueryDraft {
  return { name: '', description: '', root: '', attributes: [], measures: [], filters: [], order: [], params: [] }
}

// ---------------------------------------------------------------------------
// draftToYaml
// ---------------------------------------------------------------------------

describe('draftToYaml', () => {
  it('filter quoting — double quotes in value wrapped in single quotes', () => {
    const draft: QueryDraft = {
      ...emptyDraft(),
      name: 'q',
      filters: [{ id: '1', expr: 'status == "Done"' }],
    }
    const yaml = draftToYaml(draft)
    expect(yaml).toContain(`- 'status == "Done"'`)
  })

  it('filter quoting — single quotes in value wrapped in double quotes', () => {
    const draft: QueryDraft = {
      ...emptyDraft(),
      name: 'q',
      filters: [{ id: '1', expr: "status == 'open'" }],
    }
    const yaml = draftToYaml(draft)
    expect(yaml).toContain(`- "status == 'open'"`)
  })

  it('filter quoting — plain filter is quoted', () => {
    const draft: QueryDraft = {
      ...emptyDraft(),
      name: 'q',
      filters: [{ id: '1', expr: 'count > 5' }],
    }
    const yaml = draftToYaml(draft)
    // should be wrapped in either single or double quotes
    expect(yaml).toMatch(/- ['"]count > 5['"]/)
  })

  it('measures with cross-entity fields produce correct expr', () => {
    const measure: MeasureRow = {
      id: '1', name: 'review_count', agg: 'count',
      field: 'PullRequestReviewComment.pull_request_key',
      formatType: '', formatPattern: '', formatUnit: '', showFormat: false,
    }
    const draft: QueryDraft = { ...emptyDraft(), name: 'q', measures: [measure] }
    const yaml = draftToYaml(draft)
    expect(yaml).toContain('expr: count(PullRequestReviewComment.pull_request_key)')
  })

  it('measures with empty field and count produce count()', () => {
    const measure: MeasureRow = {
      id: '1', name: 'total', agg: 'count', field: '',
      formatType: '', formatPattern: '', formatUnit: '', showFormat: false,
    }
    const draft: QueryDraft = { ...emptyDraft(), name: 'q', measures: [measure] }
    const yaml = draftToYaml(draft)
    expect(yaml).toContain('expr: count()')
  })

  it('order rows produce correct order block', () => {
    const order: OrderRow[] = [
      { id: '1', field: 'week_key', direction: 'asc' },
      { id: '2', field: 'score', direction: 'desc' },
    ]
    const draft: QueryDraft = { ...emptyDraft(), name: 'q', order }
    const yaml = draftToYaml(draft)
    expect(yaml).toContain('order:')
    expect(yaml).toContain('  - week_key: asc')
    expect(yaml).toContain('  - score: desc')
  })

  it('empty draft name falls back to my_query', () => {
    const draft: QueryDraft = { ...emptyDraft(), name: '' }
    const yaml = draftToYaml(draft)
    expect(yaml).toContain('name: my_query')
  })
})

// ---------------------------------------------------------------------------
// detailToDraft — fixture
// ---------------------------------------------------------------------------

const personCountsWeeklyDetail = {
  name: 'person_counts_weekly',
  root: 'PullRequest',
  description: null,
  group_by: [],
  attributes: [
    { field: 'created_at', alias: 'week_key', format_pattern: 'YYYY-WW' },
    { field: 'is_authored_by.subject.name', alias: 'identity', format_pattern: '' },
  ],
  measures: {
    pr_count: { expr: 'count(pull_request_key)', format: { type: 'number', pattern: '0', unit: null } },
    review_comment_count: { expr: 'count(PullRequestReviewComment.pull_request_key)', format: { type: 'number', pattern: '0', unit: null } },
  },
  where: ['created_at >= now() - 4w'],
  order_by: [{ field: 'week_key', direction: 'asc' }, { field: 'identity', direction: 'asc' }],
  detail_attributes: [],
  compiled_sql: null,
  raw_yaml: null,
}

describe('detailToDraft', () => {
  it('measures are parsed — 2 entries', () => {
    const draft = detailToDraft(personCountsWeeklyDetail)
    expect(draft.measures).toHaveLength(2)
  })

  it('cross-entity measure field is preserved', () => {
    const draft = detailToDraft(personCountsWeeklyDetail)
    const reviewMeasure = draft.measures.find(m => m.name === 'review_comment_count')
    expect(reviewMeasure).toBeDefined()
    expect(reviewMeasure!.field).toBe('PullRequestReviewComment.pull_request_key')
  })

  it('order is parsed — 2 entries with correct field/direction', () => {
    const draft = detailToDraft(personCountsWeeklyDetail)
    expect(draft.order).toHaveLength(2)
    expect(draft.order[0].field).toBe('week_key')
    expect(draft.order[0].direction).toBe('asc')
    expect(draft.order[1].field).toBe('identity')
    expect(draft.order[1].direction).toBe('asc')
  })

  it('attribute with format_pattern uses format_time expr', () => {
    const draft = detailToDraft(personCountsWeeklyDetail)
    const weekAttr = draft.attributes.find(a => a.alias === 'week_key')
    expect(weekAttr).toBeDefined()
    expect(weekAttr!.expr).toContain('format_time')
  })

  it('format info is preserved on measure', () => {
    const draft = detailToDraft(personCountsWeeklyDetail)
    const prCount = draft.measures.find(m => m.name === 'pr_count')
    expect(prCount).toBeDefined()
    expect(prCount!.formatType).toBe('number')
    expect(prCount!.formatPattern).toBe('0')
  })
})

// ---------------------------------------------------------------------------
// applyNumberPattern
// ---------------------------------------------------------------------------

describe('applyNumberPattern', () => {
  const N = SAMPLE  // 12345.6

  it("'0' → integer rounded", () => {
    expect(applyNumberPattern(N, '0')).toBe('12346')
  })

  it("'0,0' → thousands separator", () => {
    expect(applyNumberPattern(N, '0,0')).toBe('12,346')
  })

  it("'0.0' → one decimal", () => {
    expect(applyNumberPattern(N, '0.0')).toBe('12345.6')
  })

  it("'0,0.0' → thousands + one decimal", () => {
    expect(applyNumberPattern(N, '0,0.0')).toBe('12,345.6')
  })

  it("'0a' → abbreviated integer", () => {
    expect(applyNumberPattern(N, '0a')).toBe('12k')
  })

  it("'0.0a' → abbreviated one decimal", () => {
    expect(applyNumberPattern(N, '0.0a')).toBe('12.3k')
  })
})
