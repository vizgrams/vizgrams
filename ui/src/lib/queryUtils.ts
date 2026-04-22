// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import type { QueryDetail } from '@/api/client'

// ---------------------------------------------------------------------------
// Draft types
// ---------------------------------------------------------------------------

export interface AttributeRow { id: string; alias: string; expr: string }
export interface MeasureRow {
  id: string; name: string; agg: string; field: string
  formatType: string; formatPattern: string; formatUnit: string; showFormat: boolean
  rawExpr?: string  // set when expr can't be represented as simple agg(field)
}
export interface FilterRow { id: string; expr: string }
export interface OrderRow { id: string; field: string; direction: 'asc' | 'desc' }

export interface ParamRow { name: string; type: string; label: string; default: string; optional: boolean }

export interface QueryDraft {
  name: string; description: string; root: string
  attributes: AttributeRow[]
  measures: MeasureRow[]
  filters: FilterRow[]
  order: OrderRow[]
  params: ParamRow[]
}

export function makeId() { return Math.random().toString(36).slice(2, 9) }

// ---------------------------------------------------------------------------
// detailToDraft
// ---------------------------------------------------------------------------

export function detailToDraft(detail: QueryDetail): QueryDraft {
  // detail_attributes: plain-string or detail:true dict attrs (QueryAttribute objects server-side)
  // attributes: aggregate slices (SliceDef objects server-side)
  // group_by: legacy alias for attributes
  // Priority: detail_attributes when there are no slices (pure detail query),
  // otherwise slices take precedence (aggregate or mixed query).
  const srcAttrs: { field: string; alias: string; format_pattern?: string }[] =
    detail.detail_attributes?.length && !detail.attributes?.length
      ? detail.detail_attributes
      : detail.attributes?.length
      ? detail.attributes
      : detail.group_by.map(g => ({ field: g, alias: '' }))
  const attributes: AttributeRow[] = srcAttrs.map(a => ({
    id: makeId(), alias: a.alias || '',
    expr: a.format_pattern ? `format_time(${a.field}, '${a.format_pattern}')` : a.field,
  }))
  const measures: MeasureRow[] = Object.entries(detail.measures).map(([name, config]) => {
    const c = config as Record<string, unknown>
    const expr = String(c.expr || '')
    const match = expr.match(/^(\w+)\(([^)]*)\)$/)
    const fmt = c.format as Record<string, string> | null
    return {
      id: makeId(), name, agg: match?.[1] || 'count', field: match?.[2] || '',
      formatType: fmt?.type || '', formatPattern: fmt?.pattern || '',
      formatUnit: fmt?.unit || '', showFormat: !!fmt?.type,
      rawExpr: match ? undefined : expr,
    }
  })
  const filters: FilterRow[] = detail.where.map(w => ({ id: makeId(), expr: w }))
  const order: OrderRow[] = detail.order_by.map(o => ({ id: makeId(), field: o.field, direction: o.direction as 'asc' | 'desc' }))
  const params: ParamRow[] = (detail.params ?? []).map(p => ({ name: p.name, type: p.type, label: p.label ?? '', default: p.default ?? '', optional: p.optional }))
  return { name: detail.name, description: detail.description || '', root: detail.root || '', attributes, measures, filters, order, params }
}

// ---------------------------------------------------------------------------
// draftToYaml
// ---------------------------------------------------------------------------

export function draftToYaml(d: QueryDraft): string {
  const lines: string[] = []
  lines.push(`name: ${d.name || 'my_query'}`)
  if (d.description) lines.push(`description: "${d.description.replace(/"/g, '\\"')}"`)
  if (d.root) lines.push(`root: ${d.root}`)
  lines.push('')
  if (d.params.length > 0) {
    lines.push('params:')
    for (const p of d.params) {
      lines.push(`  - name: ${p.name}`)
      lines.push(`    type: ${p.type}`)
      if (p.label) lines.push(`    label: ${p.label}`)
      if (p.default) lines.push(`    default: "${p.default}"`)
      if (p.optional) lines.push(`    optional: true`)
    }
    lines.push('')
  }
  if (d.attributes.length > 0) {
    lines.push('attributes:')
    for (const a of d.attributes) {
      const alias = a.alias || a.expr.split('.').pop()?.replace(/\(.*/, '') || 'field'
      lines.push(`  - ${alias}: ${a.expr || 'field_name'}`)
    }
    lines.push('')
  }
  if (d.measures.length > 0) {
    lines.push('measures:')
    for (const m of d.measures) {
      if (!m.name) continue
      const expr = m.rawExpr ?? (m.agg === 'count' && !m.field ? 'count()' : `${m.agg}(${m.field})`)
      lines.push(`  - ${m.name}:`)
      lines.push(`      expr: ${expr}`)
      if (m.formatType) {
        lines.push('      format:')
        lines.push(`        type: ${m.formatType}`)
        if (m.formatPattern) lines.push(`        pattern: "${m.formatPattern}"`)
        if (m.formatUnit) lines.push(`        unit: ${m.formatUnit}`)
      }
    }
    lines.push('')
  }
  const validFilters = d.filters.filter(f => f.expr.trim())
  if (validFilters.length > 0) {
    lines.push('where:')
    for (const f of validFilters) {
      const expr = f.expr
      const quoted = expr.includes('"') ? `'${expr}'` : `"${expr}"`
      lines.push(`  - ${quoted}`)
    }
    lines.push('')
  } else {
    lines.push('where: []')
    lines.push('')
  }
  if (d.order.length > 0) {
    lines.push('order:')
    for (const o of d.order) { if (o.field) lines.push(`  - ${o.field}: ${o.direction}`) }
  }
  return lines.join('\n').trimEnd()
}

// ---------------------------------------------------------------------------
// applyNumberPattern
// ---------------------------------------------------------------------------

export const SAMPLE = 12345.6

export function applyNumberPattern(value: number, pattern: string): string {
  const abbreviated = pattern.endsWith('a')
  const base = abbreviated ? pattern.slice(0, -1) : pattern
  const useThousands = base.includes(',')
  const decMatch = base.match(/\.(\d+)$/)
  const decimals = decMatch ? decMatch[1].length : 0

  let v = value
  let suffix = ''
  if (abbreviated) {
    if (Math.abs(v) >= 1_000_000) { v = v / 1_000_000; suffix = 'M' }
    else if (Math.abs(v) >= 1_000) { v = v / 1_000; suffix = 'k' }
  }

  const fixed = v.toFixed(decimals)
  const [intPart, decPart] = fixed.split('.')
  const formattedInt = useThousands
    ? intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
    : intPart
  return decPart !== undefined ? `${formattedInt}.${decPart}${suffix}` : `${formattedInt}${suffix}`
}
