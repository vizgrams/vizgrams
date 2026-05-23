// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * Drilldown configuration types + resolvers shared between every view-
 * rendering surface (ViewsPage, EntitiesPage, AppPage, chat).
 *
 * A "drilldown" turns a chart-point click / table-row click / map-marker
 * click into a navigation target — typically another view, an app, or
 * an entity detail page. Resolvers take the raw click data + the
 * current view's param values and produce a typed ``DrillFrame``;
 * ``frameToUrl`` converts that to a router path the caller can navigate to.
 */

// ---------------------------------------------------------------------------
// Frame types + URL serialization
// ---------------------------------------------------------------------------

export type DrillFrame =
  | { kind: 'view'; name: string; params: Record<string, string> }
  | { kind: 'entity-list'; entity: string }
  | { kind: 'entity-detail'; entity: string; id: string }
  | { kind: 'app'; name: string; params: Record<string, string> }

/** Serialize a ``DrillFrame`` to a router path. Pair with ``navigate()``. */
export function frameToUrl(frame: DrillFrame): string {
  switch (frame.kind) {
    case 'view': {
      const qs = new URLSearchParams(frame.params).toString()
      return `/views/${encodeURIComponent(frame.name)}${qs ? '?' + qs : ''}`
    }
    case 'app': {
      const qs = new URLSearchParams(frame.params).toString()
      return `/apps/${encodeURIComponent(frame.name)}${qs ? '?' + qs : ''}`
    }
    case 'entity-list':
      return `/entities/${encodeURIComponent(frame.entity)}`
    case 'entity-detail':
      return `/entities/${encodeURIComponent(frame.entity)}/${encodeURIComponent(frame.id)}`
  }
}

export interface ViewDrilldownConfig {
  label?: string
  app?: string
  view?: string
  entity?: string
  id_column?: string
  params?: Record<string, string>
}

/** Resolve ``row.<col>`` template references against a row's cells. */
export function resolveRowParams(
  tpl: Record<string, string>,
  row: (string | number | null)[],
  columns: string[],
  sourceParams: Record<string, string>,
): Record<string, string> {
  const resolved: Record<string, string> = { ...sourceParams }
  for (const [key, template] of Object.entries(tpl)) {
    if (typeof template === 'string' && template.startsWith('row.')) {
      const colName = template.slice(4)
      const idx = columns.indexOf(colName)
      resolved[key] = idx >= 0 && row[idx] != null ? String(row[idx]) : ''
    } else {
      resolved[key] = String(template)
    }
  }
  return resolved
}

/** Row-click drilldown → DrillFrame. Used by table-type views. */
export function resolveViewDrilldown(
  config: ViewDrilldownConfig,
  row: (string | number | null)[],
  columns: string[],
  sourceParams: Record<string, string>,
): DrillFrame | null {
  if (config.entity && config.id_column) {
    const idx = columns.indexOf(config.id_column)
    const id = idx >= 0 && row[idx] != null ? String(row[idx]) : null
    if (!id) return null
    return { kind: 'entity-detail', entity: config.entity, id }
  }
  if (config.app) {
    const params = resolveRowParams(config.params ?? {}, row, columns, sourceParams)
    return { kind: 'app', name: config.app, params }
  }
  if (config.view) {
    const params = resolveRowParams(config.params ?? {}, row, columns, sourceParams)
    return { kind: 'view', name: config.view, params }
  }
  return null
}

/** Map-marker action drilldown → DrillFrame. Used by map-type views. */
export function resolveMarkerAction(
  config: ViewDrilldownConfig,
  rowDict: Record<string, unknown>,
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolveParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const resolved: Record<string, string> = { ...sourceParams }
    for (const [key, template] of Object.entries(tpl)) {
      if (typeof template === 'string' && template.startsWith('row.')) {
        const col = template.slice(4)
        const val = rowDict[col]
        resolved[key] = val != null ? String(val) : ''
      } else {
        resolved[key] = String(template)
      }
    }
    return resolved
  }

  if (config.entity && config.id_column) {
    const val = rowDict[config.id_column]
    const id = val != null ? String(val) : null
    if (!id) return null
    return { kind: 'entity-detail', entity: config.entity, id }
  }
  if (config.app) return { kind: 'app', name: config.app, params: resolveParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolveParams(config.params) }
  return null
}

/** Chart-point click drilldown → DrillFrame. Used by line/bar/calendar charts. */
export function resolvePointDrilldown(
  config: ViewDrilldownConfig,
  pointData: Record<string, unknown>,
  sourceParams: Record<string, string>,
): DrillFrame | null {
  const resolvePointParams = (tpl: Record<string, string> = {}): Record<string, string> => {
    const resolved: Record<string, string> = { ...sourceParams }
    for (const [key, template] of Object.entries(tpl)) {
      if (typeof template === 'string' && template.startsWith('point.')) {
        const col = template.slice(6)
        const val = pointData[col]
        resolved[key] = val != null ? String(val) : ''
      } else {
        resolved[key] = String(template)
      }
    }
    return resolved
  }

  if (config.entity && config.id_column) {
    const val = pointData[config.id_column]
    const id = val != null ? String(val) : null
    if (!id) return null
    return { kind: 'entity-detail', entity: config.entity, id }
  }
  if (config.app) return { kind: 'app', name: config.app, params: resolvePointParams(config.params) }
  if (config.view) return { kind: 'view', name: config.view, params: resolvePointParams(config.params) }
  return null
}
