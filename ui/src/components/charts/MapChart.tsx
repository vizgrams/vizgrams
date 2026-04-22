// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import 'leaflet/dist/leaflet.css'
import L from 'leaflet'
import { useEffect, useRef, useState } from 'react'
import { cn } from '@/lib/utils'

// Fix default marker icon paths broken by bundlers
delete (L.Icon.Default.prototype as unknown as Record<string, unknown>)._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
})

export interface MapMarkerAction {
  label: string
}

interface ContextMenu {
  x: number
  y: number
  rowDict: Record<string, unknown>
}

interface MapChartProps {
  rows: unknown[][]
  columns: string[]
  latKey: string
  lonKey: string
  labelKey?: string
  tooltipKeys?: string[]
  sizeKey?: string
  zoom?: number       // fixed zoom level; if omitted, fitBounds is used
  centerLat?: number  // explicit centre latitude (works with or without zoom)
  centerLon?: number  // explicit centre longitude (works with or without zoom)
  markerActions?: MapMarkerAction[]
  onMarkerAction?: (index: number, rowDict: Record<string, unknown>) => void
  height?: number
}

export function MapChart({
  rows,
  columns,
  latKey,
  lonKey,
  labelKey,
  tooltipKeys,
  sizeKey,
  zoom,
  centerLat,
  centerLon,
  markerActions,
  onMarkerAction,
  height = 400,
}: MapChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<L.Map | null>(null)
  const [menu, setMenu] = useState<ContextMenu | null>(null)

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    const map = L.map(containerRef.current, { zoomControl: true }).setView([20, 0], 2)
    mapRef.current = map

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 18,
    }).addTo(map)

    map.on('click', () => setMenu(null))
    map.on('contextmenu', (e) => L.DomEvent.preventDefault(e as unknown as Event))

    const latIdx = columns.indexOf(latKey)
    const lonIdx = columns.indexOf(lonKey)
    const labelIdx = labelKey ? columns.indexOf(labelKey) : -1
    const sizeIdx = sizeKey ? columns.indexOf(sizeKey) : -1
    const tooltipIndices = (tooltipKeys ?? []).map((k) => ({ key: k, idx: columns.indexOf(k) }))

    if (latIdx < 0 || lonIdx < 0) return

    const points: L.LatLng[] = []

    let minSize = Infinity, maxSize = -Infinity
    if (sizeIdx >= 0) {
      for (const row of rows) {
        const v = Number(row[sizeIdx])
        if (isFinite(v)) { minSize = Math.min(minSize, v); maxSize = Math.max(maxSize, v) }
      }
    }

    for (const row of rows) {
      const lat = Number(row[latIdx])
      const lon = Number(row[lonIdx])
      if (!isFinite(lat) || !isFinite(lon)) continue

      let radius = 5
      if (sizeIdx >= 0 && maxSize > minSize) {
        const v = Number(row[sizeIdx])
        radius = 4 + ((v - minSize) / (maxSize - minSize)) * 10
      }

      const marker = L.circleMarker([lat, lon], {
        radius,
        fillColor: '#3b82f6',
        fillOpacity: 0.7,
        color: '#1d4ed8',
        weight: 1,
      })

      const label = labelIdx >= 0 ? String(row[labelIdx] ?? '') : ''
      const tooltipLines = tooltipIndices
        .filter(({ idx }) => idx >= 0 && row[idx] != null)
        .map(({ key, idx }) => `<span style="color:#6b7280">${key}:</span> ${row[idx]}`)

      if (label || tooltipLines.length > 0) {
        const hasActions = markerActions && markerActions.length > 0
        const popupHtml = [
          label ? `<strong>${label}</strong>` : '',
          ...tooltipLines,
          hasActions ? `<em style="color:#9ca3af;font-size:11px">Right-click for options</em>` : '',
        ].filter(Boolean).join('<br/>')
        marker.bindPopup(popupHtml)
      }

      if (markerActions && markerActions.length > 0) {
        // Build row dict for use in action callbacks
        const rowDict: Record<string, unknown> = {}
        columns.forEach((col, i) => { rowDict[col] = row[i] })

        marker.on('contextmenu', (e: L.LeafletMouseEvent) => {
          L.DomEvent.stopPropagation(e)
          const rect = containerRef.current!.getBoundingClientRect()
          setMenu({
            x: e.originalEvent.clientX - rect.left,
            y: e.originalEvent.clientY - rect.top,
            rowDict,
          })
        })
      }

      marker.addTo(map)
      points.push(L.latLng(lat, lon))
    }

    // Determine initial view
    const hasCenter = centerLat !== undefined && centerLon !== undefined
    if (hasCenter && zoom !== undefined) {
      map.setView([centerLat!, centerLon!], zoom)
    } else if (hasCenter) {
      // Center set without zoom — use a sensible default rather than getBoundsZoom
      // (getBoundsZoom on worldwide data returns ~1–2 which looks like the equator)
      map.setView([centerLat!, centerLon!], 4)
    } else if (zoom !== undefined && points.length > 0) {
      map.setView(L.latLngBounds(points).getCenter(), zoom)
    } else if (points.length > 0) {
      map.fitBounds(L.latLngBounds(points), { padding: [20, 20], maxZoom: 6 })
    }

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="relative w-full" style={{ height }}>
      <div ref={containerRef} className="w-full h-full rounded-md overflow-hidden" />
      {menu && (
        <div
          className={cn(
            'absolute z-[1000] min-w-[160px] rounded-md border shadow-md py-1',
            'text-sm bg-white text-gray-800 dark:bg-gray-900 dark:text-gray-100'
          )}
          style={{ left: menu.x, top: menu.y }}
          onMouseDown={(e) => e.stopPropagation()}
        >
          {(markerActions ?? []).map((action, i) => (
            <button
              key={i}
              className="w-full text-left px-3 py-1.5 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
              onClick={() => {
                onMarkerAction?.(i, menu.rowDict)
                setMenu(null)
              }}
            >
              {action.label}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
