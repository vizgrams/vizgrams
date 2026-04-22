// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useCallback, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import cytoscape from 'cytoscape'
// @ts-expect-error no types for cytoscape-fcose
import fcose from 'cytoscape-fcose'
import type { GraphOut } from '@/api/client'
import { useApi } from '@/hooks/useApi'
import { useModel } from '@/context/ModelContext'
import { Spinner, ErrorMessage } from '@/components/Layout'

cytoscape.use(fcose)

// ---------------------------------------------------------------------------
// Edge styles by cardinality
// ---------------------------------------------------------------------------

const CARD_STYLE: Record<string, { colour: string; width: number; dash: boolean }> = {
  ONE_TO_ONE:   { colour: '#a78bfa', width: 1.2, dash: true  },
  ONE_TO_MANY:  { colour: '#38bdf8', width: 1.5, dash: false },
  MANY_TO_ONE:  { colour: '#38bdf8', width: 1.5, dash: false },
  MANY_TO_MANY: { colour: '#fb923c', width: 2.5, dash: false },
}

const LEGEND = [
  { label: 'ONE : ONE',   colour: '#a78bfa', dash: true,  width: 1.2 },
  { label: 'ONE : MANY',  colour: '#38bdf8', dash: false, width: 1.5 },
  { label: 'MANY : MANY', colour: '#fb923c', dash: false, width: 2.5 },
]

const NODE_COLOUR    = '#1e293b'
const NODE_BORDER    = '#475569'
const NODE_SIZE      = 8
const NODE_HIGHLIGHT = '#7c6af5'

const CY_STYLE: cytoscape.StylesheetCSS[] = [
  {
    selector: 'node',
    css: {
      'background-color': NODE_COLOUR,
      'width': NODE_SIZE,
      'height': NODE_SIZE,
      'label': 'data(label)',
      'color': '#64748b',
      'font-size': 9,
      'font-family': 'Inter, system-ui, sans-serif',
      'font-weight': 400,
      'text-valign': 'bottom',
      'text-halign': 'center',
      'text-margin-y': 4,
      'text-outline-color': '#020617',
      'text-outline-width': 2,
      'border-width': 1.5,
      'border-color': NODE_BORDER,
    },
  },
  {
    selector: 'node.highlighted',
    css: {
      'background-color': NODE_HIGHLIGHT,
      'width': NODE_SIZE * 2,
      'height': NODE_SIZE * 2,
      'color': '#e2e8f0',
      'font-size': 11,
      'font-weight': 500,
      'border-width': 0,
      'z-index': 999,
    },
  },
  {
    selector: 'node.dimmed',
    css: { 'opacity': 0.1 },
  },
  {
    selector: 'edge',
    css: {
      'width': 'data(width)',
      'line-color': 'data(colour)',
      'line-style': 'data(dash)' as never,
      'line-opacity': 0.4,
      'target-arrow-color': 'data(colour)',
      'target-arrow-shape': 'triangle',
      'arrow-scale': 0.7,
      'curve-style': 'bezier',
      'label': '',
    },
  },
  {
    selector: 'edge.highlighted',
    css: {
      'line-opacity': 1,
      'label': 'data(label)',
      'font-size': 10,
      'color': 'data(colour)',
      'font-family': 'Inter, system-ui, sans-serif',
      'text-outline-color': '#020617',
      'text-outline-width': 3,
      'z-index': 999,
    },
  },
  {
    selector: 'edge.dimmed',
    css: { 'line-opacity': 0.03, 'target-arrow-shape': 'none' },
  },
]

// ---------------------------------------------------------------------------
// Build cytoscape elements from graph data
// ---------------------------------------------------------------------------

function buildElements(graph: GraphOut): cytoscape.ElementDefinition[] {
  const els: cytoscape.ElementDefinition[] = []

  for (const node of graph.nodes) {
    els.push({ data: { id: node.id, label: node.label } })
  }

  for (const edge of graph.edges) {
    const s = CARD_STYLE[edge.cardinality] ?? { colour: '#64748b', width: 1.2, dash: false }
    els.push({
      data: {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        label: edge.label,
        colour: s.colour,
        width: s.width,
        dash: s.dash ? 'dashed' : 'solid',
      },
    })
  }

  return els
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function GraphPage() {
  const { api, model } = useModel()
  const navigate = useNavigate()
  const containerRef = useRef<HTMLDivElement>(null)
  const cyRef        = useRef<cytoscape.Core | null>(null)

  const state = useApi(() => api.getGraph(), [model])

  const exportOwl = useCallback(async () => {
    const turtle = await api.getGraphOwl()
    const blob = new Blob([turtle], { type: 'text/turtle' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${model}.owl.ttl`
    a.click()
    URL.revokeObjectURL(url)
  }, [api, model])

  useEffect(() => {
    if (state.status !== 'ok' || !containerRef.current) return

    const graph    = state.data
    const n        = graph.nodes.length
    const elements = buildElements(graph)

    cyRef.current?.destroy()
    cyRef.current = null

    const tid = setTimeout(() => {
      if (!containerRef.current) return

      const cy = cytoscape({
        container: containerRef.current,
        elements,
        style: CY_STYLE,
        layout: {
          name: 'fcose',
          animate: true,
          animationDuration: 800,
          animationEasing: 'ease-out',
          fit: true,
          padding: 60,
          nodeRepulsion: Math.max(12000, n * 1400),
          idealEdgeLength: Math.max(100, n * 8),
          edgeElasticity: 0.35,
          gravity: 0.25,
          gravityRange: 1.5,
          numIter: Math.max(2500, n * 120),
          nodeSeparation: Math.max(80, n * 4),
          randomize: true,
          tilingPaddingVertical: 10,
          tilingPaddingHorizontal: 10,
        } as never,
        minZoom: 0.05,
        maxZoom: 5,
        wheelSensitivity: 0.2,
      })

      cyRef.current = cy

      cy.resize()
      cy.on('layoutstop', () => { cy.resize(); cy.fit(undefined, 60) })

      cy.on('mouseover', 'node', (e) => {
        const nb = e.target.closedNeighborhood()
        cy.elements().not(nb).addClass('dimmed').removeClass('highlighted')
        nb.addClass('highlighted').removeClass('dimmed')
      })
      cy.on('mouseout',  'node', () => cy.elements().removeClass('dimmed highlighted'))
      cy.on('tap',       'node', (e) => navigate(`/explore/${e.target.id()}`))
      cy.on('mouseover', 'node', () => { if (containerRef.current) containerRef.current.style.cursor = 'pointer' })
      cy.on('mouseout',  'node', () => { if (containerRef.current) containerRef.current.style.cursor = 'default' })
    }, 50)

    return () => { clearTimeout(tid); cyRef.current?.destroy(); cyRef.current = null }
  }, [state, navigate])

  const nodeCount = state.status === 'ok' ? state.data.nodes.length : 0
  const edgeCount = state.status === 'ok' ? state.data.edges.length : 0

  return (
    <div style={{ width: '100%', height: '100%', background: '#020617', position: 'relative' }}>
      <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative' }} />

      {state.status !== 'ok' && (
        <div style={{ position: 'absolute', top: 60, left: 0, right: 0, display: 'flex', justifyContent: 'center', zIndex: 20 }}>
          {state.status === 'loading' && <Spinner />}
          {state.status === 'error'   && <ErrorMessage message={state.error} />}
        </div>
      )}

      {/* Overlay header */}
      <div style={{ position: 'absolute', top: 0, left: 0, right: 0, zIndex: 10, display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '12px 24px', boxSizing: 'border-box' }}>
        <div style={{ pointerEvents: 'none' }}>
          <h1 style={{ fontSize: 15, fontWeight: 600, color: '#fff', margin: 0 }}>Ontology Graph</h1>
          <p style={{ fontSize: 11, color: '#64748b', margin: 0 }}>{nodeCount} entities · {edgeCount} relationships</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <button
            onClick={exportOwl}
            style={{ background: 'rgba(15,23,42,0.85)', backdropFilter: 'blur(8px)', border: '1px solid rgba(100,116,139,0.25)', borderRadius: 6, padding: '5px 12px', fontSize: 11, color: '#94a3b8', cursor: 'pointer' }}
          >
            Export OWL
          </button>
          <div style={{ display: 'flex', alignItems: 'center', gap: 20, background: 'rgba(15,23,42,0.85)', backdropFilter: 'blur(8px)', border: '1px solid rgba(100,116,139,0.25)', borderRadius: 8, padding: '6px 16px', pointerEvents: 'none' }}>
            {LEGEND.map(({ label, colour, dash, width }) => (
              <span key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, fontSize: 11, color: '#94a3b8' }}>
                <svg width="24" height="10" style={{ flexShrink: 0 }}>
                  <line
                    x1="0" y1="5" x2="24" y2="5"
                    stroke={colour}
                    strokeWidth={width}
                    strokeDasharray={dash ? '4 3' : undefined}
                  />
                </svg>
                {label}
              </span>
            ))}
          </div>
        </div>
      </div>

      <div style={{ position: 'absolute', bottom: 12, left: 0, right: 0, display: 'flex', justifyContent: 'center', pointerEvents: 'none', zIndex: 10 }}>
        <span style={{ fontSize: 11, color: '#334155', background: 'rgba(15,23,42,0.6)', padding: '4px 12px', borderRadius: 999 }}>
          Click a node to explore · Scroll to zoom · Drag to pan
        </span>
      </div>
    </div>
  )
}
