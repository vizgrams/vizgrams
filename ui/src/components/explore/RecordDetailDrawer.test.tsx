// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

import type { EntityRecord } from '@/api/client'
import { RecordDetailDrawer } from './RecordDetailDrawer'

const getEntityRecord = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { getEntityRecord } }),
}))

function makeRecord(overrides: Partial<EntityRecord> = {}): EntityRecord {
  return {
    entity: 'PullRequest',
    id: 'abc-123',
    properties: { state: 'open', title: 'Add the thing' },
    relationships: {
      author: { target: 'Person', cardinality: 'MANY_TO_ONE', id: 'p_42' },
      reviews: { target: 'Review', cardinality: 'ONE_TO_MANY', count: 3 },
    },
    feature_values: {
      lead_time_hours: { value: 12.5, computed_at: '2026-05-01T10:00:00Z' },
    },
    ...overrides,
  }
}

describe('RecordDetailDrawer', () => {
  it('fetches + renders attributes / relations / computed', async () => {
    getEntityRecord.mockClear().mockResolvedValue(makeRecord())
    render(<RecordDetailDrawer entity="PullRequest" id="abc-123" onClose={() => {}} />)
    expect(await screen.findByText('open')).toBeInTheDocument()
    expect(screen.getByText('Add the thing')).toBeInTheDocument()
    expect(screen.getByText('→ Person · p_42')).toBeInTheDocument()
    expect(screen.getByText('3 Review')).toBeInTheDocument()
    expect(screen.getByText('12.5')).toBeInTheDocument()
    expect(screen.getByText(/computed 2026-05-01/)).toBeInTheDocument()
    expect(getEntityRecord).toHaveBeenCalledWith('PullRequest', 'abc-123')
  })

  it('shows empty markers when sections are empty', async () => {
    getEntityRecord.mockClear().mockResolvedValue(makeRecord({
      properties: {}, relationships: {}, feature_values: {},
    }))
    render(<RecordDetailDrawer entity="PullRequest" id="abc-123" onClose={() => {}} />)
    expect(await screen.findByText('No attributes.')).toBeInTheDocument()
    expect(screen.getByText('No relations.')).toBeInTheDocument()
    expect(screen.getByText('No computed features.')).toBeInTheDocument()
  })

  it('shows error inline when fetch rejects', async () => {
    getEntityRecord.mockClear().mockRejectedValue(new Error('forbidden'))
    render(<RecordDetailDrawer entity="X" id="1" onClose={() => {}} />)
    expect(await screen.findByText(/forbidden/)).toBeInTheDocument()
  })

  it('relation open button calls onNavigateRelated', async () => {
    getEntityRecord.mockClear().mockResolvedValue(makeRecord())
    const onNavigate = vi.fn()
    render(<RecordDetailDrawer
      entity="PullRequest" id="abc-123"
      onClose={() => {}} onNavigateRelated={onNavigate}
    />)
    await screen.findByText(/→ Person/)
    // The reviews row also has an open button; click the author one first.
    const buttons = screen.getAllByRole('button', { name: /open →/i })
    fireEvent.click(buttons[0])
    expect(onNavigate).toHaveBeenCalledWith('Person')
  })

  it('close button calls onClose', async () => {
    getEntityRecord.mockClear().mockResolvedValue(makeRecord())
    const onClose = vi.fn()
    render(<RecordDetailDrawer entity="X" id="1" onClose={onClose} />)
    await screen.findByText('open')
    const xBtn = screen.getAllByRole('button').find((b) => b.querySelector('svg.lucide-x'))
    fireEvent.click(xBtn!)
    expect(onClose).toHaveBeenCalled()
  })
})
