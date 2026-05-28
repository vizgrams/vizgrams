// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import type { Proposal } from '@/api/client'
import { ProposalCard } from './ProposalCard'

const approveProposal = vi.fn()
const rejectProposal = vi.fn()
vi.mock('@/api/client', async (orig) => {
  const original: object = await orig()
  return {
    ...original,
    approveProposal: (...args: unknown[]) => approveProposal(...args),
    rejectProposal: (...args: unknown[]) => rejectProposal(...args),
  }
})

function makeProposal(overrides: Partial<Proposal> = {}): Proposal {
  return {
    id: 'p_1',
    model_id: 'demo',
    entity_name: 'PullRequest',
    artifact_kind: 'attribute',
    artifact_name: 'state',
    proposed_by: 'alice',
    reason: 'add a draft state',
    before_yaml: 'enum<open,closed>',
    after_yaml: 'enum<draft,open,closed>',
    status: 'pending',
    notified_to: ['bob (owner)', 'admin@example.com'],
    decision_actor: null,
    decision_at: null,
    decision_comment: null,
    superseded_by: null,
    created_at: '2026-05-28T10:00:00Z',
    ...overrides,
  }
}

describe('ProposalCard', () => {
  it('renders the diff + reason + reviewers', () => {
    render(<ProposalCard proposal={makeProposal()} />)
    expect(screen.getByText('enum<open,closed>')).toBeInTheDocument()
    expect(screen.getByText('enum<draft,open,closed>')).toBeInTheDocument()
    expect(screen.getByText(/"add a draft state"/)).toBeInTheDocument()
    expect(screen.getByText('bob (owner)')).toBeInTheDocument()
    expect(screen.getByText('admin@example.com')).toBeInTheDocument()
  })

  it('approve button calls API and triggers onDecided', async () => {
    approveProposal.mockClear().mockResolvedValue(makeProposal({ status: 'approved' }))
    const onDecided = vi.fn()
    render(<ProposalCard proposal={makeProposal()} onDecided={onDecided} />)
    fireEvent.click(screen.getByRole('button', { name: /✓ approve/i }))
    await waitFor(() => expect(approveProposal).toHaveBeenCalledWith('p_1'))
    await waitFor(() => expect(onDecided).toHaveBeenCalled())
  })

  it('reject button reveals a comment field on first click', () => {
    render(<ProposalCard proposal={makeProposal()} />)
    fireEvent.click(screen.getByRole('button', { name: /reject/i }))
    expect(screen.getByPlaceholderText(/reason for rejecting/i)).toBeInTheDocument()
  })

  it('reject submission requires a non-empty comment', async () => {
    rejectProposal.mockClear()
    render(<ProposalCard proposal={makeProposal()} />)
    // First click reveals field
    fireEvent.click(screen.getByRole('button', { name: /reject/i }))
    // Second click without a comment → inline error
    fireEvent.click(screen.getByRole('button', { name: /submit rejection/i }))
    expect(await screen.findByText(/reason is required/i)).toBeInTheDocument()
    expect(rejectProposal).not.toHaveBeenCalled()
  })

  it('reject submits with the comment + triggers onDecided', async () => {
    rejectProposal.mockClear().mockResolvedValue(makeProposal({ status: 'rejected' }))
    const onDecided = vi.fn()
    render(<ProposalCard proposal={makeProposal()} onDecided={onDecided} />)
    fireEvent.click(screen.getByRole('button', { name: /reject/i }))
    fireEvent.change(screen.getByPlaceholderText(/reason for rejecting/i),
                     { target: { value: 'too risky' } })
    fireEvent.click(screen.getByRole('button', { name: /submit rejection/i }))
    await waitFor(() => expect(rejectProposal).toHaveBeenCalledWith('p_1', 'too risky'))
    await waitFor(() => expect(onDecided).toHaveBeenCalled())
  })

  it('shows API error inline without triggering onDecided', async () => {
    approveProposal.mockClear().mockRejectedValue(new Error('forbidden'))
    const onDecided = vi.fn()
    render(<ProposalCard proposal={makeProposal()} onDecided={onDecided} />)
    fireEvent.click(screen.getByRole('button', { name: /✓ approve/i }))
    expect(await screen.findByText(/forbidden/i)).toBeInTheDocument()
    expect(onDecided).not.toHaveBeenCalled()
  })

  it('handles empty notified_to gracefully', () => {
    render(<ProposalCard proposal={makeProposal({ notified_to: [] })} />)
    expect(screen.getByText(/none/i)).toBeInTheDocument()
  })
})
