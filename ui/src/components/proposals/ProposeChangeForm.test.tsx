// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import { ProposeChangeForm } from './ProposeChangeForm'

const createProposal = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { createProposal } }),
}))

describe('ProposeChangeForm', () => {
  it('shows current value read-only and proposed value pre-filled', () => {
    render(<ProposeChangeForm
      artifactKind="attribute" artifactName="state"
      current="enum<open,closed>" onClose={() => {}}
    />)
    expect(screen.getByText('enum<open,closed>')).toBeInTheDocument()
    const proposedInput = screen.getByDisplayValue('enum<open,closed>')
    expect(proposedInput).toBeInTheDocument()
  })

  it('requires a reason before submitting', async () => {
    createProposal.mockClear()
    render(<ProposeChangeForm
      artifactKind="attribute" artifactName="state"
      current="x" onClose={() => {}}
    />)
    fireEvent.click(screen.getByRole('button', { name: /propose change/i }))
    expect(await screen.findByText(/reason is required/i)).toBeInTheDocument()
    expect(createProposal).not.toHaveBeenCalled()
  })

  it('submits with the right payload + calls onSubmitted then onClose', async () => {
    createProposal.mockClear().mockResolvedValue({ id: 'p_1' })
    const onClose = vi.fn()
    const onSubmitted = vi.fn()
    render(<ProposeChangeForm
      artifactKind="attribute" artifactName="state" entityName="PullRequest"
      current="old" onClose={onClose} onSubmitted={onSubmitted}
    />)

    fireEvent.change(screen.getByDisplayValue('old'), { target: { value: 'new' } })
    fireEvent.change(screen.getByPlaceholderText(/add a draft state/i),
                     { target: { value: 'add draft' } })
    fireEvent.click(screen.getByRole('button', { name: /propose change/i }))

    await waitFor(() => expect(createProposal).toHaveBeenCalledWith({
      artifact_kind: 'attribute',
      artifact_name: 'state',
      entity_name: 'PullRequest',
      reason: 'add draft',
      before_yaml: 'old',
      after_yaml: 'new',
    }))
    await waitFor(() => expect(onSubmitted).toHaveBeenCalled())
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('shows server error and keeps form open on failure', async () => {
    createProposal.mockClear().mockRejectedValue(new Error('forbidden'))
    const onClose = vi.fn()
    render(<ProposeChangeForm
      artifactKind="attribute" artifactName="state"
      current="x" onClose={onClose}
    />)
    fireEvent.change(screen.getByPlaceholderText(/add a draft state/i),
                     { target: { value: 'because' } })
    fireEvent.click(screen.getByRole('button', { name: /propose change/i }))
    expect(await screen.findByText(/forbidden/i)).toBeInTheDocument()
    expect(onClose).not.toHaveBeenCalled()
  })

  it('shows "owner + admins" reviewer hint', () => {
    render(<ProposeChangeForm
      artifactKind="attribute" artifactName="state"
      current="x" onClose={() => {}}
    />)
    const block = screen.getByText(/reviewers:/i)
    expect(block).toHaveTextContent(/owner/i)
    expect(block).toHaveTextContent(/admins/i)
  })

  it('Cancel button closes without submitting', () => {
    createProposal.mockClear()
    const onClose = vi.fn()
    render(<ProposeChangeForm
      artifactKind="attribute" artifactName="state"
      current="x" onClose={onClose}
    />)
    fireEvent.click(screen.getByRole('button', { name: /^cancel$/i }))
    expect(onClose).toHaveBeenCalled()
    expect(createProposal).not.toHaveBeenCalled()
  })
})
