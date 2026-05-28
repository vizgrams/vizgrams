// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import type { PlatformRole } from '@/api/client'
import { GovernedYamlEditor } from './GovernedYamlEditor'

let mockRole: PlatformRole = 'admin'
vi.mock('@/context/RoleContext', () => ({
  useRole: () => ({
    email: 'u@example.com', userId: 'u1', role: mockRole, loading: false,
  }),
}))

function setRole(r: PlatformRole) { mockRole = r }

describe('GovernedYamlEditor — admin', () => {
  it('renders without reason field and Save label', () => {
    setRole('admin')
    render(<GovernedYamlEditor
      title="mapper: pull_request"
      initialContent="kind: Mapper\nname: x"
      onDirectSave={vi.fn()}
      onProposeChange={vi.fn()}
      onClose={vi.fn()}
    />)
    expect(screen.queryByPlaceholderText(/reason/i)).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^save$/i })).toBeInTheDocument()
  })

  it('calls onDirectSave with edited content then onClose', async () => {
    setRole('admin')
    const onDirectSave = vi.fn().mockResolvedValue(undefined)
    const onClose = vi.fn()
    render(<GovernedYamlEditor
      title="mapper: pull_request" initialContent="a"
      onDirectSave={onDirectSave}
      onProposeChange={vi.fn()}
      onClose={onClose}
    />)
    fireEvent.change(screen.getByDisplayValue('a'), { target: { value: 'b' } })
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    await waitFor(() => expect(onDirectSave).toHaveBeenCalledWith('b'))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('shows inline error on save failure and keeps editor open', async () => {
    setRole('admin')
    const onDirectSave = vi.fn().mockRejectedValue(new Error('forbidden'))
    const onClose = vi.fn()
    render(<GovernedYamlEditor
      title="mapper: pull_request" initialContent="a"
      onDirectSave={onDirectSave}
      onProposeChange={vi.fn()}
      onClose={onClose}
    />)
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }))
    expect(await screen.findByText(/forbidden/i)).toBeInTheDocument()
    expect(onClose).not.toHaveBeenCalled()
  })
})

describe('GovernedYamlEditor — member', () => {
  it('renders reason field and Propose-change label', () => {
    setRole('member')
    render(<GovernedYamlEditor
      title="mapper: pull_request" initialContent="a"
      onDirectSave={vi.fn()}
      onProposeChange={vi.fn()}
      onClose={vi.fn()}
    />)
    expect(screen.getByPlaceholderText(/reason for this change/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /propose change/i })).toBeInTheDocument()
  })

  it('requires a non-empty reason before submitting', async () => {
    setRole('member')
    const onProposeChange = vi.fn()
    render(<GovernedYamlEditor
      title="mapper: pull_request" initialContent="a"
      onDirectSave={vi.fn()}
      onProposeChange={onProposeChange}
      onClose={vi.fn()}
    />)
    fireEvent.click(screen.getByRole('button', { name: /propose change/i }))
    expect(await screen.findByText(/reason is required/i)).toBeInTheDocument()
    expect(onProposeChange).not.toHaveBeenCalled()
  })

  it('submits trimmed reason + edited content then onClose', async () => {
    setRole('member')
    const onProposeChange = vi.fn().mockResolvedValue(undefined)
    const onClose = vi.fn()
    render(<GovernedYamlEditor
      title="mapper: pull_request" initialContent="a"
      onDirectSave={vi.fn()}
      onProposeChange={onProposeChange}
      onClose={onClose}
    />)
    fireEvent.change(screen.getByDisplayValue('a'), { target: { value: 'b' } })
    fireEvent.change(screen.getByPlaceholderText(/reason for this change/i),
                     { target: { value: '  drop unused col  ' } })
    fireEvent.click(screen.getByRole('button', { name: /propose change/i }))
    await waitFor(() => expect(onProposeChange).toHaveBeenCalledWith('b', 'drop unused col'))
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('shows inline error on propose failure and keeps editor open', async () => {
    setRole('member')
    const onProposeChange = vi.fn().mockRejectedValue(new Error('server is busy'))
    const onClose = vi.fn()
    render(<GovernedYamlEditor
      title="mapper: pull_request" initialContent="a"
      onDirectSave={vi.fn()}
      onProposeChange={onProposeChange}
      onClose={onClose}
    />)
    fireEvent.change(screen.getByPlaceholderText(/reason for this change/i),
                     { target: { value: 'why' } })
    fireEvent.click(screen.getByRole('button', { name: /propose change/i }))
    expect(await screen.findByText(/server is busy/i)).toBeInTheDocument()
    expect(onClose).not.toHaveBeenCalled()
  })
})

describe('GovernedYamlEditor — common', () => {
  it('Cancel calls onClose without invoking any save', () => {
    setRole('admin')
    const onClose = vi.fn()
    const onDirectSave = vi.fn()
    render(<GovernedYamlEditor
      title="x" initialContent="a"
      onDirectSave={onDirectSave}
      onProposeChange={vi.fn()}
      onClose={onClose}
    />)
    fireEvent.click(screen.getByRole('button', { name: /^cancel$/i }))
    expect(onClose).toHaveBeenCalled()
    expect(onDirectSave).not.toHaveBeenCalled()
  })

  it('picks up new initialContent when prop changes', () => {
    setRole('admin')
    const { rerender } = render(<GovernedYamlEditor
      title="x" initialContent="first"
      onDirectSave={vi.fn()}
      onProposeChange={vi.fn()}
      onClose={vi.fn()}
    />)
    expect(screen.getByDisplayValue('first')).toBeInTheDocument()
    rerender(<GovernedYamlEditor
      title="x" initialContent="second"
      onDirectSave={vi.fn()}
      onProposeChange={vi.fn()}
      onClose={vi.fn()}
    />)
    expect(screen.getByDisplayValue('second')).toBeInTheDocument()
  })
})
