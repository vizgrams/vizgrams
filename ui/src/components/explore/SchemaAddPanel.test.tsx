// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import { SchemaAddPanel } from './SchemaAddPanel'

const createProposal = vi.fn()
vi.mock('@/context/ModelContext', () => ({
  useModel: () => ({ model: 'demo', api: { createProposal } }),
}))

describe('SchemaAddPanel', () => {
  it('starts collapsed; clicking the button reveals the form', () => {
    render(<SchemaAddPanel entity="Widget" kind="attribute" />)
    expect(screen.getByRole('button', { name: /Add attribute/i })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /Add attribute/i }))
    expect(screen.getByText(/Propose new attribute/i)).toBeInTheDocument()
  })

  it('rejects invalid names inline before calling createProposal', async () => {
    createProposal.mockClear()
    render(<SchemaAddPanel entity="Widget" kind="attribute" />)
    fireEvent.click(screen.getByRole('button', { name: /Add attribute/i }))
    fireEvent.change(screen.getByPlaceholderText(/name \(snake_case\)/i),
                     { target: { value: 'BadName' } })
    fireEvent.change(screen.getByPlaceholderText(/STRING/),
                     { target: { value: 'STRING' } })
    fireEvent.change(screen.getByPlaceholderText(/reason/i),
                     { target: { value: 'because' } })
    fireEvent.click(screen.getByRole('button', { name: /^propose$/i }))
    expect(await screen.findByText(/lowercase letters/i)).toBeInTheDocument()
    expect(createProposal).not.toHaveBeenCalled()
  })

  it('requires definition + reason', async () => {
    createProposal.mockClear()
    render(<SchemaAddPanel entity="Widget" kind="attribute" />)
    fireEvent.click(screen.getByRole('button', { name: /Add attribute/i }))
    fireEvent.change(screen.getByPlaceholderText(/name \(snake_case\)/i),
                     { target: { value: 'state' } })
    fireEvent.click(screen.getByRole('button', { name: /^propose$/i }))
    expect(await screen.findByText(/Definition is required/i)).toBeInTheDocument()
    expect(createProposal).not.toHaveBeenCalled()
  })

  it('attribute: submits a proposal with after_yaml = "name: definition"', async () => {
    createProposal.mockClear().mockResolvedValue({})
    const onProposed = vi.fn()
    render(<SchemaAddPanel entity="Widget" kind="attribute" onProposed={onProposed} />)
    fireEvent.click(screen.getByRole('button', { name: /Add attribute/i }))
    fireEvent.change(screen.getByPlaceholderText(/name \(snake_case\)/i),
                     { target: { value: 'state' } })
    fireEvent.change(screen.getByPlaceholderText(/STRING/),
                     { target: { value: 'enum<open,closed>' } })
    fireEvent.change(screen.getByPlaceholderText(/reason/i),
                     { target: { value: 'track lifecycle' } })
    fireEvent.click(screen.getByRole('button', { name: /^propose$/i }))
    await waitFor(() => expect(createProposal).toHaveBeenCalledWith({
      artifact_kind: 'attribute',
      artifact_name: 'state',
      entity_name: 'Widget',
      reason: 'track lifecycle',
      before_yaml: '',
      after_yaml: 'state: enum<open,closed>',
    }))
    await waitFor(() => expect(onProposed).toHaveBeenCalled())
  })

  it('relation: uses kind=relation in the proposal payload', async () => {
    createProposal.mockClear().mockResolvedValue({})
    render(<SchemaAddPanel entity="Widget" kind="relation" />)
    fireEvent.click(screen.getByRole('button', { name: /Add relation/i }))
    fireEvent.change(screen.getByPlaceholderText(/name \(snake_case\)/i),
                     { target: { value: 'owner' } })
    fireEvent.change(screen.getByPlaceholderText(/MANY_TO_ONE/),
                     { target: { value: 'MANY_TO_ONE → Person' } })
    fireEvent.change(screen.getByPlaceholderText(/reason/i),
                     { target: { value: 'link to person' } })
    fireEvent.click(screen.getByRole('button', { name: /^propose$/i }))
    await waitFor(() => expect(createProposal).toHaveBeenCalledWith(
      expect.objectContaining({ artifact_kind: 'relation', artifact_name: 'owner' }),
    ))
  })

  it('shows server error inline and keeps form open', async () => {
    createProposal.mockClear().mockRejectedValue(new Error('forbidden'))
    render(<SchemaAddPanel entity="Widget" kind="attribute" />)
    fireEvent.click(screen.getByRole('button', { name: /Add attribute/i }))
    fireEvent.change(screen.getByPlaceholderText(/name \(snake_case\)/i),
                     { target: { value: 'state' } })
    fireEvent.change(screen.getByPlaceholderText(/STRING/),
                     { target: { value: 'STRING' } })
    fireEvent.change(screen.getByPlaceholderText(/reason/i),
                     { target: { value: 'why' } })
    fireEvent.click(screen.getByRole('button', { name: /^propose$/i }))
    expect(await screen.findByText(/forbidden/i)).toBeInTheDocument()
    // Form still open
    expect(screen.getByText(/Propose new attribute/i)).toBeInTheDocument()
  })
})
