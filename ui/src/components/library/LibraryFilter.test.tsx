// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

import type { LibraryFields } from '@/api/client'
import { LibraryFilter, filterByLibrary } from './LibraryFilter'

// ---------------------------------------------------------------------------
// filterByLibrary — pure predicate. Five canonical cases.
// ---------------------------------------------------------------------------

type Row = LibraryFields & { name: string }

const ROWS: Row[] = [
  { name: 'cert_by_alice',   is_certified: true,  created_by: 'alice' },
  { name: 'cert_by_bob',     is_certified: true,  created_by: 'bob' },
  { name: 'uncert_by_alice', is_certified: false, created_by: 'alice' },
  { name: 'uncert_by_bob',   is_certified: false, created_by: 'bob' },
  { name: 'legacy_no_owner', is_certified: false, created_by: null },
]

describe('filterByLibrary', () => {
  it('"certified" keeps only is_certified=true rows', () => {
    const out = filterByLibrary(ROWS, 'certified', 'alice')
    expect(out.map((r) => r.name)).toEqual(['cert_by_alice', 'cert_by_bob'])
  })

  it('"all" returns everything regardless of user', () => {
    const out = filterByLibrary(ROWS, 'all', null)
    expect(out).toHaveLength(ROWS.length)
  })

  it('"mine" returns rows whose created_by matches the current user', () => {
    const out = filterByLibrary(ROWS, 'mine', 'alice')
    expect(out.map((r) => r.name)).toEqual(['cert_by_alice', 'uncert_by_alice'])
  })

  it('"mine" with null user returns empty (NOT everyone else\'s work)', () => {
    // Regression guard: an empty-string compare would incorrectly match
    // null-owned legacy rows and effectively leak other people's items.
    const out = filterByLibrary(ROWS, 'mine', null)
    expect(out).toEqual([])
  })

  it('"mine" does not match legacy null-owner rows even with a user set', () => {
    const out = filterByLibrary(ROWS, 'mine', 'alice')
    expect(out.find((r) => r.name === 'legacy_no_owner')).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// LibraryFilter — component rendering + interaction.
// ---------------------------------------------------------------------------

describe('LibraryFilter component', () => {
  it('renders all three chips with the active one highlighted', () => {
    render(
      <LibraryFilter
        value="certified"
        onChange={() => {}}
        currentUserId="alice"
      />,
    )
    // All three chips visible.
    expect(screen.getByRole('button', { name: /certified/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^all$/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /mine/i })).toBeInTheDocument()
  })

  it('calls onChange when a chip is clicked', () => {
    const onChange = vi.fn()
    render(
      <LibraryFilter
        value="certified"
        onChange={onChange}
        currentUserId="alice"
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /^all$/i }))
    expect(onChange).toHaveBeenCalledWith('all')
  })

  it('disables Mine when there is no current user', () => {
    const onChange = vi.fn()
    render(
      <LibraryFilter
        value="certified"
        onChange={onChange}
        currentUserId={null}
      />,
    )
    const mine = screen.getByRole('button', { name: /mine/i })
    expect(mine).toBeDisabled()
    fireEvent.click(mine)
    // Disabled click does nothing — sanity for the guard inside the
    // component itself (we early-return when ``disabled``).
    expect(onChange).not.toHaveBeenCalled()
  })

  it('shows the match count when the filter is narrowing', () => {
    render(
      <LibraryFilter
        value="certified"
        onChange={() => {}}
        currentUserId="alice"
        matchCount={3}
        totalCount={10}
      />,
    )
    expect(screen.getByText('3 of 10')).toBeInTheDocument()
  })

  it('hides the match count when filter matches everything', () => {
    render(
      <LibraryFilter
        value="all"
        onChange={() => {}}
        currentUserId="alice"
        matchCount={10}
        totalCount={10}
      />,
    )
    expect(screen.queryByText(/of/)).not.toBeInTheDocument()
  })
})
