// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

import { ShareButton } from './VizgramCard'

// ---------------------------------------------------------------------------
// ShareButton — clipboard happy path + fallback.
//
// Why test this: the live regression was a 404 share link when query_ref
// wasn't a view name; that's fixed at the data layer (view_ref column)
// but the button's URL construction is what users actually see. Lock it
// in so it can't silently regress.
// ---------------------------------------------------------------------------

describe('ShareButton', () => {
  const ORIGIN = 'https://example.com'

  beforeEach(() => {
    // jsdom doesn't ship with a deterministic origin; stub it so we
    // can assert on the exact URL written to the clipboard.
    Object.defineProperty(window, 'location', {
      writable: true,
      value: { ...window.location, origin: ORIGIN },
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('writes /views/<name> to the clipboard on click and shows a tick', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })

    render(<ShareButton viewName="dora_clt_by_team" />)

    const button = screen.getByRole('button', { name: /copy link/i })
    fireEvent.click(button)

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(`${ORIGIN}/views/dora_clt_by_team`)
    })
  })

  it('URL-encodes view names that contain unsafe characters', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.assign(navigator, { clipboard: { writeText } })

    render(<ShareButton viewName="foo bar" />)
    fireEvent.click(screen.getByRole('button', { name: /copy link/i }))

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith(`${ORIGIN}/views/foo%20bar`)
    })
  })

  it('falls back to window.prompt when the clipboard API is unavailable', async () => {
    // Non-secure context: navigator.clipboard is undefined. The button
    // should still offer the URL via prompt() so the user can copy
    // manually rather than the click silently doing nothing.
    Object.assign(navigator, { clipboard: undefined })
    const promptSpy = vi.spyOn(window, 'prompt').mockReturnValue(null)

    render(<ShareButton viewName="dora_clt_by_team" />)
    fireEvent.click(screen.getByRole('button', { name: /copy link/i }))

    await waitFor(() => {
      expect(promptSpy).toHaveBeenCalledWith(
        'Copy this link:',
        `${ORIGIN}/views/dora_clt_by_team`,
      )
    })
  })

  it('falls back to window.prompt when clipboard.writeText rejects', async () => {
    // Secure context but write fails (permissions, focus etc.). Same
    // graceful degradation — user always gets the URL.
    const writeText = vi.fn().mockRejectedValue(new Error('denied'))
    Object.assign(navigator, { clipboard: { writeText } })
    const promptSpy = vi.spyOn(window, 'prompt').mockReturnValue(null)

    render(<ShareButton viewName="dora_clt_by_team" />)
    fireEvent.click(screen.getByRole('button', { name: /copy link/i }))

    await waitFor(() => {
      expect(promptSpy).toHaveBeenCalledWith(
        'Copy this link:',
        `${ORIGIN}/views/dora_clt_by_team`,
      )
    })
  })
})
