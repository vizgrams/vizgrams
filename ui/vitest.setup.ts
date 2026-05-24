// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

/**
 * Vitest global setup — runs once before any test file.
 *
 * Pulls in @testing-library/jest-dom's custom matchers (toBeInTheDocument,
 * toHaveTextContent, etc.) and registers an afterEach to clean up the DOM
 * between tests so jsdom state doesn't leak across cases.
 */

import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

afterEach(() => {
  cleanup()
})
