// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useRef, useState, useCallback } from 'react'
import { Bookmark } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import type { VizgramSummary } from '@/api/client'
import { listFeed } from '@/api/client'
import { VizgramCard } from '@/components/VizgramCard'
import { Spinner, ErrorMessage } from '@/components/Layout'

const PAGE_SIZE = 20

export function SavedPage() {
  const [items, setItems] = useState<VizgramSummary[]>([])
  const [offset, setOffset] = useState(0)
  const [hasMore, setHasMore] = useState(true)
  const [loading, setLoading] = useState(false)
  const [initialLoading, setInitialLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const sentinelRef = useRef<HTMLDivElement>(null)
  const loadingRef = useRef(false)
  const navigate = useNavigate()

  const fetchPage = useCallback(async (pageOffset: number) => {
    if (loadingRef.current) return
    loadingRef.current = true
    setLoading(true)
    try {
      const page = await listFeed({ limit: PAGE_SIZE, offset: pageOffset, saved_only: true })
      setItems((prev) => pageOffset === 0 ? page : [...prev, ...page])
      setOffset(pageOffset + page.length)
      setHasMore(page.length === PAGE_SIZE)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
      loadingRef.current = false
      if (pageOffset === 0) setInitialLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchPage(0)
  }, [fetchPage])

  useEffect(() => {
    const sentinel = sentinelRef.current
    if (!sentinel) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting && hasMore && !loadingRef.current) {
          fetchPage(offset)
        }
      },
      { rootMargin: '200px' },
    )
    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [hasMore, offset, fetchPage])

  if (initialLoading) return <div className="py-16 flex justify-center"><Spinner /></div>
  if (error) return <ErrorMessage message={error} />

  if (items.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-24 text-center gap-4">
        <Bookmark className="h-10 w-10 text-muted-foreground/30" />
        <h1 className="text-xl font-semibold text-foreground">No saved vizgrams</h1>
        <p className="text-sm text-muted-foreground max-w-sm">
          Tap the bookmark icon on any card in the{' '}
          <button
            className="underline underline-offset-2 hover:text-foreground transition-colors"
            onClick={() => navigate('/feed')}
          >
            feed
          </button>{' '}
          to save it here.
        </p>
      </div>
    )
  }

  return (
    <div className="max-w-2xl mx-auto py-6 px-4 space-y-4">
      {items.map((v) => (
        <VizgramCard key={v.id} vizgram={v} />
      ))}

      <div ref={sentinelRef} />

      {loading && (
        <div className="flex justify-center py-4">
          <Spinner />
        </div>
      )}

      {!hasMore && items.length > 0 && (
        <p className="text-center text-xs text-muted-foreground py-4">
          That's everything you've saved
        </p>
      )}
    </div>
  )
}
