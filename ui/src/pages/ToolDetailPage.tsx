// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useState } from 'react'
import { useParams } from 'react-router-dom'
import { Play, PlayCircle } from 'lucide-react'
import { useApi } from '@/hooks/useApi'
import { useJobPoller } from '@/hooks/useJobPoller'
import { useModel } from '@/context/ModelContext'
import { Badge, Card, ErrorMessage, Spinner } from '@/components/Layout'
import { JobStatusPanel } from '@/components/JobStatusPanel'
import { cn } from '@/lib/utils'

export function ToolDetailPage() {
  const { tool = '' } = useParams<{ tool: string }>()
  const { api, model } = useModel()
  const { state: jobState, start: startJob, reset: resetJob } = useJobPoller()
  const [fullRefresh, setFullRefresh] = useState(false)

  const extractorState = useApi(() => api.getExtractor(tool), [model, tool])

  if (extractorState.status === 'loading') return <Spinner />
  if (extractorState.status === 'error') return <ErrorMessage message={extractorState.error} />

  const extractor = extractorState.data

  async function runTask(taskName: string) {
    resetJob()
    const job = await api.runExtractor(tool, taskName, fullRefresh)
    startJob(job.job_id, `Extract — ${taskName}`)
  }

  async function runAll() {
    resetJob()
    const job = await api.runExtractor(tool, undefined, fullRefresh)
    startJob(job.job_id, `Extract — all tasks`)
  }

  const running = jobState.phase === 'running'

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-semibold">{tool}</h1>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-2 text-sm text-muted-foreground select-none cursor-pointer">
            <input
              type="checkbox"
              checked={fullRefresh}
              onChange={(e) => setFullRefresh(e.target.checked)}
              className="rounded"
            />
            Full refresh
          </label>
          <button
            disabled={running}
            onClick={runAll}
            className="flex items-center gap-1.5 border rounded-md px-3 py-1.5 text-sm hover:bg-muted transition-colors disabled:opacity-50"
          >
            <PlayCircle className={cn('h-3.5 w-3.5', running && 'animate-pulse')} />
            Run All
          </button>
        </div>
      </div>

      <JobStatusPanel state={jobState} onDismiss={resetJob} />

      {extractor.tasks.length === 0 ? (
        <p className="text-muted-foreground text-sm mt-4">No tasks defined for this extractor.</p>
      ) : (
        <Card className="p-0 mt-4">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                <th className="text-left px-4 py-2.5 font-medium">Task</th>
                <th className="text-left px-4 py-2.5 font-medium">Table</th>
                <th className="text-left px-4 py-2.5 font-medium">Mode</th>
                <th className="px-4 py-2.5" />
              </tr>
            </thead>
            <tbody>
              {extractor.tasks.map((t) => (
                <tr key={t.name} className="border-b last:border-0">
                  <td className="px-4 py-2.5 font-medium">{t.name}</td>
                  <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground">{t.table}</td>
                  <td className="px-4 py-2.5">
                    <Badge className={t.incremental ? 'text-blue-700' : 'text-muted-foreground'}>
                      {t.incremental ? 'incremental' : 'full'}
                    </Badge>
                  </td>
                  <td className="px-4 py-2.5 text-right">
                    <button
                      disabled={running}
                      onClick={() => runTask(t.name)}
                      className="flex items-center gap-1.5 border rounded-md px-2.5 py-1 text-xs hover:bg-muted transition-colors disabled:opacity-50 ml-auto"
                    >
                      <Play className="h-3 w-3" />
                      Run
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  )
}
