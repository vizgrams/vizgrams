// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { Loader2, Play, Save } from 'lucide-react'
import { useModel } from '@/context/ModelContext'
import { Spinner, ErrorMessage } from '@/components/Layout'
import { YamlEditor } from '@/components/YamlEditor'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { ExtractorDetail } from '@/api/client'
import { cn, pollJob } from '@/lib/utils'

export function ToolsPage() {
  const { api, model } = useModel()
  const [extractors, setExtractors] = useState<ExtractorDetail[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedTool, setSelectedTool] = useState<string | null>(null)
  const [editorContent, setEditorContent] = useState('')
  const [savedContent, setSavedContent] = useState('')
  const [saving, setSaving] = useState(false)
  const [extracting, setExtracting] = useState(false)
  const [editMode, setEditMode] = useState<'builder' | 'yaml'>('yaml')
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])

  useEffect(() => {
    setLoading(true)
    setSelectedTool(null)
    api.listTools()
      .then(async (tools) => {
        const details = await Promise.all(
          tools.map((t) => api.getExtractor(t.name).catch(() => null)),
        )
        const valid = details.filter(Boolean) as ExtractorDetail[]
        setExtractors(valid)
        setLoading(false)
        if (valid.length > 0) selectExtractor(valid[0])
      })
      .catch((e) => { setError(String(e)); setLoading(false) })
  }, [model])

  function selectExtractor(e: ExtractorDetail) {
    setSelectedTool(e.tool)
    const yaml = e.raw_yaml ?? ''
    setEditorContent(yaml)
    setSavedContent(yaml)
    setValidStatus('idle')
    setValidErrors([])
    setValidStatus('pending')
    api.validateExtractor(e.tool)
      .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
      .catch(() => setValidStatus('idle'))
  }

  async function handleSave() {
    if (!selectedTool || saving) return
    setSaving(true); setValidErrors([])
    try {
      const updated = await api.saveExtractor(selectedTool, editorContent)
      const yaml = updated.raw_yaml ?? editorContent
      setSavedContent(yaml)
      setExtractors(prev => prev.map(e => e.tool === selectedTool ? { ...e, raw_yaml: yaml } : e))
      setValidStatus('pending')
      api.validateExtractor(selectedTool)
        .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
        .catch(() => setValidStatus('idle'))
    } catch (e) {
      const msg = String(e)
      setValidStatus('invalid')
      setValidErrors([{ path: '', message: msg }])
    } finally {
      setSaving(false)
    }
  }

  async function handleExtract() {
    if (!selectedTool || extracting) return
    setExtracting(true)
    try {
      const job = await api.runExtractor(selectedTool)
      await pollJob(api.getJob, job.job_id)
    } finally {
      setExtracting(false)
    }
  }

  if (loading) return <Spinner />
  if (error) return <ErrorMessage message={error} />

  const dirty = editorContent !== savedContent

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left: extractor list */}
      <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="px-4 py-3 border-b">
          <h2 className="text-sm font-semibold">Extractors</h2>
          <p className="text-xs text-muted-foreground mt-0.5">{extractors.length} configured</p>
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {extractors.map((e) => (
            <button
              key={e.tool}
              onClick={() => selectExtractor(e)}
              title={e.tool}
              className={cn(
                'w-full text-left px-4 py-2 text-sm transition-colors',
                selectedTool === e.tool
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground',
              )}
            >
              <div className="line-clamp-2 break-all">{e.tool}</div>
              <div className="text-[10px] text-muted-foreground/60 mt-0.5">
                {e.tasks.length} task{e.tasks.length !== 1 ? 's' : ''}
              </div>
            </button>
          ))}
          {extractors.length === 0 && (
            <p className="px-4 py-6 text-xs text-muted-foreground text-center">No extractors found</p>
          )}
        </div>
      </aside>

      {/* Right: detail + editor */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {!selectedTool ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select an extractor to edit
          </div>
        ) : (
          <>
            {/* Toolbar */}
            <div className="shrink-0 border-b px-6 py-3 flex items-center gap-2">
              <h1 className="text-lg font-semibold flex-1">{selectedTool}</h1>
              {dirty && !saving && <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />}
              <button disabled={!dirty || saving} onClick={handleSave}
                className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
                {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
                {saving ? 'Saving…' : 'Save'}
              </button>
              <button disabled={extracting} onClick={handleExtract}
                className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
                {extracting ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                {extracting ? 'Extracting…' : 'Extract'}
              </button>
            </div>

            {/* Collapsibles */}
            <div className="flex-1 overflow-y-auto px-6 py-4 space-y-3">
              <EditSection
                defaultOpen={false}
                mode={editMode}
                onModeChange={setEditMode}
                isDirty={dirty}
                validStatus={validStatus}
                builderContent={<p className="text-sm text-muted-foreground">Visual builder coming soon.</p>}
                yamlContent={
                  <YamlEditor
                    name={`extractor_${selectedTool}.yaml`}
                    historyKey={{ type: 'extractor', name: selectedTool }}
                    content={editorContent}
                    savedContent={savedContent}
                    onChange={setEditorContent}
                    onSave={handleSave}
                    hideHeader
                    hideSaveButton
                  />
                }
                historyKey={{ type: 'extractor', name: selectedTool }}
                onRestoreVersion={(content) => setEditorContent(content)}
                validErrors={validErrors}
              />
            </div>
          </>
        )}
      </div>
    </div>
  )
}
