// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { Loader2, Play, Save } from 'lucide-react'
import { useModel } from '@/context/ModelContext'
import { Spinner, ErrorMessage } from '@/components/Layout'
import { YamlEditor } from '@/components/YamlEditor'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { MapperSummary } from '@/api/client'
import { cn, pollJob } from '@/lib/utils'

export function MappersPage() {
  const { api } = useModel()
  const [mappers, setMappers] = useState<MapperSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedName, setSelectedName] = useState<string | null>(null)
  const [selectedEntity, setSelectedEntity] = useState<string | null>(null)
  const [editorContent, setEditorContent] = useState('')
  const [savedContent, setSavedContent] = useState('')
  const [saving, setSaving] = useState(false)
  const [mapping, setMapping] = useState(false)
  const [editMode, setEditMode] = useState<'builder' | 'yaml'>('yaml')
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])

  useEffect(() => {
    setLoading(true)
    api.listMappers()
      .then((list) => {
        setMappers(list)
        setLoading(false)
        if (list.length > 0 && !selectedName) selectMapper(list[0])
      })
      .catch((e) => { setError(e.message); setLoading(false) })
  }, [api])

  function selectMapper(m: MapperSummary) {
    setSelectedName(m.name)
    setSelectedEntity(m.entity)
    const yaml = m.raw_yaml ?? ''
    setEditorContent(yaml)
    setSavedContent(yaml)
    setValidStatus('idle')
    setValidErrors([])
    if (m.entity) {
      setValidStatus('pending')
      api.validateMapper(m.entity)
        .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
        .catch(() => setValidStatus('idle'))
    }
  }

  async function handleSave() {
    if (!selectedName || saving) return
    setSaving(true); setValidErrors([])
    try {
      const updated = await api.saveMapper(selectedName, editorContent)
      const yaml = updated.raw_yaml ?? editorContent
      setSavedContent(yaml)
      setMappers(prev => prev.map(m => m.name === selectedName ? { ...m, raw_yaml: yaml } : m))
      if (selectedEntity) {
        setValidStatus('pending')
        api.validateMapper(selectedEntity)
          .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
          .catch(() => setValidStatus('idle'))
      }
    } catch (e) {
      const msg = String(e)
      setValidStatus('invalid')
      setValidErrors([{ path: '', message: msg }])
    } finally {
      setSaving(false)
    }
  }

  async function handleMap() {
    if (!selectedEntity || mapping) return
    setMapping(true)
    try {
      const job = await api.runMapper(selectedEntity)
      await pollJob(api.getJob, job.job_id)
    } finally {
      setMapping(false)
    }
  }

  if (loading) return <Spinner />
  if (error) return <ErrorMessage message={error} />

  const dirty = editorContent !== savedContent

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left: mapper list */}
      <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="px-4 py-3 border-b">
          <h2 className="text-sm font-semibold">Mappers</h2>
          <p className="text-xs text-muted-foreground mt-0.5">{mappers.length} defined</p>
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {mappers.map((m) => (
            <button
              key={m.name}
              onClick={() => selectMapper(m)}
              title={m.name}
              className={cn(
                'w-full text-left px-4 py-2 text-sm transition-colors',
                selectedName === m.name
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground',
              )}
            >
              <div className="line-clamp-2 break-all">{m.name}</div>
              {m.target_table && (
                <div className="text-[10px] font-mono text-muted-foreground/60 mt-0.5">
                  → {m.target_table}
                </div>
              )}
            </button>
          ))}
          {mappers.length === 0 && (
            <p className="px-4 py-6 text-xs text-muted-foreground text-center">No mappers found</p>
          )}
        </div>
      </aside>

      {/* Right: detail + editor */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {!selectedName ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select a mapper to edit
          </div>
        ) : (
          <>
            {/* Toolbar */}
            <div className="shrink-0 border-b px-6 py-3 flex items-center gap-2">
              <h1 className="text-lg font-semibold flex-1">{selectedName}</h1>
              {dirty && !saving && <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />}
              <button disabled={!dirty || saving} onClick={handleSave}
                className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
                {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
                {saving ? 'Saving…' : 'Save'}
              </button>
              {selectedEntity && (
                <button disabled={mapping} onClick={handleMap}
                  className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
                  {mapping ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                  {mapping ? 'Mapping…' : 'Map'}
                </button>
              )}
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
                    name={`${selectedName}.yaml`}
                    historyKey={{ type: 'mapper', name: selectedName }}
                    content={editorContent}
                    savedContent={savedContent}
                    onChange={setEditorContent}
                    onSave={handleSave}
                    hideHeader
                    hideSaveButton
                  />
                }
                historyKey={{ type: 'mapper', name: selectedName }}
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
