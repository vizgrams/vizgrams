// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Loader2, Play, Plus, Save } from 'lucide-react'
import { useModel } from '@/context/ModelContext'
import { Spinner, ErrorMessage } from '@/components/Layout'
import { YamlEditor } from '@/components/YamlEditor'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import type { EntitySummary, EntityDetail } from '@/api/client'
import { cn, pollJob } from '@/lib/utils'

export function EntitiesPage() {
  const { api } = useModel()
  const [entities, setEntities] = useState<EntitySummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedName, setSelectedName] = useState<string | null>(null)
  const [detail, setDetail] = useState<EntityDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [editorContent, setEditorContent] = useState('')
  const [savedContent, setSavedContent] = useState('')
  const [saving, setSaving] = useState(false)
  const [materializing, setMaterializing] = useState(false)
  const [editMode, setEditMode] = useState<'builder' | 'yaml'>('yaml')
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])
  const [isNewMode, setIsNewMode] = useState(false)
  const [entityRefresh, setEntityRefresh] = useState(0)

  useEffect(() => {
    setLoading(true)
    api.listEntities()
      .then((list) => { setEntities(list); setLoading(false) })
      .catch((e) => { setError(e.message); setLoading(false) })
  }, [api, entityRefresh])

  useEffect(() => {
    if (!selectedName) return
    setDetailLoading(true)
    setValidStatus('idle')
    setValidErrors([])
    api.getEntity(selectedName)
      .then((d) => {
        setDetail(d)
        const yaml = d.raw_yaml ?? ''
        setEditorContent(yaml)
        setSavedContent(yaml)
        setDetailLoading(false)
      })
      .catch(() => setDetailLoading(false))
  }, [selectedName, api])

  useEffect(() => {
    if (!selectedName) return
    setValidStatus('pending')
    api.validateEntity(selectedName)
      .then((r) => { setValidStatus(r.valid ? 'valid' : 'invalid'); setValidErrors(r.errors) })
      .catch(() => setValidStatus('idle'))
  }, [selectedName, api])

  function startNew() {
    setSelectedName(null)
    setIsNewMode(true)
    setDetail(null)
    const template = `entity: NewEntity\ndescription: ""\n\nidentity:\n  id:\n    type: STRING\n    semantic: PRIMARY_KEY\n\nattributes: {}\n\nrelations: {}\n`
    setEditorContent(template)
    setSavedContent('')
    setValidStatus('idle')
    setValidErrors([])
  }

  async function handleSave() {
    const saveName = isNewMode
      ? (editorContent.match(/^entity:\s*(\S+)/m)?.[1] ?? 'new_entity')
      : selectedName
    if (!saveName || saving) return
    setSaving(true); setValidErrors([])
    try {
      const updated = await api.saveEntityYaml(saveName, editorContent)
      const yaml = updated.raw_yaml ?? editorContent
      setSavedContent(yaml)
      setDetail(updated)
      setSelectedName(saveName)
      setIsNewMode(false)
      setEntityRefresh(c => c + 1)
      setValidStatus('pending')
      api.validateEntity(saveName)
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

  async function handleMaterialize() {
    if (!selectedName || materializing) return
    setMaterializing(true)
    try {
      const job = await api.rematerializeEntity(selectedName)
      await pollJob(api.getJob, job.job_id)
    } finally {
      setMaterializing(false)
    }
  }

  if (loading) return <Spinner />
  if (error) return <ErrorMessage message={error} />

  const dirty = editorContent !== savedContent

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left: entity list */}
      <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="px-3 py-3 border-b flex items-center justify-between shrink-0">
          <span className="text-sm font-semibold">Entities</span>
          <button onClick={startNew} className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors">
            <Plus className="h-3.5 w-3.5" /> New
          </button>
        </div>
        <div className="flex-1 overflow-y-auto py-1">
          {isNewMode && (
            <div className="w-full text-left px-4 py-2.5 border-b border-border/30 bg-muted">
              <div className="text-sm font-medium text-foreground/50 italic flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-orange-400 shrink-0" />
                {editorContent.match(/^entity:\s*(\S+)/m)?.[1] ?? 'new_entity'}
              </div>
            </div>
          )}
          {entities.map((e) => (
            <button
              key={e.name}
              onClick={() => { setSelectedName(e.name); setIsNewMode(false) }}
              className={cn(
                'w-full text-left px-4 py-2 text-sm transition-colors flex items-center justify-between gap-2',
                selectedName === e.name
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground',
              )}
            >
              <span className="truncate">{e.name}</span>
              {e.row_count != null && (
                <span className="text-[10px] font-mono text-muted-foreground/60 shrink-0">
                  {e.row_count.toLocaleString()}
                </span>
              )}
            </button>
          ))}
        </div>
      </aside>

      {/* Right: detail + editor */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {!selectedName && !isNewMode ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select an entity to view and edit its YAML
          </div>
        ) : (
          <>
            {/* Toolbar */}
            <div className="shrink-0 border-b px-6 py-3 flex items-center gap-2">
              <h1 className="text-lg font-semibold flex-1">{isNewMode ? (editorContent.match(/^entity:\s*(\S+)/m)?.[1] ?? 'new_entity') : selectedName}</h1>
              {dirty && !saving && <span className="h-1.5 w-1.5 rounded-full bg-amber-400" title="Unsaved changes" />}
              <button disabled={(!dirty && !isNewMode) || saving} onClick={handleSave}
                className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40">
                {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
                {saving ? 'Saving…' : 'Save'}
              </button>
              <button disabled={materializing} onClick={handleMaterialize}
                className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40">
                {materializing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                {materializing ? 'Materializing…' : 'Materialize'}
              </button>
            </div>

            {/* Stats strip */}
            {detail && (
              <div className="shrink-0 border-b px-6 py-2.5 flex items-center gap-6 text-xs text-muted-foreground bg-muted/30">
                <span><strong className="text-foreground">{detail.attributes.length}</strong> attributes</span>
                <span><strong className="text-foreground">{detail.relations.length}</strong> relations</span>
                <span><strong className="text-foreground">{detail.features.length}</strong> features</span>
                <span>
                  {detail.database.present
                    ? <><strong className="text-foreground">{detail.database.row_count?.toLocaleString() ?? 0}</strong> rows</>
                    : <span className="text-amber-500">table not materialized</span>}
                </span>
                <span className="font-mono opacity-60">{detail.table_name}</span>
                <Link to={`/explore/${selectedName}`} className="ml-auto text-primary hover:underline">
                  Browse records →
                </Link>
              </div>
            )}

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
                    name={`${(selectedName ?? 'new_entity').replace(/(?<=[a-z0-9])([A-Z])/g, '_$1').toLowerCase()}.yaml`}
                    historyKey={{ type: 'entity', name: selectedName ?? 'new_entity' }}
                    content={editorContent}
                    savedContent={savedContent}
                    onChange={setEditorContent}
                    onSave={handleSave}
                    loading={detailLoading}
                    hideHeader
                    hideSaveButton
                  />
                }
                historyKey={{ type: 'entity', name: selectedName ?? 'new_entity' }}
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
