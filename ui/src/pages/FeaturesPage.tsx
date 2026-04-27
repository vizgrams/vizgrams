// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useState } from 'react'
import { useApi } from '@/hooks/useApi'
import { useModel } from '@/context/ModelContext'

import { ExpressionEditor } from '@/components/ExpressionEditor'
import { YamlEditor } from '@/components/YamlEditor'
import { EditSection } from '@/pages/explore/EditSection'
import type { ValidStatus } from '@/components/StatusBadge'
import { Plus, Save, Play, Eye, Loader2, Check } from 'lucide-react'
import { cn, pollJob } from '@/lib/utils'
import type { FeatureSummary } from '@/api/client'

// ---------------------------------------------------------------------------
// Feature form draft
// ---------------------------------------------------------------------------

interface FeatureDraft {
  feature_id: string
  name: string
  description: string
  data_type: string
  expr: string
  entity: string
}

function emptyDraft(entity: string): FeatureDraft {
  return { feature_id: '', name: '', description: '', data_type: 'FLOAT', expr: '', entity }
}

function featureToYaml(draft: FeatureDraft): string {
  const lines = [
    `feature_id: "${draft.feature_id}"`,
    `name: "${draft.name}"`,
    `entity_type: "${draft.entity}"`,
    `entity_key: id`,
    `data_type: ${draft.data_type}`,
    `materialization_mode: materialized`,
  ]
  if (draft.description) lines.push(`description: "${draft.description}"`)
  lines.push(`expr: "${draft.expr.replace(/"/g, '\\"')}"`)
  return lines.join('\n')
}

// ---------------------------------------------------------------------------
// Detail panel (form fields — buttons live in the toolbar)
// ---------------------------------------------------------------------------

function DetailPanel({ draft, isNew, onChange }: {
  draft: FeatureDraft
  isNew: boolean
  onChange: (d: FeatureDraft) => void
}) {
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-1">
          <label className="text-xs font-medium text-muted-foreground">Name</label>
          <input
            className="w-full text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
            value={draft.name}
            onChange={(e) => onChange({ ...draft, name: e.target.value })}
            placeholder="e.g. Cycle Time (hours)"
          />
        </div>
        <div className="space-y-1">
          <label className="text-xs font-medium text-muted-foreground">Feature ID</label>
          <input
            className={cn(
              'w-full text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring font-mono',
              !isNew && 'opacity-60 cursor-not-allowed',
            )}
            value={draft.feature_id}
            onChange={(e) => isNew && onChange({ ...draft, feature_id: e.target.value })}
            readOnly={!isNew}
            placeholder="entity.feature_name"
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-1">
          <label className="text-xs font-medium text-muted-foreground">Entity</label>
          <input
            className="w-full text-sm border border-border rounded-md px-3 py-1.5 bg-background opacity-60 cursor-not-allowed"
            value={draft.entity}
            readOnly
          />
        </div>
        <div className="space-y-1">
          <label className="text-xs font-medium text-muted-foreground">Data Type</label>
          <select
            className="w-full text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
            value={draft.data_type}
            onChange={(e) => onChange({ ...draft, data_type: e.target.value })}
          >
            <option value="FLOAT">FLOAT</option>
            <option value="INTEGER">INTEGER</option>
            <option value="STRING">STRING</option>
          </select>
        </div>
      </div>

      <div className="space-y-1">
        <label className="text-xs font-medium text-muted-foreground">Description</label>
        <input
          className="w-full text-sm border border-border rounded-md px-3 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
          value={draft.description}
          onChange={(e) => onChange({ ...draft, description: e.target.value })}
          placeholder="What does this feature measure?"
        />
      </div>

      <div className="space-y-1">
        <label className="text-xs font-medium text-muted-foreground">Expression</label>
        <ExpressionEditor
          entity={draft.entity}
          mode="feature"
          value={draft.expr}
          onChange={(v) => onChange({ ...draft, expr: v })}
          rows={4}
          placeholder='e.g. datetime_diff(created_at, merged_at, unit="hours")'
          hidePreviewButton
        />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function FeaturesPage() {
  const { api, model } = useModel()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [draft, setDraft] = useState<FeatureDraft | null>(null)
  const [isNew, setIsNew] = useState(false)
  const [filterEntity, setFilterEntity] = useState<string>('all')
  const [refreshKey, setRefreshKey] = useState(0)
  const [yamlContent, setYamlContent] = useState('')
  const [savedYaml, setSavedYaml] = useState('')
  const [editMode, setEditMode] = useState<'builder' | 'yaml'>('builder')
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [reconciling, setReconciling] = useState(false)
  const [_error, setError] = useState<string | null>(null)
  const [validStatus, setValidStatus] = useState<ValidStatus>('idle')
  const [validErrors, setValidErrors] = useState<{ path: string; message: string }[]>([])
  const [previewState, setPreviewState] = useState<
    | { status: 'idle' }
    | { status: 'loading' }
    | { status: 'ok'; results: { entity_id: string; value: string | number | null }[] }
    | { status: 'error'; message: string }
  >({ status: 'idle' })

  const allFeaturesState = useApi(
    () => api.listAllFeatures(filterEntity !== 'all' ? filterEntity : undefined),
    [model, filterEntity, refreshKey],
  )
  const entitiesState = useApi(() => api.listEntities(), [model])

  const features: FeatureSummary[] = allFeaturesState.status === 'ok' ? allFeaturesState.data : []
  const entities = entitiesState.status === 'ok' ? entitiesState.data : []

  const grouped = features.reduce<Record<string, FeatureSummary[]>>((acc, f) => {
    ;(acc[f.entity] ??= []).push(f)
    return acc
  }, {})

  async function handlePreview() {
    if (!draft?.expr || !draft?.entity) return
    setPreviewState({ status: 'loading' })
    try {
      const result = await api.previewExpression(draft.entity, draft.expr)
      setPreviewState({ status: 'ok', results: result.results })
    } catch (e) {
      setPreviewState({ status: 'error', message: e instanceof Error ? e.message : 'Preview failed' })
    }
  }

  function handleSelect(f: FeatureSummary) {
    const id = f.feature_id ?? `${f.entity}.${f.name}`
    setSelectedId(id)
    setIsNew(false)
    setDraft({
      feature_id: id,
      name: f.name ?? '',
      description: f.description ?? '',
      data_type: f.data_type ?? 'FLOAT',
      expr: f.expr ?? '',
      entity: f.entity,
    })
    const yaml = f.raw_yaml ?? ''
    setYamlContent(yaml)
    setSavedYaml(yaml)
    setError(null)
    setValidStatus('valid')
    setValidErrors([])
    setPreviewState({ status: 'idle' })
  }

  function handleNew() {
    const entity = filterEntity !== 'all' ? filterEntity : (entities[0]?.name ?? '')
    setIsNew(true)
    setSelectedId('__new__')
    setDraft(emptyDraft(entity))
    setYamlContent('')
    setSavedYaml('')
    setError(null)
    setValidStatus('idle')
    setValidErrors([])
  }

  async function handleSave() {
    if (!draft) return
    setSaving(true); setError(null); setSaved(false); setValidErrors([])
    const shortName = draft.feature_id.split('.').pop() ?? draft.feature_id
    try {
      const yaml = featureToYaml(draft)
      const res = await fetch(
        `/api/v1/model/${model}/entity/${draft.entity}/feature/${encodeURIComponent(shortName)}`,
        { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ content: yaml }) },
      )
      if (!res.ok) throw new Error(await res.text())
      setIsNew(false)
      setRefreshKey((k) => k + 1)
      setSaved(true); setTimeout(() => setSaved(false), 2000)
      setValidStatus('valid')
      setTimeout(() => setValidStatus('idle'), 3000)
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Save failed'
      setError(msg)
      setValidStatus('invalid')
      setValidErrors([{ path: '', message: msg }])
    } finally { setSaving(false) }
  }

  async function handleReconcile() {
    if (!draft) return
    setReconciling(true); setError(null)
    try {
      const job = await api.reconcileFeatures(draft.entity)
      await pollJob(api.getJob, job.job_id)
    } catch (e) { setError(e instanceof Error ? e.message : 'Reconcile failed') }
    finally { setReconciling(false) }
  }

  async function handleYamlSave() {
    if (!selectedId || selectedId === '__new__') return
    const updated = await api.saveFeatureYaml(selectedId, yamlContent)
    setSavedYaml(yamlContent)
    setRefreshKey((k) => k + 1)
    if (updated.raw_yaml) setSavedYaml(updated.raw_yaml)
  }

  const canSave = !saving && !!draft?.name && !!draft?.feature_id && !!draft?.expr

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left: feature list */}
      <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="px-3 py-3 border-b flex items-center justify-between shrink-0">
          <span className="text-sm font-semibold">Features</span>
          <button
            onClick={handleNew}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            <Plus className="h-3.5 w-3.5" /> New
          </button>
        </div>

        <div className="px-3 py-2 border-b">
          <select
            className="w-full text-xs border border-border rounded px-2 py-1.5 bg-background focus:outline-none focus:ring-1 focus:ring-ring"
            value={filterEntity}
            onChange={(e) => setFilterEntity(e.target.value)}
          >
            <option value="all">All entities</option>
            {entities.map((e) => <option key={e.name} value={e.name}>{e.name}</option>)}
          </select>
        </div>

        <div className="flex-1 overflow-y-auto py-1">
          {allFeaturesState.status === 'loading' && (
            <p className="px-4 py-3 text-xs text-muted-foreground">Loading…</p>
          )}
          {allFeaturesState.status === 'error' && (
            <p className="px-4 py-3 text-xs text-red-600">{allFeaturesState.error}</p>
          )}
          {allFeaturesState.status === 'ok' && (
            <>
              {Object.entries(grouped).map(([entity, fns]) => (
                <div key={entity}>
                  <div className="px-4 pt-2 pb-0.5 text-[10px] font-semibold text-muted-foreground uppercase tracking-wide">
                    {entity}
                  </div>
                  {fns.map((f) => {
                    const id = f.feature_id ?? `${f.entity}.${f.name}`
                    return (
                      <button
                        key={id}
                        onClick={() => handleSelect(f)}
                        title={f.name}
                        className={cn(
                          'w-full text-left px-4 py-2 text-sm transition-colors flex items-center gap-2',
                          selectedId === id
                            ? 'bg-primary/10 text-primary font-medium'
                            : 'text-muted-foreground hover:bg-muted hover:text-foreground',
                        )}
                      >
                        <span className="text-[9px] font-bold text-primary/50 border border-primary/30 rounded px-0.5 leading-none shrink-0">ƒ</span>
                        <span className="line-clamp-2 break-all">{f.name}</span>
                      </button>
                    )
                  })}
                </div>
              ))}
              {features.length === 0 && (
                <p className="px-4 py-6 text-xs text-muted-foreground text-center">No features yet</p>
              )}
            </>
          )}
        </div>
      </aside>

      {/* Right: detail + editor */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {!draft ? (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select a feature to edit, or click <Plus className="h-3.5 w-3.5 mx-1 inline" /> New.
          </div>
        ) : (
          <>
            {/* Toolbar */}
            <div className="shrink-0 border-b px-6 py-3 flex items-center gap-2">
              <h1 className="text-lg font-semibold flex-1 truncate">
                {isNew ? 'New feature' : (draft.name || 'Untitled')}
              </h1>
              <button
                disabled={!canSave}
                onClick={handleSave}
                className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40"
              >
                {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : saved ? <Check className="h-3.5 w-3.5" /> : <Save className="h-3.5 w-3.5" />}
                {saving ? 'Saving…' : saved ? 'Saved' : 'Save'}
              </button>
              <button
                disabled={!draft?.expr || previewState.status === 'loading'}
                onClick={handlePreview}
                className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40"
              >
                {previewState.status === 'loading' ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Eye className="h-3.5 w-3.5" />}
                {previewState.status === 'loading' ? 'Loading…' : 'Preview'}
              </button>
              {!isNew && (
                <button
                  disabled={reconciling}
                  onClick={handleReconcile}
                  className="flex items-center gap-1.5 bg-primary text-primary-foreground rounded-md px-3 py-1.5 text-xs font-medium hover:opacity-90 transition-opacity disabled:opacity-40"
                >
                  {reconciling ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Play className="h-3.5 w-3.5" />}
                  {reconciling ? 'Reconciling…' : 'Reconcile'}
                </button>
              )}
            </div>

            {/* Scrollable body */}
            <div className="flex-1 overflow-y-auto px-6 py-4 space-y-3">
              <EditSection
                defaultOpen={isNew}
                mode={editMode}
                onModeChange={setEditMode}
                isDirty={yamlContent !== savedYaml}
                builderContent={<DetailPanel draft={draft} isNew={isNew} onChange={setDraft} />}
                yamlContent={
                  !isNew ? (
                    <YamlEditor
                      name={`${selectedId}.yaml`}
                      historyKey={{ type: 'feature', name: selectedId! }}
                      content={yamlContent}
                      savedContent={savedYaml}
                      onChange={setYamlContent}
                      onSave={handleYamlSave}
                      hideHeader
                      hideSaveButton
                    />
                  ) : (
                    <div className="px-4 py-6 text-center text-xs text-muted-foreground">Save the feature first to edit its YAML.</div>
                  )
                }
                historyKey={!isNew && selectedId ? { type: 'feature', name: selectedId } : undefined}
                onRestoreVersion={(content) => setYamlContent(content)}
                validStatus={validStatus}
                validErrors={validErrors}
              />

              {/* Preview results */}
              {previewState.status === 'ok' && (
                <div className="border rounded-lg overflow-hidden text-xs">
                  <div className="px-4 py-2 bg-muted/30 border-b text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                    Preview
                  </div>
                  <table className="w-full">
                    <thead>
                      <tr className="border-b bg-muted/20">
                        <th className="text-left px-4 py-2 font-medium text-muted-foreground">Entity ID</th>
                        <th className="text-right px-4 py-2 font-medium text-muted-foreground">Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {previewState.results.map((r) => (
                        <tr key={r.entity_id} className="border-b last:border-0">
                          <td className="px-4 py-2 font-mono text-muted-foreground truncate max-w-xs">{r.entity_id}</td>
                          <td className="px-4 py-2 font-mono text-right font-medium">
                            {r.value === null ? <span className="italic opacity-40">null</span> : String(r.value)}
                          </td>
                        </tr>
                      ))}
                      {previewState.results.length === 0 && (
                        <tr><td colSpan={2} className="px-4 py-3 text-center text-muted-foreground italic">No results</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              )}
              {previewState.status === 'error' && (
                <p className="text-xs text-destructive bg-destructive/10 border border-destructive/30 rounded-lg px-4 py-2.5">
                  {previewState.message}
                </p>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
