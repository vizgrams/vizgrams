// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

import { useEffect, useState } from 'react'
import { Plus, Trash2, Archive, CircleDot } from 'lucide-react'
import {
  listModels, getModel, createModel, updateModel, archiveModel, deleteModel, setActiveModel, setModelAccess,
  getModelConfig, updateModelConfig,
} from '@/api/client'
import type { ModelSummary, ModelDetail, AccessRule, ModelCreate, ModelPatch, ModelConfig } from '@/api/client'
import { Badge, ErrorMessage, Spinner } from '@/components/Layout'
import { useModel } from '@/context/ModelContext'
import { cn } from '@/lib/utils'

const STATUS_STYLE: Record<string, string> = {
  active:       'border-blue-200 bg-blue-50 text-blue-700',
  experimental: 'border-yellow-200 bg-yellow-50 text-yellow-700',
  archived:     'border-gray-200 bg-gray-50 text-gray-500',
}

const STATUS_LABEL: Record<string, string> = {
  active:       'live',
  experimental: 'experimental',
  archived:     'archived',
}

const ROLES = ['VIEWER', 'OPERATOR', 'ADMIN'] as const

export function ModelsPage() {
  const { refresh: refreshActiveModel } = useModel()
  const [models, setModels] = useState<ModelSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [listError, setListError] = useState<string | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [detail, setDetail] = useState<ModelDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [showCreate, setShowCreate] = useState(false)

  function reloadList(selectName?: string) {
    setLoading(true)
    listModels()
      .then((ms) => {
        setModels(ms)
        const pick = selectName ?? selected ?? ms[0]?.name ?? null
        if (pick) selectModel(pick)
        else { setSelected(null); setDetail(null) }
      })
      .catch((e) => setListError(String(e)))
      .finally(() => setLoading(false))
  }

  useEffect(() => { reloadList() }, [])

  function selectModel(name: string) {
    setSelected(name)
    setDetailLoading(true)
    setDetail(null)
    getModel(name)
      .then(setDetail)
      .catch(() => setDetail(null))
      .finally(() => setDetailLoading(false))
  }

  function handleCreated(name: string) {
    setShowCreate(false)
    reloadList(name)
  }

  function handleArchived() {
    reloadList(selected ?? undefined)
  }

  function handleDeleted() {
    const remaining = models.filter((m) => m.name !== selected)
    const next = remaining[0]?.name ?? null
    reloadList(next ?? undefined)
  }

  return (
    <div className="flex h-full -mx-6 -my-6 overflow-hidden">
      {/* Left: model list */}
      <aside className="w-56 shrink-0 border-r flex flex-col overflow-hidden bg-card">
        <div className="px-4 py-3 border-b flex items-center justify-between">
          <div>
            <h2 className="text-sm font-semibold">Models</h2>
            <p className="text-xs text-muted-foreground mt-0.5">{models.length} registered</p>
          </div>
          <button
            onClick={() => setShowCreate(true)}
            title="New model"
            className="p-1 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
          >
            <Plus className="h-4 w-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto py-1">
          {loading && <p className="px-4 py-3 text-xs text-muted-foreground">Loading…</p>}
          {listError && <p className="px-4 py-3 text-xs text-red-600">{listError}</p>}
          {!loading && models.map((m) => (
            <button
              key={m.name}
              onClick={() => selectModel(m.name)}
              className={cn(
                'w-full text-left px-4 py-2 text-sm transition-colors',
                selected === m.name
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground',
                m.status === 'archived' && 'opacity-50',
              )}
            >
              <div className="flex items-center gap-1.5">
                {m.is_active && <CircleDot className="h-3 w-3 text-green-500 shrink-0" />}
                <span className="line-clamp-1">{m.display_name}</span>
              </div>
              <div className="text-[10px] font-mono text-muted-foreground/60 mt-0.5">{m.name}</div>
            </button>
          ))}
          {!loading && models.length === 0 && !listError && (
            <p className="px-4 py-6 text-xs text-muted-foreground text-center">No models yet</p>
          )}
        </div>
      </aside>

      {/* Right: detail */}
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        {detailLoading && <Spinner />}
        {!detailLoading && !detail && !selected && (
          <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
            Select a model to configure
          </div>
        )}
        {!detailLoading && detail && (
          <>
            {/* Toolbar */}
            <div className="shrink-0 border-b px-6 py-3 flex items-center gap-3">
              <h1 className="text-lg font-semibold flex-1">{detail.display_name}</h1>
              <Badge className={STATUS_STYLE[detail.status] ?? ''}>{STATUS_LABEL[detail.status] ?? detail.status}</Badge>
              {!detail.is_active && detail.status !== 'archived' && (
                <SetActiveButton model={detail} onActivated={() => { refreshActiveModel(); reloadList(detail.name) }} />
              )}
              {detail.is_active && (
                <span className="flex items-center gap-1 text-xs text-green-600 font-medium">
                  <CircleDot className="h-3.5 w-3.5" /> Active
                </span>
              )}
              {detail.status !== 'archived' && (
                <ArchiveButton model={detail} onArchived={handleArchived} />
              )}
              <DeleteButton model={detail} onDeleted={handleDeleted} />
            </div>

            {/* Scrollable body */}
            <div className="flex-1 overflow-y-auto px-6 py-5 space-y-5">
              <MetadataSection
                model={detail}
                onSaved={() => selectModel(detail.name)}
              />
              <AccessRulesSection
                model={detail}
                onSaved={() => selectModel(detail.name)}
              />
              <ConfigSection modelName={detail.name} />
            </div>
          </>
        )}
      </div>

      {showCreate && (
        <CreateModelModal
          onClose={() => setShowCreate(false)}
          onCreated={handleCreated}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Toolbar actions
// ---------------------------------------------------------------------------

function SetActiveButton({ model, onActivated }: { model: ModelDetail; onActivated: () => void }) {
  const [busy, setBusy] = useState(false)
  async function handle() {
    setBusy(true)
    try { await setActiveModel(model.name); onActivated() }
    catch (e) { alert(String(e)) }
    finally { setBusy(false) }
  }
  return (
    <button
      onClick={handle}
      disabled={busy}
      className="flex items-center gap-1.5 border border-green-200 rounded-md px-2.5 py-1.5 text-xs hover:bg-green-50 text-green-700 transition-colors disabled:opacity-40"
    >
      <CircleDot className="h-3.5 w-3.5" />
      {busy ? 'Setting…' : 'Set as active'}
    </button>
  )
}

function ArchiveButton({ model, onArchived }: { model: ModelDetail; onArchived: () => void }) {
  const [busy, setBusy] = useState(false)
  async function handle() {
    if (!confirm(`Archive "${model.name}"? It will be hidden but data is preserved.`)) return
    setBusy(true)
    try { await archiveModel(model.name); onArchived() }
    catch (e) { alert(String(e)) }
    finally { setBusy(false) }
  }
  return (
    <button
      onClick={handle}
      disabled={busy}
      className="flex items-center gap-1.5 border rounded-md px-2.5 py-1.5 text-xs hover:bg-muted transition-colors disabled:opacity-40 text-muted-foreground"
    >
      <Archive className="h-3.5 w-3.5" />
      {busy ? 'Archiving…' : 'Archive'}
    </button>
  )
}

function DeleteButton({ model, onDeleted }: { model: ModelDetail; onDeleted: () => void }) {
  const [busy, setBusy] = useState(false)
  async function handle() {
    const confirmed = prompt(`Type "${model.name}" to confirm deletion from the registry:`)
    if (confirmed !== model.name) return
    setBusy(true)
    try { await deleteModel(model.name); onDeleted() }
    catch (e) { alert(String(e)) }
    finally { setBusy(false) }
  }
  return (
    <button
      onClick={handle}
      disabled={busy}
      className="flex items-center gap-1.5 border border-red-200 rounded-md px-2.5 py-1.5 text-xs hover:bg-red-50 text-red-600 transition-colors disabled:opacity-40"
    >
      <Trash2 className="h-3.5 w-3.5" />
      {busy ? 'Deleting…' : 'Delete'}
    </button>
  )
}

// ---------------------------------------------------------------------------
// Metadata section
// ---------------------------------------------------------------------------

function MetadataSection({ model, onSaved }: { model: ModelDetail; onSaved: () => void }) {
  const [form, setForm] = useState<ModelPatch>({
    display_name: model.display_name,
    description: model.description,
    owner: model.owner,
    tags: [...model.tags],
  })
  const [tagInput, setTagInput] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  // Reset when model changes
  useEffect(() => {
    setForm({ display_name: model.display_name, description: model.description, owner: model.owner, tags: [...model.tags] })
    setTagInput('')
    setError(null)
    setSaved(false)
  }, [model.name])

  function set(k: keyof ModelPatch, v: string) { setForm((f) => ({ ...f, [k]: v })) }

  function addTag() {
    const t = tagInput.trim()
    if (t && !(form.tags ?? []).includes(t)) setForm((f) => ({ ...f, tags: [...(f.tags ?? []), t] }))
    setTagInput('')
  }

  function removeTag(t: string) { setForm((f) => ({ ...f, tags: (f.tags ?? []).filter((x) => x !== t) })) }

  async function handleSave() {
    setSaving(true); setError(null)
    try {
      await updateModel(model.name, form)
      setSaved(true); setTimeout(() => setSaved(false), 2000)
      onSaved()
    } catch (e) { setError(String(e)) }
    finally { setSaving(false) }
  }

  return (
    <section className="space-y-4">
      <h2 className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Metadata</h2>
      {error && <ErrorMessage message={error} />}

      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1">
          <label className="text-xs text-muted-foreground">Slug</label>
          <p className="font-mono text-sm text-muted-foreground bg-muted/40 rounded px-2.5 py-1.5">{model.name}</p>
        </div>
        <div className="space-y-1">
          <label className="text-xs text-muted-foreground">Created</label>
          <p className="text-sm text-muted-foreground bg-muted/40 rounded px-2.5 py-1.5">{model.created_at.slice(0, 10)}</p>
        </div>
      </div>

      <div className="space-y-1">
        <label className="text-xs text-muted-foreground">Display name</label>
        <input value={form.display_name ?? ''} onChange={(e) => set('display_name', e.target.value)}
          className="w-full border rounded px-2.5 py-1.5 text-sm bg-background" />
      </div>

      <div className="space-y-1">
        <label className="text-xs text-muted-foreground">Description</label>
        <textarea value={form.description ?? ''} onChange={(e) => set('description', e.target.value)}
          rows={2} className="w-full border rounded px-2.5 py-1.5 text-sm bg-background resize-none" />
      </div>

      <div className="space-y-1">
        <label className="text-xs text-muted-foreground">Owner</label>
        <input value={form.owner ?? ''} onChange={(e) => set('owner', e.target.value)}
          className="w-full border rounded px-2.5 py-1.5 text-sm bg-background" />
      </div>

      <div className="space-y-1.5">
        <label className="text-xs text-muted-foreground">Tags</label>
        <div className="flex gap-1.5">
          <input value={tagInput} onChange={(e) => setTagInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addTag() } }}
            className="flex-1 border rounded px-2.5 py-1.5 text-sm bg-background" placeholder="Add tag…" />
          <button type="button" onClick={addTag}
            className="px-2.5 py-1.5 border rounded text-sm hover:bg-muted transition-colors">Add</button>
        </div>
        {(form.tags ?? []).length > 0 && (
          <div className="flex flex-wrap gap-1">
            {(form.tags ?? []).map((t) => (
              <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 rounded border text-xs">
                {t}
                <button type="button" onClick={() => removeTag(t)} className="text-muted-foreground hover:text-foreground">&times;</button>
              </span>
            ))}
          </div>
        )}
      </div>

      <div className="flex justify-end">
        <button onClick={handleSave} disabled={saving}
          className={cn(
            'px-3 py-1.5 text-sm rounded transition-colors disabled:opacity-50',
            saved ? 'bg-green-100 text-green-700 border border-green-200'
                  : 'bg-primary text-primary-foreground hover:bg-primary/90',
          )}>
          {saving ? 'Saving…' : saved ? 'Saved' : 'Save'}
        </button>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Access rules section
// ---------------------------------------------------------------------------

function AccessRulesSection({ model, onSaved }: { model: ModelDetail; onSaved: () => void }) {
  const [rules, setRules] = useState<AccessRule[]>(model.access_rules ?? [])
  const [newEmail, setNewEmail] = useState('')
  const [newRole, setNewRole] = useState('VIEWER')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    setRules(model.access_rules ?? [])
    setError(null); setSaved(false)
  }, [model.name])

  function addRule() {
    const email = newEmail.trim()
    if (!email) return
    setRules((r) => [...r, { email, role: newRole }])
    setNewEmail('')
  }

  function removeRule(i: number) { setRules((r) => r.filter((_, j) => j !== i)) }
  function updateRole(i: number, role: string) { setRules((r) => r.map((x, j) => j === i ? { ...x, role } : x)) }

  async function handleSave() {
    setSaving(true); setError(null)
    try {
      await setModelAccess(model.name, rules)
      setSaved(true); setTimeout(() => setSaved(false), 2000)
      onSaved()
    } catch (e) { setError(String(e)) }
    finally { setSaving(false) }
  }

  return (
    <section className="space-y-3 pt-4 border-t">
      <h2 className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Access rules</h2>

      {error && <ErrorMessage message={error} />}

      <div className="space-y-2">
        {rules.length === 0 ? (
          <p className="text-sm text-muted-foreground">No rules — all authenticated users have admin access.</p>
        ) : (
          <div className="rounded border divide-y">
            {rules.map((r, i) => (
              <div key={i} className="flex items-center gap-2 px-3 py-2">
                <span className="flex-1 text-sm font-mono">{r.email}</span>
                <select value={r.role} onChange={(e) => updateRole(i, e.target.value)}
                  className="border rounded px-2 py-1 text-xs bg-background">
                  {ROLES.map((ro) => <option key={ro}>{ro}</option>)}
                </select>
                <button onClick={() => removeRule(i)}
                  className="p-1 rounded hover:bg-red-50 text-muted-foreground hover:text-red-600 transition-colors">
                  <Trash2 className="h-3.5 w-3.5" />
                </button>
              </div>
            ))}
          </div>
        )}

        <div className="flex gap-1.5">
          <input value={newEmail} onChange={(e) => setNewEmail(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addRule() } }}
            placeholder="user@example.com  or  *@domain.com  or  *"
            className="flex-1 border rounded px-2.5 py-1.5 text-sm bg-background" />
          <select value={newRole} onChange={(e) => setNewRole(e.target.value)}
            className="border rounded px-2 py-1.5 text-sm bg-background">
            {ROLES.map((r) => <option key={r}>{r}</option>)}
          </select>
          <button onClick={addRule}
            className="px-2.5 py-1.5 border rounded text-sm hover:bg-muted transition-colors">Add</button>
        </div>
      </div>

      <div className="flex justify-end">
        <button onClick={handleSave} disabled={saving}
          className={cn(
            'px-3 py-1.5 text-sm rounded transition-colors disabled:opacity-50',
            saved ? 'bg-green-100 text-green-700 border border-green-200'
                  : 'bg-primary text-primary-foreground hover:bg-primary/90',
          )}>
          {saving ? 'Saving…' : saved ? 'Saved' : 'Save access rules'}
        </button>
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Config section — tools + database (editable, reads from DB)
// ---------------------------------------------------------------------------

function ConfigSection({ modelName }: { modelName: string }) {
  const [config, setConfig] = useState<ModelConfig | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [editTools, setEditTools] = useState<string>('')
  const [editing, setEditing] = useState(false)

  useEffect(() => {
    setLoading(true)
    setError('')
    setEditing(false)
    getModelConfig(modelName)
      .then((c) => {
        setConfig(c)
        setEditTools(JSON.stringify(c.tools, null, 2))
      })
      .catch((e) => setError(e.message || 'Failed to load config'))
      .finally(() => setLoading(false))
  }, [modelName])

  const handleSave = () => {
    setSaving(true)
    setError('')
    let tools: Record<string, Record<string, unknown>>
    try {
      tools = JSON.parse(editTools)
    } catch {
      setError('Invalid JSON')
      setSaving(false)
      return
    }
    updateModelConfig(modelName, { tools })
      .then((c) => {
        setConfig(c)
        setEditTools(JSON.stringify(c.tools, null, 2))
        setEditing(false)
      })
      .catch((e) => {
        const detail = e?.body?.detail || e.message || 'Failed to save'
        setError(typeof detail === 'string' ? detail : JSON.stringify(detail))
      })
      .finally(() => setSaving(false))
  }

  if (loading) return <div className="pt-4 border-t"><Spinner /></div>

  return (
    <section className="space-y-3 pt-4 border-t">
      <div className="flex items-center justify-between">
        <h2 className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
          Tools
        </h2>
        {!editing ? (
          <button
            className="text-xs text-blue-600 hover:underline"
            onClick={() => setEditing(true)}
          >
            Edit
          </button>
        ) : (
          <div className="flex gap-2">
            <button
              className="text-xs text-muted-foreground hover:underline"
              onClick={() => {
                setEditing(false)
                setError('')
                if (config) setEditTools(JSON.stringify(config.tools, null, 2))
              }}
            >
              Cancel
            </button>
            <button
              className="text-xs text-blue-600 hover:underline font-medium"
              onClick={handleSave}
              disabled={saving}
            >
              {saving ? 'Saving...' : 'Save'}
            </button>
          </div>
        )}
      </div>

      {error && <ErrorMessage message={error} />}

      {!editing && config ? (
        <ConfigReadOnly config={config} />
      ) : (
        <div className="space-y-3">
          <textarea
            className="w-full font-mono text-xs border rounded p-2 bg-muted/30 min-h-[120px]"
            value={editTools}
            onChange={(e) => setEditTools(e.target.value)}
          />
          <p className="text-xs text-muted-foreground">
            Credentials must use <code className="font-mono">env:VAR_NAME</code> or <code className="font-mono">file:secret_name</code> — literal values are rejected.
          </p>
        </div>
      )}
    </section>
  )
}

function ConfigReadOnly({ config }: { config: ModelConfig }) {
  const enabledTools = Object.entries(config.tools)
    .filter(([, cfg]) => typeof cfg === 'object' && cfg?.enabled)
    .map(([name]) => name)

  return (
    <div className="flex items-start gap-2 text-sm">
      <span className="text-muted-foreground w-28 shrink-0">Enabled</span>
      <div className="flex flex-wrap gap-1">
        {enabledTools.length > 0
          ? enabledTools.map((t) => (
              <span key={t} className="inline-flex items-center rounded border px-2 py-0.5 text-xs text-muted-foreground">{t}</span>
            ))
          : <span className="text-muted-foreground text-xs">none configured</span>
        }
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Create model modal
// ---------------------------------------------------------------------------

function CreateModelModal({ onClose, onCreated }: { onClose: () => void; onCreated: (name: string) => void }) {
  const [form, setForm] = useState<ModelCreate>({ name: '', display_name: '', description: '', owner: '', status: 'experimental', tags: [] })
  const [tagInput, setTagInput] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  function set(k: keyof ModelCreate, v: string) { setForm((f) => ({ ...f, [k]: v })) }
  function addTag() {
    const t = tagInput.trim()
    if (t && !(form.tags ?? []).includes(t)) setForm((f) => ({ ...f, tags: [...(f.tags ?? []), t] }))
    setTagInput('')
  }
  function removeTag(t: string) { setForm((f) => ({ ...f, tags: (f.tags ?? []).filter((x) => x !== t) })) }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault(); setError(null); setSaving(true)
    try { await createModel(form); onCreated(form.name) }
    catch (e) { setError(String(e)) }
    finally { setSaving(false) }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card border rounded-lg shadow-xl w-full max-w-md p-6 space-y-4">
        <h2 className="text-base font-semibold">New model</h2>
        {error && <ErrorMessage message={error} />}
        <form onSubmit={handleSubmit} className="space-y-3">
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Slug <span className="text-[10px]">— lowercase, no spaces</span></label>
            <input required pattern="[a-z0-9_-]+" value={form.name} onChange={(e) => set('name', e.target.value)}
              className="w-full border rounded px-2.5 py-1.5 text-sm font-mono bg-background" placeholder="my_model" />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Display name</label>
            <input required value={form.display_name} onChange={(e) => set('display_name', e.target.value)}
              className="w-full border rounded px-2.5 py-1.5 text-sm bg-background" placeholder="My Model" />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Description</label>
            <textarea value={form.description} onChange={(e) => set('description', e.target.value)}
              rows={2} className="w-full border rounded px-2.5 py-1.5 text-sm bg-background resize-none" />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Owner</label>
            <input value={form.owner} onChange={(e) => set('owner', e.target.value)}
              className="w-full border rounded px-2.5 py-1.5 text-sm bg-background" placeholder="vizgrams" />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Status</label>
              <select value={form.status} onChange={(e) => set('status', e.target.value)}
                className="w-full border rounded px-2.5 py-1.5 text-sm bg-background">
                <option value="experimental">experimental</option>
                <option value="active">active</option>
              </select>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Tags</label>
              <div className="flex gap-1">
                <input value={tagInput} onChange={(e) => setTagInput(e.target.value)}
                  onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addTag() } }}
                  className="flex-1 border rounded px-2.5 py-1.5 text-sm bg-background min-w-0" placeholder="tag…" />
                <button type="button" onClick={addTag} className="px-2 border rounded text-sm hover:bg-muted transition-colors">+</button>
              </div>
            </div>
          </div>
          {(form.tags ?? []).length > 0 && (
            <div className="flex flex-wrap gap-1">
              {(form.tags ?? []).map((t) => (
                <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 rounded border text-xs">
                  {t}
                  <button type="button" onClick={() => removeTag(t)} className="text-muted-foreground hover:text-foreground">&times;</button>
                </span>
              ))}
            </div>
          )}
          <p className="text-xs text-muted-foreground bg-muted/40 rounded px-3 py-2">
            After creating, configure tools and database settings from the model's Configuration section.
          </p>
          <div className="flex justify-end gap-2 pt-1">
            <button type="button" onClick={onClose} className="px-3 py-1.5 text-sm border rounded hover:bg-muted transition-colors">Cancel</button>
            <button type="submit" disabled={saving}
              className="px-3 py-1.5 text-sm rounded bg-primary text-primary-foreground hover:bg-primary/90 transition-colors disabled:opacity-50">
              {saving ? 'Creating…' : 'Create'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
