// Copyright 2024-2026 Oliver Fenton
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ModelSummary {
  name: string
  display_name: string
  description: string
  owner: string
  created_at: string
  status: string
  tags: string[]
  is_active: boolean
}

export interface AccessRule {
  email: string
  role: string
}

export interface ModelDetail extends ModelSummary {
  config: { tools_enabled: string[]; managed: Record<string, unknown> } | null
  database: {
    path: string
    present: boolean
    raw_tables: number
    raw_row_count: number
    semantic_tables: number
    semantic_row_count: number
    last_extract_at: string | null
    last_map_at: string | null
  }
  audit: { timestamp: string; event: string; actor: string; detail: string }[]
  access_rules: AccessRule[] | null
}

export interface ModelCreate {
  name: string
  display_name: string
  description: string
  owner: string
  status?: string
  tags?: string[]
}

export interface ModelPatch {
  display_name?: string
  description?: string
  owner?: string
  tags?: string[]
}

export interface EntitySummary {
  name: string
  table_name: string
  attribute_count: number
  relation_count: number
  feature_count: number
  row_count: number | null
  table_exists: boolean
}

export interface FeatureOut {
  feature_id: string
  name: string
  description: string | null
  data_type: string
  expr: string
}

export interface AttributeOut {
  name: string
  type: string
  semantic: string
}

export interface RelationOut {
  name: string
  target: string
  cardinality: string
  via: string[]
}

export interface DbStats {
  present: boolean
  row_count: number
  last_updated_at: string | null
}

export interface EntityDetail {
  name: string
  table_name: string
  attributes: AttributeOut[]
  relations: RelationOut[]
  features: FeatureOut[]
  database: DbStats
  display_list: string[]
  display_detail: string[]
  display_order: { column: string; direction: 'asc' | 'desc' }[]
  raw_yaml: string | null
}

export interface QueryResult {
  query: string
  sql: string
  columns: string[]
  rows: (string | number | null)[][]
  row_count: number
  total_row_count: number
  duration_ms: number
  truncated: boolean
  formats: Record<string, { type: string; pattern: string | null; unit: string | null }>
}

export interface GraphNode {
  id: string
  label: string
}

export interface GraphEdge {
  id: string
  source: string
  target: string
  label: string
  cardinality: string
}

export interface GraphOut {
  nodes: GraphNode[]
  edges: GraphEdge[]
}

export type RelationshipStub =
  | { target: string; cardinality: 'MANY_TO_ONE' | 'ONE_TO_ONE'; id: string | null }
  | { target: string; cardinality: 'ONE_TO_MANY' | 'MANY_TO_MANY'; count: number | null }

export interface EntityRecord {
  entity: string
  id: string
  properties: Record<string, string | number | null>
  relationships: Record<string, RelationshipStub>
  feature_values: Record<string, { value: string | number | null; computed_at: string | null }>
}

export interface ToolSummary {
  name: string
  enabled: boolean
}

export interface TaskDef {
  name: string
  command: string
  table: string
  incremental: boolean
}

export interface ExtractorDetail {
  tool: string
  tasks: TaskDef[]
  raw_yaml: string | null
}

export interface MapperSummary {
  name: string
  file: string
  depends_on: string[]
  target_table: string | null
  entity: string | null
  raw_yaml: string | null
}

export interface FeatureSummary {
  feature_id: string | null
  name: string
  entity: string
  feature_type: string
  description: string | null
  data_type: string | null
  expr: string | null
  raw_yaml: string | null
}

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'

export interface JobOut {
  job_id: string
  model: string
  operation: string
  status: JobStatus
  started_at: string
  entity: string | null
  extractor: string | null
  task: string | null
  completed_at: string | null
  result: Record<string, unknown> | null
  error: string | null
  progress: string[]
  warnings: string[]
}

export interface RelatedResult {
  entity: string
  id: string
  relationship: string
  target: string
  target_pk: string | null
  cardinality: string
  columns: string[]
  rows: (string | number | null)[][]
  row_count: number
  total_row_count: number
  truncated: boolean
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}: ${path}`)
  const ct = res.headers.get('content-type') ?? ''
  if (!ct.includes('application/json')) {
    throw new Error(`Expected JSON but got ${ct || 'unknown content type'}: ${path}`)
  }
  return res.json() as Promise<T>
}

async function put<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(path, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(`${res.status}: ${detail}`)
  }
  return res.json() as Promise<T>
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(`${res.status}: ${detail}`)
  }
  return res.json() as Promise<T>
}

// ---------------------------------------------------------------------------
// API factory — call makeApi(model) to get a bound client
// ---------------------------------------------------------------------------

export function makeApi(model: string) {
  const BASE = `/api/v1/model/${model}`
  return {
    listEntities: () =>
      get<EntitySummary[]>(`${BASE}/entity`),

    getEntity: (entity: string) =>
      get<EntityDetail>(`${BASE}/entity/${entity}`),

    executeInline: (query: object, limit = 200, offset = 0) =>
      post<QueryResult>(
        `${BASE}/query/execute-inline?limit=${limit}&offset=${offset}`,
        query,
      ),

    getEntityRecord: (entity: string, id: string) =>
      get<EntityRecord>(`${BASE}/explore/${entity}/${encodeURIComponent(id)}`),

    getRelated: (entity: string, id: string, relationship: string, limit = 50, offset = 0) =>
      get<RelatedResult>(
        `${BASE}/explore/${entity}/${encodeURIComponent(id)}/related/${relationship}?limit=${limit}&offset=${offset}`,
      ),

    runMapper: (entity: string) =>
      post<JobOut>(`${BASE}/entity/${entity}/mapper/execute`, {}),

    rematerializeEntity: (entity: string) =>
      post<JobOut>(`${BASE}/entity/${entity}/rematerialize`, {}),

    getJob: (jobId: string) =>
      get<JobOut>(`${BASE}/job/${jobId}`),

    listJobs: (params?: { status?: string; operation?: string; limit?: number }) => {
      const qs = new URLSearchParams()
      if (params?.status) qs.set('status', params.status)
      if (params?.operation) qs.set('operation', params.operation)
      if (params?.limit) qs.set('limit', String(params.limit))
      const q = qs.toString()
      return get<JobOut[]>(`${BASE}/job${q ? `?${q}` : ''}`)
    },

    getGraph: () =>
      get<GraphOut>(`${BASE}/graph`),

    getGraphOwl: () =>
      fetch(`${BASE}/graph?format=turtle`).then((res) => res.text()),

    listQueries: () =>
      get<QuerySummary[]>(`${BASE}/query`),

    getQuery: (name: string) =>
      get<QueryDetail>(`${BASE}/query/${encodeURIComponent(name)}`),

    executeQuery: (name: string, limit = 1000, offset = 0) =>
      post<QueryResult>(`${BASE}/query/${encodeURIComponent(name)}/execute?limit=${limit}&offset=${offset}`, {}),

    validateQuery: (name: string) =>
      post<ValidationResult>(`${BASE}/query/${encodeURIComponent(name)}/validate`, {}),

    saveQuery: (name: string, content: string) =>
      put<QueryDetail>(`${BASE}/query/${encodeURIComponent(name)}`, { content }),

    executeInlineYaml: (name: string, content: string, limit = 1000, offset = 0) =>
      post<QueryResult>(`${BASE}/query/_execute?limit=${limit}&offset=${offset}`, { name, content }),

    validateInline: (name: string, content: string) =>
      post<ValidationResult>(`${BASE}/query/_validate`, { name, content }),

    getEntityFeatureValues: (entity: string) =>
      get<Record<string, Record<string, string | number | null>>>(`${BASE}/entity/${entity}/feature-values`),

    validateExpression: (entity: string, expr: string, mode: ExprMode) =>
      post<ExprValidationResult>(`${BASE}/expression/validate`, { entity, expr, mode }),

    previewExpression: (entity: string, expr: string, entity_id?: string) =>
      post<ExprPreviewResult>(`${BASE}/expression/preview`, { entity, expr, entity_id }),

    listFunctions: (mode?: ExprMode) => {
      const qs = mode ? `?mode=${mode}` : ''
      return get<FunctionDoc[]>(`${BASE}/expression/functions${qs}`)
    },

    updateFeatureExpr: (entity: string, featureId: string, expr: string) =>
      put<FeatureOut>(`${BASE}/entity/${entity}/feature/${encodeURIComponent(featureId)}`, { expr }),

    listTools: () => get<ToolSummary[]>(`${BASE}/tool`),

    getExtractor: (tool: string) => get<ExtractorDetail>(`${BASE}/tool/${tool}/extract`),

    saveExtractor: (tool: string, content: string) =>
      put<ExtractorDetail>(`${BASE}/tool/${tool}/extract`, { content }),

    listMappers: () => get<MapperSummary[]>(`${BASE}/mapper`),

    getMapper: (name: string) => get<MapperSummary>(`${BASE}/mapper/${encodeURIComponent(name)}`),

    saveMapper: (name: string, content: string) =>
      put<MapperSummary>(`${BASE}/mapper/${encodeURIComponent(name)}`, { content }),

    listAllFeatures: (entity?: string) => {
      const qs = entity ? `?entity=${encodeURIComponent(entity)}` : ''
      return get<FeatureSummary[]>(`${BASE}/feature${qs}`)
    },

    saveFeatureYaml: (featureId: string, content: string) =>
      put<FeatureSummary>(`${BASE}/feature/${encodeURIComponent(featureId)}/yaml`, { content }),

    saveEntityYaml: (entity: string, content: string) =>
      put<EntityDetail>(`${BASE}/entity/${encodeURIComponent(entity)}/yaml`, { content }),

    reconcileFeatures: (entity?: string) => {
      const qs = entity ? `?entity=${encodeURIComponent(entity)}` : ''
      return post<JobOut>(`${BASE}/feature/reconcile${qs}`, {})
    },

    runExtractor: (tool: string, task?: string, fullRefresh = false) => {
      const qs = new URLSearchParams()
      if (task) qs.set('task', task)
      if (fullRefresh) qs.set('full_refresh', 'true')
      return post<JobOut>(`${BASE}/tool/${tool}/extract/execute?${qs}`, {})
    },

    listViews: () =>
      get<ViewSummary[]>(`${BASE}/view`),

    getView: (name: string) =>
      get<ViewDetail>(`${BASE}/view/${encodeURIComponent(name)}`),

    executeView: (name: string, limit = 1000, offset = 0, params?: Record<string, string>) =>
      post<ViewResult>(`${BASE}/view/${encodeURIComponent(name)}/execute?limit=${limit}&offset=${offset}`, params ? { params } : {}),

    validateEntity: (entity: string) =>
      post<ValidationResult>(`${BASE}/entity/${encodeURIComponent(entity)}/validate`, {}),

    validateMapper: (entity: string) =>
      post<ValidationResult>(`${BASE}/entity/${encodeURIComponent(entity)}/mapper/validate`, {}),

    validateExtractor: (tool: string) =>
      post<ValidationResult>(`${BASE}/tool/${encodeURIComponent(tool)}/extract/validate`, {}),

    validateView: (name: string) =>
      post<ValidationResult>(`${BASE}/view/${encodeURIComponent(name)}/validate`, {}),

    saveView: (name: string, content: string) =>
      put<ViewDetail>(`${BASE}/view/${encodeURIComponent(name)}`, { content }),

    listApplications: () =>
      get<ApplicationSummary[]>(`${BASE}/application`),

    getApplication: (name: string) =>
      get<ApplicationDetail>(`${BASE}/application/${encodeURIComponent(name)}`),

    validateApplication: (name: string) =>
      post<ValidationResult>(`${BASE}/application/${encodeURIComponent(name)}/validate`, {}),

    saveApplication: (name: string, content: string) =>
      put<ApplicationDetail>(`${BASE}/application/${encodeURIComponent(name)}`, { content }),

    listVersions: (artifactType: string, name: string) => {
      const seg: Record<string, string> = { entity: 'entity', mapper: 'mapper', feature: 'feature', extractor: 'tool', query: 'query', view: 'view', application: 'application' }
      return get<VersionSummary[]>(`${BASE}/${seg[artifactType] ?? artifactType}/${encodeURIComponent(name)}/versions`)
    },

    getVersion: (artifactType: string, name: string, versionId: string) => {
      const seg: Record<string, string> = { entity: 'entity', mapper: 'mapper', feature: 'feature', extractor: 'tool', query: 'query', view: 'view', application: 'application' }
      return get<VersionDetail>(`${BASE}/${seg[artifactType] ?? artifactType}/${encodeURIComponent(name)}/versions/${versionId}`)
    },
  }
}

export interface ViewSummary {
  name: string
  type: string
  query: string
}

export interface ViewDetail {
  name: string
  type: string
  query: string
  measure: string | null
  visualization: Record<string, unknown>
  inputs: Record<string, { type: string; default: unknown }>
  params: ParamDef[]
  raw_yaml: string | null
}

export interface ViewResult extends Omit<ViewDetail, 'raw_yaml'> {
  columns: string[]
  rows: (string | number | null)[][]
  row_count: number
  total_row_count: number
  duration_ms: number
  truncated: boolean
  formats: Record<string, { type: string; pattern: string | null; unit: string | null }>
}

export interface ParamDef {
  name: string
  type: 'string' | 'number' | 'duration'
  label: string | null
  default: string | null
  optional: boolean
}

export interface ApplicationSummary {
  name: string
  view_count: number
}

export interface ApplicationDetail {
  name: string
  views: string[]
  layout: { row: string[] }[]
  params: ParamDef[]
  raw_yaml: string | null
}

export interface QuerySummary {
  name: string
  root: string | null
  measure_count: number
  group_by_count: number
}

export interface QueryDetail {
  name: string
  root: string | null
  description: string | null
  group_by: string[]
  attributes: { field: string; alias: string; format_pattern: string }[]
  detail_attributes: { field: string; alias: string }[]
  measures: Record<string, unknown>
  where: string[]
  params?: ParamDef[]
  order_by: { field: string; direction: string }[]
  compiled_sql: string | null
  raw_yaml: string | null
}

export interface ValidationResult {
  valid: boolean
  errors: { path: string; message: string }[]
  compiled_sql: string | null
}

export type ExprMode = 'feature' | 'measure' | 'filter'

export interface VersionSummary {
  id: string
  version_num: number
  checksum: string
  message: string | null
  created_at: string
  is_current: number
}

export interface VersionDetail extends VersionSummary {
  content: string
}

export interface ExprValidationResult {
  valid: boolean
  errors: { message: string }[]
  compiled_sql: string | null
}

export interface ExprPreviewResult {
  results: { entity_id: string; value: string | number | null }[]
  sql: string
}

export interface FunctionDoc {
  name: string
  signature: string
  description: string
  example: string
  valid_modes: ExprMode[]
  category: string
}

async function del(path: string): Promise<void> {
  const res = await fetch(path, { method: 'DELETE' })
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(`${res.status}: ${detail}`)
  }
}

async function patch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(path, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(`${res.status}: ${detail}`)
  }
  return res.json() as Promise<T>
}

export const listModels = () => get<ModelSummary[]>('/api/v1/model')
export const getModel = (name: string) => get<ModelDetail>(`/api/v1/model/${name}`)
export const createModel = (data: ModelCreate) => post<ModelDetail>('/api/v1/model', data)
export const updateModel = (name: string, data: ModelPatch) => patch<ModelDetail>(`/api/v1/model/${name}`, data)
export const archiveModel = (name: string, reason?: string) =>
  post<ModelDetail>(`/api/v1/model/${name}/archive`, { reason: reason ?? null })
export const deleteModel = (name: string) => del(`/api/v1/model/${name}`)
export const setActiveModel = (name: string) =>
  post<{ active: string }>(`/api/v1/model/${name}/set-active`, {})
export const getModelAccess = (name: string) => get<AccessRule[] | null>(`/api/v1/model/${name}/access`)
export const setModelAccess = (name: string, rules: AccessRule[] | null) =>
  put<AccessRule[] | null>(`/api/v1/model/${name}/access`, { rules })

export interface ModelConfig {
  tools: Record<string, Record<string, unknown>>
  database: Record<string, unknown>
  database_managed: boolean
}
export const getModelConfig = (name: string) => get<ModelConfig>(`/api/v1/model/${name}/config`)
export const updateModelConfig = (name: string, data: { tools?: Record<string, Record<string, unknown>>; database?: Record<string, unknown> }) =>
  put<ModelConfig>(`/api/v1/model/${name}/config`, data)
export type PlatformRole = 'admin' | 'creator' | 'viewer'
export type MeResponse = {
  email: string | null
  display_name: string | null
  provider: string
  is_system_admin: boolean
  is_creator: boolean
  role: PlatformRole
  hard_logout_url: string
}
export const getMe = () => get<MeResponse>('/api/v1/me')

export interface VizgramSummary {
  id: string
  dataset_ref: string
  query_ref: string
  title: string
  caption: string | null
  author_id: string
  published_at: string
  significance_score: number
  chart_config: {
    type: string
    visualization: Record<string, unknown>
    columns: string[]
  }
  data_snapshot: (string | number | null)[][] | null
  tags: string[]
  author_display_name: string | null
  like_count: number
  save_count: number
  viewer_liked: boolean
  viewer_saved: boolean
}

export const listFeed = (params?: { limit?: number; offset?: number; dataset_ref?: string; author_id?: string; saved_only?: boolean }) => {
  const qs = new URLSearchParams()
  if (params?.limit != null) qs.set('limit', String(params.limit))
  if (params?.offset != null) qs.set('offset', String(params.offset))
  if (params?.dataset_ref) qs.set('dataset_ref', params.dataset_ref)
  if (params?.author_id) qs.set('author_id', params.author_id)
  if (params?.saved_only) qs.set('saved_only', 'true')
  const q = qs.toString()
  return get<VizgramSummary[]>(`/api/v1/vizgrams${q ? `?${q}` : ''}`)
}

export interface PublishVizgramRequest {
  model: string
  query_ref: string
  title: string
  slice_config: Record<string, unknown>
  chart_config: Record<string, unknown>
  data_snapshot: (string | number | null)[][] | null
  caption?: string
}
export const publishVizgram = (body: PublishVizgramRequest) =>
  post<{ id: string }>('/api/v1/vizgrams', body)

export interface EngageResponse {
  like_count: number
  save_count: number
  liked: boolean
  saved: boolean
}
export const engageVizgram = (id: string, type: 'like' | 'save') =>
  post<EngageResponse>(`/api/v1/vizgrams/${id}/engage`, { type })

export const previewCaption = (body: Omit<PublishVizgramRequest, 'caption'>) =>
  post<{ caption: string | null; cached: boolean; error?: string }>('/api/v1/vizgrams/preview-caption', body)

// Default fallback (used before context is available)
export const DEFAULT_MODEL = import.meta.env.VITE_MODEL as string ?? 'example'
