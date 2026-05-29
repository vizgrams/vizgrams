# vizgrams Helm chart

Install vizgrams on Kubernetes. Chart and app versions move together — every git tag publishes both an image set and a chart at the same version.

## Quick start

```bash
helm install vizgrams oci://ghcr.io/vizgrams/charts/vizgrams \
  --version 0.1.0 \
  -f values-enterprise.example.yaml
```

The chart installs three Deployments (`api`, `batch`, `ui`) behind one Ingress, plus a PVC for the SQLite metadata DB if `persistence.enabled=true`.

## What it does NOT install

- **ClickHouse** — point at an existing one via `clickhouse.host`. Use Altinity's operator, Bitnami's chart, or your DBA's setup of choice.
- **oauth2-proxy** — assume the cluster has one (Bitnami chart). Wire it via the Ingress's `auth-url` / `auth-signin` annotations.
- **cert-manager / ingress controller** — bring your own.

Keeping these out of the chart is deliberate: every enterprise has opinions about how to run them, and bundling locks you in.

## Values reference

See [`values.yaml`](./values.yaml) for the full list of knobs. The notable ones:

| Key | Purpose |
|---|---|
| `image.tag` | Override the image version (defaults to chart `appVersion`) |
| `api.replicas`, `batch.replicas`, `ui.replicas` | Scale each component independently |
| `persistence.{enabled, size, storageClass}` | PVC for the SQLite metadata DB |
| `models.existingClaim` | PVC holding the model YAML directory (or use a GitOps sync) |
| `clickhouse.{host, port, user, existingSecret}` | Point at your ClickHouse |
| `auth.{systemAdmins, existingSecret}` | Admin emails + the Secret with OIDC / cookie / batch secrets |
| `ingress.{className, host, annotations, tls}` | Cluster ingress configuration |
| `extraEnv`, `extraEnvFrom`, `extraVolumes`, `extraVolumeMounts` | Escape hatches before forking |

## Customising beyond values

If `extraEnv` etc. don't cover what you need, use one of the layered patterns in order of preference:

1. **Chart-of-charts**: your own meta-chart depends on `vizgrams` and adds resources alongside it
2. **`--post-renderer kustomize`**: patch the rendered output without forking
3. **Fork** — only if upstream is permanently misaligned. You take on security maintenance and lose the upgrade path

Open an issue if you find yourself reaching for option 3 — we'd rather add a first-class knob.

## Auth Secret shape

The Secret referenced by `auth.existingSecret` should contain (all optional):

```
OAUTH2_COOKIE_SECRET     # base64 32-byte random
OIDC_CLIENT_SECRET       # from your OIDC provider
BATCH_SERVICE_SECRET     # shared between api ↔ batch
OPENAI_API_KEY           # for LLM features (optional)
```

These are injected via `envFrom` so you can manage them in your secret store of choice (External Secrets, Sealed Secrets, etc.).
