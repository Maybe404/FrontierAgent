# Local Langfuse

Self-hosted Langfuse for viewing FrontierAgent traces during development.
`docker-compose.yml` is derived from `docker-compose.upstream.yml` (the
official file, kept verbatim for diffing on upgrades).

## What differs from upstream

- Compose project name `frontier-langfuse`, so containers, network and
  volumes are namespaced and removable as one unit.
- Only the web UI (`3100`) and object storage (`9190`, needed for media
  uploads) are published, on loopback unless `LANGFUSE_BIND` is set.
  Postgres, ClickHouse, Redis and the worker are reachable only inside the
  network.
- Telemetry to Langfuse is off.
- Every secret comes from `./.env` (git-ignored). `.env.example` lists the
  keys; the `LANGFUSE_INIT_*` values create the org, project, API keys and a
  local login on first start.

## Usage

```bash
cp .env.example .env      # then fill every empty value, e.g. openssl rand -hex 16
docker compose up -d
open http://localhost:3100
```

Log in with `LANGFUSE_INIT_USER_EMAIL` / `LANGFUSE_INIT_USER_PASSWORD`. Point
the agent at it with `LANGFUSE_HOST=http://localhost:3100` and the project's
`LANGFUSE_INIT_PROJECT_PUBLIC_KEY` / `LANGFUSE_INIT_PROJECT_SECRET_KEY`.

```bash
docker compose down       # stop, keep data
docker compose down -v    # stop and delete all data volumes
```

The `LANGFUSE_INIT_*` values only apply to an empty database; after changing
them, run `docker compose down -v` first.

## Shared server

The same files run a Langfuse that several projects share over the LAN. In
`.env`, set `LANGFUSE_BIND=0.0.0.0` and point `NEXTAUTH_URL`,
`LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT` and
`LANGFUSE_S3_BATCH_EXPORT_EXTERNAL_ENDPOINT` at the server's address (see the
commented block in `.env.example`); without them, login redirects and media
uploads point at `localhost`. Give each project its own Langfuse project and
API keys (Settings → Projects in the UI).

When the server cannot reach Docker Hub, copy the images from a machine that
can (add `--platform linux/amd64` on Apple silicon):

```bash
docker compose config --images | xargs -n1 docker pull --platform linux/amd64
docker compose config --images | xargs docker save --platform linux/amd64 | gzip | ssh <server> 'gunzip | docker load'
```
