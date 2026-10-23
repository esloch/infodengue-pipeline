# Issue #9: staging-host production-profile rehearsal

Run these phases manually **on the staging host**, from the reviewed
`feat/prod-deployment-validation-9` checkout. Stop at a failed gate. Keep the
real production server and resources untouched. Do not upload SINAN objects,
start a second watcher, or use `down -v`. Keep the private env files untracked.
Commands that inspect rendered Compose JSON can contain secrets internally;
do not paste that JSON or private env files into tickets or logs.

## 1. Establish the host and existing state

```sh
pwd
git status --short --branch
docker compose version
docker network inspect infodengue-staging_infodengue --format '{{.Name}}'
docker ps --filter label=com.docker.compose.project=infodengue-pipeline-prod \
  --format '{{.Names}} {{.Status}}'
docker volume ls --filter name=infodengue-pipeline-prod
docker image inspect infodengue-pipeline:staging \
  infodengue-pipeline-analysis:staging \
  --format '{{.RepoTags}} {{.Id}} {{json .RepoDigests}}'
test -d /opt/data/staging/sinan/incoming
test ! -e /opt/data/staging/prod-analysis-output || \
  stat -c '%n %u:%g %a' /opt/data/staging/prod-analysis-output
```

Confirm the checkout, network, and both **already built** image tags. If an
image has another tag, substitute its inspected tag in phase 2. Stop if the
production-profile project or volumes already exist until their ownership and
state are reviewed. Confirm that loopback ports 5436 and 5675 are free with
`ss -ltn '( sport = :5436 or sport = :5675 )'`.

## 2. Obtain pullable immutable image references

`docker image inspect .Id` is the image configuration digest. It is not a
pullable OCI manifest reference. First inspect `RepoDigests` from phase 1. If
both images already have accessible `repository@sha256:<64 lowercase hex>`
references, use those and verify their image IDs below. Otherwise, publish
the unchanged local image content to a **staging-local** registry. A new tag
is added to each existing image; the original tags and image bytes remain.

```sh
test -z "$(docker ps -aq --filter name='^/infodengue-issue9-registry$')"
docker run -d --name infodengue-issue9-registry --restart no \
  -p 127.0.0.1:5001:5000 \
  -v infodengue_issue9_registry:/var/lib/registry registry:2
CORE_ID=$(docker image inspect -f '{{.Id}}' infodengue-pipeline:staging)
ANALYSIS_ID=$(docker image inspect -f '{{.Id}}' infodengue-pipeline-analysis:staging)
docker tag infodengue-pipeline:staging \
  127.0.0.1:5001/infodengue-pipeline:issue9
docker tag infodengue-pipeline-analysis:staging \
  127.0.0.1:5001/infodengue-pipeline-analysis:issue9
docker push 127.0.0.1:5001/infodengue-pipeline:issue9
docker push 127.0.0.1:5001/infodengue-pipeline-analysis:issue9
docker image inspect -f '{{range .RepoDigests}}{{println .}}{{end}}' \
  127.0.0.1:5001/infodengue-pipeline:issue9
docker image inspect -f '{{range .RepoDigests}}{{println .}}{{end}}' \
  127.0.0.1:5001/infodengue-pipeline-analysis:issue9
```

Copy the two matching `127.0.0.1:5001/...@sha256:...` references, then verify
the digest references resolve to the original image IDs:

```sh
CORE_REF='PASTE_VERIFIED_CORE_REPOSITORY_AT_DIGEST'
ANALYSIS_REF='PASTE_VERIFIED_ANALYSIS_REPOSITORY_AT_DIGEST'
docker pull "$CORE_REF"
docker pull "$ANALYSIS_REF"
test "$(docker image inspect -f '{{.Id}}' "$CORE_REF")" = "$CORE_ID"
test "$(docker image inspect -f '{{.Id}}' "$ANALYSIS_REF")" = "$ANALYSIS_ID"
```

For already available `RepoDigests`, set `CORE_ID` and `ANALYSIS_ID` from the
inspected original tags, set the two references, and run only the final four
verification commands. Keep the local registry running until teardown if it
was used. If the registry image cannot be obtained or the digest pull fails,
stop and arrange an accessible staging-only OCI registry; never substitute an
image ID or a mutable tag.

## 3. Prove rehearsal MinIO isolation

Choose a **staging-only** bucket and a new prefix such as
`issue9-prod-profile/`. Verify the staging MinIO endpoint and bucket with the
staging administrator. The prefix must be empty of objects and outside every
existing consumer's watched namespace. Inspect all Docker containers'
non-secret MinIO routing settings:

```sh
docker ps -aq | xargs -r docker inspect | python3 -c '
import json, sys
for c in json.load(sys.stdin):
    env = dict(e.split("=", 1) for e in c["Config"].get("Env", []) if "=" in e)
    keys = ("INFODENGUE_PIPELINE_MINIO_ENDPOINT", "INFODENGUE_PIPELINE_MINIO_BUCKET", "INFODENGUE_PIPELINE_MINIO_PREFIX")
    if any(k in env for k in keys):
        print(c["Name"], c["State"]["Status"], c["HostConfig"]["RestartPolicy"]["Name"], *(env.get(k, "<unset>") for k in keys))
'
```

Also inspect any host services and other ingress/mirror consumers that read
this MinIO bucket; Docker environment alone cannot prove absence. For a
consumer using the same endpoint and bucket, its listed prefix must neither
contain nor be contained by the rehearsal prefix. An empty or broad prefix is
an overlap. Use the staging MinIO administrator's listing tool to verify no
objects exist under the new prefix. If any consumer or mirror can process it,
provision a dedicated **staging** bucket and prefix or stop that consumer only
through its normal operator procedure. Do not start phase 6 until isolation is
positively established. The read-only validator checks bucket access, not
consumer isolation or an empty prefix.

## 4. Provision private configuration and directory

```sh
cp -n .env.prod.example .env.prod
cp -n .env.prod.analysis.example .env.prod.analysis
chmod 600 .env.prod .env.prod.analysis
${EDITOR:-vi} .env.prod .env.prod.analysis
```

Set the following in `.env.prod`: `PIPELINE_WORKER_ENV_FILE=../.env.prod`,
`PIPELINE_ANALYSIS_ENV_FILE=../.env.prod.analysis`, `PIPELINE_IMAGE=$CORE_REF`,
`PIPELINE_ANALYSIS_IMAGE=$ANALYSIS_REF`,
`PROD_INTEGRATION_NETWORK=infodengue-staging_infodengue`,
`PROD_SINAN_INCOMING_HOST_PATH=/opt/data/staging/sinan/incoming`,
`PROD_ANALYSIS_OUTPUT_HOST_PATH=/opt/data/staging/prod-analysis-output`,
`PIPELINE_POSTGRES_HOST_PORT=5436`, and `PIPELINE_RABBITMQ_HOST_PORT=5675`.
Use distinct new `PIPELINE_POSTGRES_PASSWORD` and
`PIPELINE_RABBITMQ_PASSWORD`. Their matching URLs must use
`pipeline-postgres:5432/pipeline_prod` and `pipeline-rabbitmq:5672//` with
user `pipeline_prod`. Set `INFODENGUE_PIPELINE_ENVIRONMENT=prod`,
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=false`, and
`INFODENGUE_PIPELINE_ANALYSIS_IMAGE=$ANALYSIS_REF` as literal values (the env
file does not expand shell variables). Use only the verified staging MinIO
endpoint, bucket, unique prefix and a dedicated runtime credential able to
perform the validator's bucket existence check and read/list within its
authorized scope. Set `INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL` to a
staging AlertaDengue read-only role. Keep the template's container paths.

In `.env.prod.analysis`, set the matching pipeline PostgreSQL and RabbitMQ
URLs, `INFODENGUE_PIPELINE_ENVIRONMENT=prod`,
`INFODENGUE_PIPELINE_ANALYSIS_LOAD=false`, and
`INFODENGUE_PIPELINE_ANALYSIS_IMAGE=$ANALYSIS_REF` as literal values. Set
`INFODENGUE_PIPELINE_ANALYSIS_DATABASE_URL` to a distinct read-only staging
analysis role. Do not reuse production or staging-profile pipeline state.
Have the staging database administrator verify both external roles lack write
grants; the validator only checks connectivity.

Determine the analysis image's actual runtime UID and GID without running its
application, then create only the dedicated empty output directory:

```sh
ANALYSIS_UID=$(docker run --rm --network none --entrypoint id "$ANALYSIS_REF" -u)
ANALYSIS_GID=$(docker run --rm --network none --entrypoint id "$ANALYSIS_REF" -g)
if test ! -e /opt/data/staging/prod-analysis-output; then
  sudo install -d -m 0750 -o "$ANALYSIS_UID" -g "$ANALYSIS_GID" \
    /opt/data/staging/prod-analysis-output
else
  test ! -L /opt/data/staging/prod-analysis-output
  test -d /opt/data/staging/prod-analysis-output
  test -z "$(find /opt/data/staging/prod-analysis-output -mindepth 1 -maxdepth 1 -print -quit)"
  sudo chown "$ANALYSIS_UID:$ANALYSIS_GID" /opt/data/staging/prod-analysis-output
  sudo chmod 0750 /opt/data/staging/prod-analysis-output
fi
docker run --rm --network none --user "$ANALYSIS_UID:$ANALYSIS_GID" \
  -v /opt/data/staging/prod-analysis-output:/out \
  --entrypoint /bin/sh "$ANALYSIS_REF" -c 'test -d /out && test -w /out && test -x /out'
```

The last command checks permissions as the image user without creating a
file. The production validator repeats this check in the running worker.
Inspect `stat` and resolve any UID-mapping or ACL policy before startup. Do
not alter the regular staging analysis output path.

## 5. Static validation and final gate

```sh
uv run makim containers.prod-config
docker compose --env-file .env.prod -f containers/compose-prod.yaml config --quiet
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  config --images
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  config --services
```

Confirm both application images are the verified digest references, the
network and binds are staging-only, both private files contain no placeholders,
all five worker environments say `prod` and `analysis_load=false`, and the
rehearsal prefix passed phase 3. Do not print `docker compose config` without
`--quiet`, `--images`, or `--services`: full output includes secrets.

## 6. Start and inspect only the production-profile project

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml \
  up -d --no-build --pull never
docker compose --env-file .env.prod -f containers/compose-prod.yaml ps --all
```

Require healthy PostgreSQL, RabbitMQ, and all five workers. `migrate` must
have exited with code zero. Stop on an unexpected state; do not submit an
object or manually retry scientific work.

## 7. Run the acceptance probe

```sh
uv run python scripts/validate_prod.py
```

Record the sanitized output and exit status. Success ends with `production
validation passed`; check the named PostgreSQL/migrations, RabbitMQ, MinIO,
source DB, analysis DB, incoming bind, output permissions, worker health,
concurrency, image identity, and `analysis_load: false` lines. The command
does not prove source/analysis role grants or prefix isolation; those are
phase 3 and 4 operator gates.

## 8. Safe teardown

```sh
docker compose --env-file .env.prod -f containers/compose-prod.yaml down
docker ps --filter label=com.docker.compose.project=infodengue-pipeline-prod \
  --format '{{.Names}} {{.Status}}'
```

Keep the production-profile PostgreSQL/RabbitMQ volumes, all ordinary staging
containers and volumes, and the private env files for reviewed follow-up. If
phase 2 created the dedicated registry and the profile is down, stop and
remove only that named registry container with
`docker stop infodengue-issue9-registry` and
`docker rm infodengue-issue9-registry`. Keep its dedicated volume until an
operator decides whether the digest references must be reusable. Never use
`down -v` or remove existing staging images.
