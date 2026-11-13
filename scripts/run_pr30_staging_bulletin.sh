#!/usr/bin/env bash
# Operator-only: render a bulletin against the isolated PR30 staging network.
set -euo pipefail
umask 077
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

: "${BULLETIN_WEEK:?set selected CDC epiweek YYYYWW}"
: "${BULLETIN_ENV_FILE:?set path to read-only source and history URL env file}"
: "${EXPECTED_IMAGE_ID:?set reviewed bulletin image ID from docker image inspect}"

reference=/opt/data/staging/pr30-rehearsal/geo/recife_bairros.rds
output=/opt/data/staging/pr30-rehearsal/bulletins
image=infodengue-pipeline-bulletin:dev
network=infodengue-staging_infodengue

[[ "$BULLETIN_WEEK" =~ ^[0-9]{6}$ ]] || {
  echo 'BULLETIN_WEEK must be YYYYWW' >&2; exit 2;
}
[[ -s "$reference" && -r "$reference" ]] || {
  echo 'Reviewed Recife RDS is missing or unreadable' >&2; exit 2;
}
[[ -r "$BULLETIN_ENV_FILE" ]] || {
  echo 'Read-only URL env file is missing or unreadable' >&2; exit 2;
}
for key in INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL INFODENGUE_PIPELINE_BULLETIN_HISTORY_DATABASE_URL; do
  grep -q "^${key}=" "$BULLETIN_ENV_FILE" || {
    echo "Missing $key in env file" >&2; exit 2;
  }
done
python3 "$repo_root/scripts/validate_pr30_reference.py" "$reference"
actual_image=$(docker image inspect --format '{{.Id}}' "$image")
[[ "$actual_image" == "$EXPECTED_IMAGE_ID" ]] || {
  echo 'Bulletin image ID mismatch' >&2; exit 2;
}
# Inspect RDS structure in the disposable image before opening database URLs.
docker run --rm --network none --read-only --tmpfs /tmp \
  --mount "type=bind,src=$reference,dst=/reference/recife_bairros.rds,readonly" \
  "$image" Rscript -e 'x <- readRDS("/reference/recife_bairros.rds"); stopifnot(inherits(x, "sf"), nrow(x) == 94, all(c("nm_bairro", "id_bairro", "distrito", "pop", "geometry") %in% names(x)), !anyNA(x[, c("nm_bairro", "id_bairro", "distrito", "pop")]), length(unique(x$nm_bairro)) == 94, length(unique(x$id_bairro)) == 94, length(unique(x$distrito)) == 6, all(x$pop > 0), sum(x$pop) == 1540874, !is.na(sf::st_crs(x)), all(!sf::st_is_empty(x)), all(sf::st_is_valid(x)), all(as.character(sf::st_geometry_type(x)) %in% c("POLYGON", "MULTIPOLYGON")))'
docker network inspect "$network" >/dev/null
[[ -d "$output" && -w "$output" ]] || {
  echo 'Staging bulletin output directory is missing or unwritable' >&2; exit 2;
}

docker run --rm --network "$network" --env-file "$BULLETIN_ENV_FILE" \
  -e INFODENGUE_PIPELINE_ENVIRONMENT=dev \
  --mount "type=bind,src=$reference,dst=/reference/recife_bairros.rds,readonly" \
  --mount "type=bind,src=$output,dst=/output" \
  "$image" python -m infodengue_pipeline.bulletin.runner \
  --week "$BULLETIN_WEEK" --reference /reference/recife_bairros.rds \
  --output-root /output
