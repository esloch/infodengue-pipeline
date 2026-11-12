#!/usr/bin/env Rscript
# Prepare one reviewed Recife sf reference. No geocoding or fuzzy matching.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L) {
  stop('usage: Rscript prepare_recife_reference.R MANIFEST.json OUTPUT.rds')
}
if (!requireNamespace('sf', quietly = TRUE) ||
    !requireNamespace('digest', quietly = TRUE) ||
    !requireNamespace('jsonlite', quietly = TRUE)) {
  stop('sf, digest and jsonlite are required')
}
manifest <- jsonlite::fromJSON(args[[1]])
required <- c('geometry_file', 'geometry_sha256', 'mapping_file',
              'mapping_sha256', 'name_column', 'source_url',
              'source_version', 'population_source', 'population_year',
              'approved_by', 'expected_neighborhoods')
if (any(!required %in% names(manifest)) ||
    any(vapply(manifest[required], function(x) length(x) != 1L || is.na(x) ||
                 !nzchar(as.character(x)), logical(1)))) {
  stop('manifest has missing provenance or input fields')
}
verify_file <- function(path, expected) {
  if (!file.exists(path) || dir.exists(path)) stop('input file missing: ', path)
  if (!grepl('^[0-9a-f]{64}$', expected) ||
      digest::digest(file = path, algo = 'sha256') != expected) {
    stop('SHA-256 mismatch: ', path)
  }
}
verify_file(manifest$geometry_file, manifest$geometry_sha256)
verify_file(manifest$mapping_file, manifest$mapping_sha256)
geo <- sf::st_read(manifest$geometry_file, quiet = TRUE,
                   options = 'ENCODING=UTF-8')
if (!inherits(geo, 'sf') || nrow(geo) != manifest$expected_neighborhoods ||
    is.na(sf::st_crs(geo)) ||
    !manifest$name_column %in% names(geo)) {
  stop('unexpected geometry count, CRS or neighborhood field')
}
types <- as.character(sf::st_geometry_type(geo))
if (any(!types %in% c('POLYGON', 'MULTIPOLYGON')) ||
    any(sf::st_is_empty(geo)) || any(!sf::st_is_valid(geo))) {
  stop('empty, invalid or non-polygon neighborhood geometry')
}
geo$nm_bairro <- trimws(enc2utf8(as.character(
  geo[[manifest$name_column]])))
if (anyNA(geo$nm_bairro) || any(!nzchar(geo$nm_bairro)) ||
    anyDuplicated(geo$nm_bairro)) {
  stop('missing or duplicate geographic neighborhood name')
}
mapping <- read.csv(manifest$mapping_file, fileEncoding = 'UTF-8',
                    stringsAsFactors = FALSE, check.names = FALSE)
if (!all(c('nm_bairro', 'id_bairro', 'distrito', 'pop') %in%
         names(mapping))) stop('mapping requires nm_bairro,id_bairro,distrito,pop')
mapping$nm_bairro <- trimws(enc2utf8(mapping$nm_bairro))
if (nrow(mapping) != nrow(geo) || anyNA(mapping) ||
    anyDuplicated(mapping$nm_bairro) ||
    anyDuplicated(mapping$id_bairro) ||
    any(!nzchar(mapping$nm_bairro)) ||
    any(!nzchar(as.character(mapping$distrito))) ||
    any(!nzchar(as.character(mapping$id_bairro))) ||
    any(is.na(suppressWarnings(as.numeric(mapping$pop)))) ||
    any(as.numeric(mapping$pop) <= 0)) {
  stop('mapping has missing, duplicate or invalid attributes')
}
order <- match(geo$nm_bairro, mapping$nm_bairro)
if (anyNA(order) || any(!mapping$nm_bairro %in% geo$nm_bairro)) {
  stop('mapping does not exactly cover the official neighborhoods')
}
geo$id_bairro <- mapping$id_bairro[order]
geo$distrito <- mapping$distrito[order]
geo$pop <- as.numeric(mapping$pop[order])
out <- geo[, c('nm_bairro', 'id_bairro', 'distrito', 'pop',
               attr(geo, 'sf_column'))]
names(out)[names(out) == attr(geo, 'sf_column')] <- 'geometry'
sf::st_geometry(out) <- 'geometry'
saveRDS(out, args[[2]])
readback <- readRDS(args[[2]])
if (!inherits(readback, 'sf') || nrow(readback) != nrow(geo) ||
    anyNA(readback[, c('nm_bairro', 'id_bairro', 'distrito', 'pop')]) ||
    any(!sf::st_is_valid(readback))) {
  stop('RDS read-back validation failed')
}
provenance <- list(source_url = manifest$source_url,
                   source_version = manifest$source_version,
                   geometry_sha256 = manifest$geometry_sha256,
                   mapping_sha256 = manifest$mapping_sha256,
                   population_source = manifest$population_source,
                   population_year = manifest$population_year,
                   approved_by = manifest$approved_by,
                   transformations = c('UTF-8 trim', 'exact name join'),
                   neighborhoods = nrow(readback),
                   output_sha256 = digest::digest(file = args[[2]],
                                                   algo = 'sha256'))
jsonlite::write_json(provenance, paste0(args[[2]], '.provenance.json'),
                     auto_unbox = TRUE, pretty = TRUE)
