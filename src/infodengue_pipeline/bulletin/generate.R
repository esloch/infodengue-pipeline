args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 8L) stop('Expected config, notifications, histories, reference, QMD, year and population')
if (!requireNamespace('infodengueintramunicipal', quietly = TRUE)) {
  stop('infodengueintramunicipal is not installed')
}
config <- jsonlite::fromJSON(args[[1]], simplifyVector = FALSE)
reference <- args[[5]]
infodengueintramunicipal::preparar_referencia(
  reference, config$reference_columns$neighborhood,
  config$reference_columns$id, config$reference_columns$district,
  config$reference_columns$population, config$reference_columns$geometry
)
writeLines(as.character(utils::packageVersion('infodengueintramunicipal')),
           file.path(dirname(args[[6]]), 'package-version.txt'))
infodengueintramunicipal::criar_qmd_boletim(
  municipio_nome = 'Recife', uf = 'PE',
  municipio_geocodigo = config$geocode,
  ano = as.integer(args[[7]]), populacao = as.integer(args[[8]]),
  output_qmd = args[[6]], notificacoes_path = args[[2]],
  historico_alerta_dengue_path = args[[3]],
  historico_alerta_chikungunya_path = args[[4]],
  referencia_path = reference,
  col_bairro_ref = config$reference_columns$neighborhood,
  col_bairro_id_ref = config$reference_columns$id,
  col_distrito_ref = config$reference_columns$district,
  col_pop_ref = config$reference_columns$population,
  col_geometry_ref = config$reference_columns$geometry,
  limiar_preseason = config$thresholds$preseason,
  limiar_epidemico = config$thresholds$epidemic,
  limiar_posseason = config$thresholds$postseason,
  config_climatica = config$climate,
  executar_nowcasting = FALSE, render = FALSE, overwrite = FALSE
)
