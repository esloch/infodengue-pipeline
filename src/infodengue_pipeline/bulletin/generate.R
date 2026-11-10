args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 8L) stop('Expected config, notifications, histories, reference, QMD, year and population')
if (!requireNamespace('infodengueintramunicipal', quietly = TRUE)) {
  stop('infodengueintramunicipal is not installed')
}
cfg <- jsonlite::fromJSON(args[[1]], simplifyVector = FALSE)
ref <- args[[5]]
infodengueintramunicipal::preparar_referencia(
  ref, cfg$reference_columns$neighborhood, cfg$reference_columns$id,
  cfg$reference_columns$district, cfg$reference_columns$population,
  cfg$reference_columns$geometry
)
writeLines(as.character(utils::packageVersion('infodengueintramunicipal')),
           file.path(dirname(args[[6]]), 'package-version.txt'))
infodengueintramunicipal::criar_qmd_boletim(
  municipio_nome = 'Recife', uf = 'PE', municipio_geocodigo = 2611606,
  ano = as.integer(args[[7]]), populacao = as.integer(args[[8]]),
  output_qmd = args[[6]], notificacoes_path = args[[2]],
  historico_alerta_dengue_path = args[[3]],
  historico_alerta_chikungunya_path = args[[4]],
  referencia_path = ref,
  col_bairro_ref = cfg$reference_columns$neighborhood,
  col_bairro_id_ref = cfg$reference_columns$id,
  col_distrito_ref = cfg$reference_columns$district,
  col_pop_ref = cfg$reference_columns$population,
  col_geometry_ref = cfg$reference_columns$geometry,
  limiar_preseason = cfg$thresholds$preseason,
  limiar_epidemico = cfg$thresholds$epidemic,
  limiar_posseason = cfg$thresholds$postseason,
  config_climatica = cfg$climate,
  executar_nowcasting = FALSE, render = FALSE, overwrite = FALSE
)
