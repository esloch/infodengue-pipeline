"""Fixed network identity of the isolated scientific rehearsal database."""

ANALYSIS_E2E_HOST = 'analysis-e2e-db'
ANALYSIS_E2E_DATABASE = 'analysis_e2e'
ANALYSIS_E2E_ROLE = 'analysis_e2e_writer'

RESTORE_EVIDENCE_SQL = (
    'SELECT backup_sha256 FROM rehearsal.restore_evidence '
    'WHERE source_environment = :environment '
    'AND restored_database = current_database()'
)
