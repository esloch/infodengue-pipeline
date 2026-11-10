"""Export and validate local inputs for the upstream bulletin generator."""

import argparse
import csv
import json
import subprocess  # nosec B404
import sys

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from infodengue_pipeline.config import get_settings

GEOCODE = 2611606
NOTIFICATION_COLUMNS = (
    'municipio_geocodigo',
    'nu_notific',
    'cid10_codigo',
    'ano_notif',
    'se_notif',
    'se_sin_pri',
    'dt_notific',
    'dt_sin_pri',
    'dt_digita',
    'id_bairro',
    'nm_bairro',
)
HISTORY_COLUMNS = (
    'SE',
    'data_iniSE',
    'casos',
    'p_inc100k',
    'nivel',
    'Rt',
    'casos_est',
    'receptivo',
    'tempmin',
)
NOTIFICATION_SQL = text("""
    SELECT municipio_geocodigo, nu_notific::text AS nu_notific,
           cid10_codigo, ano_notif, se_notif, se_sin_pri,
           dt_notific, dt_sin_pri, dt_digita, id_bairro, nm_bairro
    FROM "Municipio"."Notificacao"
    WHERE municipio_geocodigo = :geocode
      AND upper(replace(cid10_codigo, '.', '')) IN ('A90', 'A920')
      AND CASE
        WHEN ano_notif ~ '^[0-9]{4}$' AND se_notif ~ '^[0-9]{1,2}$'
        THEN ano_notif::integer < :year OR
             (ano_notif::integer = :year AND se_notif::integer <= :week)
        ELSE TRUE
      END
    ORDER BY ano_notif, se_notif, nu_notific, cid10_codigo
""")
HISTORY_SQL = text("""
    SELECT "SE", "data_iniSE", casos, p_inc100k, nivel, "Rt", casos_est,
           receptivo, tempmin, pop
    FROM "Municipio"."Historico_alerta"
    WHERE municipio_geocodigo = :geocode AND "SE" <= :last_week
    ORDER BY "SE"
""")
CHIK_HISTORY_SQL = text("""
    SELECT "SE", "data_iniSE", casos, p_inc100k, nivel, "Rt", casos_est,
           receptivo, tempmin, pop
    FROM "Municipio"."Historico_alerta_chik"
    WHERE municipio_geocodigo = :geocode AND "SE" <= :last_week
    ORDER BY "SE"
""")


def _week(value: object) -> int:
    """Validate a Sunday-start epidemiological week without changing it."""
    from epiweeks import Week

    raw = str(value)
    if len(raw) != 6 or not raw.isdecimal():
        raise ValueError(f'invalid epidemiological week: {raw}')
    year, week = int(raw[:4]), int(raw[4:])
    try:
        candidate = Week(year, week, system='cdc')
        if candidate.enddate().year > year + 1:
            raise ValueError
    except ValueError as exc:
        raise ValueError(f'invalid epidemiological week: {raw}') from exc
    return int(raw)


def validate_notifications(rows: list[dict[str, Any]], last_week: int) -> None:
    """Require usable, unique source notifications for the chosen period."""
    if not rows:
        raise ValueError('no Recife notifications for the selected period')
    seen: set[tuple[str, str]] = set()
    latest = 0
    for row in rows:
        missing = set(NOTIFICATION_COLUMNS) - row.keys()
        if missing:
            raise ValueError(
                f'notification columns missing: {sorted(missing)}'
            )
        if int(row['municipio_geocodigo']) != GEOCODE:
            raise ValueError('notification outside Recife')
        cid = str(row['cid10_codigo']).upper().replace('.', '')
        if cid not in {'A90', 'A920'}:
            raise ValueError('unsupported notification disease')
        identifier = str(row['nu_notific'] or '')
        if not identifier:
            raise ValueError('notification identifier missing')
        key = cid, identifier
        if key in seen:
            raise ValueError('duplicate notification identifier per disease')
        seen.add(key)
        year = str(row['ano_notif'] or '')
        week = str(row['se_notif'] or '')
        if len(year) != 4 or not year.isdecimal() or not week.isdecimal():
            raise ValueError('original notification week missing or malformed')
        epiweek = _week(f'{year}{int(week):02d}')
        if epiweek > last_week:
            raise ValueError('notification after reference period')
        latest = max(latest, epiweek)
        if not row['dt_notific']:
            raise ValueError('notification date missing')
    if latest != last_week:
        raise ValueError('no notifications in the reference week')


def validate_history(rows: list[dict[str, Any]], last_week: int) -> int:
    """Require a noninvented municipal alert series and population."""
    if not rows:
        raise ValueError('no Recife dengue alert history')
    weeks: set[int] = set()
    populations: list[int] = []
    for row in rows:
        missing = (set(HISTORY_COLUMNS) | {'pop'}) - row.keys()
        if missing:
            raise ValueError(
                f'alert history columns missing: {sorted(missing)}'
            )
        week = _week(row['SE'])
        if week > last_week or week in weeks:
            raise ValueError('alert history has future or duplicate weeks')
        weeks.add(week)
        if row['casos'] is None or int(row['casos']) < 0:
            raise ValueError('alert history cases missing or negative')
        if row['p_inc100k'] is None or float(row['p_inc100k']) < 0:
            raise ValueError('alert history incidence missing or negative')
        if row['nivel'] is not None and int(row['nivel']) not in range(1, 5):
            raise ValueError('alert history level invalid')
        if row['pop'] is not None and int(row['pop']) > 0:
            populations.append(int(row['pop']))
    if last_week not in weeks:
        raise ValueError('alert history lacks reference week')
    if not populations:
        raise ValueError('alert history lacks municipal population')
    return populations[-1]


def validate_reference(reference: Path) -> Path:
    """Resolve the required non-empty neighborhood reference file."""
    reference = reference.resolve()
    if (
        not reference.is_file()
        or reference.suffix.lower() not in {'.rds', '.rda', '.rdata'}
        or reference.stat().st_size == 0
    ):
        raise ValueError(
            'approved neighborhood sf reference RDS/RData is required'
        )
    return reference


def validate_config(config: Path) -> dict[str, Any]:
    """Load the bulletin settings and require Recife as their municipality."""
    settings = json.loads(config.resolve().read_text(encoding='utf-8'))
    if not isinstance(settings, dict):
        raise ValueError('bulletin configuration must be a JSON object')
    if settings.get('municipio_geocodigo') != GEOCODE:
        raise ValueError('pilot config must select Recife')
    return settings


def load_validated_source_data(
    database_url: str, last_week: int
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    int,
]:
    """Read and validate the same DB inputs consumed by QMD generation."""
    notifications, history, chik_history = export_from_database(
        database_url, last_week
    )
    validate_notifications(notifications, last_week)
    population = validate_history(history, last_week)
    has_chik = any(
        str(row['cid10_codigo']).upper().replace('.', '') == 'A920'
        for row in notifications
    )
    if has_chik:
        validate_history(chik_history, last_week)
    return notifications, history, chik_history, population


def _csv(
    path: Path, rows: list[dict[str, Any]], columns: tuple[str, ...]
) -> None:
    """Write source values without inferring epidemiological dates or weeks."""
    with path.open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in columns})


def _hash(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def export_from_database(
    url: str, last_week: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Take a read-only consistent snapshot from the existing source DB."""
    engine = create_engine(url, poolclass=NullPool, hide_parameters=True)
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'
            )
            params = {
                'geocode': GEOCODE,
                'year': last_week // 100,
                'last_week': last_week,
                'week': last_week % 100,
            }
            notifications = [
                dict(row)
                for row in connection.execute(
                    NOTIFICATION_SQL, params
                ).mappings()
            ]
            history = [
                dict(row)
                for row in connection.execute(HISTORY_SQL, params).mappings()
            ]
            chik_history = [
                dict(row)
                for row in connection.execute(
                    CHIK_HISTORY_SQL, params
                ).mappings()
            ]
            connection.rollback()
        return notifications, history, chik_history
    finally:
        engine.dispose()


def run(
    *,
    week: int,
    reference: Path,
    output_root: Path,
    config: Path,
    database_url: str,
    rscript: str = 'Rscript',
    quarto: str = 'quarto',
    render: bool = True,
) -> Path:
    """Run one isolated Recife bulletin attempt and write its manifest."""
    _week(week)
    reference = validate_reference(reference)
    config = config.resolve()
    output_root = output_root.resolve()
    validate_config(config)
    output_root.mkdir(parents=True, exist_ok=True)
    directory = output_root / f'recife-{week}-{uuid4().hex}'
    directory.mkdir(mode=0o700)
    manifest: dict[str, Any] = {
        'municipality': 'Recife',
        'geocode': GEOCODE,
        'reference_week': week,
        'started_at': datetime.now(timezone.utc).isoformat(),
        'status': 'started',
        'package_version': None,
        'input_hashes': {},
        'outputs': {},
    }
    manifest_path = directory / 'manifest.json'
    try:
        (
            notifications,
            history,
            chik_history,
            population,
        ) = load_validated_source_data(database_url, week)
        has_chik = any(
            str(row['cid10_codigo']).upper().replace('.', '') == 'A920'
            for row in notifications
        )
        notification_path = directory / 'notifications.csv'
        history_path = directory / 'history_dengue.csv'
        chik_path = directory / 'history_chikungunya.csv' if has_chik else None
        _csv(notification_path, notifications, NOTIFICATION_COLUMNS)
        _csv(history_path, history, HISTORY_COLUMNS)
        if chik_path is not None:
            _csv(chik_path, chik_history, HISTORY_COLUMNS)
        manifest['input_hashes'] = {
            'notifications': _hash(notification_path),
            'history_dengue': _hash(history_path),
            'reference': _hash(reference),
            'configuration': _hash(config),
        }
        manifest['row_counts'] = {
            'notifications': len(notifications),
            'history_dengue': len(history),
        }
        manifest['outputs'] = {
            'notifications': str(notification_path),
            'history_dengue': str(history_path),
        }
        if chik_path is not None:
            manifest['input_hashes']['history_chikungunya'] = _hash(chik_path)
            manifest['row_counts']['history_chikungunya'] = len(chik_history)
            manifest['outputs']['history_chikungunya'] = str(chik_path)
        qmd = directory / 'bulletin.qmd'
        command = [
            rscript,
            str(Path(__file__).with_name('generate.R')),
            str(config),
            str(notification_path),
            str(history_path),
            str(chik_path) if chik_path else '',
            str(reference),
            str(qmd),
            str(week // 100),
            str(population),
        ]
        with (directory / 'generate.log').open('x') as log:
            subprocess.run(
                command,
                check=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=300,
            )  # nosec B603
        if not qmd.is_file():
            raise ValueError('R generator did not create QMD')
        manifest['package_version'] = (
            (directory / 'package-version.txt').read_text().strip()
        )
        manifest['outputs']['qmd'] = str(qmd)
        if render:
            with (directory / 'render.log').open('x') as log:
                subprocess.run(
                    [quarto, 'render', str(qmd), '--to', 'pdf'],
                    cwd=directory,
                    check=True,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    timeout=3600,
                )  # nosec B603
            pdf = qmd.with_suffix('.pdf')
            if not pdf.is_file() or pdf.stat().st_size == 0:
                raise ValueError('Quarto did not create a nonempty PDF')
            manifest['outputs']['pdf'] = str(pdf)
        manifest['status'] = 'succeeded'
    except Exception as exc:
        manifest['status'] = 'failed'
        manifest['error_type'] = type(exc).__name__
        raise
    finally:
        manifest['finished_at'] = datetime.now(timezone.utc).isoformat()
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + '\n', encoding='utf-8'
        )
    return manifest_path


def main() -> None:
    """Run the isolated manual pilot, never using production settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--week', type=int, required=True, help='reference week YYYYWW'
    )
    parser.add_argument(
        '--reference',
        type=Path,
        required=True,
        help='approved neighborhood sf RDS/RData',
    )
    parser.add_argument(
        '--output-root', type=Path, default=Path('.local/bulletins')
    )
    parser.add_argument(
        '--config', type=Path, default=Path('configs/bulletin/recife.json')
    )
    parser.add_argument('--qmd-only', action='store_true')
    args = parser.parse_args()
    settings = get_settings()
    if settings.environment != 'dev':
        parser.error('bulletin pilot is development only')
    if settings.alerta_source_database_url is None:
        parser.error(
            'configure INFODENGUE_PIPELINE_ALERTA_SOURCE_DATABASE_URL'
        )
    try:
        manifest = run(
            week=args.week,
            reference=args.reference,
            output_root=args.output_root,
            config=args.config,
            database_url=settings.alerta_source_database_url.get_secret_value(),
            render=not args.qmd_only,
        )
    except Exception as exc:
        print(f'bulletin failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        raise SystemExit(1) from exc
    print(manifest)


if __name__ == '__main__':
    main()
