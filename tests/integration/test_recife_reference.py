"""Exercise the real sf reference preparation boundary when R is available."""

import csv
import hashlib
import json
import shutil
import subprocess

from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / 'scripts'
    / ('prepare_recife_reference.R')
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.skipif(shutil.which('Rscript') is None, reason='R unavailable')
def test_reference_requires_exact_reviewed_mapping(tmp_path: Path) -> None:
    """Accept valid polygons and reject duplicate official mapping keys."""
    available = subprocess.run(
        [
            'Rscript',
            '-e',
            'quit(status=as.integer(!all(vapply('
            'c("sf","digest","jsonlite"), requireNamespace, '
            'logical(1), quietly=TRUE))))',
        ],
        capture_output=True,
        check=False,
    )
    if available.returncode:
        pytest.skip('sf, digest or jsonlite unavailable')
    shape = tmp_path / 'neighborhoods.geojson'
    create = (
        'x <- sf::st_sf(bairro=c("A", "B"), '
        'geometry=sf::st_sfc('
        'sf::st_polygon(list(rbind(c(0,0),c(1,0),c(1,1),c(0,0)))),'
        'sf::st_polygon(list(rbind(c(2,0),c(3,0),c(3,1),c(2,0)))),'
        'crs=4326)); sf::st_write(x, commandArgs(TRUE)[1], quiet=TRUE)'
    )
    subprocess.run(['Rscript', '-e', create, str(shape)], check=True)
    mapping = tmp_path / 'mapping.csv'
    with mapping.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream)
        writer.writerows(
            [
                ['nm_bairro', 'id_bairro', 'distrito', 'pop'],
                ['A', 1, 'D1', 100],
                ['B', 2, 'D2', 200],
            ]
        )
    manifest = {
        'geometry_file': str(shape),
        'geometry_sha256': _sha(shape),
        'mapping_file': str(mapping),
        'mapping_sha256': _sha(mapping),
        'name_column': 'bairro',
        'source_url': 'official.example/test',
        'source_version': 'reviewed',
        'population_source': 'approved census',
        'population_year': 2022,
        'approved_by': 'reviewer',
        'expected_neighborhoods': 2,
    }
    manifest_path = tmp_path / 'manifest.json'
    output = tmp_path / 'reference.rds'
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    run = subprocess.run(
        ['Rscript', str(SCRIPT), str(manifest_path), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert run.returncode == 0, run.stderr
    assert output.stat().st_size > 0
    assert (tmp_path / 'reference.rds.provenance.json').is_file()
    with mapping.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.writer(stream)
        writer.writerows(
            [
                ['nm_bairro', 'id_bairro', 'distrito', 'pop'],
                ['A', 1, 'D1', 100],
                ['A', 2, 'D2', 200],
            ]
        )
    manifest['mapping_sha256'] = _sha(mapping)
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    rejected = subprocess.run(
        ['Rscript', str(SCRIPT), str(manifest_path), str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected.returncode != 0
    assert 'duplicate' in rejected.stderr
