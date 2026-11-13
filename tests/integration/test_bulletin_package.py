"""Container integration check for the pinned upstream bulletin package."""

import os
import re
import shutil
import subprocess
import zlib

from pathlib import Path

import pytest

IMAGE = 'infodengue-pipeline-bulletin:dev'
OPT_IN = 'RUN_BULLETIN_CONTAINER_INTEGRATION'


def _stream_at(data: bytes, offset: int) -> tuple[bytes, bytes]:
    """Read a PDF stream at an indirect-object offset."""
    header = re.match(rb'\d+\s+\d+\s+obj\b', data[offset:])
    assert header is not None, 'PDF cross-reference points outside an object'
    stream_marker = re.search(rb'\bstream\r?\n', data[offset + header.end() :])
    assert stream_marker is not None, 'PDF stream object has no stream payload'
    dictionary_end = offset + header.end() + stream_marker.start()
    payload_start = offset + header.end() + stream_marker.end()
    length = re.search(rb'/Length\s+(\d+)\b', data[offset:dictionary_end])
    if length is not None:
        payload_end = payload_start + int(length.group(1))
        assert payload_end <= len(data), 'PDF stream payload is truncated'
        payload = data[payload_start:payload_end]
    else:
        end_marker = data.find(b'endstream', payload_start)
        assert end_marker >= 0, 'PDF stream has no endstream marker'
        payload = data[payload_start:end_marker].rstrip(b'\r\n')
    dictionary = data[offset + header.end() : dictionary_end]
    if re.search(rb'/Filter\s*/FlateDecode\b', dictionary):
        payload = zlib.decompress(payload)
    return dictionary, payload


def _xref_entries(data: bytes) -> dict[int, tuple[int, ...]]:
    """Decode the final PDF cross-reference stream."""
    marker = re.search(rb'startxref\s+(\d+)\s+%%EOF\s*$', data, re.DOTALL)
    assert marker is not None, 'PDF has no valid startxref/EOF trailer'
    offset = int(marker.group(1))
    dictionary, decoded = _stream_at(data, offset)
    assert re.search(rb'/Type\s*/XRef\b', dictionary), (
        'PDF startxref does not point to a cross-reference stream'
    )
    widths_match = re.search(rb'/W\s*\[([^]]+)\]', dictionary)
    size_match = re.search(rb'/Size\s+(\d+)\b', dictionary)
    assert widths_match is not None and size_match is not None, (
        'PDF cross-reference stream metadata is invalid'
    )
    widths = [int(value) for value in widths_match.group(1).split()]
    ranges_match = re.search(rb'/Index\s*\[([^]]+)\]', dictionary)
    ranges = (
        [int(value) for value in ranges_match.group(1).split()]
        if ranges_match is not None
        else [0, int(size_match.group(1))]
    )
    assert len(widths) == 3 and len(ranges) % 2 == 0, (
        'PDF cross-reference stream has unsupported dimensions'
    )
    entry_size = sum(widths)
    entries: dict[int, tuple[int, ...]] = {}
    cursor = 0
    for start, count in zip(ranges[::2], ranges[1::2]):
        for object_number in range(start, start + count):
            raw = decoded[cursor : cursor + entry_size]
            assert len(raw) == entry_size, (
                'PDF cross-reference data is truncated'
            )
            values: list[int] = []
            field_cursor = 0
            for width in widths:
                value = int.from_bytes(
                    raw[field_cursor : field_cursor + width], 'big'
                )
                values.append(value)
                field_cursor += width
            if widths[0] == 0:
                values[0] = 1
            entries[object_number] = tuple(values)
            cursor += entry_size
    return entries


def _object_body(
    data: bytes, object_number: int, entries: dict[int, tuple[int, ...]]
) -> bytes:
    """Return a direct or compressed indirect PDF object's body."""
    pattern = rb'(?m)^' + str(object_number).encode() + rb'\s+\d+\s+obj\b'
    match = re.search(pattern + rb'(.*?)\bendobj\b', data, re.DOTALL)
    if match is not None:
        return match.group(1)

    entry = entries.get(object_number)
    assert entry is not None and entry[0] == 2, (
        f'PDF object {object_number} is missing from its cross-reference'
    )
    object_stream = entries.get(entry[1])
    assert object_stream is not None and object_stream[0] == 1, (
        'PDF compressed object has an invalid object-stream reference'
    )
    dictionary, decoded = _stream_at(data, object_stream[1])
    count = re.search(rb'/N\s+(\d+)\b', dictionary)
    first = re.search(rb'/First\s+(\d+)\b', dictionary)
    assert count is not None and first is not None, (
        'PDF object stream metadata is invalid'
    )
    pairs = [int(value) for value in decoded[: int(first.group(1))].split()]
    number_of_objects = int(count.group(1))
    assert len(pairs) >= number_of_objects * 2, (
        'PDF object stream is truncated'
    )
    index = entry[2]
    assert index < number_of_objects and pairs[index * 2] == object_number, (
        f'PDF compressed object {object_number} has an invalid object index'
    )
    object_start = int(first.group(1)) + pairs[index * 2 + 1]
    object_end = (
        int(first.group(1)) + pairs[(index + 1) * 2 + 1]
        if index + 1 < number_of_objects
        else len(decoded)
    )
    return decoded[object_start:object_end]


def _validate_pdf(path: Path) -> None:
    """Check the PDF header, EOF, cross-reference and catalog/page tree."""
    data = path.read_bytes()
    assert len(data) > 1000, 'PDF is unexpectedly small'
    assert re.match(rb'%PDF-\d\.\d', data[:16]), 'PDF header is invalid'
    startxref = re.search(rb'startxref\s+(\d+)\s+%%EOF\s*$', data, re.DOTALL)
    assert startxref is not None, 'PDF has no valid startxref/EOF trailer'
    xref_offset = int(startxref.group(1))
    assert xref_offset < startxref.start(), 'PDF startxref is out of range'
    xref_entries = _xref_entries(data)
    xref_dictionary, _ = _stream_at(data, xref_offset)
    root = re.search(rb'/Root\s+(\d+)\s+\d+\s+R', xref_dictionary)
    assert root is not None, 'PDF cross-reference has no document catalog'
    catalog = _object_body(data, int(root.group(1)), xref_entries)
    assert re.search(rb'/Type\s*/Catalog\b', catalog), 'PDF catalog is invalid'
    pages_ref = re.search(rb'/Pages\s+(\d+)\s+\d+\s+R', catalog)
    assert pages_ref is not None, 'PDF catalog has no page tree'
    pages = _object_body(data, int(pages_ref.group(1)), xref_entries)
    assert re.search(rb'/Type\s*/Pages\b', pages), 'PDF page tree is invalid'
    page_count = re.search(rb'/Count\s+(\d+)\b', pages)
    assert page_count is not None and int(page_count.group(1)) > 0, (
        'PDF contains no pages'
    )


def test_upstream_package_generates_synthetic_qmd_and_pdf(
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    """Run the synthetic fixture through the bulletin image's R and Quarto."""
    if os.environ.get(OPT_IN) != '1':
        pytest.skip(f'set {OPT_IN}=1 to require the Docker integration test')

    docker = shutil.which('docker')
    if docker is None:
        pytest.fail(
            'Docker integration requested but the docker CLI is missing'
        )

    daemon_check = subprocess.run(
        [docker, 'info'],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    if daemon_check.returncode:
        pytest.fail(
            'Docker integration requested but the Docker daemon is '
            'unavailable.\n'
            f'{daemon_check.stderr.strip()}'
        )

    image_check = subprocess.run(
        [docker, 'image', 'inspect', IMAGE],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    if image_check.returncode:
        pytest.fail(
            f'Docker integration requested but image {IMAGE!r} is '
            'unavailable; '
            'build it with containers/compose-bulletin.yaml first.\n'
            f'{image_check.stderr.strip()}'
        )

    artifacts = tmp_path / 'bulletin-artifacts'
    artifacts.mkdir()
    request.addfinalizer(lambda: shutil.rmtree(artifacts, ignore_errors=True))
    script = artifacts / 'generate_synthetic.R'
    script.write_text(
        r"""stopifnot(
  Sys.getenv("INTRAMUNICIPAL_REVISION") ==
    "fe82d1d5f6caf0a91aca585f0385cf1abea2e953"
)
stopifnot(
  requireNamespace("infodengueintramunicipal", quietly=TRUE)
)
stopifnot(identical(system2("quarto", "--version", stdout=TRUE), "1.11.9"))
stopifnot(as.character(packageVersion("infodengueintramunicipal")) == "0.1.1")
ns <- asNamespace("infodengueintramunicipal")
stopifnot(
  "criar_qmd_boletim" %in%
    getNamespaceExports("infodengueintramunicipal")
)
stopifnot(is.function(get("criar_qmd_boletim", envir=ns)))
base <- system.file(
  "extdata", "feira_livre", package="infodengueintramunicipal"
)
files <- c("shape/feira_livre_bairros.shp", "shape/feira_livre_bairros.shx",
           "shape/feira_livre_bairros.dbf", "notificacoes.csv",
           "historico_alerta.csv")
stopifnot(nzchar(base), all(file.exists(file.path(base, files))))
ref <- sf::st_read(
  file.path(base, "shape/feira_livre_bairros.shp"), quiet=TRUE
)
ref_path <- "/tmp/bulletin-artifacts/reference.rds"
qmd <- "/tmp/bulletin-artifacts/test.qmd"
saveRDS(ref, ref_path)
config_path <- "/tmp/bulletin-artifacts/config.json"
jsonlite::write_json(list(
  geocode=999999,
  reference_columns=list(neighborhood="nm_bairro", id="id_bairro",
                         district="nm_distr", population="pop",
                         geometry=attr(ref, "sf_column")),
  thresholds=list(preseason=5, epidemic=50, postseason=10),
  climate=list(modelo="Af", colunas=list(temp_min="tempmin"),
               limiares=list(temp_min=18))
), config_path, auto_unbox=TRUE)
status <- system2("Rscript", c(
  "/tmp/generate.R", config_path,
  file.path(base, "notificacoes.csv"),
  file.path(base, "historico_alerta.csv"), "''", ref_path, qmd,
  "2026", "250000"
))
stopifnot(status == 0L)
stopifnot(file.exists(qmd), file.info(qmd)$size > 0)
qmd_text <- tolower(paste(readLines(qmd), collapse="\n"))
stopifnot(grepl("executar_nowcasting: false", qmd_text, fixed=TRUE))
""",
        encoding='utf-8',
    )

    adapter = (
        Path(__file__).resolve().parents[2]
        / 'src/infodengue_pipeline/bulletin/generate.R'
    )
    container_output = subprocess.run(
        [
            docker,
            'run',
            '--rm',
            '--volume',
            f'{artifacts}:/tmp/bulletin-artifacts:rw',
            '--volume',
            f'{adapter}:/tmp/generate.R:ro',
            IMAGE,
            'bash',
            '-lc',
            'Rscript /tmp/bulletin-artifacts/generate_synthetic.R '
            '&& quarto --version '
            '&& quarto render /tmp/bulletin-artifacts/test.qmd --to pdf',
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=3600,
    )
    assert container_output.returncode == 0, (
        'Docker R/QMD/Quarto command failed with exit status '
        f'{container_output.returncode}.\n'
        f'stdout:\n{container_output.stdout[-12000:]}\n'
        f'stderr:\n{container_output.stderr[-12000:]}'
    )

    qmd = artifacts / 'test.qmd'
    pdf = artifacts / 'test.pdf'
    assert qmd.is_file(), 'synthetic QMD was not created'
    assert pdf.is_file(), 'Quarto did not create the synthetic PDF'
    _validate_pdf(pdf)
