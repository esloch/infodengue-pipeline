"""Check known PR30 reference bytes without asserting scientific approval."""

import argparse
import hashlib
import json

from pathlib import Path

RECORD = (
    Path(__file__).resolve().parents[1]
    / 'staging-data/pr30-recife-reference.json'
)


def sha256(path: Path) -> str:
    """Hash a file without loading the geometry into memory."""
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate(reference: Path, source: Path | None = None) -> None:
    """Require exact known bytes for the adapted and optional source files."""
    record = json.loads(RECORD.read_text(encoding='utf-8'))
    if sha256(reference) != record['adapted_sha256']:
        raise ValueError('adapted Recife RDS SHA-256 mismatch')
    if source is not None and sha256(source) != record['source_sha256']:
        raise ValueError('upstream Recife RData SHA-256 mismatch')


def main() -> None:
    """Validate local input files and report the limited provenance claim."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference', type=Path)
    parser.add_argument('--source', type=Path)
    args = parser.parse_args()
    validate(args.reference, args.source)
    print('Known PR30 reference hashes verified; approval is not documented.')


if __name__ == '__main__':
    main()
