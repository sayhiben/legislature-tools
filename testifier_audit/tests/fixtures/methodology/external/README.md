# External Methodology Fixtures

This directory stores frozen external benchmark fixtures used by automated tests for:

- Duplicate-rate methodology parity (`duplicates_exact` expected-rate checks)
- Voter-linkage methodology parity (`voter_registry_match` strict/loose checks)
- Voter-linkage ground-truth quality checks against FEBRL record keys (`rec_id`)

## Source

Fixtures are derived from FEBRL benchmark datasets distributed via `recordlinkage`:

- Record Linkage toolkit docs: https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html
- Repository: https://github.com/J535D165/recordlinkage
- License: BSD-3-Clause

Raw FEBRL data URLs are recorded in `expected/duplicates_manifest.json` and
`expected/voter_manifest.json`.

## Layout

- `febrl/`: vendored benchmark input fixtures used by tests.
- `expected/`: frozen expected outputs and manifests.

## Provenance and Checksums

Each manifest and expected artifact includes:

- source dataset id/version
- source checksum
- fixture checksum
- reference method id/version

Ground-truth artifact for FEBRL4 linkage:

- `expected/febrl_dataset4_voter_fast_ground_truth.json`

## Regeneration

Regenerate fixtures and expected outputs from upstream benchmark data:

```bash
cd testifier_audit
python ./scripts/tests/refresh_external_benchmarks.py
```

This is a maintenance workflow only. Test execution does not download data.
