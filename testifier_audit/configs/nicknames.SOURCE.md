# Nicknames Dataset Source

- Upstream project: `carltonnorthern/nicknames`
- Upstream file: `names.csv`
- Pinned commit: `e13a5c051689bebe5178c0b2d4730cb46a3cb698`
- Upstream URL:
  - `https://raw.githubusercontent.com/carltonnorthern/nicknames/e13a5c051689bebe5178c0b2d4730cb46a3cb698/names.csv`
- Upstream license: Apache License 2.0
  - `https://raw.githubusercontent.com/carltonnorthern/nicknames/e13a5c051689bebe5178c0b2d4730cb46a3cb698/License.txt`
- Optional supplemental source: `tfmorris/Names`
  - `https://raw.githubusercontent.com/tfmorris/Names/master/eval/src/main/resources/givenname_nicknames.txt`
  - repository license: Apache License 2.0 (`LICENSE` in repo root)
  - dataset notes in upstream README reference additional community-contributed sources.

## Local transformation

`configs/nicknames.csv` is generated from the upstream file by:

1. Keeping only `relationship == has_nickname`.
2. Normalizing names to uppercase ASCII tokens.
3. Dropping empty/self-mappings and multi-token values.
4. Keeping only aliases that map to a single canonical name (to avoid ambiguous rewrites).
5. Merging unambiguous aliases from the supplemental source:
   - existing primary alias mappings win on conflict,
   - new aliases from supplemental are appended,
   - supplemental aliases that would rewrite an existing primary canonical token are skipped
     (prevents root-direction flips like `ANTHONY -> TONY` plus `TONY -> ANTHONY`).
6. Applying local overrides:
   - `BOB -> ROBERT`
   - `BILL -> WILLIAM`
   - `BECKY -> REBECCA`
   - `JIM -> JAMES`

Use `./testifier_audit/scripts/data/update_nicknames.py` to regenerate.
Supplemental merge is the default; use `--primary-only` to disable it.
Use `--report-path` for merge stats.
