# Nicknames Dataset Source

- Upstream project: `carltonnorthern/nicknames`
- Upstream file: `names.csv`
- Pinned commit: `e13a5c051689bebe5178c0b2d4730cb46a3cb698`
- Upstream URL:
  - `https://raw.githubusercontent.com/carltonnorthern/nicknames/e13a5c051689bebe5178c0b2d4730cb46a3cb698/names.csv`
- Upstream license: Apache License 2.0
  - `https://raw.githubusercontent.com/carltonnorthern/nicknames/e13a5c051689bebe5178c0b2d4730cb46a3cb698/License.txt`

## Local transformation

`configs/nicknames.csv` is generated from the upstream file by:

1. Keeping only `relationship == has_nickname`.
2. Normalizing names to uppercase ASCII tokens.
3. Dropping empty/self-mappings and multi-token values.
4. Keeping only aliases that map to a single canonical name (to avoid ambiguous rewrites).
5. Applying local overrides:
   - `BOB -> ROBERT`
   - `BILL -> WILLIAM`
   - `JIM -> JAMES`

Use `/Users/sayhiben/dev/legislature-tools/testifier_audit/scripts/data/update_nicknames.py` to regenerate.
