-- Canonical PostgreSQL schema for local testifier_audit setup.
-- Keep this aligned with:
--   - src/testifier_audit/io/submissions_postgres.py
--   - src/testifier_audit/io/vrdb_postgres.py
--   - src/testifier_audit/io/import_tracking.py

BEGIN;

CREATE TABLE IF NOT EXISTS public_submissions (
  submission_key TEXT PRIMARY KEY,
  source_file TEXT NOT NULL,
  source_row_number BIGINT NOT NULL,
  source_hash TEXT NOT NULL,
  source_id TEXT,
  name_raw TEXT NOT NULL,
  name_clean TEXT NOT NULL,
  name_last TEXT,
  name_first TEXT,
  organization_raw TEXT,
  organization_clean TEXT,
  organization_is_blank BOOLEAN NOT NULL,
  position_raw TEXT,
  position_normalized TEXT NOT NULL,
  time_signed_in_raw TEXT,
  signed_at TIMESTAMPTZ,
  minute_bucket TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS public_submissions_source_file_idx
  ON public_submissions (source_file);
CREATE INDEX IF NOT EXISTS public_submissions_source_row_number_idx
  ON public_submissions (source_row_number);
CREATE INDEX IF NOT EXISTS public_submissions_source_file_source_row_number_idx
  ON public_submissions (source_file, source_row_number);
CREATE INDEX IF NOT EXISTS public_submissions_minute_bucket_idx
  ON public_submissions (minute_bucket);
CREATE INDEX IF NOT EXISTS public_submissions_position_normalized_idx
  ON public_submissions (position_normalized);
CREATE INDEX IF NOT EXISTS public_submissions_organization_is_blank_idx
  ON public_submissions (organization_is_blank);

CREATE TABLE IF NOT EXISTS voter_registry (
  voter_key TEXT PRIMARY KEY,
  state_voter_id TEXT,
  first_name TEXT NOT NULL,
  middle_name TEXT,
  last_name TEXT NOT NULL,
  name_suffix TEXT,
  birth_year TEXT,
  status_code TEXT,
  reg_city TEXT NOT NULL DEFAULT '',
  county_code TEXT NOT NULL DEFAULT '',
  canonical_first TEXT NOT NULL,
  canonical_last TEXT NOT NULL,
  canonical_name TEXT NOT NULL,
  canonical_middle_initial TEXT NOT NULL DEFAULT '',
  canonical_suffix TEXT NOT NULL DEFAULT '',
  canonical_key_strict TEXT NOT NULL DEFAULT '',
  canonical_key_medium TEXT NOT NULL DEFAULT '',
  canonical_key_loose TEXT NOT NULL DEFAULT '',
  canonical_key_nickname TEXT NOT NULL DEFAULT '',
  collision_key_strict TEXT NOT NULL DEFAULT '',
  collision_key_medium TEXT NOT NULL DEFAULT '',
  collision_key_loose TEXT NOT NULL DEFAULT '',
  full_name_key TEXT NOT NULL DEFAULT '',
  first_name_key TEXT NOT NULL DEFAULT '',
  last_name_key TEXT NOT NULL DEFAULT '',
  name_normalized TEXT NOT NULL DEFAULT '',
  normalization_version TEXT NOT NULL DEFAULT '',
  normalization_version_hash TEXT NOT NULL DEFAULT '',
  source_file TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS reg_city TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS county_code TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS canonical_middle_initial TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS canonical_suffix TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS canonical_key_strict TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS canonical_key_medium TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS canonical_key_loose TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS canonical_key_nickname TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS collision_key_strict TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS collision_key_medium TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS collision_key_loose TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS full_name_key TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS first_name_key TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS last_name_key TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS name_normalized TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS normalization_version TEXT NOT NULL DEFAULT '';
ALTER TABLE voter_registry ADD COLUMN IF NOT EXISTS normalization_version_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS voter_registry_canonical_name_idx
  ON voter_registry (canonical_name);
CREATE INDEX IF NOT EXISTS voter_registry_canonical_last_idx
  ON voter_registry (canonical_last);
CREATE INDEX IF NOT EXISTS voter_registry_status_code_idx
  ON voter_registry (status_code);
CREATE INDEX IF NOT EXISTS voter_registry_county_code_idx
  ON voter_registry (county_code);
CREATE INDEX IF NOT EXISTS voter_registry_reg_city_idx
  ON voter_registry (reg_city);
CREATE INDEX IF NOT EXISTS voter_registry_canonical_key_strict_idx
  ON voter_registry (canonical_key_strict);
CREATE INDEX IF NOT EXISTS voter_registry_canonical_key_medium_idx
  ON voter_registry (canonical_key_medium);
CREATE INDEX IF NOT EXISTS voter_registry_canonical_key_loose_idx
  ON voter_registry (canonical_key_loose);
CREATE INDEX IF NOT EXISTS voter_registry_canonical_key_nickname_idx
  ON voter_registry (canonical_key_nickname);
CREATE INDEX IF NOT EXISTS voter_registry_collision_key_strict_idx
  ON voter_registry (collision_key_strict);
CREATE INDEX IF NOT EXISTS voter_registry_collision_key_medium_idx
  ON voter_registry (collision_key_medium);
CREATE INDEX IF NOT EXISTS voter_registry_collision_key_loose_idx
  ON voter_registry (collision_key_loose);
CREATE INDEX IF NOT EXISTS voter_registry_full_name_key_idx
  ON voter_registry (full_name_key);
CREATE INDEX IF NOT EXISTS voter_registry_first_name_key_idx
  ON voter_registry (first_name_key);
CREATE INDEX IF NOT EXISTS voter_registry_last_name_key_idx
  ON voter_registry (last_name_key);

CREATE TABLE IF NOT EXISTS data_imports (
  import_id BIGSERIAL PRIMARY KEY,
  import_kind TEXT NOT NULL,
  target_table TEXT NOT NULL,
  source_file TEXT NOT NULL,
  file_hash TEXT NOT NULL,
  file_size_bytes BIGINT NOT NULL,
  importer_version TEXT NOT NULL,
  status TEXT NOT NULL,
  rows_processed BIGINT NOT NULL DEFAULT 0,
  rows_upserted BIGINT NOT NULL DEFAULT 0,
  message TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS data_imports_lookup_idx
  ON data_imports (import_kind, target_table, file_hash, importer_version, status);
CREATE INDEX IF NOT EXISTS data_imports_created_at_idx
  ON data_imports (created_at DESC);

COMMIT;
