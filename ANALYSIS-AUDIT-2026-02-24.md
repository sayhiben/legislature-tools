# Analysis Capability Audit (2026-02-24)

## Scope

This audit inventories what the codebase is capable of doing, independent of current runtime allowlists.

Primary sources inspected:

- `./testifier_audit/src/testifier_audit/report/analysis_registry.py`
- `./testifier_audit/src/testifier_audit/detectors/registry.py`
- `./testifier_audit/configs/default.yaml`
- `./testifier_audit/src/testifier_audit/report/render.py`
- `./testifier_audit/src/testifier_audit/report/static/report/main.js`
- `./testifier_audit/src/testifier_audit/report/triage_builder.py`
- `./testifier_audit/src/testifier_audit/report/quality_builder.py`
- `./testifier_audit/src/testifier_audit/report/help_registry.py`
- `./testifier_audit/src/testifier_audit/detectors/*.py`

## Top-Line Inventory

- Analyses defined in registry: 14.
- Charts emitted in interactive payload: 75.
- Analysis-section charts (hero/detail IDs in registry): 66.
- Additional compatibility/support charts in payload: 9.
- Detector/artifact table sources feeding charts: 64 keys.

Additional payload/support previews:

- `composite_score.evidence_bundle_preview`
- `rare_names.rarity_coverage_preview`
- `rare_names.rarity_unmatched_first_preview`
- `rare_names.rarity_unmatched_last_preview`
- `periodicity.clockface_top_preview`

## Capability vs Runtime Scope

Current date context for this audit: February 24, 2026.

| Analysis ID | Detector | Capability Present | Default Config Status | Current Registry Scope (`ANALYSES_TO_PERFORM`) |
| --- | --- | --- | --- | --- |
| `baseline_profile` | n/a | Yes | Active | Not in scope |
| `bursts` | `bursts` | Yes | Enabled | Not in scope |
| `procon_swings` | `procon_swings` | Yes | Enabled | Not in scope |
| `changepoints` | `changepoints` | Yes | Enabled (`changepoints.enabled: true`) | Not in scope |
| `off_hours` | `off_hours` | Yes | Enabled | In scope |
| `duplicates_exact` | `duplicates_exact` | Yes | Enabled | In scope |
| `duplicates_near` | `duplicates_near` | Yes | Enabled | In scope |
| `sortedness` | `sortedness` | Yes | Enabled | Not in scope |
| `rare_names` | `rare_names` | Yes | Core enabled, rarity enrichment disabled (`rarity.enabled: false`) | Not in scope |
| `org_anomalies` | `org_anomalies` | Yes | Enabled | Not in scope |
| `voter_registry_match` | `voter_registry_match` | Yes | Disabled by default (`voter_registry.enabled: false`) | In scope |
| `periodicity` | `periodicity` | Yes | Enabled (`periodicity.enabled: true`) | Not in scope |
| `multivariate_anomalies` | `multivariate_anomalies` | Yes | Enabled (`multivariate_anomaly.enabled: true`) | Not in scope |
| `composite_score` | `composite_score` | Yes | Enabled | Not in scope |

Scope behavior note: when `ANALYSES_TO_PERFORM` is non-empty, pass2 detector execution is explicitly restricted to those analyses' detectors.

## Per-Analysis Deep Inventory

### 1) `baseline_profile`

Sub-analyses:

1. Minute-level baseline volume and pro-rate trend.
2. Day-of-week x hour-of-day baseline volume/pro-rate profile.
3. Top-name concentration profile.
4. Name-length distribution profile.

Produced tables:

- `artifacts.counts_per_minute`
- `artifacts.counts_per_hour`
- `artifacts.name_frequency`
- `artifacts.name_text_features`

Report charts:

- `baseline_volume_pro_rate`
- `baseline_day_hour_volume`
- `baseline_top_names`
- `baseline_name_length_distribution`

### 2) `bursts`

Sub-analyses:

1. Significant burst-window detection (observed vs expected exceedance).
2. Window-size significance summary.
3. Burst composition shift vs baseline (pro-rate deltas).
4. Null simulation reference distribution.

Produced tables:

- `burst_significant_windows`
- `burst_window_tests`
- `burst_null_distribution`

Report charts:

- `bursts_hero_timeline`
- `bursts_significance_by_window`
- `bursts_composition_shift`
- `bursts_null_distribution`

### 3) `procon_swings`

Sub-analyses:

1. Swing significance tests and significant-window extraction.
2. Time-bucket trend of pro-rate vs baseline stability bands.
3. Day-slot deviation heatmap.
4. Weekday-hour pro-rate profile.
5. Slot-of-day pro-rate profile.
6. Directional run persistence and run summary.
7. Null distribution of max absolute pro-rate swings.

Produced tables:

- `swing_significant_windows`
- `swing_window_tests`
- `time_bucket_profiles`
- `day_bucket_profiles`
- `pro_rate_by_hour`
- `time_of_day_bucket_profiles`
- `direction_runs`
- `direction_runs_summary`
- `swing_null_distribution`

Report charts:

- `procon_swings_hero_bucket_trend`
- `procon_swings_shift_heatmap`
- `procon_swings_day_hour_heatmap`
- `procon_swings_time_of_day_profile`
- `procon_swings_direction_runs`
- `procon_swings_null_distribution`

### 4) `changepoints`

Sub-analyses:

1. Structural break detection across monitored metrics.
2. Break magnitude ranking.
3. Hour-of-day distribution of break timing.

Produced tables:

- `all_changepoints`
- `volume_changepoints`
- `pro_rate_changepoints`

Report charts:

- `changepoints_hero_timeline`
- `changepoints_magnitude`
- `changepoints_hour_hist`

### 5) `off_hours`

Sub-analyses:

1. Off-hours summary and off-vs-on comparison metrics.
2. Hourly and hour-of-week composition profiles.
3. Date-hour composition heatmaps (pro-rate and volume).
4. Date-hour primary-residual heatmap.
5. Window-level control profile with day/model/primary/global baselines.
6. Primary residual timeline and flag-channel accounting.
7. Funnel plot diagnostics for support vs pro-rate.
8. Model-fit diagnostics across bucket sizes.
9. Flagged-window diagnostics table.

Produced tables:

- `off_hours_summary`
- `hourly_distribution`
- `hour_of_week_distribution`
- `date_hour_distribution`
- `date_hour_primary_residual_distribution`
- `window_control_profile`
- `model_fit_diagnostics`
- `flag_channel_summary`
- `flagged_window_diagnostics`

Report charts (section):

- `off_hours_control_timeline`
- `off_hours_funnel_plot`
- `off_hours_primary_residual_timeline`
- `off_hours_primary_flag_channels`
- `off_hours_model_fit_diagnostics`
- `off_hours_date_hour_pro_heatmap`
- `off_hours_date_hour_primary_residual_heatmap`
- `off_hours_date_hour_volume_heatmap`

Additional support charts in payload:

- `off_hours_hourly_profile`
- `off_hours_summary_compare`
- `off_hours_day_hour_heatmap`

### 6) `duplicates_exact`

Sub-analyses:

1. Collision method/runtime metadata and baseline provenance.
2. Collision overview and duplicate-metric overviews.
3. Bucket-level duplicate burden (including legacy fallback forms).
4. Per-name anomaly testing and per-name displays.
5. Top repeated names and top-name timing by match mode.
6. Position concentration and switching effects.
7. Temporal burst diagnostics for repeated names.
8. Null distribution under baseline model.
9. Pro/Con swing-impact sensitivity scenarios.
10. Stratification sensitivity diagnostics.

Produced tables:

- `collision_methods`
- `collision_overview`
- `duplicate_metrics_overview`
- `collision_by_bucket`
- `duplicate_by_bucket`
- `repeated_same_bucket`
- `repeated_same_bucket_summary`
- `repeated_same_minute`
- `per_name_anomalies`
- `per_name_display`
- `per_name_tests`
- `top_repeated_names`
- `top_name_timing_by_mode`
- `position_concentration_tests`
- `position_duplicate_metrics`
- `position_switching_names`
- `temporal_burst_signals`
- `null_distribution`
- `swing_impact_scenarios`
- `collision_stratification_sensitivity`

Report charts (section):

- `duplicates_exact_bucket_concentration`
- `duplicates_exact_metric_diagnostics`
- `duplicates_exact_per_name_anomalies`
- `duplicates_exact_top_name_timing_exact`
- `duplicates_exact_top_name_timing_medium`
- `duplicates_exact_top_name_timing_loose`
- `duplicates_exact_position_concentration`
- `duplicates_exact_temporal_burst`
- `duplicates_exact_swing_impact`

Additional support/compat charts in payload:

- `duplicates_exact_null_distribution`
- `duplicates_exact_top_names` (alias of per-name anomalies)
- `duplicates_exact_position_switch` (alias of per-name anomalies)

### 7) `duplicates_near`

Sub-analyses:

1. Candidate blocking and skipped-block diagnostics.
2. Similarity-edge extraction.
3. Cluster member and cluster summary construction.
4. Cluster time concentration and concentration summary.
5. Cluster timeline and cluster-size distributions.

Produced tables:

- `candidate_blocks`
- `skipped_blocks`
- `similarity_edges`
- `cluster_members`
- `cluster_summary`
- `cluster_time_concentration`
- `cluster_time_concentration_summary`

Report charts:

- `duplicates_near_cluster_timeline`
- `duplicates_near_cluster_size`
- `duplicates_near_time_concentration`
- `duplicates_near_similarity`

### 8) `sortedness`

Sub-analyses:

1. Bucket-level ordering metrics.
2. Bucket-level ordering summary.
3. Minute-level ordering spikes.
4. Sortedness metric rollup.

Produced tables:

- `bucket_ordering`
- `bucket_ordering_summary`
- `minute_ordering`
- `sortedness_metrics`

Report charts:

- `sortedness_bucket_ratio`
- `sortedness_bucket_summary`
- `sortedness_kendall_tau_summary`
- `sortedness_minute_spikes`

### 9) `rare_names`

Sub-analyses:

1. Singleton-name extraction.
2. Unique-ratio window profiling.
3. Weird-name scoring.
4. Rarity-by-minute timeline.
5. High-rarity window extraction.
6. Top rarity records.
7. Rarity lookup coverage diagnostics.
8. Unmatched first-token diagnostics.
9. Unmatched last-token diagnostics.

Produced tables:

- `singleton_names`
- `unique_ratio_windows`
- `weird_names`
- `rarity_by_minute`
- `rarity_high_windows`
- `rarity_top_records`
- `rarity_lookup_coverage`
- `rarity_unmatched_first_tokens`
- `rarity_unmatched_last_tokens`

Report charts:

- `rare_names_unique_ratio`
- `rare_names_weird_scores`
- `rare_names_singletons`
- `rare_names_rarity_timeline`

Additional preview tables in analysis panel:

- `rare_names.rarity_coverage_preview`
- `rare_names.rarity_unmatched_first_preview`
- `rare_names.rarity_unmatched_last_preview`

### 10) `org_anomalies`

Sub-analyses:

1. Bucket-level blank-organization rate trend.
2. Position-split blank-organization rates.
3. Minute-level organization burst detection.
4. Top organization-value concentration.
5. Blank-rate summary rollup.

Produced tables:

- `organization_blank_rate_by_bucket`
- `organization_blank_rate_by_bucket_position`
- `organization_minute_bursts`
- `organization_counts`
- `organization_blank_rate_summary`

Report charts:

- `org_anomalies_blank_rate`
- `org_anomalies_position_rates`
- `org_anomalies_bursts`
- `org_anomalies_top_orgs`

### 11) `voter_registry_match`

Sub-analyses:

1. Bucket-level match/unmatched rates.
2. Position-level match/unmatched rates (row-level and unique-name units).
3. Pairwise position tests.
4. Linkage sensitivity-mode comparison.
5. Unmatched-name ranking.
6. Linkage overview and assignment outputs.
7. Position-bucket trend decomposition.

Produced tables:

- `match_by_bucket`
- `match_by_bucket_position`
- `linkage_by_position_rows`
- `linkage_by_position_unique`
- `position_pairwise_tests`
- `sensitivity_modes`
- `unmatched_names`
- `linkage_overview`
- `match_assignments`

Report charts (section):

- `voter_registry_match_rates`
- `voter_registry_linkage_by_position_rows`
- `voter_registry_linkage_by_position_unique`
- `voter_registry_pairwise_tests`
- `voter_registry_sensitivity_modes`
- `voter_registry_unmatched_names`

Additional support/compat charts in payload:

- `voter_registry_position_buckets`
- `voter_registry_match_by_position` (alias of linkage-by-position rows)
- `voter_registry_match_tiers` (alias of sensitivity modes)

### 12) `periodicity`

Sub-analyses:

1. Clockface minute-of-hour concentration.
2. Clockface top-minute extraction and clockface null calibration.
3. Autocorrelation profile and significant autocorrelation extraction.
4. Spectrum profile and significant-spectrum extraction.
5. Periodicity null calibration.
6. Rolling Fano overdispersion profile and summary.

Produced tables:

- `clockface_distribution`
- `clockface_top_minutes`
- `clockface_null_distribution`
- `autocorr`
- `autocorr_significant`
- `spectrum_top`
- `spectrum_significant`
- `periodicity_null_distribution`
- `rolling_fano`
- `rolling_fano_summary`

Report charts:

- `periodicity_clockface`
- `periodicity_autocorr`
- `periodicity_spectrum`
- `periodicity_rolling_fano`

Additional preview table in analysis panel:

- `periodicity.clockface_top_preview`

### 13) `multivariate_anomalies`

Sub-analyses:

1. Bucket anomaly scoring in multivariate feature space.
2. Top anomaly bucket extraction.
3. Feature projection/scatter diagnostics.

Produced tables:

- `bucket_anomaly_scores`
- `top_bucket_anomalies`

Report charts:

- `multivariate_score_timeline`
- `multivariate_top_buckets`
- `multivariate_feature_projection`

### 14) `composite_score`

Sub-analyses:

1. Ranked composite evidence windows.
2. High-priority composite windows.
3. Evidence-bundle rollup and flag-frequency projection.

Produced tables:

- `ranked_windows`
- `high_priority_windows`
- `evidence_bundle_windows`

Report charts:

- `composite_score_timeline`
- `composite_evidence_flags` (derived from `evidence_bundle_windows`)
- `composite_high_priority`

Additional preview table in analysis panel:

- `composite_score.evidence_bundle_preview`

## Shared Report Surfaces (Non-Analysis-Specific)

These are produced regardless of individual detector chart sections and materially affect the auditability of outputs:

1. Triage views (`raw`, `exact_row_dedup`, `side_by_side`) from `build_investigation_views`.
2. Triage summary + evidence queues:
   - `window_evidence_queue`
   - `record_evidence_queue`
   - `cluster_evidence_queue`
3. Data quality panel tables:
   - warning rows
   - raw-vs-dedup delta metrics
   - sourced from `artifacts.basic_quality`, `artifacts.counts_per_minute`, and org blank-rate outputs
4. Cross-hearing comparator table (`cross_hearing_baseline.metric_comparators`).
5. Hearing context tables:
   - metadata rows
   - deadline ramp metrics
   - stance-by-deadline rows
6. Drilldown tables:
   - causative rows
   - duplicate names in selected window
   - near-duplicate clusters in selected window
   - run + weirdness comparison rows
7. Name/cluster forensics tables:
   - top repeated names
   - top near-duplicate clusters
8. Methodology tables:
   - definitions
   - tests/calibrations used
   - evidence taxonomy
   - ethical guardrails
   - artifact row coverage
9. Analysis table previews mounted per section (`reportData.table_previews[detector]`), plus automatic table-help and column glossary.

## Defensibility-Weighted Scorecard

Scoring model (defensibility-heavy):

- Statistical defensibility: 35%
- Test/contract coverage depth: 25%
- Investigative utility: 20%
- Report interpretability/UX maturity: 10%
- Operational readiness (data dependencies, failure handling, runtime stability): 10%

Tiering:

- Keep: weighted score >= 75
- Refactor: 60 <= weighted score < 75
- Cull-candidate: weighted score < 60

| Analysis | Def (35) | Test (25) | Utility (20) | UX (10) | Ops (10) | Weighted Score | Recommendation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `off_hours` | 5.0 | 5.0 | 5.0 | 5.0 | 4.0 | 98.0 | Keep |
| `duplicates_exact` | 4.5 | 4.5 | 5.0 | 4.5 | 4.0 | 91.0 | Keep |
| `bursts` | 4.0 | 4.0 | 4.0 | 4.0 | 4.0 | 80.0 | Keep |
| `procon_swings` | 4.0 | 4.0 | 4.0 | 4.0 | 4.0 | 80.0 | Keep |
| `duplicates_near` | 3.5 | 4.0 | 4.0 | 4.0 | 4.0 | 76.5 | Keep |
| `baseline_profile` | 3.5 | 3.0 | 4.0 | 4.0 | 4.0 | 71.5 | Refactor |
| `composite_score` | 3.0 | 3.5 | 4.0 | 4.0 | 3.0 | 68.5 | Refactor |
| `voter_registry_match` | 3.0 | 4.0 | 3.5 | 4.0 | 2.5 | 68.0 | Refactor |
| `changepoints` | 3.0 | 3.5 | 3.0 | 3.0 | 4.0 | 64.5 | Refactor |
| `periodicity` | 3.0 | 3.5 | 3.0 | 3.0 | 4.0 | 64.5 | Refactor |
| `multivariate_anomalies` | 2.5 | 3.5 | 3.0 | 3.0 | 4.0 | 61.0 | Refactor |
| `rare_names` | 2.5 | 3.5 | 3.0 | 3.0 | 4.0 | 61.0 | Refactor |
| `org_anomalies` | 2.5 | 3.0 | 2.5 | 3.0 | 4.0 | 56.5 | Cull-candidate |
| `sortedness` | 2.0 | 2.5 | 2.0 | 2.5 | 4.0 | 47.5 | Cull-candidate |

## Recommendation Stack (No Removals Applied)

Keep (highest defensibility + maturity):

- `off_hours`
- `duplicates_exact`
- `bursts`
- `procon_swings`
- `duplicates_near`

Refactor (retain, but tighten methodology/claims/contracts):

- `baseline_profile`
- `composite_score`
- `voter_registry_match`
- `changepoints`
- `periodicity`
- `multivariate_anomalies`
- `rare_names`

Cull-candidate (lowest current defensibility-to-maintenance ratio):

- `sortedness`
- `org_anomalies`

Rationale summary:

- Keep tier: strongest combination of calibrated/statistical evidence, mature chart/table contracts, and high triage utility.
- Refactor tier: useful signal families, but either weaker inferential grounding, stronger dependency caveats, or lower contract maturity than top tier.
- Cull-candidate tier: lower standalone defensibility and lower incremental triage value relative to maintenance burden and overlap with stronger analyses.

Important: this audit did not remove or disable any analysis, detector, table, chart, or report surface.
