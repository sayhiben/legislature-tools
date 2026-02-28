# Off-Hours Detector Statistical Methodology Dossier (Statistician-Ready)

## 1. Purpose and Audience

This dossier documents the current off-hours detector implementation in enough detail for an expert statistician to audit:

- inferential validity,
- baseline construction and fallback behavior,
- multiple-testing control,
- alert decision logic,
- report-layer reinterpretations of detector output,
- test coverage strength and remaining methodological risk.

The intent is implementation-faithful reconstruction, not proposal of new methods.

## 2. Scope and Non-Scope

### In scope

- Detector-core off-hours preprocessing, aggregation, baselines, inference, and alerting.
- Report payload/JS transformations that alter semantics exposed to analysts.
- Cross-hearing baseline machinery as contextual (separate) comparators.
- Test mapping from statistical claims to existing tests.

### Out of scope

- Recommending model replacements or threshold changes.
- Product/policy interpretation of findings in specific hearings.
- Non-off-hours detectors except where needed for context.

## 3. Canonical Source Mapping

Primary implementation and test sources reviewed:

1. `testifier_audit/src/testifier_audit/detectors/off_hours.py`
2. `testifier_audit/src/testifier_audit/detectors/off_hours_pipeline.py`
3. `testifier_audit/src/testifier_audit/detectors/off_hours_statistics.py`
4. `testifier_audit/src/testifier_audit/detectors/off_hours_tables.py`
5. `testifier_audit/src/testifier_audit/detectors/off_hours_methodology.py`
6. `testifier_audit/src/testifier_audit/proportion_stats.py`
7. `testifier_audit/src/testifier_audit/preprocess/time.py`
8. `testifier_audit/src/testifier_audit/config.py`
9. `testifier_audit/configs/default.yaml`
10. `testifier_audit/configs/voter_registry_enabled.yaml`
11. `testifier_audit/src/testifier_audit/report/analysis_registry.py`
12. `testifier_audit/src/testifier_audit/report/rendering/payload/builder.py`
13. `testifier_audit/src/testifier_audit/report/static/report/main.js`
14. `testifier_audit/src/testifier_audit/report/global_baselines.py`
15. `testifier_audit/scripts/report/build_global_baselines.py`
16. `testifier_audit/tests/test_off_hours_detector.py`
17. `testifier_audit/tests/test_off_hours_pipeline.py`
18. `testifier_audit/tests/test_off_hours_statistics.py`
19. `testifier_audit/tests/test_off_hours_methodology.py`
20. `testifier_audit/tests/test_report_chart_payload.py`

Supplemental report-layer context:

- `testifier_audit/src/testifier_audit/report/rendering/help_docs.py`
- `testifier_audit/src/testifier_audit/report/help_registry.py`
- `testifier_audit/tests/test_time.py`
- `testifier_audit/tests/test_global_baselines.py`
- `testifier_audit/src/testifier_audit/report/rendering/serialization.py`

## 4. Configuration and Runtime Defaults

### 4.1 Detector constructor defaults

`OffHoursDetector` defaults include:

- bucket minutes: `(1, 5, 15, 30, 60, 120, 240)`
- `min_window_total = DEFAULT_LOW_POWER_MIN_TOTAL` (from `proportion_stats.py`, constant `30`)
- `fdr_alpha = 0.05`
- `primary_bucket_minutes = 30`
- `model_min_rows = 24`
- `model_hour_harmonics = 3`
- `alert_off_hours_min_fraction = 1.0`
- `primary_alert_min_abs_delta = 0.03`

Parameter guards:

- `fdr_alpha` clipped to `[1e-6, 0.5]`
- `model_min_rows >= 8`
- `model_hour_harmonics in [1, 6]`
- `alert_off_hours_min_fraction in [0.5, 1.0]`
- `primary_alert_min_abs_delta in [0.0, 1.0]`

### 4.2 Effective run defaults via config wiring

In normal pipeline use (`detectors/registry.py`), detector parameters are injected from `config.off_hours`.
`default.yaml` and `voter_registry_enabled.yaml` both set:

- `min_window_total: 25`
- `fdr_alpha: 0.05`
- `primary_bucket_minutes: 30`
- `model_min_rows: 24`
- `model_hour_harmonics: 3`
- `alert_off_hours_min_fraction: 1.0`
- `primary_alert_min_abs_delta: 0.03`

Important implementation nuance:

- raw detector class default low-power threshold is 30,
- configured pipeline default is 25.

### 4.3 Bucket selection behavior

- Detector bucket set defaults to config `windows.analysis_bucket_minutes` unless `off_hours.bucket_minutes` is provided.
- Primary bucket is requested value if available; else 30 if present; else smallest available bucket.

## 5. Data Preprocessing and Off-Hours Labeling

## 5.1 Timestamp parsing and timezone

`add_time_features`:

1. Parses `time_signed_in` with explicit format `%m/%d/%Y %I:%M %p` first, then generic parse fallback.
2. Localizes/ converts to configured timezone (`TimeConfig.timezone`), default `America/Los_Angeles`.
3. Creates `timestamp`, `minute_bucket`, `hour`, `day_of_week`, `date`.

Explicit policy in code comments: hearing metadata timezone is preserved for context but does not override analysis timezone.

## 5.2 Off-hours indicator definition

Let `h` be local hour in `[0, 23]`, `s = off_hours_start`, `e = off_hours_end`.

- If `s <= e`, off-hours is `h in [s, e]` (inclusive).
- If `s > e` (wrap-around), off-hours is `(h >= s) OR (h <= e)`.

Default config (`s=0`, `e=5`) defines off-hours as inclusive `00:00-05:59` hour bins.

## 5.3 Detector event-time resolution

Windowing in off-hours pipeline uses:

- `minute_bucket` if present and has any valid datetimes,
- else `timestamp`,
- else `NaT` (leading to empty profile).

## 6. Window Aggregation and Derived Fields

For each bucket size `b` minutes and bucket start `t`:

Define counts over rows in that bucket:

- `n_total(t,b)`: all rows
- `n_pro(t,b)`: `position_normalized == "Pro"`
- `n_con(t,b)`: `position_normalized == "Con"`
- `n_off_hours(t,b)`: rows where `is_off_hours == True`

Derived:

- `n_known = n_pro + n_con`
- `n_unknown = max(n_total - n_known, 0)`
- `off_hours_fraction = n_off_hours / n_total` (0 when `n_total = 0`)
- `pro_rate = n_pro / n_known` when `n_known > 0`, else missing

Window labels:

- `is_off_hours_window = (off_hours_fraction >= 0.5)`
- `is_pure_off_hours_window = (n_off_hours == n_total)`
- `is_alert_off_hours_window = (off_hours_fraction >= alert_off_hours_min_fraction)`

By default `alert_off_hours_min_fraction = 1.0`, so inferential eligibility is restricted to pure off-hours windows.

Uncertainty and support:

- Wilson interval and Wilson half-width computed on `n_pro` of `n_known`.
- Low-power flag: `is_low_power = (n_known < min_window_total)` (or non-finite).

Unknown-position handling:

- Unknown positions are excluded from inferential denominator (`n_known`),
- but retained in `n_total` and therefore affect `off_hours_fraction`, eligibility, and descriptive volume context.

## 7. Baseline Construction

Three baseline layers are computed for expected pro-rate.

## 7.1 Global on-hours baseline

From known-position on-hours rows only:

\[
\hat p_{\text{global,on}} = \frac{\sum \mathbf{1}(\text{Pro} \land \text{on-hours})}{\sum \mathbf{1}(\text{known} \land \text{on-hours})}
\]

If unavailable, fallback to overall known pro-rate:

\[
\hat p_{\text{overall}} = \frac{\sum \mathbf{1}(\text{Pro} \land \text{known})}{\sum \mathbf{1}(\text{known})}
\]

## 7.2 Day-specific on-hours baseline with support gating

For each calendar day key `d`:

\[
\hat p_{\text{day,on}}(d) = \frac{\text{day\_on\_hours\_pro}(d)}{\text{day\_on\_hours\_known}(d)}
\]

Used only if `day_on_hours_known >= min_window_total` and non-missing; otherwise fallback to global-on-hours.
If no finite global-on-hours exists and overall known exists, fallback to overall known.

Outputs:

- `expected_pro_rate_global`
- `expected_pro_rate_day`
- `baseline_source in {day_on_hours, global_on_hours, overall_known, unavailable}`

## 7.3 Model baseline: weighted binomial GLM with day fixed effects and harmonic hour terms

Fit frame: all windows with `n_known > 0` and valid hour (not limited to on-hours).

Response and weights:

- \(y_i = n_{pro,i}/n_{known,i}\), clipped to \([10^{-6}, 1-10^{-6}]\)
- weights \(w_i = n_{known,i}\) (clipped lower bound 1)

Design:

\[
\text{logit}(E[y_i]) = \beta_0
+ \sum_{h=1}^{H} \alpha_h \sin\left(\frac{2\pi h \cdot \text{hour}_i}{24}\right)
+ \sum_{h=1}^{H} \gamma_h \cos\left(\frac{2\pi h \cdot \text{hour}_i}{24}\right)
+ \sum_{d \in D\setminus d_0} \delta_d \mathbf{1}(\text{day}_i=d)
\]

where `H = model_hour_harmonics`.

Availability gates:

- at least `model_min_rows` rows
- at least 3 unique hours
- non-empty day levels

Fit strategy:

1. primary: `statsmodels.GLM(..., family=Binomial(), freq_weights=...)` with `.fit(maxiter=250)`
2. fallback: regularized GLM `.fit_regularized(alpha=1e-4, L1_wt=0.0, maxiter=500)`
3. failure: model unavailable

Diagnostics retained per row:

- fit method
- fit rows
- unique days/hours
- converged flag (if available)
- AIC (if available)
- harmonics used

Model baseline outputs:

- `expected_pro_rate_model`
- `is_model_baseline_available`
- `model_baseline_source`

## 7.4 Primary expected baseline (decision baseline)

\[
\hat p_{\text{primary}} =
\begin{cases}
\hat p_{\text{model}}, & \text{if model prediction available} \\
\hat p_{\text{day/global fallback}}, & \text{otherwise}
\end{cases}
\]

`primary_baseline_source` is `model_day_hour` when model is available, else inherited day/global/overall source.

## 7.5 Baseline hierarchy summary

Ordered fallback chain for `expected_pro_rate_primary`:

1. model (GLM day fixed + harmonic hour)
2. day on-hours baseline (support-gated)
3. global on-hours baseline
4. overall known baseline
5. unavailable

## 8. Inferential Statistics and Diagnostics

## 8.1 Wilson intervals

Used for descriptive uncertainty on proportions, not as anomaly tests.

Implemented in `proportion_stats.wilson_interval` with default `z=1.96`.

## 8.2 Control limits (p-chart style normal approximation)

For expected rate \(p\) and known count \(n\):

\[
SE = \sqrt{\frac{p(1-p)}{n}},\quad
L = \max\{0, p - z\cdot SE\},\quad
U = \min\{1, p + z\cdot SE\}
\]

Computed for:

- day baseline
- global baseline
- model baseline
- primary baseline

with `z=1.96` (95%) and `z=3.0` (99.8%).

Invalid when expected/totals non-finite or `n<=0`.

## 8.3 Standardized residuals

For observed pro count `k`, known total `n`, expected rate `p`:

\[
z = \frac{k - np}{\sqrt{np(1-p)}}
\]

Expected rates are clipped to `(1e-6, 1-1e-6)` before residual computation.

## 8.4 Exact binomial p-values

For each baseline (day/model/primary):

- lower tail: `BinomCDF(k; n, p)`
- upper tail: `BinomSF(k-1; n, p)`
- two-sided: `scipy.stats.binomtest(k, n, p, alternative="two-sided")`

Validity requires:

- `n > 0`
- finite expected rate
- positive binomial variance `n*p*(1-p)`

Expected rate for testing is clipped to `(1e-6, 1-1e-6)`.

## 8.5 Tested-window gating before p-values are retained

Define:

- `tested = (~is_low_power) & pro_rate.notna() & (n_known > 0)`

Then p-values are populated only where `valid_exact & tested`.

Important nuance:

- this tested mask is not restricted to alert-eligible off-hours,
- so non-off-hours/non-alert windows can still hold raw p-values,
- but they are excluded from BH families (next section).

## 8.6 BH-FDR adjustment families

For each bucket size `b`, baseline `j in {day, model, primary}`, and tail
`t in {lower, upper, two_sided}`:

- define family as windows satisfying:
  - `bucket_minutes == b`
  - `is_alert_off_hours_window == True`
  - `is_low_power == False`
  - `n_known > 0`
  - non-missing p-value in that channel
- apply Benjamini-Hochberg (`method="fdr_bh"`) at `alpha=fdr_alpha`

Consequences:

- q-values and significance flags are populated only for family members,
- non-family rows keep q-values as missing and significance false,
- multiplicity control is *within-bucket and within-tail and within-baseline*, not global across all hypotheses.

Alias fields:

- `q_value_day`, `q_value_model`, `q_value_primary` are lower-tail aliases.
- `is_significant_day`, `is_significant_model`, `is_significant_primary` are lower-tail aliases.

## 9. Alert Logic and Flag Channels

## 9.1 Support and effect-size gates

For all windows:

- `is_material_primary_shift = |delta_pro_rate_primary| >= primary_alert_min_abs_delta`
- lower shift: `delta_pro_rate_primary <= -threshold`
- upper shift: `delta_pro_rate_primary >= +threshold`

## 9.2 Detector-core robust primary alert (lower-tail only)

Detector-core robust alert is:

\[
\text{is\_primary\_alert\_window} =
\text{tested}
\land \text{is\_alert\_off\_hours\_window}
\land \text{is\_below\_primary\_control\_998}
\land \text{is\_significant\_primary\_lower}
\land \text{is\_material\_primary\_lower\_shift}
\]

This is intentionally conjunctive and conservative.

## 9.3 Two-sided channel fields

Detector also creates:

- `is_primary_spc_998_two_sided = tested & is_outside_primary_control_998`
- `is_primary_fdr_two_sided = tested & is_significant_primary_two_sided`
- `is_primary_any_flag_channel = SPC OR FDR`
- `is_primary_both_flag_channels = SPC AND FDR`

These are not themselves the robust-primary decision rule.

## 10. Summary Metrics and Tables

## 10.1 Aggregate off-vs-on summary

`off_hours_summary` includes:

- counts: total/off/on,
- off-hours ratio,
- off/on pro rates and Wilson intervals,
- `chi_square_p_value` from Pearson chi-square on
  \(\begin{bmatrix}off\_pro & off\_con \\ on\_pro & on\_con\end{bmatrix}\)
  with `correction=False`.

Chi-square guard: only computed when both rows and both columns have positive mass.
Else p-value defaults to 1.0.

## 10.2 Primary-bucket inferential summary

All `off_hours_windows_*` inferential counts are computed from the **primary bucket** and from
**alert-eligible windows** (default pure off-hours), with tested subset excluding low-power.

Includes:

- tested/eligible/low-power counts and fractions,
- control-limit breach counts,
- FDR-significant counts by tail,
- SPC/FDR channel counts and overlaps,
- robust primary alert counts/fractions,
- z/delta extrema,
- model-availability and fit diagnostics,
- run metrics for contiguous robust alert windows.

## 10.3 Run-statistics implementation detail

Alert run segmentation uses sorted primary-bucket windows and `gap_break = diff(bucket_start) > 2 * bucket_minutes`.
This means a single missing bucket interval does not necessarily break runs.

## 10.4 Additional tables

Produced tables include:

- `window_control_profile` (row-level inferential state)
- `model_fit_diagnostics`
- `flag_channel_summary`
- `flagged_window_diagnostics`
- `hourly_distribution`
- `hour_of_week_distribution`
- `date_hour_distribution`
- `date_hour_primary_residual_distribution`

Notable for `date_hour_primary_residual_distribution`:

- `z_score_primary` and deltas use support windows (`~is_low_power & n_known>0`),
- tested-window counts and robust-alert counts are reported separately,
- all-hour context is retained (not only off-hours).

## 11. Detector vs Report-Layer Semantics

This section is critical for interpreting what users see versus what detector summary metrics count.

## 11.1 Payload-layer augmentation in builder

`builder.py` mutates/augments off-hours rows before chart serialization:

- retains detector `is_primary_alert_window` as lower-tail robust alert,
- adds `is_primary_lower_alert_window` alias,
- computes **new** `is_primary_upper_alert_window` with criteria:
  - alert-eligible off-hours,
  - not low-power,
  - above primary 99.8% upper control,
  - significant in upper or two-sided channel (or primary two-sided FDR channel),
  - material positive shift,
- defines `is_primary_two_sided_alert_window = lower OR upper`.

## 11.2 Front-end rendering choices

`main.js` uses two-sided robust field for highlighting:

- `off_hours_control_timeline.flaggedField = is_primary_two_sided_alert_window`
- `off_hours_primary_residual_timeline.flaggedField = is_primary_two_sided_alert_window`
- run overlays also use two-sided robust field.

Tooltips and legends explicitly reference both robust lower-tail and robust upper-tail alerts.

## 11.3 Consequence for interpretation

- Detector summary metric `off_hours_windows_primary_alert` counts lower-tail robust alerts only.
- Chart highlights may include both lower and upper robust alerts.

Therefore, visible robust markers in charts can exceed backend lower-tail robust counts.

## 11.4 Methodology metadata divergence

`detectors/off_hours_methodology.py` contains rich structured method specs and column-method maps,
but code search indicates these specs are currently consumed only by tests, while report methodology
content is built from generic `help_registry.py` definitions/tests-used content.

## 12. Cross-Hearing Baselines (Contextual, Not Detector-Core)

This subsystem is separate from within-hearing off-hours inference.

## 12.1 Feature-vector metrics

Cross-hearing feature vectors include `off_hours_ratio` among other metrics (submission totals, top scores, duplicate metrics, etc.).

## 12.2 Comparator construction

For each metric, comparators include:

- observed,
- expected (median),
- delta,
- percentile rank,
- p10/p50/p90 bands,
- robust z-score (MAD-based),
- empirical two-sided tail p.

## 12.3 Support tiers

By number of comparison reports:

- `<10`: `unavailable`
- `10-19`: `descriptive_only`
- `>=20`: `supported`

## 12.4 Leave-one-out channels

LOO payload includes two channels:

- `cohort_loo` (committee+chamber, then chamber, then global fallback strategy)
- `global_loo`

`selected_channel` picks cohort if available, else global.

## 12.5 Separation from detector-core claims

Cross-hearing comparators are corpus-relative context and do not replace per-window detector baseline/inference.

## 13. Public Interface / Contracted Keys Consumed by Reporting

No code/API changes are made in this phase. Current payload keys function as interface contract.

High-impact off-hours fields consumed by renderer include:

- timeline/funnel/residual core: `pro_rate`, `n_known`, `expected_pro_rate_*`, `control_*`, `z_score_*`, `delta_pro_rate_primary`
- inferential fields: `p_value_*`, `q_value_*`, `is_significant_*`
- eligibility/support: `is_alert_off_hours_window`, `is_low_power`, `is_model_baseline_available`
- robust/channel flags: `is_primary_alert_window`, `is_primary_lower_alert_window`, `is_primary_upper_alert_window`, `is_primary_two_sided_alert_window`, `is_primary_spc_998_two_sided`, `is_primary_fdr_two_sided`
- baseline provenance: `baseline_source`, `model_baseline_source`, `primary_baseline_source`
- summary counters: `off_hours_windows_*`

Finite-safe payload behavior:

- payload built with `version: 4`,
- `_json_safe` converts non-finite floats (`NaN`, `+/-Inf`) to `null`,
- tests assert JSON-safe scalar output.

## 14. End-to-End Algorithm (Implementation Faithful Pseudocode)

```text
INPUT: preprocessed hearing rows with timestamp/minute_bucket/is_off_hours/position labels
PARAMS: buckets, min_window_total, fdr_alpha, model_min_rows, harmonics,
        alert_off_hours_min_fraction, primary_alert_min_abs_delta

FOR each bucket size b:
  bucket rows by floor(event_time, b)
  compute n_total, n_pro, n_con, n_off_hours, n_known, n_unknown
  compute off_hours_fraction and off-hours window labels
  compute pro_rate + Wilson interval + low_power

  build day/global fallback baselines from on-hours known rows
  fit model baseline (weighted binomial GLM with day FE + harmonic hour)
  compose primary baseline = model if available else day/global

  compute control limits (95/99.8) for day/global/model/primary
  tested = (~low_power) & (n_known>0) & pro_rate available

  FOR baseline in {day, model, primary}:
    compute z-score
    compute exact binomial lower/upper/two-sided p-values where valid & tested

  initialize q-values/significance flags

CONCAT all bucket profiles

FOR each bucket b, baseline j, tail t:
  family = alert-eligible off-hours & tested windows in bucket b
  BH-adjust p-values in family -> q-values/significance

derive aliases (q_value_primary = lower-tail q, etc.)
compute material shift flags
compute robust lower-tail primary alert
compute SPC/FDR two-sided channel flags and overlaps

select primary bucket profile
build summary tables and diagnostics
return detector summary + tables
```

## 15. Statistical Claims to Test Coverage Mapping

Coverage grading used here:

- Strong: direct formula/behavior assertions.
- Partial: shape/presence assertions or indirect behavior checks.
- Gap: not explicitly tested.

| Statistical/Method Claim | Primary Tests | Coverage | Notes |
|---|---|---|---|
| Control-limit formula correctness | `test_control_limits_matches_expected_formula_values` | Strong | Includes invalid `n=0` handling. |
| Standardized residual formula | `test_standardized_residual_computes_expected_z_scores` | Strong | Exact expected z-values asserted. |
| Exact binomial p-value correctness | `test_exact_binomial_tail_p_values_match_scipy_reference` | Strong | Lower/upper/two-sided aligned to SciPy. |
| BH FDR with missing p-values | `test_apply_bh_fdr_handles_missing_values` | Strong | NaN exclusion + significance behavior checked. |
| Model baseline availability path | `test_pipeline_uses_model_baseline_when_support_is_sufficient` | Strong | Asserts model availability and primary source. |
| GLM regularized fallback path | `test_pipeline_falls_back_to_regularized_glm_when_standard_fit_fails` | Strong | Monkeypatch forces fallback. |
| Day/global fallback when model unavailable | `test_pipeline_uses_day_global_fallback_when_model_is_unavailable` | Strong | Asserts model unavailable + fallback source. |
| Alert eligibility + support gating | `test_pipeline_alert_eligibility_and_support_gating` | Strong | Mixed-window eligibility and zero tested windows path. |
| Detector table/summary schema stability | `test_off_hours_detector_schema_contract_is_stable` | Strong | Comprehensive table/summary key sets asserted. |
| Off-hours detector output breadth (fields) | `test_off_hours_detector_emits_wilson_and_low_power_columns` | Strong | Broad column coverage; runtime summary fields checked. |
| Chi-square fallback when contingency invalid | `test_off_hours_detector_skips_chi_square_when_contingency_column_is_empty` | Strong | Ensures p-value defaults to 1.0. |
| Primary residual heatmap dimensional behavior | `test_off_hours_primary_residual_distribution_*` | Partial | Ensures bucket/hour presence and tested counts. |
| Off-hours methodology metadata completeness | `test_off_hours_methodology_specs_are_complete`, mapping tests | Strong (module), Gap (runtime integration) | Specs validated but not integrated into report methodology payload. |
| Payload includes augmented upper/two-sided robust fields | `test_payload_color_semantics_cover_key_chart_families` | Partial | Presence asserted; not full semantic equivalence test. |
| Payload finite-safety | `test_payload_values_are_json_safe_scalars` | Strong | Guards against NaN/Inf leakage. |
| Timezone + basic off-hours labeling | `test_add_time_features_builds_minute_bucket_and_off_hours` | Partial | Non-wrap case covered. |
| Hearing-relative time feature derivation | `test_add_time_features_populates_hearing_relative_minutes` | Strong | Cutoff/open/start deltas checked. |
| Cross-hearing support tiers and LOO channels | `test_global_baselines.py` suite | Strong | Tiering, fallback, channel selection, comparator fields. |

## 16. Weakly Tested or Untested Assumptions

1. **Wrap-around off-hours interval logic** (`off_hours_start > off_hours_end`) is implemented but lacks explicit test.
2. **DST/ambiguous local-time handling** (`ambiguous="NaT"`, `nonexistent="shift_forward"`) is implemented but not directly tested in off-hours context.
3. **BH family definition policy** (eligible off-hours only; by bucket, tail, baseline) is implemented but not directly asserted via targeted unit tests.
4. **No global multiplicity control** across bucket sizes/tails/baselines is a policy choice; not stress-tested via simulation.
5. **Model baseline contamination risk**: GLM uses all known windows (including off-hours), not only on-hours; no dedicated sensitivity tests quantify this choice.
6. **Run segmentation gap rule** (`diff > 2*bucket`) may bridge single missing buckets; no explicit run-gap edge-case test.
7. **Small-cell chi-square validity diagnostics** (beyond all-positive-margins check) are not explicitly tested.
8. **Upper-tail robust semantics parity** between payload augmentation and front-end rendering is partially tested by field presence, not end-to-end decision equivalence tests.
9. **Runtime integration of `off_hours_methodology.py` specs** into published methodology content is absent.
10. **Computational scaling/performance** of exact two-sided `binomtest` per window at large row counts is not benchmark-tested.

## 17. Key Methodological Caveats for External Statistical Review

1. Primary robust alerts are intentionally one-sided (lower tail) in detector summary metrics.
2. Front-end robust highlighting is two-sided due report-layer augmentation.
3. Day/global baselines use on-hours-only composition; model baseline does not.
4. FDR control is family-scoped and not global across all tested hypotheses.
5. Off-hours inferential eligibility defaults to pure off-hours windows only (`off_hours_fraction == 1` threshold).
6. Low-power gating is threshold policy (`n_known < min_window_total`), not probabilistic posterior support.
7. Control limits use normal approximation; exact binomial p-values are separately computed.
8. Unknown positions affect window classification (`off_hours_fraction`) and volume context but not inferential pro-rate denominator.

## 18. Expert Statistician Feedback Checklist (Targeted)

### Baseline and model validity

1. Is the mixed strategy (on-hours day/global baseline + all-hours GLM baseline) coherent for causal interpretation of off-hours deviations?
2. Should the model baseline be fit on on-hours only to avoid potential contamination from anomalous off-hours windows?
3. Is the day fixed-effect + harmonic-hour specification adequate, or should additional structure (weekday, interactions, random effects) be considered?
4. Are current model-availability gates (`model_min_rows`, `>=3` unique hours) statistically defensible?

### Multiplicity and inference

5. Is BH by `(bucket, baseline, tail)` the right family partition for this use case?
6. Should a global or hierarchical multiplicity strategy be preferred over per-family BH?
7. Does restricting BH families to alert-eligible off-hours windows induce desirable power/error tradeoffs or problematic selection effects?
8. Is two-sided exact binomial testing the right choice for this discrete, potentially sparse regime?

### Alert logic and operational semantics

9. Is the conjunctive robust-alert rule too conservative, appropriately conservative, or insufficiently conservative?
10. Is `primary_alert_min_abs_delta = 0.03` an appropriate practical-effect threshold across varying supports?
11. Should lower and upper robust alerts be symmetric in detector summary metrics if front-end highlights both?
12. Is the current split between robust alert metrics and SPC/FDR channel metrics interpretable to non-statisticians?

### Power and stability

13. Is `min_window_total` (typically 25 in config) appropriate given bucket sizes up to 240 minutes?
14. Are there preferred alternatives to hard low-power gating (for example, graded confidence or shrinkage) for interpretability?
15. Does run segmentation with `gap > 2*bucket` represent temporal persistence adequately?

### Diagnostic choices

16. Should normal-approximation control limits be retained given exact binomial p-values are already available?
17. Should chi-square aggregate off-vs-on reporting include explicit sparse-cell caveats or exact alternatives?
18. Are current residual/heatmap summaries appropriately weighted for support and inferential eligibility?

### Reporting contract and UX

19. Is detector-vs-UI divergence (lower-only backend robust counts vs two-sided UI robust markers) acceptable if clearly documented?
20. Should methodology content in UI be generated from `off_hours_methodology.py` specs to enforce implementation-doc synchronization?

## 19. Implementation Observations That Could Affect Interpretation

1. `is_primary_spc_998_two_sided` and `is_primary_fdr_two_sided` are computed on all tested windows, not only alert-eligible off-hours windows.
2. Summary channel counts restrict to tested alert-eligible off-hours windows in primary bucket.
3. Raw p-values may exist for windows outside BH families; their q-values remain missing.
4. `q_value_primary` and `is_significant_primary` aliases refer to lower-tail channels.
5. Report payload version is currently `4`; JSON finite-safety is enforced via `_json_safe`.

## 20. Acceptance-Criteria Traceability

The current documentation and code satisfy the requested acceptance criteria as follows:

1. **Reconstruct inferential path from counts to alerts**: covered by Sections 6-10 and pseudocode.
2. **Document every fallback/gating branch**: covered by Sections 4, 7, 8.5, 8.6, 9, 10.
3. **Backend-vs-frontend semantic differences explicit**: covered by Section 11.
4. **Caveats and failure modes actionable**: covered by Sections 16-19.
5. **Constructive critique readiness for expert review**: covered by Section 18 checklist.

---

## Appendix A: Quick Function-Level Implementation Map

- Detector orchestration: `off_hours.py::OffHoursDetector.run`
- Window construction + inference glue: `off_hours_pipeline.py::build_window_control_profile`
- Statistical primitives + GLM fit: `off_hours_statistics.py`
- Summary/tables production: `off_hours_tables.py::build_off_hours_tables_and_summary`
- Off-hours preprocessing and timezone behavior: `preprocess/time.py::add_time_features`
- Detector config schema: `config.py::OffHoursConfig`
- Config-to-detector wiring: `detectors/registry.py::default_detectors`
- Payload shaping and semantic augmentation: `report/rendering/payload/builder.py`
- Front-end chart semantics and tooltips: `report/static/report/main.js`
- Cross-hearing/LOO comparator system: `report/global_baselines.py`

## Appendix B: Explicit Assumptions Used in This Dossier

1. Current repository state is authoritative.
2. Scope is analysis/documentation only; no detector behavior changes proposed here.
3. Timezone interpretation remains `America/Los_Angeles`.
4. Off-hours defaults and bucket defaults are those in current config/code unless overridden per run.
5. Cross-hearing comparators are contextual overlays and not replacements for detector-core inference.
