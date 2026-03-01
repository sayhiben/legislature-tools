# DUP-007 Backtest Memo: VRDB Collision Module

Date: 2026-02-28

## Objective
Backtest VRDB collision evidence before operational rollout using:
1. normal historical hearings
2. known/suspected hearings
3. synthetic hearings

with explicit sensitivity checks for geography conditioning, denominator variant, normalization, and bucket size behavior.

## Reproducible run
Command used:

```bash
cd testifier_audit
python scripts/tests/backtest_vrdb_collision_module.py \
  --log-level WARNING \
  --historical-normal-count 6 \
  --historical-suspect-count 2 \
  --synthetic-replicates 1 \
  --bucket-minutes 15,60,240 \
  --small-bucket-minutes 15 \
  --monte-carlo-draws 96
```

Fixed seed: `6346` (default).

Artifacts written to:
- `output/dup007/vrdb_collision_backtest_case_manifest.csv`
- `output/dup007/vrdb_collision_backtest_case_metrics.csv`
- `output/dup007/vrdb_collision_backtest_scenario_summary.csv`
- `output/dup007/vrdb_collision_backtest_summary.json`
- `output/dup007/vrdb_collision_backtest_memo.md`

## Backtest cohort
Historical normal (6):
- `HB1676-20250203-1330`
- `HB2304-20260218-1030`
- `SB5058-20250129-0800`
- `SB5082-20250113-1400`
- `SB5667-20250225-1330`
- `SSB5570-20250402-1600`

Historical suspected (2):
- `ESSB6346-20260224-0800`
- `SB6346-20260206-1330`

Synthetic (4):
- `synthetic_null_large__r00`
- `synthetic_null_sparse__r00`
- `synthetic_injected_mild__r00`
- `synthetic_injected_strong__r00`

## Scenario matrix
- `state_wa_all_default`
- `state_wa_all_canonical_proxy`
- `state_wa_active_default`
- `county_ki_all_default`
- `city_ki_seattle_all_default`
- `city_ad_benge_all_default`
- `city_ad_missing_all_default`

## Key findings
1. Statewide/county/city-supported baselines (`state_*`, `county_ki_*`, `city_ki_seattle_*`) produced very high alert rates on historical normal holdout (1.00 in this run).
2. Geography-quality stress scenarios with city fallback (`city_ad_benge_*`, `city_ad_missing_*`) had materially lower normal holdout alert rate (0.50) and `fallback_steps=1` as expected.
3. Synthetic null cases stayed unflagged (`synthetic_null_alert_rate=0.00`) while synthetic injected cases were flagged (`synthetic_injected_alert_rate=1.00`) across scenarios.
4. Denominator sensitivity (`all_registrants` vs `active_only`) was large in effect size; state-active scenario remained highly sensitive with large pair overrun ratios.
5. Artifact currently contains one normalization version (`shared_name_normalization_v1:4f856a65edcd`), so cross-version sensitivity is not directly measurable from artifact variants.
6. A normalization proxy comparison (`canonical_medium_proxy` vs default on state/all) yielded zero delta in this run (`median_delta_tail_prob_pairs=0.0`).

## Stability interpretation
- The module clearly separates synthetic injected from synthetic null in this cohort.
- Historical-control false-positive behavior is scenario-dependent and remains high for several baseline choices; this requires review gating before any operational rollout.
- City quality/fallback behavior is functioning and measurable (fallback-step behavior is visible in outputs).

## Rollout gate status
- Backtest memo exists: yes.
- Reproducible fixed-seed pipeline exists: yes.
- Review sign-off before rollout: pending.

Operational rollout should remain blocked until review explicitly accepts the observed control behavior and scenario defaults.

## 2026-03-01 Addendum (Sign-off Refresh)
- Re-analyzed `output/dup007/vrdb_collision_backtest_case_metrics.csv` to add explicit sample-size and interval context.
- Produced:
  - `output/dup007/vrdb_collision_backtest_threshold_feasibility.csv`
- Key additions from this addendum:
  - holdout-normal sample size per scenario is small (`n=2`), yielding wide Wilson intervals;
  - despite uncertainty width, point estimates remain elevated (`0.5` to `1.0` depending on scenario);
  - threshold-feasibility scan found no threshold on `full_tail_prob_pairs` that simultaneously met:
    - holdout-normal alert rate `<= 0.20`, and
    - synthetic-injected alert rate `>= 0.80`.
- Conclusion remains unchanged:
  - synthetic null/injected separation is strong;
  - historical-control false-positive behavior is still too high for rollout approval;
  - review sign-off should remain pending and rollout blocked.

## 2026-03-01 Addendum (Instrumentation Rerun)
- Backtest pipeline was updated to emit interval-aware and operating-point diagnostics directly:
  - Wilson interval bounds for alert rates by family/split.
  - Scenario-level threshold-feasibility scan against explicit targets:
    - holdout-normal alert rate `<= 0.20`
    - synthetic-injected alert rate `>= 0.80`
- Reproducible rerun completed (seed `6346`) and regenerated:
  - `output/dup007/vrdb_collision_backtest_case_metrics.csv`
  - `output/dup007/vrdb_collision_backtest_scenario_summary.csv`
  - `output/dup007/vrdb_collision_backtest_summary.json`
  - `output/dup007/vrdb_collision_backtest_memo.md`
  - `output/dup007/vrdb_collision_backtest_threshold_feasibility.csv`
- Rerun outcomes:
  - holdout-normal sample sizes remain `n=2` per scenario (wide Wilson intervals).
  - holdout-normal alert rates remain elevated (`0.50` to `1.00`).
  - synthetic-injected alert rate remains `1.00`.
  - no scenario has a feasible threshold under the 0.20/0.80 targets (`threshold_feasible_count=0` everywhere).
- Operational interpretation remains unchanged:
  - synthetic separation is strong;
  - historical-control behavior is still too high for rollout approval;
  - sign-off stays pending and rollout remains blocked.
