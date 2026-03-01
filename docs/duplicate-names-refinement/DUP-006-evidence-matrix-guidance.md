# DUP-006 Analyst Guidance Memo: Cross-Family Evidence Matrix

Date: 2026-02-28  
Work item: DUP-006

## Purpose
The cross-family evidence matrix is an interpretation layer for disagreement and agreement between:
- Duplicate collision burden (report-layer duplicate baseline)
- VRDB collision-null evidence (sidecar null model)
- Behavioral timing evidence (off-hours alert behavior)

It is intentionally additive. It is not a merged score.

## Interpretation Rules
1. VRDB collision evidence may increase concern, but does not suppress other flags.
2. Existing timing/content/metadata evidence may increase concern, but does not erase a VRDB extreme.
3. Agreement across evidence families strengthens follow-up priority.
4. Disagreement narrows the question being answered; it does not imply one method failed.
5. No composite score in v1; review separate evidence-family columns.

## Scenario Guidance
### VRDB high + duplicate normal
- Meaning: VRDB collision-null is elevated while report-layer duplicate burden is normal.
- Action: investigate null-model collision interpretation first; do not suppress other families.

### Duplicate high + VRDB normal
- Meaning: duplicate burden is elevated relative to report baseline without VRDB-null elevation.
- Action: treat as duplicate concentration evidence; avoid over-claiming VRDB anomaly.

### Duplicate high + VRDB high
- Meaning: both name-evidence families are elevated in aligned windows.
- Action: concordance increases follow-up priority.

### Name families normal + behavioral high
- Meaning: name evidence is not carrying the case while behavioral timing is elevated.
- Action: prioritize behavioral hypotheses; avoid escalation based on name evidence alone.

## Operational Notes
- Matrix rows are generated per available bucket window and include scenario counts.
- Tooltips provide window counts, share of aligned windows, and first/last matching windows.
- The matrix should be used with detector-level details, not as a replacement for them.
