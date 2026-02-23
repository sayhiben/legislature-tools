# Legislative Hearing Data Analysis Plan: Anomaly Detection and Entity Resolution

### Phase 1: Entity Resolution and Standardization (The Foundation)
Before any statistical counting begins, you must unify variations of the same identity. If you skip this, your duplicate counts will be artificially low, and your miss rate against the voter file artificially high.

* **String Normalization:** Convert all names to lowercase, strip punctuation, remove whitespace padding, and standardize suffixes (e.g., "jr", "sr", "iii").
* **Fuzzy Matching:** Use algorithms designed specifically for names. Jaro-Winkler distance is highly recommended here, as it gives more weight to the beginning of strings, which is how humans typically spell names even when making typographical errors.
* **Nickname Expansion:** Map your common nickname lookup to a standardized root. Both "Bill" and "Will" should point back to "William" during the comparison phase. 
* **Organization and Time Heuristics:** If two identically named records have the exact same `Organization` or signed in within exactly one minute of each other, the probability that they are the same person approaches 1, regardless of name commonality.

---

### Phase 2: Baseline Probability and Name Frequency Analysis
To prove a duplicate is anomalous, you must prove it defies natural probability. A "John Smith" appearing three times in a list of 5,000 testifiers might be expected. A "Zephyr Bartholomew" appearing twice is a massive red flag.

* **Establish the Base Rate:** Calculate the relative frequency of every standardized name in your registered voters list. Let p_i be the probability of a randomly selected voter having name i.
* **Calculate Expected Values:** For a testifier list of size N, the expected number of times a benign name collision should occur is λ = N * p_i.
* **Poisson Probability Testing:** Because the probability of any specific name being drawn is very small, and the sample size N is large, the Poisson distribution is the correct mathematical approach to find the probability of observing k or more sign-ins for a specific name. 
  * Formula: P(X ≥ k) = 1 - sum_{j=0}^{k-1} (λ^j * e^-λ) / j!
* **Assigning Suspicion Scores:** Any name where P(X ≥ k) falls below your chosen significance level (e.g., α = 0.05 or 0.01) should be flagged as a statistically significant anomaly.

---

### Phase 3: Positional Skew and "Miss Rate" Analysis
Once entities are resolved and probabilities calculated, you need to determine if the anomalies disproportionately favor the Pro or Con side.

* **Duplicate Skew Analysis:** Isolate the population of flagged anomalous duplicate testifiers. Use a **Chi-Square Goodness of Fit test** to compare the distribution of their positions (Pro/Con/Other) against the distribution of the general (non-flagged) testifier population. This will definitively tell you if the duplicates are systematically pushing one side of the debate.
* **Miss Rate Calculation:** Attempt to join your resolved testifier list to the voter file. A testifier not found in the voter file is a "miss" (potentially out-of-state, unregistered, or a fabricated identity).
* **Miss Rate Proportion Testing:** Calculate the proportion of misses for the Pro side (p_hat_pro) and the Con side (p_hat_con). Use a **Two-Proportion Z-Test** to determine if one side has a statistically significant higher rate of unverified identities.
  * Formula: Z = (p_hat_pro - p_hat_con) / sqrt(p_hat * (1 - p_hat) * (1/n_pro + 1/n_con))

---

### Phase 4: Time-Series Anomaly Detection
Astroturfing campaigns often rely on scripts or coordinated groups signing in at the exact same time. Your `Datetime` column is highly valuable here.

* **Arrival Rate Analysis:** Group sign-ins by minute. Legitimate sign-ins usually follow a relatively smooth curve leading up to a hearing. Coordinated attacks show massive, unnatural spikes.
* **Inter-Arrival Time:** Calculate the time difference between consecutive sign-ins for the same position. A sudden cluster of sign-ins with near-zero inter-arrival time is highly anomalous.

---

### Phase 5: Visualizations and Reporting
The final tool needs to clearly explain these mathematical concepts to non-technical users (like journalists, lawmakers, or the public).

* **The "Suspicion Scatterplot":** Y-axis: Number of appearances in the testifier list. X-axis: Frequency of the name in the voter file. Color-code dots by Position. Dots high on the Y-axis but low on the X-axis represent clear anomalies.
* **Cumulative Sign-in Curve:** A line chart showing cumulative sign-ins over time (X-axis is `Datetime`, Y-axis is total count), with one line for Pro and one for Con. Vertical steps or sheer cliffs in the line indicate bot-like bursts.
* **The Verification Funnel:** A stacked bar chart per position showing the total testifiers, the number successfully matched to the voter file, and the resulting "miss" segment.
* **Anomaly Table:** A simple table listing the highest-scoring anomalous names, their position, their observed count, their expected count, and their p-value (Suspicion Score).

---

### Common Failure Modes & Mitigations

* **The "Family" False Positive:** A family of four registered voters sharing the same last name and organization might sign in from the same IP at the same time. *Mitigation:* Ensure your baseline analysis looks at *full name* (first and last) combinations, not just last names.
* **The Out-of-State Legitimate Testifier:** National organizations often rally out-of-state members for state-level controversial bills. These will heavily skew your "Miss Rate" against the state voter file. *Mitigation:* Explicitly label the Miss Rate as "Unverified against WA Voter File" rather than "Fake", and cross-reference with the `Organization` column to see if out-of-state advocacy groups explain the spike.
* **Multiple P-Value Problem:** Because you are running a Poisson test on potentially thousands of names, you will get false positives purely by chance. *Mitigation:* Apply a **Bonferroni correction** or adjust your False Discovery Rate (FDR) using the Benjamini-Hochberg procedure when setting your alpha threshold.