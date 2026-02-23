# Statistical Analysis Plan: WA Legislature Hearing Sign-in Anomalies

## 1. Data Normalization & Entity Linkage (Phase I)
Before calculating frequencies, the raw text must be converted into a "Canonical Entity Key" to account for human error and formatting variance.

* **String Standardizing:** * Lowercase all strings, remove non-alphanumeric characters (except commas).
    * Split `Name` into `[last_name]`, `[first_name]`, and `[middle_initial]`.
* **Fuzzy Key Generation:**
    * **Nickname Mapping:** Apply the provided lookup table (e.g., "William" -> "Bill") to create a `Normalized_First_Name` field.
    * **Phonetic Encoding:** Use **Double Metaphone** or **Soundex** on last names to catch common phonetic misspellings (e.g., "Smithe" vs "Smith").
* **Jaro-Winkler Distance ($d_j$):** * Calculate string similarity between the testifier list and the Registered Voter list. 
    * Establish a threshold (e.g., $d_j > 0.92$) for a "Likely Match" to handle typos in the public dataset.

## 2. Establishing the "Benign Baseline" (Phase II)
To prove manipulation, you must determine the probability of "Name Collision" ($P_c$) occurring naturally in Washington State.

* **The Birthday Paradox Application:** In a large population, the probability of two people sharing a name is higher than intuition suggests. 
* **Voter File Frequency Analysis:**
    * Calculate the **Relative Frequency** of every name in the WA Voter File.
    * **Monte Carlo Simulation:** Draw 1,000 random samples from the Voter File, each matching the exact $N$ (size) of your testifier list. 
    * **Result:** This yields a **Normal Distribution** of "Expected Duplicates" for a group of that size.

## 3. Statistical Methodologies (Phase III)
These specific tests will identify if the testifier list deviates from the expected baseline.

### A. The "Miss Rate" Partitioning (Chi-Squared Test)
* **Goal:** Determine if one position (Pro/Con) has a significantly higher rate of "Unregistered" names.
* **Method:** Perform a **Pearson’s Chi-squared ($\chi^2$) test of independence**.
    * Compare the observed counts of "Matched to Voter File" vs "Unmatched" across the "Pro" and "Con" categories.
    * **Anomaly Trigger:** A $p$-value $< 0.05$ suggests the distribution of unmatched names is not random and is likely tied to the position taken.

### B. Temporal Density (Poisson Process)
* **Goal:** Identify "Sign-in Bursts" that suggest automated or coordinated entry.
* **Method:** Model sign-in arrivals as a **Poisson Distribution**.
    * Calculate the **Inter-Arrival Time (IAT)** for each position.
    * **Anomaly Trigger:** Identify "Clusters" where the IAT is significantly lower than the mean (e.g., 50 sign-ins in 60 seconds). If these clusters consist of duplicate names or "Unmatched" names, it is a high-confidence indicator of manipulation.

### C. Z-Score Weighting for Name Rarity
* **Formula:** $W_d = \frac{1}{Freq_{voter}}$
* **Logic:** A duplicate for a rare name (e.g., "Zebulon Vark") is weighted more heavily as an anomaly than a duplicate for a common name (e.g., "John Smith").
* **Metric:** Calculate the **Z-Score** of the duplicate rate for each position relative to the Monte Carlo baseline. $Z > 3$ is a "Hard Anomaly."

## 4. Visualization & Reporting Strategy
Visuals should focus on the *divergence* between positions and the baseline.

| Visualization | Purpose | Key Indicator |
| :--- | :--- | :--- |
| **Position Match Histogram** | Show % of Pro vs Con matched to Voter File. | Disparity in "Unmatched" rates between sides. |
| **Temporal Burst Chart** | Time (X) vs Sign-in Volume (Y) by Position. | Vertical spikes in one color but not the other. |
| **Name Rarity Scatterplot** | Name Frequency (X) vs Duplication Count (Y). | High Y-values for low X-values (Rare name duplicates). |
| **Inter-arrival Heatmap** | Heatmap of time gaps between sign-ins. | "Hot zones" indicating rapid-fire batch entries. |

## 5. Failure Modes & Mitigation
* **The "Correction" False Positive:** A user signs in, sees a mistake, and signs in again.
    * *Mitigation:* Collapse duplicates that occur within a 10-minute window for the same name/position into a single "Cleaned Entry" before analysis.
* **Common Name Collision:** High population density of names like "Maria Garcia."
    * *Mitigation:* Use the **Voter Frequency Weighting** described in Section 3C. Do not treat "Common Name" duplicates as anomalies unless they exceed the simulated baseline.
* **Geographic Validity:** The testifier might be a WA resident not yet on the voter rolls (e.g., recently moved).
    * *Mitigation:* Report the "Miss Rate" as "Non-Voter Match" rather than "Fraudulent" to maintain statistical neutrality and avoid libel.