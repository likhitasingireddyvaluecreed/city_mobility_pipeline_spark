# The Great City Mobility Mystery — Investigation Report

## Scope and workflow

This repository has two separate parts: `main.py` runs extraction, profiling, transformation, validation, and saving; `analytics.ipynb` performs the business investigation using the saved clean datasets. The notebook must be run after the pipeline. It saves analytical Parquet datasets under `outputs/analytics/`.

## Data quality and validation

The saved validation summary records these input/clean/quarantined counts:

| Dataset | Input | Clean | Quarantined |
|---|---:|---:|---:|
| Stations | 8 | 8 | 0 |
| Bikes | 250 | 213 | 37 |
| Users | 1,600 | 1,600 | 0 |
| Trips | 4,203 | 4,130 | 73 |
| Maintenance | 300 | 289 | 11 |

The bike quarantine includes 37 rows whose `purchase_year` is `unknown`. The current rule quarantines the whole bike row. Trip foreign-key validation checks references against the original transformed parent datasets, not the clean parent outputs. Consequently, some trips can remain clean even when their bike row is later absent from `outputs/clean/bikes`. The notebook explicitly reports this join-quality issue. A future refinement could retain the bike row and flag its invalid purchase year rather than removing the entire bike dimension record.

## Findings available in the saved notebook outputs

### Station balance

The previously generated station-flow output shows the following net flow, calculated as departures minus arrivals:

| Station | Departures | Arrivals | Net flow |
|---|---:|---:|---:|
| B007 — Museum Road | 289 | 331 | -42 |
| B006 — Airport Link | 325 | 294 | +31 |
| B005 — Old Market | 312 | 335 | -23 |
| B004 — University Gate | 305 | 327 | -22 |
| B002 — Riverfront | 318 | 299 | +19 |
| B008 — Lake View | 317 | 301 | +16 |
| B001 — Central Square | 310 | 296 | +14 |
| B003 — Tech Park | 309 | 302 | +7 |

These are net trip flows over the observed dataset, not a direct measurement of real-time bike inventory. The notebook now also calculates station flow by hour to investigate time-specific patterns.

### Membership usage

| Membership | Completed trips | Average duration (minutes) |
|---|---:|---:|
| Annual | 922 | 208.93 |
| Student | 556 | 206.57 |
| Casual | 524 | 193.75 |
| Monthly | 483 | 208.50 |

These counts describe completed trips joined to the clean users dimension; they should not be interpreted as unique user counts.

### Bike type and join quality

| Bike type | Completed trips | Average duration (minutes) |
|---|---:|---:|
| Standard | 873 | 202.32 |
| Electric | 462 | 208.22 |
| E-bike | 422 | 205.53 |
| Cargo | 369 | 207.43 |
| NULL after clean-bike join | 359 | 205.08 |

The 359 NULL bike-type rows (14.45% of 2,485 completed trips) are explained by the clean-bike join behavior described above. Raw bike type values were populated; this NULL group should not be reported as evidence that 14.45% of raw bike types were missing.

### Maintenance issues

| Issue type | Reports | Average repair time (minutes) |
|---|---:|---:|
| Vandalism | 60 | 143.88 |
| Flat tire | 53 | 105.38 |
| Chain | 50 | 106.44 |
| Brake | 44 | 133.48 |
| Other | 43 | 134.81 |
| Battery | 39 | 116.41 |

The source also contains a `resolution` field. The notebook now reports `fixed` and `replaced` as resolved, `pending` as pending, and blank/null values as unknown.

## Analytical techniques

The notebook includes three window-function uses: station departure ranking within districts, cumulative daily trip totals, and ranking the five longest trips within each bike type. It includes three Spark SQL analyses (hourly demand, station net flow, and maintenance issue summary) and compares each with a DataFrame equivalent using `exceptAll` in both directions. Two formatted execution plans are inspected: station flow and bike-type usage. The station-flow plan can scan completed trips separately for departures and arrivals; the saved plan reported `isFinalPlan=false`, while the saved bike-type plan reported `isFinalPlan=true`. These plans are descriptive; no runtime speedup claim is made.

## Long-trip review and limitations

There is no official long-trip threshold in the source data. The notebook therefore uses 180 minutes (three hours) only as an exploratory flag for review. Flagged trips are not removed or quarantined by this analysis. Other limitations include the small synthetic dataset, no time-stamped bike inventory snapshots, and the need to rerun the notebook to generate the new `outputs/analytics/` Parquet tables.

## Run order

1. Install dependencies from `requirements.txt`.
2. From the repository root, run `python main.py`.
3. Open `analytics.ipynb` from the same root and use **Restart Kernel and Run All**.
4. Run `python -m pytest -v tests/test_validation.py`.

Test execution must be confirmed in the user's configured Python/Java/Spark environment; the code audit environment did not have PySpark installed, so it could not verify a passing test run.
