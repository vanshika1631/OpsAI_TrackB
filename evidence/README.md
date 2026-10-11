# Evidence provenance

## Historical evidence

`b2_historical/` is copied unchanged from the team's local starter-kit evidence
directory. Captures are dated September 25, 2026. They document the original
false-green behavior, path-only correction, readiness hardening, bad-path
re-injection, corrupt artifact, probe timeline, and latency summary. The latency
summary reports **300 sequential requests per endpoint after 20 warm-ups**;
individual latency samples are unavailable. Do not calculate new intervals from
the three reported summary quantiles.

The original B1 command line, lag log and producer delivery counters are
unavailable. Its reported 73,607 lag is inconsistent with a clean, finite
14,076-message replay. Looping or residual backlog are plausible, not established.
The original B3 model and evaluation script are unavailable. New B3 measurements
must not be described as confidence intervals for the slide's 0.208/0.303 pair.

## Supplemental evidence (October 10, 2026, America/New_York)

`supplemental/results.json` records input SHA-256 hashes, row counts, environment,
new B3 model protocol and results, and B2 application-level status checks.

- `b1_save_trials.csv` / `b1_save_summary.csv`: 7 randomized trials for each
  writer and 0/5,000/20,000/80,000 existing rows. Same new batch of 25. File
  construction is outside the timer. Append footer close is outside the timed
  append, exposing the performance/durability tradeoff rather than equal
  durability. Warm-cache host measurements; not fsync or power-loss benchmarks.
- `b1_stream_summary.csv`, `b1_stream_telemetry.csv`, `b1_stream_*.log`: final
  isolated runs, three per mode, fresh topic/group per run, one partition and
  consumer, target 1,500 raw messages/s. Deadlined pacing in the experiment
  harness differs from the original replay script's per-message sleep. Achieved
  rate includes delivery acknowledgement flush. Lag is broker log-end minus
  committed offset, sampled approximately every second. Completion is observed
  at polling resolution, not exact final-row latency. All 11,754 feature rows
  equal the single-pass reference in every run; all 14,076 raw messages acknowledged.
- `b1_sigkill.json`: actual Linux SIGKILL after 1,000 completed rows. Standard
  PyArrow readability measured, not forensic recovery. Rewrite 1,000 readable;
  append 0 readable. No Kafka commits occur in this isolated writer harness.
- `b3_drift.csv`: direct SciPy KS calculation, independently checking the
  presentation's Evidently finding. Numerical p=0 means floating-point underflow,
  not mathematically zero probability. Serial dependence makes KS p-values
  descriptive here; effect size and controlled paired inputs matter more.
- `b3_paired_scores.csv`: new logistic regression, scaler fitted on 371 training
  rows, chronological test indices 522..632 inclusive (111 rows), 20-row purge
  before the training boundary, 20 unavailable forward labels removed at the end.
  No test rows used for fitting the model/scaler. Label cutoff uses the whole
  reference window, as in the starter's retrospective definition; labels are
  held fixed for the synthetic perturbation. This is NOT prospective validation.
  A training-only 90th-percentile cutoff produces zero test positives, so AP
  cannot meaningfully evaluate that prospective variant on this short tail.
- `b3_bootstrap.csv`: paired circular moving-block bootstrap, 3,000 attempted
  draws at each block length 10 and 20, seeds 889 and 899. Reported percentile
  intervals condition on resamples containing both classes. Counts of valid
  draws are recorded. Training uncertainty and live-label uncertainty excluded.
- `b3_thresholds.csv`: thresholds .3/.5/.7/.9 on the same 111 paired rows;
  illustrative sensitivity, not tuned operating points. At zero predictions,
  precision is stored as 0 by the scorer's zero-division convention and is
  conceptually undefined.

Timing pilots overlapped other calculations and were superseded: storage
timings and all six Kafka runs were recollected serially for the final tables.

## Environment

Windows 11 host; Python 3.12.14. Package versions in `results.json` and
`experiments/requirements.txt`. Docker Engine 29.1.3; Linux container for SIGKILL.

- Kafka image ID: `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- SIGKILL image ID: `sha256:7d5501020efe94091d088f9cd796bd0166e6507f48a63f53fdb247beadf3ee00`
- Linux image used scikit-learn 1.9.1, PyArrow 15.0.2, SciPy 1.17.1.

Image IDs identify local images, not registry pullable digests. Rebuild recipes
are provided, but hardware and image differences can change timing results.
