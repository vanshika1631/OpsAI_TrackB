# Notes about our evidence

## Earlier results

We copied `b2_historical/` from our local starter-kit evidence folder without
changing the logs. They are dated September 25, 2026. They show the original
health-check problem, the path-only fix, the new readiness check, the reintroduced
bad path, the corrupt model test, the probe timeline and the latency summary.
The latency test used **300 sequential requests per endpoint after 20 warm-ups**.
We no longer have the individual latency samples, so we cannot calculate a new
confidence interval from that summary.

We do not have the original B1 command, lag log or delivery counters. A clean,
one-pass replay of 14,076 messages would not explain lag of 73,607. The replay may
have looped or started with a backlog, but we cannot confirm either. We also do
not have the original B3 model or evaluation script. The new B3 intervals apply
to the new model, not the 0.208/0.303 results from the slides.

## New tests (October 10, 2026, America/New_York)

We saved input SHA-256 hashes, row counts, package versions, the new B3 setup and
results, and the B2 application checks in `supplemental/results.json`.

- `b1_save_trials.csv` / `b1_save_summary.csv`: 7 randomized trials for each
  writer and 0/5,000/20,000/80,000 existing rows. Same new batch of 25. File
  construction is outside the timer. We also left footer closure outside the
  append timer, so the two methods do not offer equal durability at the timed
  point. These were warm-cache tests, not fsync or power-loss tests.
- `b1_stream_summary.csv`, `b1_stream_telemetry.csv`, `b1_stream_*.log`: final
  isolated runs, three per mode, fresh topic/group per run, one partition and
  consumer, target 1,500 raw messages/s. The test schedules each send against a
  target time, unlike the original script's sleep after each message. Achieved
  rate includes delivery acknowledgement flush. Lag is broker log-end minus
  committed offset, sampled approximately every second. Completion is observed
  when the test polls, rather than at the exact final-row time. All 11,754 feature rows
  equal the single-pass reference in every run; all 14,076 raw messages acknowledged.
- `b1_sigkill.json`: actual Linux SIGKILL after 1,000 completed rows. Standard
  PyArrow reads were tested; we did not attempt forensic recovery. Rewrite left
  1,000 readable rows and append left zero. This test did not commit Kafka offsets.
- `b3_drift.csv`: direct SciPy KS calculation, independently checking the
  presentation's Evidently finding. Numerical p=0 means floating-point underflow,
  not a mathematically zero probability. Since rows are time-dependent, we rely
  more on the size of the change and the paired inputs than on the p-value alone.
- `b3_paired_scores.csv`: new logistic regression, scaler fitted on 371 training
  rows, chronological test indices 522..632 inclusive (111 rows), 20-row purge
  before the training boundary, 20 unavailable forward labels removed at the end.
  No test rows used for fitting the model/scaler. Label cutoff uses the whole
  reference window, as in the starter's retrospective definition; labels are
  held fixed for the changed inputs. This is a retrospective comparison.
  A training-only 90th-percentile cutoff produces zero test positives, so AP
  cannot meaningfully evaluate that prospective variant on this short tail.
- `b3_bootstrap.csv`: paired circular moving-block bootstrap, 3,000 attempted
  draws at each block length 10 and 20, seeds 889 and 899. Reported percentile
  intervals use only samples with both classes, and we recorded how many were
  valid. They do not include uncertainty from refitting the model or live labels.
- `b3_thresholds.csv`: thresholds .3/.5/.7/.9 on the same 111 paired rows;
  used to explore sensitivity, not choose an operating threshold. When there are
  no alerts, the metric code stores precision as 0 to avoid division by zero.
  We describe it as undefined in the report.

Some early timing runs overlapped other calculations. We replaced them by
rerunning the storage tests and all six Kafka tests one at a time for the final tables.

## Environment

Windows 11 host; Python 3.12.14. Package versions in `results.json` and
`experiments/requirements.txt`. Docker Engine 29.1.3; Linux container for SIGKILL.

- Kafka image ID: `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`
- SIGKILL image ID: `sha256:7d5501020efe94091d088f9cd796bd0166e6507f48a63f53fdb247beadf3ee00`
- Linux image used scikit-learn 1.9.1, PyArrow 15.0.2, SciPy 1.17.1.

These IDs identify the local images we used; they are not registry digests that
can be pulled directly. The README explains how to rebuild, but different
hardware or images can change the timings.
