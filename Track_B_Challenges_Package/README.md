# Track B Challenges Package

This is an addition to the starter kit you already have, not a replacement.
Nothing here overwrites your working environment except the two files named
explicitly below, and only those two.

## What to do with each file

1. **code_updates/features/featurizer.py**
   Copy this over your existing `starter-kit/features/featurizer.py`.
   Adds an optional `--parquet_path` flag. If you never pass that flag,
   behavior is identical to what you already have. This version also
   commits Kafka offsets after each flush, so `kafka-consumer-groups.sh
   --describe` shows real CURRENT-OFFSET and LAG values instead of `-`.
   This matters directly for B1: without it, lag is not observable at all.

2. **code_updates/monitoring/drift_report.py**
   Copy this over your existing `starter-kit/monitoring/drift_report.py`.
   Adds optional `--reference_features` and `--current_features` flags for
   comparing two separate files directly. If you don't pass them, it falls
   back to the original single-file, percentage-split behavior. If you
   pass only one of the two flags, it exits with an error rather than
   silently ignoring it.

3. **data/sample_ticks_long.jsonl**
   Copy into `starter-kit/data/sample/`. A longer, monotonic-timestamp
   version of the sample data (about 14,000 messages), sized so that even
   at a high replay rate there is enough real time to take multiple lag
   observations, not just one.

   **Finding the right replay rate:** start the workload at `--rate 500`.
   While the finite replay runs, sample the consumer group's lag several
   times over about one minute (for example, every 10 to 15 seconds),
   rather than checking only once. If lag does not show a sustained
   upward trend across those samples, restart from a clean B1 baseline
   and retry at a higher rate (750, 1000, ...). Repeat until you observe
   a sustained upward trend. Record the rate you ultimately used as part
   of your baseline.

4. **data/sample_ticks_drifted.jsonl**
   Copy into `starter-kit/data/sample/`. A modified version of the sample
   data with bid/ask spreads widened.

5. **compose_variants/docker-compose.b2-challenge.yaml**
   Copy into `starter-kit/docker/`. An alternate compose file using the
   same service names as the default stack (broker, mlflow, scorer).
   **Stop the default stack first** (`docker compose -f docker/compose.yaml
   down`) before starting this one, or Docker will fail with a naming/port
   conflict. Run it with `docker compose -f docker/docker-compose.b2-challenge.yaml
   up -d --build` instead of the default `docker/compose.yaml`. Your
   original compose file is untouched.

## If you've already modified featurizer.py or drift_report.py yourself

Back up your version first (`cp featurizer.py featurizer.py.bak`) before
copying the new one over it, so you don't lose any changes you made.
