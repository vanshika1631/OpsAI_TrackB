"""Reproduce supplemental B1/B2/B3 evidence without Kafka. Run from any cwd.

Uses repository inputs, a newly fitted model, and explicit seeds. Historical
slide metrics are not reconstructed. Outputs go to evidence/supplemental.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import sys
import tempfile
import time
from importlib.metadata import version

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats import ks_2samp
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "starter-kit"))
from features.featurizer import (FEATURE_COLUMNS, TradeIntensityWindow,
    _ParquetAppender, compute_features, parse_ticker_message, parse_timestamp)

OUT = ROOT / "evidence" / "supplemental"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BASELINE = load_module("baseline", ROOT / "Track_B_Challenges_Package/code_updates/features/featurizer.py")


def features(path):
    rows, previous, window = [], None, TradeIntensityWindow()
    timestamps = []
    lines = path.read_text().splitlines()
    for line in lines:
        tick = parse_ticker_message(line)
        if tick is None or not tick.get("timestamp"):
            continue
        ts = parse_timestamp(tick["timestamp"])
        timestamps.append(ts)
        row = compute_features(tick, previous)
        row["trade_intensity"] = window.update(ts)
        previous = row["midprice"]
        rows.append(row)
    return pd.DataFrame(rows), {
        "raw_messages": len(lines), "feature_rows": len(rows),
        "backward_timestamp_steps": sum(b < a for a, b in zip(timestamps, timestamps[1:])),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def b1(reference):
    rows = reference.to_dict("records")
    long_df, audit = features(ROOT / "starter-kit/data/sample/sample_ticks_long.jsonl")
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        original, changed = temp / "original.parquet", temp / "append.parquet"
        writer = _ParquetAppender(str(changed))
        for i in range(0, len(long_df), 25):
            batch = long_df.iloc[i:i+25].to_dict("records")
            BASELINE._append_parquet(batch, str(original))
            writer.append(batch)
        writer.close()
        pd.testing.assert_frame_equal(pd.read_parquet(original), pd.read_parquet(changed), check_exact=True)
        pd.testing.assert_frame_equal(long_df, pd.read_parquet(changed), check_exact=True)
        audit["output_rows_exactly_equal"] = len(long_df)

        # Randomized trials; construct the pre-existing file outside the timer.
        # Both modes use the same batch, schema, host and storage. Warm-cache test.
        schedule = [(mode, size, rep) for mode in ["rewrite", "append"]
                    for size in [0, 5000, 20000, 80000] for rep in range(7)]
        random.Random(879).shuffle(schedule)
        trials = []
        for mode, size, rep in schedule:
            path = temp / f"trial-{mode}-{size}-{rep}.parquet"
            seed = (rows * (size // len(rows) + 1))[:size]
            writer = _ParquetAppender(str(path)) if mode == "append" else None
            if size:
                if writer:
                    writer.append(seed)
                else:
                    BASELINE._append_parquet(seed, str(path))
            start = time.perf_counter_ns()
            if writer:
                writer.append(rows[:25])
            else:
                BASELINE._append_parquet(rows[:25], str(path))
            elapsed = (time.perf_counter_ns() - start) / 1e6
            if writer:
                writer.close()
            assert len(pd.read_parquet(path)) == size + 25
            trials.append({"mode": mode, "existing_rows": size, "repeat": rep,
                           "save_ms": elapsed})
        pd.DataFrame(trials).to_csv(OUT / "b1_save_trials.csv", index=False)
        summary = pd.DataFrame(trials).groupby(["mode", "existing_rows"]).save_ms.agg(["count", "median", "min", "max"])
        summary.to_csv(OUT / "b1_save_summary.csv")
    return audit


def b3(ref, cur):
    assert len(ref) == len(cur)
    drift = []
    for column in FEATURE_COLUMNS:
        stat = ks_2samp(ref[column], cur[column])
        drift.append({"feature": column, "ks_statistic": float(stat.statistic),
                      "ks_pvalue": float(stat.pvalue), "mean_reference": float(ref[column].mean()),
                      "mean_current": float(cur[column].mean())})
    pd.DataFrame(drift).to_csv(OUT / "b3_drift.csv", index=False)
    raw_spread_ref = ref.midprice * ref.spread_normalized
    raw_spread_cur = cur.midprice * cur.spread_normalized
    ratio = raw_spread_cur / raw_spread_ref

    # Fixed temporal 60/20/20 split BEFORE dropping unlabeled tail; purge
    # horizon from training boundary. Starter-style retrospective labels use
    # a reference-wide percentile, explicitly NOT a prospective evaluation.
    horizon, n = 20, len(ref)
    train_end, test_start = int(n * .6) - horizon, int(n * .8)
    # Hold reference labels fixed to isolate the synthetic input perturbation.
    # Trade prices are identical but rounding changes a few midprice returns.
    assert np.array_equal(ref.price.to_numpy(), cur.price.to_numpy())
    future_vol = ref.midprice_returns.rolling(horizon).std().shift(-horizon)
    training_tau = float(future_vol.iloc[:train_end].quantile(.9))
    tau = float(future_vol.quantile(.9))
    labels = (future_vol >= tau).astype(int)
    train = np.arange(train_end)
    test = np.arange(test_start, n - horizon)
    assert future_vol.iloc[test].notna().all()
    scaler = StandardScaler().fit(ref.iloc[train][FEATURE_COLUMNS])
    model = LogisticRegression(max_iter=500, class_weight="balanced", random_state=879)
    model.fit(scaler.transform(ref.iloc[train][FEATURE_COLUMNS]), labels.iloc[train])
    y = labels.iloc[test].to_numpy()
    p = model.predict_proba(scaler.transform(ref.iloc[test][FEATURE_COLUMNS]))[:, 1]
    q = model.predict_proba(scaler.transform(cur.iloc[test][FEATURE_COLUMNS]))[:, 1]
    pd.DataFrame({"row_index": test, "label": y, "reference_score": p, "current_score": q}).to_csv(OUT / "b3_paired_scores.csv", index=False)

    # Paired circular moving-block bootstrap preserves local dependence and
    # pairing. Intervals are conditional on this fitted model and proxy labels.
    bootstrap = []
    intervals = {}
    for block in [10, 20]:
        rng = np.random.default_rng(879 + block)
        for iteration in range(3000):
            starts = rng.integers(0, len(y), int(np.ceil(len(y) / block)))
            ix = np.concatenate([(s + np.arange(block)) % len(y) for s in starts])[:len(y)]
            if len(np.unique(y[ix])) < 2:
                continue
            a, b = average_precision_score(y[ix], p[ix]), average_precision_score(y[ix], q[ix])
            bootstrap.append({"block_length": block, "iteration": iteration,
                              "reference_ap": a, "current_ap": b, "delta_ap": b-a})
        selected = pd.DataFrame([r for r in bootstrap if r["block_length"] == block])
        intervals[str(block)] = {"valid_replicates": len(selected), "attempted_replicates": 3000,
            **{c: np.quantile(selected[c], [.025, .975]).tolist() for c in ["reference_ap", "current_ap", "delta_ap"]}}
    pd.DataFrame(bootstrap).to_csv(OUT / "b3_bootstrap.csv", index=False)
    thresholds = []
    for threshold in [.3, .5, .7, .9]:
        for name, probs in [("reference", p), ("current", q)]:
            alert = probs >= threshold
            thresholds.append({"window": name, "threshold": threshold,
                "alerts": int(alert.sum()), "n": len(y), "alert_rate": float(alert.mean()),
                "precision": float(precision_score(y, alert, zero_division=0)),
                "recall": float(recall_score(y, alert, zero_division=0))})
    pd.DataFrame(thresholds).to_csv(OUT / "b3_thresholds.csv", index=False)
    import pickle
    model_dir = ROOT / "starter-kit/models/artifacts"
    model_dir.mkdir(parents=True, exist_ok=True)
    with (model_dir / "model.pkl").open("wb") as f:
        pickle.dump({"model": model, "scaler": scaler, "features": FEATURE_COLUMNS}, f)
    return {"model_provenance": "new supplemental fit; NOT the unavailable slide model",
        "n_features": n, "train_rows": len(train), "test_rows": len(test), "positive_test_labels": int(y.sum()),
        "train_end_exclusive": train_end, "test_start": test_start, "horizon": horizon,
        "label_threshold_reference_wide_retrospective": tau,
        "label_threshold_training_only": training_tau,
        "test_positives_at_training_only_threshold": int((future_vol.iloc[test] >= training_tau).sum()),
        "max_abs_midprice_return_change": float(np.abs(ref.midprice_returns-cur.midprice_returns).max()),
        "raw_spread_ratio_min": float(ratio.min()), "raw_spread_ratio_max": float(ratio.max()),
        "raw_spread_mean_ratio": float(raw_spread_cur.mean()/raw_spread_ref.mean()),
        "normalized_spread_mean_ratio": float(cur.spread_normalized.mean()/ref.spread_normalized.mean()),
        "reference_ap": float(average_precision_score(y, p)), "current_ap": float(average_precision_score(y, q)),
        "mean_abs_score_change": float(np.abs(q-p).mean()), "bootstrap": intervals}


def b2():
    service = load_module("hardened_service", ROOT / "starter-kit/scoring/score_service.py")
    service.MODEL_PATH = str(ROOT / "starter-kit/models/artifacts/model.pkl")
    service.load_model()
    client = service.app.test_client()
    payload = service.CANARY_INPUT
    evidence = []
    for case in ["valid", "missing", "corrupt"]:
        if case != "valid":
            service._state.update(model=None, scaler=None, load_error=None)
            with tempfile.TemporaryDirectory() as temp:
                service.MODEL_PATH = str(Path(temp) / "model.pkl")
                if case == "corrupt":
                    Path(service.MODEL_PATH).write_bytes(b"not a pickle")
                try:
                    service.load_model()
                except Exception as error:
                    service._state["load_error"] = f"{type(error).__name__}: {error}"
        statuses = [client.get("/health").status_code, client.get("/ready").status_code,
                    client.post("/score", json=payload).status_code]
        assert statuses == ([200, 200, 200] if case == "valid" else [200, 503, 503])
        evidence.append({"case": case, "health": statuses[0], "ready": statuses[1], "score": statuses[2],
                         "readiness_body": client.get("/ready").get_json()})
    return evidence


def crash_worker(mode, path, marker):
    frame, _ = features(ROOT / "starter-kit/data/sample/sample_ticks.jsonl")
    rows = (frame.to_dict("records") * 2)[:1000]
    writer = _ParquetAppender(path) if mode == "append" else None
    for i in range(0, len(rows), 25):
        if writer:
            writer.append(rows[i:i+25])
        else:
            BASELINE._append_parquet(rows[i:i+25], path)
    Path(marker).write_text(str(len(rows)))
    # Wait until the parent delivers SIGKILL; no close/finally is possible.
    time.sleep(120)


def crash():
    if os.name != "posix":
        raise RuntimeError("Run --crash under Linux/Docker for real SIGKILL")
    import signal
    evidence = []
    with tempfile.TemporaryDirectory() as temp:
        for mode in ["rewrite", "append"]:
            path, marker = Path(temp) / f"{mode}.parquet", Path(temp) / f"{mode}.ready"
            proc = subprocess.Popen([sys.executable, __file__, "--worker", mode, str(path), str(marker)])
            try:
                deadline = time.monotonic() + 60
                while not marker.exists():
                    if proc.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError("Crash worker did not reach checkpoint")
                    time.sleep(.05)
                completed = int(marker.read_text())
                os.kill(proc.pid, signal.SIGKILL)
                proc.wait(timeout=10)
                try:
                    readable = pq.read_table(path).num_rows
                    error = None
                except Exception as exc:
                    readable, error = 0, str(exc)
                evidence.append({"mode": mode, "signal": "SIGKILL (kill -9)", "returncode": proc.returncode,
                    "rows_in_completed_append_calls": completed, "rows_readable_normally": readable,
                    "rows_unavailable_normally": completed-readable, "reader_error": error,
                    "scope": "writer checkpoint; no Kafka offsets committed by this harness"})
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "b1_sigkill.json").write_text(json.dumps(evidence, indent=2))
    print(json.dumps(evidence, indent=2))


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    ref, ref_audit = features(ROOT / "starter-kit/data/sample/sample_ticks.jsonl")
    cur, cur_audit = features(ROOT / "starter-kit/data/sample/sample_ticks_drifted.jsonl")
    result = {"environment": {"python": sys.version, "platform": platform.platform(),
        "packages": {p: version(p) for p in ["numpy", "pandas", "pyarrow", "scipy", "scikit-learn", "flask"]}},
        "reference_input": ref_audit, "current_input": cur_audit}
    result["b1"] = b1(ref)
    print("B1 storage trials and exact row equality passed", flush=True)
    result["b3"] = b3(ref, cur)
    print("B3 paired bootstrap and threshold analysis complete", flush=True)
    result["b2"] = b2()
    (OUT / "results.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--storage-only", action="store_true")
    parser.add_argument("--worker", nargs=3)
    args = parser.parse_args()
    if args.worker:
        crash_worker(*args.worker)
    elif args.crash:
        crash()
    elif args.storage_only:
        OUT.mkdir(parents=True, exist_ok=True)
        reference, _ = features(ROOT / "starter-kit/data/sample/sample_ticks.jsonl")
        print(json.dumps(b1(reference), indent=2))
    else:
        main()
