"""Finite Kafka replay, three repetitions per writer, actual offset telemetry.

Runs the supplied baseline and submitted featurizer functions unchanged except
for an isolated consumer-group name. Each run uses NEW topics. No topic deletion.
Use --broker localhost:9092 after starting the supplied Kafka service.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import pandas as pd
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from validate_submission import BASELINE, ROOT, features

OUT = ROOT / "evidence/supplemental"


def worker(args):
    import quixstreams
    original = quixstreams.Application
    def isolated_app(*pos, **kw):
        kw["consumer_group"] = args.group
        return original(*pos, **kw)
    quixstreams.Application = isolated_app
    if args.worker == "rewrite":
        module = BASELINE
    else:
        from features import featurizer as module
    module.run_featurizer(args.broker, args.topic, args.topic + ".features",
                          args.count, parquet_path=args.path)


def main(args):
    OUT.mkdir(parents=True, exist_ok=True)
    scratch = OUT / ("stream_files_" + uuid.uuid4().hex[:8])
    scratch.mkdir(exist_ok=True)
    source = ROOT / "starter-kit/data/sample/sample_ticks_long.jsonl"
    expected, audit = features(source)
    assert audit["backward_timestamp_steps"] == 0
    lines = source.read_text().splitlines()
    admin = AdminClient({"bootstrap.servers": args.broker})
    results, telemetry = [], []
    for repeat in range(3):
        # Reverse within-pair order on alternate repetitions.
        for mode in (["rewrite", "append"] if repeat % 2 == 0 else ["append", "rewrite"]):
            name = f"submission-{uuid.uuid4().hex[:10]}"
            for future in admin.create_topics([NewTopic(name, 1, 1), NewTopic(name + ".features", 1, 1)]).values():
                future.result()
            path = scratch / f"{mode}-{repeat}.parquet"
            if path.exists():
                raise RuntimeError(f"Use a new output directory; refusing to overwrite {path}")
            log = (OUT / f"b1_stream_{mode}_{repeat}.log").open("w")
            proc = subprocess.Popen([sys.executable, "-u", __file__, "--worker", mode,
                "--broker", args.broker, "--group", name, "--topic", name,
                "--path", str(path), "--count", str(len(expected))], stdout=log, stderr=subprocess.STDOUT)
            observer = Consumer({"bootstrap.servers": args.broker, "group.id": name,
                                 "enable.auto.commit": False})
            tp = TopicPartition(name, 0)
            producer = Producer({"bootstrap.servers": args.broker})
            state = {"acked": 0, "errors": [], "end": None}
            def delivered(error, message):
                if error:
                    state["errors"].append(str(error))
                else:
                    state["acked"] += 1
            # Give each worker the same startup allowance; no records exist yet.
            time.sleep(4)
            start = time.perf_counter()
            def send():
                try:
                    for i, line in enumerate(lines):
                        remaining = start + i / args.rate - time.perf_counter()
                        if remaining > 0:
                            time.sleep(remaining)
                        producer.produce(name, key="BTC-USD", value=line, on_delivery=delivered)
                        producer.poll(0)
                    pending = producer.flush(30)
                    if pending:
                        state["errors"].append(f"{pending} undelivered records")
                except Exception as error:
                    state["errors"].append(str(error))
                finally:
                    state["end"] = time.perf_counter()
            sender = threading.Thread(target=send)
            sender.start()
            run_rows = []
            try:
                while True:
                    now = time.perf_counter() - start
                    high = observer.get_watermark_offsets(tp, timeout=10, cached=False)[1]
                    committed = observer.committed([tp], timeout=10)[0].offset
                    committed = max(committed, 0)
                    row = {"mode": mode, "repeat": repeat, "elapsed_s": now,
                        "log_end_offset": high, "committed_offset": committed, "lag": high-committed}
                    run_rows.append(row)
                    if proc.poll() is not None and state["end"] is not None:
                        break
                    if now > 180:
                        raise TimeoutError(f"{mode} run exceeded 180 seconds")
                    time.sleep(1)
                sender.join(timeout=35)
                assert proc.returncode == 0, f"Worker failed: see {log.name}"
                assert not state["errors"], state["errors"]
                assert state["acked"] == len(lines)
                actual = pd.read_parquet(path)
                pd.testing.assert_frame_equal(expected, actual, check_exact=True)
                duration = time.perf_counter() - start
                results.append({"mode": mode, "repeat": repeat, "topic": name,
                    "target_rate": args.rate, "acked_raw_messages": state["acked"],
                    "producer_elapsed_s": state["end"]-start,
                    "achieved_acked_rate": state["acked"]/(state["end"]-start),
                    "completion_elapsed_s": duration, "feature_rows": len(actual),
                    "peak_sampled_lag": max(r["lag"] for r in run_rows),
                    "final_sampled_lag": run_rows[-1]["lag"], "exact_feature_equality": True})
                telemetry.extend(run_rows)
                pd.DataFrame(results).to_csv(OUT / "b1_stream_summary.csv", index=False)
                pd.DataFrame(telemetry).to_csv(OUT / "b1_stream_telemetry.csv", index=False)
                print(json.dumps(results[-1]), flush=True)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()
                sender.join(timeout=35)
                observer.close()
                log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=1500)
    parser.add_argument("--worker", choices=["rewrite", "append"])
    parser.add_argument("--group")
    parser.add_argument("--topic")
    parser.add_argument("--path")
    parser.add_argument("--count", type=int)
    args = parser.parse_args()
    if args.worker:
        worker(args)
    else:
        main(args)
