"""
features/featurizer.py

Kafka consumer/producer that turns raw Coinbase ticker messages (ticks.raw)
into a small set of predictive features (ticks.features).

Kept deliberately small: parse -> compute_features() -> produce. The pure
functions at the top have no Kafka dependency, so scripts/replay.py can
import them directly and guarantee replay == live processing.
"""
import argparse
import json
import os
import signal
from collections import deque
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

WINDOW_SIZE_SECONDS = 60
FEATURE_COLUMNS = [
    "midprice_returns",
    "spread_normalized",
    "order_book_imbalance",
    "trade_intensity",
]


def parse_ticker_message(raw_value):
    """Extract the fields we need from one Coinbase ticker JSON message."""
    try:
        data = json.loads(raw_value)
        for event in data.get("events", []):
            if event.get("type") in ("update", "snapshot") and "tickers" in event:
                t = event["tickers"][0]
                return {
                    "timestamp": data.get("timestamp"),
                    "price": float(t.get("price", 0)),
                    "best_bid": float(t.get("best_bid", 0)),
                    "best_ask": float(t.get("best_ask", 0)),
                    "best_bid_quantity": float(t.get("best_bid_quantity", 0)),
                    "best_ask_quantity": float(t.get("best_ask_quantity", 0)),
                }
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return None
    return None


def compute_features(tick, previous_midprice=None):
    """Turn one parsed tick into a feature row (no windowing yet)."""
    if tick is None:
        return None
    midprice = (tick["best_bid"] + tick["best_ask"]) / 2
    if previous_midprice and previous_midprice > 0:
        midprice_returns = (midprice - previous_midprice) / previous_midprice
    else:
        midprice_returns = 0.0

    spread = tick["best_ask"] - tick["best_bid"]
    spread_normalized = spread / midprice if midprice > 0 else 0.0

    total_qty = tick["best_bid_quantity"] + tick["best_ask_quantity"]
    imbalance = (
        (tick["best_bid_quantity"] - tick["best_ask_quantity"]) / total_qty
        if total_qty > 0
        else 0.0
    )

    return {
        "timestamp": tick["timestamp"],
        "price": tick["price"],
        "midprice": midprice,
        "midprice_returns": midprice_returns,
        "spread_normalized": spread_normalized,
        "order_book_imbalance": imbalance,
    }


def parse_timestamp(ts_str):
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    ts = datetime.fromisoformat(ts_str)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


class TradeIntensityWindow:
    """Rolling count of ticks in the last WINDOW_SIZE_SECONDS. Shared by
    the live consumer and replay so both compute trade_intensity the same way."""

    def __init__(self, window_seconds=WINDOW_SIZE_SECONDS):
        self._timestamps = deque()
        self._delta = timedelta(seconds=window_seconds)

    def update(self, current_ts):
        window_start = current_ts - self._delta
        while self._timestamps and self._timestamps[0] < window_start:
            self._timestamps.popleft()
        self._timestamps.append(current_ts)
        return len(self._timestamps)


def _raise_system_exit(signum, frame):
    """Turn a termination signal into SystemExit so the run loop's
    try/finally gets a chance to close the Parquet writer cleanly.
    Covers SIGTERM (docker stop / Kubernetes pod termination send this
    before SIGKILL) and SIGINT. On Windows, SIGTERM delivered via os.kill
    from another process maps to TerminateProcess and bypasses any
    handler -- SIGBREAK (Ctrl+Break / CTRL_BREAK_EVENT) is the closest
    interceptable equivalent there, so it's registered too when available.
    A hard kill (SIGKILL, or a forced TerminateProcess) still cannot be
    caught by any handler; that gap is a known, documented limitation."""
    raise SystemExit(0)


def _install_signal_handlers():
    signal.signal(signal.SIGTERM, _raise_system_exit)
    signal.signal(signal.SIGINT, _raise_system_exit)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _raise_system_exit)


def run_featurizer(broker, topic_in, topic_out, max_messages=None,
                    parquet_path="data/processed/features.parquet", flush_every=25):
    """Live mode: consume ticks.raw, produce ticks.features, and periodically
    append the same rows to a Parquet file (used by training + drift report)."""
    from quixstreams import Application

    _install_signal_handlers()
    app = Application(
        broker_address=broker,
        consumer_group="featurizer_group",
        auto_offset_reset="earliest",
    )
    producer = app.get_producer()
    window = TradeIntensityWindow()
    previous_midprice = None
    processed = 0
    buffer = []
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    writer = _ParquetAppender(parquet_path)

    try:
        with app.get_consumer() as consumer:
            consumer.subscribe([topic_in])
            print(f"[featurizer] listening on {topic_in} -> {topic_out}")
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error() is not None:
                    print(f"[featurizer] consumer error: {msg.error()}")
                    continue

                tick = parse_ticker_message(msg.value())
                if tick is None or not tick.get("timestamp"):
                    continue

                features = compute_features(tick, previous_midprice)
                if features is None:
                    continue
                features["trade_intensity"] = window.update(parse_timestamp(tick["timestamp"]))
                previous_midprice = features["midprice"]

                producer.produce(topic=topic_out, key="BTC-USD", value=json.dumps(features))
                buffer.append(features)
                processed += 1

                if len(buffer) >= flush_every:
                    writer.append(buffer)
                    consumer.commit(message=msg, asynchronous=False)
                    print(f"[featurizer] produced {processed} feature rows (flushed {len(buffer)} to parquet, committed offsets)")
                    buffer = []
                if max_messages and processed >= max_messages:
                    if buffer:
                        writer.append(buffer)
                    consumer.commit(message=msg, asynchronous=False)
                    print(f"[featurizer] reached max_messages={max_messages}, exiting")
                    break
    finally:
        writer.close()


class _ParquetAppender:
    """Appends feature rows to `path` as new Parquet row groups instead of
    reading back and rewriting the whole file on every flush (the original
    approach was O(n^2) over a run: each flush re-read and re-serialized
    every row produced so far, so throughput degraded as the file grew).

    Tradeoff: the file's footer is only written on close(). A hard kill
    (SIGKILL, power loss, a forced TerminateProcess) bypasses all signal
    handlers and leaves an unreadable file, whereas the old read-rewrite
    approach left a valid file after every flush. Graceful exit paths
    (max_messages, SIGTERM/SIGINT/SIGBREAK via _install_signal_handlers)
    are covered via the caller's try/finally.
    """

    def __init__(self, path):
        self.path = path
        self._writer = None

    def append(self, rows):
        table = pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, table.schema)
        self._writer.write_table(table)

    def close(self):
        if self._writer is not None:
            self._writer.close()
            self._writer = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer that computes features")
    parser.add_argument("--broker", default=os.environ.get("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic_in", default="ticks.raw")
    parser.add_argument("--topic_out", default="ticks.features")
    parser.add_argument("--max_messages", type=int, default=None,
                         help="Optional: exit after N messages (handy for smoke tests / grading)")
    parser.add_argument("--parquet_path", default="data/processed/features.parquet",
                         help="Where to write/append feature rows. Use a distinct path per run "
                              "(e.g. data/processed/features_reference.parquet vs "
                              "features_current.parquet) when building a drift comparison, "
                              "so a second run doesn't append onto the first.")
    args = parser.parse_args()
    run_featurizer(args.broker, args.topic_in, args.topic_out, args.max_messages,
                   parquet_path=args.parquet_path)
