"""
scoring/score_service.py

A minimal HTTP scoring service. Loads the pickled model produced by
models/train_baseline.py and exposes three endpoints:

  GET  /health          -> liveness: the process is up and answering HTTP.
                            {"status": "ok", "model_loaded": bool}  (always 200)
  GET  /ready           -> readiness: the service can actually score.
                            200 {"ready": true, ...} only if the model is loaded
                            AND a canary prediction succeeds; otherwise
                            503 {"ready": false, "reason": ...}
  POST /score           -> {"score": float, "spike": bool}
                            body: {"midprice_returns": .., "spread_normalized": ..,
                                    "order_book_imbalance": .., "trade_intensity": ..}

Liveness answers "should this process be restarted?"; readiness answers
"should traffic be sent here?". A missing or unusable model is a readiness
failure, not a liveness failure: restarting the process does not fix it.

Deliberately framework-light (Flask) and single-file so the whole thing
can be read in one sitting.
"""
import math
import os
import pickle

import pandas as pd
from flask import Flask, jsonify, request

MODEL_PATH = os.environ.get("MODEL_PATH", "models/artifacts/model.pkl")
FEATURE_COLUMNS = ["midprice_returns", "spread_normalized", "order_book_imbalance", "trade_intensity"]
CANARY_INPUT = {"midprice_returns": 0.0, "spread_normalized": 0.0001,
                "order_book_imbalance": 0.0, "trade_intensity": 10.0}

app = Flask(__name__)
_state = {"model": None, "scaler": None, "load_error": None}


def load_model():
    with open(MODEL_PATH, "rb") as f:
        bundle = pickle.load(f)
    _state["model"] = bundle["model"]
    _state["scaler"] = bundle["scaler"]
    _state["load_error"] = None
    print(f"[score_service] loaded model from {MODEL_PATH}")


def predict(features):
    x = pd.DataFrame([{c: features[c] for c in FEATURE_COLUMNS}])
    x_scaled = _state["scaler"].transform(x)
    return float(_state["model"].predict_proba(x_scaled)[0, 1])


def readiness():
    if _state["model"] is None:
        return False, f"model not loaded from {MODEL_PATH}: {_state['load_error']}"
    try:
        prob = predict(CANARY_INPUT)
    except Exception as e:
        return False, f"canary prediction failed: {type(e).__name__}: {e}"
    if not (math.isfinite(prob) and 0.0 <= prob <= 1.0):
        return False, f"canary prediction out of range: {prob}"
    return True, "canary prediction ok"


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "model_loaded": _state["model"] is not None})


@app.route("/ready", methods=["GET"])
def ready():
    ok, reason = readiness()
    return jsonify({"ready": ok, "reason": reason, "model_path": MODEL_PATH}), (200 if ok else 503)


@app.route("/score", methods=["POST"])
def score():
    if _state["model"] is None:
        return jsonify({"error": "model not loaded"}), 503

    payload = request.get_json(force=True)
    missing = [c for c in FEATURE_COLUMNS if c not in payload]
    if missing:
        return jsonify({"error": f"missing fields: {missing}"}), 400

    prob = predict(payload)
    return jsonify({"score": prob, "spike": prob >= 0.5})


if __name__ == "__main__":
    try:
        load_model()
    except Exception as e:
        _state["load_error"] = f"{type(e).__name__}: {e}"
        print(f"[score_service] ERROR: could not load model from {MODEL_PATH} ({_state['load_error']}). "
              f"Starting anyway; /health (liveness) will pass but /ready will return 503 until this is fixed.")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
