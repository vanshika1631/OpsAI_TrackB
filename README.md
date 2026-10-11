# Track B final submission

**Team:** Vanshika Tiwari, Harshid Bhinde, Hritwik Ekade  
**Course:** 94-879 Fundamentals of Operationalizing AI  
**Final report due:** October 13, 2026

The six-page [Technical Operationalization Brief](submission/technical_operationalization_brief.pdf)
is our final report. This repository also contains our code, test scripts and
results so the experiments can be repeated. We have already completed the presentation.

## What we found

- **B1:** append was faster and produced the same feature rows. However, a hard
  kill made all 1,000 tested rows in the open file unreadable. We would only use
  this version where the input can be replayed until storage and recovery are fixed.
  The normal shutdown handler does not protect against SIGKILL.
- **B2:** we recommend the corrected model path and separate `/health` and
  `/ready` checks. Compose can show the failure, but its unhealthy label does not
  automatically stop traffic or restart the container.
- **B3:** the mean spread increased 3.98x. We would investigate this change
  before retraining. Our small test uses retrospective proxy labels, so it cannot
  tell us whether the model would be better or worse in production.

## Run the new experiments

Use Python 3.12. Create a virtual environment and install
`experiments/requirements.txt`. Run from the repository root:

```bash
python -m venv .venv
# Activate .venv using your shell's normal activation command.
python -m pip install -r experiments/requirements.txt
python experiments/validate_submission.py
```

This checks every feature row in the finite long workload, runs randomized
save-time trials at controlled existing file sizes, fits the documented B3
model, computes paired block-bootstrap intervals and threshold sensitivity,
and checks B2 endpoint status codes using Flask's test client. It creates the
model at `starter-kit/models/artifacts/model.pkl` and CSV/JSON evidence at
`evidence/supplemental/`. This model is a supplemental fit, not the slide model.
Rerunning the script replaces the generated results and the new model.

For the Kafka comparison, start Docker and the supplied broker:

```bash
docker compose -f starter-kit/docker/compose.yaml up -d broker
python experiments/stream_comparison.py --rate 1500
```

If an existing starter-kit broker already owns port 9092, use that running
broker instead of starting a second one. The test script creates unique topics and
groups, uses a finite replay without `--loop`, runs each writer three times,
alternates run order, measures acknowledged production and broker offsets,
and checks each output against the direct feature calculation. It never resets
or deletes an existing topic. Temporary test topics remain in the broker.
Raw Parquet outputs are regenerated and ignored by Git; raw logs, CSV telemetry
and equality results are included. Do not run CPU-heavy experiments concurrently
with these timing trials. We ran the final timing tests one at a time.

For real Linux SIGKILL testing, use Linux with the experiment dependencies and
run `python experiments/validate_submission.py --crash`, or build the scorer
image and use Docker (replace the absolute source path with your checkout):

```bash
docker compose -f starter-kit/docker/compose.yaml build scorer
docker run --rm --user 0 --mount "type=bind,source=ABSOLUTE_REPO_PATH,target=/work" --workdir /work --entrypoint python docker-scorer:latest experiments/validate_submission.py --crash
```

Compose may name the image differently depending on its project name. Obtain
the image name with `docker compose -f starter-kit/docker/compose.yaml images`.
The crash test sends `SIGKILL` to its own child after 40 completed
25-row saves. It tests writer readability; it does **not** commit Kafka offsets
or claim a measured end-to-end recovery loss. Never run it against real outputs.

## Repeat the B2 deployment test

We kept the September 25 logs under `evidence/b2_historical/`.
These are the earlier results, not a new Docker run. The original
fault configuration remains in `Track_B_Challenges_Package/compose_variants/`;
the original scorer can be recovered from initial commit
`eae698f2f38b2feae5f2cba910c2971a3b0ece25`.

After generating the supplemental model and building the image:

```bash
docker compose -f starter-kit/docker/compose.yaml up -d scorer
curl http://localhost:8000/health
curl http://localhost:8000/ready
docker compose -f starter-kit/docker/compose.yaml -f starter-kit/docker/docker-compose.fault-bad-model-path.yaml up -d scorer
```

With the fault override, `/health` stays 200, `/ready` and valid `/score`
requests return 503, and Docker eventually marks the scorer unhealthy. Repair
by re-running `up -d scorer` with only the base compose file. Both the base and
repaired B2 compose files probe `/ready` and use `/app/models/artifacts/model.pkl`.
Do not launch two full stacks simultaneously: their names and ports conflict.

## Evidence and limitations

- [Response to the feedback](submission/feedback_response.md): every feedback item,
  evidence location, and remaining limitation.
- [Notes about the evidence](evidence/README.md): historical versus new measurements,
  the environment, and what the measurements can tell us.
- [Editable brief text](submission/technical_operationalization_brief.md): a
  Markdown copy of the six-page PDF. To regenerate both, edit the content in
  `experiments/build_brief.py` and run it.
- Original B1 run logs and original B3 scoring artifact are unavailable. The
  brief replaces their numerical claims with clearly identified new experiments.
- The append writer opens a new file; it does not safely resume an existing
  file. Always use a fresh output path. Forced termination can invalidate the
  entire open file. The old in-place rewrite is also vulnerable during a write.
- Broker image `apache/kafka:latest` is not an immutable deployment reference.
  We recorded the image IDs used in our tests. Before production, we would pin a tested digest.

To rebuild the PDF, install `reportlab==4.4.9` and run
`python experiments/build_brief.py`. Report contents and plots read the submitted
CSV/JSON evidence. Regenerate only after reviewing new run results.
