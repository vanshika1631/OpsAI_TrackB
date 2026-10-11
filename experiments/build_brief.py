"""Build the six-page brief from recorded evidence. Requires reportlab 4.4.9."""
import csv
import json
import math
from pathlib import Path
from statistics import median
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.graphics.shapes import Drawing, Line, String, Path as DrawPath

ROOT = Path(__file__).resolve().parents[1]
E = ROOT / "evidence/supplemental"
DEST = ROOT / "submission"
NAVY = colors.HexColor("#17324d")
TEAL = colors.HexColor("#007d83")
ORANGE = colors.HexColor("#ba5825")
PALE = colors.HexColor("#edf3f6")


def read_csv(name):
    return list(csv.DictReader((E / name).open()))


def lag_chart():
    rows = read_csv("b1_stream_telemetry.csv")
    w, h = 480, 195
    d = Drawing(w, h)
    left, bottom, width, height = 47, 32, 412, 130
    xmax = math.ceil(max(float(r["elapsed_s"]) for r in rows) / 5) * 5
    ymax = math.ceil(max(float(r["lag"]) for r in rows) / 1000) * 1000
    for tick in range(0, ymax + 1, 1000):
        y = bottom + tick / ymax * height
        d.add(Line(left, y, left+width, y, strokeColor=colors.HexColor("#d7e0e7"), strokeWidth=.5))
        d.add(String(left-7, y-3, str(tick), textAnchor="end", fontName="Helvetica", fontSize=8))
    for tick in range(0, xmax+1, 5):
        x = left+tick/xmax*width
        d.add(String(x, bottom-13, str(tick), textAnchor="middle", fontSize=8))
    d.add(String(left, h-8, "Lag (raw messages; log-end minus committed offset)", fontSize=9, fillColor=NAVY))
    d.add(String(left+width/2, 3, "Elapsed time since replay start (seconds)", textAnchor="middle", fontSize=9))
    for mode, color in [("rewrite", ORANGE), ("append", TEAL)]:
        for repeat in range(3):
            selected = [r for r in rows if r["mode"] == mode and int(r["repeat"]) == repeat]
            path = DrawPath(strokeColor=color, strokeWidth=1.3, fillColor=None,
                            strokeDashArray=[[], [4, 2], [1, 2]][repeat])
            for i, row in enumerate(selected):
                x, y = left+float(row["elapsed_s"])/xmax*width, bottom+float(row["lag"])/ymax*height
                (path.moveTo if i == 0 else path.lineTo)(x, y)
            d.add(path)
    d.add(String(left+218, h-22, "Rewrite (3 runs)", fontSize=8, fillColor=ORANGE))
    d.add(String(left+322, h-22, "Append (3 runs)", fontSize=8, fillColor=TEAL))
    d.scale(1, .92)
    d.height = h * .92
    return d


def build():
    r = json.loads((E / "results.json").read_text())
    b3 = r["b3"]
    runs = read_csv("b1_stream_summary.csv")
    assert len(runs) == 6 and all(x["exact_feature_equality"] == "True" for x in runs)
    bench = read_csv("b1_save_summary.csv")
    thresholds = read_csv("b3_thresholds.csv")
    def interval(value):
        return f"[{value[0]:.3f}, {value[1]:.3f}]"
    def vals(mode, field):
        return [float(x[field]) for x in runs if x["mode"] == mode]
    def span(v, places=0):
        return f"{min(v):,.{places}f}-{max(v):,.{places}f}"
    pages = []
    pages.append(("1", "Problem, hypothesis and controlled diagnosis", [
        ("lead", "When 'healthy' is not enough"),
        ("p", "Vanshika Tiwari | Harshid Bhinde | Hritwik Ekade<br/>94-879 Fundamentals of Operationalizing AI | Track B | October 10, 2026"),
        ("p", "Decision: ship B2's path repair and readiness check; use B1's append writer only for controlled, replayable runs until durability is addressed; in B3, alert and investigate rather than automatically retrain. These are distinct operational failures: falling behind, being unable to score, and serving changed inputs."),
        ("h", "System and evidence boundary"),
        ("p", "Recorded ticks enter Kafka, become rolling features, persist to Parquet, and feed a model exposed through an API. Our presentation examined B1, B2 and B3. This brief supplements it with controlled measurements. B2's September 25 captures survive. Original B1 logs and the original B3 model/script do not; their numerical results are not treated as verified final evidence."),
        ("h", "B1: hypothesis and falsification"),
        ("p", "The initial hypothesis was that more partitions and consumers would remove growing lag; unchanged lag would falsify it. The presentation reported that falsification. The producer's constant BTC-USD key explains why the added consumers could remain idle: one key is routed to one partition. We did not repeat the historical partition comparison and do not claim to recover its measurements."),
        ("p", "The replacement hypothesis is that reading and rewriting all prior rows every 25 features makes storage cost grow with accumulated data. If save time stays flat as existing row count increases, or the append change fails under equivalent load, this diagnosis weakens. Rewriting successively larger files entails quadratic cumulative row-serialization work; a throughput decline over time alone cannot establish the cause."),
        ("h", "Controlled save test"),
        ("p", "Seven randomized trials per writer and file size; identical 25-row additions, schema, host and storage. Seed-file creation is outside the timer. Existing rows vary independently of elapsed run time. Append footer closure is outside its save timer: the operations have different durability guarantees."),
        ("table", [["Rows already stored", "Rewrite median (ms)", "Append median (ms)"]] +
            [[f"{size:,}"] + [f"{float(next(x['median'] for x in bench if x['mode']==mode and int(x['existing_rows'])==size)):.2f}" for mode in ["rewrite", "append"]] for size in [0,5000,20000,80000]]),
        ("p", "Rewrite cost rises with stored rows while open-writer append cost is nearly flat after initialization. This supports the code-based mechanism, not a fitted asymptotic timing law. Measurements are warm-cache and do not include fsync or power-failure testing."),
        ("source", "Evidence: b1_save_trials.csv (56 trials); b1_save_summary.csv; supplied baseline _append_parquet and submitted _ParquetAppender."),
    ]))
    pages.append(("2", "B1: measured improvement, bounded recommendation", [
        ("h", "Equivalent finite replay, repeated three times"),
        ("p", "Each run starts with a unique empty topic and group, one partition/consumer, and a fresh output file. The same 14,076 raw messages yield 11,754 features; non-ticker messages do not produce rows. Input timestamps have zero backward steps. Replay is finite, never looped. Run order alternates; final timing runs were collected without concurrent benchmark/bootstrap work."),
        ("chart", "lag"),
        ("table", [["Measure (3 runs each)", "Rewrite", "Append"],
            ["Achieved raw arrivals/s", span(vals("rewrite","achieved_acked_rate"),1), span(vals("append","achieved_acked_rate"),1)],
            ["Peak sampled lag (messages)", span(vals("rewrite","peak_sampled_lag")), span(vals("append","peak_sampled_lag"))],
            ["Observed completion (seconds)", span(vals("rewrite","completion_elapsed_s"),2), span(vals("append","completion_elapsed_s"),2)],
            ["Features / observed second", span([11754/t for t in vals("rewrite","completion_elapsed_s")],1), span([11754/t for t in vals("append","completion_elapsed_s")],1)],
            ["Final sampled lag", "0 in all runs", "0 in all runs"],
            ["Exact feature-row equality", "11,754 / 11,754", "11,754 / 11,754"]]),
        ("p", "Achieved rate uses acknowledged delivery count divided by producer elapsed time, including final flush. Lag is sampled about every second; completion times include observation delay. Every output column and row matches a direct single-pass calculation exactly. This supports the bounded finite-load improvement, not a multi-hour capacity claim."),
        ("h", "Hard kill reverses an unconditional ship recommendation"),
        ("p", "A Linux SIGKILL after 40 completed 25-row writes leaves 1,000/1,000 rows readable for rewrite and 0/1,000 normally readable for append. The open file lacks its footer: all 1,000 rows are unavailable to standard PyArrow reads, not just the last row group. This writer-only test performs no Kafka commits; it does not measure end-to-end recovery loss. Production code commits after appends, so unreadable committed output is a real risk."),
        ("p", "Keep the SIGTERM/SIGINT close handler, but do not equate graceful closure with crash durability. The old in-place rewrite is also vulnerable if killed during a write. Use append only with retained replayable inputs and fresh output paths; durable closed segments and offset/data recovery must be validated before production."),
        ("source", "Evidence: b1_stream_summary.csv; b1_stream_telemetry.csv and six logs; b1_sigkill.json. Original 73,607 lag and 3,000/s achieved-rate claims are withdrawn: looping or pre-existing backlog cannot be distinguished without the missing logs."),
    ]))
    pages.append(("3", "B2: make deployment health agree with scoring", [
        ("h", "Diagnosis and controlled causal test"),
        ("p", "The supplied B2 configuration points MODEL_PATH to /app/models/artifact/model.pkl, while the mounted directory is artifacts. The original process catches a missing file and stays alive. Docker probes /health and sees HTTP 200 even when /score returns 503. This is both a configuration defect and a detection defect."),
        ("p", "Hypothesis: changing only MODEL_PATH, keeping image, mount and scoring payload constant, restores scoring. A continued 503 would reject that explanation. Historical captures show the configured path absent, the mounted file present, and the path-only override restoring HTTP 200 and the baseline score. Three restarts with the wrong path all reproduced the fault; restarting is not the repair."),
        ("h", "Intervention"),
        ("p", "Repair the model path. Keep /health as a cheap liveness response. Add /ready: require a loaded model, run a canary through the same predict function as /score, and require a finite probability in [0,1]. Return HTTP 503 with a diagnostic reason on failure. Point Docker's healthcheck to /ready. Model-load exceptions are surfaced as unready instead of silently looking usable."),
        ("table", [["Controlled condition", "Docker", "/health", "/ready", "/score"],
            ["Original + bad path", "healthy", "200", "absent", "503"],
            ["Repaired + hardened", "healthy", "200", "200", "200"],
            ["Hardened + bad path", "unhealthy", "200", "503", "503"]]),
        ("p", "The historical probe timeline was sampled every 10 seconds: starting at 0-40 seconds and unhealthy at 50 seconds after five failed probes. Liveness remained 200 after startup. A corrupt artifact made the original process exit with code 1; the hardened service stayed up and exposed UnpicklingError through readiness."),
        ("h", "Cost and verification"),
        ("p", "Historical host-side latency: 300 sequential requests per endpoint after 20 warm-ups. /health p50 3.50 ms, p95 5.51 ms; /ready p50 4.65 ms, p95 6.03 ms. Median difference is 1.15 ms. These are local, sequential probes, not a concurrency benchmark; raw individual timings are unavailable."),
        ("p", "The final repository includes the previously local hardened scorer and repaired compose files. Supplemental Flask test-client checks with the newly generated model confirm health/ready/score = 200/200/200 for a valid artifact and 200/503/503 for missing and corrupt artifacts. These are application-level regression checks, not a new Docker timing run."),
        ("h", "Deployment recommendation and limits"),
        ("p", "Ship both changes. In Compose, unhealthy is a status label: it does not remove traffic or restart the container. A stale canary could mark all replicas unready, and a runnable but wrong model can pass. Monitor ready replicas, caller 5xx, load errors and time-to-ready. Test traffic gating and failed rollouts separately before claiming Kubernetes reliability."),
        ("source", "Evidence: evidence/b2_historical/01-08 captures, especially 03_diagnosis, 05_fault_injected_hardened_code and 07_probe_cost; supplemental/results.json, b2 section."),
    ]))
    pages.append(("4", "B3: material drift, limited model-quality evidence", [
        ("h", "Hypothesis partly rejected"),
        ("p", "The initial hypothesis linked wider spreads to detectable spread_normalized drift and a consequential model effect. Its stated rejection rule included drift with an unaffected model. Detection is supported; demonstrated degradation is not. We therefore partly reject the original combined hypothesis. An investigation is still warranted because an input contract changed substantially and this small synthetic test cannot establish safety."),
        ("h", "Resolve the spread measurement"),
        ("p", f"Both input files contain 782 raw messages and 653 feature rows, computed with the same featurizer. Mean normalized spread rises {b3['normalized_spread_mean_ratio']:.4f}x; mean raw spread rises {b3['raw_spread_mean_ratio']:.4f}x. The first tick changes from 0.01 to 0.05 (5x), but per-tick ratios vary approximately 3x-5x. Thus 5x is an example, not the aggregate change."),
        ("table", [["Feature", "KS statistic", "Interpretation"],
            ["spread_normalized", "0.9525", "Large distribution shift"],
            ["midprice_returns", "0.0046", "Small rounding perturbation"],
            ["order_book_imbalance", "0.0000", "Unchanged"],
            ["trade_intensity", "0.0000", "Unchanged"]]),
        ("p", "Direct SciPy KS calculations independently corroborate the presentation's feature-level finding. Spread p-value is below numerical precision; other p-values are 1. Serial dependence limits literal significance interpretation. One of four features shifted; a 50% dataset-level rule can miss this consequential single-feature change."),
        ("h", "New, reproducible model-sensitivity evaluation"),
        ("p", "The original slide model is unavailable. We fit a new balanced logistic regression and scaler on the first 371 rows, purging 20 rows before the 60% training boundary. Test indices 522-632 provide 111 paired rows after removing the unlabeled 20-row tail. Proxy labels use forward 20-row return volatility and a reference-wide 90th-percentile cutoff, held fixed across the synthetic perturbation; 19 test labels are positive."),
        ("p", "This label definition is retrospective, not prospective validation. A cutoff estimated only from training gives zero positive test cases. Trade prices are identical, but bid/ask rounding slightly perturbs midprice returns (maximum absolute change 9.85e-8). No model or scaler fitting uses test rows."),
        ("table", [["Average precision (AP)", "Estimate", "95% block-bootstrap interval"],
            ["Reference", f"{b3['reference_ap']:.3f}", interval(b3['bootstrap']['20']['reference_ap'])],
            ["Current", f"{b3['current_ap']:.3f}", interval(b3['bootstrap']['20']['current_ap'])],
            ["Current minus reference", f"{b3['current_ap']-b3['reference_ap']:.3f}", interval(b3['bootstrap']['20']['delta_ap'])]]),
        ("p", "AP is sklearn average_precision_score, commonly called PR-AUC here; it is not trapezoidal PR area. Paired circular moving-block bootstrap: 3,000 attempts, block length 20, 2,732 valid two-class draws. The delta interval includes zero. These are not intervals for the slide's 0.208/0.303 result and do not justify calling that result noise."),
        ("source", "Evidence: results.json; b3_drift.csv; b3_paired_scores.csv; b3_bootstrap.csv. Block length 10 sensitivity: delta 95% interval [0.000, 0.171], 2,902 valid draws. Intervals omit model-training uncertainty."),
    ]))
    table = [["Threshold", "Alerts: ref / current", "Precision: ref / current", "Recall: ref / current"]]
    for threshold in [.3,.5,.7,.9]:
        a,b = [next(x for x in thresholds if x["window"] == name and float(x["threshold"]) == threshold) for name in ["reference","current"]]
        table.append([f"{threshold:.1f}", f"{a['alerts']}/111 / {b['alerts']}/111",
            "undefined / undefined" if threshold == .9 else f"{float(a['precision']):.3f} / {float(b['precision']):.3f}",
            f"{float(a['recall']):.3f} / {float(b['recall']):.3f}"])
    pages.append(("5", "Operational actions and tradeoffs", [
        ("h", "Threshold sensitivity changes the interpretation"),
        ("table", table),
        ("p", "At 0.5, 90.1% of reference and 88.3% of current rows are flagged: the alert rate is saturated and weak as a standalone quality signal. Recall falls from 0.895 to 0.789 even though AP rises, so 'the model is unaffected' is too strong. At 0.7, rates fall to 64.9% and 63.1%, with equal 0.684 recall. At 0.9 no positives are predicted; precision is undefined. Thresholds are illustrative, not selected using this tiny test tail."),
        ("p", f"Mean absolute score change is {b3['mean_abs_score_change']:.4f}. Metric behavior is threshold-dependent and intervals are wide. We observe sensitivity to the perturbation, but cannot establish consistent degradation, equivalence, or improvement in production. Keep serving while investigating; do not automatically retrain from a drift p-value or AP point estimate."),
        ("h", "Proposed monitoring and response policy"),
        ("table", [["Signal", "Trigger / response", "Owner"],
            ["Infrastructure / readiness", "Zero ready replicas or scoring 5xx: page; inspect path/artifact, restore known-good config.", "Service operator"],
            ["B1 lag + freshness", "Sustained lag growth or stale output: inspect save cost; reduce intake or replay safely.", "Pipeline owner"],
            ["Feature drift", "Proposed: KS effect >0.2 AND spread mean ratio >1.5; investigate feed/units and market context.", "Model + data owner"],
            ["Model quality", "Collect delayed labels; compare precision/recall and AP with uncertainty and operating cost.", "Model owner"]]),
        ("p", "The drift trigger is an operational proposal, not a pre-registered or false-alarm-calibrated threshold. Add window minimums, persistence and deduplication before alerting continuously. Infrastructure health, data validity and model quality must remain separate signals. A per-feature alert is appropriate even if the dataset-wide fraction is below its threshold."),
        ("h", "Deployment gates"),
        ("p", "B1: preserve source data and use a fresh output path for every run. The current writer can truncate a pre-existing path on restart. Before production, require closed durable segments, an explicit offset/data recovery protocol and kill-during-write/restart tests. Do not infer exactly-once durability from a committed Kafka offset."),
        ("p", "B2: release path repair plus readiness after canary-schema review. Provide operator rollback because repeated restart cannot repair a missing model. Validate model identity separately; readiness proves execution, not correctness. Pin tested image digests before production; the starter still uses a mutable Kafka latest tag."),
        ("p", "B3: investigate source/units, retain both windows and collect labels from an actual wide-spread regime. Retraining requires sustained, consequential labeled degradation and validation of the proposed replacement. Sensitive alerts incur operator load; suppressing a single-feature shift risks overlooking a broken input contract."),
        ("source", "Evidence: b3_thresholds.csv and b3_paired_scores.csv. Proposed operational triggers are recommendations, not tested SLAs or production guarantees."),
    ]))
    pages.append(("6", "AI supervision, reproducibility and remaining uncertainty", [
        ("h", "Consequential AI decision 1: replace unverifiable evidence"),
        ("p", "AI proposed replacing the unverifiable B1 slide measurements with new controlled experiments, rather than treating the configured 3,000/s rate and 73,607 lag as established. Disposition: accepted by the submitting team member during final preparation. Reason: 73,607 lag cannot arise from a clean one-pass 14,076-message workload, and the original run mode/offset history cannot be recovered."),
        ("p", "Verification: new unique Kafka topics/groups; 14,076 acknowledged arrivals per run; measured arrival rates; zero backward timestamps; six before/after runs; every one of 11,754 output rows exactly equal to the direct feature reference. A separate randomized file-size experiment isolates storage cost. Missing historical logs remain missing; the new results do not retroactively validate the old charts."),
        ("h", "Consequential AI decision 2: restrict the B1 recommendation"),
        ("p", "AI proposed modifying 'ship append with a shutdown handler' to conditional use pending durable storage and recovery. Disposition: accepted by the submitting team member after reviewing the SIGKILL result. Reason: a graceful close handler cannot run under SIGKILL, and the open file's missing footer makes the entire file unreadable through the normal reader."),
        ("p", "Verification: the harness completed 40 batches (1,000 rows), then delivered actual Linux SIGKILL. Rewrite retained 1,000 readable rows; append retained zero. The brief distinguishes this writer-level result from unmeasured Kafka recovery loss and withdraws the old blanket 'crash-safe every flush' claim. The team retains responsibility for the deployment decision and interpretation."),
        ("h", "How the working artifact can be checked"),
        ("p", "From the repository root, install experiments/requirements.txt under Python 3.12 and run experiments/validate_submission.py. Start the supplied Kafka broker and run experiments/stream_comparison.py --rate 1500. On Linux, run validate_submission.py --crash for SIGKILL. The root README includes Docker commands, B2 fault injection and repair, environment details and expected outputs."),
        ("p", "Scripts, input hashes, raw trial CSVs, lag telemetry, worker logs, paired scores and bootstrap samples are included. B2 captures remain dated September 25; supplemental evidence is dated October 10. The original baseline is retained in the supplied challenge package and initial Git commit. The PDF and editable text summarize evidence; they are not substitutes for the raw outputs."),
        ("h", "Most important remaining uncertainty"),
        ("p", "Can the faster B1 pipeline preserve or recover all committed output after a hard failure and restart? Until a durable-write/offset protocol is implemented and tested, the observed throughput benefit does not establish production reliability. Separately, B3 needs representative delayed labels: synthetic paired inputs and retrospective proxy labels cannot tell us whether the system remains useful in a real wide-spread regime."),
        ("h", "Sources and supporting material"),
        ("p", "Assignment: 94-879 Final Team Project Track B - Technical Challenges, Final Deliverables (six-page brief limit). Team presentation: When Healthy Isn't Working, October presentation; numerical discrepancies are corrected here. Instructor's post-presentation feedback motivates the supplemental tests. Repository: vanshika1631/OpsAI_TrackB, initial commit eae698f. Internal evidence index: evidence/README.md. Feedback closure: submission/feedback_response.md."),
        ("source", "AI decisions recorded in the final-preparation conversation: user explicitly accepted both recommendations. No unavailable historical AI advice or verification is invented."),
    ]))

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleCustom", fontName="Helvetica-Bold", fontSize=18, leading=21, textColor=NAVY, spaceAfter=10))
    styles.add(ParagraphStyle(name="LeadCustom", fontName="Helvetica-Bold", fontSize=14, leading=18, textColor=TEAL, spaceAfter=8))
    styles.add(ParagraphStyle(name="HeadCustom", fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=TEAL, spaceBefore=7, spaceAfter=4))
    styles.add(ParagraphStyle(name="BodyCustom", fontName="Helvetica", fontSize=9.6, leading=12.3, spaceAfter=6, textColor=NAVY))
    styles.add(ParagraphStyle(name="SourceCustom", fontName="Helvetica", fontSize=8, leading=10.5, spaceBefore=5, textColor=colors.HexColor("#516374")))
    styles.add(ParagraphStyle(name="CellCustom", fontName="Helvetica", fontSize=8.1, leading=10.5, textColor=NAVY))
    story, markdown = [], []
    for i, (num, title, content) in enumerate(pages):
        if i:
            story.append(PageBreak())
        story.append(Paragraph(title, styles["TitleCustom"]))
        markdown.append(f"# {title}\n")
        for kind, value in content:
            if kind == "table":
                cells = [[Paragraph(escape(str(cell)), styles["CellCustom"]) for cell in row] for row in value]
                ncols = len(value[0])
                widths = {3: [175,145,160], 4:[70,130,140,140], 5:[180,78,74,74,74]}.get(ncols)
                if num == "5" and ncols == 3:
                    widths = [112,278,90]
                table_obj = Table(cells, colWidths=widths, hAlign="LEFT")
                table_obj.setStyle(TableStyle([
                    ("BACKGROUND",(0,0),(-1,0),PALE),
                    ("VALIGN",(0,0),(-1,-1),"TOP"),
                    ("LINEBELOW",(0,0),(-1,0),.8,TEAL),
                    ("LINEBELOW",(0,1),(-1,-1),.3,colors.HexColor("#dce5eb")),
                    ("LEFTPADDING",(0,0),(-1,-1),6), ("RIGHTPADDING",(0,0),(-1,-1),6),
                    ("TOPPADDING",(0,0),(-1,-1),5), ("BOTTOMPADDING",(0,0),(-1,-1),5)]))
                story.extend([table_obj, Spacer(1,8)])
                markdown.extend(["| " + " | ".join(map(str,row)) + " |" for row in value[:1]])
                markdown.append("| " + " | ".join(["---"]*ncols) + " |")
                markdown.extend(["| " + " | ".join(map(str,row)) + " |" for row in value[1:]])
                markdown.append("")
            elif kind == "chart":
                story.extend([lag_chart(), Spacer(1,6)])
                markdown.append("[Lag chart: all six runs, shared axes; see PDF and b1_stream_telemetry.csv.]\n")
            else:
                style = {"lead":"LeadCustom", "h":"HeadCustom", "p":"BodyCustom", "source":"SourceCustom"}[kind]
                story.append(Paragraph(value, styles[style]))
                markdown.append(("## " if kind=="h" else "")+value.replace("<br/>","  \n")+"\n")
        markdown.append("\n<!-- page break -->\n")
    DEST.mkdir(exist_ok=True)
    (DEST / "technical_operationalization_brief.md").write_text("\n".join(markdown), encoding="utf-8")
    def page_decoration(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(TEAL)
        canvas.setLineWidth(2)
        canvas.line(54, 752, 558, 752)
        canvas.setFillColor(NAVY)
        canvas.setFont("Helvetica", 8)
        canvas.drawString(54, 762, "94-879  |  TRACK B  |  TECHNICAL OPERATIONALIZATION BRIEF")
        canvas.setFillColor(colors.HexColor("#516374"))
        canvas.drawString(54, 28, "Tiwari / Bhinde / Ekade  |  Final submission  |  October 2026")
        canvas.drawRightString(558,28,str(doc.page))
        canvas.restoreState()
    doc = SimpleDocTemplate(str(DEST / "technical_operationalization_brief.pdf"),
        pagesize=(612,792), leftMargin=60,rightMargin=60,topMargin=55,bottomMargin=45,
        title="Track B Technical Operationalization Brief", author="Vanshika Tiwari; Harshid Bhinde; Hritwik Ekade")
    doc.build(story,onFirstPage=page_decoration,onLaterPages=page_decoration)


if __name__ == "__main__":
    build()
