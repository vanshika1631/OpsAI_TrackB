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
INK = colors.black
PAPER = colors.white


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
        d.add(Line(left, y, left+width, y, strokeColor=INK, strokeWidth=.2, strokeDashArray=[1, 4]))
        d.add(String(left-7, y-3, str(tick), textAnchor="end", fontName="Helvetica", fontSize=8))
    for tick in range(0, xmax+1, 5):
        x = left+tick/xmax*width
        d.add(String(x, bottom-13, str(tick), textAnchor="middle", fontSize=8))
    d.add(String(left, h-8, "Lag (raw messages; log-end minus committed offset)", fontSize=9, fillColor=INK))
    d.add(String(left+width/2, 3, "Elapsed time since replay start (seconds)", textAnchor="middle", fontSize=9))
    for mode, color in [("rewrite", INK), ("append", INK)]:
        for repeat in range(3):
            selected = [r for r in rows if r["mode"] == mode and int(r["repeat"]) == repeat]
            path = DrawPath(strokeColor=color, strokeWidth=[.8, 1.2, 1.6][repeat], fillColor=None,
                            strokeDashArray=[] if mode == "rewrite" else [4, 3])
            for i, row in enumerate(selected):
                x, y = left+float(row["elapsed_s"])/xmax*width, bottom+float(row["lag"])/ymax*height
                (path.moveTo if i == 0 else path.lineTo)(x, y)
            d.add(path)
    d.add(String(left+170, h-22, "Rewrite: solid (3 runs)", fontSize=8, fillColor=INK))
    d.add(String(left+303, h-22, "Append: dashed (3 runs)", fontSize=8, fillColor=INK))
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
    pages.append(("1", "Our problem and approach", [
        ("lead", "Track B final project report"),
        ("p", "Vanshika Tiwari | Harshid Bhinde | Hritwik Ekade<br/>94-879 Fundamentals of Operationalizing AI | Track B | October 10, 2026"),
        ("p", "We recommend using B2's corrected model path and readiness check. B1's append fix is faster, but we would only use it for tests where the input can be replayed until its crash-recovery problem is fixed. For B3, we recommend investigating the drift before deciding whether to retrain. Each challenge shows a different way the system can look healthy while something is wrong."),
        ("h", "The system and the evidence we kept"),
        ("p", "The system sends recorded market ticks through Kafka, calculates rolling features, saves them in Parquet, and serves model predictions through an API. We covered B1, B2 and B3 in our presentation. After the feedback, we added the tests in this report. We still have the September 25 B2 logs, but not the original B1 logs or B3 model and script. We use new measurements where we could not verify the old ones."),
        ("h", "B1: what we thought was causing the lag"),
        ("p", "We first thought adding partitions and consumers would reduce lag. We said we would reject this idea if lag kept growing at the same rate. Our presentation reported that result. The producer uses the same BTC-USD key for every message, so all messages go to one partition and the extra consumers can sit idle. We did not repeat that partition experiment for this report, and its original logs are unavailable."),
        ("p", "Our next hypothesis was that rewriting the full Parquet file every 25 feature rows was slowing the consumer down. If save time did not increase with the rows already on disk, or append did not help under the same load, we would question this explanation. The code repeatedly serializes all earlier rows, giving quadratic total work as the file grows. A drop in throughput during one run was not enough to isolate that cause."),
        ("h", "Testing save time at different file sizes"),
        ("p", "We ran seven trials for each writer and file size, in random order. Each trial added the same 25 rows using the same schema, computer and storage. We prepared the existing file before starting the timer, so we could vary file size without relying on how long the run had been going. The append timing does not include closing its footer, which is an important difference in durability."),
        ("table", [["Rows already stored", "Rewrite median (ms)", "Append median (ms)"]] +
            [[f"{size:,}"] + [f"{float(next(x['median'] for x in bench if x['mode']==mode and int(x['existing_rows'])==size)):.2f}" for mode in ["rewrite", "append"]] for size in [0,5000,20000,80000]]),
        ("p", "The rewrite took longer as the stored row count increased. After initialization, append time stayed close to 1 ms. This supports our explanation of the code, although the timings alone do not prove an asymptotic growth rate. These tests used a warm cache and did not test fsync or power loss."),
        ("source", "Evidence: b1_save_trials.csv (56 trials); b1_save_summary.csv; supplied baseline _append_parquet and submitted _ParquetAppender."),
    ]))
    pages.append(("2", "B1: results and the crash problem", [
        ("h", "Repeating the same workload"),
        ("p", "For each run, we used a new empty topic and consumer group, one partition, one consumer and a fresh output file. We replayed the same 14,076 messages once, without looping. They produced 11,754 feature rows because non-ticker messages were skipped. Timestamps never moved backward. We tested each writer three times, alternated the order, and ran the final timings without other benchmarks or bootstrap calculations running."),
        ("chart", "lag"),
        ("table", [["Measure (3 runs each)", "Rewrite", "Append"],
            ["Achieved raw arrivals/s", span(vals("rewrite","achieved_acked_rate"),1), span(vals("append","achieved_acked_rate"),1)],
            ["Peak sampled lag (messages)", span(vals("rewrite","peak_sampled_lag")), span(vals("append","peak_sampled_lag"))],
            ["Observed completion (seconds)", span(vals("rewrite","completion_elapsed_s"),2), span(vals("append","completion_elapsed_s"),2)],
            ["Features / observed second", span([11754/t for t in vals("rewrite","completion_elapsed_s")],1), span([11754/t for t in vals("append","completion_elapsed_s")],1)],
            ["Final sampled lag", "0 in all runs", "0 in all runs"],
            ["Exact feature-row equality", "11,754 / 11,754", "11,754 / 11,754"]]),
        ("p", "We calculated the arrival rate from acknowledged messages divided by producer time, including the final flush. We sampled lag about once a second, so completion times include some observation delay. Every output row and column matched the direct single-pass calculation exactly. This shows an improvement on this finite workload; it does not tell us the capacity over several hours."),
        ("h", "What happened when we used kill -9"),
        ("p", "We sent Linux SIGKILL after 40 completed batches of 25 rows. The rewrite file still had 1,000 readable rows, but the append file had zero rows readable by PyArrow because its footer was missing. This affected the whole open file, not just the last row group. This test only checked the writer and did not commit Kafka offsets. The actual consumer commits after appending, so it could commit offsets for output that becomes unreadable after a crash."),
        ("p", "We would keep the SIGTERM/SIGINT handler because it closes the writer during a normal shutdown. It cannot help with SIGKILL. The old rewrite method can also corrupt a file if it is killed during a write. For now, we recommend append only when the input is retained and can be replayed, with a new output path each time. Durable file segments and recovery of data and offsets still need to be tested."),
        ("source", "Evidence: b1_stream_summary.csv; b1_stream_telemetry.csv and six logs; b1_sigkill.json. Original 73,607 lag and 3,000/s achieved-rate claims are withdrawn: looping or pre-existing backlog cannot be distinguished without the missing logs."),
    ]))
    pages.append(("3", "B2: fixing the model path and health check", [
        ("h", "Finding the cause"),
        ("p", "We found that MODEL_PATH used /app/models/artifact/model.pkl, but the mounted folder was named artifacts. The original scorer caught the missing-file error and kept running. Docker checked /health, which returned 200 even when /score returned 503. The wrong path caused the scoring failure, and the health check failed to detect it."),
        ("p", "Our hypothesis was that correcting only MODEL_PATH would restore scoring with the same image, volume and request payload. If scoring still returned 503, the path would not explain the failure. The saved logs show that the configured path did not exist, the mounted model did exist, and changing only the path restored HTTP 200 and the baseline score. Restarting three times with the wrong path did not fix it."),
        ("h", "What we changed"),
        ("p", "We corrected the model path and kept /health as a simple check that the process is responding. We added /ready to check that the model is loaded and run a canary through the same predict function as /score. It only returns 200 if the prediction is finite and between 0 and 1. Otherwise, it returns 503 and explains the failure. Docker now checks /ready, so a model-loading failure no longer looks healthy."),
        ("table", [["Controlled condition", "Docker", "/health", "/ready", "/score"],
            ["Original + bad path", "healthy", "200", "absent", "503"],
            ["Repaired + hardened", "healthy", "200", "200", "200"],
            ["Hardened + bad path", "unhealthy", "200", "503", "503"]]),
        ("p", "In the saved test, we checked Docker's status every 10 seconds. It showed starting from 0 to 40 seconds, then unhealthy at 50 seconds after five failed probes. /health stayed at 200 after startup. We also tested a corrupt model: the original scorer exited with code 1, while the updated scorer stayed up and reported UnpicklingError through /ready."),
        ("h", "Checking the result and probe cost"),
        ("p", "The earlier latency test used 300 sequential requests per endpoint after 20 warm-ups. /health had p50 3.50 ms and p95 5.51 ms; /ready had p50 4.65 ms and p95 6.03 ms. The median difference was 1.15 ms. These were local sequential requests, so they do not show the cost under concurrent load. We have the summary, but not the individual timings."),
        ("p", "We added the locally tested scorer and corrected Compose files to the final repository. Using Flask's test client and the new model, we checked /health, /ready and /score again. They returned 200/200/200 with a valid model and 200/503/503 with missing or corrupt models. These new checks test the application; they are not a repeat of the Docker timing experiment."),
        ("h", "What we recommend and what is still untested"),
        ("p", "We recommend both changes. In Compose, unhealthy is just a status label; it does not stop traffic or restart the container. A canary with outdated inputs could mark every replica unready, and a working but incorrect model could still pass. We would monitor ready replicas, scoring errors, model-load errors and startup time. Traffic removal and failed rollouts would need separate Kubernetes tests."),
        ("source", "Evidence: evidence/b2_historical/01-08 captures, especially 03_diagnosis, 05_fault_injected_hardened_code and 07_probe_cost; supplemental/results.json, b2 section."),
    ]))
    pages.append(("4", "B3: checking drift and model performance", [
        ("h", "Why we partly rejected our hypothesis"),
        ("p", "We expected wider spreads to cause drift in spread_normalized and affect the model. Our original rejection rule included the case where drift appeared but the model was unaffected. We detected drift, but we did not show clear model degradation, so our hypothesis was partly rejected. We still recommend investigating because the inputs changed substantially and this small synthetic test cannot show that the change is harmless."),
        ("h", "Was the spread change 4x or 5x?"),
        ("p", f"We used the same featurizer on both files. Each had 782 raw messages and 653 feature rows. Mean normalized spread increased {b3['normalized_spread_mean_ratio']:.4f}x, and mean raw spread increased {b3['raw_spread_mean_ratio']:.4f}x. The first tick changed from 0.01 to 0.05, which is 5x, but the ratios across ticks ranged from about 3x to 5x. This explains the slide discrepancy: the average change was about 4x, while the example was 5x."),
        ("table", [["Feature", "KS statistic", "Interpretation"],
            ["spread_normalized", "0.9525", "Large distribution shift"],
            ["midprice_returns", "0.0046", "Small rounding perturbation"],
            ["order_book_imbalance", "0.0000", "Unchanged"],
            ["trade_intensity", "0.0000", "Unchanged"]]),
        ("p", "We used SciPy to check the KS results from the presentation. The spread p-value was too small to represent numerically; the other p-values were 1. Since the rows are time-dependent, we put more weight on the size of the change than the p-value. Only one of four features changed substantially, so the 50% dataset-level rule could miss the spread change."),
        ("h", "Testing a new model on the two inputs"),
        ("p", "We did not have the original model from the slides, so we fitted a new balanced logistic regression and scaler on the first 371 rows. We left a 20-row gap before the 60% training boundary. The test set used rows 522-632, giving 111 paired rows after removing the last 20 rows without future labels. We labeled spikes using forward 20-row return volatility and the reference-wide 90th percentile. We kept these labels fixed for both inputs; 19 test rows were positive."),
        ("p", "These labels use the full reference window, so this is a retrospective comparison. A cutoff based only on training data gave no positive test cases. Trade prices were unchanged, but rounding the bid and ask slightly changed midprice returns (maximum absolute difference 9.85e-8). We did not use test rows to fit the model or scaler."),
        ("table", [["Average precision (AP)", "Estimate", "95% block-bootstrap interval"],
            ["Reference", f"{b3['reference_ap']:.3f}", interval(b3['bootstrap']['20']['reference_ap'])],
            ["Current", f"{b3['current_ap']:.3f}", interval(b3['bootstrap']['20']['current_ap'])],
            ["Current minus reference", f"{b3['current_ap']-b3['reference_ap']:.3f}", interval(b3['bootstrap']['20']['delta_ap'])]]),
        ("p", "We used sklearn's average_precision_score for AP, which we call PR-AUC here; it is not the trapezoidal area under the PR curve. We tried 3,000 paired circular block-bootstrap samples with block length 20. Of these, 2,732 had both classes. The interval for the difference includes zero. These intervals apply to the new model, not the slides' 0.208/0.303 result, so we cannot label that earlier result as noise."),
        ("source", "Evidence: results.json; b3_drift.csv; b3_paired_scores.csv; b3_bootstrap.csv. Block length 10 sensitivity: delta 95% interval [0.000, 0.171], 2,902 valid draws. Intervals omit model-training uncertainty."),
    ]))
    table = [["Threshold", "Alerts: ref / current", "Precision: ref / current", "Recall: ref / current"]]
    for threshold in [.3,.5,.7,.9]:
        a,b = [next(x for x in thresholds if x["window"] == name and float(x["threshold"]) == threshold) for name in ["reference","current"]]
        table.append([f"{threshold:.1f}", f"{a['alerts']}/111 / {b['alerts']}/111",
            "undefined / undefined" if threshold == .9 else f"{float(a['precision']):.3f} / {float(b['precision']):.3f}",
            f"{float(a['recall']):.3f} / {float(b['recall']):.3f}"])
    pages.append(("5", "What we would monitor and do next", [
        ("h", "Trying different alert thresholds"),
        ("table", table),
        ("p", "At 0.5, 90.1% of reference rows and 88.3% of current rows were flagged, so alert rate alone tells us little. Recall fell from 0.895 to 0.789 despite higher AP; saying the model was unaffected was too strong. At 0.7, alert rates were 64.9% and 63.1%, with recall 0.684 in both. At 0.9 there were no alerts. These comparisons explore sensitivity; we did not use them to choose a threshold."),
        ("p", f"The mean absolute score change was {b3['mean_abs_score_change']:.4f}. Results depend on the threshold, and the intervals are wide. We cannot conclude that production performance would be better or worse. We would keep serving while investigating, rather than retrain from a drift p-value or one AP estimate."),
        ("h", "Our suggested monitoring plan"),
        ("table", [["Signal", "Trigger / response", "Owner"],
            ["Infrastructure / readiness", "Zero ready replicas or scoring 5xx: page; inspect path/artifact, restore known-good config.", "Service operator"],
            ["B1 lag + freshness", "Sustained lag growth or stale output: inspect save cost; reduce intake or replay safely.", "Pipeline owner"],
            ["Feature drift", "Proposed: KS effect >0.2 AND spread mean ratio >1.5; investigate feed/units and market context.", "Model + data owner"],
            ["Model quality", "Collect delayed labels; compare precision/recall and AP with uncertainty and operating cost.", "Model owner"]]),
        ("p", "This drift threshold is a starting point; we have not measured its false-alarm rate. We would set a minimum window size, require persistent changes and avoid duplicate alerts. We would track health, drift and model quality separately, since the dataset rule can miss a large change in one feature."),
        ("h", "Before using these changes in production"),
        ("p", "B1: keep the source data and use a fresh output path for each run. The current writer can overwrite an existing file after a restart. Before production, we need closed, durable file segments, a clear plan for recovering data and offsets, and tests that kill the process during a write and then restart it. A committed Kafka offset does not by itself prove the output is safely stored."),
        ("p", "B2: use the path repair and readiness check after checking that the canary inputs match the model. Keep a rollback procedure because restarting cannot fix a missing model. Check model identity separately: /ready shows that prediction runs, not that it is the intended model. We would also pin tested image digests before production because the starter uses Kafka's latest tag."),
        ("p", "B3: check the data source and units, save both windows, and collect labels from a real period with wider spreads. We would retrain only after finding a sustained, meaningful drop in labeled performance and testing a replacement. Too many drift alerts could waste operators' time, but ignoring a large change in one feature could hide a data problem."),
        ("source", "Evidence: b3_thresholds.csv and b3_paired_scores.csv. Proposed operational triggers are recommendations, not tested SLAs or production guarantees."),
    ]))
    pages.append(("6", "How we used AI and what is still open", [
        ("h", "AI example 1: rerunning B1 with better controls"),
        ("p", "AI suggested replacing the B1 slide numbers we could not verify with new controlled experiments. We accepted this during final preparation. The reason was that a clean, one-pass workload of 14,076 messages could not explain lag of 73,607. We also could not confirm that the configured rate of 3,000/s was achieved without the original logs."),
        ("p", "We checked this recommendation by running six tests with new Kafka topics and groups. Each acknowledged all 14,076 messages, recorded the achieved rate, and had no backward timestamps. All 11,754 feature rows matched the direct calculation exactly. We also timed saves at fixed file sizes. These tests gave us evidence for the final report, but they do not recover or validate the missing original runs."),
        ("h", "AI example 2: changing our B1 recommendation"),
        ("p", "AI also suggested changing our recommendation from 'ship append with a shutdown handler' to using it only where replay is possible until durability is fixed. We accepted this after reviewing the SIGKILL result. A normal shutdown handler cannot run after SIGKILL, and without the footer the open Parquet file cannot be read normally."),
        ("p", "We checked this with a test that completed 40 batches (1,000 rows) and then sent Linux SIGKILL. The rewrite file had 1,000 readable rows; the append file had none. We changed our recommendation and removed the claim that the old method was always crash-safe. This test checked file readability, not recovery of committed Kafka data. We are responsible for explaining that difference and for the final recommendation."),
        ("h", "How to repeat our tests"),
        ("p", "To repeat the tests, install experiments/requirements.txt with Python 3.12 and run experiments/validate_submission.py from the repository root. Start the Kafka broker and run experiments/stream_comparison.py --rate 1500 for the streaming comparison. On Linux, run validate_submission.py --crash for the kill test. The README has the Docker setup, B2 fault test and expected outputs."),
        ("p", "We included the scripts, input hashes, trial results, lag logs, paired scores and bootstrap samples. The older B2 logs are dated September 25, and the new tests are dated October 10. The original code is available in the challenge package and initial commit. The evidence files contain the details behind the tables in this report."),
        ("h", "Our main unanswered question"),
        ("p", "Our biggest remaining question is whether the faster B1 pipeline can recover all committed output after a crash and restart. The speed improvement is useful, but we cannot call it production-ready until we test a reliable way to save data and commit offsets. For B3, we still need labels from a real wide-spread period. The synthetic input change and retrospective labels cannot tell us how useful the model would be there."),
        ("h", "Sources and supporting files"),
        ("p", "We used the assignment, 94-879 Final Team Project Track B - Technical Challenges, and the professor's feedback to plan the final tests. This report updates the results in our presentation, When Healthy Isn't Working. Our repository is vanshika1631/OpsAI_TrackB (initial commit eae698f). Details about the evidence are in evidence/README.md, and our response to each feedback item is in submission/feedback_response.md."),
        ("source", "These examples come from the final-preparation work. Both recommendations were accepted in that discussion."),
    ]))

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TitleCustom", fontName="Helvetica-Bold", fontSize=18, leading=21, textColor=INK, spaceAfter=10))
    styles.add(ParagraphStyle(name="LeadCustom", fontName="Helvetica-Bold", fontSize=14, leading=18, textColor=INK, spaceAfter=8))
    styles.add(ParagraphStyle(name="HeadCustom", fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=INK, spaceBefore=7, spaceAfter=4))
    styles.add(ParagraphStyle(name="BodyCustom", fontName="Helvetica", fontSize=9.6, leading=12.3, spaceAfter=6, textColor=INK))
    styles.add(ParagraphStyle(name="SourceCustom", fontName="Helvetica", fontSize=8, leading=10.5, spaceBefore=5, textColor=INK))
    styles.add(ParagraphStyle(name="CellCustom", fontName="Helvetica", fontSize=8.1, leading=10.5, textColor=INK))
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
                    ("BACKGROUND",(0,0),(-1,0),PAPER),
                    ("VALIGN",(0,0),(-1,-1),"TOP"),
                    ("LINEBELOW",(0,0),(-1,0),.8,INK),
                    ("LINEBELOW",(0,1),(-1,-1),.3,INK),
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
        canvas.setStrokeColor(INK)
        canvas.setLineWidth(2)
        canvas.line(54, 752, 558, 752)
        canvas.setFillColor(INK)
        canvas.setFont("Helvetica", 8)
        canvas.drawString(54, 762, "94-879  |  TRACK B  |  TECHNICAL OPERATIONALIZATION BRIEF")
        canvas.setFillColor(INK)
        canvas.drawString(54, 28, "Tiwari / Bhinde / Ekade  |  Final submission  |  October 2026")
        canvas.drawRightString(558,28,str(doc.page))
        canvas.restoreState()
    doc = SimpleDocTemplate(str(DEST / "technical_operationalization_brief.pdf"),
        pagesize=(612,792), leftMargin=60,rightMargin=60,topMargin=55,bottomMargin=45,
        title="Track B Technical Operationalization Brief", author="Vanshika Tiwari; Harshid Bhinde; Hritwik Ekade")
    doc.build(story,onFirstPage=page_decoration,onLaterPages=page_decoration)


if __name__ == "__main__":
    build()
