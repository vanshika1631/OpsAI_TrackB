# How we addressed the professor's feedback

| Feedback | What we changed or checked |
|---|---|
| Isolate save cost from elapsed time | We ran seven trials at each existing file size: 0, 5k, 20k and 80k rows. We randomized the order and timed the save separately. See `b1_save_trials.csv`. The results support the rewrite-cost explanation, although timings alone do not prove the asymptotic growth rate. |
| B3 falsifier was met | We now say the hypothesis was partly rejected. We detected drift but did not show clear degradation. We still recommend checking the source of the change because the inputs changed substantially and our model test is limited. |
| State probe sample size | We checked the saved summary: 300 requests per endpoint after 20 warm-ups. The median difference was 1.15 ms. |
| Reconcile 14,076 messages and 73,607 lag | We could not recover the original commands, so we cannot confirm looping. We replaced those lag/rate claims with new runs using empty topics, one-pass input, acknowledged message counts and no backward timestamps. |
| Report achieved arrival rate | We divided acknowledged Kafka messages by producer elapsed time, including the final flush. Each run's rate is in `b1_stream_summary.csv`. |
| Confirm rolling features after replay | We did not loop the input. All 11,754 rows and all seven columns matched the direct single-pass calculation in all six Kafka runs. |
| Same chart axes, labeled x-axis, repeated trials | The new plot shows all three runs for each writer on the same axes. It labels elapsed seconds and lag in messages, and uses solid/dashed lines for black-and-white printing. |
| Reconcile 5x vs 4x spread | The first tick changed 5x, but the mean normalized spread changed 3.9792x. Individual raw-spread ratios ranged from about 3x to 5x. We explain this difference in the report. |
| Output rows identical after append | We compared the full dataframes exactly for both storage methods and every Kafka run. |
| Hard-kill test and loss | We used Linux SIGKILL after 1,000 completed rows. Rewrite left 1,000 readable rows; append left zero. The whole open file was unreadable, not just the last group. This checked the writer, not Kafka recovery. |
| Old approach called crash-safe | We removed that claim. A completed rewrite is readable, but killing it during an in-place write can still corrupt the file. |
| Bootstrap AP interval | We fitted a new model and used 111 paired test rows with 19 proxy positives. We tried 3,000 block-bootstrap samples each at block sizes 10 and 20. These intervals apply to the new model, not the unavailable slide model. |
| Different alert threshold | We compared thresholds .3, .5, .7 and .9 using alert counts/rates, precision and recall. We explain why the high alert rate at .5 is weak evidence. We did not choose an operating threshold from this small test set. |
| Two consequential AI examples | We describe two recommendations accepted during final preparation: rerun B1 with better controls, and limit append's use after the kill test. For each, we explain the advice, our decision, the reason and how we checked it. |

## What we still cannot answer

We still do not know exactly how the original B1 replay ran, and we do not have
the original B3 model. The new B3 comparison uses retrospective labels; a cutoff
based only on training data gives no positive test cases. The kill test shows the
file problem but does not tell us how much committed Kafka data would be recovered
after a restart. Longer capacity tests, reliable storage and offset recovery, and
Kubernetes rollout tests are still needed. We explain these limits in the report.
