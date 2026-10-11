# Professor feedback: closure and remaining limits

| Feedback | Final response and evidence |
|---|---|
| Isolate save cost from elapsed time | Randomized 7-trial fixed-size comparisons at 0, 5k, 20k, 80k pre-existing rows; `b1_save_trials.csv`. Per-save growth supports cumulative quadratic rewrite work; timing alone does not prove an asymptotic law. |
| B3 falsifier was met | Hypothesis partly rejected: drift detection supported, claimed necessary model degradation/action escalation not supported. Investigation remains justified by material input change and limited proxy evidence. |
| State probe sample size | Historical evidence states 300 requests per endpoint after 20 warm-ups; median overhead 1.15 ms. |
| Reconcile 14,076 workload and 73,607 lag | Original commands unavailable; no claim that looping is confirmed. Old lag/rate claims withdrawn from the decision evidence. Fresh unique topics, finite input, exact delivered-message count, zero backward timestamp steps. |
| Report achieved arrival rate | Kafka delivery acknowledgements divided by producer elapsed time, including final flush; per-run values in `b1_stream_summary.csv`. |
| Confirm rolling features after replay | No loop; all 11,754 rows and all seven columns compare exactly with direct single-pass features in all six Kafka runs. |
| Same chart axes, labeled x-axis, repeated trials | One lag plot with shared axes and all three runs per condition; elapsed seconds and lag in messages explicitly labeled. |
| Reconcile 5x vs 4x spread | First example is 5x; measured aggregate normalized-spread mean ratio is 3.9792x. Per-tick raw-spread ratios range about 3x to 5x. |
| Output rows identical after append | Exact dataframe checks for all finite input feature rows, both storage paths and every Kafka run. |
| Hard-kill test and loss | Real Linux SIGKILL: rewrite 1,000/1,000 readable, open append 0/1,000 readable. Entire open-file visibility lost, not only final group. Writer-only test, not a measured Kafka recovery test. |
| Old approach called crash-safe | Withdrawn: completed rewrite is readable, but interruption during an in-place rewrite can corrupt it. |
| Bootstrap AP interval | New identifiable model with 111 paired rows and 19 proxy positives; 3,000 block-bootstrap attempts at block sizes 10 and 20. Not retroactive intervals for unavailable slide model. |
| Different alert threshold | .3/.5/.7/.9 table includes alert counts/rates, precision and recall. Saturation at .5 acknowledged; no threshold selected on this tiny test set. |
| Two consequential AI examples | User accepted replacing unverifiable B1 measurements and limiting B1 deployment after the kill test. Brief records proposal, disposition, reason and verification for both. |

## Remaining uncertainty, not hidden omissions

The original B1 replay mode cannot be recovered. The original B3 model cannot be
reconstructed with confidence. The new B3 label definition is retrospective;
training-only calibration yields no test positives. The hard-kill harness proves
writer failure, not an end-to-end offset-recovery loss bound. Multi-hour capacity,
crash-safe offset/data coordination and Kubernetes rollout gating remain future
work. These limits change the recommendations and are explicit in the brief.
