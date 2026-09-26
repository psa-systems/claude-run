# CI-failure pipeline

How a failed Forgejo Actions run becomes YouTrack work, as the code runs today. Any change to the pipeline updates this
file in the same PR. The improvement discussion lives in [`ROADMAP.md`](ROADMAP.md#ci-failure-pipeline).

Steps with an orange border call `claude`. See [CI failures pipeline](./ci-failures-pipeline.mmd).

## Where this lives

| Step                                                                                                                    | Code                                                                                                     |
|-------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Webhook ingest, store dedupe, and triage                                                                                | `src/web/webhooks.rs`, `src/triage.rs`                                                                   |
| PR-merge bookkeeping (`pull_request` delivery); resolving the issue is a YouTrack workflow, not the runner (CLAUDE-327) | `src/web/webhooks.rs` (`handle_merged_pr`), `docs/yt-workflow-resolve-on-pr-merge.js`                    |
| Match-time regression check against the live YouTrack `State` (CLAUDE-327)                                              | `src/web/webhooks.rs` (`issue_state_is_resolved`)                                                        |
| Event-driven automerge (`success`/`recover` delivery)                                                                   | `src/web/webhooks.rs` (`consume_action_success_events`), `src/native/youtrack.rs` (`complete_automerge`) |
| At-ingest analysis (`AnalysisClassifier`)                                                                               | `src/ci_failure/analysis.rs`                                                                             |
| Failing-test fingerprint and the `ci_failures` store                                                                    | `src/ci_failure/fingerprint.rs`, `src/ci_failure/store.rs`                                               |
| Gates and issue creation (no dedupe)                                                                                    | `src/mint.rs`                                                                                            |
| youtrack                                                                                                                | `src/native/youtrack.rs`                                                                                 |
