# CI-failure pipeline

How a failed Forgejo Actions run becomes YouTrack work, as the code runs today. Any change to the pipeline updates this
file in the same PR. The improvement discussion lives in [`ROADMAP.md`](ROADMAP.md#ci-failure-pipeline).

Steps with an orange border call `claude`.

```mermaid
flowchart TD
    F(["Forgejo: Actions run finished"])

    subgraph WH ["webhook-forgejo"]
        W1{"run failed?"}
        W1 -- "no: success, recover, cancelled" --> N1["ignored"]
        W1 -- yes --> W2["fetch run logs"]
        W2 --> FP0["fingerprint failing tests"]
        FP0 --> PR1["resolve_open_pr: this commit's own open PR, if any"]
        PR1 -- "lookup error" --> N4["run fails (ok=false); nothing minted or commented"]
        PR1 --> PR2{"open PR for this commit?"}

        PR2 -- no --> S1{"ci_failure store match?"}
        S1 -- "open" --> S2["attach run + tests, comment"]
        S1 -- "resolved" --> S3["reopen: set_state, regression comment"]
        S3 --> T1
        S1 -- "none / pr_only" --> L1{"legacy YouTrack marker search hit?"}
        L1 -- yes --> L2["adopt: record store row, attach"]
        L1 -- no --> T1{"transient failure?"}
        T1 -- yes --> N2["no issue"]
        T1 -- no --> T2{"known mechanical fix? cargo fmt, checks workflow"}
        T2 -- yes --> BA["auto_fix"]
        T2 -- no --> T3["AnalysisClassifier: Read/Grep/Bash over the full log + failing workflow YAML, one fenced JSON verdict"]:::claude
        T3 -- "code_defect" --> BC["code_fix"]
        T3 -- "needs_human" --> BH["human_action"]
        T3 -- "flaky / infra" --> N2
        BA --> I1["create YouTrack issue"]
        BC --> I1
        BH --> I1
        I1 --> R0["record ci_failures row"]
        R0 --> I2{"bucket"}
        I2 -- human_action --> I3["assign a human"]
        I2 -- "code_fix, auto_fix" --> G2{"webhook auto_fix on?"}
        G2 -- yes --> I4["AI Agent: Queued"]
        G2 -- no --> I5["no AI Agent write"]
        S3 --> G3{"code_fix/auto_fix bucket and auto_fix on?"}
        G3 -- yes --> I4

        PR2 -- yes --> AG1{"agent_pull_requests row open for this PR?"}
        AG1 -- yes --> AG2{"fix_attempts below config.webhook.agent_pr_fix_attempts?"}
        AG2 -- yes --> AG3["comment the issue: run + failing tests + analysis; bump fix_attempts; AI Agent: Queued"]
        AG2 -- no --> AG4["AI Agent: Needs Review; comment lists every attempt's run"]
        AG1 -- no --> H1{"failing tests match a ci_failure row?"}
        H1 -- "open" --> H2["comment PR: already tracked in KEY; attach run; no analysis"]
        H1 -- "pr_only, now 2+ distinct branches" --> H3["mint issue, point the store row at it, comment BOTH PRs"]
        H1 -- "pr_only (still 1 branch) / none" --> H4["analyze, comment the PR, record/keep a pr_only row"]
    end

    F --> W1
    I3 --> YT[("open CI-failure issues")]
    I4 --> YT
    I5 --> YT
    S2 --> YT
    L2 --> YT
    AG3 --> YT
    AG4 --> YT
    H3 --> YT

    subgraph SC ["scheduled actions, every 60s, independent of each other"]
        YT --> A1["ci-failure-analysis: analyze the log (scheduled for retirement, CLAUDE-279 - superseded by the webhook's own AnalysisClassifier above)"]:::claude
        A1 --> A2["comment root cause on the issue"]
        YT --> R1{"ci-failure-reconcile: duplicate of an older open issue?"}
        R1 -- yes --> R2["close as duplicate"]
        R1 -- no --> R3{"job green again on main?"}
        R3 -- yes --> R4["close issue"]
        YT -- "AI Agent: Queued" --> Y1["youtrack: fix the code"]:::claude
        Y1 --> Y2["open PR"]
        FP[("Forgejo: bot's own PRs with failing CI")] --> X1["forgejo-ci-fix: fix and push"]:::claude
    end

    Y2 -. "CI runs on the PR" .-> F

    classDef claude stroke:#d9480f,stroke-width:4px
```

## Where this lives

| Step                              | Code                                  |
|-----------------------------------|---------------------------------------|
| Webhook ingest, store dedupe, and triage | `src/web/webhooks.rs`, `src/triage.rs` |
| At-ingest analysis (`AnalysisClassifier`), shared with the scheduled action below | `src/ci_failure/analysis.rs` |
| Failing-test fingerprint and the `ci_failures` store | `src/ci_failure/fingerprint.rs`, `src/ci_failure/store.rs` |
| Gates and issue creation (no dedupe)     | `src/mint.rs`                   |
| ci-failure-analysis               | `src/native/ci_failure_analysis.rs`   |
| ci-failure-reconcile              | `src/native/ci_failure_reconcile.rs`  |
| youtrack                          | `src/native/youtrack.rs`              |
| forgejo-ci-fix                    | `src/native/forgejo_ci_fix.rs`        |
