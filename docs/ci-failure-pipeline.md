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
        W2 --> T1{"transient failure?"}
        T1 -- yes --> N2["no issue"]
        T1 -- no --> T2{"known mechanical fix? cargo fmt, checks workflow"}
        T2 -- yes --> BA["auto_fix"]
        T2 -- no --> T3["classify the failure"]:::claude
        T3 --> BC["code_fix"]
        T3 --> BH["human_action"]
        BA --> G1{"webhook auto_fix on, default off, and no open PR?"}
        BC --> G1
        G1 -- no --> N3["no issue"]
        G1 -- yes --> D1
        BH --> D1{"matches a resolved issue?"}
        D1 -- yes --> N4["no issue, no comment"]
        D1 -- no --> D2{"matches an open issue?"}
        D2 -- yes --> C1["comment 'failed again' on it"]
        D2 -- no --> I1["create YouTrack issue"]
        I1 --> I2{"bucket"}
        I2 -- human_action --> I3["assign a human"]
        I2 -- "code_fix, auto_fix" --> I4["AI Agent: Queued"]
    end

    F --> W1
    I3 --> YT[("open CI-failure issues")]
    I4 --> YT

    subgraph SC ["scheduled actions, every 60s, independent of each other"]
        YT --> A1["ci-failure-analysis: analyze the log"]:::claude
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
| Webhook ingest and triage         | `src/web/webhooks.rs`, `src/triage.rs` |
| Gates, dedupe, and issue creation | `src/mint.rs`                         |
| ci-failure-analysis               | `src/native/ci_failure_analysis.rs`   |
| ci-failure-reconcile              | `src/native/ci_failure_reconcile.rs`  |
| youtrack                          | `src/native/youtrack.rs`              |
| forgejo-ci-fix                    | `src/native/forgejo_ci_fix.rs`        |
