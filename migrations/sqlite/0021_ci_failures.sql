-- CLAUDE-272: the CI-failure store. Records a failure's failing-test
-- fingerprint, every run it was seen on, and the PRs the agent opens against
-- it, so a later failure can be matched by test-id overlap instead of a
-- YouTrack full-text search that misses an unindexed issue.
--
-- `ci_failures.issue_key` is nullable: a failure seen only on a human's PR has
-- no issue. `state` is `open` / `resolved` / `pr_only`, textual rather than an
-- enum because sqlx's `Any` driver has no shared enum type across backends.

CREATE TABLE IF NOT EXISTS ci_failures (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    repo        TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    job_name    TEXT NOT NULL,
    infra_kind  TEXT,
    issue_key   TEXT,
    state       TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- The failing-test fingerprint. A `(failure_id, test_id)` pair is unique so
-- `add_tests` can be called with an overlapping set on a repeat sighting
-- without duplicating rows; the index on `test_id` alone is what makes
-- `find_match`'s overlap lookup a seek instead of a scan of every failure.
CREATE TABLE IF NOT EXISTS ci_failure_tests (
    failure_id INTEGER NOT NULL REFERENCES ci_failures(id),
    test_id    TEXT NOT NULL,
    UNIQUE (failure_id, test_id)
);
CREATE INDEX IF NOT EXISTS idx_ci_failure_tests_test_id ON ci_failure_tests(test_id);

-- One row per sighting of a failure: which run, which commit, which branch,
-- and (when the sighting is on a PR) which PR number.
CREATE TABLE IF NOT EXISTS ci_failure_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    failure_id  INTEGER NOT NULL REFERENCES ci_failures(id),
    run_index   INTEGER NOT NULL,
    commit_sha  TEXT NOT NULL,
    branch      TEXT NOT NULL,
    pr_number   INTEGER,
    ts          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ci_failure_runs_ts ON ci_failure_runs(ts);

-- Every PR the runner's own `youtrack` action opens, so a later fix attempt on
-- the same issue can be recognised as a continuation rather than minted as a
-- fresh one. Unique on `(host, repo, pr_number)`: a PR is addressed by where it
-- lives, not by which issue currently claims it.
CREATE TABLE IF NOT EXISTS agent_pull_requests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    action_name  TEXT NOT NULL,
    issue_key    TEXT NOT NULL,
    host         TEXT NOT NULL,
    repo         TEXT NOT NULL,
    pr_number    INTEGER NOT NULL,
    head_branch  TEXT NOT NULL,
    fix_attempts INTEGER NOT NULL DEFAULT 0,
    state        TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    UNIQUE (host, repo, pr_number)
);
CREATE INDEX IF NOT EXISTS idx_agent_pull_requests_issue_key ON agent_pull_requests(issue_key);
