# Specification

## The specification is normative (CLAUDE-485)

`docs/specification.md` states what claude-run must do, and code that disagrees with it is a defect. Requirements that code relies on carry a `SPEC-<section>` identifier (for example `SPEC-3.3.1`). A comment on code that implements or enforces one cites it, and a change to behavior a requirement covers updates the specification in the same PR.

## Every attempt with commits ends with a pushed branch (CLAUDE-485)

`SPEC-3.3.1`: on every finalize path, success or not, an attempt's commits are pushed to a branch on the forge before the run is recorded finished. A failed push is a failure, never a success.

## The `specification` branch flow

`docs/specification.md` section 11.2 (`SPEC-11.2` through `SPEC-11.10`) is this repository's form of the shared `common` standard on specification changes (`STD-10`). A requirement is decided by a person first, on the long-lived `specification` branch, and merged to `main` before any implementing change starts. A commit on `specification` changes only `docs/specification.md` and `docs/standards.md`; the branch is protected so only a person can push to it, open a pull request from it, or merge one, and no workflow of this repository triggers on a push to it.

A pull request to `main` always runs `.forgejo/workflows/check.yml`'s `specification` and `spelling` jobs (calling the shared `check-specification` and `check-spelling` actions) and its `secrets` job. For a pull request whose head branch is `specification`, every other job in that workflow (`changes`, `claude-md`, `check`) is skipped rather than run, so the workflow still posts a status for each required check.
