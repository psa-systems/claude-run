# Specification

## The specification is normative (CLAUDE-485)

`docs/specification.md` states what claude-run must do, and code that disagrees with it is a defect. Requirements that code relies on carry a `SPEC-<section>` identifier (for example `SPEC-3.3.1`). A comment on code that implements or enforces one cites it, and a change to behavior a requirement covers updates the specification in the same PR.

## Every attempt with commits ends with a pushed branch (CLAUDE-485)

`SPEC-3.3.1`: on every finalize path, success or not, an attempt's commits are pushed to a branch on the forge before the run is recorded finished. A failed push is a failure, never a success.
