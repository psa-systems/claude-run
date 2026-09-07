-- CLAUDE-93: per-job outbound links for a run, as a JSON object mapping job
-- key to URL (sibling of the `job_keys` JSON array). The webhook fast-path
-- stores the failure event's `html_url` here so the run-detail page can link
-- the job key straight to the Forgejo Actions run; the synthetic
-- webhook-fast-path action has no `link_template` row, so without this the
-- key renders as plain text. NULL for legacy rows and for actions that link
-- via `link_template`.
ALTER TABLE runs ADD COLUMN job_links TEXT;
