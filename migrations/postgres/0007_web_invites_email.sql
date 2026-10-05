-- Invite-by-email (CLAUDE-394): record who an invite was sent to, so the
-- outstanding-invites table can show a "Sent to" column. Nullable: an invite
-- with no email is still created with the link-only flow.
ALTER TABLE web_invites ADD COLUMN email TEXT;
