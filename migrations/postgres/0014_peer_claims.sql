-- SPEC-3.17 item 7 / CLAUDE-615: one row per webhook event this instance has
-- claimed or granted to a peer. `holder_id` is the instance that processes the
-- event. Pruned after 24 hours by `claimed_at` (SPEC-3.8.4).
CREATE TABLE peer_claims (
    event_key TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    holder_load BIGINT NOT NULL,
    claimed_at TEXT NOT NULL
);
CREATE INDEX idx_peer_claims_claimed_at ON peer_claims(claimed_at);
