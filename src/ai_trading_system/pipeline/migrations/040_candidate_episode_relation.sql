-- ADR-0006 A1: append-only momentum-to-breakout episode supersession.

CREATE TABLE IF NOT EXISTS candidate_episode_relation (
    relation_id VARCHAR PRIMARY KEY,
    predecessor_candidate_id VARCHAR NOT NULL,
    successor_candidate_id VARCHAR NOT NULL,
    relation_type VARCHAR NOT NULL,
    related_at TIMESTAMP NOT NULL,
    rule_version VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    source_artifact_hash VARCHAR NOT NULL,
    idempotency_key VARCHAR NOT NULL,
    semantic_payload_hash VARCHAR NOT NULL,
    schema_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_episode_relation_idempotency
    ON candidate_episode_relation(idempotency_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_episode_relation_predecessor
    ON candidate_episode_relation(predecessor_candidate_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_candidate_episode_relation_successor
    ON candidate_episode_relation(successor_candidate_id);
