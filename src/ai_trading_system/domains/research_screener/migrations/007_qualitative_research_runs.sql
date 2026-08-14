CREATE TABLE IF NOT EXISTS research_discovery_run (
    run_id VARCHAR PRIMARY KEY,
    parent_screening_run_id VARCHAR NOT NULL,
    research_type VARCHAR NOT NULL,
    semantic_version VARCHAR NOT NULL,
    as_of_date DATE NOT NULL,
    snapshot_hash VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    document_count BIGINT NOT NULL,
    evidence_count BIGINT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL
);
