CREATE TABLE IF NOT EXISTS ingestion_artifact (
    ingestion_run_id VARCHAR NOT NULL,
    artifact_id VARCHAR NOT NULL,
    attached_at TIMESTAMP NOT NULL,
    PRIMARY KEY (ingestion_run_id, artifact_id)
);

INSERT INTO ingestion_artifact (ingestion_run_id, artifact_id, attached_at)
SELECT ingestion_run_id, artifact_id, retrieved_at
FROM source_artifact
WHERE ingestion_run_id IS NOT NULL
ON CONFLICT DO NOTHING;
