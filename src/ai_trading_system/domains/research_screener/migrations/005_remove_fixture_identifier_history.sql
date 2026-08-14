DELETE FROM security_identifier_history h
WHERE EXISTS (
    SELECT 1 FROM source_artifact a
    WHERE a.artifact_id = h.source_artifact_id
      AND a.source_key = 'canary_fixture'
);
