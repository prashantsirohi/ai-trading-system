DELETE FROM security_identifier_history
WHERE identifier_history_id IN (
    SELECT identifier_history_id
    FROM (
        SELECT identifier_history_id,
               row_number() OVER (
                   PARTITION BY security_id, identifier_type, identifier_value,
                                coalesce(exchange, ''), valid_from
                   ORDER BY source_artifact_id, identifier_history_id
               ) AS duplicate_ordinal
        FROM security_identifier_history
    ) ranked
    WHERE duplicate_ordinal > 1
);
