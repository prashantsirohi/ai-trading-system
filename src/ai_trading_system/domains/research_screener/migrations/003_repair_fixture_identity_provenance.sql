-- Migration 001 initially allowed replay-only fixture rows to seed canonical
-- security masters. Repair canonical rows from official listing evidence and
-- remove fixture-only securities that have no validated listing.
UPDATE universe_member AS u
SET company_id = 'company:fixture:' || u.fixture_symbol,
    security_id = 'security:fixture:' || u.fixture_symbol,
    identity_status = 'FROZEN_FIXTURE_ONLY'
FROM universe_snapshot s
JOIN screening_run r ON r.run_id = s.run_id
WHERE u.universe_snapshot_id = s.universe_snapshot_id
  AND r.run_mode = 'regression_replay';

UPDATE security_master AS s
SET source_artifact_id = l.source_artifact_id,
    valid_from = l.valid_from
FROM (
    SELECT security_id, min(source_artifact_id) AS source_artifact_id, min(valid_from) AS valid_from
    FROM listing_master
    GROUP BY security_id
) AS l
WHERE s.security_id = l.security_id;

UPDATE company_master AS c
SET source_artifact_id = s.source_artifact_id,
    valid_from = s.valid_from
FROM security_master AS s
WHERE c.company_id = s.company_id
  AND EXISTS (SELECT 1 FROM listing_master l WHERE l.security_id = s.security_id);

DELETE FROM security_identifier_history h
WHERE NOT EXISTS (SELECT 1 FROM listing_master l WHERE l.security_id = h.security_id)
  AND EXISTS (
      SELECT 1 FROM security_master s
      JOIN source_artifact a ON a.artifact_id = s.source_artifact_id
      WHERE s.security_id = h.security_id AND a.source_key = 'canary_fixture'
  );

DELETE FROM security_master s
WHERE NOT EXISTS (SELECT 1 FROM listing_master l WHERE l.security_id = s.security_id)
  AND EXISTS (
      SELECT 1 FROM source_artifact a
      WHERE a.artifact_id = s.source_artifact_id AND a.source_key = 'canary_fixture'
  );

DELETE FROM company_master c
WHERE NOT EXISTS (SELECT 1 FROM security_master s WHERE s.company_id = c.company_id)
  AND c.company_id NOT LIKE 'company:fixture:%';
