INSERT INTO security_identifier_history
SELECT
    'identifier:backfill:' || md5(security_id || ':ISIN:' || isin || ':' || cast(valid_from AS VARCHAR)),
    security_id, 'ISIN', isin, NULL, valid_from, valid_to, source_artifact_id
FROM security_master s
WHERE NOT EXISTS (
    SELECT 1 FROM security_identifier_history h
    WHERE h.security_id = s.security_id AND h.identifier_type = 'ISIN'
      AND h.identifier_value = s.isin AND h.valid_from = s.valid_from
);

INSERT INTO security_identifier_history
SELECT
    'identifier:backfill:' || md5(security_id || ':NSE_SYMBOL:' || symbol || ':' || cast(valid_from AS VARCHAR)),
    security_id, 'NSE_SYMBOL', symbol, 'NSE', valid_from, valid_to, source_artifact_id
FROM listing_master l
WHERE exchange = 'NSE' AND symbol IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM security_identifier_history h
    WHERE h.security_id = l.security_id AND h.identifier_type = 'NSE_SYMBOL'
      AND h.identifier_value = l.symbol AND h.valid_from = l.valid_from
  );

INSERT INTO security_identifier_history
SELECT
    'identifier:backfill:' || md5(security_id || ':BSE_CODE:' || bse_code || ':' || cast(valid_from AS VARCHAR)),
    security_id, 'BSE_CODE', bse_code, 'BSE', valid_from, valid_to, source_artifact_id
FROM listing_master l
WHERE exchange = 'BSE' AND bse_code IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM security_identifier_history h
    WHERE h.security_id = l.security_id AND h.identifier_type = 'BSE_CODE'
      AND h.identifier_value = l.bse_code AND h.valid_from = l.valid_from
  );
