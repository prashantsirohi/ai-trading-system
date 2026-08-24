ALTER TABLE jcurve_seed_candidate
    ADD COLUMN IF NOT EXISTS screen_exchange VARCHAR;

ALTER TABLE jcurve_seed_candidate
    ADD COLUMN IF NOT EXISTS screen_listing_code VARCHAR;

ALTER TABLE jcurve_seed_candidate
    ADD COLUMN IF NOT EXISTS screen_isin VARCHAR;

ALTER TABLE jcurve_seed_candidate
    ADD COLUMN IF NOT EXISTS screen_nse_symbol VARCHAR;

ALTER TABLE jcurve_seed_candidate
    ADD COLUMN IF NOT EXISTS screen_bse_code VARCHAR;
