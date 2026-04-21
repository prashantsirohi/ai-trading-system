"""
Delete symbols from catalog that only have 1 year of data (stale from before
we knew about inception dates). These need to be re-ingested with full history.
Keeps symbols that have >300 rows (indicating inception data).
"""

import sys, os, duckdb
from datetime import datetime, timedelta

from core.bootstrap import ensure_project_root_on_path
ensure_project_root_on_path(__file__)
from ai_trading_system.platform.utils.env import load_project_env

load_project_env(__file__)

conn = duckdb.connect("data/ohlcv.duckdb")

ONE_YEAR_AGO = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

stale = conn.execute(f"""
    SELECT symbol_id, COUNT(*) as rows, MIN(timestamp::DATE)::TEXT as first_date
    FROM _catalog
    GROUP BY symbol_id
    HAVING MAX(timestamp::DATE)::TEXT <= '{ONE_YEAR_AGO}'
       OR COUNT(*) < 300
""").fetchall()

print(f"Stale symbols found: {len(stale)}")
for sym_id, cnt, first in stale:
    print(f"  {sym_id:25s}: {cnt:5d} rows  from {first}")

if stale:
    sym_ids = [r[0] for r in stale]
    placeholders = ",".join(["?"] * len(sym_ids))
    deleted = conn.execute(
        f"DELETE FROM _catalog WHERE symbol_id IN ({placeholders})", sym_ids
    ).fetchone()[0]
    print(f"\nDeleted {deleted} rows for {len(sym_ids)} stale symbols")
else:
    print("\nNo stale symbols found.")

conn.close()
