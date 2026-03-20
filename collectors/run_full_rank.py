"""
Full screener run: rank all 1,306 stocks with real data.
"""

import os, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

from analytics.ranker import StockRanker

DB = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "ohlcv.duckdb")
FEAT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "feature_store")

ranker = StockRanker(ohlcv_db_path=DB, feature_store_dir=FEAT)

print("Ranking all NSE stocks...")
t0 = time.time()
result = ranker.rank_all(
    date="2026-03-18",
    exchanges=["NSE"],
    top_n=None,
)
elapsed = time.time() - t0

if result is not None and not result.empty:
    print(f"SUCCESS: {len(result)} stocks ranked in {elapsed:.1f}s")
    print(
        f"\nTop 20:\n{result[['symbol_id', 'close', 'composite_score', 'rel_strength_score', 'trend_score_score', 'prox_high_score']].head(20).to_string()}"
    )
    print(
        f"\nBottom 5:\n{result[['symbol_id', 'close', 'composite_score']].tail().to_string()}"
    )
    result.to_csv("rankings_latest.csv", index=False)
    print(f"\nSaved to rankings_latest.csv")
else:
    print(f"FAILED")
