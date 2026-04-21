#!/usr/bin/env python3
"""Audit ranking output with actual calculated parameters."""

import duckdb
import pandas as pd
import os

conn = duckdb.connect('data/control_plane.duckdb', read_only=True)
latest = conn.execute('''
    SELECT pa.uri, pr.run_date, pr.run_id
    FROM pipeline_artifact pa
    JOIN pipeline_run pr ON pa.run_id = pr.run_id
    WHERE pa.stage_name = 'rank' AND pa.artifact_type = 'ranked_signals'
    ORDER BY pr.ended_at DESC
    LIMIT 1
''').fetchone()

if latest:
    print('=== RANK ARTIFACT AUDIT ===')
    print(f'Run: {latest[2]}')
    print(f'Date: {latest[1]}')
    print(f'Path: {latest[0]}')
    
    df = pd.read_csv(latest[0])
    print(f'\nTotal rows: {len(df)}')
    print(f'Columns: {list(df.columns)}')
    
    print('\n=== COMPOSITE SCORE STATS ===')
    print(f'  Min: {df.composite_score.min():.2f}')
    print(f'  Max: {df.composite_score.max():.2f}')
    print(f'  Mean: {df.composite_score.mean():.2f}')
    print(f'  Median: {df.composite_score.median():.2f}')
    print(f'  Std: {df.composite_score.std():.2f}')
    
    print('\n=== FACTOR SCORES (Top 10) ===')
    cols = [c for c in ['symbol_id', 'composite_score', 'eligible_rank', 'rel_strength_score', 'vol_intensity_score', 'trend_score_score', 'prox_high_score', 'delivery_pct_score'] if c in df.columns]
    print(df[cols].head(10).to_string(index=False))
    
    print('\n=== MULTI-PERIOD RETURNS (Top 10) ===')
    ret_cols = [c for c in ['symbol_id', 'return_20', 'return_60', 'return_120', 'rel_strength'] if c in df.columns]
    if ret_cols:
        print(df[ret_cols].head(10).to_string(index=False))
    
    if 'sector_name' in df.columns:
        print('\n=== SECTOR DISTRIBUTION ===')
        sector_counts = df['sector_name'].value_counts().head(10)
        for sector, count in sector_counts.items():
            pct = count / len(df) * 100
            print(f'  {sector}: {count} ({pct:.1f}%)')
    
    print('\n=== SCORE DISTRIBUTION ===')
    bins = [0, 20, 40, 60, 80, 100]
    labels = ['0-20', '20-40', '40-60', '60-80', '80-100']
    df['score_bin'] = pd.cut(df['composite_score'], bins=bins, labels=labels)
    print(df['score_bin'].value_counts().sort_index())
    
    summary_path = latest[0].replace('ranked_signals.csv', 'rank_summary.json')
    if os.path.exists(summary_path):
        print('\n=== RANK SUMMARY ===')
        with open(summary_path) as f:
            summary = f.read()
            print(summary[:2000])
    else:
        print('\n=== RANK SUMMARY ===')
        print('No summary found')
else:
    print('No rank artifacts found')

conn.close()