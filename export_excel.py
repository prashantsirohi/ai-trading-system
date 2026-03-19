import sqlite3
import pandas as pd

conn = sqlite3.connect('ai-trading-system/data/masterdata.db')
df = pd.read_sql('SELECT symbol_id, symbol_name, exchange, sector, industry FROM symbols ORDER BY sector, symbol_id', conn)
conn.close()

output = 'ai-trading-system/data/masterdata.xlsx'
df.to_excel(output, index=False, engine='openpyxl')
print(f'Exported {len(df)} symbols to {output}')
