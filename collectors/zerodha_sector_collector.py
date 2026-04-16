import os
import sqlite3
import requests
import re
from datetime import datetime
from typing import Dict, List
from core.logging import logger


class ZerodhaSectorCollector:
    """
    Zerodha Sector Data Collector.
    Scrapes sector-wise stock data from Zerodha Markets.
    
    Flow:
    1. Visit https://zerodha.com/markets/sector/ - get all sector links
    2. Visit each sector page - extract stocks with market cap
    3. Update symbols table in SQLite
    """

    def __init__(self, db_path: str = "ai-trading-system/data/masterdata.db"):
        self.db_path = db_path
        self.base_url = "https://zerodha.com"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.sector_stocks: List[Dict] = []

    def fetch_page(self, url: str) -> str:
        """Fetch HTML page content"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""

    def get_sector_list(self) -> List[Dict[str, str]]:
        """Get list of all sectors"""
        logger.info("Fetching sector list...")
        
        html = self.fetch_page(f"{self.base_url}/markets/sector/")
        if not html:
            return []
        
        links = re.findall(r'href="(/markets/sector/([^"]+))', html)
        
        sectors = []
        seen = set()
        for href, slug in links:
            slug = slug.strip('/')
            if slug and slug not in seen and slug != "sector":
                seen.add(slug)
                sector_name = slug.replace("-", " ").title()
                sectors.append({
                    "sector_slug": slug,
                    "sector_name": sector_name,
                    "url": f"{self.base_url}/markets/sector/{slug}/"
                })
        
        logger.info(f"Found {len(sectors)} sectors")
        return sectors

    def get_sector_stocks(self, sector_info: Dict) -> List[Dict]:
        """Get stocks for a specific sector"""
        stocks = []
        
        html = self.fetch_page(sector_info["url"])
        if not html:
            return stocks
        
        rows = re.findall(r'<a[^>]*href="/markets/stocks/NSE/([^"]+)/"[^>]*>.*?<div[^>]*class="table_row"[^>]*>(.*?)</div>\s*</a>', html, re.DOTALL)
        
        for row in rows:
            symbol = row[0].strip()
            row_html = row[1]
            
            name_match = re.search(r'<div[^>]*class="left"[^>]*>.*?<div[^>]*>([^<]+)</div>', row_html)
            name = name_match.group(1).strip() if name_match else ""
            
            mcap_match = re.search(r'<div[^>]*class="market_cap"[^>]*>\s*([^<]+?)\s*</div>', row_html)
            mcap = mcap_match.group(1).strip() if mcap_match else ""
            
            if symbol:
                stocks.append({
                    "sector": sector_info["sector_name"],
                    "symbol": symbol,
                    "name": name,
                    "market_cap": mcap
                })
        
        logger.info(f"Found {len(stocks)} stocks in {sector_info['sector_name']}")
        return stocks

    def scrape_all_sectors(self) -> List[Dict]:
        """Scrape all sectors"""
        all_stocks = []
        
        sectors = self.get_sector_list()
        
        for i, sector in enumerate(sectors):
            logger.info(f"Processing {i+1}/{len(sectors)}: {sector['sector_name']}")
            
            stocks = self.get_sector_stocks(sector)
            all_stocks.extend(stocks)
        
        logger.info(f"Total stocks scraped: {len(all_stocks)}")
        return all_stocks

    def update_database(self, stocks: List[Dict]):
        """Update symbols table with sector and market cap"""
        if not stocks:
            logger.warning("No stocks to update")
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        timestamp = datetime.now().isoformat()
        
        updated = 0
        not_found = []
        
        for stock in stocks:
            symbol = stock["symbol"].upper()
            sector = stock["sector"]
            mcap = stock.get("market_cap", "")
            
            # Try exact match first
            cursor.execute("""
                UPDATE symbols 
                SET sector = ?, 
                    industry = ?,
                    last_updated = ?
                WHERE symbol_id = ?
            """, (sector, mcap, timestamp, symbol))
            
            if cursor.rowcount == 0:
                # Try case-insensitive match
                cursor.execute("""
                    UPDATE symbols 
                    SET sector = ?, 
                        industry = ?,
                        last_updated = ?
                    WHERE UPPER(symbol_id) = ?
                """, (sector, mcap, timestamp, symbol))
                
                if cursor.rowcount == 0:
                    # Try partial match (for symbols like JKPAPER vs JK Paper)
                    cursor.execute("""
                        UPDATE symbols 
                        SET sector = ?, 
                            industry = ?,
                            last_updated = ?
                        WHERE symbol_id LIKE ? OR symbol_id LIKE ?
                    """, (sector, mcap, timestamp, f"%{symbol}%", f"%{symbol.lower()}%"))
                
            if cursor.rowcount > 0:
                updated += 1
            else:
                not_found.append(symbol)
            
            if cursor.rowcount > 0:
                updated += 1
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM symbols WHERE sector != '' AND sector IS NOT NULL")
        total_sectored = cursor.fetchone()[0]
        
        conn.close()
        
        logger.info(f"Updated {updated} symbols in database ({total_sectored} total with sectors)")

    def get_sector_summary(self) -> Dict:
        """Get summary of stocks by sector"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT sector, COUNT(*) as count 
            FROM symbols 
            WHERE sector IS NOT NULL AND sector != ''
            GROUP BY sector 
            ORDER BY count DESC
            LIMIT 30
        """)
        
        summary = {}
        for row in cursor.fetchall():
            summary[row[0]] = row[1]
        
        conn.close()
        return summary

    def run(self):
        """Run the complete scraping pipeline"""
        logger.info("Starting Zerodha sector scraper...")
        
        stocks = self.scrape_all_sectors()
        
        if stocks:
            self.update_database(stocks)
            
            summary = self.get_sector_summary()
            logger.info(f"Sector summary: {summary}")
        
        logger.info("Zerodha sector scraper complete")


def main():
    """Main entry point"""
    collector = ZerodhaSectorCollector()
    collector.run()


if __name__ == "__main__":
    main()
