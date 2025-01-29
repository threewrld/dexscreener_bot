import yaml
import requests
import pandas as pd
import time
from datetime import datetime
from typing import Dict, List, Optional
from web3 import Web3
import sqlalchemy as db

class DexTradingBot:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = self._load_config(config_path)
        self.base_url = "https://api.dexscreener.com/latest/dex"
        self.web3 = Web3(Web3.HTTPProvider(self.config['web3_provider']))
        self._init_db()
        self._init_telegram()

    def _load_config(self, path: str) -> Dict:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _init_db(self):
        """Initialize database connection"""
        self.engine = db.create_engine(self.config['database']['url'])
        self._create_tables()

    def _create_tables(self):
        """Create necessary database tables"""
        with self.engine.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    pair_address TEXT,
                    action TEXT,
                    amount NUMERIC,
                    price NUMERIC
                );
            """)

    def _init_telegram(self):
        """Initialize Telegram components"""
        self.bot_token = self.config['telegram']['bot_token']
        self.chat_id = self.config['telegram']['chat_id']
        self.trade_chat_id = self.config['telegram'].get('trade_chat_id', self.chat_id)

    def send_telegram_msg(self, text: str, is_trade: bool = False):
        """Send message to Telegram"""
        chat_id = self.trade_chat_id if is_trade else self.chat_id
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
        except Exception as e:
            print(f"Telegram error: {e}")

    def execute_trade(self, action: str, pair_data: Dict, amount: float):
        """Execute trade through Telegram commands"""
        symbol = pair_data['baseToken']['symbol']
        price = float(pair_data['priceUsd'])
        
        # Format trade command for BonkBot
        trade_command = f"/{action} {symbol} {amount}"
        
        # Send trade command and store in database
        self.send_telegram_msg(trade_command, is_trade=True)
        self._log_trade(pair_data, action, amount, price)

    def _log_trade(self, pair_data: Dict, action: str, amount: float, price: float):
        """Record trade in database"""
        with self.engine.connect() as conn:
            conn.execute(
                db.text("""
                    INSERT INTO trades (pair_address, action, amount, price)
                    VALUES (:address, :action, :amount, :price)
                """),
                {
                    'address': pair_data['pairAddress'],
                    'action': action,
                    'amount': amount,
                    'price': price
                }
            )

    def analyze_and_trade(self):
        """Main trading analysis loop"""
        self.send_telegram_msg("🚀 Trading bot started")
        
        while True:
            try:
                pairs = self._fetch_pairs()
                for pair in pairs:
                    if self._is_valid_trade(pair):
                        self.execute_trade('buy', pair, 0.1)  # Example 0.1 ETH trade
                        self.send_telegram_msg(
                            f"✅ Bought {pair['baseToken']['symbol']} at ${pair['priceUsd']}"
                        )
                time.sleep(300)  # 5 minute interval
                
            except Exception as e:
                self.send_telegram_msg(f"❌ Error: {str(e)}")
                time.sleep(60)

    def _fetch_pairs(self) -> List[Dict]:
        """Fetch pairs from DexScreener"""
        try:
            response = requests.get(f"{self.base_url}/pairs/ethereum", timeout=10)
            return response.json().get('pairs', [])
        except Exception as e:
            print(f"API error: {e}")
            return []

    def _is_valid_trade(self, pair: Dict) -> bool:
        """Implement your trading strategy here"""
        liquidity = float(pair.get('liquidity', 0))
        volume = float(pair.get('volume', {}).get('h24', 0))
        return liquidity > 100000 and volume > 500000

if __name__ == "__main__":
    bot = DexTradingBot()
    bot.analyze_and_trade()
