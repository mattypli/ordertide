# reads environment from .env file and OS environment variables, provides a dictionary
import os 
from dotenv import load_dotenv 

load_dotenv() # search for .env file and load environment variables 

WS_URL = os.getenv("WS_URL", "wss://api.hyperliquid.xyz/ws") # variable name, default value if not set in .env 
DB_DSN = os.getenv("DB_DSN") # no default because data is private 

coins_env = os.getenv("COINS", "BTC,ETH") # default value is a comma separated list of coins but in text form 
COINS = coins_env.split(",") # split text by each comma and create a list of coins 

