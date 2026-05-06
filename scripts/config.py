import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Config variables
DATABASE_URL = os.getenv("DATABASE_URL")
WATCHLIST = os.getenv("WATCHLIST", "").split(",")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
