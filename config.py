import os
from dotenv import load_dotenv

load_dotenv()

WS_URL = os.getenv("WS_URL", "wss://pumpportal.fun/api/data")

# --- Фильтры входа ---
MIN_DEV_BUY_SOL = float(os.getenv("MIN_DEV_BUY_SOL", "0.5"))   # минимум, сколько создатель купил сам
MAX_DEV_BUY_SOL = float(os.getenv("MAX_DEV_BUY_SOL", "5"))     # слишком большая покупка создателя = подозрительно
REQUIRE_SOCIALS = os.getenv("REQUIRE_SOCIALS", "false").lower() == "true"

# --- Параметры сделки (paper) ---
BUY_SOL = float(os.getenv("BUY_SOL", "0.1"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "50"))    # +50%
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "-25"))       # -25%
MAX_HOLD_SECONDS = int(os.getenv("MAX_HOLD_SECONDS", "120"))   # выход по таймеру
FEE_PCT = float(os.getenv("FEE_PCT", "1.0"))                   # комиссия на каждую сторону, %
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))

LOG_FILE = os.getenv("LOG_FILE", "trades.csv")
