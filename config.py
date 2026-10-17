import os
from dotenv import load_dotenv

load_dotenv()

WS_URL = os.getenv("WS_URL", "wss://pumpportal.fun/api/data")
LOG_FILE = os.getenv("LOG_FILE", "trades.csv")

# Веб-панель доступна только с этого компьютера
WEB_HOST = "127.0.0.1"
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))
OPEN_BROWSER = os.getenv("OPEN_BROWSER", "true").lower() == "true"

# Стартовые значения (можно задать в .env), дальше меняются из панели
DEFAULTS = {
    "min_dev_buy_sol": float(os.getenv("MIN_DEV_BUY_SOL", "0.5")),
    "max_dev_buy_sol": float(os.getenv("MAX_DEV_BUY_SOL", "5")),
    "buy_sol": float(os.getenv("BUY_SOL", "0.1")),
    "take_profit_pct": float(os.getenv("TAKE_PROFIT_PCT", "50")),
    "stop_loss_pct": float(os.getenv("STOP_LOSS_PCT", "-25")),
    "max_hold_seconds": int(os.getenv("MAX_HOLD_SECONDS", "120")),
    "fee_pct": float(os.getenv("FEE_PCT", "1.0")),
    "max_open_positions": int(os.getenv("MAX_OPEN_POSITIONS", "5")),
}

# Описание полей: из него панель сама строит форму настроек
SPEC = [
    {"key": "min_dev_buy_sol", "label": "Мин. покупка создателя, SOL",
     "hint": "Входим, только если создатель сам купил не меньше", "type": "float", "min": 0, "max": 100, "step": 0.1},
    {"key": "max_dev_buy_sol", "label": "Макс. покупка создателя, SOL",
     "hint": "Слишком большая покупка создателя выглядит подозрительно", "type": "float", "min": 0, "max": 100, "step": 0.1},
    {"key": "buy_sol", "label": "Размер сделки, SOL",
     "hint": "Виртуальная сумма на одну сделку", "type": "float", "min": 0.001, "max": 100, "step": 0.01},
    {"key": "take_profit_pct", "label": "Тейк-профит, %",
     "hint": "Продать при росте market cap на столько процентов", "type": "float", "min": 1, "max": 10000, "step": 1},
    {"key": "stop_loss_pct", "label": "Стоп-лосс, %",
     "hint": "Отрицательное число, например -25", "type": "float", "min": -100, "max": -1, "step": 1},
    {"key": "max_hold_seconds", "label": "Макс. время в позиции, сек",
     "hint": "Если за это время не сработал TP или SL, закрываем по таймеру", "type": "int", "min": 5, "max": 86400, "step": 5},
    {"key": "fee_pct", "label": "Комиссия, % за сторону",
     "hint": "Учитывается дважды: на покупку и на продажу", "type": "float", "min": 0, "max": 10, "step": 0.1},
    {"key": "max_open_positions", "label": "Макс. открытых позиций",
     "hint": "Новые токены пропускаются, пока лимит занят", "type": "int", "min": 1, "max": 100, "step": 1},
]

settings = dict(DEFAULTS)


def update_settings(data: dict) -> list[str]:
    """Проверяет и применяет настройки. Возвращает список ошибок (пустой = всё ок)."""
    errors = []
    new = dict(settings)
    for s in SPEC:
        k = s["key"]
        if k not in data:
            continue
        v = data[k]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            errors.append(f"{s['label']}: нужно число")
            continue
        v = int(v) if s["type"] == "int" else float(v)
        if not (s["min"] <= v <= s["max"]):
            errors.append(f"{s['label']}: допустимо от {s['min']} до {s['max']}")
            continue
        new[k] = v
    if new["min_dev_buy_sol"] > new["max_dev_buy_sol"]:
        errors.append("Мин. покупка создателя не может быть больше максимальной")
    if not errors:
        settings.update(new)
    return errors


def reset_settings():
    settings.clear()
    settings.update(DEFAULTS)
