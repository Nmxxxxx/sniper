import os
from dotenv import load_dotenv

load_dotenv()

WS_URL = os.getenv("WS_URL", "wss://pumpportal.fun/api/data")
# Бесплатный источник данных: WebSocket-адрес Solana RPC (например, от Helius). Если задан,
# бот читает сделки pump.fun прямо из блокчейна, а PumpPortal не используется.
# Адрес содержит ваш ключ: не публикуйте его и не коммитьте .env.
SOLANA_WS_URL = os.getenv("SOLANA_WS_URL", "").strip()

# Нужен для подписки на сделки по токенам (только если не используется SOLANA_WS_URL) (без него бот видит только создание токенов).
# Ключ никогда не показывается в панели и не должен попадать в git.
PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY", "").strip()


# ИИ-оценка токенов через API DeepSeek (платный). Ключ никогда не показывается в панели и не должен попадать в git.
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-flash")
DEEPSEEK_API_URL = os.getenv("DEEPSEEK_API_URL", "https://api.deepseek.com/chat/completions")
IPFS_GATEWAY = os.getenv("IPFS_GATEWAY", "https://ipfs.io/ipfs/")
# Цена за миллион токенов, $: пиковая по прайсу DeepSeek на октябрь 2026 (в остальные часы вдвое дешевле),
# чтобы приблизительная оценка расходов в панели не занижала их
DEEPSEEK_PRICE_IN = float(os.getenv("DEEPSEEK_PRICE_IN", "0.30"))
DEEPSEEK_PRICE_OUT = float(os.getenv("DEEPSEEK_PRICE_OUT", "1.20"))


def ws_url() -> str:
    return f"{WS_URL}?api-key={PUMPPORTAL_API_KEY}" if PUMPPORTAL_API_KEY else WS_URL
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
    # Окно наблюдения и обучение
    "entry_delay_s": int(os.getenv("ENTRY_DELAY_S", "5")),
    "learning_enabled": os.getenv("LEARNING_ENABLED", "true").lower() == "true",
    "learning_min_trades": int(os.getenv("LEARNING_MIN_TRADES", "30")),
    "min_expected_pct": float(os.getenv("MIN_EXPECTED_PCT", "0")),
    "explore_pct": float(os.getenv("EXPLORE_PCT", "15")),
    "blacklist_losses": int(os.getenv("BLACKLIST_LOSSES", "2")),
    # ИИ-оценка токенов
    "ai_enabled": os.getenv("AI_ENABLED", "true").lower() == "true",
    "ai_timeout_s": float(os.getenv("AI_TIMEOUT_S", "4")),
    "ai_max_calls_per_hour": int(os.getenv("AI_MAX_CALLS_PER_HOUR", "300")),
    "ai_block_risk": int(os.getenv("AI_BLOCK_RISK", "0")),
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
    {"key": "entry_delay_s", "label": "Окно наблюдения перед входом, сек",
     "hint": "Бот смотрит на токен это время и входит уже по новой цене. Так симуляция ближе к реальности", "type": "int", "min": 1, "max": 60, "step": 1},
    {"key": "learning_enabled", "label": "Обучение включено",
     "hint": "Модель фильтрует входы по опыту прошлых сделок", "type": "bool"},
    {"key": "learning_min_trades", "label": "Сделок для старта модели",
     "hint": "Пока сделок меньше, бот берёт всё подряд и собирает данные. Чем больше, тем надёжнее", "type": "int", "min": 10, "max": 5000, "step": 10},
    {"key": "min_expected_pct", "label": "Порог ожидаемого результата, %",
     "hint": "Модель входит, только если ждёт не меньше этого значения (после комиссий)", "type": "float", "min": -50, "max": 50, "step": 0.5},
    {"key": "explore_pct", "label": "Пробные входы, %",
     "hint": "Доля сделок, которые берутся вопреки модели. Нужны, чтобы она не застряла и могла проверять себя", "type": "float", "min": 0, "max": 100, "step": 1},
    {"key": "blacklist_losses", "label": "Чёрный список создателей: убытков",
     "hint": "Создателя блокируем после стольких убыточных сделок по его токенам", "type": "int", "min": 1, "max": 20, "step": 1},
    {"key": "ai_enabled", "label": "ИИ-оценка токенов включена",
     "hint": "Работает, только если в .env указан DEEPSEEK_API_KEY. Каждый вызов платный (доли цента)", "type": "bool"},
    {"key": "ai_timeout_s", "label": "Время ожидания ИИ, сек",
     "hint": "Если ответ не пришёл за это время (и до конца окна наблюдения), токен оценивается без ИИ", "type": "float", "min": 1, "max": 10, "step": 0.5},
    {"key": "ai_max_calls_per_hour", "label": "Лимит вызовов ИИ в час",
     "hint": "Защита от неожиданных расходов: при превышении ИИ временно не вызывается", "type": "int", "min": 10, "max": 5000, "step": 10},
    {"key": "ai_block_risk", "label": "Блокировать токены с риском ИИ от (0 = не блокировать)",
     "hint": "Жёсткий фильтр по оценке ИИ. Пока не убедитесь на вкладке «Обучение», что оценка полезна, лучше оставить 0", "type": "int", "min": 0, "max": 10, "step": 1},
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
        if s["type"] == "bool":
            if not isinstance(v, bool):
                errors.append(f"{s['label']}: нужно значение да/нет")
                continue
            new[k] = v
            continue
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
