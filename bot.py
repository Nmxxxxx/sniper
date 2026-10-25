"""
Paper-trading снайпер для pump.fun с веб-панелью и обучением на своих сделках.

Как идёт токен:
  1. Появляется новый токен, проходит базовые фильтры -> становится кандидатом.
  2. Несколько секунд (окно наблюдения) бот смотрит, кто и как торгует токеном.
  3. Модель по признакам оценивает ожидаемый результат и решает, входить ли.
  4. Позиция закрывается по TP / SL / таймеру, результат пополняет базу для обучения.

Реальных сделок нет. Управление и статистика: http://127.0.0.1:8000
"""
import asyncio
import csv
import json
import os
import random
import re
import time
import webbrowser
from collections import Counter, deque
from dataclasses import dataclass, field

import websockets
from aiohttp import web

import config
from config import settings
from ai_eval import AIEvaluator
from learning import LEARN_REASONS, Learner
from solana_feed import SolanaFeed
from server import create_app

CSV_FIELDS = ["time", "mint", "symbol", "entry_mcap", "exit_mcap",
              "hold_s", "reason", "pnl_pct", "pnl_sol", "features"]

REASON_NAMES = {
    "take_profit": "тейк-профит",
    "stop_loss": "стоп-лосс",
    "timeout": "таймер",
    "manual_stop": "остановка бота",
    "disconnect": "обрыв связи",
}


def _num(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


class NullWS:
    """Заглушка: из блокчейна приходят сделки по всем токенам сразу, отдельные подписки не нужны."""
    async def send(self, data):
        pass


def _redact(text: str) -> str:
    """Убирает из текста ошибок адрес RPC и ключи, чтобы они не попали в лог панели."""
    for secret in (config.SOLANA_WS_URL, config.PUMPPORTAL_API_KEY, config.DEEPSEEK_API_KEY):
        if secret:
            text = text.replace(secret, "***")
    return re.sub(r"(api[-_]?key=)[^&\s]+", r"\1***", text, flags=re.I)


@dataclass
class Candidate:
    """Токен в окне наблюдения: ещё не купили, собираем признаки."""
    mint: str
    symbol: str
    creator: str
    dev_buy: float
    create_mcap: float
    created_at: float
    last_mcap: float
    buyers: set = field(default_factory=set)
    buys: int = 0
    sells: int = 0
    volume: float = 0.0
    creator_sold: bool = False
    name: str = ""
    uri: str = ""
    ai_task: object = None          # фоновый запрос к ИИ, идёт параллельно с окном наблюдения


@dataclass
class Position:
    mint: str
    symbol: str
    entry_mcap: float      # market cap (в SOL) в момент входа
    last_mcap: float
    opened_at: float
    buy_sol: float
    features: dict


class Engine:
    def __init__(self):
        self.status = "stopped"          # stopped / connecting / running
        self.candidates: dict[str, Candidate] = {}
        self.positions: dict[str, Position] = {}
        self.trades: list[dict] = []
        self.events = deque(maxlen=200)
        self.learner = Learner()
        self.creator_losses = Counter()
        self.entry_tags = Counter()      # model / explore / warmup / plain
        self.seen = 0                    # новых токенов увидели
        self.entered = 0                 # в скольких виртуально купили
        self.skipped_model = 0           # отклонено моделью
        self.skipped_blacklist = 0       # отклонено из-за чёрного списка создателей
        self.started_at = None
        self._task = None
        self.msg_counts = Counter()      # сколько сообщений каких типов пришло
        self.server_msgs = set()         # уникальные служебные сообщения сервера (ошибки, подтверждения)
        self.ai = AIEvaluator()
        self.skipped_ai = 0              # отклонено жёстким фильтром по риску ИИ
        self.feed = SolanaFeed()         # разбор событий pump.fun из блокчейна (если задан SOLANA_WS_URL)
        self._connected_at = None
        self.needs_api_key = False       # сервер ответил, что для сделок нужен API-ключ
        self.subs_sent = 0               # подписок на сделки по токенам отправлено (в текущем соединении)
        self.tracked_trades = 0          # сделок по отслеживаемым токенам получено (в текущем соединении)
        self._load_history()

    # ---------- служебное ----------
    def event(self, level: str, msg: str):
        self.events.append({"t": time.strftime("%H:%M:%S"), "level": level, "msg": msg})

    def _drop_candidates(self):
        for c in self.candidates.values():
            if c.ai_task and not c.ai_task.done():
                c.ai_task.cancel()
        self.candidates.clear()

    def _learn(self, t: dict):
        f = t.get("features") or {}
        if not f or t["reason"] not in LEARN_REASONS:
            return
        self.learner.add(f, t["pnl_pct"])
        creator = f.get("creator")
        if creator and t["pnl_pct"] < 0:
            self.creator_losses[creator] += 1

    def _load_history(self):
        """Подхватываем прошлые сделки из CSV: статистика и обучение переживают перезапуск."""
        if not os.path.exists(config.LOG_FILE):
            return
        old_format = False
        try:
            with open(config.LOG_FILE, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                old_format = "features" not in (reader.fieldnames or [])
                for row in reader:
                    try:
                        feats = {}
                        if row.get("features"):
                            try:
                                feats = json.loads(row["features"])
                            except json.JSONDecodeError:
                                feats = {}
                        t = {
                            "time": row["time"], "mint": row["mint"], "symbol": row["symbol"],
                            "entry_mcap": float(row["entry_mcap"]), "exit_mcap": float(row["exit_mcap"]),
                            "hold_s": float(row["hold_s"]), "reason": row["reason"],
                            "pnl_pct": float(row["pnl_pct"]), "pnl_sol": float(row["pnl_sol"]),
                            "features": feats,
                        }
                    except (KeyError, ValueError, TypeError):
                        continue
                    self.trades.append(t)
                    self._learn(t)
        except OSError:
            return
        if old_format:           # файл от прошлой версии без колонки features: переписываем с новой шапкой
            self._rewrite_csv()

    def _csv_row(self, t: dict) -> dict:
        row = dict(t)
        row["features"] = json.dumps(t.get("features") or {}, ensure_ascii=False)
        return row

    def _rewrite_csv(self):
        with open(config.LOG_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()
            for t in self.trades:
                w.writerow(self._csv_row(t))

    def _append_csv(self, t: dict):
        new_file = not os.path.exists(config.LOG_FILE)
        with open(config.LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if new_file:
                w.writeheader()
            w.writerow(self._csv_row(t))

    # ---------- управление ----------
    def start(self):
        if self._task and not self._task.done():
            return
        self.started_at = time.time()
        self.status = "connecting"
        self._task = asyncio.create_task(self._run())
        self.event("info", "Бот запущен")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._drop_candidates()
        for mint in list(self.positions):
            self._finalize(mint, "manual_stop")
        was_running = self.status != "stopped"
        self.status = "stopped"
        self.started_at = None
        if was_running:
            self.event("info", "Бот остановлен")

    def reset_stats(self):
        """Обнуляет статистику и обучение. Старый CSV не удаляем, а переименовываем в .bak."""
        if os.path.exists(config.LOG_FILE):
            os.replace(config.LOG_FILE, f"{config.LOG_FILE}.{time.strftime('%Y%m%d_%H%M%S')}.bak")
        self.trades.clear()
        self.learner.reset()
        self.creator_losses.clear()
        self.entry_tags.clear()
        self.seen = self.entered = self.skipped_model = self.skipped_blacklist = self.skipped_ai = 0
        self.event("info", "Статистика и обучение сброшены (старый CSV сохранён как .bak)")

    # ---------- торговая логика ----------
    def _passes_filters(self, msg: dict) -> bool:
        dev_buy = _num(msg.get("solAmount"))
        if not (settings["min_dev_buy_sol"] <= dev_buy <= settings["max_dev_buy_sol"]):
            return False
        if len(self.positions) + len(self.candidates) >= settings["max_open_positions"]:
            return False
        return True

    def _finalize(self, mint: str, reason: str):
        pos = self.positions.pop(mint, None)
        if not pos:
            return
        gross = (pos.last_mcap / pos.entry_mcap - 1) * 100
        pnl_pct = gross - 2 * settings["fee_pct"]          # комиссия на вход и выход
        pnl_sol = pos.buy_sol * pnl_pct / 100
        trade = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"), "mint": pos.mint, "symbol": pos.symbol,
            "entry_mcap": round(pos.entry_mcap, 2), "exit_mcap": round(pos.last_mcap, 2),
            "hold_s": round(time.time() - pos.opened_at, 1), "reason": reason,
            "pnl_pct": round(pnl_pct, 2), "pnl_sol": round(pnl_sol, 5),
            "features": pos.features,
        }
        self.trades.append(trade)
        self._append_csv(trade)
        self._learn(trade)
        self.event("win" if pnl_sol > 0 else "loss",
                   f"SELL {pos.symbol}: {pnl_pct:+.1f}% ({REASON_NAMES.get(reason, reason)})")

    async def _unsubscribe(self, ws, mint: str):
        try:
            await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))
        except Exception:
            pass

    async def _close(self, ws, mint: str, reason: str):
        self._finalize(mint, reason)
        await self._unsubscribe(ws, mint)

    async def _evaluate(self, ws, cand: Candidate):
        """Окно наблюдения закончилось: решаем, входить ли."""
        self.candidates.pop(cand.mint, None)
        f = {
            "creator": cand.creator,
            "dev_buy_sol": round(cand.dev_buy, 3),
            "unique_buyers": len(cand.buyers),
            "buys": cand.buys,
            "sells": cand.sells,
            "creator_sold": cand.creator_sold,
            "volume_sol": round(cand.volume, 3),
            "mcap_change_pct": round((cand.last_mcap / cand.create_mcap - 1) * 100, 1),
        }

        ai = None
        if cand.ai_task:
            if cand.ai_task.done() and not cand.ai_task.cancelled():
                ai = cand.ai_task.result()
            else:                                    # не успел к концу окна наблюдения: входим без оценки ИИ
                cand.ai_task.cancel()
                self.ai.stats["too_slow"] += 1
        if ai:
            f["ai_risk"], f["ai_hype"], f["ai_reason"] = ai["risk"], ai["hype"], ai["reason"]
            block = settings["ai_block_risk"]
            if block > 0 and ai["risk"] >= block:
                self.skipped_ai += 1
                self.event("info", f"SKIP {cand.symbol}: риск ИИ {ai['risk']}. {ai['reason']}")
                await self._unsubscribe(ws, cand.mint)
                return

        tag = "plain"
        if settings["learning_enabled"]:
            if self.learner.n >= settings["learning_min_trades"]:
                expected = self.learner.score(f)
                f["expected_pct"] = round(expected, 2)
                if expected >= settings["min_expected_pct"]:
                    tag = "model"
                elif random.random() * 100 < settings["explore_pct"]:
                    tag = "explore"
                else:
                    self.skipped_model += 1
                    await self._unsubscribe(ws, cand.mint)
                    return
            else:
                tag = "warmup"
        f["tag"] = tag

        if len(self.positions) >= settings["max_open_positions"] or cand.last_mcap <= 0:
            await self._unsubscribe(ws, cand.mint)
            return

        self.positions[cand.mint] = Position(cand.mint, cand.symbol, cand.last_mcap, cand.last_mcap,
                                             time.time(), settings["buy_sol"], f)
        self.entered += 1
        self.entry_tags[tag] += 1
        ai_txt = f", ИИ: риск {ai['risk']} хайп {ai['hype']}" if ai else ""
        self.event("buy", f"BUY {cand.symbol} [{tag}]: mcap {cand.last_mcap:.1f} SOL, "
                          f"покупателей {len(cand.buyers)}, продаж {cand.sells}{ai_txt}")

    def _note_server_message(self, msg: dict):
        """Служебные сообщения без txType/mint (подтверждения, ошибки, лимиты) пишем в лог, без дублей."""
        text = json.dumps(msg, ensure_ascii=False)[:200]
        if "api key" in text.lower():
            self.needs_api_key = True
        if text not in self.server_msgs and len(self.server_msgs) < 15:
            self.server_msgs.add(text)
            self.event("warn", f"Сообщение сервера: {text}")

    async def handle_message(self, ws, msg: dict):
        tx = msg.get("txType")
        self.msg_counts[tx or "other"] += 1
        mint = msg.get("mint")
        if not tx or not mint:
            self._note_server_message(msg)
            return

        if tx == "create":
            self.seen += 1
            if not self._passes_filters(msg):
                return
            mcap = _num(msg.get("marketCapSol"))
            if mcap <= 0:
                return
            creator = str(msg.get("traderPublicKey") or "")
            if creator and self.creator_losses[creator] >= settings["blacklist_losses"]:
                self.skipped_blacklist += 1
                return
            cand = Candidate(
                mint=mint, symbol=str(msg.get("symbol") or "?")[:20], creator=creator,
                dev_buy=_num(msg.get("solAmount")), create_mcap=mcap,
                created_at=time.time(), last_mcap=mcap,
                name=str(msg.get("name") or "")[:60], uri=str(msg.get("uri") or ""))
            self.candidates[mint] = cand
            if self.ai.enabled:       # ИИ думает параллельно с окном наблюдения и не задерживает вход
                limit = max(0.7, min(settings["ai_timeout_s"], settings["entry_delay_s"] - 0.3))
                cand.ai_task = asyncio.create_task(self.ai.evaluate(
                    {"name": cand.name, "symbol": cand.symbol, "uri": cand.uri, "dev_buy": cand.dev_buy}, limit))
            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
            self.subs_sent += 1
            return

        if tx not in ("buy", "sell"):
            return
        mcap = _num(msg.get("marketCapSol"))

        cand = self.candidates.get(mint)
        if cand:                                   # собираем признаки в окне наблюдения
            self.tracked_trades += 1
            if mcap > 0:
                cand.last_mcap = mcap
            trader = msg.get("traderPublicKey")
            if trader == cand.creator:
                if tx == "sell":
                    cand.creator_sold = True
            else:
                cand.volume += _num(msg.get("solAmount"))
                if tx == "buy":
                    cand.buys += 1
                    cand.buyers.add(trader)
                else:
                    cand.sells += 1
            return

        pos = self.positions.get(mint)
        if pos:
            self.tracked_trades += 1
        if pos and mcap > 0:
            pos.last_mcap = mcap
            change = (mcap / pos.entry_mcap - 1) * 100
            if change >= settings["take_profit_pct"]:
                await self._close(ws, mint, "take_profit")
            elif change <= settings["stop_loss_pct"]:
                await self._close(ws, mint, "stop_loss")

    async def _tick(self, ws):
        now = time.time()
        for cand in list(self.candidates.values()):
            if now - cand.created_at >= settings["entry_delay_s"]:
                await self._evaluate(ws, cand)
        for mint, pos in list(self.positions.items()):
            if now - pos.opened_at >= settings["max_hold_seconds"]:
                await self._close(ws, mint, "timeout")

    async def _watcher(self, ws):
        while True:
            await asyncio.sleep(0.5)
            await self._tick(ws)

    async def _stream(self, engine_ws, messages):
        """Общий цикл: читаем сообщения из любого источника и отдаём их торговой логике."""
        self.status = "running"
        self._connected_at = time.time()
        self.subs_sent = 0
        self.tracked_trades = 0
        watcher = asyncio.create_task(self._watcher(engine_ws))
        try:
            async for msg in messages:
                await self.handle_message(engine_ws, msg)
        finally:
            watcher.cancel()

    async def _pumpportal_messages(self, ws):
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(msg, dict):
                yield msg

    async def _run(self):
        delay = 3
        while True:
            self._connected_at = None
            try:
                self.status = "connecting"
                if config.SOLANA_WS_URL:
                    async with websockets.connect(config.SOLANA_WS_URL, ping_interval=20) as ws:
                        self.event("info", "Подключено к Solana RPC")
                        await self._stream(NullWS(), self.feed.stream(ws, self.event))
                else:
                    async with websockets.connect(config.ws_url(), ping_interval=20) as ws:
                        await ws.send(json.dumps({"method": "subscribeNewToken"}))
                        self.event("info", "Подключено к PumpPortal")
                        await self._stream(ws, self._pumpportal_messages(ws))
                self.event("warn", "Сервер закрыл соединение")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.event("warn", _redact(f"Соединение потеряно: {type(e).__name__}: {e}"))
            self._drop_candidates()
            for mint in list(self.positions):
                self._finalize(mint, "disconnect")
            self.status = "connecting"
            # Если соединение продержалось дольше 30 с, начинаем заново с 3 с, иначе увеличиваем паузу (до 60 с),
            # чтобы частыми переподключениями не получить блокировку от сервера
            if self._connected_at and time.time() - self._connected_at > 30:
                delay = 3
            self.event("info", f"Переподключение через {delay} с")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    # ---------- данные для панели ----------
    def snapshot(self) -> dict:
        t = self.trades
        n = len(t)
        wins = sum(1 for x in t if x["pnl_sol"] > 0)
        stats = {
            "trades": n,
            "wins": wins,
            "win_rate": wins / n * 100 if n else 0,
            "total_pnl_sol": sum(x["pnl_sol"] for x in t),
            "avg_pnl_pct": sum(x["pnl_pct"] for x in t) / n if n else 0,
            "best_pct": max((x["pnl_pct"] for x in t), default=0),
            "worst_pct": min((x["pnl_pct"] for x in t), default=0),
            "avg_hold_s": sum(x["hold_s"] for x in t) / n if n else 0,
        }

        by_reason = {}
        for x in t:
            r = by_reason.setdefault(x["reason"], {"count": 0, "pnl_sol": 0.0})
            r["count"] += 1
            r["pnl_sol"] += x["pnl_sol"]

        full, c = [], 0.0
        for x in t:
            c += x["pnl_sol"]
            full.append(round(c, 5))
        step = max(1, -(-len(full) // 300))
        equity = full[::step]
        if full and (len(full) - 1) % step:
            equity.append(full[-1])

        now = time.time()
        positions = []
        unrealized = 0.0
        for p in self.positions.values():
            pnl_pct = (p.last_mcap / p.entry_mcap - 1) * 100 - 2 * settings["fee_pct"]
            unrealized += p.buy_sol * pnl_pct / 100
            positions.append({
                "mint": p.mint, "symbol": p.symbol, "entry_mcap": p.entry_mcap,
                "last_mcap": p.last_mcap, "pnl_pct": pnl_pct, "age_s": now - p.opened_at,
                "tag": p.features.get("tag", ""),
                "ai_risk": p.features.get("ai_risk"), "ai_hype": p.features.get("ai_hype"),
            })

        rep = self.learner.report(t, settings["learning_min_trades"], settings["min_expected_pct"])
        learning = {
            "enabled": settings["learning_enabled"],
            "n": self.learner.n,
            "min_trades": settings["learning_min_trades"],
            "ready": self.learner.n >= settings["learning_min_trades"],
            "skipped_model": self.skipped_model,
            "skipped_blacklist": self.skipped_blacklist,
            "blacklist": sum(1 for v in self.creator_losses.values() if v >= settings["blacklist_losses"]),
            "entries": dict(self.entry_tags),
            "watching": len(self.candidates),
            **rep,
        }
        order = {"<3": 0, "3–6": 1, "≥6": 2}
        by_risk = sorted((b for b in rep["buckets"] if b["feature"] == "ai_risk"), key=lambda b: order.get(b["bucket"], 9))

        return {
            "status": self.status,
            "uptime_s": now - self.started_at if self.started_at else 0,
            "seen": self.seen,
            "entered": self.entered,
            "stats": stats,
            "unrealized_sol": unrealized,
            "by_reason": by_reason,
            "equity": equity,
            "positions": positions,
            "trades": [{**x, "tag": (x.get("features") or {}).get("tag", ""),
                        "ai_risk": (x.get("features") or {}).get("ai_risk"),
                        "ai_hype": (x.get("features") or {}).get("ai_hype"),
                        "features": None} for x in t[-200:][::-1]],
            "ai": {**self.ai.snapshot(), "skipped": self.skipped_ai, "by_risk": by_risk},
            "events": list(self.events)[-100:][::-1],
            "learning": learning,
            "diagnostics": {
                "messages": dict(self.msg_counts),
                "subs_sent": self.subs_sent,
                "tracked_trades": self.tracked_trades,
                # подписались на 10+ токенов, а сделок по ним нет вообще: данные не приходят
                "no_trade_data": self.status == "running" and self.subs_sent >= 10 and self.tracked_trades == 0,
                "needs_api_key": self.needs_api_key and self.tracked_trades == 0,
                "has_key": bool(config.PUMPPORTAL_API_KEY),
                "source": "solana" if config.SOLANA_WS_URL else "pumpportal",
                "feed": dict(self.feed.stats),
                # получаем транзакции pump.fun, но события в логах не находим (возможно, изменился формат)
                "no_events": bool(config.SOLANA_WS_URL) and self.feed.stats["notifications"] >= 50
                             and self.feed.stats["create_events"] + self.feed.stats["trade_events"] == 0,
            },
        }


async def main():
    engine = Engine()
    runner = web.AppRunner(create_app(engine))
    await runner.setup()
    await web.TCPSite(runner, config.WEB_HOST, config.WEB_PORT).start()
    url = f"http://{config.WEB_HOST}:{config.WEB_PORT}"
    print(f"Панель управления: {url}")
    if config.OPEN_BROWSER:
        webbrowser.open(url)
    try:
        await asyncio.Event().wait()
    finally:
        await engine.stop()
        await engine.ai.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено")
