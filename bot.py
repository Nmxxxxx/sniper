"""
Paper-trading снайпер для pump.fun с веб-панелью.

Бот слушает новые токены через PumpPortal WebSocket, виртуально покупает
подходящие и закрывает позиции по TP / SL / таймеру. Реальных сделок нет.
Управление и статистика: http://127.0.0.1:8000
"""
import asyncio
import csv
import json
import os
import time
import webbrowser
from collections import deque
from dataclasses import dataclass

import websockets
from aiohttp import web

import config
from config import settings
from server import create_app

CSV_FIELDS = ["time", "mint", "symbol", "entry_mcap", "exit_mcap",
              "hold_s", "reason", "pnl_pct", "pnl_sol"]

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


@dataclass
class Position:
    mint: str
    symbol: str
    entry_mcap: float      # market cap (в SOL) в момент входа
    last_mcap: float
    opened_at: float
    buy_sol: float


class Engine:
    def __init__(self):
        self.status = "stopped"          # stopped / connecting / running
        self.positions: dict[str, Position] = {}
        self.trades: list[dict] = []
        self.events = deque(maxlen=200)
        self.seen = 0                    # сколько новых токенов увидели
        self.entered = 0                 # в скольких виртуально купили
        self.started_at = None
        self._task = None
        self._load_history()

    # ---------- служебное ----------
    def event(self, level: str, msg: str):
        self.events.append({"t": time.strftime("%H:%M:%S"), "level": level, "msg": msg})

    def _load_history(self):
        """Подхватываем прошлые сделки из CSV, чтобы статистика не пропадала после перезапуска."""
        if not os.path.exists(config.LOG_FILE):
            return
        try:
            with open(config.LOG_FILE, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    try:
                        self.trades.append({
                            "time": row["time"], "mint": row["mint"], "symbol": row["symbol"],
                            "entry_mcap": float(row["entry_mcap"]), "exit_mcap": float(row["exit_mcap"]),
                            "hold_s": float(row["hold_s"]), "reason": row["reason"],
                            "pnl_pct": float(row["pnl_pct"]), "pnl_sol": float(row["pnl_sol"]),
                        })
                    except (KeyError, ValueError):
                        continue
        except OSError:
            pass

    def _append_csv(self, trade: dict):
        new_file = not os.path.exists(config.LOG_FILE)
        with open(config.LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if new_file:
                w.writeheader()
            w.writerow(trade)

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
        for mint in list(self.positions):
            self._finalize(mint, "manual_stop")
        was_running = self.status != "stopped"
        self.status = "stopped"
        self.started_at = None
        if was_running:
            self.event("info", "Бот остановлен")

    def reset_stats(self):
        """Обнуляет статистику. Старый CSV не удаляем, а переименовываем в .bak."""
        if os.path.exists(config.LOG_FILE):
            os.replace(config.LOG_FILE, f"{config.LOG_FILE}.{time.strftime('%Y%m%d_%H%M%S')}.bak")
        self.trades.clear()
        self.seen = 0
        self.entered = 0
        self.event("info", "Статистика сброшена (старый CSV сохранён как .bak)")

    # ---------- торговая логика ----------
    def _passes_filters(self, msg: dict) -> bool:
        dev_buy = _num(msg.get("solAmount"))
        if not (settings["min_dev_buy_sol"] <= dev_buy <= settings["max_dev_buy_sol"]):
            return False
        if len(self.positions) >= settings["max_open_positions"]:
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
        }
        self.trades.append(trade)
        self._append_csv(trade)
        self.event("win" if pnl_sol > 0 else "loss",
                   f"SELL {pos.symbol}: {pnl_pct:+.1f}% ({REASON_NAMES.get(reason, reason)})")

    async def _close(self, ws, mint: str, reason: str):
        self._finalize(mint, reason)
        try:
            await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))
        except Exception:
            pass

    async def handle_message(self, ws, msg: dict):
        mint = msg.get("mint")
        if not mint:
            return
        tx = msg.get("txType")

        if tx == "create":
            self.seen += 1
            if not self._passes_filters(msg):
                return
            mcap = _num(msg.get("marketCapSol"))
            if mcap <= 0:
                return
            symbol = str(msg.get("symbol") or "?")[:20]
            self.positions[mint] = Position(mint, symbol, mcap, mcap, time.time(), settings["buy_sol"])
            self.entered += 1
            self.event("buy", f"BUY {symbol}: mcap {mcap:.1f} SOL, покупка создателя {_num(msg.get('solAmount')):.2f} SOL")
            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
            return

        pos = self.positions.get(mint)
        if pos and tx in ("buy", "sell"):
            mcap = _num(msg.get("marketCapSol"))
            if mcap <= 0:
                return
            pos.last_mcap = mcap
            change = (mcap / pos.entry_mcap - 1) * 100
            if change >= settings["take_profit_pct"]:
                await self._close(ws, mint, "take_profit")
            elif change <= settings["stop_loss_pct"]:
                await self._close(ws, mint, "stop_loss")

    async def _timeout_watcher(self, ws):
        while True:
            await asyncio.sleep(1)
            now = time.time()
            for mint, pos in list(self.positions.items()):
                if now - pos.opened_at >= settings["max_hold_seconds"]:
                    await self._close(ws, mint, "timeout")

    async def _run(self):
        while True:
            try:
                self.status = "connecting"
                async with websockets.connect(config.WS_URL, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    self.status = "running"
                    self.event("info", "Подключено к PumpPortal")
                    watcher = asyncio.create_task(self._timeout_watcher(ws))
                    try:
                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(msg, dict):
                                await self.handle_message(ws, msg)
                    finally:
                        watcher.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.event("warn", f"Соединение потеряно: {e}")
            for mint in list(self.positions):
                self._finalize(mint, "disconnect")
            self.status = "connecting"
            await asyncio.sleep(3)

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
            })

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
            "trades": t[-200:][::-1],
            "events": list(self.events)[-100:][::-1],
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
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено")
