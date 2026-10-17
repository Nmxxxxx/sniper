"""
Paper-trading снайпер для pump.fun.
Слушает новые токены через PumpPortal WebSocket, виртуально покупает
подходящие и закрывает позиции по TP / SL / таймеру. Реальных сделок нет.
"""
import asyncio
import csv
import json
import os
import time
from dataclasses import dataclass

import websockets

import config


@dataclass
class Position:
    mint: str
    symbol: str
    entry_mcap: float      # market cap (в SOL) в момент входа
    opened_at: float
    last_mcap: float


positions: dict[str, Position] = {}
total_pnl_sol = 0.0
closed_count = 0


def log_trade(pos: Position, reason: str, pnl_pct: float, pnl_sol: float):
    new_file = not os.path.exists(config.LOG_FILE)
    with open(config.LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["time", "mint", "symbol", "entry_mcap", "exit_mcap",
                        "hold_s", "reason", "pnl_pct", "pnl_sol"])
        w.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), pos.mint, pos.symbol,
                    round(pos.entry_mcap, 2), round(pos.last_mcap, 2),
                    round(time.time() - pos.opened_at, 1), reason,
                    round(pnl_pct, 2), round(pnl_sol, 5)])


def passes_filters(msg: dict) -> bool:
    dev_buy = float(msg.get("solAmount", 0) or 0)
    if not (config.MIN_DEV_BUY_SOL <= dev_buy <= config.MAX_DEV_BUY_SOL):
        return False
    if config.REQUIRE_SOCIALS and not msg.get("uri"):
        return False
    if len(positions) >= config.MAX_OPEN_POSITIONS:
        return False
    return True


async def close_position(ws, mint: str, reason: str):
    global total_pnl_sol, closed_count
    pos = positions.pop(mint, None)
    if not pos:
        return
    gross = (pos.last_mcap / pos.entry_mcap - 1) * 100
    pnl_pct = gross - 2 * config.FEE_PCT           # комиссия на вход и выход
    pnl_sol = config.BUY_SOL * pnl_pct / 100
    total_pnl_sol += pnl_sol
    closed_count += 1
    log_trade(pos, reason, pnl_pct, pnl_sol)
    print(f"[SELL] {pos.symbol} {reason} pnl={pnl_pct:+.1f}% "
          f"| всего: {total_pnl_sol:+.4f} SOL за {closed_count} сделок")
    await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": [mint]}))


async def handle_message(ws, msg: dict):
    mint = msg.get("mint")
    if not mint:
        return

    # Новый токен
    if msg.get("txType") == "create":
        if not passes_filters(msg):
            return
        mcap = float(msg.get("marketCapSol", 0) or 0)
        if mcap <= 0:
            return
        positions[mint] = Position(mint, msg.get("symbol", "?"), mcap, time.time(), mcap)
        print(f"[BUY ] {msg.get('symbol')} mcap={mcap:.1f} SOL dev_buy={msg.get('solAmount')}")
        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
        return

    # Сделки по токенам, на которые мы подписаны
    pos = positions.get(mint)
    if pos and msg.get("txType") in ("buy", "sell"):
        mcap = float(msg.get("marketCapSol", 0) or 0)
        if mcap <= 0:
            return
        pos.last_mcap = mcap
        change = (mcap / pos.entry_mcap - 1) * 100
        if change >= config.TAKE_PROFIT_PCT:
            await close_position(ws, mint, "take_profit")
        elif change <= config.STOP_LOSS_PCT:
            await close_position(ws, mint, "stop_loss")


async def timeout_watcher(ws):
    while True:
        await asyncio.sleep(1)
        now = time.time()
        for mint, pos in list(positions.items()):
            if now - pos.opened_at >= config.MAX_HOLD_SECONDS:
                await close_position(ws, mint, "timeout")


async def main():
    while True:
        try:
            async with websockets.connect(config.WS_URL, ping_interval=20) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                print("Подключились, ждём новые токены (paper mode)...")
                watcher = asyncio.create_task(timeout_watcher(ws))
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await handle_message(ws, msg)
                finally:
                    watcher.cancel()
        except Exception as e:
            print(f"Соединение потеряно ({e}), переподключаюсь через 3с")
            positions.clear()
            await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\nОстановлено. Итого: {total_pnl_sol:+.4f} SOL за {closed_count} сделок")
