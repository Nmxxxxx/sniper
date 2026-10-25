"""
Бесплатный источник данных: чтение сделок pump.fun прямо из блокчейна Solana.

Бот подписывается на логи программы pump.fun (logsSubscribe через любой RPC с WebSocket).
В каждой транзакции программа пишет события в строки вида "Program data: <base64>".
Мы разбираем два события и превращаем их в сообщения того же вида, что давал PumpPortal,
поэтому остальной код бота не меняется:
  CreateEvent -> {"txType": "create", ...}        (новый токен)
  TradeEvent  -> {"txType": "buy" | "sell", ...}  (сделка по токену)

Формат событий взят из публичного IDL pump.fun (поля могут дописываться в конец,
поэтому читаем только нужное начало и не требуем точной длины).
"""
import base64
import json
import struct
from collections import Counter

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Первые 8 байт sha256("event:<Имя>"), стандарт Anchor
TRADE_DISC = bytes([189, 219, 127, 211, 78, 230, 97, 238])
CREATE_DISC = bytes([27, 114, 169, 77, 222, 235, 99, 118])

# Стартовые резервы кривой pump.fun (используются, если в событии создания их нет)
INITIAL_VSOL = 30_000_000_000            # 30 SOL в лампортах
INITIAL_VTOKEN = 1_073_000_000_000_000   # 1.073 млрд токенов с 6 знаками

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    pad = len(data) - len(data.lstrip(b"\0"))
    return "1" * pad + out


def mcap_sol(vsol: int, vtoken: int) -> float:
    """Рыночная капитализация в SOL: цена токена * 1 млрд токенов (общий выпуск pump.fun)."""
    return vsol * 1e6 / vtoken if vtoken else 0.0


def _read_str(d: bytes, o: int):
    (n,) = struct.unpack_from("<I", d, o)
    o += 4
    if n > 2000 or o + n > len(d):
        raise ValueError("bad string")
    return d[o:o + n].decode("utf-8", "replace"), o + n


def parse_trade(d: bytes):
    """TradeEvent без 8 байт идентификатора: mint, sol, tokens, is_buy, user, timestamp, vsol, vtoken, ..."""
    if len(d) < 105:
        return None
    sol, tok = struct.unpack_from("<QQ", d, 32)
    _, vsol, vtok = struct.unpack_from("<qQQ", d, 81)
    return {
        "mint": b58encode(d[0:32]), "sol": sol, "tokens": tok, "is_buy": d[48] == 1,
        "user": b58encode(d[49:81]), "vsol": vsol, "vtoken": vtok,
    }


def parse_create(d: bytes):
    """CreateEvent: name, symbol, uri, mint, bonding_curve, user, [creator, timestamp, vtoken, vsol, ...]"""
    name, o = _read_str(d, 0)
    symbol, o = _read_str(d, o)
    uri, o = _read_str(d, o)
    if o + 96 > len(d):
        return None
    mint, user = b58encode(d[o:o + 32]), b58encode(d[o + 64:o + 96])
    o += 96
    creator = user
    if len(d) - o >= 40:                   # creator + timestamp (в старых версиях события этих полей нет)
        creator = b58encode(d[o:o + 32])
        o += 40
    vtoken = vsol = None
    if len(d) - o >= 16:
        vtoken, vsol = struct.unpack_from("<QQ", d, o)
    return {"name": name, "symbol": symbol, "uri": uri, "mint": mint, "user": user,
            "creator": creator, "vsol": vsol, "vtoken": vtoken}


def decode_logs(logs, stats: Counter):
    events = []
    for line in logs:
        if not isinstance(line, str) or not line.startswith("Program data: "):
            continue
        try:
            raw = base64.b64decode(line[14:])
        except Exception:
            stats["decode_errors"] += 1
            continue
        disc = raw[:8]
        try:
            if disc == TRADE_DISC:
                ev = parse_trade(raw[8:])
                if ev:
                    events.append(("trade", ev))
                else:
                    stats["decode_errors"] += 1
            elif disc == CREATE_DISC:
                ev = parse_create(raw[8:])
                if ev:
                    events.append(("create", ev))
                else:
                    stats["decode_errors"] += 1
        except (struct.error, ValueError):
            stats["decode_errors"] += 1
    return events


class SolanaFeed:
    def __init__(self):
        self.stats = Counter()

    def process(self, value: dict) -> list:
        """Одна транзакция (уведомление logsSubscribe) -> список сообщений в формате PumpPortal."""
        self.stats["notifications"] += 1
        if value.get("err"):
            self.stats["failed_tx"] += 1
            return []
        events = decode_logs(value.get("logs") or [], self.stats)
        if not events:
            self.stats["no_events"] += 1
            return []

        creates = [e for k, e in events if k == "create"]
        trades = [e for k, e in events if k == "trade"]
        used = set()
        sig = value.get("signature")
        msgs = []

        for c in creates:
            self.stats["create_events"] += 1
            dev = None
            for i, t in enumerate(trades):
                # покупка создателя в той же транзакции = «dev buy»
                if i not in used and t["mint"] == c["mint"] and t["is_buy"] and t["user"] in (c["user"], c["creator"]):
                    dev = t
                    used.add(i)
                    break
            if dev:
                dev_sol = dev["sol"] / 1e9
                mc = mcap_sol(dev["vsol"], dev["vtoken"])
            else:
                dev_sol = 0.0
                mc = mcap_sol(c["vsol"] or INITIAL_VSOL, c["vtoken"] or INITIAL_VTOKEN)
            msgs.append({"txType": "create", "mint": c["mint"], "symbol": c["symbol"], "name": c["name"],
                         "uri": c["uri"], "traderPublicKey": c["creator"], "solAmount": dev_sol,
                         "marketCapSol": mc, "signature": sig})

        for i, t in enumerate(trades):
            self.stats["trade_events"] += 1
            if i in used:
                continue
            msgs.append({"txType": "buy" if t["is_buy"] else "sell", "mint": t["mint"],
                         "traderPublicKey": t["user"], "solAmount": t["sol"] / 1e9,
                         "marketCapSol": mcap_sol(t["vsol"], t["vtoken"]), "signature": sig})
        return msgs

    async def stream(self, ws, log):
        """Подписывается на логи pump.fun и отдаёт нормализованные сообщения."""
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
            "params": [{"mentions": [PUMP_PROGRAM]}, {"commitment": "processed"}],
        }))
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            if msg.get("id") == 1:
                if "error" in msg:
                    raise RuntimeError(f"RPC отклонил подписку: {json.dumps(msg['error'], ensure_ascii=False)[:200]}")
                log("info", "Подписка на логи pump.fun активна")
                continue
            if msg.get("method") != "logsNotification":
                continue
            value = ((msg.get("params") or {}).get("result") or {}).get("value")
            if isinstance(value, dict):
                for m in self.process(value):
                    yield m
