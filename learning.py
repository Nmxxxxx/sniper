"""
Простая и проверяемая модель «обучения на сделках».

Для каждой закрытой сделки бот запоминает признаки входа (сколько было покупателей,
продавал ли создатель, как вырос mcap за окно наблюдения и т.д.) и итоговый результат.
Признаки разбиваются на корзины. Для новой сделки ожидаемый результат считается как
среднее по корзинам с поправкой к общему среднему (shrinkage), чтобы редкие корзины
с парой случайных сделок не давали ложных выводов.

Проверка честности: walk_forward() идёт по сделкам по времени и для каждой
предсказывает результат, зная только прошлое. Так видно, действительно ли модель
отличает хорошие входы от плохих, а не просто подогнана под старые данные.
"""
from collections import defaultdict

FEATURES = ["dev_buy_sol", "unique_buyers", "sells", "creator_sold", "volume_sol", "mcap_change_pct"]

# Выходы, которые отражают реальный исход сделки (ручная остановка и обрыв связи не учитываем)
LEARN_REASONS = {"take_profit", "stop_loss", "timeout"}

EDGES = {
    "dev_buy_sol": [1, 2, 3],
    "unique_buyers": [1, 3, 6, 11],
    "sells": [1, 2, 4],
    "volume_sol": [0.5, 2, 5],
    "mcap_change_pct": [0, 10, 30, 60],
}

PRIOR_N = 10   # «вес» общего среднего: корзина с n << 10 сделок почти не двигает оценку


def _label(edges, v):
    for i, e in enumerate(edges):
        if v < e:
            return f"<{e:g}" if i == 0 else f"{edges[i-1]:g}–{e:g}"
    return f"≥{edges[-1]:g}"


def bucket(name, value):
    if name == "creator_sold":
        return "да" if value else "нет"
    try:
        v = float(value or 0)
    except (TypeError, ValueError):
        v = 0.0
    return _label(EDGES[name], v)


def _buckets(f):
    return {name: bucket(name, f.get(name, 0)) for name in FEATURES}


class Learner:
    def __init__(self):
        self.reset()

    def reset(self):
        self.n = 0
        self.total = 0.0
        self.stats = {}          # (признак, корзина) -> [n, сумма результата %]
        self._cache = None

    def add(self, features: dict, pnl_pct: float):
        self.n += 1
        self.total += pnl_pct
        for k, b in _buckets(features).items():
            s = self.stats.setdefault((k, b), [0, 0.0])
            s[0] += 1
            s[1] += pnl_pct
        self._cache = None

    def score(self, features: dict) -> float:
        """Ожидаемый результат сделки в % (после комиссий)."""
        if self.n == 0:
            return 0.0
        g = self.total / self.n
        means = []
        for k, b in _buckets(features).items():
            n, s = self.stats.get((k, b), (0, 0.0))
            means.append((s + PRIOR_N * g) / (n + PRIOR_N))
        return sum(means) / len(means)

    def report(self, trades: list, min_trades: int, threshold: float) -> dict:
        key = (self.n, len(trades), min_trades, threshold)
        if self._cache and self._cache[0] == key:
            return self._cache[1]
        g = self.total / self.n if self.n else 0.0
        rows = []
        for (feat, b), (n, s) in self.stats.items():
            m = (s + PRIOR_N * g) / (n + PRIOR_N)
            rows.append({"feature": feat, "bucket": b, "n": n, "avg_pct": s / n, "effect": m - g})
        rows.sort(key=lambda r: -r["effect"])
        res = {"global_avg": g, "buckets": rows, "walk": walk_forward(trades, min_trades, threshold)}
        self._cache = (key, res)
        return res


def walk_forward(trades: list, min_trades: int, threshold: float) -> dict:
    """Для каждой сделки модель знает только прошлое. Сравниваем, что было бы с «взятыми» и «пропущенными»."""
    sim = Learner()
    take, skip = [], []
    for t in trades:
        f = t.get("features")
        if not f or t.get("reason") not in LEARN_REASONS:
            continue
        if sim.n >= min_trades:
            (take if sim.score(f) >= threshold else skip).append(t["pnl_pct"])
        sim.add(f, t["pnl_pct"])
    avg = lambda a: sum(a) / len(a) if a else 0.0
    return {"take_n": len(take), "take_avg": avg(take), "skip_n": len(skip), "skip_avg": avg(skip)}
