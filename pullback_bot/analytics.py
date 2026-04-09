"""
analytics.py — Computes full backend analytics from DB trades.
"""
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, Any, List

def compute_analytics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_trades = len(trades)
    if total_trades == 0:
        return empty_analytics()

    wins = 0
    total_pnl = 0.0

    # Static Buckets
    strategy_stats = {"PULLBACK": {"trades": 0, "wins": 0, "pnl": 0.0}, "BREAKOUT": {"trades": 0, "wins": 0, "pnl": 0.0}}
    dir_stats = {"LONG": {"trades": 0, "wins": 0, "pnl": 0.0}, "SHORT": {"trades": 0, "wins": 0, "pnl": 0.0}}
    
    session_names = ["Sydney", "Tokyo", "Asia", "London", "London/NY", "New York", "After Hours"]
    sessions = {s: {"trades": 0, "wins": 0, "pnl": 0.0} for s in session_names}
    
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_stats = {d: {"trades": 0, "wins": 0, "pnl": 0.0} for d in days}
    
    hour_stats = {str(h): {"trades": 0, "wins": 0, "pnl": 0.0} for h in range(24)}
    
    score_bands = ["60-65", "65-70", "70-75", "75-80", "80+"]
    score_stats = {b: {"trades": 0, "wins": 0, "pnl": 0.0} for b in score_bands}
    
    reasons = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
    sym_pnls = defaultdict(float)

    for t in trades:
        pnl = float(t.get("pnl_usdt") or 0.0)
        is_win = pnl > 0
        total_pnl += pnl
        if is_win:
            wins += 1

        score = float(t.get("score") or 0.0)
        entry_ms = int(t.get("entry_time") or 0)
        dt = datetime.fromtimestamp(entry_ms / 1000.0, tz=timezone.utc)
        
        # Exit Reason
        r = t.get("close_reason") or "UNKNOWN"
        reasons[r]["trades"] += 1
        reasons[r]["pnl"] += pnl
        if is_win: reasons[r]["wins"] += 1

        # Symbol
        sym = t.get("symbol", "UNKNOWN")
        sym_pnls[sym] += pnl

        # Strategy
        stype = t.get("signal_type", "UNKNOWN")
        if stype in strategy_stats:
            strategy_stats[stype]["trades"] += 1
            strategy_stats[stype]["pnl"] += pnl
            if is_win: strategy_stats[stype]["wins"] += 1

        # Direction
        d = t.get("direction", "UNKNOWN")
        if d in dir_stats:
            dir_stats[d]["trades"] += 1
            dir_stats[d]["pnl"] += pnl
            if is_win: dir_stats[d]["wins"] += 1

        # Date / Time
        d_name = days[dt.weekday()]
        day_stats[d_name]["trades"] += 1
        day_stats[d_name]["pnl"] += pnl
        if is_win: day_stats[d_name]["wins"] += 1

        hr = str(dt.hour)
        hour_stats[hr]["trades"] += 1
        hour_stats[hr]["pnl"] += pnl
        if is_win: hour_stats[hr]["wins"] += 1

        h = dt.hour
        # Timezone Overlaps
        if h >= 21 or h < 6:
            sessions["Sydney"]["trades"] += 1
            sessions["Sydney"]["pnl"] += pnl
            if is_win: sessions["Sydney"]["wins"] += 1
        if 0 <= h < 9:
            sessions["Tokyo"]["trades"] += 1
            sessions["Tokyo"]["pnl"] += pnl
            if is_win: sessions["Tokyo"]["wins"] += 1
        if 0 <= h < 8:
            sessions["Asia"]["trades"] += 1
            sessions["Asia"]["pnl"] += pnl
            if is_win: sessions["Asia"]["wins"] += 1
        if 8 <= h < 16:
            sessions["London"]["trades"] += 1
            sessions["London"]["pnl"] += pnl
            if is_win: sessions["London"]["wins"] += 1
        if 13 <= h < 16:
            sessions["London/NY"]["trades"] += 1
            sessions["London/NY"]["pnl"] += pnl
            if is_win: sessions["London/NY"]["wins"] += 1
        if 13 <= h < 21:
            sessions["New York"]["trades"] += 1
            sessions["New York"]["pnl"] += pnl
            if is_win: sessions["New York"]["wins"] += 1
        if 21 <= h < 24:
            sessions["After Hours"]["trades"] += 1
            sessions["After Hours"]["pnl"] += pnl
            if is_win: sessions["After Hours"]["wins"] += 1

        # Score buckets
        b = "80+"
        if score < 65: b = "60-65"
        elif score < 70: b = "65-70"
        elif score < 75: b = "70-75"
        elif score < 80: b = "75-80"
        
        score_stats[b]["trades"] += 1
        score_stats[b]["pnl"] += pnl
        if is_win: score_stats[b]["wins"] += 1

    # Format Leaders/Bleeders
    sym_list = [{"symbol": k, "pnl": v} for k, v in sym_pnls.items()]
    
    top_10 = sorted(sym_list, key=lambda x: x["pnl"], reverse=True)[:10]
    bottom_10 = sorted(sym_list, key=lambda x: x["pnl"])[:10]

    return {
        "summary": {
            "total_trades": total_trades,
            "win_rate": (wins / total_trades * 100) if total_trades else 0.0,
            "total_pnl": total_pnl,
            "avg_pnl": total_pnl / total_trades if total_trades else 0.0,
        },
        "strategy": strategy_stats,
        "direction": dir_stats,
        "sessions": sessions,
        "day_of_week": day_stats,
        "hour_of_day": hour_stats,
        "score_buckets": score_stats,
        "exit_reasons": dict(reasons),
        "symbols": {"top_10": top_10, "bottom_10": bottom_10}
    }

def empty_analytics() -> Dict[str, Any]:
    return {
        "summary": {"total_trades": 0, "win_rate": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0},
        "strategy": {},
        "direction": {},
        "sessions": {},
        "day_of_week": {},
        "hour_of_day": {},
        "score_buckets": {},
        "exit_reasons": {},
        "symbols": {"top_10": [], "bottom_10": []}
    }
