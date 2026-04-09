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

    # Advanced metrics tracking
    pnl_series = []
    
    win_duration_ms = 0
    los_duration_ms = 0
    real_losers = 0
    real_wins = 0

    current_drawdown = 0.0
    max_drawdown = 0.0
    peak_pnl = 0.0
    
    current_loss_streak = 0
    max_loss_streak = 0

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
    
    # Store multi-dimensional symbol stats => {"trades": N, "wins": N, "pnl": N}
    sym_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})

    # Chronologically sort to enforce exact visual curve and streak logic
    trades_sorted = sorted(trades, key=lambda x: int(x.get("entry_time") or 0))

    for t in trades_sorted:
        pnl = float(t.get("pnl_usdt") or 0.0)
        is_win = pnl > 0
        
        # PnL & Series Flow
        total_pnl += pnl
        pnl_series.append(total_pnl)
        
        # Drawdown logic
        if total_pnl > peak_pnl:
            peak_pnl = total_pnl
            current_drawdown = 0.0
        else:
            current_drawdown = peak_pnl - total_pnl
            if current_drawdown > max_drawdown:
                max_drawdown = current_drawdown
        
        # Win / Streak Logic
        if is_win:
            wins += 1
            real_wins += 1
            current_loss_streak = 0
        else:
            real_losers += 1
            current_loss_streak += 1
            if current_loss_streak > max_loss_streak:
                max_loss_streak = current_loss_streak

        # Hold Duration Logic
        entry_ms = int(t.get("entry_time") or 0)
        close_ms = int(t.get("close_time") or 0)
        if close_ms > entry_ms:
            dur = close_ms - entry_ms
            if is_win: win_duration_ms += dur
            else:      los_duration_ms += dur

        score = float(t.get("score") or 0.0)
        dt = datetime.fromtimestamp(entry_ms / 1000.0, tz=timezone.utc)
        
        # Exit Reason
        r = t.get("close_reason") or "UNKNOWN"
        reasons[r]["trades"] += 1
        reasons[r]["pnl"] += pnl
        if is_win: reasons[r]["wins"] += 1

        # Symbol
        sym = t.get("symbol", "UNKNOWN")
        sym_stats[sym]["trades"] += 1
        sym_stats[sym]["pnl"] += pnl
        if is_win: sym_stats[sym]["wins"] += 1

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

    # Format Leaders/Bleeders Matrix
    sym_list = []
    for k, v in sym_stats.items():
        wr = (v["wins"]/v["trades"])*100 if v["trades"]>0 else 0
        sym_list.append({
            "symbol": k,
            "pnl": v["pnl"],
            "trades": v["trades"],
            "win_rate": wr
        })
    
    top_10 = sorted(sym_list, key=lambda x: x["pnl"], reverse=True)[:10]
    bottom_10 = sorted(sym_list, key=lambda x: x["pnl"])[:10]

    # Duration calculations (seconds)
    avg_win_s = (win_duration_ms / real_wins / 1000) if real_wins > 0 else 0
    avg_los_s = (los_duration_ms / real_losers / 1000) if real_losers > 0 else 0

    stats = {
        "summary": {
            "total_trades": total_trades,
            "win_rate": (wins / total_trades * 100) if total_trades else 0.0,
            "total_pnl": total_pnl,
            "avg_pnl": total_pnl / total_trades if total_trades else 0.0,
            "max_drawdown": max_drawdown,
            "max_loss_streak": max_loss_streak,
            "avg_winner_duration_s": avg_win_s,
            "avg_loser_duration_s": avg_los_s
        },
        "pnl_series": pnl_series,
        "strategy": strategy_stats,
        "direction": dir_stats,
        "sessions": sessions,
        "day_of_week": day_stats,
        "hour_of_day": hour_stats,
        "score_buckets": score_stats,
        "exit_reasons": dict(reasons),
        "symbols": {"top_10": top_10, "bottom_10": bottom_10}
    }
    
    stats["insights"] = generate_insights(stats)
    return stats

def empty_analytics() -> Dict[str, Any]:
    return {
        "summary": {
            "total_trades": 0, 
            "win_rate": 0.0, 
            "total_pnl": 0.0, 
            "avg_pnl": 0.0,
            "max_drawdown": 0.0,
            "max_loss_streak": 0,
            "avg_winner_duration_s": 0.0,
            "avg_loser_duration_s": 0.0
        },
        "pnl_series": [],
        "strategy": {},
        "direction": {},
        "sessions": {},
        "day_of_week": {},
        "hour_of_day": {},
        "score_buckets": {},
        "exit_reasons": {},
        "symbols": {"top_10": [], "bottom_10": []},
        "insights": []
    }

def generate_insights(stats: Dict[str, Any]) -> List[Dict[str, str]]:
    insights = []
    summary = stats["summary"]
    if summary["total_trades"] < 5:
        insights.append({"type": "info", "message": "Not enough trade history to generate reliable statistical recommendations. Accumulate at least 5 trades to unlock the Health Check system."})
        return insights
        
    # ── Strategy Discrepancy ──
    strat = stats["strategy"]
    pb = strat.get("PULLBACK", {})
    br = strat.get("BREAKOUT", {})
    pb_wr = (pb.get("wins", 0) / pb.get("trades", 1) * 100) if pb.get("trades", 0) > 0 else 0
    br_wr = (br.get("wins", 0) / br.get("trades", 1) * 100) if br.get("trades", 0) > 0 else 0
    
    if pb.get("trades", 0) >= 3 and br.get("trades", 0) >= 3:
        if pb_wr > br_wr + 15:
            insights.append({"type": "warning", "message": f"Strategy Deviation: Pullback severely outperforms Breakout ({pb_wr:.0f}% vs {br_wr:.0f}% win rate). Consider disabling Breakouts entirely or demanding a higher score threshold."})
        elif br_wr > pb_wr + 15:
            insights.append({"type": "warning", "message": f"Strategy Deviation: Breakout severely outperforms Pullback ({br_wr:.0f}% vs {pb_wr:.0f}% win rate). Consider disabling Pullback entirely or demanding a higher score threshold."})

    # ── Score Calibration Check ──
    scores = stats["score_buckets"]
    low = scores.get("60-65", {})
    high = scores.get("80+", {})
    
    low_wr = (low.get("wins", 0) / low.get("trades", 1) * 100) if low.get("trades", 0) > 0 else 0
    high_wr = (high.get("wins", 0) / high.get("trades", 1) * 100) if high.get("trades", 0) > 0 else 0
    
    if low.get("trades", 0) >= 3 and high.get("trades", 0) >= 3:
        if high_wr > low_wr + 10 and summary["total_pnl"] < 0:
            insights.append({"type": "warning", "message": f"Score Calibration: Filtering logic detects that high scoring signals (80+) win significantly more often ({high_wr:.0f}%) than 60-65 signals ({low_wr:.0f}%). You are bleeding capital due to low-score noise. Increase your SIGNAL_SCORE_THRESHOLD to >70."})
        elif low_wr > high_wr:
            insights.append({"type": "info", "message": "Score Correlation: High scoring signals are currently underperforming low scoring ones. The algorithm's structural rating system indicates trend noise is overriding mathematical setups."})

    # ── Specific Target Bleeders V2 (Using Matrix logic) ──
    bot = stats["symbols"]["bottom_10"]
    for sym_dat in bot:
        if sym_dat["pnl"] < -10.0 and sym_dat["win_rate"] < 30.0 and sym_dat["trades"] >= 3:
            insights.append({
                "type": "danger", 
                "message": f"Bleeder Detected: {sym_dat['symbol']} has an abysmal {sym_dat['win_rate']:.0f}% win-rate over {sym_dat['trades']} trades resulting in {sym_dat['pnl']:.2f} USDT loss. Strongly recommend adding this pair to your Blocklist."
            })
            break # Only alert the absolute worst one so we don't spam 

    # ── Timezone Hazard ──
    sessions = stats["sessions"]
    worst_session = None
    worst_pnl = 0.0
    for s_name, s_data in sessions.items():
        if s_data.get("trades", 0) >= 3 and s_data.get("pnl", 0.0) < worst_pnl:
            worst_pnl = s_data["pnl"]
            worst_session = s_name
    
    if worst_session and worst_pnl < -15.0:
        insights.append({"type": "warning", "message": f"Timezone Bleed: The {worst_session} session destroys capital systematically ({worst_pnl:.2f} total loss). Turn the bot off dynamically during this window."})

    # ── Duration Reversal Logic ──
    if summary["avg_loser_duration_s"] > summary["avg_winner_duration_s"] * 1.5 and summary["avg_loser_duration_s"] > 3600:
        insights.append({"type": "warning", "message": f"Hold-Time Inversion: You are holding losers {(summary['avg_loser_duration_s']/60):.0f} minutes on average vs winners at {(summary['avg_winner_duration_s']/60):.0f} minutes. The bot is acting as a bag-holder. Tighten structural sl_price math."})

    # ── Risk:Reward Macro Profile ──
    wr = summary["win_rate"]
    pnl = summary["total_pnl"]
    if pnl > 0 and wr >= 55:
        insights.append({"type": "success", "message": "System Health is excellent. You are profitable with a dominant win rate. Stick to the methodology without adjustments."})
    elif pnl > 0 and wr < 50:
        insights.append({"type": "success", "message": "System is highly resilient. You are profitable despite a sub-50% win rate due to exceptionally cut losses and fat trailing profit arms. Macro structure is doing the heavy lifting."})
    elif pnl < 0 and wr >= 50:
        insights.append({"type": "danger", "message": "Fat-Tail Risk Detected: System loses capital systematically despite winning most of the trades! Your losses are far larger than your wins. Dramatically tighten your absolute Stop Losses immediately."})

    return insights
