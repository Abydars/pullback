import os
import re

file_path = "/Users/abid/Projects/pullback/pullback_bot/frontend/index.html"
with open(file_path, "r") as f:
    text = f.read()

new_fonts = """<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet" />"""

# Replace old font links
text = re.sub(r'<link rel="preconnect" href="https://fonts\.googleapis\.com" />\n<link href="https://fonts\.googleapis\.com.*?/>\n?', new_fonts + "\n", text, flags=re.DOTALL)

new_style = """<style>
/* ── Reset & Base ─────────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
:root {
  --bg: #09090b;
  --bg2: #18181b;
  --bg3: #27272a;
  --border: rgba(255, 255, 255, 0.08);
  --border-glow: rgba(255, 255, 255, 0.12);
  --text: #fafafa;
  --muted: #a1a1aa;
  --green: #22c55e;
  --red: #ef4444;
  --amber: #f59e0b;
  --indigo: #6366f1;
  --blue: #3b82f6;
  --font-ui: 'Inter', sans-serif;
  --font-mono: 'JetBrains Mono', monospace;
  --glass-bg: rgba(24, 24, 27, 0.85);
}
html, body { 
  height: 100%; background: var(--bg); color: var(--text); 
  font-family: var(--font-ui); font-size: 11px; overflow: hidden; 
  -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
}

/* ── Scrollbars ───────────────────────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { 
  background: rgba(255,255,255,0.15); border-radius: 4px; 
  border: 1px solid transparent; background-clip: padding-box; transition: background 0.2s;
}
::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.25); border-color: transparent; }

/* ── Layout ───────────────────────────────────────────────────────────────── */
#app { display: grid; grid-template-rows: 44px 1fr 28px; height: 100vh; }
#main { 
  display: grid; 
  grid-template-columns: minmax(220px, 16vw) 1fr minmax(280px, 20vw); 
  overflow: hidden; 
}

/* ── Header ───────────────────────────────────────────────────────────────── */
#header {
  display: flex; align-items: center; gap: 10px; padding: 0 12px;
  background: var(--glass-bg); backdrop-filter: blur(12px); border-bottom: 1px solid var(--border);
  position: relative; z-index: 50; box-shadow: 0 4px 24px rgba(0,0,0,0.2);
}
#header .logo { 
  font-weight: 700; font-size: 14px; letter-spacing: .05em; 
  background: linear-gradient(135deg, var(--amber) 0%, #fbbf24 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.mode-badge {
  padding: 2px 8px; border-radius: 4px; font-size: 9px; font-weight: 700; letter-spacing: .08em;
  box-shadow: 0 2px 8px rgba(0,0,0,0.2);
}
.mode-paper { background: rgba(99,102,241,.15); color: #818cf8; border: 1px solid rgba(99,102,241,.3); }
.mode-live  { background: rgba(239,68,68,.15);  color: #f87171; border: 1px solid rgba(239,68,68,.3); }
.regime-badge {
  padding: 2px 8px; border-radius: 4px; font-size: 9px; font-weight: 700; letter-spacing: .08em;
}
.regime-neutral { color: var(--muted); border: 1px solid var(--border); }
.regime-bull    { color: var(--green); background: rgba(34,197,94,0.1); border: 1px solid rgba(34,197,94,0.3); }
.regime-bear    { color: var(--red);   background: rgba(239,68,68,0.1); border: 1px solid rgba(239,68,68,0.3); }
#conn-dot {
  width: 6px; height: 6px; border-radius: 50%; background: var(--red);
  box-shadow: 0 0 8px var(--red); transition: background .3s, box-shadow .3s;
}
#conn-dot.ok { background: var(--green); box-shadow: 0 0 8px var(--green); }
#server-time { margin-left: auto; color: var(--muted); font-size: 11px; font-family: var(--font-mono); font-weight: 500; }
.stat-pill { color: var(--muted); font-size: 11px; font-weight: 500; }
.stat-pill span { color: var(--text); font-family: var(--font-mono); margin-left: 4px; }
#btn-config, #btn-sizing {
  display: flex; align-items: center; gap: 4px;
  padding: 4px 10px; border-radius: 6px; font-size: 10px;
  font-weight: 600; cursor: pointer; border: 1px solid var(--border);
  background: rgba(255,255,255,0.03); color: var(--muted); transition: all .2s cubic-bezier(0.4, 0, 0.2, 1);
}
#btn-config:hover, #btn-sizing:hover { 
  transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,0,0,0.2); 
  border-color: var(--border-glow); color: var(--text); background: rgba(255,255,255,0.06);
}

/* ── Modals & Glassmorphism ───────────────────────────────────────────────── */
.modal-overlay {
  position: fixed; inset: 0; background: rgba(0,0,0,.8); z-index: 900;
  backdrop-filter: blur(4px); display: flex; align-items: center; justify-content: center;
  transition: opacity .2s;
}
.modal-overlay.hidden { display: none; opacity: 0; }
.modal {
  background: var(--bg2); border: 1px solid var(--border); border-radius: 10px;
  width: 520px; max-width: 96vw; max-height: 88vh;
  display: flex; flex-direction: column; box-shadow: 0 24px 64px rgba(0,0,0,.6), 0 0 0 1px rgba(255,255,255,0.05) inset;
  background-image: linear-gradient(to bottom, rgba(255,255,255,0.03), transparent);
}
.modal-hdr {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 16px; border-bottom: 1px solid var(--border); flex-shrink: 0;
}
.modal-hdr span { font-size: 12px; font-weight: 600; }
.modal-hdr button {
  background: none; border: none; color: var(--muted); cursor: pointer;
  font-size: 16px; line-height: 1; transition: color .2s;
}
.modal-hdr button:hover { color: var(--text); }
.modal-body { flex: 1; overflow-y: auto; padding: 16px 20px; }
.modal-ftr {
  display: flex; align-items: center; gap: 10px; padding: 12px 16px;
  border-top: 1px solid var(--border); flex-shrink: 0; background: rgba(0,0,0,0.1);
  border-radius: 0 0 10px 10px;
}

/* ── Sizing modal ─────────────────────────────────────────────────────────── */
.modal-wide { max-width: 680px; width: 95%; }
.sz-summary { display: flex; gap: 1.2rem; padding: .4rem 0 .6rem; flex-wrap: wrap; border-bottom: 1px solid var(--border); margin-bottom: .6rem; }
.sz-kv { display: flex; flex-direction: column; gap: 4px; }
.sz-kv .k { font-size: 9px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; font-weight: 600; }
.sz-kv .v { font-size: 13px; font-weight: 600; font-family: var(--font-mono); }
.sz-table { width: 100%; border-collapse: collapse; font-size: 11px; margin: 8px 0 16px; }
.sz-table th { text-align: left; color: var(--muted); font-weight: 600; font-size: 9px; padding: 6px 8px; border-bottom: 1px solid var(--border); text-transform: uppercase; letter-spacing: .04em; }
.sz-table td { padding: 8px; border-bottom: 1px solid var(--border); vertical-align: middle; font-family: var(--font-mono); }
.tier-badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 9px; font-weight: 700; letter-spacing: .04em; font-family: var(--font-ui); }
.tier-H { background: rgba(239,68,68,0.15); color: var(--red); }
.tier-M { background: rgba(245,158,11,0.15); color: var(--amber); }
.tier-L { background: rgba(34,197,94,0.15); color: var(--green); }
.cov-bar { display: flex; align-items: center; gap: .5rem; }
.cov-bg { flex: 1; height: 4px; background: #27272a; border-radius: 2px; overflow: hidden; }
.cov-fill { height: 100%; border-radius: 2px; transition: width .3s; }
.cov-ok   { background: var(--green); }
.cov-warn { background: var(--amber); }
.cov-bad  { background: var(--red); }
.cov-lbl  { font-size: 10px; font-weight: 700; min-width: 3rem; text-align: right; }
.sz-recs { display: flex; flex-direction: column; gap: 6px; margin-top: 6px; }
.rec-item { display: flex; align-items: flex-start; gap: 8px; padding: 8px 12px; border-radius: 6px; font-size: 11px; line-height: 1.4; font-weight: 500; }
.rec-ok   { background: rgba(34,197,94,0.1); color: var(--green); border: 1px solid rgba(34,197,94,0.2); }
.rec-warn { background: rgba(245,158,11,0.1); color: var(--amber); border: 1px solid rgba(245,158,11,0.2); }
.rec-info { background: rgba(59,130,246,0.1); color: var(--blue); border: 1px solid rgba(59,130,246,0.2); }
.rec-err  { background: rgba(239,68,68,0.1); color: var(--red); border: 1px solid rgba(239,68,68,0.2); }
.rec-icon { flex-shrink: 0; font-size: 12px; }
.sz-section-lbl { font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin: 12px 0 6px; }

/* ── Toast notifications ──────────────────────────────────────────────────── */
#toast-stack {
  position: fixed; top: 54px; right: 16px; z-index: 800;
  display: flex; flex-direction: column; gap: 8px;
  pointer-events: none; width: 300px;
}
.toast {
  background: var(--glass-bg); backdrop-filter: blur(12px); border: 1px solid var(--border); border-radius: 6px;
  padding: 10px 12px 10px 0; display: flex; align-items: stretch; gap: 0;
  pointer-events: all; cursor: pointer; box-shadow: 0 8px 24px rgba(0,0,0,.4);
  animation: toastIn .25s cubic-bezier(0.4, 0, 0.2, 1); transition: opacity .3s, transform .3s;
}
.toast.out { opacity: 0; transform: translateX(20px); }
@keyframes toastIn { from { opacity:0; transform:translateX(20px) scale(0.95); } to { opacity:1; transform:none scale(1); } }
.toast-bar  { width: 3px; border-radius: 2px; flex-shrink: 0; margin: 0 10px 0 4px; }
.toast-icon { font-size: 16px; margin-right: 10px; flex-shrink: 0; display: flex; align-items: center; }
.toast-body { flex: 1; min-width: 0; display: flex; flex-direction: column; justify-content: center; }
.toast-title { font-size: 12px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.toast-sub   { font-size: 10px; color: var(--muted); margin-top: 3px; line-height: 1.4; font-family: var(--font-mono); }
.toast-close { color: var(--muted); font-size: 12px; align-self: flex-start; margin-left: 6px; padding: 2px; cursor: pointer; transition: color .2s; }
.toast-close:hover { color: var(--text); }
.t-green  .toast-bar { background: var(--green); box-shadow: 0 0 6px var(--green); }
.t-red    .toast-bar { background: var(--red); box-shadow: 0 0 6px var(--red); }
.t-amber  .toast-bar { background: var(--amber); box-shadow: 0 0 6px var(--amber); }
.t-indigo .toast-bar { background: var(--indigo); box-shadow: 0 0 6px var(--indigo); }
@media (max-width: 900px) { #toast-stack { top: auto; bottom: 54px; right: 8px; left: 8px; width: auto; } }

/* ── Config Inputs ────────────────────────────────────────────────────────── */
.cfg-section { margin-bottom: 20px; }
.cfg-section-title {
  font-size: 10px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase;
  color: var(--amber); margin-bottom: 10px; padding-bottom: 6px;
  border-bottom: 1px solid var(--border);
}
.cfg-row { display: grid; grid-template-columns: 1fr 140px; align-items: start; gap: 6px 12px; margin-bottom: 10px; }
.cfg-label { font-size: 12px; font-weight: 500; color: var(--text); padding-top: 4px; }
.cfg-hint  { font-size: 10px; color: var(--muted); margin-top: 2px; line-height: 1.3; }
.cfg-input, .cfg-select {
  background: var(--bg); border: 1px solid var(--border); border-radius: 4px;
  color: var(--text); font-family: var(--font-mono); font-size: 11px;
  padding: 6px 10px; outline: none; width: 100%; transition: all .2s;
  box-shadow: 0 1px 2px rgba(0,0,0,0.1) inset;
}
.cfg-input:focus, .cfg-select:focus { border-color: var(--amber); box-shadow: 0 0 0 2px rgba(245,158,11,0.2), 0 1px 2px rgba(0,0,0,0.1) inset; }
.cfg-input.err { border-color: var(--red); }
.cfg-row-err { font-size: 10px; color: var(--red); grid-column: 2; margin-top: -4px; }
#cfg-save {
  padding: 6px 18px; border-radius: 4px; font-family: var(--font-ui); font-size: 11px;
  font-weight: 600; cursor: pointer; text-transform: uppercase; letter-spacing: .05em;
  background: var(--amber); border: none; color: #000; transition: all .2s; box-shadow: 0 4px 10px rgba(245,158,11,0.3);
}
#cfg-save:hover { background: #fbbf24; transform: translateY(-1px); box-shadow: 0 6px 14px rgba(245,158,11,0.4); }
#cfg-save:disabled { opacity: .5; cursor: not-allowed; transform: none; box-shadow: none; }
#cfg-msg { font-size: 11px; font-weight: 500; flex: 1; text-align: right; }
#cfg-msg.ok  { color: var(--green); }
#cfg-msg.err { color: var(--red); }
.cfg-restart-note {
  font-size: 10px; font-weight: 500; color: var(--amber);
  background: rgba(245,158,11,.1); border: 1px solid rgba(245,158,11,.2);
  border-radius: 4px; padding: 8px 12px; margin-bottom: 12px; display: flex; align-items: center; gap: 6px;
}

/* ── Panel common ─────────────────────────────────────────────────────────── */
.panel { border-right: 1px solid var(--border); display: flex; flex-direction: column; overflow: hidden; background: var(--bg2); }
.panel-title {
  padding: 8px 12px; font-size: 10px; font-weight: 700; letter-spacing: .1em;
  text-transform: uppercase; color: var(--text); background: var(--glass-bg); backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border); flex-shrink: 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); position: relative; z-index: 10;
}
.scroll { overflow-y: auto; flex: 1; }

/* ── Scanner panel ────────────────────────────────────────────────────────── */
.sym-row {
  display: grid; grid-template-columns: 1fr 64px 24px;
  align-items: center; gap: 6px;
  padding: 6px 12px; border-bottom: 1px solid var(--border);
  cursor: pointer; transition: all .15s ease;
  animation: slideIn .2s ease;
}
.sym-row:hover { background: var(--bg3); }
.sym-row.active { background: rgba(255,255,255,0.05); box-shadow: inset 3px 0 0 var(--amber); }
.sym-row.signal { background: rgba(245,158,11,.1); border-left: 3px solid var(--amber); }
.sym-name { font-size: 12px; font-weight: 600; font-family: var(--font-mono); }
.sym-score-bar { position: relative; height: 4px; background: rgba(0,0,0,0.5); border-radius: 2px; overflow: hidden; box-shadow: inset 0 1px 2px rgba(0,0,0,0.3); }
.sym-score-fill { height: 100%; border-radius: 2px; background: linear-gradient(90deg, #f59e0b, #eab308); transition: width .3s cubic-bezier(0.4, 0, 0.2, 1); }
.sym-dir { font-size: 10px; font-weight: 700; text-align: right; font-family: var(--font-ui); }
.dir-long  { color: var(--green); }
.dir-short { color: var(--red); }
.sparkline-wrap { display: flex; align-items: center; justify-content: center; }
canvas.sparkline { display: block; filter: drop-shadow(0 1px 2px rgba(0,0,0,0.2)); width: 64px; height: 24px; }

/* ── Chart panel ──────────────────────────────────────────────────────────── */
#chart-panel { background: var(--bg); display: flex; flex-direction: column; overflow: hidden; }

/* Modern Pill Tabs */
#chart-tabs {
  display: flex; align-items: center; gap: 4px; padding: 6px 12px;
  background: var(--bg2); border-bottom: 1px solid var(--border); flex-shrink: 0;
}
#chart-tabs .ctab {
  display: flex; align-items: center; gap: 6px;
  padding: 4px 10px; border-radius: 16px; cursor: pointer;
  font-size: 10px; font-weight: 600; letter-spacing: .05em; text-transform: uppercase;
  color: var(--muted); border: 1px solid transparent; background: transparent; font-family: var(--font-ui);
  transition: all .2s cubic-bezier(0.4, 0, 0.2, 1);
}
#chart-tabs .ctab:hover { color: var(--text); background: rgba(255,255,255,0.05); }
#chart-tabs .ctab.active { color: var(--text); background: var(--bg3); border-color: var(--border); box-shadow: 0 1px 6px rgba(0,0,0,0.2); }
#chart-tabs .ctab-count { color: var(--amber); background: rgba(245,158,11,0.15); padding: 1px 5px; border-radius: 8px; font-size: 9px; }

#chart-pane, #hist-pane, #pos-chart-pane { flex: 1; overflow: hidden; display: none; flex-direction: column; }
#chart-pane.active, #hist-pane.active, #pos-chart-pane.active { display: flex; }

#chart-header {
  display: flex; align-items: center; gap: 12px; padding: 8px 16px;
  border-bottom: 1px solid var(--border); flex-shrink: 0; background: var(--glass-bg); backdrop-filter: blur(12px); position: relative; z-index: 10;
}
#chart-container, #pos-chart-container { flex: 1; position: relative; min-height: 0; }
.pos-chart-empty-state { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--muted); font-size: 11px; font-weight: 500; }
#pos-pnl-overlay { position: absolute; top: 16px; left: 60px; z-index: 10; pointer-events: none; text-shadow: 0 2px 8px rgba(0,0,0,0.8); }
#pos-pnl-overlay .pnl-val { font-size: 32px; font-weight: 700; font-family: var(--font-mono); letter-spacing: -1px; }
#pos-pnl-overlay .pnl-label { font-size: 10px; font-weight: 600; color: var(--muted); letter-spacing: .1em; margin-top: 4px; text-transform: uppercase; }

/* History pane */
#hist-pane { overflow: auto; position: relative; }
#hist-toolbar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 6px 12px; border-bottom: 1px solid var(--border);
  background: var(--bg2); flex-shrink: 0; gap: 10px; position: sticky; left: 0;
}
#hist-summary { font-size: 12px; font-weight: 500; color: var(--text); }
#btn-del-all {
  font-family: var(--font-ui); font-size: 10px; font-weight: 600; cursor: pointer; text-transform: uppercase; letter-spacing: .05em;
  padding: 4px 10px; border-radius: 4px; border: 1px solid var(--border); background: transparent; color: var(--muted); transition: all .2s;
}
#btn-del-all:hover { border-color: rgba(239,68,68,0.5); color: var(--red); background: rgba(239,68,68,0.1); transform: translateY(-1px); box-shadow: 0 2px 8px rgba(0,0,0,0.1); }

/* Unified Table styling (Sticky Symbol column) */
.hist-table, .sess-trade-tbl { width: max-content; min-width: 100%; border-collapse: collapse; font-family: var(--font-mono); }
.hist-table th, .sess-trade-tbl th {
  padding: 6px 12px; text-align: left; font-size: 10px; font-weight: 600; font-family: var(--font-ui);
  letter-spacing: .05em; color: var(--muted); text-transform: uppercase;
  background: var(--glass-bg); backdrop-filter: blur(12px); position: sticky; top: 0; z-index: 10;
  border-bottom: 1px solid var(--border); white-space: nowrap; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}
/* Sticky first column */
.hist-table th:first-child, .sess-trade-tbl th:first-child { position: sticky; left: 0; z-index: 11; border-right: 1px solid var(--border); }
.hist-table td:first-child, .sess-trade-tbl td:first-child { position: sticky; left: 0; z-index: 5; background: var(--bg); border-right: 1px solid var(--border); font-weight: 600; }
.hist-table tbody tr:hover td:first-child, .sess-trade-tbl tbody tr:hover td:first-child { background: var(--bg3); }

.hist-table th[data-col] { cursor: pointer; transition: color .2s; }
.hist-table th[data-col]:hover { color: var(--text); }
.hist-table th.sort-asc::after  { content: ' ▲'; color: var(--amber); }
.hist-table th.sort-desc::after { content: ' ▼'; color: var(--amber); }
.hist-table th:last-child, .sess-trade-tbl th:last-child { text-align: right; }
.hist-table td, .sess-trade-tbl td { padding: 6px 12px; font-size: 12px; border-bottom: 1px solid var(--border); white-space: nowrap; }
.hist-table td:last-child, .sess-trade-tbl td:last-child { text-align: right; }
.hist-table tbody tr, .sess-trade-tbl tbody tr { transition: background .15s; }
.hist-table tbody tr:hover, .sess-trade-tbl tbody tr:hover { background: var(--bg3); }
.hist-table tbody tr td:not(:last-child) { cursor: pointer; }
.btn-del-row { font-size: 12px; padding: 2px 6px; border-radius: 4px; border: none; background: transparent; color: var(--muted); cursor: pointer; transition: all .2s; }
.btn-del-row:hover { background: rgba(239,68,68,0.15); color: var(--red) !important; transform: scale(1.1); }

/* ── Session-grouped trade history ──────────────────────────────────────── */
#sessions-container { display: flex; flex-direction: column; flex-shrink: 0; }
.sess-block { border-bottom: 1px solid var(--border); }
.sess-hdr { display: flex; align-items: center; gap: 12px; padding: 8px 16px; cursor: pointer; user-select: none; transition: background .2s; }
.sess-hdr:hover { background: var(--bg3); }
.sess-arrow { font-size: 10px; color: var(--muted); transition: transform .2s; min-width: 14px; display: flex; align-items: center; justify-content: center; }
.sess-arrow.open { transform: rotate(90deg); color: var(--text); }
.sess-num  { font-size: 12px; font-weight: 600; color: var(--text); min-width: 80px; font-family: var(--font-ui); }
.sess-time { font-size: 11px; color: var(--muted); font-family: var(--font-mono); flex: 1; }
.sess-tc   { font-size: 11px; font-weight: 500; color: var(--muted); background: rgba(255,255,255,0.05); padding: 2px 8px; border-radius: 10px; font-family: var(--font-ui); }
.sess-exit-badge { border-radius: 4px; padding: 2px 8px; font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; font-family: var(--font-ui); }
.sess-exit-trail { background: rgba(245,158,11,.15); color: var(--amber); border: 1px solid rgba(245,158,11,0.3); }
.sess-exit-sl    { background: rgba(239,68,68,.15);  color: var(--red); border: 1px solid rgba(239,68,68,0.3); }
.sess-exit-tp    { background: rgba(34,197,94,.15);  color: var(--green); border: 1px solid rgba(34,197,94,0.3); }
.sess-pnl-pos { color: var(--green); font-weight: 700; font-size: 13px; font-family: var(--font-mono); }
.sess-pnl-neg { color: var(--red);   font-weight: 700; font-size: 13px; font-family: var(--font-mono); }
.sess-trades { overflow-x: auto; background: var(--bg); }
#ungrouped-section { margin-top: 12px; }
.ungrouped-label { padding: 8px 16px; font-size: 10px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; border-bottom: 1px solid var(--border); background: var(--bg2); font-family: var(--font-ui); }

#chart-symbol { font-size: 16px; font-weight: 700; font-family: var(--font-mono); }
#chart-price  { font-size: 16px; font-weight: 700; color: var(--green); font-family: var(--font-mono); }
#chart-change { font-size: 12px; font-weight: 500; font-family: var(--font-mono); }
#chart-change.pos { color: var(--green); }
#chart-change.neg { color: var(--red); }
.sym-search-wrap { display: flex; align-items: center; gap: 4px; }
#sym-search {
  background: rgba(0,0,0,0.3); border: 1px solid var(--border); border-radius: 4px;
  color: var(--text); font-family: var(--font-mono); font-size: 11px;
  padding: 4px 10px; width: 120px; outline: none; text-transform: uppercase;
  transition: all .2s; box-shadow: inset 0 1px 2px rgba(0,0,0,0.2);
}
#sym-search::placeholder { color: var(--muted); text-transform: none; font-family: var(--font-ui); }
#sym-search:focus { border-color: var(--amber); box-shadow: inset 0 1px 2px rgba(0,0,0,0.2), 0 0 0 2px rgba(245,158,11,0.2); }
#sym-search-btn {
  padding: 4px 10px; border-radius: 4px; font-family: var(--font-ui); font-size: 10px; font-weight: 600;
  background: var(--bg3); border: 1px solid var(--border); color: var(--text);
  cursor: pointer; transition: all .15s;
}
#sym-search-btn:hover { background: rgba(255,255,255,0.1); transform: translateY(-1px); box-shadow: 0 2px 8px rgba(0,0,0,0.2); }
.tf-bar { margin-left: auto; display: flex; gap: 4px; background: rgba(0,0,0,0.2); padding: 3px; border-radius: 6px; border: 1px solid var(--border); }
.tf-btn {
  padding: 3px 10px; border-radius: 3px; font-family: var(--font-ui); font-size: 10px; font-weight: 600;
  background: transparent; border: none; color: var(--muted);
  cursor: pointer; transition: all .15s;
}
.tf-btn:hover { color: var(--text); }
.tf-btn.active { background: var(--bg3); color: var(--text); box-shadow: 0 1px 4px rgba(0,0,0,0.2); }

/* ── Manual trade buttons ─────────────────────────────────────────────────── */
.btn-manual {
  padding: 4px 12px; border-radius: 4px; font-family: var(--font-ui); font-size: 11px;
  font-weight: 700; letter-spacing: .05em; cursor: pointer; border: 1px solid;
  transition: all .2s cubic-bezier(0.4, 0, 0.2, 1); text-transform: uppercase;
}
.btn-long  { background: rgba(34,197,94,.1);  border-color: rgba(34,197,94,0.4); color: var(--green); }
.btn-long:hover  { background: var(--green); border-color: var(--green); color: #000; transform: translateY(-1px); box-shadow: 0 4px 12px rgba(34,197,94,0.3); }
.btn-short { background: rgba(239,68,68,.1);  border-color: rgba(239,68,68,0.4);   color: var(--red); }
.btn-short:hover { background: var(--red); border-color: var(--red); color: #fff; transform: translateY(-1px); box-shadow: 0 4px 12px rgba(239,68,68,0.3); }
.btn-manual:disabled { opacity: .4; cursor: not-allowed; transform: none; box-shadow: none; }

/* ── Right panel ──────────────────────────────────────────────────────────── */
#right-panel { background: var(--bg); border-right: none; }

/* Open Position cards */
.pos-card {
  background: var(--bg2); margin: 8px 12px; border-radius: 8px; border: 1px solid var(--border);
  padding: 12px; cursor: pointer; transition: all .2s cubic-bezier(0.4, 0, 0.2, 1); animation: slideIn .2s ease;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}
.pos-card:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(0,0,0,0.2); border-color: rgba(255,255,255,0.15); }
.pos-card-header { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
.pos-card-sym { font-size: 13px; font-weight: 700; font-family: var(--font-mono); }
.pos-card-dir { display: inline-block; font-size: 9px; font-weight: 700; padding: 2px 6px; border-radius: 3px; letter-spacing: .08em; font-family: var(--font-ui); margin-top: 2px;}
.pos-dir-long  { background: rgba(34,197,94,.15);  color: var(--green); border: 1px solid rgba(34,197,94,0.3); }
.pos-dir-short { background: rgba(239,68,68,.15);   color: var(--red); border: 1px solid rgba(239,68,68,0.3); }
.trail-badge { font-family: var(--font-ui); font-size: 8px; font-weight: 700; padding: 2px 4px; border-radius: 3px; letter-spacing: .08em; background: rgba(245,158,11,.15); color: var(--amber); border: 1px solid rgba(245,158,11,.3); animation: trailPulse 2s ease-in-out infinite; margin-top: 2px; display: inline-block;}
@keyframes trailPulse { 0%, 100% { opacity: 1; box-shadow: 0 0 6px rgba(245,158,11,0.2); } 50% { opacity: .5; box-shadow: none; } }
.prox-badge { font-family: var(--font-ui); font-size: 8px; font-weight: 700; padding: 2px 4px; border-radius: 3px; letter-spacing: .06em; border: 1px solid; margin-top: 2px; display: inline-block;}
.pos-card-pnl  { font-size: 15px; font-weight: 700; font-family: var(--font-mono); text-align: right; }
.pos-card-roe  { font-size: 11px; font-weight: 600; font-family: var(--font-mono); text-align: right; margin-top: 2px; }
.pos-card-grid { display: flex; flex-direction: column; gap: 4px; }
.pos-kv { display: flex; align-items: baseline; justify-content: space-between; padding: 1px 0; border-bottom: 1px solid rgba(255,255,255,0.03); }
.pos-kv:last-child { border-bottom: none; }
.pos-kv .k   { font-size: 9px; font-weight: 500; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; font-family: var(--font-ui); }
.pos-kv .v   { font-size: 12px; font-weight: 600; font-family: var(--font-mono); display: flex; align-items: baseline; gap: 6px; }
.pos-kv .pct { font-size: 9px; font-weight: 500; padding: 1px 4px; border-radius: 3px; background: rgba(255,255,255,0.05); }
.pos-liq { color: var(--amber); }
.pnl-pos { color: var(--green); }
.pnl-neg { color: var(--red); }
.pnl-blink { animation: blink .5s cubic-bezier(0.4, 0, 0.2, 1); }
@keyframes blink { 0%,100%{opacity:1; transform:scale(1);} 50%{opacity:.4; transform:scale(1.05);} }

.hist-dir { font-size: 9px; font-weight: 700; padding: 2px 6px; border-radius: 3px; display: inline-block; font-family: var(--font-ui); letter-spacing: .05em; }
.hist-dir.long  { background: rgba(34,197,94,.15); color: var(--green); border: 1px solid rgba(34,197,94,0.3); }
.hist-dir.short { background: rgba(239,68,68,.15);  color: var(--red); border: 1px solid rgba(239,68,68,0.3); }
.hist-reason { font-size: 9px; padding: 2px 6px; border-radius: 3px; display: inline-block; font-weight: 600; letter-spacing: .05em; white-space: nowrap; font-family: var(--font-ui); box-shadow: 0 1px 2px rgba(0,0,0,0.1); }
.hr-sl      { background: rgba(239,68,68,.15);  color: var(--red); border: 1px solid rgba(239,68,68,0.3); }
.hr-trail   { background: rgba(245,158,11,.15); color: var(--amber); border: 1px solid rgba(245,158,11,0.3); }
.hr-tp      { background: rgba(34,197,94,.15);  color: var(--green); border: 1px solid rgba(34,197,94,0.3); }
.hr-manual  { background: rgba(99,102,241,.15); color: #818cf8; border: 1px solid rgba(99,102,241,0.3); }
.hr-pf-sl   { background: rgba(239,68,68,.25);  color: #f87171; border: 1px solid rgba(239,68,68,.5); }
.hr-pf-tp   { background: rgba(34,197,94,.25);  color: #4ade80; border: 1px solid rgba(34,197,94,.5); }
.hr-other   { background: rgba(255,255,255,.05); color: var(--muted); border: 1px solid var(--border); }

/* ── Stats bar ────────────────────────────────────────────────────────────── */
#stats-bar {
  display: flex; align-items: center; gap: 16px; padding: 0 16px;
  background: var(--bg2); border-top: 1px solid var(--border);
  font-size: 11px; font-weight: 500; color: var(--muted); flex-shrink: 0; overflow-x: auto;
  white-space: nowrap; box-shadow: 0 -2px 8px rgba(0,0,0,0.1); z-index: 20; position: relative;
}
#stats-bar .s { color: var(--text); font-weight: 600; font-family: var(--font-mono); margin-left: 4px; }
#stats-bar .sb-sep { color: rgba(255,255,255,0.1); user-select: none; }

/* ── Animations ───────────────────────────────────────────────────────────── */
@keyframes slideIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:none; } }

/* ── Empty states ─────────────────────────────────────────────────────────── */
.empty { padding: 24px 16px; color: var(--muted); text-align: center; font-size: 11px; font-weight: 500; font-family: var(--font-ui); }

/* ── Position stats summary ───────────────────────────────────────────────── */
#pos-stats { padding: 10px 16px; border-bottom: 1px solid var(--border); background: var(--bg2); flex-shrink: 0; box-shadow: 0 2px 8px rgba(0,0,0,0.1); position: relative; z-index: 5; }
.pos-stats-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.pos-stat-item { display: flex; flex-direction: column; gap: 2px; background: var(--bg); padding: 8px; border-radius: 6px; border: 1px solid var(--border); }
.pos-stat-item .k { font-size: 9px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; font-family: var(--font-ui);}
.pos-stat-item .v { font-size: 13px; font-weight: 700; font-family: var(--font-mono); }

/* ── Close buttons ────────────────────────────────────────────────────────── */
.btn-close-pos {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 4px 10px; border-radius: 4px; font-family: var(--font-ui); font-size: 10px;
  font-weight: 700; letter-spacing: .05em; cursor: pointer; border: 1px solid transparent;
  transition: all .2s; text-transform: uppercase;
  background: rgba(239,68,68,.1); color: var(--red);
}
.btn-close-pos:hover { background: var(--red); color: #fff; transform: translateY(-1px); box-shadow: 0 2px 8px rgba(239,68,68,0.3); }
.btn-close-pos:disabled { opacity: .5; cursor: not-allowed; transform: none; box-shadow: none; }

.btn-close-all {
  display: flex; align-items: center; gap: 4px;
  padding: 4px 10px; border-radius: 4px; font-family: var(--font-ui); font-size: 10px;
  font-weight: 700; letter-spacing: .05em; cursor: pointer; border: 1px solid rgba(239,68,68,0.4);
  transition: all .2s; text-transform: uppercase; margin-left: auto;
  background: rgba(239,68,68,.1); color: var(--red); white-space: nowrap;
}
.btn-close-all:hover { background: var(--red); color: #fff; transform: translateY(-1px); box-shadow: 0 2px 8px rgba(239,68,68,0.3); border-color: var(--red); }
.btn-close-all:disabled { opacity: .5; cursor: not-allowed; transform: none; box-shadow: none; }

/* ── Mobile tabs ──────────────────────────────────────────────────────────── */
#mobile-tabs {
  display: none; align-items: center; justify-content: space-around; padding: 6px 8px;
  background: var(--bg2); border-bottom: 1px solid var(--border); flex-shrink: 0; gap: 6px;
}
.mob-tab {
  flex: 1; padding: 8px 4px; text-align: center; font-size: 10px; font-weight: 700;
  letter-spacing: .05em; text-transform: uppercase; color: var(--muted);
  cursor: pointer; border: 1px solid transparent; background: transparent; font-family: var(--font-ui);
  border-radius: 16px; transition: all .2s;
}
.mob-tab.active { color: var(--text); background: var(--bg3); border-color: var(--border); box-shadow: 0 1px 4px rgba(0,0,0,0.2); }

/* ── Responsive layout ────────────────────────────────────────────────────── */
@media (max-width: 1100px) {
  #main { grid-template-columns: 220px 1fr 240px; }
  .sym-row { padding: 6px 8px; }
}
@media (max-width: 900px) {
  html, body { overflow: auto; }
  #app { grid-template-rows: 44px auto 1fr 32px; height: auto; min-height: 100vh; }
  #mobile-tabs { display: flex; }
  #main { grid-template-columns: 1fr; overflow: visible; }
  #scanner-panel { display: none; height: 50vh; min-height: 280px; }
  #chart-panel   { display: none; height: 60vh; min-height: 360px; }
  #right-panel   { display: none; min-height: 50vh; }
  #scanner-panel.mob-active, #chart-panel.mob-active, #right-panel.mob-active { display: flex; }
  #header { gap: 8px; padding: 6px 12px; flex-wrap: wrap; height: auto; min-height: 44px; }
  #server-time { display: none; }
  #stats-bar { flex-wrap: wrap; gap: 8px 16px; padding: 8px 12px; height: auto; line-height: 1.5; }
  #chart-header { flex-wrap: wrap; gap: 6px 10px; padding: 10px 12px; position: static; }
  .tf-bar { margin-left: 0; width: 100%; justify-content: space-between; }
  #right-panel { width: 100%; border-right: none; }
  .pos-stats-row { grid-template-columns: 1fr 1fr 1fr 1fr; gap: 8px; }
}
@media (max-width: 480px) {
  html, body { font-size: 10px; }
  .pos-stats-row { grid-template-columns: 1fr 1fr; }
  #chart-header { gap: 6px; }
  .sym-search-wrap { width: 100%; }
  #sym-search { width: 100%; flex: 1; }
  .tf-bar { width: 100%; justify-content: space-between; }
  .tf-btn { padding: 4px 6px; flex: 1; text-align: center; }
  .hist-table td, .sess-trade-tbl td { padding: 6px 8px; font-size: 11px; }
}
</style>"""

text = re.sub(r'<style>.*?</style>', new_style, text, flags=re.DOTALL)

with open(file_path, "w") as f:
    f.write(text)

print("CSS injected successfully!")
