import re

def update_index():
    with open("frontend/index.html", "r") as f:
        html = f.read()

    # 1. Add Tab
    tab_html = """        <button class="ctab" data-ctab="hist-pane">
          Trade History <span class="ctab-count" id="hist-count"></span>
        </button>
        <button class="ctab" data-ctab="analytics-pane" onclick="fetchAnalytics()">Analytics</button>"""
    if "data-ctab=\"analytics-pane\"" not in html:
        html = re.sub(
            r'<button class="ctab" data-ctab="hist-pane">.*?<\/button>',
            tab_html,
            html,
            flags=re.DOTALL
        )
    
    # 2. Add Display Flex rule
    if "#analytics-pane { flex: 1" not in html:
        html = html.replace(
            "#chart-pane, #hist-pane, #pos-chart-pane",
            "#chart-pane, #hist-pane, #pos-chart-pane, #analytics-pane"
        )
    
    # 3. Add CSS
    css = """

/* --- Analytics Pane --- */
#analytics-pane { overflow: hidden; position: relative; }
#analytics-toolbar {
  padding: 12px 16px; border-bottom: 1px solid var(--border);
  display: flex; gap: 12px; align-items: center; background: rgba(0,0,0,0.2);
}
#analytics-toolbar input[type="date"] {
  background: var(--bg-hover); border: 1px solid var(--border);
  color: var(--text); padding: 4px 8px; border-radius: 4px; font-size: 13px;
}
#analytics-toolbar button {
  background: var(--bg-card); border: 1px solid var(--border);
  color: var(--text); padding: 5px 12px; border-radius: 4px; cursor: pointer;
  font-size: 12px; transition: 0.15s;
}
#analytics-toolbar button:hover { background: var(--bg-hover); border-color: var(--text-dim); }

#an-content { padding: 16px; overflow-y: auto; flex: 1; }
#an-summary-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  gap: 16px; margin-bottom: 24px;
}
.an-card {
  background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px;
  padding: 16px; display: flex; flex-direction: column; align-items: center; justify-content: center;
  box-shadow: 0 4px 12px rgba(0,0,0,0.2);
}
.an-title { font-size: 11px; text-transform: uppercase; color: var(--text-dim); letter-spacing: 0.5px; margin-bottom: 8px; }
.an-val { font-size: 24px; font-weight: 500; font-family: 'JetBrains Mono', monospace; }

.an-collapsible { margin-bottom: 12px; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
.an-toggle {
  width: 100%; text-align: left; background: var(--bg-card); color: var(--text);
  border: none; padding: 12px 16px; font-size: 14px; font-weight: 500; cursor: pointer;
  transition: 0.2s; display: flex; justify-content: space-between; align-items: center;
}
.an-toggle:hover { background: var(--bg-hover); }
.an-toggle::after { content: '\\25BC'; font-size: 10px; color: var(--text-dim); transition: transform 0.2s; }
.an-toggle.active { background: var(--bg-hover); border-bottom: 1px solid var(--border); }
.an-toggle.active::after { transform: rotate(180deg); }
.an-panel { display: none; padding: 16px; background: rgba(0,0,0,0.1); }
.an-panel.show { display: block; }

.an-bar-row { display: flex; align-items: center; margin-bottom: 10px; font-size: 13px; }
.an-bar-label { width: 120px; color: var(--text-dim); font-weight: 500; }
.an-bar-wrapper { flex: 1; margin: 0 16px; }
.an-bar-bg { width: 100%; height: 8px; background: var(--bg-hover); border-radius: 4px; overflow: hidden; display: flex; }
.an-bar-fill { height: 100%; transition: width 0.5s ease-out; }
.an-bar-stats { width: 180px; text-align: right; font-family: 'JetBrains Mono', monospace; font-size: 12px; }

.an-sym-list { font-family: 'JetBrains Mono', monospace; font-size: 13px; }
.an-sym-item { display: flex; justify-content: space-between; padding: 6px 8px; border-bottom: 1px solid var(--border); }
.an-sym-item:last-child { border-bottom: none; }
"""
    if "/* --- Analytics Pane --- */" not in html:
        html = html.replace("</style>", css + "\n</style>")

    # 4. Add DOM Pane
    dom = """
      <!-- Analytics pane -->
      <div id="analytics-pane">
        <div id="analytics-toolbar">
          <input type="date" id="an-start" />
          <span style="color:var(--text-dim)">&mdash;</span>
          <input type="date" id="an-end" />
          <button id="an-filter-btn" onclick="fetchAnalytics()">Filter</button>
          <button id="an-reset-btn" onclick="resetAnalytics()">Reset All Time</button>
        </div>
        
        <div id="an-content" class="scroll">
          <div id="an-loader" style="padding: 24px; color: var(--text-dim); text-align: center;">Loading analytics...</div>
          
          <div id="an-dash" style="display:none">
            <div id="an-summary-grid">
              <div class="an-card"><div class="an-title">Trades</div><div class="an-val" id="an-val-trades">0</div></div>
              <div class="an-card"><div class="an-title">Win Rate</div><div class="an-val" id="an-val-wr">0.0%</div></div>
              <div class="an-card"><div class="an-title">Total PnL</div><div class="an-val" id="an-val-pnl">$0.00</div></div>
              <div class="an-card"><div class="an-title">Avg PnL</div><div class="an-val" id="an-val-avg">$0.00</div></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle active">Strategy Performance</button>
              <div class="an-panel show" id="an-strategy-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Direction (Long/Short)</button>
              <div class="an-panel" id="an-direction-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Trading Sessions (UTC)</button>
              <div class="an-panel" id="an-session-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Day of Week</button>
              <div class="an-panel" id="an-day-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Hour of Day (UTC)</button>
              <div class="an-panel" id="an-hour-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Signal Score Buckets</button>
              <div class="an-panel" id="an-score-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Exit Reasons</button>
              <div class="an-panel" id="an-reason-container"></div>
            </div>

            <div class="an-collapsible">
              <button class="an-toggle">Top & Bottom Symbols</button>
              <div class="an-panel" id="an-symbols-container">
                 <div style="display:flex;gap:24px;flex-wrap:wrap">
                   <div style="flex:1;min-width:200px">
                     <div style="color:var(--text-dim);font-size:12px;margin-bottom:8px">TOP 10</div>
                     <div class="an-sym-list" id="an-top-sym"></div>
                   </div>
                   <div style="flex:1;min-width:200px">
                     <div style="color:var(--text-dim);font-size:12px;margin-bottom:8px">BOTTOM 10</div>
                     <div class="an-sym-list" id="an-bot-sym"></div>
                   </div>
                 </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    """
    if 'id="analytics-pane"' not in html:
        html = html.replace("</div>\n\n    <!-- Right Panel", dom + "\n    </div>\n\n    <!-- Right Panel")

    # 5. Add JS
    js = """
// ── Analytics ────────────────────────────────────────────────────────────────
let _anFetched = false;

function resetAnalytics() {
  document.getElementById('an-start').value = '';
  document.getElementById('an-end').value = '';
  fetchAnalytics();
}

async function fetchAnalytics() {
  const st = document.getElementById('an-start').value;
  const en = document.getElementById('an-end').value;
  let url = '/api/analytics';
  let params = [];
  if (st) params.push(`start=${new Date(st).getTime()}`);
  if (en) {
    let ed = new Date(en);
    ed.setHours(23, 59, 59, 999);
    params.push(`end=${ed.getTime()}`);
  }
  if (params.length > 0) url += '?' + params.join('&');

  document.getElementById('an-loader').style.display = 'block';
  document.getElementById('an-dash').style.display = 'none';

  try {
    const r = await fetch(url);
    if (!r.ok) throw new Error('API format completely dropped');
    const data = await r.json();
    renderAnalytics(data);
    _anFetched = true;
  } catch (e) {
    console.error(e);
  } finally {
    document.getElementById('an-loader').style.display = 'none';
    document.getElementById('an-dash').style.display = 'block';
  }
}

function _fmtAnPnl(v) {
  if (!v) return '$0.00';
  const c = v >= 0 ? 'var(--green)' : 'var(--red)';
  const s = v >= 0 ? '+' : '';
  return `<span style="color:${c}">${s}${v.toFixed(2)}</span>`;
}

function _buildBarRow(label, stats, maxPnl) {
  const tr = stats.trades || 0;
  if(tr === 0) return '';
  const wr = (stats.wins / tr * 100).toFixed(1);
  const pnl = stats.pnl || 0;
  
  // Bar uses WR for green fill and (100-WR) for red fill
  const w = parseFloat(wr);
  
  return `
    <div class="an-bar-row">
      <div class="an-bar-label">${label}</div>
      <div class="an-bar-wrapper">
        <div class="an-bar-bg">
          <div class="an-bar-fill" style="width:${w}%; background:var(--green)"></div>
          <div class="an-bar-fill" style="width:${100 - w}%; background:var(--red)"></div>
        </div>
      </div>
      <div class="an-bar-stats">
        <span style="color:var(--text-dim)">${tr}t | ${wr}% |</span> ${_fmtAnPnl(pnl)}
      </div>
    </div>
  `;
}

function renderAnalytics(d) {
  // Summary
  document.getElementById('an-val-trades').textContent = d.summary.total_trades;
  document.getElementById('an-val-wr').textContent = d.summary.win_rate.toFixed(1) + '%';
  document.getElementById('an-val-pnl').innerHTML = _fmtAnPnl(d.summary.total_pnl);
  document.getElementById('an-val-avg').innerHTML = _fmtAnPnl(d.summary.avg_pnl);

  // Helper
  const draw = (id, obj) => {
    const html = Object.entries(obj).map(([k,v]) => _buildBarRow(k, v, 1)).join('');
    document.getElementById(id).innerHTML = html || '<div class="empty">No data</div>';
  };

  draw('an-strategy-container', d.strategy);
  draw('an-direction-container', d.direction);
  draw('an-session-container', d.sessions);
  
  // Sort days correctly: Mon to Sun
  const days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  const domDay = days.map(day => _buildBarRow(day, d.day_of_week[day] || {trades:0, wins:0, pnl:0}, 1)).join('');
  document.getElementById('an-day-container').innerHTML = domDay || '<div class="empty">No data</div>';
  
  draw('an-hour-container', d.hour_of_day);
  draw('an-score-container', d.score_buckets);
  
  // Exit reasons - sort by trades
  const reasons = Object.entries(d.exit_reasons).sort((a,b) => b[1].trades - a[1].trades);
  const reasObj = Object.fromEntries(reasons);
  draw('an-reason-container', reasObj);

  // Symbols
  const st = (syms) => syms.map(x => `
    <div class="an-sym-item">
      <span>${x.symbol}</span>
      <span>${_fmtAnPnl(x.pnl)}</span>
    </div>
  `).join('');
  document.getElementById('an-top-sym').innerHTML = st(d.symbols.top_10) || '<div class="empty">No data</div>';
  document.getElementById('an-bot-sym').innerHTML = st(d.symbols.bottom_10) || '<div class="empty">No data</div>';
}

// Collapsible setup
document.querySelectorAll('.an-toggle').forEach(btn => {
  btn.addEventListener('click', function() {
    this.classList.toggle('active');
    const p = this.nextElementSibling;
    p.classList.toggle('show');
  });
});

// Update chart tab listener to fetch analytics on first open
document.querySelectorAll('.ctab').forEach(b => {
  b.addEventListener('click', e => {
    const t = e.target.getAttribute('data-ctab');
    if(t === 'analytics-pane' && !_anFetched) {
      fetchAnalytics();
    }
  });
});
"""
    if "function resetAnalytics()" not in html:
        html = html.replace("</script>\n</body>", js + "\n</script>\n</body>")
        
    with open("frontend/index.html", "w") as f:
        f.write(html)
        
if __name__ == "__main__":
    update_index()
