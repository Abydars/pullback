import re

def update_ui():
    with open("frontend/index.html", "r") as f:
        html = f.read()

    # 1. Add CSS
    css = """
.an-insight-item { padding: 12px 16px; border-radius: 6px; margin-bottom: 12px; font-size: 13px; line-height: 1.5; border-left: 4px solid transparent; }
.an-insight-item.success { background: rgba(16, 185, 129, 0.1); border-left-color: var(--green); color: var(--green); }
.an-insight-item.warning { background: rgba(245, 158, 11, 0.1); border-left-color: var(--amber); color: var(--amber); }
.an-insight-item.danger  { background: rgba(239, 68, 68, 0.1); border-left-color: var(--red); color: var(--red); }
.an-insight-item.info    { background: rgba(59, 130, 246, 0.1); border-left-color: #3b82f6; color: #60a5fa; }
"""
    if ".an-insight-item" not in html:
        html = html.replace("</style>", css + "\n</style>")

    # 2. Add DOM element
    dom = """
          <div id="an-insights-container" style="margin-bottom: 24px;"></div>
          
          <div id="an-summary-grid">"""
    
    if 'id="an-insights-container"' not in html:
        html = html.replace('<div id="an-summary-grid">', dom)

    # 3. Add JS rendering logic
    js = """
  // Insights
  const insCont = document.getElementById('an-insights-container');
  if (d.insights && d.insights.length > 0) {
    insCont.innerHTML = d.insights.map(i => `<div class="an-insight-item ${i.type}"><b>Heuristic Scan:</b> ${i.message}</div>`).join('');
  } else {
    insCont.innerHTML = `<div class="an-insight-item info">Playbook scanning... requires more trades to generate statistical insights.</div>`;
  }

  // Summary"""

    if "const insCont = document.getElementById" not in html:
        html = html.replace('  // Summary', js)

    with open("frontend/index.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    update_ui()
