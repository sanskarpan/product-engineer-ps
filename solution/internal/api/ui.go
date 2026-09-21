package api

import "net/http"

// Minimal list UI: enough for the demo, not a management dashboard
// (out of scope). Auto-refreshes; detail links carry full JSON.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><html><head><meta charset="utf-8">
<title>Durable Reminders</title>
<style>body{font-family:system-ui,sans-serif;max-width:1000px;margin:24px auto;padding:0 16px}
table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:6px 8px;font-size:13px}
code{background:#f4f4f4;padding:1px 4px;border-radius:4px}.pill{border-radius:999px;padding:1px 8px;font-size:12px}
.delivered{background:#dff5df}.failed,.cancelled{background:#fdd}.scheduled,.retrying{background:#fff3cd}.running{background:#cfe8ff}</style>
</head><body>
<h1>Durable Reminders</h1>
<p><code>POST /reminders</code> &middot; <code>GET /reminders?status=</code> &middot;
<code>GET /metrics</code> &middot; <code>POST /admin/clock {"advanceMs":N}</code></p>
<p><button onclick="load()">Refresh</button> <span id="ts"></span> <span id="clock"></span></p>
<table><thead><tr><th>id</th><th>content</th><th>tz</th><th>fire at</th><th>status</th><th>v</th><th>attempts</th></tr></thead>
<tbody id="rows"></tbody></table>
<script>
async function load(){
  const r = await fetch('/reminders'); const j = await r.json();
  const c = await (await fetch('/admin/clock')).json();
  document.getElementById('ts').textContent = 'updated ' + new Date().toLocaleTimeString();
  document.getElementById('clock').textContent = 'clock: ' + c.now + (c.manual ? ' (manual)' : '');
  var rows = (j.reminders||[]).map(function(m){
    return '<tr><td><a href="/reminders/' + encodeURIComponent(m.id) + '"><code>' + m.id + '</code></a></td>' +
    '<td>' + m.content + '</td><td>' + m.tz + '</td><td>' + m.fireAtUTC + '</td>' +
    '<td><span class="pill ' + m.status + '">' + m.status + '</span></td>' +
    '<td>' + m.version + '</td><td>' + m.attemptCount + '</td></tr>';
  }).join('') || '<tr><td colspan=7>no reminders yet</td></tr>';
  document.getElementById('rows').innerHTML = rows;
}
load(); setInterval(load, 1500);
</script></body></html>`))
}
