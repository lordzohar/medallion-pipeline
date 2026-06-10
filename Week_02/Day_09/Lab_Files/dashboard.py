"""
Live Taxi Dashboard
===================
Flask + Socket.IO web app that shows:
  - Real-time taxi positions on a Leaflet map of NYC
  - Surge zones color-coded by multiplier
  - Live trip counter + revenue
  - Recent trip events stream

Open: http://localhost:5000

Reads `gps-pings`, `taxi-trips`, `surge-events` from Kafka and
pushes them to the browser over websockets.
"""

from __future__ import annotations

import json
import logging
import os
import threading

from confluent_kafka import Consumer
from flask import Flask, render_template_string
from flask_socketio import SocketIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [dash] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9095")

app = Flask(__name__)
app.config["SECRET_KEY"] = "taxi-dev"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>NYC Taxi Pipeline - Live</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js"></script>
  <style>
    body { font-family: -apple-system, Segoe UI, sans-serif; margin: 0; background: #0e1117; color: #eee; }
    #header { padding: 12px 20px; background: #1c2129; border-bottom: 1px solid #333; display: flex; gap: 24px; align-items: center; flex-wrap: wrap; }
    #header h1 { margin: 0; font-size: 18px; color: #ffcc00; }
    .stat { font-size: 13px; }
    .stat b { color: #4fd1c5; font-size: 18px; display: block; }
    #main { display: flex; height: calc(100vh - 280px); }
    #map { flex: 2; }
    #side { flex: 1; background: #14171d; overflow-y: auto; border-left: 1px solid #333; }
    .panel { padding: 10px 14px; border-bottom: 1px solid #333; }
    .panel h3 { margin: 0 0 8px 0; font-size: 13px; color: #ffcc00; }
    .panel table { width: 100%; border-collapse: collapse; font-size: 11px; font-family: monospace; }
    .panel th { text-align: left; color: #888; font-weight: normal; border-bottom: 1px solid #333; padding: 2px 4px; }
    .panel td { padding: 2px 4px; border-bottom: 1px solid #20242c; }
    .reasons { margin-top: 6px; font-size: 11px; color: #ff9999; font-family: monospace; }
    #events { height: 130px; overflow-y: scroll; background: #14171d; padding: 8px 16px; font-family: monospace; font-size: 11px; border-top: 1px solid #333; }
    .ev-trip { color: #4fd1c5; }
    .ev-surge { color: #ffcc00; }
    .ev-dlq { color: #ff6b6b; }
    .surge-badge { display:inline-block; background:#ff6b6b; color:#fff; border-radius:4px; padding:2px 6px; font-size:11px; }
  </style>
</head>
<body>
  <div id="header">
    <h1>🚕 NYC Taxi Pipeline — Live Monitoring</h1>
    <div class="stat">Active Drivers <b id="m_drivers">0</b></div>
    <div class="stat">Trips/min <b id="m_tpm">0</b></div>
    <div class="stat">Revenue (live) <b id="m_rev">$0</b></div>
    <div class="stat">Surge Zones <b id="m_surge">0</b></div>
    <div class="stat">DLQ Total <b id="m_dlq">0</b></div>
    <div class="stat">Quality Score <b id="m_quality">100%</b></div>
  </div>
  <div id="main">
    <div id="map"></div>
    <div id="side">
      <div class="panel">
        <h3>🚨 Dead Letter Queue — recent bad records</h3>
        <table id="dlq_table">
          <thead><tr><th>Time</th><th>Reason</th><th>Trip</th><th>Driver</th></tr></thead>
          <tbody></tbody>
        </table>
        <div class="reasons" id="reason_summary"></div>
      </div>
      <div class="panel">
        <h3>✅ Quality Validator (Great Expectations)</h3>
        <table id="quality_table">
          <thead><tr><th>Rule</th><th>Pass</th><th>Fail</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>
  <div id="events"></div>

<script>
const map = L.map('map').setView([40.74, -73.96], 12);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19, attribution: '© OSM'
}).addTo(map);

const zones = {
  TIMES_SQUARE:  [40.7580, -73.9855],
  MIDTOWN:       [40.7549, -73.9840],
  WALL_STREET:   [40.7074, -74.0113],
  JFK_AIRPORT:   [40.6413, -73.7781],
  BROOKLYN:      [40.6782, -73.9442],
  HARLEM:        [40.8116, -73.9465],
};

const zoneCircles = {};
for (const [name, ll] of Object.entries(zones)) {
  zoneCircles[name] = L.circle(ll, {
    radius: 1200, color: '#4fd1c5', weight: 1, fillOpacity: 0.05
  }).addTo(map).bindTooltip(name);
}

const driverMarkers = {};
const taxiIcon = L.divIcon({className:'', html:'🚕', iconSize:[20,20]});

const socket = io();
let dlqCount = 0;
let tripsInLastMinute = [];
let totalRevenue = 0;
let reasonCounts = {};
let totalSeen = 0;
let totalFailed = 0;

socket.on('gps', (ping) => {
  let m = driverMarkers[ping.driver_id];
  if (!m) {
    m = L.marker([ping.lat, ping.lon], {icon: taxiIcon}).addTo(map);
    driverMarkers[ping.driver_id] = m;
  } else {
    m.setLatLng([ping.lat, ping.lon]);
  }
  m.setOpacity(ping.state === 'ON_TRIP' ? 1.0 : 0.45);
  document.getElementById('m_drivers').textContent = Object.keys(driverMarkers).length;
});

socket.on('trip', (t) => {
  totalRevenue += t.total_amount || 0;
  document.getElementById('m_rev').textContent = '$' + totalRevenue.toFixed(0);
  tripsInLastMinute.push(Date.now());
  pushEvent('ev-trip', `TRIP ${t.trip_id?.slice(0,12)} ${t.pickup_zone}→${t.dropoff_zone}  $${t.total_amount}  surge=${t.surge_multiplier}x`);
});

socket.on('surge', (s) => {
  const c = zoneCircles[s.zone];
  if (c) {
    const color = s.surge_multiplier >= 2.0 ? '#ff3333' :
                  s.surge_multiplier >= 1.5 ? '#ff9933' :
                  s.surge_multiplier >= 1.2 ? '#ffcc00' : '#4fd1c5';
    c.setStyle({color, fillColor: color, fillOpacity: 0.15 + (s.surge_multiplier-1)*0.2});
    c.bindTooltip(`${s.zone} — ${s.surge_multiplier}x (${s.trips_per_min}/min)`);
  }
  if (s.surge_multiplier > 1.0) {
    pushEvent('ev-surge', `SURGE ${s.zone} ${s.surge_multiplier}x  demand=${s.trips_per_min} supply=${s.idle_drivers}`);
  }
});

socket.on('dlq', (d) => {
  dlqCount++;
  document.getElementById('m_dlq').textContent = dlqCount;
  pushEvent('ev-dlq', `DLQ  ${d._dlq_reason}  trip=${d.trip_id}`);

  // Maintain table of recent DLQ rows
  const tbody = document.querySelector('#dlq_table tbody');
  const row = document.createElement('tr');
  row.innerHTML = `<td>${new Date().toLocaleTimeString().slice(0,8)}</td>
                   <td style="color:#ff9999">${d._dlq_reason || '?'}</td>
                   <td>${(d.trip_id || '').slice(0,12)}</td>
                   <td>${d.driver_id || '?'}</td>`;
  tbody.insertBefore(row, tbody.firstChild);
  while (tbody.childNodes.length > 12) tbody.removeChild(tbody.lastChild);

  // Update reason tally
  reasonCounts[d._dlq_reason] = (reasonCounts[d._dlq_reason] || 0) + 1;
  document.getElementById('reason_summary').textContent =
    Object.entries(reasonCounts).sort((a,b)=>b[1]-a[1])
      .map(([k,v]) => `${k}:${v}`).join('  ');

  // Update overall quality score
  totalSeen += 1;
  totalFailed += 1;
  updateQuality();
});

socket.on('clean', (c) => {
  totalSeen += 1;
  updateQuality();
});

function updateQuality() {
  if (totalSeen === 0) return;
  const pct = ((totalSeen - totalFailed) / totalSeen * 100).toFixed(1);
  document.getElementById('m_quality').textContent = pct + '%';

  // Per-rule table (counts by rule)
  const tbody = document.querySelector('#quality_table tbody');
  tbody.innerHTML = '';
  const allRules = new Set([...Object.keys(reasonCounts), 'overall']);
  for (const rule of allRules) {
    const fail = reasonCounts[rule] || 0;
    const pass = rule === 'overall' ? (totalSeen - totalFailed) : 0;
    const row = document.createElement('tr');
    row.innerHTML = `<td>${rule}</td><td style="color:#4fd1c5">${pass}</td><td style="color:#ff9999">${fail}</td>`;
    tbody.appendChild(row);
  }
}

function pushEvent(cls, txt){
  const el = document.getElementById('events');
  const line = document.createElement('div');
  line.className = cls;
  line.textContent = new Date().toLocaleTimeString() + ' — ' + txt;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
  while (el.childNodes.length > 200) el.removeChild(el.firstChild);
}

setInterval(() => {
  const cutoff = Date.now() - 60000;
  tripsInLastMinute = tripsInLastMinute.filter(t => t > cutoff);
  document.getElementById('m_tpm').textContent = tripsInLastMinute.length;
  const surging = Object.values(zoneCircles).filter(c => c.options.color !== '#4fd1c5').length;
  document.getElementById('m_surge').textContent = surging;
}, 1000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(INDEX_HTML)


@app.route("/health")
def health():
    return {"status": "ok"}


def consume_topic(topic: str, event_name: str, group: str):
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "latest",
    })
    c.subscribe([topic])
    log.info("subscribed %s -> emit '%s'", topic, event_name)
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error():
            continue
        try:
            data = json.loads(msg.value())
            socketio.emit(event_name, data)
        except Exception as e:
            log.warning("emit err: %s", e)


def start_consumers():
    threads = [
        threading.Thread(target=consume_topic, args=("gps-pings", "gps", "dash-gps"), daemon=True),
        threading.Thread(target=consume_topic, args=("trips-clean", "clean", "dash-clean"), daemon=True),
        threading.Thread(target=consume_topic, args=("taxi-trips", "trip", "dash-trip"), daemon=True),
        threading.Thread(target=consume_topic, args=("surge-events", "surge", "dash-surge"), daemon=True),
        threading.Thread(target=consume_topic, args=("trips-dlq", "dlq", "dash-dlq"), daemon=True),
    ]
    for t in threads:
        t.start()


if __name__ == "__main__":
    start_consumers()
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
