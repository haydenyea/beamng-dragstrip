/**
 * BeamNG Drag Strip — Backend Server
 * Single-port HTTP + WebSocket (upgrade) — compatible with Railway, Render, Fly.io
 *
 * Install: npm install ws
 * Run:     node server.js
 *
 * On hosting platforms the PORT env var is set automatically.
 * WebSocket connects to the same URL as HTTP, using the /ws path:
 *   wss://your-app.railway.app/ws   (hosting platform)
 *   ws://localhost:3000/ws          (local dev)
 */

const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

// Hosting platforms inject PORT; fall back to 3000 for local dev
const PORT      = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'runs.json');

// ──────────────────────────────────────────────
// Persistent storage
// ──────────────────────────────────────────────
function loadRuns() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    }
  } catch (e) { console.error('Load error:', e.message); }
  return [];
}

function saveRuns(runs) {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(runs, null, 2));
  } catch (e) { console.error('Save error:', e.message); }
}

let runs = loadRuns();

// ──────────────────────────────────────────────
// WebSocket — attached to the HTTP server (single port)
// Clients connect to:  ws(s)://your-host/ws
// ──────────────────────────────────────────────
const wss = new WebSocketServer({ noServer: true });
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[WS] Client connected — ${clients.size} total`);

  // Send full state immediately on connect
  ws.send(JSON.stringify({ type: 'init', runs }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[WS] Client disconnected — ${clients.size} remaining`);
  });

  ws.on('error', (e) => console.error('[WS] Error:', e.message));
});

function broadcast(payload, exclude = null) {
  const data = JSON.stringify(payload);
  for (const c of clients) {
    if (c !== exclude && c.readyState === WebSocket.OPEN) {
      c.send(data);
    }
  }
}

// ──────────────────────────────────────────────
// Message handlers
// ──────────────────────────────────────────────
function handleMessage(ws, msg) {
  switch (msg.type) {

    case 'submit_run': {
      const { driver, car, time, speed } = msg;
      if (!driver || !car || typeof time !== 'number' || typeof speed !== 'number') {
        ws.send(JSON.stringify({ type: 'error', text: 'Invalid run data' }));
        return;
      }

      const carKey = car.trim().toLowerCase();
      const existIdx = runs.findIndex(r => r.carKey === carKey);
      const existing = existIdx >= 0 ? runs[existIdx] : null;
      const now = new Date();

      const newRun = {
        id: Date.now(),
        driver: driver.trim().slice(0, 32),
        car: car.trim().slice(0, 40),
        carKey,
        time: Math.round(time * 1000) / 1000,
        speed: Math.round(speed * 10) / 10,
        date: now.toLocaleDateString('en-AU', { day:'2-digit', month:'short', year:'2-digit' }),
        timestamp: now.toISOString()
      };

      let isRecord = false;
      if (!existing || newRun.time < existing.time) {
        if (existIdx >= 0) runs.splice(existIdx, 1);
        runs.push(newRun);
        saveRuns(runs);
        isRecord = !existing || newRun.time < existing.time;

        // Tell submitter
        ws.send(JSON.stringify({ type: 'run_accepted', run: newRun, isRecord }));
        // Tell everyone else
        broadcast({ type: 'run_update', run: newRun, isRecord, runs }, ws);
      } else {
        ws.send(JSON.stringify({
          type: 'run_rejected',
          text: `Existing best (${existing.time}s) still stands!`,
          existing
        }));
      }
      break;
    }

    // In-game: driver started a run
    case 'run_start': {
      broadcast({ type: 'run_start', driver: msg.driver, car: msg.car }, ws);
      break;
    }

    // In-game: live telemetry during a run
    case 'telemetry': {
      broadcast({ type: 'telemetry', driver: msg.driver, car: msg.car, speed: msg.speed, elapsed: msg.elapsed }, ws);
      break;
    }

    // In-game: run completed
    case 'run_complete': {
      // Same logic as submit_run but triggered from game
      handleMessage(ws, { ...msg, type: 'submit_run' });
      break;
    }

    case 'get_runs': {
      ws.send(JSON.stringify({ type: 'init', runs }));
      break;
    }

    default:
      console.log('[WS] Unknown message type:', msg.type);
  }
}

// ──────────────────────────────────────────────
// HTTP API (for admin UI / REST fallback)
// ──────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  // GET /runs — return all runs
  if (req.method === 'GET' && url.pathname === '/runs') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(runs));
    return;
  }

  // GET /top5 — return top 5 by ET
  if (req.method === 'GET' && url.pathname === '/top5') {
    const top5 = [...runs].sort((a, b) => a.time - b.time).slice(0, 5);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(top5));
    return;
  }

  // POST /runs — submit a run via HTTP
  if (req.method === 'POST' && url.pathname === '/runs') {
    let body = '';
    req.on('data', d => { body += d; });
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        // Re-use WS handler logic via a fake ws object
        const fakeWs = {
          send: (payload) => {
            const p = JSON.parse(payload);
            if (p.type === 'run_accepted') {
              res.writeHead(201, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: true, run: p.run, isRecord: p.isRecord }));
            } else if (p.type === 'run_rejected') {
              res.writeHead(409, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: false, text: p.text }));
            } else if (p.type === 'error') {
              res.writeHead(400, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ ok: false, text: p.text }));
            }
          },
          readyState: WebSocket.OPEN
        };
        handleMessage(fakeWs, { type: 'submit_run', ...data });
      } catch {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: false, text: 'Invalid JSON' }));
      }
    });
    return;
  }

  // DELETE /runs/:carKey — remove a car's record
  if (req.method === 'DELETE' && url.pathname.startsWith('/runs/')) {
    const carKey = decodeURIComponent(url.pathname.split('/runs/')[1]).toLowerCase();
    const before = runs.length;
    runs = runs.filter(r => r.carKey !== carKey);
    if (runs.length < before) {
      saveRuns(runs);
      broadcast({ type: 'run_deleted', carKey, runs });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, text: 'Not found' }));
    }
    return;
  }

  // Serve static files from server-ui/
  const staticDir = path.join(__dirname, 'server-ui');
  let filePath = path.join(staticDir, url.pathname === '/' ? 'index.html' : url.pathname);
  const ext = path.extname(filePath);
  const mime = { '.html':'text/html', '.js':'text/javascript', '.css':'text/css', '.json':'application/json' };
  if (fs.existsSync(filePath)) {
    res.writeHead(200, { 'Content-Type': mime[ext] || 'text/plain' });
    res.end(fs.readFileSync(filePath));
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// ──────────────────────────────────────────────
// HTTP → WebSocket upgrade on /ws
// This is how a single port serves both HTTP and WS.
// Hosting platforms proxy all traffic through one port,
// and forward the Upgrade header automatically.
// ──────────────────────────────────────────────
httpServer.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  if (url.pathname === '/ws') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

httpServer.listen(PORT, () => {
  console.log(`\n🏁  BeamNG Drag Strip Server`);
  console.log(`    Admin UI  → http://localhost:${PORT}`);
  console.log(`    REST API  → http://localhost:${PORT}/runs`);
  console.log(`    WebSocket → ws://localhost:${PORT}/ws`);
  console.log(`    Data file → ${DATA_FILE}`);
  console.log(`\n    On Railway/Render/Fly.io:`);
  console.log(`    WebSocket → wss://YOUR-APP-URL/ws`);
  console.log(`    (replace ws:// with wss:// and localhost with your app domain)\n`);
});
