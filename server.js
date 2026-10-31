'use strict';
// Super Shooter Stars (SSS) オンライン対戦サーバー(1ファイル版)
//  - index.html(ゲーム本体)を配信
//  - /ws でWebSocketの部屋中継を提供
//  - /healthz はRenderのヘルスチェック用
const http = require('http');
const fs = require('fs');
const path = require('path');

// 部屋(ルーム)の管理。通信方式に依存しないので、テストしやすい作りにしてある。
// 1部屋2人まで。各プレイヤーの「presence」(状態)を持ち、変更を相手へ中継する。

const BAD_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
const NAME_RE = /^[a-z0-9_-]{1,32}$/;
const MAX_PRESENCE_BYTES = 20000;

function createHub(opts) {
  const clock = (opts && opts.now) || Date.now; // テストで時計を差し替えられる
  const rooms = new Map(); // name -> Map(id -> {send, presence})
  let nextId = 1;

  function attach(sock) {
    let roomName = null;
    let id = null;
    let tokens = 80; // 1秒あたり60通まで(バースト80)
    let last = clock();

    const reply = (o) => {
      try { if (sock.isOpen()) sock.send(JSON.stringify(o)); } catch (e) { /* ignore */ }
    };
    const bcast = (room, o, exceptId) => {
      for (const [mid, m] of room) if (mid !== exceptId) m.send(o);
    };
    const peersMsg = (room) => ({
      t: 'peers',
      list: [...room].map(([mid, m]) => ({ id: mid, presence: m.presence })),
    });
    const fail = (msg) => { reply({ t: 'err', msg }); try { sock.close(); } catch (e) { /* ignore */ } };

    function leave() {
      if (!roomName) return;
      const room = rooms.get(roomName);
      if (room) {
        room.delete(id);
        if (room.size === 0) rooms.delete(roomName);
        else bcast(room, peersMsg(room));
      }
      roomName = null;
      id = null;
    }

    function onMessage(str) {
      const now = clock();
      tokens = Math.min(80, tokens + ((now - last) / 1000) * 60);
      last = now;
      if (--tokens < 0) return; // 送りすぎは無視
      let m;
      try { m = JSON.parse(str); } catch (e) { return; }
      if (!m || typeof m !== 'object') return;

      if (m.t === 'join' && !roomName) {
        const name = String(m.room || '');
        if (!NAME_RE.test(name)) return fail('bad');
        let room = rooms.get(name);
        if (m.create) {
          if (room) return fail('exists');
          room = new Map();
          rooms.set(name, room);
        } else if (!room) {
          return fail('notfound');
        }
        if (room.size >= 2) return fail('full');
        id = 'p' + nextId++;
        roomName = name;
        room.set(id, { send: reply, presence: {} });
        reply({ t: 'joined', id });
        bcast(room, peersMsg(room));
      } else if (m.t === 'p' && roomName) {
        const room = rooms.get(roomName);
        if (!room || !m.d || typeof m.d !== 'object' || Array.isArray(m.d)) return;
        const me = room.get(id);
        if (!me) return;
        const clean = {};
        for (const k of Object.keys(m.d)) if (!BAD_KEYS.has(k)) clean[k] = m.d[k];
        const merged = Object.assign({}, me.presence, clean);
        if (JSON.stringify(merged).length > MAX_PRESENCE_BYTES) return;
        me.presence = merged;
        bcast(room, { t: 'pu', id, d: clean }, id);
      } else if (m.t === 'leave') {
        leave();
      }
    }

    return { onMessage, onClose: leave };
  }

  return { attach, stats: () => ({ rooms: rooms.size }) };
}

// ---------------- HTTP + WebSocket ----------------
if (require.main === module) {
  const { WebSocketServer } = require('ws');
  const PORT = process.env.PORT || 3000;
  const INDEX = path.join(__dirname, 'index.html');
  const hub = createHub();

  const server = http.createServer((req, res) => {
    const p = req.url.split('?')[0];
    if (p === '/healthz') {
      res.writeHead(200, { 'content-type': 'text/plain' });
      return res.end('ok');
    }
    if (p === '/' || p === '/index.html') {
      return fs.readFile(INDEX, (err, data) => {
        if (err) { res.writeHead(500, { 'content-type': 'text/plain' }); return res.end('index.html not found'); }
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
        res.end(data);
      });
    }
    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  });

  const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 32 * 1024 });
  wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    const h = hub.attach({
      send: (s) => ws.send(s),
      close: () => ws.close(),
      isOpen: () => ws.readyState === 1,
    });
    ws.on('message', (d) => h.onMessage(d.toString()));
    ws.on('close', h.onClose);
    ws.on('error', () => {});
  });

  // 切れた接続の掃除(アイドル切断対策のpingも兼ねる)
  setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) { ws.terminate(); continue; }
      ws.isAlive = false;
      try { ws.ping(); } catch (e) { /* ignore */ }
    }
  }, 25000);

  server.listen(PORT, () => console.log('SSS server listening on', PORT));
}

module.exports = { createHub };
