/**
 * МОРСКОЙ БОЙ — server.js  (переписан начисто)
 * Node.js + Express + Socket.io
 */
'use strict';

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const path       = require('path');
const crypto     = require('crypto');
const Database   = require('better-sqlite3');
const fs         = require('fs');

const PORT        = process.env.PORT    || 3000;
const DB_PATH     = process.env.DB_PATH || './data/game.db';
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';

const app    = express();
const server = http.createServer(app);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html'))
);

const io = new Server(server, {
  cors: { origin: CORS_ORIGIN, methods: ['GET', 'POST'] },
  pingTimeout:  30000,
  pingInterval: 10000,
});

if (!fs.existsSync('./data')) fs.mkdirSync('./data');
const db = new Database(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS players (
    id          TEXT PRIMARY KEY,
    name        TEXT,
    wins        INTEGER DEFAULT 0,
    losses      INTEGER DEFAULT 0,
    total_shots INTEGER DEFAULT 0,
    total_hits  INTEGER DEFAULT 0,
    updated_at  INTEGER DEFAULT (strftime('%s','now'))
  );
`);

function upsertPlayer(id, name) {
  db.prepare(`
    INSERT INTO players (id, name) VALUES (?, ?)
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, updated_at=strftime('%s','now')
  `).run(id, name || 'Игрок');
}
function addWin(id, shots, hits) {
  db.prepare(`UPDATE players SET wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+? WHERE id=?`).run(shots, hits, id);
}
function addLoss(id, shots, hits) {
  db.prepare(`UPDATE players SET losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+? WHERE id=?`).run(shots, hits, id);
}
function getLeaderboard() {
  return db.prepare(`SELECT id, name, wins, losses, total_shots, total_hits FROM players ORDER BY wins DESC LIMIT 50`).all();
}
function getPlayerStats(id) {
  return db.prepare(`SELECT * FROM players WHERE id=?`).get(id);
}

// rooms: Map<roomId, Room>
// waitingPool: [{socketId, playerId, name}]
const rooms       = new Map();
const waitingPool = [];

function makePlayer(info) {
  return { socketId: info.socketId, playerId: info.playerId, name: info.name, field: null, ready: false, shots: 0, hits: 0 };
}

function getPlayer(room, socketId) {
  if (room.p1?.socketId === socketId) return room.p1;
  if (room.p2?.socketId === socketId) return room.p2;
  return null;
}
function getOpponent(room, socketId) {
  if (room.p1?.socketId === socketId) return room.p2;
  if (room.p2?.socketId === socketId) return room.p1;
  return null;
}

function notifyBothMatched(room) {
  io.to(room.p1.socketId).emit('matched', {
    roomId:   room.id,
    opponent: { playerId: room.p2.playerId, name: room.p2.name },
  });
  io.to(room.p2.socketId).emit('matched', {
    roomId:   room.id,
    opponent: { playerId: room.p1.playerId, name: room.p1.name },
  });
}

io.on('connection', (socket) => {
  console.log(`[+] ${socket.id}`);

  socket.on('matchmake', ({ mode, roomId: friendRoomId, playerId, playerName }) => {
    if (!playerId) return;
    socket.data.playerId = playerId;
    upsertPlayer(playerId, playerName);

    const info = { socketId: socket.id, playerId, name: playerName };

    // ── СЛУЧАЙНЫЙ СОПЕРНИК ──────────────────────────
    if (mode === 'random') {
      const selfIdx = waitingPool.findIndex(p => p.playerId === playerId);
      if (selfIdx >= 0) waitingPool.splice(selfIdx, 1);

      const oppIdx = waitingPool.findIndex(p => p.playerId !== playerId);
      if (oppIdx >= 0) {
        const opp    = waitingPool.splice(oppIdx, 1)[0];
        const roomId = crypto.randomUUID();
        const room   = { id: roomId, p1: makePlayer(info), p2: makePlayer(opp), turn: playerId, started: false, over: false };
        rooms.set(roomId, room);
        socket.join(roomId);
        io.sockets.sockets.get(opp.socketId)?.join(roomId);
        notifyBothMatched(room);
      } else {
        waitingPool.push(info);
      }
    }

    // ── СОЗДАТЬ КОМНАТУ ДЛЯ ДРУГА ───────────────────
    else if (mode === 'friend_create') {
      const roomId = crypto.randomUUID();
      const room   = { id: roomId, p1: makePlayer(info), p2: null, turn: playerId, started: false, over: false };
      rooms.set(roomId, room);
      socket.join(roomId);
      socket.emit('room_created', { roomId });
      console.log(`Room created: ${roomId}`);
    }

    // ── ПОДКЛЮЧИТЬСЯ К ДРУГУ ────────────────────────
    else if (mode === 'friend_join') {
      const room = rooms.get(friendRoomId);
      if (!room) { socket.emit('error_msg', { message: 'Комната не найдена' }); return; }
      if (room.p2) { socket.emit('error_msg', { message: 'Комната заполнена' }); return; }

      room.p2 = makePlayer(info);
      socket.join(friendRoomId);
      notifyBothMatched(room);
      console.log(`Room joined: ${friendRoomId}`);
    }
  });

  // ── РАССТАНОВКА ─────────────────────────────────
  socket.on('place_ships', ({ roomId, field }) => {
    const room = rooms.get(roomId);
    if (!room) return;
    const player = getPlayer(room, socket.id);
    if (!player || player.ready) return;

    player.field = field;
    player.ready = true;

    const opp = getOpponent(room, socket.id);
    if (opp?.socketId) io.to(opp.socketId).emit('enemy_ready');

    if (room.p1.ready && room.p2?.ready) {
      room.started = true;
      room.turn    = room.p1.playerId;
      io.to(room.p1.socketId).emit('game_start', { isMyTurn: true });
      io.to(room.p2.socketId).emit('game_start', { isMyTurn: false });
      console.log(`Game started: ${roomId}`);
    }
  });

  // ── СДАЧА ────────────────────────────────────────
  socket.on('surrender', ({ roomId }) => {
    const room = rooms.get(roomId);
    if (!room || room.over) return;
    const surrenderer = getPlayer(room, socket.id);
    const winner      = getOpponent(room, socket.id);
    if (!surrenderer || !winner) return;
    room.over = true;
    // Победителю — победа немедленно
    io.to(winner.socketId).emit('opponent_surrendered');
    // Сдавшемуся — поражение (он сам это обработает)
    io.to(surrenderer.socketId).emit('surrender_confirmed');
    addWin(winner.playerId,      winner.shots,      winner.hits);
    addLoss(surrenderer.playerId, surrenderer.shots, surrenderer.hits);
  });

  // ── ВЫСТРЕЛ ──────────────────────────────────────
  socket.on('shoot', ({ roomId, r, c }) => {
    const room = rooms.get(roomId);
    if (!room || !room.started || room.over) return;

    const shooter = getPlayer(room, socket.id);
    const target  = getOpponent(room, socket.id);
    if (!shooter || !target) return;
    if (room.turn !== shooter.playerId) return;

    const cell = target.field?.[r]?.[c];
    if (cell === undefined || cell === 2 || cell === 3 || cell === 4) return;

    const hit  = cell === 1;
    target.field[r][c] = hit ? 2 : 3;
    shooter.shots++;
    if (hit) shooter.hits++;

    const sunk    = hit && checkSunkServer(target.field, r, c);
    const allGone = hit && !target.field.flat().includes(1);

    io.to(roomId).emit('shot_result', {
      r, c, hit, sunk,
      shooter:  shooter.playerId,
      gameOver: allGone,
      winner:   allGone ? shooter.playerId : null,
    });

    if (allGone) {
      room.over = true;
      addWin(shooter.playerId,  shooter.shots, shooter.hits);
      addLoss(target.playerId,  target.shots,  target.hits);
    } else if (!hit) {
      room.turn = target.playerId;
    }
  });

  // ── ОТКЛЮЧЕНИЕ ───────────────────────────────────
  socket.on('disconnect', () => {
    console.log(`[-] ${socket.id}`);
    const idx = waitingPool.findIndex(p => p.socketId === socket.id);
    if (idx >= 0) waitingPool.splice(idx, 1);

    for (const [roomId, room] of rooms) {
      if (room.over) continue;
      if (room.p1?.socketId === socket.id || room.p2?.socketId === socket.id) {
        io.to(roomId).emit('opponent_left');
        rooms.delete(roomId);
        break;
      }
    }
  });
});

function checkSunkServer(field, hitR, hitC) {
  const visited = new Set();
  const stack   = [[hitR, hitC]];
  const ship    = [];
  while (stack.length) {
    const [r, c] = stack.pop();
    const key = `${r},${c}`;
    if (visited.has(key)) continue;
    visited.add(key);
    const v = field[r]?.[c];
    if (v === 1 || v === 2) {
      ship.push([r, c]);
      for (const [nr, nc] of [[r-1,c],[r+1,c],[r,c-1],[r,c+1]])
        if (nr >= 0 && nr < 10 && nc >= 0 && nc < 10) stack.push([nr, nc]);
    }
  }
  return ship.length > 0 && ship.every(([r, c]) => field[r][c] === 2);
}

app.get('/api/leaderboard', (req, res) => {
  try   { res.json({ ok: true, data: getLeaderboard() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/api/stats/:id', (req, res) => {
  try   { res.json({ ok: true, data: getPlayerStats(req.params.id) || null }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
const bteship_bot = process.env.bteship_bot || ''; // e.g. 'my_battleship_bot'

app.get('/api/config', (req, res) => {
  res.json({ botUsername: bteship_bot });
});
app.get('/api/status', (req, res) => {
  res.json({ ok: true, rooms: rooms.size, waiting: waitingPool.length, uptime: process.uptime() });
});

server.listen(PORT, () => console.log(`\n🚢 http://localhost:${PORT}\n`));
module.exports = { app, server };
