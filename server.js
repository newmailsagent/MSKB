/**
 * МОРСКОЙ БОЙ — server.js
 * п.4: гости не пишутся в БД, старые удаляются
 * п.5: disconnect во время игры = победа оставшемуся
 * п.6: таймер хода 60с, 2 просрочки = поражение
 */
'use strict';

const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const path       = require('path');
const crypto     = require('crypto');
const Database   = require('better-sqlite3');
const fs         = require('fs');

const PORT        = process.env.PORT        || 3000;
const DB_PATH     = process.env.DB_PATH     || './data/game.db';
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';
const BOT_USERNAME = process.env.BOT_USERNAME || '';

const TURN_TIMEOUT_MS = 60000; // 60 сек на ход
const MAX_TIMEOUTS    = 2;     // 2 просрочки = поражение
const WARN_AT_MS      = 40000; // предупреждение за 20 сек (на 40-й секунде)

const app    = express();
const server = http.createServer(app);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const io = new Server(server, {
  cors: { origin: CORS_ORIGIN, methods: ['GET', 'POST'] },
  pingTimeout:  20000, // 20 сек — быстрее детектируем разрыв
  pingInterval: 8000,  // ping каждые 8 сек
  connectTimeout: 10000,
});

if (!fs.existsSync('./data')) fs.mkdirSync('./data');
const db = new Database(DB_PATH);
db.exec(`
  CREATE TABLE IF NOT EXISTS players (
    id           TEXT PRIMARY KEY,
    name         TEXT,
    wins         INTEGER DEFAULT 0,
    losses       INTEGER DEFAULT 0,
    total_shots  INTEGER DEFAULT 0,
    total_hits   INTEGER DEFAULT 0,
    online_wins  INTEGER DEFAULT 0,
    online_losses INTEGER DEFAULT 0,
    online_shots INTEGER DEFAULT 0,
    online_hits  INTEGER DEFAULT 0,
    updated_at   INTEGER DEFAULT (strftime('%s','now'))
  );
`);

// Добавляем новые колонки если их нет (миграция)
try { db.exec(`ALTER TABLE players ADD COLUMN rating_active INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN rating_since  INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN rated_wins    INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN rated_losses  INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN rated_shots   INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN rated_hits    INTEGER DEFAULT 0`); } catch(e) {}
// Удаляем старые: (миграция — колонки уже есть)
try { db.exec(`ALTER TABLE players ADD COLUMN online_wins    INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN online_losses  INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN online_shots   INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE players ADD COLUMN online_hits    INTEGER DEFAULT 0`); } catch(e) {}

// Чистим гостей
try { db.prepare(`DELETE FROM players WHERE id LIKE 'guest_%'`).run(); } catch(e) {}

function upsertPlayer(id, name) {
  if (id?.startsWith('guest_')) return;
  db.prepare(`
    INSERT INTO players (id, name) VALUES (?, ?)
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, updated_at=strftime('%s','now')
  `).run(id, name || 'Игрок');
}

function addWin(id, shots, hits, isOnline = false) {
  if (id?.startsWith('guest_')) return;
  const player = db.prepare(`SELECT rating_active FROM players WHERE id=?`).get(id);
  const isRated = isOnline && player?.rating_active === 1;
  if (isOnline) {
    db.prepare(`UPDATE players SET
      wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+?,
      online_wins=online_wins+1, online_shots=online_shots+?, online_hits=online_hits+?,
      rated_wins=rated_wins+${isRated?1:0}, rated_shots=rated_shots+${isRated?'?':0}, rated_hits=rated_hits+${isRated?'?':0},
      updated_at=strftime('%s','now') WHERE id=?
    `).run(shots, hits, shots, hits, ...(isRated ? [shots, hits] : []), id);
  } else {
    db.prepare(`UPDATE players SET wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+?,
      updated_at=strftime('%s','now') WHERE id=?`).run(shots, hits, id);
  }
}

function addLoss(id, shots, hits, isOnline = false) {
  if (id?.startsWith('guest_')) return;
  const player = db.prepare(`SELECT rating_active FROM players WHERE id=?`).get(id);
  const isRated = isOnline && player?.rating_active === 1;
  if (isOnline) {
    db.prepare(`UPDATE players SET
      losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+?,
      online_losses=online_losses+1, online_shots=online_shots+?, online_hits=online_hits+?,
      rated_losses=rated_losses+${isRated?1:0}, rated_shots=rated_shots+${isRated?'?':0}, rated_hits=rated_hits+${isRated?'?':0},
      updated_at=strftime('%s','now') WHERE id=?
    `).run(shots, hits, shots, hits, ...(isRated ? [shots, hits] : []), id);
  } else {
    db.prepare(`UPDATE players SET losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+?,
      updated_at=strftime('%s','now') WHERE id=?`).run(shots, hits, id);
  }
}

// Рейтинг: только rated_ (матчи когда игрок участвовал), анти-бот фильтр
function getRating() {
  return db.prepare(`
    SELECT id, name, rating_active,
      rated_wins, rated_losses, rated_shots, rated_hits,
      online_wins, online_losses, online_shots, online_hits,
      CASE
        WHEN rated_shots > 0 THEN ROUND(CAST(rated_hits AS REAL) / rated_shots, 3)
        ELSE 0
      END AS accuracy,
      CASE
        WHEN rated_shots > 0 THEN
          CAST(rated_wins AS REAL) *
          MAX(0, 1.0 - MAX(0, CAST(rated_hits AS REAL)/rated_shots - 0.6) * 5.0)
        ELSE 0
      END AS rating_score
    FROM players
    WHERE rating_active = 1 AND rated_wins + rated_losses >= 1
    ORDER BY rating_score DESC, rated_wins DESC
    LIMIT 50
  `).all();
}

// Вступить в рейтинг
function joinRating(id) {
  if (id?.startsWith('guest_')) return { ok: false };
  db.prepare(`UPDATE players SET rating_active=1, rating_since=strftime('%s','now'),
    rated_wins=0, rated_losses=0, rated_shots=0, rated_hits=0
    WHERE id=?`).run(id);
  return { ok: true };
}

// Покинуть рейтинг
function leaveRating(id) {
  if (id?.startsWith('guest_')) return { ok: false };
  db.prepare(`UPDATE players SET rating_active=0,
    rated_wins=0, rated_losses=0, rated_shots=0, rated_hits=0
    WHERE id=?`).run(id);
  return { ok: true };
}

function getPlayerStats(id) {
  return db.prepare(`SELECT * FROM players WHERE id=?`).get(id);
}

const rooms       = new Map();
const waitingPool = [];

function makePlayer(info) {
  return {
    socketId: info.socketId,
    playerId: info.playerId,
    name:     info.name,
    field:    null,
    ready:    false,
    shots:    0,
    hits:     0,
    timeouts: 0, // п.6: счётчик просрочек
  };
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
  io.to(room.p1.socketId).emit('matched', { roomId: room.id, opponent: { playerId: room.p2.playerId, name: room.p2.name } });
  io.to(room.p2.socketId).emit('matched', { roomId: room.id, opponent: { playerId: room.p1.playerId, name: room.p1.name } });
}

// п.6: запустить таймер хода для комнаты
function startTurnTimer(room) {
  clearTurnTimer(room);

  // Предупреждение на 40-й секунде (за 20 до конца)
  room._warnTimer = setTimeout(() => {
    const currentTurnPlayer = room.turn === room.p1.playerId ? room.p1 : room.p2;
    if (currentTurnPlayer?.socketId) {
      io.to(currentTurnPlayer.socketId).emit('turn_warning', { secondsLeft: 20 });
    }
  }, WARN_AT_MS);

  // Истечение таймера
  room._turnTimer = setTimeout(() => {
    if (room.over) return;
    const timedOutPlayer = room.turn === room.p1.playerId ? room.p1 : room.p2;
    const otherPlayer    = room.turn === room.p1.playerId ? room.p2 : room.p1;
    if (!timedOutPlayer || !otherPlayer) return;

    timedOutPlayer.timeouts++;
    io.to(room.id).emit('turn_timeout', {
      playerId:  timedOutPlayer.playerId,
      timeouts:  timedOutPlayer.timeouts,
    });

    if (timedOutPlayer.timeouts >= MAX_TIMEOUTS) {
      // 2 просрочки — поражение
      room.over = true;
      io.to(room.id).emit('game_over_timeout', {
        winner: otherPlayer.playerId,
        loser:  timedOutPlayer.playerId,
      });
      addWin(otherPlayer.playerId,  otherPlayer.shots,  otherPlayer.hits,  true);
      addLoss(timedOutPlayer.playerId, timedOutPlayer.shots, timedOutPlayer.hits, true);
    } else {
      // 1 просрочка — просто передаём ход
      room.turn = otherPlayer.playerId;
      io.to(room.p1.socketId).emit('turn', { isMyTurn: room.turn === room.p1.playerId });
      io.to(room.p2.socketId).emit('turn', { isMyTurn: room.turn === room.p2.playerId });
      startTurnTimer(room);
    }
  }, TURN_TIMEOUT_MS);
}

function clearTurnTimer(room) {
  if (room._turnTimer)  { clearTimeout(room._turnTimer);  room._turnTimer  = null; }
  if (room._warnTimer)  { clearTimeout(room._warnTimer);  room._warnTimer  = null; }
}

io.on('connection', (socket) => {
  console.log(`[+] ${socket.id}`);

  socket.on('matchmake', ({ mode, roomId: friendRoomId, playerId, playerName }) => {
    if (!playerId) return;
    socket.data.playerId = playerId;
    upsertPlayer(playerId, playerName);

    const info = { socketId: socket.id, playerId, name: playerName };

    if (mode === 'random') {
      const selfIdx = waitingPool.findIndex(p => p.playerId === playerId);
      if (selfIdx >= 0) waitingPool.splice(selfIdx, 1);

      const oppIdx = waitingPool.findIndex(p => p.playerId !== playerId);
      if (oppIdx >= 0) {
        const opp    = waitingPool.splice(oppIdx, 1)[0];
        const roomId = crypto.randomUUID();
        const room   = { id: roomId, p1: makePlayer(info), p2: makePlayer(opp), turn: playerId, started: false, over: false, _turnTimer: null, _warnTimer: null };
        rooms.set(roomId, room);
        socket.join(roomId);
        io.sockets.sockets.get(opp.socketId)?.join(roomId);
        notifyBothMatched(room);
      } else {
        waitingPool.push(info);
      }
    }
    else if (mode === 'friend_create') {
      const roomId = crypto.randomUUID();
      const room   = { id: roomId, p1: makePlayer(info), p2: null, turn: playerId, started: false, over: false, _turnTimer: null, _warnTimer: null };
      rooms.set(roomId, room);
      socket.join(roomId);
      socket.emit('room_created', { roomId });
    }
    else if (mode === 'friend_join') {
      const room = rooms.get(friendRoomId);
      if (!room) { socket.emit('error_msg', { message: 'Комната не найдена' }); return; }
      if (room.p2) { socket.emit('error_msg', { message: 'Комната заполнена' }); return; }
      room.p2 = makePlayer(info);
      socket.join(friendRoomId);
      notifyBothMatched(room);
    }
  });

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
      startTurnTimer(room); // п.6: запускаем таймер
    }
  });

  socket.on('shoot', ({ roomId, r, c }) => {
    const room = rooms.get(roomId);
    if (!room || !room.started || room.over) return;

    const shooter = getPlayer(room, socket.id);
    const target  = getOpponent(room, socket.id);
    if (!shooter || !target) return;
    if (room.turn !== shooter.playerId) return;

    const cell = target.field?.[r]?.[c];
    if (cell === undefined || cell === 2 || cell === 3 || cell === 4) return;

    // Ход выполнен — сбрасываем таймер
    clearTurnTimer(room);
    // Отменяем предупреждение если было
    io.to(shooter.socketId).emit('turn_warning_cancel');

    const hit     = cell === 1;
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
      addWin(shooter.playerId,  shooter.shots, shooter.hits, true);
      addLoss(target.playerId,  target.shots,  target.hits,  true);
    } else {
      if (!hit) room.turn = target.playerId;
      // Запускаем таймер для следующего хода
      startTurnTimer(room);
    }
  });

  // п.5: сдача
  socket.on('surrender', ({ roomId }) => {
    const room = rooms.get(roomId);
    if (!room || room.over) return;
    const surrenderer = getPlayer(room, socket.id);
    const winner      = getOpponent(room, socket.id);
    if (!surrenderer || !winner) return;
    room.over = true;
    clearTurnTimer(room);
    io.to(winner.socketId).emit('opponent_surrendered');
    io.to(surrenderer.socketId).emit('surrender_confirmed');
    addWin(winner.playerId,      winner.shots,      winner.hits,      true);
    addLoss(surrenderer.playerId, surrenderer.shots, surrenderer.hits, true);
  });

  // п.5: отключение во время игры = победа оставшемуся
  socket.on('disconnect', () => {
    console.log(`[-] ${socket.id}`);
    const idx = waitingPool.findIndex(p => p.socketId === socket.id);
    if (idx >= 0) waitingPool.splice(idx, 1);

    for (const [roomId, room] of rooms) {
      if (room.over) continue;
      if (room.p1?.socketId === socket.id || room.p2?.socketId === socket.id) {
        clearTurnTimer(room);
        room.over = true;

        const leaver  = room.p1?.socketId === socket.id ? room.p1 : room.p2;
        const stayer  = room.p1?.socketId === socket.id ? room.p2 : room.p1;

        if (stayer?.socketId && room.started) {
          // Игра уже шла — победа тому, кто остался
          io.to(stayer.socketId).emit('opponent_disconnected_win');
          addWin(stayer.playerId,  stayer.shots,  stayer.hits,  true);
          addLoss(leaver.playerId, leaver.shots,  leaver.hits,  true);
        } else {
          // Игра ещё не началась — просто уведомляем
          if (stayer?.socketId) io.to(stayer.socketId).emit('opponent_left');
        }
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

app.get('/api/config',     (req, res) => res.json({ botUsername: BOT_USERNAME }));
app.get('/api/leaderboard',(req, res) => { try { res.json({ ok: true, data: getRating() }); } catch(e) { res.status(500).json({ ok: false, error: e.message }); } });
app.get('/api/rating',    (req, res) => { try { res.json({ ok: true, data: getRating() }); } catch(e) { res.status(500).json({ ok: false, error: e.message }); } });
app.post('/api/rating/join',  (req, res) => { try { res.json(joinRating(req.body.id));  } catch(e) { res.status(500).json({ ok: false }); } });
app.post('/api/rating/leave', (req, res) => { try { res.json(leaveRating(req.body.id)); } catch(e) { res.status(500).json({ ok: false }); } });
app.get('/api/stats/:id',  (req, res) => { try { res.json({ ok: true, data: getPlayerStats(req.params.id)||null }); } catch(e) { res.status(500).json({ ok: false, error: e.message }); } });
app.get('/api/status',     (req, res) => res.json({ ok: true, rooms: rooms.size, waiting: waitingPool.length, uptime: process.uptime() }));

server.listen(PORT, () => console.log(`\n🚢 http://localhost:${PORT}\n`));
module.exports = { app, server };
