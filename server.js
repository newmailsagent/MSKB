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
const helmet     = require('helmet');
const rateLimit  = require('express-rate-limit');

const PORT        = process.env.PORT        || 3000;
const DB_PATH     = process.env.DB_PATH     || './data/game.db';
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';
const BOT_USERNAME = process.env.BOT_USERNAME || '';
const APP_NAME     = process.env.APP_NAME     || 'bteship';
const BOT_TOKEN    = process.env.BOT_TOKEN    || '';
const SHOP_SECRET  = process.env.SHOP_SECRET  || 'shop_secret_change_me'; // для внутренних наград
const ADMIN_IDS    = new Set((process.env.ADMIN_IDS || '').split(',').map(s => s.trim()).filter(Boolean));

function isAdmin(userId) { return ADMIN_IDS.has(String(userId)); }

const TURN_TIMEOUT_MS = 60000; // 60 сек на ход
const MAX_TIMEOUTS    = 2;     // 2 просрочки = поражение
const WARN_AT_MS      = 40000; // предупреждение за 20 сек (на 40-й секунде)

const app    = express();
const server = http.createServer(app);
app.set('trust proxy', 1); // за nginx-прокси — для корректного rate limiting

// ─── SECURITY MIDDLEWARE ──────────────────────────────────────────────────────

// HTTP security headers (CSP, X-Frame-Options, X-Content-Type-Options и др.)
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc:  ["'self'"],
      scriptSrc:   ["'self'", "'unsafe-inline'", "https://telegram.org", "https://cdn.jsdelivr.net", "https://fonts.googleapis.com", "https://cdnjs.cloudflare.com", "https://mc.yandex.ru", "https://yastatic.net"],
      styleSrc:    ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com", "https://cdnjs.cloudflare.com"],
      fontSrc:     ["'self'", "https://fonts.gstatic.com"],
      imgSrc:      ["'self'", "data:", "https:", "https://mc.yandex.ru"],
      connectSrc:  ["'self'", "wss:", "ws:", "https://telegram.org", "https://mc.yandex.ru", "https://yandex.ru"],
      frameSrc:    ["'none'"],
      objectSrc:   ["'none'"],
    },
  },
  crossOriginEmbedderPolicy: false, // Telegram WebApp требует отключить
}));

// Ограничение размера тела запроса — защита от payload-бомб
app.use(express.json({ limit: '64kb' }));

// Rate limiting — общий для всех API
const apiLimiter = rateLimit({
  windowMs: 60 * 1000,       // 1 минута
  max: 120,                  // 120 запросов в минуту с одного IP
  standardHeaders: true,
  legacyHeaders: false,
  message: { ok: false, error: 'Too many requests' },
  skip: (req) => req.path === '/api/online' || req.path === '/api/config', // счётчик онлайна не лимитируем
});

// Жёсткий лимит для чувствительных эндпоинтов
const strictLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  message: { ok: false, error: 'Too many requests' },
});

app.use('/api/', apiLimiter);
app.use('/api/stats/reset',       strictLimiter);
app.use('/api/admin/analytics',   strictLimiter);
app.use('/api/webhook/telegram',  strictLimiter);
app.use('/api/shop/purchase',     strictLimiter);

app.use(express.static(path.join(__dirname, 'public')));
// ВАЖНО: app.get('*') регистрируется В КОНЦЕ, после всех API-маршрутов

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
// XP система
try { db.exec(`ALTER TABLE players ADD COLUMN xp INTEGER DEFAULT 0`); } catch(e) {}

// Таблица истории боёв
db.exec(`
  CREATE TABLE IF NOT EXISTS battle_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id  TEXT NOT NULL,
    result     TEXT NOT NULL,
    opponent   TEXT,
    shots      INTEGER DEFAULT 0,
    hits       INTEGER DEFAULT 0,
    date       INTEGER DEFAULT (strftime('%s','now')),
    mode       TEXT DEFAULT 'online'
  );
`);
try { db.exec(`ALTER TABLE battle_history ADD COLUMN mode TEXT DEFAULT 'online'`); } catch(e) {}
try { db.exec(`ALTER TABLE shop_items ADD COLUMN photo_url_tg TEXT`); } catch(e) {}

// ─── УВЕДОМЛЕНИЯ ─────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS notifications (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    title   TEXT NOT NULL,
    body    TEXT NOT NULL,
    date    INTEGER DEFAULT (strftime('%s','now')),
    active  INTEGER DEFAULT 1
  );
`);

// GET /api/notification — вернуть последнее активное уведомление
app.get('/api/notification', (req, res) => {
  try {
    const row = db.prepare(`SELECT * FROM notifications WHERE active=1 ORDER BY date DESC LIMIT 1`).get();
    res.json({ ok: true, data: row || null });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// POST /api/notification — создать/обновить уведомление (только через SHOP_SECRET)
app.post('/api/notification', (req, res) => {
  try {
    const secret = req.headers['x-admin-secret'] || req.body.secret;
    if (!SHOP_SECRET || secret !== SHOP_SECRET) return res.status(403).json({ ok: false, error: 'forbidden' });
    const { title, body } = req.body;
    if (!title || !body) return res.json({ ok: false, error: 'title and body required' });
    // Деактивируем старые
    db.prepare(`UPDATE notifications SET active=0`).run();
    // Создаём новое
    const result = db.prepare(`INSERT INTO notifications (title, body) VALUES (?, ?)`).run(
      String(title).slice(0, 128),
      String(body).slice(0, 1024)
    );
    res.json({ ok: true, id: result.lastInsertRowid });
  } catch(e) { console.error('notif post error:', e); res.status(500).json({ ok: false }); }
});

// ─── МАГАЗИН ──────────────────────────────────────────────────────────────────

// Каталог товаров
db.exec(`
  CREATE TABLE IF NOT EXISTS shop_items (
    id           TEXT PRIMARY KEY,
    type         TEXT NOT NULL,      -- 'frame'|'theme'|'reaction'|'title'
    name         TEXT NOT NULL,
    description  TEXT,
    price_stars  INTEGER,            -- null = бесплатный/наградной
    preview_url  TEXT,
    sort_order   INTEGER DEFAULT 0,
    is_active    INTEGER DEFAULT 1   -- 0 = скрыт из магазина
  );
`);

// Инвентарь игрока
db.exec(`
  CREATE TABLE IF NOT EXISTS inventory (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id            TEXT NOT NULL,
    item_id            TEXT NOT NULL REFERENCES shop_items(id),
    purchase_type      TEXT NOT NULL,  -- 'stars'|'reward'|'admin'
    telegram_charge_id TEXT,           -- payload от TG для обработки рефандов
    purchased_at       INTEGER DEFAULT (strftime('%s','now')),
    refunded_at        INTEGER,        -- заполняется при рефанде
    is_active          INTEGER DEFAULT 1,  -- 0 = заблокирован после рефанда
    UNIQUE(user_id, item_id)
  );
`);

// Экипировка — что сейчас надето по слотам
db.exec(`
  CREATE TABLE IF NOT EXISTS equipped (
    user_id  TEXT NOT NULL,
    slot     TEXT NOT NULL,   -- 'frame'|'theme'|'reaction'|'title'
    item_id  TEXT NOT NULL REFERENCES shop_items(id),
    PRIMARY KEY (user_id, slot)
  );
`);

// Pending invoices — ждём подтверждения от TG
db.exec(`
  CREATE TABLE IF NOT EXISTS pending_invoices (
    payload      TEXT PRIMARY KEY,    -- уникальный payload который мы шлём в TG
    user_id      TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    price_stars  INTEGER NOT NULL,
    created_at   INTEGER DEFAULT (strftime('%s','now')),
    status       TEXT DEFAULT 'pending'  -- 'pending'|'paid'|'failed'
  );
`);

// Seed — базовые товары если таблица пустая
const itemCount = db.prepare('SELECT COUNT(*) as c FROM shop_items').get().c;
if (itemCount === 0) {
  const seedItems = [
    {
      id:          'theme_light',
      type:        'theme',
      name:        'Светлая тема',
      description: 'Светлая цветовая схема',
      price_stars: 100,
      preview_url: '/shop/previews/theme/frame_theme_white.png',
      sort_order:  10,
    },
  ];
  const insertItem = db.prepare(`INSERT OR IGNORE INTO shop_items (id,type,name,description,price_stars,preview_url,sort_order) VALUES (?,?,?,?,?,?,?)`);
  for (const it of seedItems) insertItem.run(it.id, it.type, it.name, it.description, it.price_stars, it.preview_url, it.sort_order);
  console.log('[Shop] Seed items inserted');
}

// ── Миграции товаров магазина ──────────────────────────────────────────────

// Добавить theme_black если нет
try {
  db.prepare(`INSERT OR IGNORE INTO shop_items (id,type,name,description,price_stars,preview_url,sort_order,is_active)
    VALUES ('theme_black','theme','Чёрная тема (контрастная)','Максимально тёмная цветовая схема — чистый чёрный',100,'/shop/previews/theme/frame_theme_black.png',20,1)`).run();
} catch(e) { console.error('[Shop] migration theme_black:', e.message); }

// Обновить превью и название светлой темы
try {
  db.prepare(`UPDATE shop_items SET
    preview_url='/shop/previews/theme/frame_theme_white.png',
    photo_url_tg='/shop/previews/theme/frame_theme_white.png'
    WHERE id='theme_light'`).run();
} catch(e) {}

// Обновить название и превью тёмной темы (по умолчанию)
try {
  db.prepare(`UPDATE shop_items SET
    name='Тёмная тема (по умолчанию)',
    preview_url='/shop/previews/theme/frame_theme_dark.png',
    photo_url_tg='/shop/previews/theme/frame_theme_dark.png'
    WHERE id='theme_dark'`).run();
} catch(e) {}

// Обновить название и photo_url_tg чёрной темы
try {
  db.prepare(`UPDATE shop_items SET
    name='Чёрная тема (контрастная)',
    photo_url_tg='/shop/previews/theme/frame_theme_black.png'
    WHERE id='theme_black'`).run();
} catch(e) {}

// Хелперы магазина
function getInventory(userId) {
  return db.prepare(`
    SELECT i.*, s.type, s.name, s.description, s.preview_url,
           e.slot IS NOT NULL as is_equipped
    FROM inventory i
    JOIN shop_items s ON s.id = i.item_id
    LEFT JOIN equipped e ON e.user_id = i.user_id AND e.item_id = i.item_id
    WHERE i.user_id = ? AND i.is_active = 1
    ORDER BY i.purchased_at DESC
  `).all(userId);
}

function getEquipped(userId) {
  const rows = db.prepare(`SELECT slot, item_id FROM equipped WHERE user_id = ?`).all(userId);
  const result = {};
  for (const r of rows) result[r.slot] = r.item_id;
  return result;
}

function hasItem(userId, itemId) {
  return !!db.prepare(`SELECT 1 FROM inventory WHERE user_id=? AND item_id=? AND is_active=1`).get(userId, itemId);
}

function grantItem(userId, itemId, purchaseType, chargeId = null) {
  try {
    db.prepare(`
      INSERT OR IGNORE INTO inventory (user_id, item_id, purchase_type, telegram_charge_id)
      VALUES (?, ?, ?, ?)
    `).run(userId, itemId, purchaseType, chargeId);
    return true;
  } catch(e) {
    console.error('[Shop] grantItem error:', e.message);
    return false;
  }
}

// Чистим гостей
try { db.prepare(`DELETE FROM players WHERE id LIKE 'guest_%'`).run(); } catch(e) {}

// Миграция: нормализуем float-ID (364966070.0 → 364966070)
try {
  const floatPlayers = db.prepare(`SELECT * FROM players WHERE id LIKE '%.%'`).all();
  for (const fp of floatPlayers) {
    const normalId = String(parseInt(fp.id, 10));
    const existing = db.prepare(`SELECT * FROM players WHERE id=?`).get(normalId);
    if (existing) {
      // Мержим статистику в правильную запись
      db.prepare(`UPDATE players SET
        wins=wins+?, losses=losses+?, total_shots=total_shots+?, total_hits=total_hits+?,
        online_wins=online_wins+?, online_losses=online_losses+?,
        online_shots=online_shots+?, online_hits=online_hits+?,
        rated_wins=rated_wins+?, rated_losses=rated_losses+?,
        rated_shots=rated_shots+?, rated_hits=rated_hits+?
        WHERE id=?`).run(
          fp.wins, fp.losses, fp.total_shots, fp.total_hits,
          fp.online_wins, fp.online_losses, fp.online_shots, fp.online_hits,
          fp.rated_wins, fp.rated_losses, fp.rated_shots, fp.rated_hits,
          normalId
        );
      // Переносим историю
      db.prepare(`UPDATE battle_history SET player_id=? WHERE player_id=?`).run(normalId, fp.id);
    } else {
      // Переименовываем запись
      db.prepare(`UPDATE players SET id=? WHERE id=?`).run(normalId, fp.id);
      db.prepare(`UPDATE battle_history SET player_id=? WHERE player_id=?`).run(normalId, fp.id);
    }
    db.prepare(`DELETE FROM players WHERE id=?`).run(fp.id);
    console.log(`[DB] Merged player ${fp.id} → ${normalId}`);
  }
} catch(e) { console.error('[DB] Migration error:', e.message); }

// Онлайн-игроки: socketId -> {playerId, name, connectedAt}
const onlineSessions = new Map(); // socketId → session
const IDLE_TIMEOUT_MS = 30 * 60 * 1000; // 30 минут

function getOnlineCount() {
  const now = Date.now();
  const seen = new Set();
  for (const [, s] of onlineSessions) {
    if (now - s.lastActive > IDLE_TIMEOUT_MS) continue;
    // Не считаем сокет, который подключился но ещё не прислал identify
    // (у него нет playerId и он моложе 5 секунд — ещё в процессе handshake)
    const ageSec = (now - s.connectedAt) / 1000;
    if (!s.playerId && !s.identified && ageSec < 5) continue;

    if (s.playerId && !s.playerId.startsWith('guest_')) {
      // Зарегистрированный: дедупликация по playerId
      seen.add('p:' + s.playerId);
    } else if (s.playerId && s.playerId.startsWith('guest_')) {
      // Гость с известным guest_id: дедупликация по guest_id
      seen.add('g:' + s.playerId);
    } else {
      // Анонимный сокет (до identify или без него): по socketId
      seen.add('s:' + s.socketId);
    }
  }
  return seen.size;
}
function broadcastOnlineCount() { io.emit('online_count', { count: getOnlineCount() }); }

// Обновляем lastActive для сокета
function touchSession(socketId) {
  const s = onlineSessions.get(socketId);
  if (s) s.lastActive = Date.now();
}

// Периодически чистим idle и обновляем счётчик
setInterval(() => {
  const now = Date.now();
  let changed = false;
  for (const [sid, s] of onlineSessions) {
    if (now - s.lastActive > IDLE_TIMEOUT_MS) { onlineSessions.delete(sid); changed = true; }
  }
  if (changed) broadcastOnlineCount();
}, 5 * 60 * 1000); // каждые 5 минут

function normalizeId(id) {
  if (!id || String(id).startsWith('guest_')) return id;
  const n = String(id);
  // Убираем дробную часть если есть (364966070.0 → 364966070)
  return n.includes('.') ? String(parseInt(n, 10)) : n;
}

function upsertPlayer(id, name) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return;
  db.prepare(`
    INSERT INTO players (id, name) VALUES (?, ?)
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, updated_at=strftime('%s','now')
  `).run(id, name || 'Игрок');
}

function addBattleHistory(id, result, opponentName, shots, hits, mode = 'online') {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return;
  db.prepare(`INSERT INTO battle_history (player_id, result, opponent, shots, hits, mode) VALUES (?,?,?,?,?,?)`)
    .run(id, result, opponentName || '?', shots, hits, mode);
}

function getBattleHistory(id, limit = 30, mode = null) {
  id = normalizeId(id);
  if (mode) return db.prepare(`SELECT * FROM battle_history WHERE player_id=? AND mode=? ORDER BY date DESC LIMIT ?`).all(id, mode, limit);
  return db.prepare(`SELECT * FROM battle_history WHERE player_id=? ORDER BY date DESC LIMIT ?`).all(id, limit);
}

// Таблица дуэлей (счёт между двумя конкретными игроками)
db.exec(`
  CREATE TABLE IF NOT EXISTS duels (
    player_a  TEXT NOT NULL,
    player_b  TEXT NOT NULL,
    a_wins    INTEGER DEFAULT 0,
    b_wins    INTEGER DEFAULT 0,
    PRIMARY KEY (player_a, player_b)
  );
`);

function recordDuelResult(winnerId, loserId) {
  winnerId = normalizeId(winnerId); loserId = normalizeId(loserId);
  if (!winnerId || !loserId || winnerId.startsWith('guest_') || loserId.startsWith('guest_')) return;
  // Нормализуем порядок: player_a < player_b (лексикографически)
  const [a, b] = winnerId < loserId ? [winnerId, loserId] : [loserId, winnerId];
  const isWinnerA = winnerId === a;
  db.prepare(`INSERT INTO duels (player_a, player_b, a_wins, b_wins) VALUES (?,?,?,?)
    ON CONFLICT(player_a, player_b) DO UPDATE SET
    a_wins = a_wins + ?, b_wins = b_wins + ?`)
    .run(a, b, isWinnerA ? 1 : 0, isWinnerA ? 0 : 1, isWinnerA ? 1 : 0, isWinnerA ? 0 : 1);
}

function getDuelStats(myId, opponentId) {
  myId = normalizeId(myId); opponentId = normalizeId(opponentId);
  if (!myId || !opponentId) return null;
  const [a, b] = myId < opponentId ? [myId, opponentId] : [opponentId, myId];
  const row = db.prepare(`SELECT * FROM duels WHERE player_a=? AND player_b=?`).get(a, b);
  if (!row) return { myWins: 0, theirWins: 0 };
  const myWins    = myId === a ? row.a_wins : row.b_wins;
  const theirWins = myId === a ? row.b_wins : row.a_wins;
  return { myWins, theirWins };
}


/* ─── XP / УРОВНИ ───────────────────────────────────────────────────── */
const XP_LEVELS = [0,1000,2250,3813,5766,8208,11260,15075,19844,25805,33256,
  42570,54212,68765,86956,109695,138118,173647,218058,273572,342964,
  429704,538129,673660,843074,1054841,1319550,1650437,2064045,2581055,3227318];

const RANKS = [
  { minLevel: 1,  name: 'Новобранец Неона' },
  { minLevel: 5,  name: 'Хакер Дронов' },
  { minLevel: 10, name: 'Неоновый Рейдер' },
  { minLevel: 20, name: 'Кибер-Титан' },
  { minLevel: 30, name: 'Абсолютный Доминайтор' },
];

function calcLevel(xp) {
  xp = xp || 0;
  let level = 1;
  for (let i = 1; i < XP_LEVELS.length; i++) {
    if (xp >= XP_LEVELS[i]) level = i + 1;
    else break;
  }
  return Math.min(level, 30);
}

function calcRank(level) {
  let rank = RANKS[0].name;
  for (const r of RANKS) { if (level >= r.minLevel) rank = r.name; }
  return rank;
}

function calcXpReward(result, sunkenCount, shots, hits, loserShots, isFriend = false) {
  const sunken = sunkenCount || 0;

  if (result === 'win') {
    // Режим с другом — фиксированные утешительные очки
    if (isFriend) {
      return { total: 300, baseXp: 300, bonusXp: 0 };
    }

    // Соперник сдался без единого выстрела — минимум
    if (loserShots === 0) return { total: 50, baseXp: 50, bonusXp: 0 };

    // Антифарм: полный XP только если потоплено >= 2 кораблей врага
    // (sunkenCount = корабли потопленные победителем у проигравшего)
    if (sunken < 2) {
      // Мало потоплено — урезанная награда пропорционально
      const partial = Math.round(200 + sunken * 150);  // 200 за 0, 350 за 1
      return { total: partial, baseXp: partial, bonusXp: 0 };
    }

    // Нормальный бой: базовые 1000 + бонус за точность
    const acc = shots > 0 ? hits / shots : 0;
    let accBonus = 0;
    if (acc >= 0.50) accBonus = 500;
    else if (acc >= 0.45) accBonus = 300;
    else if (acc >= 0.40) accBonus = 150;
    return { total: 1000 + accBonus, baseXp: 1000, bonusXp: accBonus };

  } else {
    // Режим с другом — фиксированные очки за поражение
    if (isFriend) {
      return { total: 100, baseXp: 100, bonusXp: 0 };
    }

    // Сдался без выстрелов и без уничтоженных кораблей — 0 XP
    if (shots === 0 && sunken === 0) {
      return { total: 0, baseXp: 0, bonusXp: 0 };
    }

    // Стандарт: 300 + 10 за каждый потопленный корабль, максимум 400
    const total = Math.min(400, 300 + 10 * sunken);
    return { total, baseXp: total, bonusXp: 0 };
  }
}

function addXp(id, reward) {
  id = normalizeId(id);
  const xpGain = typeof reward === 'number' ? reward : reward.total;
  if (!id || id.startsWith('guest_')) return null;
  const before = db.prepare(`SELECT xp FROM players WHERE id=?`).get(id);
  if (!before) return null;
  const xpBefore = before.xp || 0;
  const xpAfter  = xpBefore + Math.max(0, xpGain);
  if (xpGain > 0) db.prepare(`UPDATE players SET xp=? WHERE id=?`).run(xpAfter, id);
  const levelBefore = calcLevel(xpBefore);
  const levelAfter  = calcLevel(xpAfter);
  const baseXp  = typeof reward === 'object' ? reward.baseXp  : xpGain;
  const bonusXp = typeof reward === 'object' ? reward.bonusXp : 0;
  return { xpBefore, xpAfter, xpGain, baseXp, bonusXp, levelBefore, levelAfter, levelUp: levelAfter > levelBefore };
}

function getXpInfo(id) {
  id = normalizeId(id);
  const p = db.prepare(`SELECT xp FROM players WHERE id=?`).get(id);
  const xp = p?.xp || 0;
  const level = calcLevel(xp);
  const rank  = calcRank(level);
  const xpForThis  = XP_LEVELS[level - 1] || 0;
  const xpForNext  = XP_LEVELS[level]     || XP_LEVELS[XP_LEVELS.length - 1];
  const xpInLevel  = xp - xpForThis;
  const xpNeeded   = xpForNext - xpForThis;
  return { xp, level, rank, xpInLevel, xpNeeded, xpForNext };
}

const MAX_LEGIT_ACCURACY = 0.60; // выше 60% — подозрительно, XP не начисляется

function addWin(id, shots, hits, isOnline = false, sunkenCount = 0, loserShots = null, isRated = true, isFriend = false) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return null;
  db.prepare(`UPDATE players SET wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+?,
    updated_at=strftime('%s','now') WHERE id=?`).run(shots, hits, id);
  let xpResult = null;
  if (isOnline) {
    const acc = shots > 0 ? hits / shots : 0;
    const isLegit = loserShots === 0 ? true : acc <= MAX_LEGIT_ACCURACY;

    db.prepare(`UPDATE players SET online_wins=online_wins+1,
      online_shots=online_shots+?, online_hits=online_hits+? WHERE id=?`).run(shots, hits, id);

    if (isLegit) {
      if (isRated) {
        const p = db.prepare(`SELECT rating_active FROM players WHERE id=?`).get(id);
        if (p?.rating_active === 1) {
          db.prepare(`UPDATE players SET rated_wins=rated_wins+1,
            rated_shots=rated_shots+?, rated_hits=rated_hits+? WHERE id=?`).run(shots, hits, id);
        }
      }
      const xpReward = calcXpReward('win', sunkenCount, shots, hits, loserShots ?? null, isFriend);
      xpResult = addXp(id, xpReward);
    } else {
      console.log(`[ANTI-FARM] Blocked: ${id} acc=${(acc*100).toFixed(1)}%`);
      const row = db.prepare(`SELECT xp FROM players WHERE id=?`).get(id);
      const xpNow = row?.xp || 0;
      xpResult = {
        xpBefore: xpNow, xpAfter: xpNow, xpGain: 0,
        baseXp: 0, bonusXp: 0,
        levelBefore: calcLevel(xpNow), levelAfter: calcLevel(xpNow), levelUp: false,
      };
    }
  }
  return xpResult;
}

function addLoss(id, shots, hits, isOnline = false, sunkenCount = 0, isRated = true, isFriend = false) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return null;
  db.prepare(`UPDATE players SET losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+?,
    updated_at=strftime('%s','now') WHERE id=?`).run(shots, hits, id);
  let xpResult = null;
  if (isOnline) {
    db.prepare(`UPDATE players SET online_losses=online_losses+1,
      online_shots=online_shots+?, online_hits=online_hits+? WHERE id=?`).run(shots, hits, id);
    if (isRated) {
      const p = db.prepare(`SELECT rating_active FROM players WHERE id=?`).get(id);
      if (p?.rating_active === 1) {
        db.prepare(`UPDATE players SET rated_losses=rated_losses+1,
          rated_shots=rated_shots+?, rated_hits=rated_hits+? WHERE id=?`).run(shots, hits, id);
      }
    }
    const xpReward = calcXpReward('loss', sunkenCount, shots, hits, null, isFriend);
    xpResult = addXp(id, xpReward);
  }
  return xpResult;
}

// Рейтинг: только rated_ (матчи когда игрок участвовал), анти-бот фильтр
function getRating() {
  const rows = db.prepare(`
    SELECT id, name, rating_active, xp,
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
  return rows.map(r => ({ ...r, level: calcLevel(r.xp || 0), rank: calcRank(calcLevel(r.xp || 0)) }));
}

// Вступить в рейтинг
function joinRating(id) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return { ok: false };
  db.prepare(`UPDATE players SET rating_active=1, rating_since=strftime('%s','now'),
    rated_wins=0, rated_losses=0, rated_shots=0, rated_hits=0
    WHERE id=?`).run(id);
  return { ok: true };
}

// Покинуть рейтинг
function leaveRating(id) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return { ok: false };
  db.prepare(`UPDATE players SET rating_active=0,
    rated_wins=0, rated_losses=0, rated_shots=0, rated_hits=0
    WHERE id=?`).run(id);
  return { ok: true };
}

function getPlayerStats(id) {
  id = normalizeId(id);
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
  const xp1 = getXpInfo(room.p1.playerId);
  const xp2 = getXpInfo(room.p2.playerId);
  io.to(room.p1.socketId).emit('matched', { roomId: room.id, opponent: { playerId: room.p2.playerId, name: room.p2.name, level: xp2.level, rank: xp2.rank } });
  io.to(room.p2.socketId).emit('matched', { roomId: room.id, opponent: { playerId: room.p1.playerId, name: room.p1.name, level: xp1.level, rank: xp1.rank } });
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
      const toSunken  = timedOutPlayer.field ? countSunkenShips(timedOutPlayer.field) : 0;
      const otherSunk = otherPlayer.field    ? countSunkenShips(otherPlayer.field)    : 0;
      const winXpT  = addWin( otherPlayer.playerId,    otherPlayer.shots,    otherPlayer.hits,    true, toSunken,  null, !room.isFriend, room.isFriend);
      const lossXpT = addLoss(timedOutPlayer.playerId, timedOutPlayer.shots, timedOutPlayer.hits, true, otherSunk, !room.isFriend, room.isFriend);
      addBattleHistory(otherPlayer.playerId,    'win',  timedOutPlayer.name || '?', otherPlayer.shots,    otherPlayer.hits,    'online');
      addBattleHistory(timedOutPlayer.playerId, 'loss', otherPlayer.name    || '?', timedOutPlayer.shots, timedOutPlayer.hits, 'online');
      if (winXpT  && otherPlayer.socketId)    io.to(otherPlayer.socketId).emit('xp_reward', winXpT);
      if (lossXpT && timedOutPlayer.socketId) io.to(timedOutPlayer.socketId).emit('xp_reward', lossXpT);
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

  // Регистрируем сессию как онлайн (даже без matchmake)
  onlineSessions.set(socket.id, { socketId: socket.id, playerId: null, name: null, connectedAt: Date.now(), lastActive: Date.now() });
  broadcastOnlineCount();

  // Socket.IO rate limiting — защита от спама событиями
  const _socketEventCounts = {};
  const _socketRateLimits  = { shoot: { max: 5, window: 1000 }, matchmake: { max: 5, window: 10000 }, place_ships: { max: 3, window: 10000 } };
  function socketRateOk(event) {
    const limit = _socketRateLimits[event];
    if (!limit) return true;
    const now = Date.now();
    if (!_socketEventCounts[event]) _socketEventCounts[event] = { count: 0, reset: now + limit.window };
    if (now > _socketEventCounts[event].reset) { _socketEventCounts[event] = { count: 0, reset: now + limit.window }; }
    _socketEventCounts[event].count++;
    return _socketEventCounts[event].count <= limit.max;
  }

  socket.on('matchmake', ({ mode, roomId: friendRoomId, playerId, playerName }) => {
    if (!playerId) return;
    if (!socketRateOk('matchmake')) return;

    // Санитизация — убираем HTML и обрезаем имя
    const cleanPlayerId   = normalizeId(playerId);
    const cleanPlayerName = sanitizeStr(playerName || 'Игрок', 32);
    if (!cleanPlayerId) return;

    socket.data.playerId = cleanPlayerId;
    upsertPlayer(cleanPlayerId, cleanPlayerName);
    // Обновляем онлайн-сессию
    onlineSessions.set(socket.id, { socketId: socket.id, playerId: cleanPlayerId, name: cleanPlayerName, identified: true, connectedAt: Date.now(), lastActive: Date.now() });

    const info = { socketId: socket.id, playerId: cleanPlayerId, name: cleanPlayerName };

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
      const room   = { id: roomId, p1: makePlayer(info), p2: null, turn: playerId, started: false, over: false, _turnTimer: null, _warnTimer: null, _emptyTimer: null, isFriend: true };
      rooms.set(roomId, room);
      socket.join(roomId);
      socket.emit('room_created', { roomId });
    }
    else if (mode === 'friend_join') {
      const room = rooms.get(friendRoomId);
      if (!room) { socket.emit('room_expired'); return; }
      if (room.over) { socket.emit('room_expired'); return; }
      if (room.p2) { socket.emit('error_msg', { message: 'Комната заполнена' }); return; }
      // Отменяем таймер удаления если комната ждала
      if (room._emptyTimer) { clearTimeout(room._emptyTimer); room._emptyTimer = null; }
      room.p2 = makePlayer(info);
      socket.join(friendRoomId);
      notifyBothMatched(room);
    }
  });

// ─── ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ ─────────────────────────────────────────────────

// Обрезаем строку и убираем HTML-теги
function sanitizeStr(s, maxLen = 64) {
  if (typeof s !== 'string') return '';
  return s.replace(/<[^>]*>/g, '').trim().slice(0, maxLen);
}

// Проверяет поле 10×10: только числа 0/1, ровно 10 кораблей нужного состава
function validateField(field) {
  if (!Array.isArray(field) || field.length !== 10) return false;
  for (const row of field) {
    if (!Array.isArray(row) || row.length !== 10) return false;
    for (const cell of row) {
      if (cell !== 0 && cell !== 1) return false;
    }
  }
  // Считаем корабли flood-fill
  const visited = new Set();
  const ships = [];
  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 10; c++) {
      if (field[r][c] === 1 && !visited.has(r+','+c)) {
        const stack = [[r, c]]; let size = 0;
        while (stack.length) {
          const [cr, cc] = stack.pop();
          const key = cr+','+cc;
          if (visited.has(key)) continue;
          visited.add(key); size++;
          for (const [nr, nc] of [[cr-1,cc],[cr+1,cc],[cr,cc-1],[cr,cc+1]])
            if (nr>=0&&nr<10&&nc>=0&&nc<10&&field[nr][nc]===1) stack.push([nr,nc]);
        }
        ships.push(size);
      }
    }
  }
  // Стандартный флот: 1×4, 2×3, 3×2, 4×1
  if (ships.length !== 10) return false;
  const counts = {1:0,2:0,3:0,4:0};
  for (const s of ships) {
    if (!counts.hasOwnProperty(s)) return false;
    counts[s]++;
  }
  return counts[1]===4 && counts[2]===3 && counts[3]===2 && counts[4]===1;
}

// Проверяет что корабли не касаются друг друга (включая диагонали)
function validateNoTouch(field) {
  const visited = new Set();
  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 10; c++) {
      if (field[r][c] === 1 && !visited.has(r+','+c)) {
        // Собираем корабль
        const stack = [[r, c]]; const ship = [];
        const tmp = new Set();
        while (stack.length) {
          const [cr, cc] = stack.pop();
          const key = cr+','+cc;
          if (tmp.has(key)) continue;
          tmp.add(key); visited.add(key); ship.push([cr,cc]);
          for (const [nr,nc] of [[cr-1,cc],[cr+1,cc],[cr,cc-1],[cr,cc+1]])
            if (nr>=0&&nr<10&&nc>=0&&nc<10&&field[nr][nc]===1) stack.push([nr,nc]);
        }
        // Проверяем периметр включая диагонали
        for (const [sr,sc] of ship) {
          for (let dr=-1; dr<=1; dr++) for (let dc=-1; dc<=1; dc++) {
            if (dr===0&&dc===0) continue;
            const nr=sr+dr, nc=sc+dc;
            if (nr>=0&&nr<10&&nc>=0&&nc<10&&field[nr][nc]===1&&!tmp.has(nr+','+nc)) return false;
          }
        }
      }
    }
  }
  return true;
}

  socket.on('place_ships', ({ roomId, field }) => {
    if (!socketRateOk('place_ships')) return;
    const room = rooms.get(roomId);
    if (!room) return;
    const player = getPlayer(room, socket.id);
    if (!player || player.ready) return;

    // Валидация поля — защита от читов и мусорных данных
    if (!validateField(field) || !validateNoTouch(field)) {
      socket.emit('error_msg', { message: 'Неверная расстановка кораблей' });
      return;
    }
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
    touchSession(socket.id);
    if (!socketRateOk('shoot')) return;
    const room = rooms.get(roomId);
    if (!room || !room.started || room.over) return;

    // Валидация координат
    if (!Number.isInteger(r) || !Number.isInteger(c) || r < 0 || r > 9 || c < 0 || c > 9) return;

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
      // Потопленные корабли = все корабли цели (они все потоплены)
      const shooterSunken = countSunkenShips(target.field);
      const targetSunken  = countSunkenShips(shooter.field);
      const winXp  = addWin( shooter.playerId, shooter.shots, shooter.hits, true, shooterSunken, null, !room.isFriend, room.isFriend);
      const lossXp = addLoss(target.playerId,  target.shots,  target.hits,  true, targetSunken, !room.isFriend, room.isFriend);
      // Записываем историю боя на сервере (не зависим от HTTP-запроса клиента)
      addBattleHistory(shooter.playerId, 'win',  target.name  || '?', shooter.shots, shooter.hits, 'online');
      addBattleHistory(target.playerId,  'loss', shooter.name || '?', target.shots,  target.hits,  'online');
      recordDuelResult(shooter.playerId, target.playerId);
      // Отправляем XP каждому игроку
      if (winXp  && shooter.socketId) io.to(shooter.socketId).emit('xp_reward', winXp);
      if (lossXp && target.socketId)  io.to(target.socketId).emit('xp_reward', lossXp);
    } else {
      if (!hit) room.turn = target.playerId;
      // Запускаем таймер для следующего хода
      startTurnTimer(room);
    }
  });

  // п.7: явный уход с расстановки до старта игры
  socket.on('leave_room', ({ roomId }) => {
    const room = rooms.get(roomId);
    if (!room || room.over || room.started) return; // если игра уже идёт — обрабатывает surrender/disconnect
    const leaver = getPlayer(room, socket.id);
    const stayer = getOpponent(room, socket.id);
    room.over = true;
    clearTurnTimer(room);
    if (stayer?.socketId) {
      io.to(stayer.socketId).emit('opponent_left');
    }
    rooms.delete(roomId);
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
    const wSunken = surrenderer.field ? countSunkenShips(surrenderer.field) : 0; // корабли, потопленные победителем
    const lSunken = winner.field      ? countSunkenShips(winner.field)      : 0; // корабли, потопленные сдавшимся
    const winXp2  = addWin( winner.playerId,      winner.shots,      winner.hits,      true, wSunken, surrenderer.shots, !room.isFriend, room.isFriend);
    const lossXp2 = addLoss(surrenderer.playerId, surrenderer.shots, surrenderer.hits, true, lSunken, !room.isFriend, room.isFriend);
    // Записываем историю
    addBattleHistory(winner.playerId,      'win',  surrenderer.name || '?', winner.shots,      winner.hits,      'online');
    addBattleHistory(surrenderer.playerId, 'loss', winner.name      || '?', surrenderer.shots, surrenderer.hits, 'online');
    if (winXp2  && winner.socketId)      io.to(winner.socketId).emit('xp_reward', winXp2);
    if (lossXp2 && surrenderer.socketId) io.to(surrenderer.socketId).emit('xp_reward', lossXp2);
    recordDuelResult(winner.playerId, surrenderer.playerId);
  });

  // ── Реакции ───────────────────────────────────────────
  socket.on('reaction', ({ roomId, emoji }) => {
    const room = rooms.get(roomId);
    if (!room || !room.started || room.over) return;
    const opponent = getOpponent(room, socket.id);
    if (!opponent?.socketId) return;

    const allowedEmoji = new Set(['👍','❤️','👎','🤬','😂']);
    let payload = null;

    if (allowedEmoji.has(emoji)) {
      payload = { type: 'emoji', value: emoji };
    } else if (typeof emoji === 'string' && emoji.startsWith('custom:')) {
      const itemId = emoji.slice(7).replace(/[^a-zA-Z0-9_]/g, '');
      if (itemId.length > 0) {
        const row = db.prepare(
          `SELECT preview_url FROM shop_items WHERE id=? AND type='reaction' AND is_active=1`
        ).get(itemId);
        if (row) {
          const filename = row.preview_url ? row.preview_url.replace(/^\/reactions\//, '') : null;
          if (filename) payload = { type: 'custom', id: itemId, filename };
        }
      }
    }

    if (!payload) return;
    io.to(opponent.socketId).emit('reaction_received', payload);
  });

  // ── Реванш ───────────────────────────────────────
  socket.on('rematch_request', ({ roomId }) => {
    const room = rooms.get(roomId);
    if (!room) return;
    const requester = getPlayer(room, socket.id);
    const opponent  = getOpponent(room, socket.id);
    if (!requester || !opponent) return;

    // Инициализируем объект реванша в комнате если нет
    if (!room.rematch) room.rematch = {};

    room.rematch[socket.id] = true;

    // Уведомляем соперника
    if (opponent.socketId) {
      io.to(opponent.socketId).emit('rematch_requested');
    }

    // Оба нажали реванш?
    const p1Ready = room.rematch[room.p1?.socketId];
    const p2Ready = room.rematch[room.p2?.socketId];

    if (p1Ready && p2Ready) {
      // Оба согласились — очищаем таймер и запускаем новую игру в той же комнате
      if (room._rematchTimer) { clearTimeout(room._rematchTimer); room._rematchTimer = null; }
      room.rematch = null;

      // Сбрасываем состояние комнаты для новой игры
      room.over      = false;
      room.started   = false;
      room.p1.ships  = null; room.p1.ready = false; room.p1.shots = 0; room.p1.hits = 0;
      room.p2.ships  = null; room.p2.ready = false; room.p2.shots = 0; room.p2.hits = 0;

      io.to(room.p1.socketId).emit('rematch_accepted');
      io.to(room.p2.socketId).emit('rematch_accepted');
      console.log(`[rematch] room ${roomId} restarted`);
      return;
    }

    // Запускаем 10-секундный таймер если это первый запрос
    if (!room._rematchTimer) {
      room._rematchTimer = setTimeout(() => {
        if (!room.rematch) return;
        // Кто не нажал — тому отказ, кто нажал — ему declined
        const p1 = room.p1, p2 = room.p2;
        if (room.rematch[p1?.socketId] && !room.rematch[p2?.socketId]) {
          io.to(p1.socketId).emit('rematch_declined');
        } else if (room.rematch[p2?.socketId] && !room.rematch[p1?.socketId]) {
          io.to(p2.socketId).emit('rematch_declined');
        }
        room.rematch = null;
        room._rematchTimer = null;
      }, 10000);
    }
  });

  // п.5: отключение во время игры = победа оставшемуся
  // Клиент шлёт identify сразу при загрузке — регистрирует себя в онлайн
  socket.on('identify', ({ playerId }) => {
    const normId = playerId ? normalizeId(playerId) : null;
    const existing = onlineSessions.get(socket.id) || {};
    onlineSessions.set(socket.id, {
      ...existing,
      socketId: socket.id,
      playerId: normId,
      identified: true,
      lastActive: Date.now(),
    });
    broadcastOnlineCount();
  });

  // Heartbeat пока вкладка открыта
  socket.on('active', () => { touchSession(socket.id); });

  socket.on('disconnect', () => {
    console.log(`[-] ${socket.id}`);
    onlineSessions.delete(socket.id);
    broadcastOnlineCount();
    const idx = waitingPool.findIndex(p => p.socketId === socket.id);
    if (idx >= 0) waitingPool.splice(idx, 1);

    for (const [roomId, room] of rooms) {
      if (room.over) continue;
      if (room.p1?.socketId !== socket.id && room.p2?.socketId !== socket.id) continue;

      clearTurnTimer(room);

      const leaver = room.p1?.socketId === socket.id ? room.p1 : room.p2;
      const stayer = room.p1?.socketId === socket.id ? room.p2 : room.p1;

      if (room.started && stayer?.socketId) {
        // Игра шла — победа оставшемуся
        room.over = true;
        io.to(stayer.socketId).emit('opponent_disconnected_win');
        const dSunkenStayer = leaver.field ? countSunkenShips(leaver.field) : 0;
        const dSunkenLeaver = stayer.field ? countSunkenShips(stayer.field) : 0;
        const dWinXp  = addWin( stayer.playerId, stayer.shots, stayer.hits, true, dSunkenStayer, null, !room.isFriend, room.isFriend);
        const dLossXp = addLoss(leaver.playerId, leaver.shots, leaver.hits, true, dSunkenLeaver, !room.isFriend, room.isFriend);
        if (dWinXp  && stayer.socketId) io.to(stayer.socketId).emit('xp_reward', dWinXp);
        // leaver отключён — xp_reward не шлём
        recordDuelResult(stayer.playerId, leaver.playerId);
        rooms.delete(roomId);
      } else if (stayer?.socketId) {
        // Игра не началась, второй игрок есть — уведомляем и удаляем
        room.over = true;
        io.to(stayer.socketId).emit('opponent_left');
        rooms.delete(roomId);
      } else {
        // Комната пустая — держим 5 минут, потом удаляем
        if (room._emptyTimer) clearTimeout(room._emptyTimer);
        room._emptyTimer = setTimeout(() => {
          rooms.delete(roomId);
          console.log(`[room] ${roomId} expired after 5min empty`);
        }, 5 * 60 * 1000);
      }
      break;
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
  const isSunk = ship.length > 0 && ship.every(([r, c]) => field[r][c] === 2);
  // Помечаем потопленный корабль как 4 — нужно для countSunkenShips
  if (isSunk) {
    for (const [r, c] of ship) field[r][c] = 4;
  }
  return isSunk;
}

// Считаем количество потопленных кораблей в поле (клетки со значением 4, flood-fill группами)
function countSunkenShips(field) {
  const visited = new Set();
  let count = 0;
  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 10; c++) {
      if (field[r]?.[c] === 4 && !visited.has(r+','+c)) {
        count++;
        const stack = [[r, c]];
        while (stack.length) {
          const [cr, cc] = stack.pop();
          const key = cr+','+cc;
          if (visited.has(key)) continue;
          visited.add(key);
          for (const [nr, nc] of [[cr-1,cc],[cr+1,cc],[cr,cc-1],[cr,cc+1]])
            if (nr>=0&&nr<10&&nc>=0&&nc<10&&field[nr]?.[nc]===4) stack.push([nr,nc]);
        }
      }
    }
  }
  return count;
}

app.get('/api/config',     (req, res) => res.json({ botUsername: BOT_USERNAME, appName: APP_NAME }));
app.get('/api/online',     (req, res) => res.json({ count: getOnlineCount() }));
app.get('/api/history/:id',(req, res) => { try { const mode = req.query.mode || null; res.json({ ok: true, data: getBattleHistory(req.params.id, 30, mode) }); } catch(e) { res.status(500).json({ ok: false }); } });
app.post('/api/history', (req, res) => {
  try {
    const { id, result, opponent, shots, hits, skipStats, mode } = req.body;
    const cleanId = normalizeId(id);
    if (!cleanId || cleanId.startsWith('guest_')) { res.json({ ok: false }); return; }
    // Валидация enum-полей
    const cleanResult = ['win','loss','draw'].includes(result) ? result : null;
    if (!cleanResult) { res.json({ ok: false }); return; }
    const cleanMode     = ['online','bot-easy','bot-hard'].includes(mode) ? mode : 'online';
    const cleanOpponent = sanitizeStr(opponent || '?', 64);
    const cleanShots    = Math.max(0, parseInt(shots)  || 0);
    const cleanHits     = Math.max(0, parseInt(hits)   || 0);
    const gameMode = cleanMode;
    addBattleHistory(cleanId, cleanResult, cleanOpponent, cleanShots, cleanHits, gameMode);
    if (!skipStats) {
      if (cleanResult === 'win') {
        db.prepare(`UPDATE players SET wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+?, updated_at=strftime('%s','now') WHERE id=?`).run(cleanShots, cleanHits, cleanId);
      } else if (cleanResult === 'loss') {
        db.prepare(`UPDATE players SET losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+?, updated_at=strftime('%s','now') WHERE id=?`).run(cleanShots, cleanHits, cleanId);
      }
    }
    res.json({ ok: true });
  } catch(e) { console.error('history post error:', e); res.status(500).json({ ok: false }); }
});
app.get('/api/ensure/:id', (req, res) => {
  try {
    const id   = normalizeId(req.params.id);
    const name = sanitizeStr(req.query.name || 'Игрок', 32);
    if (!id || id.startsWith('guest_')) { res.json({ ok: false }); return; }
    upsertPlayer(id, name);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false }); }
});
app.get('/api/leaderboard',(req, res) => { try { res.json({ ok: true, data: getRating() }); } catch(e) { console.error('leaderboard error:', e); res.status(500).json({ ok: false, error: e.message }); } });
app.get('/api/rating',    (req, res) => { try { res.json({ ok: true, data: getRating() }); } catch(e) { console.error('rating error:', e); res.status(500).json({ ok: false, error: e.message }); } });
app.post('/api/rating/join',  (req, res) => { try { res.json(joinRating(req.body.id));  } catch(e) { res.status(500).json({ ok: false }); } });
app.post('/api/rating/leave', (req, res) => { try { res.json(leaveRating(req.body.id)); } catch(e) { res.status(500).json({ ok: false }); } });

// ─── ADMIN ANALYTICS ─────────────────────────────────────────────────────────
app.get('/api/admin/analytics', (req, res) => {
  try {
    const secret = req.headers['x-admin-secret'] || req.query.secret;
    if (!SHOP_SECRET || secret !== SHOP_SECRET) {
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }

    const now     = Math.floor(Date.now() / 1000);
    const day     = 86400;
    const since24 = now - day;
    const since7d = now - day * 7;
    const since30d= now - day * 30;

    // ── Игроки ──
    const totalPlayers  = db.prepare(`SELECT COUNT(*) as n FROM players WHERE id NOT LIKE 'guest_%'`).get().n;
    const newToday      = db.prepare(`SELECT COUNT(*) as n FROM players WHERE updated_at >= ? AND id NOT LIKE 'guest_%'`).get(since24).n;
    const activeWeek    = db.prepare(`SELECT COUNT(DISTINCT player_id) as n FROM battle_history WHERE date >= ?`).get(since7d).n;
    const activeMonth   = db.prepare(`SELECT COUNT(DISTINCT player_id) as n FROM battle_history WHERE date >= ?`).get(since30d).n;

    // ── Бои ──
    const battlesToday  = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode='online'`).get(since24).n;
    const battles7d     = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode='online'`).get(since7d).n;
    const battles30d    = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode='online'`).get(since30d).n;
    const battlesTotal  = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE mode='online'`).get().n;

    // ── Точность (средняя по всем онлайн-игрокам) ──
    const accRow = db.prepare(`
      SELECT
        ROUND(100.0 * SUM(online_hits) / NULLIF(SUM(online_shots), 0), 1) as avg_acc,
        SUM(online_shots) as total_shots,
        SUM(online_hits)  as total_hits
      FROM players WHERE id NOT LIKE 'guest_%'
    `).get();

    // ── Винрейт (из battle_history онлайн) ──
    const vrRow = db.prepare(`
      SELECT
        SUM(CASE WHEN result='win'  THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses
      FROM battle_history WHERE mode='online'
    `).get();
    const totalBattleRows = (vrRow.wins || 0) + (vrRow.losses || 0);
    const winrate = totalBattleRows > 0
      ? Math.round(100 * vrRow.wins / totalBattleRows)
      : 0;

    // ── Покупки ──
    const purchasesToday= db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchased_at >= ? AND purchase_type='stars' AND is_active=1`).get(since24).n;
    const purchases7d   = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchased_at >= ? AND purchase_type='stars' AND is_active=1`).get(since7d).n;
    const purchases30d  = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchased_at >= ? AND purchase_type='stars' AND is_active=1`).get(since30d).n;
    const purchasesTotal= db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchase_type='stars' AND is_active=1`).get().n;
    const refundsTotal  = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchase_type='stars' AND is_active=0`).get().n;

    // ── Топ-5 товаров ──
    const topItems = db.prepare(`
      SELECT i.item_id, s.name, COUNT(*) as cnt
      FROM inventory i
      LEFT JOIN shop_items s ON s.id = i.item_id
      WHERE i.purchase_type='stars' AND i.is_active=1
      GROUP BY i.item_id ORDER BY cnt DESC LIMIT 5
    `).all();

    // ── Бои по дням (последние 7) ──
    const byDay = db.prepare(`
      SELECT
        date(date, 'unixepoch', 'localtime') as day,
        COUNT(*) as battles
      FROM battle_history
      WHERE mode='online' AND date >= ?
      GROUP BY day ORDER BY day ASC
    `).all(since7d);

    // ── Онлайн прямо сейчас ──
    const onlineNow = getOnlineCount();

    res.json({ ok: true, data: {
      ts: now,
      players:   { total: totalPlayers, new_24h: newToday, active_7d: activeWeek, active_30d: activeMonth },
      battles:   { today: battlesToday, week: battles7d, month: battles30d, total: battlesTotal, by_day: byDay },
      accuracy:  { avg_pct: accRow.avg_acc, total_shots: accRow.total_shots, total_hits: accRow.total_hits },
      winrate:   { pct: winrate, wins: vrRow.wins, losses: vrRow.losses },
      purchases: { today: purchasesToday, week: purchases7d, month: purchases30d, total: purchasesTotal, refunds: refundsTotal, top_items: topItems },
      online_now: onlineNow,
    }});
  } catch(e) { console.error('[analytics]', e); res.status(500).json({ ok: false, error: e.message }); }
});
app.get('/api/stats/:id',  (req, res) => {
  try {
    const data = getPlayerStats(req.params.id) || null;
    let xpInfo = null;
    if (data) xpInfo = getXpInfo(req.params.id);
    res.json({ ok: true, data: data ? { ...data, ...xpInfo } : null });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});
app.post('/api/stats/reset', (req, res) => {
  try {
    const id = normalizeId(req.body.id);
    if (!id || id.startsWith('guest_')) { res.json({ ok: false }); return; }
    db.prepare(`UPDATE players SET
      wins=0, losses=0, total_shots=0, total_hits=0,
      online_wins=0, online_losses=0, online_shots=0, online_hits=0,
      rated_wins=0, rated_losses=0, rated_shots=0, rated_hits=0,
      updated_at=strftime('%s','now')
      WHERE id=?`).run(id);
    db.prepare(`DELETE FROM battle_history WHERE player_id=?`).run(id);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// XP профиль отдельным эндпоинтом
app.get('/api/xp/:id', (req, res) => {
  try {
    const info = getXpInfo(req.params.id);
    res.json({ ok: true, data: info });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// Получить уровень + звание для нескольких игроков (для рейтинга)
app.post('/api/xp/batch', (req, res) => {
  try {
    const ids = req.body.ids || [];
    const result = {};
    for (const id of ids) {
      const norm = normalizeId(id);
      if (norm) result[norm] = getXpInfo(norm);
    }
    res.json({ ok: true, data: result });
  } catch(e) { res.status(500).json({ ok: false }); }
});
app.get('/api/duel/:myId/:theirId', (req, res) => {
  try { res.json({ ok: true, data: getDuelStats(req.params.myId, req.params.theirId) }); }
  catch(e) { res.status(500).json({ ok: false }); }
});
app.get('/api/status',     (req, res) => res.json({ ok: true, rooms: rooms.size, waiting: waitingPool.length, uptime: process.uptime() }));


// Уведомить пользователя через WebSocket если он онлайн
function notifyUser(userId, event, data) {
  for (const [, session] of onlineSessions) {
    if (session.playerId === userId) {
      io.to(session.socketId).emit(event, data);
      break;
    }
  }
}

// ─── SHOP API ────────────────────────────────────────────────────────────────

// Каталог магазина (все активные товары)
app.get('/api/shop/items', (req, res) => {
  try {
    const items = db.prepare(`SELECT * FROM shop_items WHERE is_active=1 ORDER BY sort_order`).all();
    res.json({ ok: true, data: items });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Публичный список кастомных реакций (из shop_items type='reaction')
// Возвращает id, name, filename (из preview_url), sort_order
app.get('/api/reactions', (req, res) => {
  try {
    const rows = db.prepare(
      `SELECT id, name, preview_url, sort_order FROM shop_items WHERE type='reaction' AND is_active=1 ORDER BY sort_order, id`
    ).all().map(r => ({
      id:         r.id,
      name:       r.name,
      filename:   r.preview_url ? r.preview_url.replace(/^\/reactions\//, '') : null,
      sort_order: r.sort_order,
    })).filter(r => r.filename);
    res.json({ ok: true, data: rows });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Страница конкретного товара
app.get('/api/shop/item/:id', (req, res) => {
  try {
    const item = db.prepare(`SELECT * FROM shop_items WHERE id=?`).get(req.params.id);
    if (!item) return res.status(404).json({ ok: false, error: 'not found' });
    res.json({ ok: true, data: item });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Инвентарь игрока + текущая экипировка
// ─── ВЕРИФИКАЦИЯ TELEGRAM INITDATA ───────────────────────────────────────────
// Проверяет подпись initData от Telegram WebApp — гарантирует что запрос от реального пользователя
function verifyTelegramInitData(initData) {
  try {
    if (!BOT_TOKEN || !initData) return null;
    const params  = new URLSearchParams(initData);
    const hash    = params.get('hash');
    if (!hash) return null;
    params.delete('hash');
    const dataCheckString = Array.from(params.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join('\n');
    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const checkHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
    if (checkHash !== hash) return null;
    // Извлекаем user.id
    const userStr = params.get('user');
    if (!userStr) return null;
    const user = JSON.parse(userStr);
    return String(user.id);
  } catch { return null; }
}

// Middleware: проверяет X-Telegram-Init-Data и кладёт userId в req.telegramUserId
function requireTelegramAuth(req, res, next) {
  const initData = req.headers['x-telegram-init-data'];
  const userId   = verifyTelegramInitData(initData);
  if (!userId) return res.status(401).json({ ok: false, error: 'unauthorized' });
  req.telegramUserId = userId;
  next();
}

app.get('/api/inventory/:userId', (req, res) => {
  try {
    const { userId } = req.params;
    if (!userId || userId.startsWith('guest_')) return res.json({ ok: true, data: { items: [], equipped: {} } });

    const initData   = req.headers['x-telegram-init-data'];
    const verifiedId = initData ? verifyTelegramInitData(initData) : null;

    console.log(`[Inventory] userId=${userId} initData=${!!initData} verifiedId=${verifiedId} isAdmin=${isAdmin(userId)}`);

    if (verifiedId && verifiedId !== normalizeId(userId)) {
      console.log(`[Inventory] BLOCKED: verifiedId=${verifiedId} !== userId=${normalizeId(userId)}`);
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }

    // Админ — весь каталог как купленный
    if (isAdmin(userId)) {
      const allItems = db.prepare(`SELECT * FROM shop_items WHERE is_active=1 ORDER BY sort_order`).all();
      const fakeInv  = allItems.map(i => ({ ...i, item_id: i.id, purchase_type: 'admin', is_active: 1, is_equipped: 0 }));
      return res.json({ ok: true, data: { items: fakeInv, equipped: getEquipped(userId) } });
    }

    // Обычный пользователь — отдаём инвентарь БЕЗ чувствительных полей
    const items = getInventory(userId).map(({ telegram_charge_id, ...safe }) => safe);
    res.json({ ok: true, data: { items, equipped: getEquipped(userId) } });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Создать invoice для покупки за Stars
app.post('/api/shop/buy', async (req, res) => {
  try {
    const { userId, itemId } = req.body;
    if (!userId || !itemId) return res.status(400).json({ ok: false, error: 'missing params' });
    if (userId.startsWith('guest_')) return res.status(403).json({ ok: false, error: 'guests cannot buy' });
    if (!BOT_TOKEN) return res.status(503).json({ ok: false, error: 'payments not configured' });

    // Админ — выдаём бесплатно
    if (isAdmin(userId)) {
      grantItem(userId, itemId, 'admin');
      return res.json({ ok: true, free: true });
    }

    const item = db.prepare(`SELECT * FROM shop_items WHERE id=? AND is_active=1`).get(itemId);
    if (!item) return res.status(404).json({ ok: false, error: 'item not found' });
    if (!item.price_stars) return res.status(400).json({ ok: false, error: 'item is not for sale' });

    // Уже куплен?
    if (hasItem(userId, itemId)) return res.status(409).json({ ok: false, error: 'already owned' });

    // Уникальный payload для этой транзакции
    const payload = `${userId}:${itemId}:${Date.now()}`;

    // Сохраняем pending invoice
    db.prepare(`INSERT OR REPLACE INTO pending_invoices (payload, user_id, item_id, price_stars) VALUES (?,?,?,?)`)
      .run(payload, userId, itemId, item.price_stars);

    // Создаём invoice через Telegram Bot API
    const tgRes = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/createInvoiceLink`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: item.name,
        description: item.description || item.name,
        payload,
        currency: 'XTR',               // Telegram Stars
        prices: [{ label: item.name, amount: item.price_stars }],
        photo_url: item.photo_url_tg ? `${req.protocol}://${req.get('host')}${item.photo_url_tg}` : undefined,
      })
    });
    const tgJson = await tgRes.json();
    if (!tgJson.ok) {
      console.error('[Shop] TG invoice error:', tgJson);
      return res.status(502).json({ ok: false, error: 'telegram error', detail: tgJson.description });
    }

    res.json({ ok: true, invoiceUrl: tgJson.result });
  } catch(e) {
    console.error('[Shop] buy error:', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Webhook от Telegram Bot (successful_payment + refunded_payment)
app.post('/api/webhook/telegram', express.json(), (req, res) => {
  try {
    // Проверяем секрет — запросы только от нашего Python бота
    const secret = req.headers['x-shop-secret'];
    if (SHOP_SECRET && secret !== SHOP_SECRET) {
      console.warn('[Shop] webhook: invalid secret');
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }

    const update = req.body;
    res.json({ ok: true }); // отвечаем сразу, обрабатываем асинхронно

    const msg = update.message;
    if (!msg) return;

    // _user_id — нормализованный id, который передаёт Python бот
    const userId = msg._user_id ? String(msg._user_id) : null;

    // ── Успешная оплата ────────────────────────────────────────────────────
    if (msg.successful_payment) {
      const sp       = msg.successful_payment;
      const payload  = sp.invoice_payload;
      const chargeId = sp.telegram_payment_charge_id;

      const invoice = db.prepare(`SELECT * FROM pending_invoices WHERE payload=?`).get(payload);
      if (!invoice) {
        console.error(`[Shop] Unknown payload: ${payload}`);
        return;
      }

      // Дополнительная проверка: user в payload совпадает с тем кто платил
      if (userId && invoice.user_id !== userId) {
        console.error(`[Shop] User mismatch: invoice=${invoice.user_id} actual=${userId}`);
        return;
      }

      grantItem(invoice.user_id, invoice.item_id, 'stars', chargeId);
      db.prepare(`UPDATE pending_invoices SET status='paid' WHERE payload=?`).run(payload);
      console.log(`[Shop] ✅ Purchased: user=${invoice.user_id} item=${invoice.item_id} charge=${chargeId}`);

      notifyUser(invoice.user_id, 'purchase_complete', { itemId: invoice.item_id });
    }

    // ── Рефанд (до 21 дня, инициирует пользователь через TG) ──────────────
    if (msg.refunded_payment) {
      const chargeId = msg.refunded_payment.telegram_payment_charge_id;
      const inv = db.prepare(`SELECT * FROM inventory WHERE telegram_charge_id=?`).get(chargeId);

      if (inv) {
        db.prepare(`
          UPDATE inventory SET is_active=0, refunded_at=strftime('%s','now')
          WHERE telegram_charge_id=?
        `).run(chargeId);

        const item = db.prepare(`SELECT type FROM shop_items WHERE id=?`).get(inv.item_id);
        if (item) {
          db.prepare(`DELETE FROM equipped WHERE user_id=? AND slot=? AND item_id=?`)
            .run(inv.user_id, item.type, inv.item_id);
        }

        console.log(`[Shop] 🔄 Refunded: user=${inv.user_id} item=${inv.item_id} charge=${chargeId}`);
        notifyUser(inv.user_id, 'item_revoked', { itemId: inv.item_id });
      } else {
        console.warn(`[Shop] Refund for unknown charge: ${chargeId}`);
      }
    }

  } catch(e) { console.error('[Shop] webhook error:', e); }
});

// Экипировать айтем (надеть/активировать)
app.post('/api/equip', (req, res) => {
  try {
    const { userId, itemId } = req.body;
    if (!userId || !itemId) return res.status(400).json({ ok: false, error: 'missing params' });
    if (!isAdmin(userId) && !hasItem(userId, itemId)) return res.status(403).json({ ok: false, error: 'not owned' });

    const item = db.prepare(`SELECT * FROM shop_items WHERE id=?`).get(itemId);
    if (!item) return res.status(404).json({ ok: false, error: 'item not found' });

    // INSERT OR REPLACE — один слот = один айтем
    db.prepare(`INSERT OR REPLACE INTO equipped (user_id, slot, item_id) VALUES (?,?,?)`)
      .run(userId, item.type, itemId);

    res.json({ ok: true, slot: item.type, itemId });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Снять экипировку со слота
app.post('/api/unequip', (req, res) => {
  try {
    const { userId, slot } = req.body;
    if (!userId || !slot) return res.status(400).json({ ok: false, error: 'missing params' });
    db.prepare(`DELETE FROM equipped WHERE user_id=? AND slot=?`).run(userId, slot);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Выдать товар за действие (внутренний — защищён секретом)
app.post('/api/reward', (req, res) => {
  try {
    const { secret, userId, itemId, reason } = req.body;
    if (secret !== SHOP_SECRET) return res.status(403).json({ ok: false, error: 'forbidden' });
    if (!userId || !itemId) return res.status(400).json({ ok: false, error: 'missing params' });
    if (hasItem(userId, itemId)) return res.json({ ok: true, already: true });

    grantItem(userId, itemId, 'reward');
    console.log(`[Shop] Reward granted: user=${userId} item=${itemId} reason=${reason}`);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Фронтенд — обязательно В САМОМ КОНЦЕ, после всех API
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

server.listen(PORT, () => console.log(`\n🚢 http://localhost:${PORT}\n`));
module.exports = { app, server };
