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
const ADMIN_IDS    = new Set((process.env.ADMIN_IDS || '').replace(/["\']/g, '').split(',').map(s => s.trim()).filter(Boolean));
console.log('[Config] ADMIN_IDS raw:', JSON.stringify(process.env.ADMIN_IDS));
console.log('[Config] ADMIN_IDS parsed:', [...ADMIN_IDS]);

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
  pingTimeout:  10000, // 10 сек — детектируем разрыв быстрее
  pingInterval:  5000, // ping каждые 5 сек
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

// ─── СИСТЕМА ДОСТИЖЕНИЙ И РЕФЕРАЛОВ ──────────────────────────────────────────

db.exec(`
  CREATE TABLE IF NOT EXISTS achievements_progress (
    user_id        TEXT NOT NULL,
    achievement_id TEXT NOT NULL,
    progress       INTEGER DEFAULT 0,
    completed_at   INTEGER,          -- timestamp первого выполнения (для одноразовых)
    times_done     INTEGER DEFAULT 0, -- сколько раз выполнено (для пополняемых)
    notified       INTEGER DEFAULT 0, -- 1 = игрок видел уведомление
    PRIMARY KEY (user_id, achievement_id)
  );
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS referrals (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    inviter_id     TEXT NOT NULL,    -- кто пригласил
    invitee_id     TEXT NOT NULL,    -- кого пригласили
    battles_done   INTEGER DEFAULT 0, -- сколько боёв сыграл приглашённый
    qualified      INTEGER DEFAULT 0, -- 1 = выполнил условие (>=3 боёв)
    created_at     INTEGER DEFAULT (strftime('%s','now')),
    UNIQUE(invitee_id)               -- игрок может быть приглашён только один раз
  );
`);

// Определения всех достижений
const ACHIEVEMENTS = [
  // — С наградой (звание), ограниченные —
  { id: 'fleet_recruit',     title: 'Новобранец флота',    desc: 'Сыграть 10 боёв',                goal: 10,   type: 'limited', reward: 'title_fleet_recruit',     countFn: 'total_battles' },
  { id: 'exp_tester',        title: 'Опытный испытатель',  desc: 'Сыграть 50 боёв',                goal: 50,   type: 'limited', reward: 'title_exp_tester',        countFn: 'total_battles' },
  { id: 'admiral',           title: 'Адмирал',             desc: 'Сыграть 200 боёв',               goal: 200,  type: 'limited', reward: 'title_admiral',           countFn: 'total_battles' },
  { id: 'marshal',           title: 'Маршал',              desc: 'Сыграть 500 боёв',               goal: 500,  type: 'limited', reward: 'title_marshal',           countFn: 'total_battles' },
  { id: 'real_intel',        title: 'Реальный интеллект',  desc: 'Выиграть 20 боёв против бота',   goal: 20,   type: 'limited', reward: 'title_real_intel',        countFn: 'bot_wins' },
  { id: 'star_scout',        title: 'Звёздный разведчик',  desc: 'Сыграть 30 случайных боёв',      goal: 30,   type: 'limited', reward: 'title_star_scout',        countFn: 'random_battles' },
  { id: 'anon_hunter',       title: 'Анонимный охотник',   desc: 'Сыграть 100 случайных боёв',     goal: 100,  type: 'limited', reward: 'title_anon_hunter',       countFn: 'random_battles' },
  { id: 'friendly_fleet',    title: 'Дружеский флот',      desc: 'Сыграть 10 боёв с другом',       goal: 10,   type: 'limited', reward: 'title_friendly_fleet',    countFn: 'friend_battles' },
  { id: 'captain_tester',    title: 'Капитан-испытатель',  desc: 'Сыграть 30 боёв с друзьями',     goal: 30,   type: 'limited', reward: 'title_captain_tester',    countFn: 'friend_battles' },
  { id: 'duelist',           title: 'Дуэлянт',             desc: 'Сыграть 100 боёв с друзьями',    goal: 100,  type: 'limited', reward: 'title_duelist',           countFn: 'friend_battles' },
  { id: 'sea_strategist',    title: 'Морской стратег',     desc: '50 побед в любых режимах',       goal: 50,   type: 'limited', reward: 'title_sea_strategist',    countFn: 'total_wins' },
  { id: 'first_time',        title: 'Время первых',        desc: 'Занять первую строчку рейтинга', goal: 1,    type: 'limited', reward: 'title_first_time',        countFn: 'rating_top1' },
  { id: 'determined',        title: 'Целеустремлённый',    desc: 'Достичь 30 уровня',              goal: 1,    type: 'limited', reward: 'title_determined',        countFn: 'level_30' },
  { id: 'collector',         title: 'Коллекционер',        desc: 'Купить три цветовые схемы',      goal: 3,    type: 'limited', reward: 'title_collector',         countFn: 'themes_bought' },
  { id: 'recruiter',         title: 'Рекрутер',            desc: 'Пригласить 3 игроков, каждый сыграет 3 боя в любом режиме', goal: 3, type: 'limited', reward: 'title_recruiter', countFn: 'referrals_qualified', hasRefPage: true },
  { id: 'space_navigator',   title: 'Космический навигатор',desc:'Пригласить 10 игроков, каждый сыграет 3 боя в любом режиме', goal: 10, type: 'limited', reward: 'title_space_navigator', countFn: 'referrals_qualified', hasRefPage: true },
  // — Без награды, пополняемые —
  { id: 'first_exp',         title: 'Первый опыт',         desc: 'Одна победа (получается один раз)',goal:1,   type: 'once',    reward: null,                      countFn: 'total_wins' },
  { id: 'cold_calc',         title: 'Холодный расчёт',     desc: 'Обойти соперника по точности',   goal: null, type: 'infinite',reward: null,                      countFn: 'acc_win' },
  { id: 'last_chance',       title: 'Последний шанс',      desc: 'Выиграть, имея один корабль',    goal: null, type: 'infinite',reward: null,                      countFn: 'last_ship_win' },
];

// Звания за достижения и покупные — добавляем в shop_items
const ACHIEVEMENT_TITLES = [
  { id: 'title_fleet_recruit',   name: 'Новобранец флота',    rank: 'initial' },
  { id: 'title_exp_tester',      name: 'Опытный испытатель',  rank: 'medium' },
  { id: 'title_admiral',         name: 'Адмирал',             rank: 'high' },
  { id: 'title_marshal',         name: 'Маршал',              rank: 'prestige' },
  { id: 'title_real_intel',      name: 'Реальный интеллект',  rank: 'initial' },
  { id: 'title_star_scout',      name: 'Звёздный разведчик',  rank: 'medium' },
  { id: 'title_anon_hunter',     name: 'Анонимный охотник',   rank: 'prestige' },
  { id: 'title_friendly_fleet',  name: 'Дружеский флот',      rank: 'initial' },
  { id: 'title_captain_tester',  name: 'Капитан-испытатель',  rank: 'medium' },
  { id: 'title_duelist',         name: 'Дуэлянт',             rank: 'prestige' },
  { id: 'title_sea_strategist',  name: 'Морской стратег',     rank: 'high' },
  { id: 'title_first_time',      name: 'Время первых',        rank: 'medium' },
  { id: 'title_determined',      name: 'Целеустремлённый',    rank: 'prestige' },
  { id: 'title_collector',       name: 'Коллекционер',        rank: 'high' },
  { id: 'title_recruiter',       name: 'Рекрутер',            rank: 'medium' },
  { id: 'title_space_navigator', name: 'Космический навигатор',rank: 'high' },
  { id: 'title_engineer',        name: 'Инженер',             rank: 'high' },
  // Покупные
  { id: 'title_cyber_pirate',    name: 'Кибер-пират',         rank: 'medium',   price: 50 },
  { id: 'title_patron',          name: 'Меценат',             rank: 'prestige', price: 1000 },
  { id: 'title_davy_jones',      name: 'Дейви Джонс',         rank: 'medium',   price: 50 },
  { id: 'title_four_deck',       name: 'Четырёхпалубный',     rank: 'high',     price: 100 },
  { id: 'title_commander',       name: 'Главнокомандующий',   rank: 'prestige', price: 2500 },
];

// Специальная карточка "По умолчанию"
try {
  db.prepare(`INSERT OR IGNORE INTO shop_items (id,type,name,description,price_stars,sort_order,is_active)
    VALUES ('title_default','title','По умолчанию','Звание соответствует вашему уровню',NULL,0,1)`).run();
} catch(e) {}

// Миграция: добавляем колонку rank если нет
try { db.exec(`ALTER TABLE shop_items ADD COLUMN title_rank TEXT`); } catch(e) {}

// Вставляем все звания
// Покупные (price > 0) — is_active=1, видны в магазине
// Наградные (price = null) — is_active=0, НЕ видны в магазине, но есть в системе
for (const t of ACHIEVEMENT_TITLES) {
  try {
    const inShop = t.price ? 1 : 0; // только у которых есть цена — видны в магазине
    db.prepare(`INSERT OR IGNORE INTO shop_items (id,type,name,price_stars,sort_order,is_active,title_rank)
      VALUES (?,?,?,?,?,?,?)`).run(
        t.id, 'title', t.name, t.price || null, 100, inShop, t.rank
    );
    // Обновляем rank и is_active если уже есть
    db.prepare(`UPDATE shop_items SET title_rank=?, is_active=? WHERE id=?`).run(t.rank, inShop, t.id);
  } catch(e) { console.error('[Titles] migration:', t.id, e.message); }
}

// Хелпер: вернуть цвет звания по рангу
function titleRankColor(rank) {
  switch(rank) {
    case 'prestige': return '#C261FB';
    case 'high':     return '#D43838';
    case 'medium':   return '#4C7DD7';
    case 'initial':  return '#4EAA74';
    default:         return '#FFFFFF';
  }
}

// Хелпер: получить активное звание игрока (id + name + rank + color)
function getActiveTitle(userId) {
  const eq = db.prepare(`SELECT item_id FROM equipped WHERE user_id=? AND slot='title'`).get(userId);
  if (!eq || eq.item_id === 'title_default') return null; // null = "по умолчанию"
  const item = db.prepare(`SELECT name, title_rank FROM shop_items WHERE id=?`).get(eq.item_id);
  if (!item) return null;
  return { id: eq.item_id, name: item.name, rank: item.title_rank, color: titleRankColor(item.title_rank) };
}

// Хелпер: обновить прогресс достижения и выдать награду если нужно
function updateAchievementProgress(userId, countFn, increment = 1, extraData = {}) {
  if (!userId || userId.startsWith('guest_')) return [];
  const newAchievements = [];

  const relevant = ACHIEVEMENTS.filter(a => a.countFn === countFn);
  for (const ach of relevant) {
    const row = db.prepare(`SELECT * FROM achievements_progress WHERE user_id=? AND achievement_id=?`).get(userId, ach.id);

    if (ach.type === 'infinite') {
      // Пополняемые: просто считаем
      if (row) {
        db.prepare(`UPDATE achievements_progress SET times_done=times_done+?, notified=0 WHERE user_id=? AND achievement_id=?`).run(increment, userId, ach.id);
      } else {
        db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress,times_done,notified) VALUES (?,?,0,?,0)`).run(userId, ach.id, increment);
      }
      newAchievements.push({ id: ach.id, title: ach.title, type: 'infinite' });
      continue;
    }

    if (ach.type === 'once') {
      if (row?.completed_at) continue; // уже выполнено
      const newProg = (row?.progress || 0) + increment;
      if (newProg >= ach.goal) {
        if (row) {
          db.prepare(`UPDATE achievements_progress SET progress=?,completed_at=?,notified=0 WHERE user_id=? AND achievement_id=?`).run(ach.goal, Math.floor(Date.now()/1000), userId, ach.id);
        } else {
          db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress,completed_at,notified) VALUES (?,?,?,?,0)`).run(userId, ach.id, ach.goal, Math.floor(Date.now()/1000));
        }
        newAchievements.push({ id: ach.id, title: ach.title, type: 'once' });
      } else {
        if (row) {
          db.prepare(`UPDATE achievements_progress SET progress=? WHERE user_id=? AND achievement_id=?`).run(newProg, userId, ach.id);
        } else {
          db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress) VALUES (?,?,?)`).run(userId, ach.id, newProg);
        }
      }
      continue;
    }

    // limited
    if (row?.completed_at) continue; // уже выполнено, не перевыдаём
    const newProg = (row?.progress || 0) + increment;
    if (newProg >= ach.goal) {
      if (row) {
        db.prepare(`UPDATE achievements_progress SET progress=?,completed_at=?,notified=0 WHERE user_id=? AND achievement_id=?`).run(ach.goal, Math.floor(Date.now()/1000), userId, ach.id);
      } else {
        db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress,completed_at,notified) VALUES (?,?,?,?,0)`).run(userId, ach.id, ach.goal, Math.floor(Date.now()/1000));
      }
      // Выдаём награду (звание)
      if (ach.reward) {
        grantItem(userId, ach.reward, 'reward');
        console.log(`[Achievements] ${userId} earned: ${ach.id} → ${ach.reward}`);
      }
      newAchievements.push({ id: ach.id, title: ach.title, reward: ach.reward, type: 'limited' });
    } else {
      if (row) {
        db.prepare(`UPDATE achievements_progress SET progress=? WHERE user_id=? AND achievement_id=?`).run(newProg, userId, ach.id);
      } else {
        db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress) VALUES (?,?,?)`).run(userId, ach.id, newProg);
      }
    }
  }
  return newAchievements;
}

// Хелпер: получить все достижения игрока с прогрессом
function getAchievementsForUser(userId) {
  const rows = db.prepare(`SELECT * FROM achievements_progress WHERE user_id=?`).all(userId);
  const map = {};
  for (const r of rows) map[r.achievement_id] = r;

  return ACHIEVEMENTS.map(a => {
    const p = map[a.id] || {};
    return {
      id:           a.id,
      title:        a.title,
      desc:         a.desc,
      goal:         a.goal,
      type:         a.type,
      reward:       a.reward,
      hasRefPage:   a.hasRefPage || false,
      progress:     p.progress    || 0,
      times_done:   p.times_done  || 0,
      completed_at: p.completed_at || null,
      notified:     p.notified    || 0,
    };
  });
}

// Хелпер: кол-во незамеченных достижений
function getUnseenAchievementCount(userId) {
  return db.prepare(`SELECT COUNT(*) as n FROM achievements_progress WHERE user_id=? AND notified=0 AND (completed_at IS NOT NULL OR times_done>0)`).get(userId)?.n || 0;
}

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
    SELECT i.*, s.type, s.name, s.description, s.preview_url, s.title_rank,
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
  // Режим с другом — те же правила, но 70% от итога (антифарм)
  const friendMult = isFriend ? 0.7 : 1.0;

  if (result === 'win') {
    // Соперник сдался без единого выстрела — минимум
    if (loserShots === 0) {
      const base = Math.round(50 * friendMult);
      return { total: base, baseXp: base, bonusXp: 0 };
    }

    // Антифарм: полный XP только если потоплено >= 2 кораблей врага
    if (sunken < 2) {
      const partial = Math.round((200 + sunken * 150) * friendMult);
      return { total: partial, baseXp: partial, bonusXp: 0 };
    }

    // Нормальный бой: базовые 1000 + бонус за точность
    const acc = shots > 0 ? hits / shots : 0;
    let accBonus = 0;
    if (acc >= 0.50) accBonus = 500;
    else if (acc >= 0.45) accBonus = 300;
    else if (acc >= 0.40) accBonus = 150;
    const base  = Math.round(1000 * friendMult);
    const bonus = Math.round(accBonus * friendMult);
    return { total: base + bonus, baseXp: base, bonusXp: bonus };

  } else {
    // Сдался без выстрелов и без уничтоженных кораблей — 0 XP
    if (shots === 0 && sunken === 0) {
      return { total: 0, baseXp: 0, bonusXp: 0 };
    }

    // Стандарт: 300 + 10 за каждый потопленный корабль, максимум 400
    const total = Math.round(Math.min(400, 300 + 10 * sunken) * friendMult);
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

const MIN_LEGIT_ACCURACY = 0.25; // ниже 25% — слишком мало попаданий, не рейтинг
const MAX_LEGIT_ACCURACY = 0.60; // выше 60% — подозрительно
const MIN_RATED_SUNKEN   = 6;    // победитель должен потопить ≥ 6 кораблей

function addWin(id, shots, hits, isOnline = false, sunkenCount = 0, loserShots = null, isRated = true, isFriend = false, opponentAcc = null, shipsLeft = null) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return null;
  db.prepare(`UPDATE players SET wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+?,
    updated_at=strftime('%s','now') WHERE id=?`).run(shots, hits, id);
  let xpResult = null;
  const newAchievements = [];
  if (isOnline) {
    const acc = shots > 0 ? hits / shots : 0;

    db.prepare(`UPDATE players SET online_wins=online_wins+1,
      online_shots=online_shots+?, online_hits=online_hits+? WHERE id=?`).run(shots, hits, id);

    if (isRated) {
      const ratedOk = (sunkenCount || 0) >= MIN_RATED_SUNKEN
                   && acc >= MIN_LEGIT_ACCURACY
                   && acc <= MAX_LEGIT_ACCURACY;
      if (ratedOk) {
        const p = db.prepare(`SELECT rating_active FROM players WHERE id=?`).get(id);
        if (p?.rating_active === 1) {
          db.prepare(`UPDATE players SET rated_wins=rated_wins+1,
            rated_shots=rated_shots+?, rated_hits=rated_hits+? WHERE id=?`).run(shots, hits, id);
        }
      } else {
        console.log(`[RATING] Skipped: sunken=${sunkenCount} acc=${(acc*100).toFixed(1)}%`);
      }
    }

    const xpReward = calcXpReward('win', sunkenCount, shots, hits, loserShots ?? null, isFriend);
    xpResult = addXp(id, xpReward);

    // Достижения за победы
    const modeKey = isFriend ? 'friend_battles' : 'random_battles';
    newAchievements.push(...updateAchievementProgress(id, 'total_wins', 1));
    newAchievements.push(...updateAchievementProgress(id, 'total_battles', 1));
    newAchievements.push(...updateAchievementProgress(id, modeKey, 1));
    // Первый опыт
    newAchievements.push(...updateAchievementProgress(id, 'total_wins', 0)); // уже учтён выше

    // Точность выше соперника
    if (opponentAcc !== null && acc > opponentAcc) {
      newAchievements.push(...updateAchievementProgress(id, 'acc_win', 1));
    }
    // Последний шанс: победа при 1 корабле
    if (shipsLeft === 1) {
      newAchievements.push(...updateAchievementProgress(id, 'last_ship_win', 1));
    }
    // Уровень 30
    if (xpResult?.levelAfter >= 30) {
      newAchievements.push(...updateAchievementProgress(id, 'level_30', 1));
    }
    // Рефералы: обновить статистику приглашённого
    _updateReferralBattles(id);
  }

  if (xpResult) xpResult.newAchievements = newAchievements;
  return xpResult;
}

function addLoss(id, shots, hits, isOnline = false, sunkenCount = 0, isRated = true, isFriend = false) {
  id = normalizeId(id);
  if (!id || id.startsWith('guest_')) return null;
  db.prepare(`UPDATE players SET losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+?,
    updated_at=strftime('%s','now') WHERE id=?`).run(shots, hits, id);
  let xpResult = null;
  const newAchievements = [];
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

    // Достижения за бои (поражение тоже считается)
    const modeKey = isFriend ? 'friend_battles' : 'random_battles';
    newAchievements.push(...updateAchievementProgress(id, 'total_battles', 1));
    newAchievements.push(...updateAchievementProgress(id, modeKey, 1));
    _updateReferralBattles(id);
  }
  if (xpResult) xpResult.newAchievements = newAchievements;
  return xpResult;
}

// Хелпер: после каждого боя обновляем счётчик боёв реферала у пригласившего
function _updateReferralBattles(userId) {
  try {
    const ref = db.prepare(`SELECT * FROM referrals WHERE invitee_id=?`).get(userId);
    if (!ref || ref.qualified) return;
    const battles = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=?`).get(userId)?.n || 0;
    db.prepare(`UPDATE referrals SET battles_done=? WHERE invitee_id=?`).run(battles, userId);
    if (battles >= 3 && !ref.qualified) {
      db.prepare(`UPDATE referrals SET qualified=1 WHERE invitee_id=?`).run(userId);
      // Обновляем прогресс достижений реферера
      const newA = updateAchievementProgress(ref.inviter_id, 'referrals_qualified', 1);
      // Уведомляем пригласившего если он онлайн
      if (newA.length > 0) notifyUser(ref.inviter_id, 'new_achievement', newA);
    }
  } catch(e) { console.error('[Referral] update error:', e.message); }
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
  return rows.map(r => {
    const level = calcLevel(r.xp || 0);
    const title = getActiveTitle(r.id);
    return { ...r, level, rank: calcRank(level), titleName: title?.name || null, titleColor: title?.color || null };
  });
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
  const title1 = getActiveTitle(room.p1.playerId);
  const title2 = getActiveTitle(room.p2.playerId);
  io.to(room.p1.socketId).emit('matched', { roomId: room.id, opponent: { playerId: room.p2.playerId, name: room.p2.name, level: xp2.level, rank: xp2.rank, titleId: title2?.id || null, titleName: title2?.name || null, titleColor: title2?.color || null } });
  io.to(room.p2.socketId).emit('matched', { roomId: room.id, opponent: { playerId: room.p1.playerId, name: room.p1.name, level: xp1.level, rank: xp1.rank, titleId: title1?.id || null, titleName: title1?.name || null, titleColor: title1?.color || null } });
}

// п.6: запустить таймер хода для комнаты
function startTurnTimer(room) {
  clearTurnTimer(room);

  // Запоминаем момент старта хода — нужно для восстановления после реконнекта
  room._turnStartedAt = Date.now();

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
      const otherShipsLeft = otherPlayer.field ? countRemainingShips(otherPlayer.field) : 0;
      const winXpT  = addWin( otherPlayer.playerId,    otherPlayer.shots,    otherPlayer.hits,    true, toSunken,  null, !room.isFriend, room.isFriend, null, otherShipsLeft);
      const lossXpT = addLoss(timedOutPlayer.playerId, timedOutPlayer.shots, timedOutPlayer.hits, true, otherSunk, !room.isFriend, room.isFriend);
      addBattleHistory(otherPlayer.playerId,    'win',  timedOutPlayer.name || '?', otherPlayer.shots,    otherPlayer.hits,    'online');
      addBattleHistory(timedOutPlayer.playerId, 'loss', otherPlayer.name    || '?', timedOutPlayer.shots, timedOutPlayer.hits, 'online');
      if (winXpT  && otherPlayer.socketId)    io.to(otherPlayer.socketId).emit('xp_reward', winXpT);
      if (lossXpT && timedOutPlayer.socketId) io.to(timedOutPlayer.socketId).emit('xp_reward', lossXpT);
      if (winXpT?.newAchievements?.length  && otherPlayer.socketId)    io.to(otherPlayer.socketId).emit('new_achievement', winXpT.newAchievements);
      if (lossXpT?.newAchievements?.length && timedOutPlayer.socketId) io.to(timedOutPlayer.socketId).emit('new_achievement', lossXpT.newAchievements);
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
      // Убираем себя из пула если уже есть (повторный вход в поиск)
      const selfIdx = waitingPool.findIndex(p => p.playerId === cleanPlayerId);
      if (selfIdx >= 0) waitingPool.splice(selfIdx, 1);

      // Чистим мёртвые сокеты из пула — игроки которые давно ждут но уже отключились
      for (let i = waitingPool.length - 1; i >= 0; i--) {
        const sock = io.sockets.sockets.get(waitingPool[i].socketId);
        if (!sock || !sock.connected) {
          console.log(`[matchmake] removing stale socket from pool: ${waitingPool[i].playerId}`);
          waitingPool.splice(i, 1);
        }
      }

      // Ищем живого соперника
      const oppIdx = waitingPool.findIndex(p => p.playerId !== cleanPlayerId);
      if (oppIdx >= 0) {
        const opp    = waitingPool.splice(oppIdx, 1)[0];
        const roomId = crypto.randomUUID();
        const room   = { id: roomId, p1: makePlayer(info), p2: makePlayer(opp), turn: cleanPlayerId, started: false, over: false, _turnTimer: null, _warnTimer: null };
        rooms.set(roomId, room);
        socket.join(roomId);
        io.sockets.sockets.get(opp.socketId)?.join(roomId);
        notifyBothMatched(room);
        console.log(`[matchmake] matched ${cleanPlayerId} ↔ ${opp.playerId}`);
      } else {
        waitingPool.push(info);
        console.log(`[matchmake] ${cleanPlayerId} waiting (pool size: ${waitingPool.length})`);
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
      const shooterAcc = shooter.shots > 0 ? shooter.hits / shooter.shots : 0;
      const targetAcc  = target.shots  > 0 ? target.hits  / target.shots  : 0;
      const shooterShipsLeft = countRemainingShips(shooter.field);
      const winXp  = addWin( shooter.playerId, shooter.shots, shooter.hits, true, shooterSunken, null, !room.isFriend, room.isFriend, targetAcc, shooterShipsLeft);
      const lossXp = addLoss(target.playerId,  target.shots,  target.hits,  true, targetSunken, !room.isFriend, room.isFriend);
      // Записываем историю боя на сервере (не зависим от HTTP-запроса клиента)
      addBattleHistory(shooter.playerId, 'win',  target.name  || '?', shooter.shots, shooter.hits, 'online');
      addBattleHistory(target.playerId,  'loss', shooter.name || '?', target.shots,  target.hits,  'online');
      recordDuelResult(shooter.playerId, target.playerId);
      checkRatingTop1(shooter.playerId);
      // Отправляем XP каждому игроку
      if (winXp  && shooter.socketId) io.to(shooter.socketId).emit('xp_reward', winXp);
      if (lossXp && target.socketId)  io.to(target.socketId).emit('xp_reward', lossXp);
      // Уведомляем об новых достижениях
      if (winXp?.newAchievements?.length  && shooter.socketId) io.to(shooter.socketId).emit('new_achievement', winXp.newAchievements);
      if (lossXp?.newAchievements?.length && target.socketId)  io.to(target.socketId).emit('new_achievement', lossXp.newAchievements);
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
    const winnerShipsLeft2 = winner.field ? countRemainingShips(winner.field) : 0;
    const winXp2  = addWin( winner.playerId,      winner.shots,      winner.hits,      true, wSunken, surrenderer.shots, !room.isFriend, room.isFriend, null, winnerShipsLeft2);
    const lossXp2 = addLoss(surrenderer.playerId, surrenderer.shots, surrenderer.hits, true, lSunken, !room.isFriend, room.isFriend);
    // Записываем историю
    addBattleHistory(winner.playerId,      'win',  surrenderer.name || '?', winner.shots,      winner.hits,      'online');
    addBattleHistory(surrenderer.playerId, 'loss', winner.name      || '?', surrenderer.shots, surrenderer.hits, 'online');
    if (winXp2  && winner.socketId)      io.to(winner.socketId).emit('xp_reward', winXp2);
    if (lossXp2 && surrenderer.socketId) io.to(surrenderer.socketId).emit('xp_reward', lossXp2);
    if (winXp2?.newAchievements?.length  && winner.socketId)      io.to(winner.socketId).emit('new_achievement', winXp2.newAchievements);
    if (lossXp2?.newAchievements?.length && surrenderer.socketId) io.to(surrenderer.socketId).emit('new_achievement', lossXp2.newAchievements);
    recordDuelResult(winner.playerId, surrenderer.playerId);
    checkRatingTop1(winner.playerId);
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
      if (room._rematchTimer) { clearTimeout(room._rematchTimer); room._rematchTimer = null; }
      room.rematch = null;

      // Отменяем таймер дисконнекта если был
      if (room.p1?._disconnectTimer) { clearTimeout(room.p1._disconnectTimer); room.p1._disconnectTimer = null; }
      if (room.p2?._disconnectTimer) { clearTimeout(room.p2._disconnectTimer); room.p2._disconnectTimer = null; }

      // Сбрасываем состояние комнаты для новой игры
      room.over    = false;
      room.started = false;
      room.p1.field = null; room.p1.ready = false; room.p1.shots = 0; room.p1.hits = 0; room.p1.timeouts = 0; room.p1.disconnected = false;
      room.p2.field = null; room.p2.ready = false; room.p2.shots = 0; room.p2.hits = 0; room.p2.timeouts = 0; room.p2.disconnected = false;

      // Отправляем актуальный счёт дуэлей вместе с rematch_accepted
      const duel1 = getDuelStats(room.p1.playerId, room.p2.playerId);
      const duel2 = getDuelStats(room.p2.playerId, room.p1.playerId);
      io.to(room.p1.socketId).emit("rematch_accepted", { duelStats: duel1 });
      io.to(room.p2.socketId).emit("rematch_accepted", { duelStats: duel2 });
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

  // Переподключение: клиент шлёт reconnect с сохранёнными roomId + playerId
  socket.on('reconnect_game', ({ roomId, playerId }) => {
    const normId = normalizeId(playerId);
    if (!normId || !roomId) return;

    const room = rooms.get(roomId);
    if (!room || room.over) {
      socket.emit('reconnect_failed', { reason: 'room_gone' });
      return;
    }

    // Ищем игрока в комнате по playerId
    let player = null;
    if (room.p1?.playerId === normId) player = room.p1;
    else if (room.p2?.playerId === normId) player = room.p2;

    if (!player) {
      socket.emit('reconnect_failed', { reason: 'not_in_room' });
      return;
    }

    // Отменяем таймер дисконнекта если был
    if (player._disconnectTimer) {
      clearTimeout(player._disconnectTimer);
      player._disconnectTimer = null;
      console.log(`[reconnect] ${normId} reconnected in time`);
    }

    const oldSocketId = player.socketId;
    player.socketId   = socket.id;
    player.disconnected = false;
    socket.join(roomId);

    // Обновляем онлайн-сессию
    onlineSessions.set(socket.id, { socketId: socket.id, playerId: normId, identified: true, connectedAt: Date.now(), lastActive: Date.now() });
    broadcastOnlineCount();

    const opponent = player === room.p1 ? room.p2 : room.p1;

    // Считаем сколько секунд осталось на текущий ход
    let turnSecondsLeft = null;
    if (room.started && room._turnStartedAt) {
      const elapsed = Math.floor((Date.now() - room._turnStartedAt) / 1000);
      turnSecondsLeft = Math.max(0, TURN_TIMEOUT_MS / 1000 - elapsed);
    }

    // Отправляем игроку текущее состояние
    socket.emit('reconnect_ok', {
      roomId,
      started:        room.started,
      isMyTurn:       room.started ? (room.turn === normId) : false,
      turnSecondsLeft,                       // сколько секунд осталось на ход (null если игра не идёт)
      myField:        player.field,
      oppField:       opponent?.field ? opponent.field.map(row => row.map(c => c === 1 ? 0 : c)) : null,
      myShots:        player.shots,
      myHits:         player.hits,
      opponent:       opponent ? { playerId: opponent.playerId, name: opponent.name } : null,
    });

    // Уведомляем соперника о возвращении
    if (opponent?.socketId) {
      io.to(opponent.socketId).emit('opponent_reconnected');
    }

    // Если сейчас ход переподключившегося и осталось <= 20 сек — сразу шлём предупреждение
    if (room.started && room.turn === normId && turnSecondsLeft !== null && turnSecondsLeft <= 20) {
      socket.emit('turn_warning', { secondsLeft: Math.ceil(turnSecondsLeft) });
    }

    console.log(`[reconnect] ${normId} restored in room ${roomId}, turnLeft=${turnSecondsLeft}s`);
  });

  socket.on('disconnect', () => {
    console.log(`[-] ${socket.id}`);
    onlineSessions.delete(socket.id);
    broadcastOnlineCount();
    const idx = waitingPool.findIndex(p => p.socketId === socket.id);
    if (idx >= 0) waitingPool.splice(idx, 1);

    for (const [roomId, room] of rooms) {
      if (room.over) continue;
      if (room.p1?.socketId !== socket.id && room.p2?.socketId !== socket.id) continue;

      const leaver = room.p1?.socketId === socket.id ? room.p1 : room.p2;
      const stayer = room.p1?.socketId === socket.id ? room.p2 : room.p1;

      if (room.started && stayer?.socketId) {
        // ── Игра шла — даём 60 секунд на переподключение ──────────────────
        leaver.disconnected = true;
        // НЕ останавливаем таймер хода — он продолжает тикать.

        // Уведомляем оставшегося игрока что у соперника проблемы с соединением
        io.to(stayer.socketId).emit('opponent_disconnected_wait', { seconds: 60 });

        console.log(`[disconnect] ${leaver.playerId} disconnected — waiting 60s`);

        leaver._disconnectTimer = setTimeout(() => {
          if (room.over) return;
          if (!leaver.disconnected) return; // уже переподключился

          // 60 секунд истекли — поражение вышедшему
          room.over = true;
          clearTurnTimer(room);

          io.to(stayer.socketId).emit('opponent_disconnected_win');

          const dSunkenStayer = leaver.field ? countSunkenShips(leaver.field) : 0;
          const dSunkenLeaver = stayer.field ? countSunkenShips(stayer.field) : 0;
          const stayerShipsLeft = stayer.field ? countRemainingShips(stayer.field) : 0;
          const dWinXp  = addWin( stayer.playerId, stayer.shots, stayer.hits, true, dSunkenStayer, null, !room.isFriend, room.isFriend, null, stayerShipsLeft);
          const dLossXp = addLoss(leaver.playerId, leaver.shots, leaver.hits, true, dSunkenLeaver, !room.isFriend, room.isFriend);
          addBattleHistory(stayer.playerId, 'win',  leaver.name || '?', stayer.shots, stayer.hits, 'online');
          addBattleHistory(leaver.playerId, 'loss', stayer.name || '?', leaver.shots, leaver.hits, 'online');
          if (dWinXp  && stayer.socketId) io.to(stayer.socketId).emit('xp_reward', dWinXp);
          if (dWinXp?.newAchievements?.length && stayer.socketId) io.to(stayer.socketId).emit('new_achievement', dWinXp.newAchievements);
          recordDuelResult(stayer.playerId, leaver.playerId);
          rooms.delete(roomId);
          console.log(`[disconnect] ${leaver.playerId} timed out — ${stayer.playerId} wins`);
        }, 60 * 1000);

      } else if (stayer?.socketId) {
        // Игра не началась, второй игрок есть — уведомляем и удаляем
        room.over = true;
        clearTurnTimer(room);
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

// Считаем живые корабли (клетки со значением 1, flood-fill группами)
function countRemainingShips(field) {
  if (!field) return 0;
  const visited = new Set();
  let count = 0;
  for (let r = 0; r < 10; r++) {
    for (let c = 0; c < 10; c++) {
      if (field[r]?.[c] === 1 && !visited.has(r+','+c)) {
        count++;
        const stack = [[r, c]];
        while (stack.length) {
          const [cr, cc] = stack.pop();
          const key = cr+','+cc;
          if (visited.has(key)) continue;
          visited.add(key);
          for (const [nr, nc] of [[cr-1,cc],[cr+1,cc],[cr,cc-1],[cr,cc+1]])
            if (nr>=0&&nr<10&&nc>=0&&nc<10&&field[nr]?.[nc]===1) stack.push([nr,nc]);
        }
      }
    }
  }
  return count;
}

// ─── SEO ─────────────────────────────────────────────────────────────────────

const SITE_URL = process.env.SITE_URL || 'https://morskoy-boy.ru';

// robots.txt
app.get('/robots.txt', (req, res) => {
  res.type('text/plain');
  res.send([
    'User-agent: *',
    'Allow: /',
    'Disallow: /api/',
    '',
    `Sitemap: ${SITE_URL}/sitemap.xml`,
    '',
    '# Yandex',
    'User-agent: Yandex',
    'Allow: /',
    'Disallow: /api/',
    'Clean-param: roomId&playerId&mode',
    `Sitemap: ${SITE_URL}/sitemap.xml`,
  ].join('\n'));
});

// sitemap.xml
app.get('/sitemap.xml', (req, res) => {
  const now = new Date().toISOString().split('T')[0];
  res.type('application/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:xhtml="http://www.w3.org/1999/xhtml">
  <url>
    <loc>${SITE_URL}/</loc>
    <lastmod>${now}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
</urlset>`);
});

// Favicon — явный маршрут чтобы Яндекс точно находил
app.get('/favicon.ico', (req, res) => res.sendFile(path.join(__dirname, 'public', 'favicon.ico')));
app.get('/favicon-32.png', (req, res) => res.sendFile(path.join(__dirname, 'public', 'favicon-32.png')));
app.get('/favicon-16.png', (req, res) => res.sendFile(path.join(__dirname, 'public', 'favicon-16.png')));

app.get('/api/config',     (req, res) => res.json({ botUsername: BOT_USERNAME, appName: APP_NAME }));
app.get('/api/online',     (req, res) => res.json({ count: getOnlineCount() }));
app.get('/api/history/:id',(req, res) => { try { const mode = req.query.mode || null; res.json({ ok: true, data: getBattleHistory(req.params.id, 30, mode) }); } catch(e) { res.status(500).json({ ok: false }); } });
app.post('/api/history', (req, res) => {
  try {
    const { id, result, opponent, shots, hits, skipStats, mode } = req.body;
    const cleanId = normalizeId(id);
    if (!cleanId || cleanId.startsWith('guest_')) { res.json({ ok: false }); return; }
    // Убеждаемся что игрок существует в таблице players (только создаём если нет)
    if (!db.prepare('SELECT 1 FROM players WHERE id=?').get(cleanId)) {
      upsertPlayer(cleanId, 'Игрок');
    }
    // Валидация enum-полей
    const cleanResult = ['win','loss','draw'].includes(result) ? result : null;
    if (!cleanResult) { res.json({ ok: false }); return; }
    const cleanMode     = ['online','bot-easy','bot-hard','friend','friend_join'].includes(mode) ? mode : 'online';
    const cleanOpponent = (s => String(s||'').trim().slice(0,64))(opponent || '?');
    const cleanShots    = Math.max(0, parseInt(shots)  || 0);
    const cleanHits     = Math.max(0, parseInt(hits)   || 0);
    const gameMode = cleanMode;
    addBattleHistory(cleanId, cleanResult, cleanOpponent, cleanShots, cleanHits, gameMode);
    const newAchievements = [];
    if (!skipStats) {
      console.log(`[History] ${cleanId} mode=${cleanMode} result=${cleanResult} skipStats=${skipStats}`);
      if (cleanResult === 'win') {
        db.prepare(`UPDATE players SET wins=wins+1, total_shots=total_shots+?, total_hits=total_hits+?, updated_at=strftime('%s','now') WHERE id=?`).run(cleanShots, cleanHits, cleanId);
        newAchievements.push(...updateAchievementProgress(cleanId, 'total_wins', 1));
        newAchievements.push(...updateAchievementProgress(cleanId, 'total_battles', 1));
        if (cleanMode.startsWith('bot')) {
          newAchievements.push(...updateAchievementProgress(cleanId, 'bot_wins', 1));
        }
      } else if (cleanResult === 'loss') {
        db.prepare(`UPDATE players SET losses=losses+1, total_shots=total_shots+?, total_hits=total_hits+?, updated_at=strftime('%s','now') WHERE id=?`).run(cleanShots, cleanHits, cleanId);
        newAchievements.push(...updateAchievementProgress(cleanId, 'total_battles', 1));
      }
      if (newAchievements.length) {
        console.log(`[History] newAchievements for ${cleanId}:`, newAchievements.map(a => a.id));
      }
      // Обновляем реферальный счётчик — считаются все режимы включая боты
      _updateReferralBattles(cleanId);
    }
    res.json({ ok: true, newAchievements });
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
// Вспомогательная функция: разбиваем онлайн на ТГ и гостей
function getOnlineSplit() {
  const now = Date.now();
  let tg = 0, guests = 0;
  const seenTg = new Set(), seenGuest = new Set();
  for (const [, s] of onlineSessions) {
    if (now - s.lastActive > IDLE_TIMEOUT_MS) continue;
    if (s.playerId && !s.playerId.startsWith('guest_')) {
      if (!seenTg.has(s.playerId)) { seenTg.add(s.playerId); tg++; }
    } else {
      const key = s.playerId || s.socketId;
      if (!seenGuest.has(key)) { seenGuest.add(key); guests++; }
    }
  }
  return { tg, guests, total: tg + guests };
}

app.get('/api/admin/analytics', (req, res) => {
  try {
    const secret = req.headers['x-admin-secret'] || req.query.secret;
    if (!SHOP_SECRET || secret !== SHOP_SECRET) {
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }

    const now     = Math.floor(Date.now() / 1000);
    const day     = 86400;
    const since24 = now - day;   // последние 24 часа
    const since7d = now - day * 7;
    const since30d= now - day * 30;

    // ── Общие игроки (только ТГ — реальные аккаунты, не гости) ──
    // Регистрация = первый запуск бота и игры (запись в players)
    const totalPlayers = db.prepare(`SELECT COUNT(*) as n FROM players WHERE id NOT LIKE 'guest_%'`).get().n;
    // Новые за 24ч = появились в players за последние 24 часа (первый раз запустили)
    const newToday     = db.prepare(`
      SELECT COUNT(*) as n FROM players
      WHERE id NOT LIKE 'guest_%'
        AND rowid IN (SELECT MIN(rowid) FROM players GROUP BY id)
        AND updated_at >= ?
    `).get(since24).n;
    // Активные = сыграли хотя бы 1 бой (онлайн или с другом, не боты)
    const activeWeek  = db.prepare(`
      SELECT COUNT(DISTINCT player_id) as n FROM battle_history
      WHERE date >= ? AND player_id NOT LIKE 'guest_%'
        AND mode IN ('online','friend','friend_join')
    `).get(since7d).n;
    const activeMonth = db.prepare(`
      SELECT COUNT(DISTINCT player_id) as n FROM battle_history
      WHERE date >= ? AND player_id NOT LIKE 'guest_%'
        AND mode IN ('online','friend','friend_join')
    `).get(since30d).n;

    // ── ТГ-специфичные метрики ──
    // Бои в ТГ = все бои от зарегистрированных игроков (не гостей)
    const tgBattlesToday = db.prepare(`
      SELECT COUNT(*) as n FROM battle_history
      WHERE date >= ? AND mode='online' AND result='win' AND player_id NOT LIKE 'guest_%'
    `).get(since24).n;
    const tgFriendBattlesToday = db.prepare(`
      SELECT COUNT(*) as n FROM battle_history
      WHERE date >= ? AND mode IN ('friend','friend_join') AND result='win'
    `).get(since24).n;

    // ── Браузерные гости ──
    // Уникальные гости за 24ч: нет таблицы, считаем через battle_history
    // Гостевые бои — тоже по win (но гость может быть и победителем и проигравшим)
    // Используем: бой есть если хотя бы один из участников гость
    // Проще всего: все записи mode='online' где player_id=guest_ и result='win'
    // ИЛИ там где оба гости (нет ТГ-победителя) — берём любую одну запись
    // Упрощаем: count(win) по гостям + count(win) по НЕгостям в боях где победитель не гость (это 0)
    // Итого: просто count где result='win' и mode='online' и НЕ считали выше
    const guestBattlesToday = db.prepare(`
      SELECT COUNT(*) as n FROM battle_history
      WHERE date >= ? AND mode='online' AND result='win' AND player_id LIKE 'guest_%'
    `).get(since24).n;
    const guestBattlesWeek = db.prepare(`
      SELECT COUNT(*) as n FROM battle_history
      WHERE date >= ? AND mode='online' AND result='win' AND player_id LIKE 'guest_%'
    `).get(since7d).n;
    const guestBattlesMonth = db.prepare(`
      SELECT COUNT(*) as n FROM battle_history
      WHERE date >= ? AND mode='online' AND result='win' AND player_id LIKE 'guest_%'
    `).get(since30d).n;
    const guestBattlesTotal = db.prepare(`
      SELECT COUNT(*) as n FROM battle_history WHERE mode='online' AND result='win' AND player_id LIKE 'guest_%'
    `).get().n;

    // ── Бои случайный режим (онлайн) — только зарегистрированные ──
    // battle_history пишет по 2 записи на бой (победитель + проигравший) — делим на 2
    // Случайные онлайн-бои: mode='online', result='win', НЕ боты
    // mode='online' пишется сервером напрямую (socket) — это точно pvp
    // Исключаем записи где opponent содержит 'бот' — на случай если клиент прислал mode='online' для бота
    const battlesToday = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode='online' AND result='win' AND opponent NOT LIKE '%бот%' AND opponent NOT LIKE '%bot%'`).get(since24).n;
    const battles7d    = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode='online' AND result='win' AND opponent NOT LIKE '%бот%' AND opponent NOT LIKE '%bot%'`).get(since7d).n;
    const battles30d   = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode='online' AND result='win' AND opponent NOT LIKE '%бот%' AND opponent NOT LIKE '%bot%'`).get(since30d).n;
    const battlesTotal = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE mode='online' AND result='win' AND opponent NOT LIKE '%бот%' AND opponent NOT LIKE '%bot%'`).get().n;

    // ── Бои с другом по ссылке — ТГ и браузер раздельно ──
    // Бои с другом — тоже по result='win' (1 на бой)
    const friendTgToday   = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode IN ('friend','friend_join') AND result='win'`).get(since24).n;
    const friendTg7d      = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode IN ('friend','friend_join') AND result='win'`).get(since7d).n;
    const friendTg30d     = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode IN ('friend','friend_join') AND result='win'`).get(since30d).n;
    const friendTgTotal   = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE mode IN ('friend','friend_join') AND result='win'`).get().n;
    // Гостевые бои с другом — тут оба могут быть гостями, считаем по win
    const friendGuestToday= db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE date >= ? AND mode IN ('friend','friend_join') AND result='win' AND (player_id LIKE 'guest_%' OR (SELECT COUNT(*) FROM battle_history b2 WHERE b2.date=battle_history.date AND b2.mode=battle_history.mode AND b2.player_id LIKE 'guest_%')>0)`).get(since24).n;
    const friendGuestTotal= db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE mode IN ('friend','friend_join') AND result='win' AND (player_id LIKE 'guest_%' OR (SELECT COUNT(*) FROM battle_history b2 WHERE b2.date=battle_history.date AND b2.mode=battle_history.mode AND b2.player_id LIKE 'guest_%')>0)`).get().n;

    // ── Бои по дням (последние 7) с разбивкой онлайн/друг ──
    const byDay = db.prepare(`
      SELECT
        date(date, 'unixepoch', 'localtime') as day,
        SUM(CASE WHEN mode='online' THEN 1 ELSE 0 END) as online,
        SUM(CASE WHEN mode IN ('friend','friend_join') THEN 1 ELSE 0 END) as friend
      FROM battle_history
      WHERE date >= ? AND player_id NOT LIKE 'guest_%'
      GROUP BY day ORDER BY day ASC
    `).all(since7d);

    // ── Точность (по всем боям: онлайн + с другом, все игроки) ──
    const accRow = db.prepare(`
      SELECT
        ROUND(100.0 * SUM(total_hits) / NULLIF(SUM(total_shots), 0), 1) as avg_acc,
        SUM(total_shots) as total_shots,
        SUM(total_hits)  as total_hits
      FROM players WHERE id NOT LIKE 'guest_%'
    `).get();

    // ── Винрейт (из battle_history — все режимы онлайн + друг) ──
    const vrRow = db.prepare(`
      SELECT
        SUM(CASE WHEN result='win'  THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses
      FROM battle_history
      WHERE mode IN ('online','friend','friend_join')
        AND player_id NOT LIKE 'guest_%'
    `).get();
    const totalBattleRows = (vrRow.wins || 0) + (vrRow.losses || 0);
    const winrate = totalBattleRows > 0 ? Math.round(100 * vrRow.wins / totalBattleRows) : 0;

    // ── Покупки ──
    const purchasesToday = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchased_at >= ? AND purchase_type='stars' AND is_active=1`).get(since24).n;
    const purchases7d    = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchased_at >= ? AND purchase_type='stars' AND is_active=1`).get(since7d).n;
    const purchases30d   = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchased_at >= ? AND purchase_type='stars' AND is_active=1`).get(since30d).n;
    const purchasesTotal = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchase_type='stars' AND is_active=1`).get().n;
    const refundsTotal   = db.prepare(`SELECT COUNT(*) as n FROM inventory WHERE purchase_type='stars' AND is_active=0`).get().n;

    // ── Топ-5 товаров ──
    const topItems = db.prepare(`
      SELECT i.item_id, s.name, COUNT(*) as cnt
      FROM inventory i LEFT JOIN shop_items s ON s.id = i.item_id
      WHERE i.purchase_type='stars' AND i.is_active=1
      GROUP BY i.item_id ORDER BY cnt DESC LIMIT 5
    `).all();

    // ── Онлайн прямо сейчас с разбивкой ──
    const onlineSplit = getOnlineSplit();

    res.json({ ok: true, data: {
      ts: now,
      online_now:     onlineSplit.total,
      online_tg:      onlineSplit.tg,
      online_guests:  onlineSplit.guests,
      players: {
        total:     totalPlayers,
        new_24h:   newToday,
        active_7d: activeWeek,
        active_30d: activeMonth,
      },
      tg: {
        players:       totalPlayers,
        new_24h:       newToday,
        active_7d:     activeWeek,
        active_30d:    activeMonth,
        battles_today: tgBattlesToday,
        friend_battles_today: tgFriendBattlesToday,
      },
      browser: {
        // Гостей не регистрируем, считаем уникальных через бои
        guest_battles_today: guestBattlesToday,
        guest_battles_week:  guestBattlesWeek,
        guest_battles_month: guestBattlesMonth,
        guest_battles_total: guestBattlesTotal,
        friend_battles_today:  friendGuestToday,
        friend_battles_total:  friendGuestTotal,
      },
      battles: {
        today: battlesToday, week: battles7d, month: battles30d, total: battlesTotal,
        by_day: byDay,
      },
      friend_battles: {
        tg_today:    friendTgToday,   tg_week:  friendTg7d,  tg_month: friendTg30d,  tg_total: friendTgTotal,
        guest_today: friendGuestToday, guest_total: friendGuestTotal,
        today: friendTgToday + friendGuestToday,
        week:  friendTg7d,
        month: friendTg30d,
        total: friendTgTotal + friendGuestTotal,
      },
      accuracy:  { avg_pct: accRow.avg_acc, total_shots: accRow.total_shots, total_hits: accRow.total_hits },
      winrate:   { pct: winrate, wins: vrRow.wins, losses: vrRow.losses },
      purchases: { today: purchasesToday, week: purchases7d, month: purchases30d, total: purchasesTotal, refunds: refundsTotal, top_items: topItems },
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

// Диагностика: разбивка battle_history по mode и result за сегодня (только с секретом)
app.get('/api/debug/battles', (req, res) => {
  try {
    const secret = req.headers['x-admin-secret'] || req.query.secret;
    if (!SHOP_SECRET || secret !== SHOP_SECRET) return res.status(403).json({ ok: false });
    const now = Math.floor(Date.now() / 1000);
    const since24 = now - 86400;
    const breakdown = db.prepare(`
      SELECT mode, result, COUNT(*) as n
      FROM battle_history
      WHERE date >= ?
      GROUP BY mode, result
      ORDER BY mode, result
    `).all(since24);
    const sample = db.prepare(`
      SELECT id, player_id, result, opponent, mode, datetime(date,'unixepoch','localtime') as dt
      FROM battle_history
      WHERE date >= ? AND mode='online' AND result='win'
      ORDER BY date DESC LIMIT 20
    `).all(since24);
    res.json({ ok: true, breakdown, sample_online_wins: sample });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// DEBUG — смотрим реальное состояние БД для игрока (только с секретом)
// Диагностика — доступна для админа без секрета
app.get('/api/debug/player/:userId', (req, res) => {
  try {
    const userId = normalizeId(req.params.userId);
    const secret = req.headers['x-admin-secret'] || req.query.secret;
    const ok = isAdmin(userId) || (SHOP_SECRET && secret === SHOP_SECRET);
    if (!ok) return res.status(403).json({ ok: false });

    const player       = db.prepare(`SELECT id, name, xp, wins, losses, total_shots, total_hits FROM players WHERE id=?`).get(userId);
    const progress     = db.prepare(`SELECT achievement_id, progress, completed_at, times_done FROM achievements_progress WHERE user_id=? ORDER BY achievement_id`).all(userId);
    const inventory    = db.prepare(`SELECT i.item_id, i.purchase_type, i.purchased_at, s.name FROM inventory i LEFT JOIN shop_items s ON s.id=i.item_id WHERE i.user_id=? AND i.is_active=1 ORDER BY i.purchased_at DESC`).all(userId);
    const history      = db.prepare(`SELECT mode, result, COUNT(*) as n FROM battle_history WHERE player_id=? GROUP BY mode, result`).all(userId);
    const totalHistory = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=?`).get(userId)?.n || 0;
    const shopTitles   = db.prepare(`SELECT id, name, is_active, title_rank FROM shop_items WHERE type='title' ORDER BY id`).all();

    res.json({ ok: true, player, progress, inventory, history, totalHistory, shopTitles });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Принудительная синхронизация достижений для игрока
app.post('/api/achievements/force-sync/:userId', (req, res) => {
  try {
    const userId = normalizeId(req.params.userId);
    if (!userId || userId.startsWith('guest_')) return res.status(400).json({ ok: false });
    // Верификация через initData или admin secret
    const secret = req.headers['x-admin-secret'];
    const initData = req.headers['x-telegram-init-data'];
    const verifiedId = initData ? verifyTelegramInitData(initData) : null;
    if (!secret && (!verifiedId || verifiedId !== userId) && !isAdmin(userId)) {
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }
    _syncAchievementProgress(userId);
    const data = getAchievementsForUser(userId);
    res.json({ ok: true, data });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});


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
    const rawUserId = req.params.userId;
    if (!rawUserId || rawUserId.startsWith('guest_')) return res.json({ ok: true, data: { items: [], equipped: {} } });
    // Нормализуем сразу — убираем .0 и лишние символы
    const userId = normalizeId(rawUserId);
    if (!userId) return res.json({ ok: true, data: { items: [], equipped: {} } });

    const initData   = req.headers['x-telegram-init-data'];
    const verifiedId = initData ? verifyTelegramInitData(initData) : null;

    console.log(`[Inventory] userId=${userId} initData=${!!initData} verifiedId=${verifiedId} isAdmin=${isAdmin(userId)}`);

    if (verifiedId && verifiedId !== userId) {
      console.log(`[Inventory] BLOCKED: verifiedId=${verifiedId} !== userId=${userId}`);
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }

    // Админ — весь каталог ПОКУПНЫХ предметов + реальный инвентарь (наградные звания)
    if (isAdmin(userId)) {
      // Синхронизируем достижения и для админа тоже
      _syncAchievementProgress(userId);
      // Выдаём title_engineer если ещё нет
      grantItem(userId, 'title_engineer', 'admin');
      const allItems = db.prepare(`SELECT * FROM shop_items WHERE is_active=1 AND (type != 'title' OR price_stars IS NOT NULL) ORDER BY sort_order`).all();
      // Реальный инвентарь — наградные звания (is_active=0 в shop_items)
      const realInv = getInventory(userId);
      const realIds = new Set(realInv.map(i => i.item_id));
      // Добавляем title_engineer отдельно (он is_active=0)
      const engineerItem = db.prepare(`SELECT * FROM shop_items WHERE id='title_engineer'`).get();
      const fakeInv = [
        ...(engineerItem && !realIds.has('title_engineer') ? [{ ...engineerItem, item_id: 'title_engineer', purchase_type: 'admin', is_active: 1, is_equipped: 0 }] : []),
        ...allItems.map(i => ({ ...i, item_id: i.id, purchase_type: 'admin', is_active: 1, is_equipped: 0 })),
        // Добавляем реальные наградные предметы которых нет в каталоге
        ...realInv.filter(i => !allItems.find(s => s.id === i.item_id) && i.item_id !== 'title_engineer'),
      ];
      return res.json({ ok: true, data: { items: fakeInv, equipped: getEquipped(userId) } });
    }

    // Синхронизируем прогресс достижений и выдаём заработанные награды
    _syncAchievementProgress(userId);
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

      // Достижение Коллекционер — считаем купленные темы
      try {
        const item = db.prepare(`SELECT type FROM shop_items WHERE id=?`).get(invoice.item_id);
        if (item?.type === 'theme') {
          const themesCount = db.prepare(`SELECT COUNT(*) as n FROM inventory i JOIN shop_items s ON s.id=i.item_id WHERE i.user_id=? AND s.type='theme' AND i.is_active=1 AND i.purchase_type='stars'`).get(invoice.user_id)?.n || 0;
          // updateAchievementProgress принимает абсолютное значение через прямой update
          const achRow = db.prepare(`SELECT * FROM achievements_progress WHERE user_id=? AND achievement_id='collector'`).get(invoice.user_id);
          if (achRow) {
            if (!achRow.completed_at) {
              db.prepare(`UPDATE achievements_progress SET progress=?,notified=0 WHERE user_id=? AND achievement_id='collector'`).run(themesCount, invoice.user_id);
              if (themesCount >= 3) {
                db.prepare(`UPDATE achievements_progress SET completed_at=? WHERE user_id=? AND achievement_id='collector'`).run(Math.floor(Date.now()/1000), invoice.user_id);
                grantItem(invoice.user_id, 'title_collector', 'reward');
                notifyUser(invoice.user_id, 'new_achievement', [{ id: 'collector', title: 'Коллекционер', reward: 'title_collector' }]);
              }
            }
          } else {
            db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress,notified) VALUES (?,?,?,0)`).run(invoice.user_id, 'collector', themesCount);
            if (themesCount >= 3) {
              db.prepare(`UPDATE achievements_progress SET completed_at=?,notified=0 WHERE user_id=? AND achievement_id='collector'`).run(Math.floor(Date.now()/1000), invoice.user_id);
              grantItem(invoice.user_id, 'title_collector', 'reward');
              notifyUser(invoice.user_id, 'new_achievement', [{ id: 'collector', title: 'Коллекционер', reward: 'title_collector' }]);
            }
          }
        }
      } catch(e) { console.error('[Achievements] collector check error:', e.message); }

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

// ─── ДОСТИЖЕНИЯ API ───────────────────────────────────────────────────────────

// Получить все достижения игрока с прогрессом
app.get('/api/achievements/:userId', (req, res) => {
  try {
    const userId = normalizeId(req.params.userId);
    if (!userId || userId.startsWith('guest_')) return res.json({ ok: true, data: [] });

    // Синхронизируем прогресс из реальных данных если achievement_progress пустой или устарел
    _syncAchievementProgress(userId);

    const data = getAchievementsForUser(userId);
    res.json({ ok: true, data });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Синхронизирует прогресс достижений с реальными данными из БД
// Вызывается при первой загрузке — не перезаписывает уже выполненные
function _syncAchievementProgress(userId) {
  try {
    const p = db.prepare(`SELECT * FROM players WHERE id=?`).get(userId);
    if (!p) return;

    // Считаем реальные данные из battle_history (точнее чем players.wins+losses)
    const totalBattles = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=?`).get(userId)?.n || 0;
    const totalWins    = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=? AND result='win'`).get(userId)?.n || 0;
    const botWins      = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=? AND mode LIKE 'bot%' AND result='win'`).get(userId)?.n || 0;
    const randomBattles= db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=? AND mode='online'`).get(userId)?.n || 0;
    const friendBattles= db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=? AND (mode='friend' OR mode='friend_join')`).get(userId)?.n || 0;
    const themesBought = db.prepare(`SELECT COUNT(*) as n FROM inventory i JOIN shop_items s ON s.id=i.item_id WHERE i.user_id=? AND s.type='theme' AND i.is_active=1 AND i.purchase_type='stars'`).get(userId)?.n || 0;
    const refQualified = db.prepare(`SELECT COUNT(*) as n FROM referrals WHERE inviter_id=? AND qualified=1`).get(userId)?.n || 0;

    const syncMap = {
      total_battles:       totalBattles,
      total_wins:          totalWins,
      bot_wins:            botWins,
      random_battles:      randomBattles,
      friend_battles:      friendBattles,
      themes_bought:       themesBought,
      referrals_qualified: refQualified,
    };

    for (const [countFn, realValue] of Object.entries(syncMap)) {
      if (realValue <= 0) continue;
      const relevant = ACHIEVEMENTS.filter(a => a.countFn === countFn && a.type === 'limited');
      for (const ach of relevant) {
        const row = db.prepare(`SELECT * FROM achievements_progress WHERE user_id=? AND achievement_id=?`).get(userId, ach.id);
        if (row?.completed_at) continue; // уже выполнено — не трогаем

        const newProg = Math.min(realValue, ach.goal);

        // Обновляем прогресс (всегда до актуального значения)
        if (row) {
          db.prepare(`UPDATE achievements_progress SET progress=? WHERE user_id=? AND achievement_id=?`).run(newProg, userId, ach.id);
        } else {
          db.prepare(`INSERT INTO achievements_progress (user_id,achievement_id,progress) VALUES (?,?,?)`).run(userId, ach.id, newProg);
        }

        // Если достигли цели — отмечаем выполненным и выдаём награду
        if (newProg >= ach.goal) {
          db.prepare(`UPDATE achievements_progress SET completed_at=?,notified=0 WHERE user_id=? AND achievement_id=?`).run(Math.floor(Date.now()/1000), userId, ach.id);
          if (ach.reward && !db.prepare(`SELECT 1 FROM inventory WHERE user_id=? AND item_id=? AND is_active=1`).get(userId, ach.reward)) {
            grantItem(userId, ach.reward, 'reward');
            console.log(`[AchSync] Granted: ${userId} → ${ach.reward} (${ach.id})`);
          }
        }
      }
    }
  } catch(e) { console.error('[AchSync] error:', e.message); }
}

// Отметить достижения как просмотренные
app.post('/api/achievements/seen', (req, res) => {
  try {
    const { userId, ids } = req.body;
    const uid = normalizeId(userId);
    if (!uid || uid.startsWith('guest_')) return res.json({ ok: false });
    if (Array.isArray(ids) && ids.length > 0) {
      const placeholders = ids.map(() => '?').join(',');
      db.prepare(`UPDATE achievements_progress SET notified=1 WHERE user_id=? AND achievement_id IN (${placeholders})`).run(uid, ...ids);
    } else {
      db.prepare(`UPDATE achievements_progress SET notified=1 WHERE user_id=?`).run(uid);
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// Выдать достижение админу

// ─── РЕФЕРАЛЫ API ─────────────────────────────────────────────────────────────

// Получить реферальную ссылку игрока
app.get('/api/referral/:userId', (req, res) => {
  try {
    const userId = normalizeId(req.params.userId);
    if (!userId || userId.startsWith('guest_')) return res.status(400).json({ ok: false });
    // Ссылка ведёт на telegram mini app с параметром start
    const refLink = `https://t.me/${BOT_USERNAME}/${APP_NAME}?startapp=ref_${userId}`;
    // Подсчёт приглашённых
    const invited = db.prepare(`SELECT * FROM referrals WHERE inviter_id=?`).all(userId);
    const qualified = invited.filter(r => r.qualified).length;
    res.json({ ok: true, data: { refLink, invited: invited.length, qualified } });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

// Зарегистрировать реферала (вызывается при первом входе с ref_ параметром)
app.post('/api/referral/register', (req, res) => {
  try {
    const { inviterId, inviteeId } = req.body;
    const inviter = normalizeId(inviterId);
    const invitee = normalizeId(inviteeId);
    if (!inviter || !invitee || inviter === invitee) return res.json({ ok: false, error: 'invalid' });
    if (invitee.startsWith('guest_') || inviter.startsWith('guest_')) return res.json({ ok: false });
    // Проверяем что invitee не уже зарегистрирован
    const existing = db.prepare(`SELECT 1 FROM referrals WHERE invitee_id=?`).get(invitee);
    if (existing) return res.json({ ok: false, error: 'already registered' });
    // Игрок должен быть новым (мало боёв)
    const battles = db.prepare(`SELECT COUNT(*) as n FROM battle_history WHERE player_id=?`).get(invitee)?.n || 0;
    if (battles > 5) return res.json({ ok: false, error: 'not new player' });
    db.prepare(`INSERT OR IGNORE INTO referrals (inviter_id, invitee_id) VALUES (?,?)`).run(inviter, invitee);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// ─── АКТИВНОЕ ЗВАНИЕ API ──────────────────────────────────────────────────────

// Получить активное звание игрока (для отображения в игре)
app.get('/api/title/:userId', (req, res) => {
  try {
    const userId = normalizeId(req.params.userId);
    if (!userId || userId.startsWith('guest_')) return res.json({ ok: true, data: null });
    const title = getActiveTitle(userId);
    res.json({ ok: true, data: title });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// Получить активные звания для нескольких игроков сразу
app.post('/api/title/batch', (req, res) => {
  try {
    const ids = req.body.ids || [];
    const result = {};
    for (const id of ids) {
      const norm = normalizeId(id);
      if (norm) result[norm] = getActiveTitle(norm);
    }
    res.json({ ok: true, data: result });
  } catch(e) { res.status(500).json({ ok: false }); }
});

// Проверить топ-1 рейтинга и выдать достижение
function checkRatingTop1(userId) {
  try {
    const rating = getRating();
    if (rating.length > 0 && rating[0].id === userId) {
      updateAchievementProgress(userId, 'rating_top1', 1);
    }
  } catch(e) {}
}

// Достижения для ботов — добавляем в /api/history
// (патчим существующий обработчик через middleware-like логику)

// Фронтенд — обязательно В САМОМ КОНЦЕ, после всех API
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

server.listen(PORT, () => console.log(`\n🚢 http://localhost:${PORT}\n`));
module.exports = { app, server };
