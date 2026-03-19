/* ═══════════════════════════════════════════════════════════════
   МОРСКОЙ БОЙ — game.js  (переписан начисто)
   Фиксы: WS-флоу, десктоп, бургер, расстановка, поле
═══════════════════════════════════════════════════════════════ */
'use strict';

/* ─── КОНСТАНТЫ ──────────────────────────────────── */
const BOARD_SIZE = 10;
const COLS       = 'ABCDEFGHIJ';
const SHIP_DEFS  = [
  { size: 4, count: 1 },
  { size: 3, count: 2 },
  { size: 2, count: 3 },
  { size: 1, count: 4 },
];
const CELL_EMPTY = 0, CELL_SHIP = 1, CELL_HIT = 2, CELL_MISS = 3, CELL_SUNK = 4;


/* ─── XP / УРОВНИ (клиент) ──────────────────────── */
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
    if (xp >= XP_LEVELS[i]) level = i + 1; else break;
  }
  return Math.min(level, 30);
}

function calcRank(level) {
  let rank = RANKS[0].name;
  for (const r of RANKS) { if (level >= r.minLevel) rank = r.name; }
  return rank;
}

function getXpProgress(xp) {
  const level    = calcLevel(xp);
  const rank     = calcRank(level);
  const xpForThis = XP_LEVELS[level - 1] || 0;
  const xpForNext = XP_LEVELS[level]     || XP_LEVELS[XP_LEVELS.length - 1];
  const xpInLevel = xp - xpForThis;
  const xpNeeded  = xpForNext - xpForThis;
  const pct       = xpNeeded > 0 ? Math.min(100, Math.round(xpInLevel / xpNeeded * 100)) : 100;
  return { xp, level, rank, xpInLevel, xpNeeded, xpForNext, pct };
}

/* ─── ПРИЛОЖЕНИЕ ─────────────────────────────────── */
const App = {
  user:     null,
  settings: {},
  stats:    {},
  history:  [],
};

/* ─── ИГРА ───────────────────────────────────────── */
const Game = {
  mode:         null,
  myBoard:      null,
  enemyBoard:   null,
  myShots:      null,
  enemyShots:   null,
  myShips:      [],
  enemyShips:   [],
  isMyTurn:     false,
  showingEnemy: true,
  active:       false,
  roomId:       null,
  opponent:     null,
  shots:        0,
  hits:         0,
  botMode:      'hunt',
  botQueue:     [],
  botLastHit:   null,
  botDirection: null,
  // Таймер хода
  _timerInterval: null,
  _timerSeconds:  null,
};

/* ─── ЗВУКИ ──────────────────────────────────────── */
const Sound = (() => {
  let ctx = null;
  const getCtx = () => {
    if (!ctx) ctx = new (window.AudioContext || window.webkitAudioContext)();
    return ctx;
  };
  const beep = (freq, type = 'sine', dur = 0.12, vol = 0.3) => {
    if (!App.settings.sound) return;
    try {
      const c   = getCtx();
      const osc = c.createOscillator();
      const g   = c.createGain();
      osc.connect(g); g.connect(c.destination);
      osc.frequency.value = freq; osc.type = type;
      g.gain.setValueAtTime(vol, c.currentTime);
      g.gain.exponentialRampToValueAtTime(0.001, c.currentTime + dur);
      osc.start(c.currentTime); osc.stop(c.currentTime + dur);
    } catch (e) {}
  };
  return {
    hit:   () => { beep(180,'sawtooth',.25,.4); setTimeout(()=>beep(120,'square',.3,.3),80); },
    miss:  () => beep(300,'sine',.08,.2),
    // Потопление — две низкие ноты
    sunk:  () => {
      beep(90,'sawtooth',.5,.5);
      setTimeout(()=>beep(55,'sawtooth',.7,.5),220);
    },
    win:   () => [523,659,784,1047].forEach((f,i)=>setTimeout(()=>beep(f,'sine',.3,.4),i*150)),
    lose:  () => [200,180,160].forEach((f,i)=>setTimeout(()=>beep(f,'sawtooth',.4,.3),i*200)),
    click: () => beep(600,'sine',.05,.15),
    place: () => beep(400,'square',.06,.2),
  };
})();

function vibrate(p = [30]) {
  if (!App.settings?.vibro) return;
  try {
    // Telegram HapticFeedback — работает на iOS и Android в Telegram
    const hf = window.Telegram?.WebApp?.HapticFeedback;
    if (hf) {
      // Определяем тип по длине паттерна
      if (p.length === 1 && p[0] <= 20) {
        hf.impactOccurred('light');
      } else if (p.length === 1) {
        hf.impactOccurred('medium');
      } else {
        hf.notificationOccurred('warning');
      }
      return; // HapticFeedback сработал — navigator.vibrate не нужен
    }
    // Fallback: navigator.vibrate (Android Chrome вне Telegram)
    if (navigator.vibrate) navigator.vibrate(p);
  } catch(e) {}
}

/* ─── УТИЛИТЫ ДОСКИ ──────────────────────────────── */
const makeBoard  = () => Array.from({length: BOARD_SIZE}, () => new Array(BOARD_SIZE).fill(CELL_EMPTY));
const cloneBoard = b  => b.map(r => [...r]);
const inBounds   = (r, c) => r >= 0 && r < BOARD_SIZE && c >= 0 && c < BOARD_SIZE;

function canPlace(board, r, c, size, vertical) {
  for (let i = 0; i < size; i++) {
    const nr = vertical ? r+i : r, nc = vertical ? c : c+i;
    if (!inBounds(nr, nc)) return false;
    for (let dr = -1; dr <= 1; dr++)
      for (let dc = -1; dc <= 1; dc++) {
        const sr = nr+dr, sc = nc+dc;
        if (inBounds(sr,sc) && board[sr][sc] !== CELL_EMPTY) return false;
      }
  }
  return true;
}

function placeShip(board, r, c, size, vertical) {
  const cells = [];
  for (let i = 0; i < size; i++) {
    const nr = vertical ? r+i : r, nc = vertical ? c : c+i;
    board[nr][nc] = CELL_SHIP;
    cells.push({r: nr, c: nc});
  }
  return cells;
}

function randomPlaceAll() {
  const board = makeBoard(), ships = [];
  for (const def of SHIP_DEFS) {
    for (let k = 0; k < def.count; k++) {
      let placed = false, tries = 0;
      while (!placed && tries < 500) {
        tries++;
        const v = Math.random() < .5;
        const r = Math.floor(Math.random() * BOARD_SIZE);
        const c = Math.floor(Math.random() * BOARD_SIZE);
        if (canPlace(board, r, c, def.size, v)) {
          ships.push({ cells: placeShip(board, r, c, def.size, v), sunk: false, size: def.size, vertical: v });
          placed = true;
        }
      }
    }
  }
  return { board, ships };
}

function checkSunk(board, ships, r, c) {
  for (const ship of ships) {
    if (ship.sunk) continue;
    if (!ship.cells.some(cell => cell.r === r && cell.c === c)) continue;
    if (ship.cells.every(cell => board[cell.r][cell.c] === CELL_HIT)) {
      ship.sunk = true;
      ship.cells.forEach(cell => { board[cell.r][cell.c] = CELL_SUNK; });
      ship.cells.forEach(({r:sr, c:sc}) => {
        for (let dr = -1; dr <= 1; dr++)
          for (let dc = -1; dc <= 1; dc++) {
            const nr = sr+dr, nc = sc+dc;
            if (inBounds(nr,nc) && board[nr][nc] === CELL_EMPTY) board[nr][nc] = CELL_MISS;
          }
      });
      return ship;
    }
  }
  return null;
}

const allSunk = ships => ships.every(s => s.sunk);

/* ─── НАВИГАЦИЯ ──────────────────────────────────── */
let currentScreen = 'loading';
let _prevScreen   = null;

// Возврат в меню с лоадером и синхронизацией данных
async function goToMenu() {
  const loader = document.getElementById('screen-loading');
  const progEl = document.getElementById('loading-progress');

  if (loader) {
    loader.style.display = '';
    if (progEl) progEl.textContent = '0%';
  }
  startLoadingCellAnim();

  let pct = 0;
  const t = setInterval(() => {
    pct = Math.min(pct + Math.random() * 20 + 8, 85);
    if (progEl) progEl.textContent = Math.round(pct) + '%';
  }, 120);

  await syncStatsFromServer().catch(() => {});
  clearInterval(t);
  if (progEl) progEl.textContent = '100%';
  updateMenuUI();
  await new Promise(r => setTimeout(r, 300));

  stopLoadingCellAnim();
  if (loader) loader.style.display = 'none';
  showScreen('menu');
}

function showScreen(name, opts = {}) {
  const isBack = opts.isBack || false;
  const prev   = document.getElementById('screen-' + currentScreen);
  const next   = document.getElementById('screen-' + name);
  if (!next || currentScreen === name) return;

  // Если уходим с расстановки или ожидания в онлайн-режиме — уведомляем сервер
  if (['placement', 'waiting'].includes(currentScreen) &&
      !['placement', 'waiting', 'game'].includes(name) &&
      pendingGameMode === 'online' && WS.socket && WS.roomId && !Game.active) {
    WS.socket.emit('leave_room', { roomId: WS.roomId });
    WS.disconnect();
  }

  // Лоадер убираем насовсем через display:none
  const loader = document.getElementById('screen-loading');
  stopLoadingCellAnim();
  if (loader) loader.style.display = 'none';

  // Снимаем классы анимации (но не трогаем лоадер)
  document.querySelectorAll('.screen:not(#screen-loading)').forEach(s => {
    s.classList.remove('active', 'slide-back-enter');
    s.style.cssText = '';
  });

  if (isBack) {
    // Шаг 1: без transition ставим начальную позицию слева (вне экрана)
    next.style.transition = 'none';
    next.style.transform  = 'translateX(-30%)';
    next.style.opacity    = '0.01';
    next.classList.add('active');
    // Шаг 2: в следующем кадре включаем transition и анимируем к центру
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        next.style.transition = '';
        next.style.transform  = '';
        next.style.opacity    = '';
      });
    });
    if (prev && prev !== loader) {
      prev.style.transition = 'transform .3s cubic-bezier(.4,0,.2,1), opacity .3s';
      prev.style.transform  = 'translateX(100%)';
      prev.style.opacity    = '0';
      setTimeout(() => { if (prev) prev.style.cssText = ''; }, 320);
    }
  } else {
    next.classList.add('active');
    if (prev && prev !== loader) {
      prev.style.transition = 'transform .3s cubic-bezier(.4,0,.2,1), opacity .3s';
      prev.style.transform  = 'translateX(-30%)';
      prev.style.opacity    = '0';
      setTimeout(() => { if (prev) prev.style.cssText = ''; }, 320);
    }
  }

  _prevScreen   = currentScreen;
  currentScreen = name;
}

/* ─── ДЕСКТОП ДЕТЕКТ ─────────────────────────────── */
function isDesktop() {
  return window.innerWidth >= 768;
}

/* ─── АВТОРИЗОВАННЫЙ FETCH ───────────────────────── */
// Добавляет X-Telegram-Init-Data для эндпоинтов требующих верификации
function authFetch(url, options = {}) {
  const initData = window.Telegram?.WebApp?.initData;
  const headers  = { ...(options.headers || {}) };
  if (initData) headers['X-Telegram-Init-Data'] = initData;
  return fetch(url, { ...options, headers });
}

/* ─── ПОЛЬЗОВАТЕЛЬ ───────────────────────────────── */
/* ─── ОПРЕДЕЛЯЕМ — мы в TG или нет ─────────────── */
function isInsideTelegram() {
  try {
    // initData непустая только когда реально открыто внутри TG
    const initData = window.Telegram?.WebApp?.initData;
    return typeof initData === 'string' && initData.length > 0;
  } catch(e) { return false; }
}

/* ─── ПОЛЬЗОВАТЕЛЬ ───────────────────────────────── */
function initUser() {
  let tgUser = null;

  if (isInsideTelegram()) {
    try {
      tgUser = window.Telegram.WebApp.initDataUnsafe?.user || null;
    } catch(e) {}
  }

  if (tgUser) {
    const cleanId = String(parseInt(tgUser.id, 10));
    // photo_url не всегда приходит — берём из localStorage если TG не отдал
    const saved   = loadJSON('bs_user', null);
    const savedPhoto = (saved && saved.id === cleanId) ? saved.photo : null;
    App.user = {
      id:       cleanId,
      name:     tgUser.first_name || tgUser.username || 'Игрок',
      username: tgUser.username ? '@' + tgUser.username : '',
      photo:    tgUser.photo_url || savedPhoto || null,
      isGuest:  false,
    };
  } else {
    const saved = loadJSON('bs_user', null);
    if (saved?.isGuest) {
      App.user = saved;
    } else {
      App.user = { id: 'guest_' + Date.now(), name: 'Гость', username: '', photo: null, isGuest: true };
    }
  }
  saveJSON('bs_user', App.user);
}

function initSettings() {
  App.settings = loadJSON('bs_settings', { sound: true, vibro: true, hints: true, anim: true, showEnemyMoves: true });
  ['sound','vibro','hints','anim'].forEach(id => {
    const el = document.getElementById('setting-' + id);
    if (el) {
      el.checked = !!App.settings[id];
      el.addEventListener('change', () => {
        App.settings[id] = el.checked;
        saveJSON('bs_settings', App.settings);
        // Fix 7: hints контролирует промо баббл
        if (id === 'hints') updatePromoBanner();
      });
    }
  });
  document.getElementById('btn-reset-stats')?.addEventListener('click', () => {
    const mode = App._statsMode || 'online';
    const label = mode === 'online' ? 'онлайн-статистику' : 'статистику против ботов';
    showModal('Сбросить статистику?', `Удалить ${label}? Действие нельзя отменить.`, [
      { label: 'Отмена',   cls: 'btn-ghost',  action: closeModal },
      { label: 'Сбросить', cls: 'btn-danger', action: async () => {
        closeModal();
        if (mode === 'online') {
          // Сбрасываем онлайн: сервер + локальный кэш
          if (!App.user.isGuest) {
            await fetch('/api/stats/reset', {
              method: 'POST',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({ id: App.user.id }),
            }).catch(() => {});
          }
          App.stats   = defaultStats();
          App.history = [];
        } else {
          // Сбрасываем боты: только localStorage
          App.statsBots   = defaultStats();
          App.historyBots = [];
          saveJSON('bs_stats_bots',   App.statsBots);
          saveJSON('bs_history_bots', App.historyBots);
        }
        updateMenuStats();
        renderStatsScreen(mode);
      }},
    ]);
  });
}

function defaultStats() { return { wins:0, losses:0, totalShots:0, totalHits:0 }; }

function initStats() {
  App.stats      = defaultStats(); // онлайн-статистика — с сервера
  App.statsBots  = loadJSON('bs_stats_bots', defaultStats()); // бот-статистика — localStorage
  App.history    = [];
  App.historyBots = loadJSON('bs_history_bots', []);
}

// Синхронизация онлайн-статистики с сервером
async function syncStatsFromServer() {
  if (App.user.isGuest) return;
  try {
    const [statsRes, histRes] = await Promise.all([
      fetch('/api/stats/' + App.user.id),
      fetch('/api/history/' + App.user.id + '?mode=online'),
    ]);
    const statsJson = await statsRes.json();
    const histJson  = await histRes.json();

    if (statsJson.ok && statsJson.data) {
      App.stats.wins       = statsJson.data.online_wins   || 0;
      App.stats.losses     = statsJson.data.online_losses || 0;
      App.stats.totalShots = statsJson.data.online_shots  || 0;
      App.stats.totalHits  = statsJson.data.online_hits   || 0;
      // XP данные
      if (statsJson.data.xp !== undefined) {
        App.user.xp = statsJson.data.xp || 0;
        saveJSON('bs_user', App.user);
        updateMenuLevel();
      }
      updateMenuStats();
    }

    if (histJson.ok && Array.isArray(histJson.data)) {
      const serverHistory = histJson.data;
      const localOnlineHistory = loadJSON('bs_history_online_backup', []);
      // Разовая миграция локальной истории
      if (localOnlineHistory.length > serverHistory.length && !loadJSON('bs_history_uploaded_' + App.user.id, false)) {
        for (const h of localOnlineHistory.slice(0, 30)) {
          await fetch('/api/history', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ id: App.user.id, result: h.result, opponent: h.opponent||'?', shots: h.shots||0, hits: h.hits||0, skipStats: true, mode: 'online' }),
          }).catch(() => {});
        }
        saveJSON('bs_history_uploaded_' + App.user.id, true);
        const r2 = await fetch('/api/history/' + App.user.id + '?mode=online');
        const j2 = await r2.json();
        if (j2.ok) App.history = j2.data.map(h => ({ result: h.result, opponent: h.opponent, shots: h.shots, hits: h.hits, date: h.date * 1000 }));
      } else {
        App.history = serverHistory.map(h => ({ result: h.result, opponent: h.opponent, shots: h.shots, hits: h.hits, date: h.date * 1000 }));
      }
    }
  } catch(e) {}
}

function recordResult(result, shots, hits, oppName) {
  const isBot = Game.mode?.startsWith('bot');

  if (isBot) {
    // Бот-статистика — только localStorage
    if (result === 'win')  App.statsBots.wins++;
    else if (result === 'loss') App.statsBots.losses++;
    App.statsBots.totalShots += shots;
    App.statsBots.totalHits  += hits;
    saveJSON('bs_stats_bots', App.statsBots);
    App.historyBots.unshift({ result, opponent: oppName || 'Бот', shots, hits, date: Date.now() });
    if (App.historyBots.length > 50) App.historyBots.pop();
    saveJSON('bs_history_bots', App.historyBots);
  } else {
    // Онлайн-статистика — обновляем локально сразу
    App.stats.wins       += result === 'win'  ? 1 : 0;
    App.stats.losses     += result === 'loss' ? 1 : 0;
    App.stats.totalShots += shots;
    App.stats.totalHits  += hits;
    // Пишем на сервер (skipStats=true — онлайн stats уже записаны через сокет)
    if (!App.user.isGuest) {
      fetch('/api/history', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ id: App.user.id, result, opponent: oppName || '?', shots, hits, skipStats: true, mode: 'online' }),
      }).then(() => syncStatsFromServer()).catch(() => {});
    }
  }
  updateMenuStats();
}

function updateMenuStats() {
  setText('stat-wins',  App.stats.wins);
  setText('stat-total', App.stats.wins + App.stats.losses);
}

/* ─── ПРОМО БАББЛ ────────────────────────────────── */
function initPromoBanner() {
  updatePromoBanner();
  document.getElementById('tg-promo-close')?.addEventListener('click', () => {
    saveJSON('bs_promo_dismissed', true);
    document.getElementById('tg-promo-menu')?.classList.add('hidden');
  });
}

function updatePromoBanner() {
  const isGuest   = !!App.user.isGuest;
  const isTG      = document.body.classList.contains('tg-app');
  const dismissed = loadJSON('bs_promo_dismissed', false);
  const hintsOn   = App.settings.hints !== false;

  const bannerMenu = document.getElementById('tg-promo-menu');
  const bannerLb   = document.getElementById('tg-promo-lb');
  const bannerSt   = document.getElementById('tg-promo-stats');

  // Fix 3: баббл на главной — только браузерным гостям (не в TG)
  if (bannerMenu) bannerMenu.classList.toggle('hidden', !isGuest || isTG || dismissed || !hintsOn);
  // На рейтинге и статистике — всем гостям (включая TG)
  if (bannerLb)   bannerLb.classList.toggle('hidden',   !isGuest);
  if (bannerSt)   bannerSt.classList.toggle('hidden',   !isGuest);
}

/* ─── РЕЙТИНГ ────────────────────────────────────── */
async function renderLeaderboard() {
  const list      = document.getElementById('leaderboard-list');
  const myCard    = document.getElementById('my-record-card');
  const mySection = document.getElementById('my-record-section');
  const btnJoin   = document.getElementById('btn-join-rating');
  const btnLeave  = document.getElementById('btn-leave-rating');
  updatePromoBanner();

  if (App.user.isGuest) {
    if (list)      list.innerHTML = '<p class="empty-state">Войди через Telegram чтобы видеть рейтинг</p>';
    if (mySection) mySection.style.display = 'none';
    return;
  }

  if (mySection) mySection.style.display = '';
  if (list)      list.innerHTML = '<p class="empty-state">Загрузка…</p>';

  // Инфо-кнопка — подписываем один раз
  const infoBtn = document.getElementById('rating-info-btn');
  if (infoBtn && !infoBtn._bound) {
    infoBtn._bound = true;
    infoBtn.addEventListener('click', () => {
      showModal('Как работает рейтинг',
        'В рейтинге учитываются только сетевые бои со случайными соперниками.\n\n' +
        'Участие добровольное — нажми «Участвовать» и твои победы пойдут в зачёт.\n\n' +
        'Если покинешь рейтинг и вернёшься — счёт обнулится.\n\n' +
        'Место определяется по количеству побед.',
        [{ label: 'Понятно', cls: 'btn-primary', action: closeModal }]
      );
    });
  }

  // ensure — неблокирующий, не ждём результата
  fetch('/api/ensure/' + encodeURIComponent(App.user.id) + '?name=' + encodeURIComponent(App.user.name)).catch(() => {});

  try {
    const [ratingRes, statsRes] = await Promise.all([
      fetch('/api/rating'),
      fetch('/api/stats/' + App.user.id),
    ]);

    if (!ratingRes.ok) throw new Error('rating ' + ratingRes.status);

    const ratingJson = await ratingRes.json();
    const statsJson  = await statsRes.json();
    const data       = ratingJson.ok ? (ratingJson.data || []) : [];
    const myStats    = statsJson.ok  ? statsJson.data : null;
    const isParticipating = myStats?.rating_active === 1;

    // Кнопки участия
    if (btnJoin)  { btnJoin.style.display  = isParticipating ? 'none' : 'inline-flex'; btnJoin.onclick  = () => doJoinRating(); }
    if (btnLeave) { btnLeave.style.display = isParticipating ? 'inline-flex' : 'none'; btnLeave.onclick = () => doLeaveRating(); }

    // Карточка "мой рекорд"
    if (myCard) {
      const rw   = myStats?.rated_wins   || 0;
      const rl   = myStats?.rated_losses || 0;
      const rs   = myStats?.rated_shots  || 0;
      const rh   = myStats?.rated_hits   || 0;
      const rank = data.findIndex(e => e.id === App.user.id) + 1;

      if (!isParticipating) {
        myCard.innerHTML = '<p class="empty-state" style="margin:0;padding:8px 0">Нажми «Участвовать» чтобы попасть в рейтинг</p>';
      } else if (rw + rl === 0) {
        myCard.innerHTML = '<p class="empty-state" style="margin:0;padding:8px 0">Сыграй сетевой бой чтобы появиться в таблице!</p>';
      } else {
        const acc = rs > 0 ? Math.round(rh/rs*100) : 0;
        const tot = rw + rl;
        const wr  = tot ? Math.round(rw/tot*100) : 0;
        myCard.innerHTML =
          '<div class="my-record-name">' + App.user.name +
          (rank > 0 ? ' <span class="rating-score-badge">#' + rank + '</span>' : '') + '</div>' +
          '<div class="my-record-grid">' +
          '<div><span class="my-record-val">' + rw + '</span><span class="my-record-lbl">Победы</span></div>' +
          '<div><span class="my-record-val">' + tot + '</span><span class="my-record-lbl">Боёв</span></div>' +
          '<div><span class="my-record-val">' + wr + '%</span><span class="my-record-lbl">Винрейт</span></div>' +
          '<div><span class="my-record-val">' + acc + '%</span><span class="my-record-lbl">Точность</span></div>' +
          '</div>';
      }
    }

    renderRatingList(data);
  } catch(e) {
    console.error('Rating load error:', e);
    if (list) list.innerHTML = '<p class="empty-state">Не удалось загрузить рейтинг. Попробуй позже.</p>';
    if (btnJoin)  { btnJoin.style.display = 'inline-flex'; btnJoin.onclick = () => doJoinRating(); }
    if (btnLeave) { btnLeave.style.display = 'none'; }
  }
}

async function doJoinRating() {
  try {
    await fetch('/api/rating/join', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id: App.user.id}) });
    renderLeaderboard();
  } catch(e) {}
}

async function doLeaveRating() {
  showModal('Покинуть рейтинг?', 'Результаты обнулятся. При возвращении счёт начнётся с нуля.', [
    { label: 'Отмена',   cls: 'btn-ghost',  action: closeModal },
    { label: 'Покинуть', cls: 'btn-danger', action: async () => {
      closeModal();
      try {
        await fetch('/api/rating/leave', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id: App.user.id}) });
        renderLeaderboard();
      } catch(e) {}
    }},
  ]);
}

function renderRatingList(data) {
  const list = document.getElementById('leaderboard-list');
  if (!list) return;
  const medals = ['gold','silver','bronze'];
  list.innerHTML = '';
  const filtered = data.filter(e => (e.rated_wins || 0) >= 1);
  if (!filtered.length) { list.innerHTML = '<p class="empty-state">Пока никто не набрал побед. Сыграй сетевой бой!</p>'; return; }
  filtered.forEach((entry, i) => {
    const rw  = entry.rated_wins   || 0;
    const rl  = entry.rated_losses || 0;
    const rs  = entry.rated_shots  || 0;
    const rh  = entry.rated_hits   || 0;
    const tot = rw + rl;
    const wr  = tot ? Math.round(rw/tot*100) : 0;
    const isMe = entry.id === App.user?.id;
    const level = entry.level || 1;
    const rank  = entry.rank  || 'Новобранец Неона';
    const div  = document.createElement('div');
    div.className = 'lb-item' + (isMe ? ' lb-item-me' : '');
    div.innerHTML =
      '<div class="lb-rank ' + (medals[i]||'') + '">' + (i < 3 ? ['🥇','🥈','🥉'][i] : i+1) + '</div>' +
      '<div class="lb-avatar-wrap"><div class="lb-avatar lb-frame-' + level + '">' + (entry.name||'?')[0].toUpperCase() + '</div>' +
      '<div class="lb-level-dot level-bg-' + level + '">' + level + '</div></div>' +
      '<div class="lb-info"><strong>' + (entry.name||'Игрок') + (isMe ? ' <small>(вы)</small>' : '') + '</strong>' +
      '<small class="lb-rank-name">' + rank + ' · ' + rw + 'W · ' + wr + '% WR</small></div>' +
      '<div class="lb-wins">' + rw + '</div>';
    list.appendChild(div);
  });
}

async function renderStatsScreen(mode) {
  const isGuest = !!App.user.isGuest;
  updatePromoBanner();
  if (!mode) mode = App._statsMode || 'online';
  App._statsMode = mode;

  const statsBody    = document.getElementById('profile-stats-body');
  const historyList  = document.getElementById('history-list');

  if (isGuest) {
    if (statsBody) statsBody.style.display = 'none';
    return;
  }
  if (statsBody) statsBody.style.display = '';

  // Подсвечиваем активный режим
  document.querySelectorAll('#profile-stats-mode-toggle .stm-btn')
    .forEach(b => b.classList.toggle('active', b.dataset.mode === mode));

  let wins, losses, totalShots, totalHits, histToShow;

  if (mode === 'online') {
    await syncStatsFromServer();
    wins       = App.stats.wins;
    losses     = App.stats.losses;
    totalShots = App.stats.totalShots;
    totalHits  = App.stats.totalHits;
    histToShow = App.history;
  } else {
    wins       = App.statsBots.wins;
    losses     = App.statsBots.losses;
    totalShots = App.statsBots.totalShots;
    totalHits  = App.statsBots.totalHits;
    histToShow = App.historyBots;
  }

  const total = wins + losses;
  setText('st-wins',    String(wins));
  setText('st-losses',  String(losses));
  setText('st-total',   String(total));
  setText('st-acc',     totalShots ? Math.round(totalHits/totalShots*100)+'%' : '0%');
  setText('st-winrate', total      ? Math.round(wins/total*100)+'%' : '0%');

  if (!historyList) return;
  historyList.innerHTML = '';
  const items = (histToShow || []).slice(0, 30);
  if (!items.length) { historyList.innerHTML = '<p class="empty-state">Нет боёв</p>'; return; }
  items.forEach(h => {
    const div = document.createElement('div');
    div.className = 'history-item';
    const icons  = {win:'✅', loss:'❌'};
    const labels = {win:'Победа', loss:'Поражение'};
    const d = new Date(h.date);
    const dateStr = d.toLocaleDateString('ru',{day:'2-digit',month:'2-digit'}) + ' ' + d.toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
    div.innerHTML = `<div class="history-icon">${icons[h.result]||'🤝'}</div><div class="history-info">${labels[h.result]||'Ничья'} vs ${h.opponent}<span>${h.shots} выстрелов · ${h.hits} попаданий</span></div><div class="history-time">${dateStr}</div>`;
    div.className = 'history-item';
    historyList.appendChild(div);
  });
}

/* ─── ОТРИСОВКА ДОСКИ ────────────────────────────── */
function renderBoard(boardEl, data, opts = {}) {
  boardEl.innerHTML = '';
  const { clickable, onCellClick, showShips, dimmed } = opts;
  for (let r = 0; r < BOARD_SIZE; r++) {
    for (let c = 0; c < BOARD_SIZE; c++) {
      const cell = document.createElement('div');
      cell.className = 'cell';
      cell.dataset.r = r; cell.dataset.c = c;
      const val = data[r][c];
      if (val === CELL_SHIP && showShips) cell.classList.add('ship');
      if (val === CELL_HIT)  cell.classList.add('hit');
      if (val === CELL_MISS) cell.classList.add('miss');
      if (val === CELL_SUNK) cell.classList.add('sunk');
      if (clickable && val === CELL_EMPTY) {
        cell.classList.add('hoverable');
        cell.addEventListener('click', () => onCellClick(r, c));
      }
      if (dimmed) cell.classList.add('dimmed');
      boardEl.appendChild(cell);
    }
  }
}

function buildLabels() {
  ['placement','game','game-enemy'].forEach(prefix => {
    const row = document.getElementById(prefix+'-row-labels');
    const col = document.getElementById(prefix+'-col-labels');
    if (!row || !col) return;
    row.innerHTML = '';
    COLS.split('').forEach(l => { const d = document.createElement('div'); d.className = 'board-label'; d.textContent = l; row.appendChild(d); });
    col.innerHTML = '';
    for (let i = 1; i <= 10; i++) { const d = document.createElement('div'); d.className = 'board-label'; d.textContent = i; col.appendChild(d); }
  });
}

/* ─── МЕНЮ АНИМАЦИЯ ──────────────────────────────── */
function initHeroGrid() {
  const grid = document.getElementById('hero-grid');
  if (!grid) return;

  // Вычисляем размер ячейки = ширина экрана / 10, задаём через CSS переменную
  function updateCellSize() {
    const cellSize = Math.floor(grid.offsetWidth / 10);
    grid.style.gridTemplateRows = `repeat(20, ${cellSize}px)`;
  }
  updateCellSize();
  window.addEventListener('resize', updateCellSize);

  const cells = [];
  for (let i = 0; i < 200; i++) {
    const d = document.createElement('div');
    d.className = 'hero-grid-cell';
    grid.appendChild(d);
    cells.push(d);
  }
  setInterval(() => {
    const cls = Math.random() < 0.35 ? 'hit' : 'active';
    const c = cells[Math.floor(Math.random() * cells.length)];
    if (c.classList.contains('hit') || c.classList.contains('active')) return;
    c.classList.add(cls);
    setTimeout(() => c.classList.remove(cls), 800 + Math.random() * 600);
  }, 180);
}

/* ─── РАССТАНОВКА ────────────────────────────────── */
const PLACEMENT_SAVE_KEY = 'bs_last_placement';

function savePlacement() {
  saveJSON(PLACEMENT_SAVE_KEY, {
    ships: Placement.ships.map(s => ({
      id: s.id, size: s.size, placed: s.placed,
      vertical: s.vertical,
      cells: s.cells.map(c => ({ r: c.r, c: c.c })),
    })),
    board: Placement.board,
  });
}

function loadSavedPlacement() {
  const saved = loadJSON(PLACEMENT_SAVE_KEY, null);
  if (!saved || !saved.ships || saved.ships.length !== Placement.ships.length) return false;
  try {
    Placement.board = saved.board;
    saved.ships.forEach((s, i) => {
      Placement.ships[i].placed   = s.placed;
      Placement.ships[i].vertical = s.vertical;
      Placement.ships[i].cells    = s.cells;
      if (s.placed) s.cells.forEach(({ r, c }) => { Placement.board[r][c] = CELL_SHIP; });
    });
    Placement.selected = null;
    return true;
  } catch(e) { return false; }
}

const Placement = {
  board: null, ships: [], selected: null, vertical: false,

  // Глобальный drag-стейт — живёт на Placement, не на DOM-элементах
  _drag: null,
  /*
    _drag = {
      ship,          // объект корабля
      ghost,         // DOM-элемент ghost
      pointerId,
      lastTap: 0,    // для double-tap на этом корабле
      longTimer,
      moved: false,
    }
  */

  init(restoreSaved = true) {
    this._killDrag();
    this.board = makeBoard(); this.ships = []; this.selected = null; this.vertical = false;
    let id = 0;
    for (const def of SHIP_DEFS)
      for (let k = 0; k < def.count; k++)
        this.ships.push({ id: id++, size: def.size, placed: false, vertical: false, cells: [] });
    if (restoreSaved) loadSavedPlacement();
    this._initBoardEvents();
    this.renderDock(); this.renderBoard();
  },

  // ─── BOARD-LEVEL POINTER EVENTS (единственный источник событий) ──────────────
  // Вешаем один раз на boardEl — переживает любые renderBoard()
  _boardEventsAttached: false,
  _initBoardEvents() {
    if (this._boardEventsAttached) return;
    const boardEl = document.getElementById('placement-board');
    if (!boardEl) return;
    boardEl.addEventListener('pointerdown', (e) => this._onBoardDown(e));
    this._boardEventsAttached = true;
  },

  _onBoardDown(e) {
    const cell = e.target.closest('[data-r][data-c]'); if (!cell) return;
    const r = +cell.dataset.r, c = +cell.dataset.c;
    const ship = this._shipAtCell(r, c); if (!ship) return;

    e.preventDefault();
    this._killDrag();

    const longTimer = setTimeout(() => {
      if (this._drag && !this._drag.moved) {
        this._killDrag();
        this._removeShip(ship.id);
        vibrate([30,15,30]); Sound.click();
      }
    }, 600);

    // setPointerCapture — все pointermove/up гарантированно приходят на этот элемент
    // даже когда палец ушёл далеко за пределы доски
    try { e.target.setPointerCapture(e.pointerId); } catch(_) {}

    const onMove = (ev) => this._onBoardMove(ev);
    const onUp   = (ev) => { this._onBoardUp(ev); document.removeEventListener('pointermove', onMove); document.removeEventListener('pointerup', onUp); document.removeEventListener('pointercancel', onUp); };

    document.addEventListener('pointermove', onMove);
    document.addEventListener('pointerup',   onUp);
    document.addEventListener('pointercancel', onUp);

    this._drag = {
      ship, ghost: null,
      pointerId: e.pointerId,
      longTimer, moved: false,
      startX: e.clientX, startY: e.clientY,
      target: e.target,   // сохраняем для releasePointerCapture
    };
  },

  _onBoardMove(e) {
    if (!this._drag || this._drag.pointerId !== e.pointerId) return;
    e.preventDefault();
    const dx = e.clientX - this._drag.startX, dy = e.clientY - this._drag.startY;

    if (!this._drag.moved && Math.hypot(dx, dy) > 8) {
      this._drag.moved = true;
      clearTimeout(this._drag.longTimer); this._drag.longTimer = null;

      const ship = this._drag.ship;
      // Запоминаем ориентацию ДО снятия с поля
      const shipVertical = ship.vertical;
      this.vertical = shipVertical;

      // Снимаем с поля
      ship.cells.forEach(({ r, c }) => { this.board[r][c] = CELL_EMPTY; });
      ship.placed = false; ship.cells = [];
      this.selected = ship;
      this._drag.ghost = null;

      // renderBoard — убираем корабль с поля визуально
      this.renderBoard();
      // renderDock — НЕ вызываем во время drag, чтобы корабль не появлялся в доке
    }

    if (this._drag.moved) {
      this._showPreview(e.clientX, e.clientY);
    }
  },

  _onBoardUp(e) {
    if (!this._drag || this._drag.pointerId !== e.pointerId) return;
    e.preventDefault();
    const drag = this._drag;
    this._drag = null;

    clearTimeout(drag.longTimer);
    if (drag.ghost) { drag.ghost.remove(); }
    this.clearPreview();

    try { e.target.releasePointerCapture(drag.pointerId); } catch(_) {}

    if (drag.moved) {
      // Drop
      this._dropAt(e.clientX, e.clientY, drag.ship);
      this.renderDock(); this.renderBoard();
    } else {
      // Короткий тап — double-tap = поворот
      const now = Date.now();
      const last = drag.ship._lastTap || 0;
      if (now - last < 350) {
        drag.ship._lastTap = 0;
        this._rotateShip(drag.ship.id);
      } else {
        drag.ship._lastTap = now;
      }
    }
  },

  _onBoardCancel(e) {
    if (!this._drag || this._drag.pointerId !== e.pointerId) return;
    const drag = this._drag;
    this._drag = null;
    clearTimeout(drag.longTimer);
    if (drag.ghost) { drag.ghost.remove(); }
    this.clearPreview();
    // Если корабль был снят с поля — возвращаем
    if (drag.moved && !drag.ship.placed) {
      drag.ship.vertical = false; this.vertical = false; this.selected = null;
      this.renderDock(); this.renderBoard();
    }
  },

  _killDrag() {
    if (!this._drag) return;
    clearTimeout(this._drag.longTimer);
    if (this._drag.ghost) this._drag.ghost.remove();
    this.clearPreview();
    this._drag = null;
  },

  // ─── ДОК ────────────────────────────────────────────────────────────────────
  renderDock() {
    const dock = document.getElementById('ship-dock');
    if (!dock) return;
    dock.innerHTML = '';
    this.ships.forEach(ship => {
      const wrap = document.createElement('div');
      wrap.className = 'ship-piece' + (ship.placed ? ' placed' : '');
      for (let i = 0; i < ship.size; i++) {
        const c = document.createElement('div'); c.className = 'ship-cell'; wrap.appendChild(c);
      }
      if (!ship.placed) {
        wrap.addEventListener('pointerup', (e) => {
          e.preventDefault(); e.stopPropagation();
          this._autoPlace(ship);
        });
      }
      dock.appendChild(wrap);
    });
    const ready = document.getElementById('btn-ready');
    if (ready) ready.disabled = !this.allPlaced();
  },

  _autoPlace(ship) {
    for (let r = 0; r < BOARD_SIZE; r++)
      for (let c = 0; c < BOARD_SIZE; c++)
        if (canPlace(this.board, r, c, ship.size, false)) {
          ship.vertical = false;
          ship.cells = placeShip(this.board, r, c, ship.size, false);
          ship.placed = true;
          Sound.place(); vibrate([15]);
          this.renderDock(); this.renderBoard(); return;
        }
    vibrate([20,10,20]);
  },

  // ─── ДОСКА ─────────────────────────────────────────────────────────────────
  renderBoard() {
    const boardEl = document.getElementById('placement-board');
    if (!boardEl) return;
    boardEl.innerHTML = '';
    // DOM пересоздан — сбрасываем кэш ссылок на ячейки превью
    this._previewCells = [];
    this._previewR = undefined; this._previewC = undefined; this._previewV = undefined;
    const cellShipMap = {};
    this.ships.forEach(ship => {
      if (ship.placed) ship.cells.forEach(({ r, c }) => { cellShipMap[r+','+c] = ship.id; });
    });
    for (let r = 0; r < BOARD_SIZE; r++) {
      for (let c = 0; c < BOARD_SIZE; c++) {
        const cell = document.createElement('div');
        cell.className = 'cell'; cell.dataset.r = r; cell.dataset.c = c;
        if (cellShipMap[r+','+c] !== undefined) cell.classList.add('ship');
        boardEl.appendChild(cell);
      }
    }
    const ready = document.getElementById('btn-ready');
    if (ready) ready.disabled = !this.allPlaced();
  },

  // ─── HELPERS ───────────────────────────────────────────────────────────────
  _shipAtCell(r, c) {
    return this.ships.find(s => s.placed && s.cells.some(sc => sc.r === r && sc.c === c)) || null;
  },

  _makeGhost(ship, vertical) {
    const v = vertical !== undefined ? vertical : ship.vertical;

    // Берём реальный размер ячейки с доски
    const boardEl = document.getElementById('placement-board');
    const sampleCell = boardEl?.querySelector('.cell');
    const cellSize = sampleCell ? sampleCell.getBoundingClientRect().width : 32;
    const gap = 2; // gap между ячейками (из CSS)

    const g = document.createElement('div');
    g.className = 'drag-ghost';
    g.style.cssText = [
      'position:fixed',
      'z-index:9999',
      'pointer-events:none',
      'touch-action:none',
      'display:flex',
      v ? 'flex-direction:column' : 'flex-direction:row',
      `gap:${gap}px`,
      'border-radius:5px',
      'opacity:0.85',
      // Центрируем ghost по пальцу (середина первой ячейки)
      'transform:translate(-50%,-50%)',
    ].join(';');

    for (let i = 0; i < ship.size; i++) {
      const c = document.createElement('div');
      c.style.cssText = [
        `width:${cellSize}px`,
        `height:${cellSize}px`,
        'background:var(--cell-ship)',
        'border-radius:3px',
        'border:1.5px solid var(--accent)',
        'box-sizing:border-box',
      ].join(';');
      g.appendChild(c);
    }
    return g;
  },

  _moveGhost(cx, cy) {
    if (!this._drag?.ghost) return;
    this._drag.ghost.style.left = cx + 'px';
    this._drag.ghost.style.top  = cy + 'px';
  },

  _showPreview(cx, cy) {
    if (!this.selected) return;

    const boardEl = document.getElementById('placement-board');
    if (!boardEl) return;
    const rect = boardEl.getBoundingClientRect();

    // Зажимаем координаты внутри доски — превью не исчезает при выходе пальца за край
    const clampedX = Math.max(rect.left, Math.min(rect.right  - 1, cx));
    const clampedY = Math.max(rect.top,  Math.min(rect.bottom - 1, cy));

    const cellW = (rect.width  - 2 * 9) / BOARD_SIZE;
    const cellH = (rect.height - 2 * 9) / BOARD_SIZE;
    let c = Math.floor((clampedX - rect.left) / (cellW + 2));
    let r = Math.floor((clampedY - rect.top)  / (cellH + 2));
    c = Math.max(0, Math.min(BOARD_SIZE - 1, c));
    r = Math.max(0, Math.min(BOARD_SIZE - 1, r));

    const size = this.selected.size;
    if (this.vertical) r = Math.min(r, BOARD_SIZE - size);
    else               c = Math.min(c, BOARD_SIZE - size);

    // Не перерисовываем если ячейка не изменилась — убирает мигание
    if (this._previewR === r && this._previewC === c && this._previewV === this.vertical) return;
    this._previewR = r; this._previewC = c; this._previewV = this.vertical;

    // Снимаем классы только с закэшированных ячеек (быстрее querySelectorAll по всей доске)
    if (this._previewCells) {
      this._previewCells.forEach(el => el.classList.remove('preview', 'invalid'));
    }
    this._previewCells = [];

    const valid = canPlace(this.board, r, c, size, this.vertical);
    for (let i = 0; i < size; i++) {
      const nr = this.vertical ? r+i : r, nc = this.vertical ? c : c+i;
      if (!inBounds(nr, nc)) continue;
      const cl = boardEl.querySelector(`[data-r="${nr}"][data-c="${nc}"]`);
      if (cl) { cl.classList.add(valid ? 'preview' : 'invalid'); this._previewCells.push(cl); }
    }
  },

  _dropAt(cx, cy, ship) {
    const boardEl = document.getElementById('placement-board');
    if (!boardEl) {
      ship.placed = false; ship.cells = []; ship.vertical = false;
      this.vertical = false; this.selected = null; return;
    }

    const rect  = boardEl.getBoundingClientRect();
    const cellW = (rect.width  - 2 * 9) / BOARD_SIZE;
    const cellH = (rect.height - 2 * 9) / BOARD_SIZE;
    const size  = ship.size;

    // Зажимаем координаты внутри доски — работает даже если палец отпущен за краем
    const clampedX = Math.max(rect.left, Math.min(rect.right  - 1, cx));
    const clampedY = Math.max(rect.top,  Math.min(rect.bottom - 1, cy));

    let baseC = Math.max(0, Math.min(BOARD_SIZE - 1, Math.floor((clampedX - rect.left) / (cellW + 2))));
    let baseR = Math.max(0, Math.min(BOARD_SIZE - 1, Math.floor((clampedY - rect.top)  / (cellH + 2))));
    if (this.vertical) baseR = Math.min(baseR, BOARD_SIZE - size);
    else               baseC = Math.min(baseC, BOARD_SIZE - size);

    // Сначала пробуем точную позицию
    if (canPlace(this.board, baseR, baseC, size, this.vertical)) {
      ship.vertical = this.vertical;
      ship.cells = placeShip(this.board, baseR, baseC, size, this.vertical);
      ship.placed = true; this.selected = null;
      Sound.place(); vibrate([15]); return;
    }

    // Ищем ближайшую свободную клетку по спирали вокруг базовой точки
    let bestR = -1, bestC = -1, bestDist = Infinity;
    for (let r = 0; r <= BOARD_SIZE - (this.vertical ? size : 1); r++) {
      for (let c = 0; c <= BOARD_SIZE - (this.vertical ? 1 : size); c++) {
        if (!canPlace(this.board, r, c, size, this.vertical)) continue;
        const dist = Math.hypot(r - baseR, c - baseC);
        if (dist < bestDist) { bestDist = dist; bestR = r; bestC = c; }
      }
    }

    if (bestR !== -1) {
      ship.vertical = this.vertical;
      ship.cells = placeShip(this.board, bestR, bestC, size, this.vertical);
      ship.placed = true; this.selected = null;
      Sound.place(); vibrate([15]); return;
    }

    // Совсем нет места — возвращаем в dok
    vibrate([20,10,20]);
    ship.placed = false; ship.cells = []; ship.vertical = false;
    this.vertical = false; this.selected = null;
  },

  _removeShip(shipId) {
    const ship = this.ships.find(s => s.id === shipId); if (!ship?.placed) return;
    ship.cells.forEach(({ r, c }) => { this.board[r][c] = CELL_EMPTY; });
    ship.placed = false; ship.cells = []; ship.vertical = false; this.selected = null;
    this.renderDock(); this.renderBoard();
  },

  _rotateShip(shipId) {
    const ship = this.ships.find(s => s.id === shipId); if (!ship?.placed || !ship.cells.length) return;
    const newV = !ship.vertical;
    const anchorR = ship.cells[0].r, anchorC = ship.cells[0].c;
    ship.cells.forEach(({ r, c }) => { this.board[r][c] = CELL_EMPTY; });
    const offsets = [[0,0],[0,1],[1,0],[0,-1],[-1,0],[1,1],[-1,-1],[1,-1],[-1,1]];
    for (const [dr, dc] of offsets) {
      const nr = anchorR+dr, nc = anchorC+dc;
      if (inBounds(nr,nc) && canPlace(this.board, nr, nc, ship.size, newV)) {
        ship.cells = placeShip(this.board, nr, nc, ship.size, newV);
        ship.vertical = newV; Sound.click(); vibrate([15]);
        this.renderBoard(); return;
      }
    }
    ship.cells = placeShip(this.board, anchorR, anchorC, ship.size, ship.vertical);
    vibrate([30,15,30,15,30]); this.renderBoard();
  },

  clearPreview() {
    if (this._previewCells && this._previewCells.length) {
      this._previewCells.forEach(el => el.classList.remove('preview', 'invalid'));
      this._previewCells = [];
    } else {
      document.querySelectorAll('#placement-board .preview, #placement-board .invalid')
        .forEach(c => c.classList.remove('preview','invalid'));
    }
    this._previewR = undefined; this._previewC = undefined; this._previewV = undefined;
  },

  clear() {
    this._killDrag();
    this.board = makeBoard();
    this.ships.forEach(s => { s.placed = false; s.cells = []; s.vertical = false; });
    this.selected = null; this.vertical = false;
    this.renderDock(); this.renderBoard();
  },

  randomize() {
    this._killDrag();
    const { board, ships } = randomPlaceAll();
    this.board = board;
    this.ships.forEach((s, i) => { s.placed = true; s.cells = ships[i]?.cells||[]; s.vertical = ships[i]?.vertical||false; });
    this.selected = null; Sound.place(); this.renderDock(); this.renderBoard();
  },

  allPlaced()      { return this.ships.every(s => s.placed); },
  getShipsForGame(){ return this.ships.map(s => ({ cells: [...s.cells], sunk: false, size: s.size })); },
};
/* ─── ИГРОВОЙ ПРОЦЕСС ────────────────────────────── */
let pendingGameMode = null;

function startGame(mode, myBoard, myShips, enemyBoard, enemyShips, opponent) {
  Game.mode         = mode;
  Game.myBoard      = cloneBoard(myBoard);
  Game.myShips      = JSON.parse(JSON.stringify(myShips));
  Game.enemyBoard   = cloneBoard(enemyBoard || makeBoard());
  Game.enemyShips   = enemyShips ? JSON.parse(JSON.stringify(enemyShips)) : [];
  Game.myShots      = makeBoard();
  Game.enemyShots   = makeBoard();
  Game.isMyTurn     = true;
  Game.showingEnemy = true;
  Game.active       = true;
  Game.shots        = 0; Game.hits = 0;
  Game.opponent     = opponent || { name: 'Бот', username: '' };

  if (mode.startsWith('bot') && !enemyShips) {
    const r = randomPlaceAll();
    Game.enemyBoard = r.board; Game.enemyShips = r.ships;
  }
  Game.botMode = 'hunt'; Game.botQueue = []; Game.botLastHit = null; Game.botDirection = null;

  setText('opp-name', opponent?.name || 'Бот');
  // Счёт дуэлей (только онлайн) — теперь через updateGameFooter
  renderOpponentAvatar(opponent?.name || 'Бот', !opponent || opponent.name === 'Бот');
  setupGameLayout();
  renderGameBoard();
  updateEnemyFleet();
  showScreen('game');
  // Переприменяем тему — убеждаемся что класс на body не слетел
  applyEquippedThemeFromState();
  updateGameStatus();
}

/* ─── ДЕСКТОП vs МОБАЙЛ ЛЭЙАУТ ─────────────────── */
function setupGameLayout() {
  const screen    = document.getElementById('screen-game');
  const enemyWrap = document.getElementById('game-board-wrap');
  const myWrap    = document.getElementById('my-board-wrap');
  const fleet     = document.getElementById('enemy-fleet');
  const footer    = document.getElementById('game-footer') || screen.querySelector('.game-footer');

  if (isDesktop()) {
    screen.classList.add('desktop');

    // Создаём/находим обёртку для двух полей
    let row = screen.querySelector('.game-boards-row');
    if (!row) {
      row = document.createElement('div');
      row.className = 'game-boards-row';
      // Вставляем после topbar
      const topbar = screen.querySelector('.game-topbar');
      topbar?.insertAdjacentElement('afterend', row);
    }

    // Добавляем заголовки и кладём поля в row
    if (!enemyWrap.parentElement?.classList.contains('board-col')) {
      const colE = document.createElement('div'); colE.className = 'board-col';
      const titleE = document.createElement('div'); titleE.className = 'board-title'; titleE.textContent = 'Поле противника';
      colE.appendChild(titleE); colE.appendChild(enemyWrap);
      row.appendChild(colE);

      const colM = document.createElement('div'); colM.className = 'board-col';
      const titleM = document.createElement('div'); titleM.className = 'board-title'; titleM.textContent = 'Моё поле';
      colM.appendChild(titleM); colM.appendChild(myWrap);
      row.appendChild(colM);
    }

    myWrap.style.display = '';
    Game.showingEnemy = true;
  } else {
    screen.classList.remove('desktop');
    // Вернуть поля обратно в screen если были перемещены
    const row = screen.querySelector('.game-boards-row');
    if (row) {
      screen.insertBefore(enemyWrap, fleet || footer);
      row.remove();
    }
    myWrap.style.display = 'none';
    setShowingField(true); // default: show enemy board
  }
}

function renderGameBoard() {
  const desktop = isDesktop();

  // Поле врага (всегда рендерим)
  const enemyEl = document.getElementById('game-board');
  if (enemyEl) {
    const display = makeBoard();
    for (let r = 0; r < BOARD_SIZE; r++)
      for (let c = 0; c < BOARD_SIZE; c++) {
        const s = Game.myShots[r][c];
        if (s !== CELL_EMPTY) display[r][c] = s;
      }

    if (!desktop && !Game.showingEnemy) {
      // Мобайл, показываем СВОЁ поле — рисуем его на том же элементе, без enemy-board класса
      enemyEl.classList.remove('enemy-board');
      const myDisplay = cloneBoard(Game.myBoard);
      for (let r = 0; r < BOARD_SIZE; r++)
        for (let c = 0; c < BOARD_SIZE; c++) {
          const s = Game.enemyShots[r][c];
          if (s !== CELL_EMPTY) myDisplay[r][c] = s;
        }
      renderBoard(enemyEl, myDisplay, { clickable: false, showShips: true });
    } else {
      // Поле врага
      enemyEl.classList.add('enemy-board');
      renderBoard(enemyEl, display, {
        clickable:   Game.isMyTurn,
        showShips:   false,
        onCellClick: (r, c) => playerShoot(r, c),
        dimmed:      desktop && !Game.isMyTurn,
      });
    }
  }

  // Наше поле (только на десктопе постоянно — my-board)
  const myEl = document.getElementById('my-board');
  if (myEl) {
    const display = cloneBoard(Game.myBoard);
    for (let r = 0; r < BOARD_SIZE; r++)
      for (let c = 0; c < BOARD_SIZE; c++) {
        const s = Game.enemyShots[r][c];
        if (s !== CELL_EMPTY) display[r][c] = s;
      }
    renderBoard(myEl, display, {
      clickable:  false,
      showShips:  true,
      dimmed:     desktop && Game.isMyTurn,
    });
  }

  updateShipsLeft();
  updateEnemyFleet();
}

function setShowingField(showEnemy) {
  Game.showingEnemy = showEnemy;
  document.getElementById('btn-show-enemy')?.classList.toggle('active',  showEnemy);
  document.getElementById('btn-show-mine')?.classList.toggle('active',  !showEnemy);
}

/* ─── ОБРАБОТКА СВОРАЧИВАНИЯ ПРИЛОЖЕНИЯ ─────────── */
function initVisibilityHandler() {
  let hiddenAt = null;
  const MAX_BG_MS = 30 * 60 * 1000; // 30 минут — после этого сессия считается завершённой

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      hiddenAt = Date.now();
    } else {
      // Вернулись в приложение
      const bgTime = hiddenAt ? Date.now() - hiddenAt : 0;
      hiddenAt = null;

      if (bgTime > MAX_BG_MS) {
        // Слишком долго в фоне — сбрасываем в меню
        if (Game.active) {
          Game.active = false;
          WS.disconnect();
          showModal('Сессия истекла', 'Вы долго не были в игре. Вернитесь в меню.', [
            { label: 'В меню', cls: 'btn-primary', action: () => { closeModal(); goToMenu(); }},
          ]);
        }
        return;
      }

      // Переподключаем сокет если он отвалился во время фона
      if (Game.active && Game.mode === 'online' && WS.socket && !WS.socket.connected) {
        // Сокет отвалился — для игрока это означает что соперник уже получил победу
        // Показываем соответствующий экран
        Game.active = false;
        showModal('Соединение потеряно', 'Игра прервана из-за потери связи.', [
          { label: 'В меню', cls: 'btn-primary', action: () => { closeModal(); goToMenu(); }},
        ]);
      }
    }
  });
}


function startTurnWarningUI(secondsLeft) {
  clearTurnWarningUI();
  const countdownEl = document.getElementById('turn-countdown');
  const countdownText = document.getElementById('turn-countdown-text');
  if (!countdownEl || !countdownText) return;

  countdownEl.classList.remove('hidden', 'urgent');

  let secs = secondsLeft;
  const update = () => {
    if (!Game.active || !Game.isMyTurn) { clearTurnWarningUI(); return; }
    const mm = String(Math.floor(secs/60)).padStart(2,'0');
    const ss = String(secs % 60).padStart(2,'0');
    countdownText.textContent = `${mm}:${ss}`;
    if (secs <= 10) countdownEl.classList.add('urgent');
    else countdownEl.classList.remove('urgent');
    if (secs <= 0) { clearTurnWarningUI(); return; }
    secs--;
  };
  update();
  Game._timerInterval = setInterval(update, 1000);
}

function clearTurnWarningUI() {
  if (Game._timerInterval) { clearInterval(Game._timerInterval); Game._timerInterval = null; }
  Game._timerSeconds = null;
  const countdownEl = document.getElementById('turn-countdown');
  if (countdownEl) countdownEl.classList.add('hidden');
  updateGameStatus();
}

function updateGameStatus() {
  const el = document.getElementById('game-status');
  if (!el || !Game.active) return;
  el.textContent = Game.isMyTurn ? 'Твой ход' : 'Ход соперника';
  el.style.color = Game.isMyTurn ? 'var(--green)' : 'var(--hint)';
}

function updateShipsLeft() {
  setText('my-ships-left',    String(Game.myShips.filter(s=>!s.sunk).length));
  setText('enemy-ships-left', String(Game.enemyShips.filter(s=>!s.sunk).length));
}

function updateEnemyFleet() {
  const container = document.getElementById('enemy-fleet-ships');
  if (!container) return;
  container.innerHTML = '';
  [...Game.enemyShips].sort((a,b) => b.size-a.size).forEach(ship => {
    const wrap = document.createElement('div');
    wrap.className = 'fleet-ship' + (ship.sunk ? ' sunk' : '');
    for (let i = 0; i < ship.size; i++) { const c = document.createElement('div'); c.className = 'fleet-cell'; wrap.appendChild(c); }
    container.appendChild(wrap);
  });
}

/* ─── ВЫСТРЕЛ ИГРОКА ─────────────────────────────── */
function playerShoot(r, c) {
  if (!Game.active || !Game.isMyTurn) return;
  if (Game.myShots[r][c] !== CELL_EMPTY) return;

  // Онлайн: только отправляем, результат придёт через shot_result
  if (Game.mode === 'online') {
    Game.isMyTurn = false; // блокируем повторные клики
    clearTurnWarningUI();  // сбрасываем таймер
    WS.sendShot(r, c);
    return;
  }

  // Бот / одиночная игра: обрабатываем локально
  Game.shots++;
  const hit = Game.enemyBoard[r][c] === CELL_SHIP;
  Game.myShots[r][c]    = hit ? CELL_HIT  : CELL_MISS;
  Game.enemyBoard[r][c] = hit ? CELL_HIT  : CELL_MISS;

  if (hit) {
    Game.hits++;
    Game.enemyBoard[r][c] = CELL_HIT;
    Sound.hit(); vibrate([30,10,30]);
    const sunk = checkSunk(Game.enemyBoard, Game.enemyShips, r, c);
    if (sunk) {
      for (let rr = 0; rr < BOARD_SIZE; rr++)
        for (let cc = 0; cc < BOARD_SIZE; cc++)
          if (Game.enemyBoard[rr][cc] === CELL_SUNK || Game.enemyBoard[rr][cc] === CELL_MISS)
            Game.myShots[rr][cc] = Game.enemyBoard[rr][cc];
      // Звук потопления врага = такой же как когда враг топит наш корабль
      Sound.sunk(); vibrate([80, 30, 80]);
    }
    if (allSunk(Game.enemyShips)) { endGame('win'); return; }
    if (!isDesktop() && App.settings.showEnemyMoves) setShowingField(true);
    renderGameBoard();
  } else {
    Game.isMyTurn = false;
    updateGameStatus();
    if (!isDesktop() && App.settings.showEnemyMoves) setShowingField(false);
    renderGameBoard();
    if (Game.mode.startsWith('bot')) setTimeout(botShoot, 800 + Math.random()*600);
  }
}

/* ─── БОТ ────────────────────────────────────────── */
/* ─── БОТ (2 сложности) ─────────────────────────── */
// ЛЕГКО: рандом, но обязательно добивает раненых
// СЛОЖНО: профессиональная тактика (пarity hunt + direction tracking)

function botGetDifficulty() {
  return (Game.mode === 'bot-easy') ? 'easy' : 'hard';
}

function botShoot() {
  if (!Game.active || Game.isMyTurn) return;
  const diff = botGetDifficulty();
  let r, c;

  if (diff === 'easy') {
    // Если есть раненый корабль — добиваем (случайный сосед)
    if (Game.botQueue.length) {
      [r, c] = Game.botQueue.shift();
      while (Game.botQueue.length && Game.enemyShots[r][c] !== CELL_EMPTY)
        [r, c] = Game.botQueue.shift();
      if (Game.enemyShots[r][c] !== CELL_EMPTY) [r, c] = randomEmpty(Game.enemyShots);
    } else {
      [r, c] = randomEmpty(Game.enemyShots);
    }
  } else {
    // HARD: шахматный паттерн + направленное добивание
    // Лучшая известная тактика: охота по чётным клеткам (parity 2),
    // после попадания — добивание по направлению
    if (Game.botQueue.length) {
      [r, c] = Game.botQueue.shift();
      while (Game.botQueue.length && Game.enemyShots[r][c] !== CELL_EMPTY)
        [r, c] = Game.botQueue.shift();
      if (Game.enemyShots[r][c] !== CELL_EMPTY) [r, c] = huntParity(Game.enemyShots);
    } else {
      [r, c] = huntParity(Game.enemyShots);
    }
  }

  if (r === undefined || c === undefined) return;

  const hit = Game.myBoard[r][c] === CELL_SHIP;
  Game.enemyShots[r][c] = hit ? CELL_HIT : CELL_MISS;

  if (hit) {
    Game.myBoard[r][c] = CELL_HIT;
    const prevHit = Game.botLastHit; // Fix 4: сохраняем предыдущее попадание ДО обновления
    Game.botLastHit = {r, c};

    if (diff === 'easy') {
      // Easy: добавляем всех доступных соседей (добивает раненых)
      const nb = getNeighbors4(r, c).filter(([nr,nc]) => Game.enemyShots[nr][nc] === CELL_EMPTY);
      Game.botQueue.push(...nb);
    } else {
      // Hard: умное добивание с определением направления
      if (Game.botDirection) {
        // Уже знаем направление — продолжаем в том же направлении
        const [dr, dc] = Game.botDirection;
        Game.botQueue = [];
        const fwd = [r+dr, c+dc], bwd = [r-dr, c-dc];
        if (inBounds(fwd[0],fwd[1]) && Game.enemyShots[fwd[0]][fwd[1]] === CELL_EMPTY)
          Game.botQueue.unshift(fwd);
        if (inBounds(bwd[0],bwd[1]) && Game.enemyShots[bwd[0]][bwd[1]] === CELL_EMPTY)
          Game.botQueue.push(bwd);
      } else {
        // Определяем направление по предыдущему попаданию (до обновления botLastHit)
        if (prevHit) {
          const dr = r - prevHit.r, dc = c - prevHit.c;
          if (Math.abs(dr) + Math.abs(dc) === 1) Game.botDirection = [dr, dc];
        }
        Game.botQueue = [];
        if (Game.botDirection) {
          const [dr, dc] = Game.botDirection;
          if (inBounds(r+dr,c+dc) && Game.enemyShots[r+dr][c+dc]===CELL_EMPTY) Game.botQueue.push([r+dr,c+dc]);
          if (inBounds(r-dr,c-dc) && Game.enemyShots[r-dr][c-dc]===CELL_EMPTY) Game.botQueue.push([r-dr,c-dc]);
        } else {
          const nb = getNeighbors4(r, c).filter(([nr,nc]) => Game.enemyShots[nr][nc] === CELL_EMPTY);
          Game.botQueue.push(...nb);
        }
      }
    }

    const sunk = checkSunk(Game.myBoard, Game.myShips, r, c);
    if (sunk) {
      for (let rr = 0; rr < BOARD_SIZE; rr++)
        for (let cc = 0; cc < BOARD_SIZE; cc++)
          if (Game.myBoard[rr][cc] === CELL_SUNK || Game.myBoard[rr][cc] === CELL_MISS)
            Game.enemyShots[rr][cc] = Game.myBoard[rr][cc];
      Game.botQueue = []; Game.botLastHit = null; Game.botDirection = null;
      Sound.sunk(); vibrate([80, 30, 80]); // враг потопил мой корабль
    } else {
      Sound.hit(); vibrate([40]); // враг попал по моему кораблю
    }

    if (allSunk(Game.myShips)) { renderGameBoard(); endGame('loss'); return; }
    renderGameBoard();
    setTimeout(botShoot, 700 + Math.random()*500);
  } else {
    Sound.miss(); vibrate([10]); // враг промахнулся — лёгкая вибрация
    Game.isMyTurn = true; updateGameStatus();
    if (!isDesktop() && App.settings.showEnemyMoves) setShowingField(true);
    renderGameBoard();
  }
}

const getEmptyCells = board => {
  const res = [];
  for (let r=0;r<BOARD_SIZE;r++) for (let c=0;c<BOARD_SIZE;c++) if(board[r][c]===CELL_EMPTY) res.push([r,c]);
  return res;
};
const randomEmpty = board => { const e=getEmptyCells(board); return e[Math.floor(Math.random()*e.length)]||[0,0]; };

// Охота по паритету (чётность r+c) — оптимальная тактика поиска кораблей
// Корабли размером >=2 всегда занимают клетку чётного паритета при клетке 2
function huntParity(board) {
  // Приоритет 1: паритет 2 (r%2===c%2, шахматная раскраска)
  const p2 = [];
  for (let r=0;r<BOARD_SIZE;r++) for (let c=0;c<BOARD_SIZE;c++)
    if ((r+c)%2===0 && board[r][c]===CELL_EMPTY) p2.push([r,c]);
  if (p2.length) return p2[Math.floor(Math.random()*p2.length)];
  return randomEmpty(board);
}

const getNeighbors4 = (r,c) => [[r-1,c],[r+1,c],[r,c-1],[r,c+1]].filter(([nr,nc])=>inBounds(nr,nc));

/* ─── СИСТЕМА РЕВАНШЕЙ ───────────────────────────── */
const Rematch = {
  _interval: null,
  _secs: 10,

  _clear() {
    if (this._interval) { clearInterval(this._interval); this._interval = null; }
    const btn = document.getElementById('btn-rematch');
    if (btn) { btn.textContent = 'Реванш'; btn.disabled = false; btn._timerActive = false; btn.classList.remove('pulse-accent', 'btn-disabled'); }
    const statusEl = document.getElementById('rematch-status');
    if (statusEl) { statusEl.classList.add('hidden'); statusEl.classList.remove('opponent-wants','declined'); statusEl.textContent = ''; }
  },

  // Таймер для того, кто НАЖАЛ реванш первым
  startTimer() {
    this._clear();
    this._secs = 10;
    const btn = document.getElementById('btn-rematch');
    if (btn) { btn._timerActive = true; btn.disabled = true; }
    const update = () => {
      if (btn) btn.textContent = `Ожидаем… ${this._secs}с`;
      if (this._secs <= 0) { clearInterval(this._interval); this._interval = null; }
      this._secs--;
    };
    update();
    this._interval = setInterval(update, 1000);
  },

  // Таймер для того, кто ПОЛУЧИЛ предложение реванша
  startOpponentTimer() {
    if (this._interval) return; // если уже нажал — не перезапускаем
    this._secs = 10;
    const btn = document.getElementById('btn-rematch');
    // Кнопка остаётся активной — противник может принять
    const update = () => {
      if (btn && !btn._timerActive) btn.textContent = `Реванш (${this._secs}с)`;
      if (this._secs <= 0) {
        clearInterval(this._interval); this._interval = null;
        // Время вышло — деактивируем кнопку и редиректим
        if (btn) {
          btn.disabled = true;
          btn.textContent = 'Реванш';
          btn.classList.add('btn-disabled');
        }
        const statusEl = document.getElementById('rematch-status');
        if (statusEl) {
          statusEl.textContent = 'Время вышло';
          statusEl.classList.remove('opponent-wants');
          statusEl.classList.add('declined');
        }
        setTimeout(() => goToMenu(), 2500);
        return;
      }
      this._secs--;
    };
    update();
    this._interval = setInterval(update, 1000);
  },

  request() {
    if (!WS.socket || !WS.roomId) return;
    WS.socket.emit('rematch_request', { roomId: WS.roomId });
    this.startTimer();
    const statusEl = document.getElementById('rematch-status');
    if (statusEl) {
      statusEl.textContent = 'Ждём ответа соперника…';
      statusEl.classList.remove('hidden','opponent-wants','declined');
    }
  },
};

/* ─── КОНЕЦ ИГРЫ ─────────────────────────────────── */
function endGame(result) {
  Game.active = false;
  clearTurnWarningUI(); // сбрасываем таймер
  recordResult(result, Game.shots, Game.hits, Game.opponent?.name);
  updateMenuStats();
  const icons  = {win:'🏆', loss:'💀', draw:'🤝'};
  const titles = {win:'ПОБЕДА!', loss:'ПОРАЖЕНИЕ', draw:'НИЧЬЯ'};
  const subs   = {win:'Все корабли потоплены!', loss:'Твои корабли уничтожены', draw:'Ничья!'};
  setHTML('gameover-icon', icons[result]);
  setText('gameover-title', titles[result]);
  setText('gameover-sub', subs[result]);
  setText('go-shots', String(Game.shots));
  setText('go-hits',  String(Game.hits));
  setText('go-acc',   Game.shots ? Math.round(Game.hits/Game.shots*100)+'%' : '0%');
  if (result === 'win')  { Sound.win();  vibrate([50,30,100,30,200]); }
  if (result === 'loss') { Sound.lose(); vibrate([200]); }
  // Сбрасываем состояние реванша перед показом экрана
  Rematch._clear();
  setTimeout(() => {
    showScreen('gameover');
    const xpBlock = document.getElementById('xp-reward-block');
    if (Game.mode === 'online' && !App.user.isGuest) {
      // Показываем блок сразу — с текущим прогрессом, без анимации
      if (xpBlock) {
        xpBlock.classList.remove('hidden');
        // Рисуем "скелет" — текущий прогресс до получения данных с сервера
        const prog = getXpProgress(App.user.xp || 0);
        setText('xp-ring-level', prog.level);
        setText('xp-progress-text', prog.xpInLevel + ' / ' + prog.xpNeeded + ' XP');
        const bar  = document.getElementById('xp-progress-bar');
        const ring = document.getElementById('xp-ring-fill');
        if (bar)  { bar.style.transition = 'none'; bar.style.width = prog.pct + '%'; }
        if (ring) { ring.style.transition = 'none'; setRingProgress(ring, prog.pct, 80); }
        const gainEl = document.getElementById('xp-gained');
        if (gainEl) gainEl.textContent = '…';
        const lvlUpEl = document.getElementById('xp-levelup');
        if (lvlUpEl) { lvlUpEl.classList.add('hidden'); lvlUpEl.classList.remove('anim-levelup'); }
      }
      // Если xp_reward уже пришёл — запускаем анимацию
      if (Game._pendingXp) {
        setTimeout(() => { showXpReward(Game._pendingXp); Game._pendingXp = null; }, 350);
      } else {
        // Показываем расчётное значение сразу, чтобы игрок не видел '...'
        // Если xp_reward придёт — перезапишет; если нет — через 4с показываем 0
        Game._xpFallbackTimer = setTimeout(() => {
          const gainEl2 = document.getElementById('xp-gained');
          if (gainEl2 && gainEl2.textContent === '…') {
            gainEl2.innerHTML = '<span class="xp-zero">+0 XP</span>';
          }
        }, 4000);
      }
    } else {
      if (xpBlock) xpBlock.classList.add('hidden');
    }
  }, 800);
}

/* ─── WEBSOCKET ──────────────────────────────────── */
const WS = {
  socket: null,
  roomId: null,

  connect(serverUrl) {
    return new Promise((resolve, reject) => {
      try {
        const load = () => this._init(serverUrl, resolve, reject);
        if (!window.io) {
          const s = document.createElement('script');
          s.src = (serverUrl||window.location.origin) + '/socket.io/socket.io.js';
          s.onload = load;
          s.onerror = () => reject(new Error('Не удалось загрузить Socket.io'));
          document.head.appendChild(s);
        } else { load(); }
      } catch(e) { reject(e); }
    });
  },

  _init(serverUrl, resolve, reject) {
    if (this.socket) { this.socket.disconnect(); this.socket = null; }
    this.socket = io(serverUrl || window.location.origin, { transports: ['websocket','polling'] });
    this.socket.once('connect',       () => resolve());
    this.socket.once('connect_error', () => reject(new Error('Ошибка подключения')));

    this.socket.on('disconnect', () => {
      if (Game.active) showModal('Соединение потеряно', 'Игра прервана.', [
        { label: 'В меню', cls: 'btn-primary', action: () => { closeModal(); goToMenu(); }},
      ]);
    });

    // ── Комната для друга создана (только создателю) ──
    this.socket.on('room_created', async ({ roomId }) => {
      this.roomId  = roomId;
      Game.roomId  = roomId;
      setText('waiting-title', 'Ждём друга…');
      setText('waiting-sub',   'Отправь ссылку другу');
      const block = document.getElementById('invite-block');
      if (block) block.classList.remove('hidden');

      // Строим ссылку — в TG всегда через бота, в браузере через origin
      let inviteLink;
      try {
        const cfg = await fetch('/api/config').then(r => r.json());
        const botUsername = cfg.botUsername || 'bteship_bot';
        const appName     = cfg.appName     || 'bteship';
        if (isInsideTelegram()) {
          inviteLink = `https://t.me/${botUsername}/${appName}?startapp=room_${roomId}`;
        } else {
          inviteLink = `${window.location.origin}/?room=${roomId}`;
        }
      } catch(e) {
        inviteLink = isInsideTelegram()
          ? `https://t.me/bteship_bot/bteship?startapp=room_${roomId}`
          : `${window.location.origin}/?room=${roomId}`;
      }

      // Сохраняем ссылку глобально для кнопок
      WS._inviteLink = inviteLink;

      // В Telegram — кнопка шеринга, в браузере — показываем ссылку текстом
      const isInTG = !!window.Telegram?.WebApp?.initData;
      const shareBtn = document.getElementById('btn-share-invite');
      const copyBtn  = document.getElementById('btn-copy-link');
      const linkDisplay = document.getElementById('invite-link-display');
      if (isInTG) {
        if (shareBtn) shareBtn.classList.remove('hidden');
        if (copyBtn)  copyBtn.classList.remove('hidden');
        if (linkDisplay) linkDisplay.classList.add('hidden');
      } else {
        if (shareBtn) shareBtn.classList.add('hidden');
        if (copyBtn)  copyBtn.classList.remove('hidden');
        if (linkDisplay) { linkDisplay.textContent = inviteLink; linkDisplay.classList.remove('hidden'); }
      }
    });

    // ── Матч найден (оба игрока) ──────────────────────
    this.socket.on('matched', ({ roomId, opponent }) => {
      stopSearchUI();
      this.roomId  = roomId;
      Game.roomId  = roomId;
      Game.opponent = { name: opponent.name, id: opponent.playerId, level: opponent.level || 1, rank: opponent.rank || '' };
      setText('waiting-title', `Соперник: ${opponent.name}`);
      setText('waiting-sub',   'Расставляй корабли!');
      const block = document.getElementById('invite-block');
      if (block) block.classList.add('hidden');
      // Загружаем счёт дуэли
      if (!App.user.isGuest && opponent.playerId) {
        fetch(`/api/duel/${App.user.id}/${opponent.playerId}`)
          .then(r => r.json())
          .then(j => {
            if (j.ok) {
              Game.opponent.duel = j.data;
              // Перерисовываем аватар с duel-счётом если игра уже началась
              renderOpponentAvatar(Game.opponent.name, false);
            }
          })
          .catch(() => {});
      }
      setTimeout(() => startPlacement('online'), 800);
    });

    // ── Соперник расставил ────────────────────────────
    this.socket.on('enemy_ready', () => {
      setText('waiting-sub', 'Соперник готов! Ждём тебя…');
    });

    // ── Оба готовы — игра началась ────────────────────
    this.socket.on('game_start', ({ isMyTurn }) => {
      const myShips = Placement.getShipsForGame();
      // Для онлайна создаём "виртуальный" флот врага из SHIP_DEFS
      // (реальных позиций нет, но нам нужно отображать сколько осталось)
      const enemyShips = [];
      for (const def of SHIP_DEFS)
        for (let k = 0; k < def.count; k++)
          enemyShips.push({ cells: [], sunk: false, size: def.size });

      startGame('online', Placement.board, myShips, makeBoard(), enemyShips, Game.opponent);
      Game.isMyTurn = isMyTurn;
      updateGameStatus();
      renderGameBoard();
    });

    // ── Результат выстрела ────────────────────────────
    this.socket.on('shot_result', ({ r, c, hit, sunk, shooter, gameOver, winner }) => {
      const isMine = shooter === App.user.id;

      if (isMine) {
        Game.shots++;
        if (hit) {
          Game.hits++;
          Game.myShots[r][c] = CELL_HIT;
          if (sunk) {
            WS._sinkShipAt(Game.myShots, r, c);
            WS._markEnemyShipSunk(Game.myShots, r, c);
            // Звук потопления врага = такой же как когда враг топит наш корабль
            Sound.sunk(); vibrate([80, 30, 80]);
          } else {
            Sound.hit(); vibrate([30,10,30]);
          }
          Game.isMyTurn = true;
          if (!isDesktop()) setShowingField(true);
        } else {
          Game.myShots[r][c] = CELL_MISS;
          Sound.miss(); vibrate([10]);
          Game.isMyTurn = false;
          // Переключаем на поле противника только если включена настройка
          if (!isDesktop() && App.settings.showEnemyMoves) setShowingField(false);
        }
      } else {
        // Соперник стреляет по нашим кораблям — вибрация
        if (hit) {
          Game.myBoard[r][c]    = CELL_HIT;
          Game.enemyShots[r][c] = CELL_HIT;
          if (sunk) {
            WS._sinkShipAt(Game.myBoard, r, c);
            for (let rr = 0; rr < BOARD_SIZE; rr++)
              for (let cc = 0; cc < BOARD_SIZE; cc++)
                if (Game.myBoard[rr][cc] === CELL_SUNK || Game.myBoard[rr][cc] === CELL_MISS)
                  Game.enemyShots[rr][cc] = Game.myBoard[rr][cc];
            Sound.sunk(); vibrate([80, 30, 80]); // враг потопил наш корабль
          } else {
            Sound.hit(); vibrate([40]); // враг попал
          }
        } else {
          Game.myBoard[r][c]    = CELL_MISS;
          Game.enemyShots[r][c] = CELL_MISS;
          Game.isMyTurn = true;
          Sound.miss(); vibrate([10]); // враг промахнулся
          if (!isDesktop() && App.settings.showEnemyMoves) setShowingField(true);
        }
      }

      updateGameStatus();
      renderGameBoard();
      if (gameOver) endGame(winner === App.user.id ? 'win' : 'loss');
    });

    // ── Соперник вышел ────────────────────────────────
    this.socket.on('opponent_left', () => {
      if (Game.active) {
        showModal('Соперник вышел', 'Тебе засчитана победа!', [
          { label: 'Ок', cls: 'btn-primary', action: () => { closeModal(); endGame('win'); }},
        ]);
      } else if (['placement', 'waiting'].includes(currentScreen)) {
        // Соперник вышел во время расстановки — уведомляем и возвращаем в меню
        WS.disconnect();
        showModal('Соперник вышел', 'Соперник покинул расстановку. Возвращаемся в меню.', [
          { label: 'Ок', cls: 'btn-primary', action: () => { closeModal(); goToMenu(); }},
        ]);
      }
    });

    this.socket.on('room_expired', () => {
      WS.disconnect();
      // Показываем экран с уведомлением и обратным отсчётом
      showScreen('waiting');
      const titleEl = document.getElementById('waiting-title');
      const subEl   = document.getElementById('waiting-sub');
      const block   = document.getElementById('invite-block');
      if (block) block.classList.add('hidden');
      if (titleEl) titleEl.textContent = 'Комната недоступна';
      if (subEl)   subEl.textContent   = 'Ссылка устарела или комната была закрыта. Переход в главное меню через 10 сек…';

      // Кнопка немедленного перехода
      const cancelBtn = document.getElementById('btn-cancel-wait');
      if (cancelBtn) {
        cancelBtn.textContent = 'Главное меню';
        cancelBtn.onclick = () => { clearTimeout(window._expiredTimer); showScreen('menu'); };
      }

      // Автоматический редирект через 10 секунд
      let secs = 10;
      const tick = () => {
        secs--;
        if (subEl) subEl.textContent = `Ссылка устарела или комната была закрыта. Переход через ${secs} сек…`;
        if (secs <= 0) { showScreen('menu'); if (cancelBtn) { cancelBtn.textContent = 'Отмена'; cancelBtn.onclick = null; } }
        else window._expiredTimer = setTimeout(tick, 1000);
      };
      window._expiredTimer = setTimeout(tick, 1000);
    });

    this.socket.on('error_msg', ({ message }) => {
      showModal('Ошибка', message, [{ label: 'Ок', cls: 'btn-ghost', action: closeModal }]);
    });

    // Обновляем счётчик онлайна
    this.socket.on('online_count', ({ count }) => {
      if (initOnlineCounter.update) initOnlineCounter.update(count);
    });

    // XP награда по итогам онлайн-боя
    this.socket.on('xp_reward', (xpData) => {
      if (Game._xpFallbackTimer) { clearTimeout(Game._xpFallbackTimer); Game._xpFallbackTimer = null; }
      Game._pendingXp = xpData;
      // Применяем сразу если уже на экране gameover, иначе endGame подберёт
      if (currentScreen === 'gameover') showXpReward(xpData);
    });

    // ── Реакция соперника ────────────────────────────
    this.socket.on('reaction_received', (payload) => {
      showReactionDisplay('opp', payload);
    });

    // ── Реванш ───────────────────────────────────────
    this.socket.on('rematch_requested', () => {
      // Соперник нажал реванш — показываем уведомление и запускаем таймер на кнопке
      const statusEl = document.getElementById('rematch-status');
      if (statusEl) {
        statusEl.textContent = 'Соперник предлагает реванш!';
        statusEl.classList.remove('hidden', 'declined');
        statusEl.classList.add('opponent-wants');
      }
      // Запускаем таймер на кнопке у получателя тоже
      Rematch.startOpponentTimer();
    });

    this.socket.on('rematch_accepted', () => {
      // Оба согласились — стартуем расстановку с тем же соперником в той же комнате
      Rematch._clear();
      // roomId остаётся тем же — просто инициируем расстановку
      startPlacement('online');
    });

    this.socket.on('rematch_declined', () => {
      // Соперник не принял в течение 10 сек — только для того, кто предлагал
      Rematch._clear();
      const statusEl = document.getElementById('rematch-status');
      if (statusEl) {
        statusEl.textContent = 'Соперник отказался от реванша';
        statusEl.classList.remove('hidden', 'opponent-wants');
        statusEl.classList.add('declined');
      }
      const rematchBtn = document.getElementById('btn-rematch');
      if (rematchBtn) {
        rematchBtn.disabled = true;
        rematchBtn.textContent = 'Реванш';
        rematchBtn.classList.add('btn-disabled');
      }
      setTimeout(() => goToMenu(), 3000);
    });

    // п.5: соперник закрыл вкладку/приложение — нам победа
    this.socket.on('opponent_disconnected_win', () => {
      clearTurnWarningUI();
      showModal('Победа!', 'Соперник покинул игру. Тебе засчитана победа!', [
        { label: 'Ок', cls: 'btn-primary', action: () => { closeModal(); endGame('win'); }},
      ]);
    });

    // п.6: предупреждение — осталось 20 секунд
    this.socket.on('turn_warning', ({ secondsLeft }) => {
      if (Game.isMyTurn) startTurnWarningUI(secondsLeft);
    });

    // п.6: отмена предупреждения (после хода)
    this.socket.on('turn_warning_cancel', () => {
      clearTurnWarningUI();
    });

    // п.6: таймер истёк у кого-то
    this.socket.on('turn_timeout', ({ playerId, timeouts }) => {
      clearTurnWarningUI();
      const isMine = playerId === App.user.id;
      const msg = isMine
        ? `Ты не успел сделать ход! (просрочка ${timeouts}/${2})`
        : `Соперник не успел сделать ход!`;
      // Кратко показываем уведомление в статусе
      const statusEl = document.getElementById('game-status');
      if (statusEl) { statusEl.textContent = msg; statusEl.style.color = 'var(--yellow)'; }
      setTimeout(updateGameStatus, 2000);
    });

    // п.6: поражение из-за 2 просрочек
    this.socket.on('game_over_timeout', ({ winner, loser }) => {
      clearTurnWarningUI();
      endGame(winner === App.user.id ? 'win' : 'loss');
    });

    // п.6: ход передан (от сервера при тайм-ауте)
    this.socket.on('turn', ({ isMyTurn }) => {
      Game.isMyTurn = isMyTurn;
      clearTurnWarningUI();
      updateGameStatus();
      renderGameBoard();
    });

    // п.5: соперник сдался — нам победа немедленно
    this.socket.on('opponent_surrendered', () => {
      clearTurnWarningUI();
      endGame('win');
    });

    // п.5: сдача подтверждена — показываем поражение
    this.socket.on('surrender_confirmed', () => {
      clearTurnWarningUI();
      endGame('loss');
    });
  },

  matchmake(mode, extraData = {}) {
    if (!this.socket) return;
    this.socket.emit('matchmake', { mode, playerId: App.user.id, playerName: App.user.name, ...extraData });
  },

  sendShot(r, c) {
    if (!this.socket) return;
    this.socket.emit('shoot', { roomId: this.roomId, r, c });
  },

  sendReaction(emoji) {
    if (!this.socket || !this.roomId) return;
    this.socket.emit('reaction', { roomId: this.roomId, emoji });
  },

  sendShips(field) {
    if (!this.socket) return;
    this.socket.emit('place_ships', { roomId: this.roomId, field });
  },

  disconnect() {
    if (this.socket) { this.socket.disconnect(); this.socket = null; }
  },

  // Flood-fill: находим все клетки корабля (CELL_HIT), помечаем CELL_SUNK, заполняем периметр CELL_MISS
  _sinkShipAt(board, startR, startC) {
    const visited = new Set();
    const stack   = [[startR, startC]];
    const shipCells = [];
    while (stack.length) {
      const [r, c] = stack.pop();
      const key = r + ',' + c;
      if (visited.has(key)) continue;
      visited.add(key);
      if (board[r]?.[c] === CELL_HIT) {
        shipCells.push([r, c]);
        for (const [nr, nc] of [[r-1,c],[r+1,c],[r,c-1],[r,c+1]])
          if (inBounds(nr, nc)) stack.push([nr, nc]);
      }
    }
    for (const [r, c] of shipCells) board[r][c] = CELL_SUNK;
    for (const [r, c] of shipCells)
      for (let dr = -1; dr <= 1; dr++)
        for (let dc = -1; dc <= 1; dc++) {
          const nr = r+dr, nc = c+dc;
          if (inBounds(nr, nc) && board[nr][nc] === CELL_EMPTY) board[nr][nc] = CELL_MISS;
        }
    return shipCells;
  },

  // Помечает один незатопленный корабль нужного размера в Game.enemyShips как потопленный
  _markEnemyShipSunk(myShots, r, c) {
    // Считаем размер потопленного корабля через flood-fill по CELL_SUNK
    const visited = new Set();
    const stack   = [[r, c]];
    let size = 0;
    while (stack.length) {
      const [cr, cc] = stack.pop();
      const key = cr + ',' + cc;
      if (visited.has(key)) continue;
      visited.add(key);
      if (myShots[cr]?.[cc] === CELL_SUNK) {
        size++;
        for (const [nr, nc] of [[cr-1,cc],[cr+1,cc],[cr,cc-1],[cr,cc+1]])
          if (inBounds(nr, nc)) stack.push([nr, nc]);
      }
    }
    // Находим первый незатопленный корабль такого размера
    const ship = Game.enemyShips.find(s => !s.sunk && s.size === size);
    if (ship) ship.sunk = true;
  },
};

/* ─── ОНЛАЙН ИГРА ────────────────────────────────── */
/* ─── АНИМАЦИЯ ПОИСКА ────────────────────────────────── */
let _searchAnimTimer = null;
let _searchClockTimer = null;

// Анимация ячейки лоадера (те же 3 состояния что и при поиске соперника)
let _loadingCellTimer = null;
function startLoadingCellAnim() {
  stopLoadingCellAnim();
  const cell = document.getElementById('loading-cell');
  if (!cell) return;
  const states = ['', 'state-miss', 'state-hit'];
  let idx = 0;
  const tick = () => {
    cell.className = 'search-cell ' + states[idx];
    idx = (idx + 1) % states.length;
    _loadingCellTimer = setTimeout(tick, 850);
  };
  tick();
}
function stopLoadingCellAnim() {
  clearTimeout(_loadingCellTimer);
  _loadingCellTimer = null;
}

function startSearchUI() {
  stopSearchUI();
  const cell = document.getElementById('search-cell');
  const timerEl = document.getElementById('search-timer');
  if (!cell || !timerEl) return;

  // Таймер поиска
  timerEl.classList.remove('hidden');
  let secs = 0;
  const pad = n => String(n).padStart(2, '0');
  timerEl.textContent = '00:00';
  _searchClockTimer = setInterval(() => {
    secs++;
    timerEl.textContent = `${pad(Math.floor(secs/60))}:${pad(secs%60)}`;
  }, 1000);

  // Анимация 3 состояний: sunk → miss → hit → sunk…
  const states = ['', 'state-miss', 'state-hit'];
  let idx = 0;
  const tick = () => {
    cell.className = 'search-cell ' + states[idx];
    idx = (idx + 1) % states.length;
    _searchAnimTimer = setTimeout(tick, 850);
  };
  tick();
}

function stopSearchUI() {
  clearTimeout(_searchAnimTimer); _searchAnimTimer = null;
  clearInterval(_searchClockTimer); _searchClockTimer = null;
  const timerEl = document.getElementById('search-timer');
  if (timerEl) timerEl.classList.add('hidden');
  const cell = document.getElementById('search-cell');
  if (cell) cell.className = 'search-cell';
}

async function startOnline(mode) {
  // Очищаем экран ожидания до подключения
  setText('waiting-title', 'Подключение…');
  setText('waiting-sub',   '');
  document.getElementById('invite-block')?.classList.add('hidden');
  showScreen('waiting');

  const serverUrl = App.settings.server || window.location.origin;
  try {
    await WS.connect(serverUrl);

    if (mode === 'random') {
      setText('waiting-title', 'Ищем соперника…');
      setText('waiting-sub',   'Это займёт какое-то время');
      startSearchUI();
      WS.matchmake('random');
    } else if (mode === 'friend') {
      setText('waiting-title', 'Создаём комнату…');
      setText('waiting-sub',   '');
      WS.matchmake('friend_create');
    } else if (mode.startsWith('friend_join:')) {
      const roomId = mode.slice('friend_join:'.length);
      setText('waiting-title', 'Подключаемся к другу…');
      setText('waiting-sub',   '');
      WS.matchmake('friend_join', { roomId });
    }
  } catch(e) {
    showModal('Нет сервера', 'Онлайн недоступен. Сыграть с ботом?', [
      { label: 'С ботом',  cls: 'btn-primary', action: () => { closeModal(); startBotGame('bot-easy'); }},
      { label: 'В меню',   cls: 'btn-ghost',   action: () => { closeModal(); showScreen('menu'); }},
    ]);
  }
}

/* ─── ПЕРЕД-ОНЛАЙН РАССТАНОВКА ───────────────────── */
function startPlacement(mode) {
  pendingGameMode = mode;
  Placement.init();
  showScreen('placement');
}

function startBotGame(mode) {
  pendingGameMode = mode;
  Placement.init();
  showScreen('placement');
}

/* ─── БУРГЕР-МЕНЮ ────────────────────────────────── */
function initBurger() {
  const btn       = document.getElementById('burger-btn');       // бургер в игре
  const btnMenu   = document.getElementById('btn-burger-menu');  // бургер в главном меню
  const menu      = document.getElementById('burger-menu');
  const overlay   = document.getElementById('burger-overlay');

  function open()  {
    menu?.classList.add('open');
    overlay?.classList.remove('hidden');
    // Скрываем Сдаться если не в игре
    const surrender = document.getElementById('burger-surrender');
    if (surrender) surrender.style.display = ['game','waiting','placement'].includes(currentScreen) ? '' : 'none';
  }
  function close() { menu?.classList.remove('open'); overlay?.classList.add('hidden'); }

  btn?.addEventListener('click', () => menu?.classList.contains('open') ? close() : open());
  btnMenu?.addEventListener('click', () => menu?.classList.contains('open') ? close() : open());
  overlay?.addEventListener('click', close);

  document.getElementById('burger-surrender')?.addEventListener('click', () => {
    close();
    showModal('Сдаться?', 'Ты хочешь завершить игру?', [
      { label: 'Продолжить', cls: 'btn-ghost',  action: closeModal },
      { label: 'Сдаться',   cls: 'btn-danger',  action: () => {
        closeModal();
        if (Game.mode === 'online' && WS.socket && WS.roomId) {
          WS.socket.emit('surrender', { roomId: WS.roomId });
        } else {
          endGame('loss');
        }
      }},
    ]);
  });

  document.getElementById('burger-sound')?.addEventListener('click', () => {
    App.settings.sound = !App.settings.sound;
    saveJSON('bs_settings', App.settings);
    updateBurgerSound();
    initSoundButton(); // обновляет иконку в шапке меню
  });

  document.getElementById('burger-enemy-moves')?.addEventListener('click', () => {
    App.settings.showEnemyMoves = !App.settings.showEnemyMoves;
    saveJSON('bs_settings', App.settings);
    updateBurgerEnemyMoves();
  });

  document.getElementById('burger-stats-btn')?.addEventListener('click', () => {
    close();
    showScreen('profile');
    // Открываем вкладку статистики
    setTimeout(() => {
      document.querySelectorAll('.profile-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === 'stats'));
      document.querySelectorAll('.profile-tab-content').forEach(el => el.classList.toggle('hidden', el.id !== 'profile-tab-stats'));
      renderStatsScreen();
    }, 50);
  });

  document.getElementById('burger-settings-btn')?.addEventListener('click', () => {
    close();
    // Кнопка "Назад" ведёт в игру если она активна
    const settingsBackBtn = document.getElementById('settings-back-btn');
    if (settingsBackBtn) settingsBackBtn.dataset.screen = Game.active ? 'game' : 'menu';
    showScreen('settings');
  });
}

function updateBurgerSound() {
  const btn  = document.getElementById('burger-sound');
  if (!btn) return;
  const muted = !App.settings.sound;
  btn.classList.toggle('muted', muted);
  // Показываем/скрываем волны в SVG иконке
  const waves = document.getElementById('burger-sound-waves');
  if (waves) waves.style.display = muted ? 'none' : '';
}

function updateBurgerEnemyMoves() {
  const btn = document.getElementById('burger-enemy-moves');
  if (!btn) return;
  const on = App.settings.showEnemyMoves !== false;
  btn.classList.toggle('muted', !on);
  // Меняем opacity иконки глаза
  const svg = btn.querySelector('svg');
  if (svg) svg.style.opacity = on ? '1' : '0.35';
}

/* ─── КНОПКА ЗВУКА В ШАПКЕ ───────────────────────── */
function initSoundButton() {
  const btn = document.getElementById('btn-sound-toggle');
  if (!btn) return;
  const muted = !App.settings.sound;
  btn.classList.toggle('muted', muted);
  const waves = document.getElementById('sound-waves');
  if (waves) waves.style.display = muted ? 'none' : '';
  let line = btn.querySelector('.sound-mute-line');
  if (muted && !line) {
    line = document.createElementNS('http://www.w3.org/2000/svg','line');
    line.setAttribute('class','sound-mute-line');
    line.setAttribute('x1','1'); line.setAttribute('y1','1');
    line.setAttribute('x2','23'); line.setAttribute('y2','23');
    btn.querySelector('svg')?.appendChild(line);
  } else if (!muted) { line?.remove(); }

  btn.onclick = () => {
    App.settings.sound = !App.settings.sound;
    saveJSON('bs_settings', App.settings);
    const cb = document.getElementById('setting-sound');
    if (cb) cb.checked = App.settings.sound;
    initSoundButton();
    updateBurgerSound();
    if (App.settings.sound) Sound.click();
  };
}

/* ─── TELEGRAM ───────────────────────────────────── */
function initTelegram() {
  if (!isInsideTelegram()) return;
  try {
    const tg = Telegram.WebApp;
    tg.ready();
    tg.expand();
    try { tg.setHeaderColor('secondary_bg_color'); } catch(e) {}
    try { tg.enableClosingConfirmation(); } catch(e) {}
    // Всегда тёмная тема — независимо от настроек TG
    // (светлые схемы будут продаваться отдельно)
  } catch(e) {}
}

/* ─── НАВИГАЦИЯ ──────────────────────────────────── */
function updateMenuUI() {
  const isGuest = !!App.user.isGuest;
  setText('user-name', App.user.name);
  const av = document.getElementById('user-avatar');
  if (av) {
    av.innerHTML = App.user.photo ? `<img src="${App.user.photo}" alt="" />` : (App.user.name[0]||'?').toUpperCase();
  }
  const statsMini = document.querySelector('.stats-mini');
  if (statsMini) statsMini.style.display = isGuest ? 'none' : '';
  updateMenuLevel();
  updateMenuStats();
}

// Цвета рамок по тирам — совпадают с .level-frame-N
const LEVEL_FRAME_COLORS = {
  1:'#607d8b',2:'#607d8b',3:'#607d8b',4:'#607d8b',
  5:'#43a047',6:'#43a047',7:'#43a047',8:'#43a047',9:'#43a047',
  10:'#1e88e5',11:'#1e88e5',12:'#1e88e5',13:'#1e88e5',14:'#1e88e5',
  15:'#8e24aa',16:'#8e24aa',17:'#8e24aa',18:'#8e24aa',19:'#8e24aa',
  20:'#e53935',21:'#e53935',22:'#e53935',23:'#e53935',24:'#e53935',
  25:'#f57f17',26:'#f57f17',27:'#f57f17',28:'#f57f17',29:'#f57f17',
  30:'#ffd600',
};

function updateMenuLevel() {
  const xp    = App.user.xp || 0;
  const prog  = getXpProgress(xp);
  setText('menu-level-badge', prog.level);
  setText('user-rank', prog.rank);
  const badge = document.getElementById('menu-level-badge');
  if (badge) badge.className = 'level-badge level-bg-' + prog.level;
  // Синхронизируем цвет рамки аватара на главной с цветом в профиле
  const avatar = document.getElementById('user-avatar');
  if (avatar) {
    const color = LEVEL_FRAME_COLORS[prog.level] || '#607d8b';
    avatar.style.borderColor = color;
    avatar.style.borderWidth = '2px';
    avatar.style.borderStyle = 'solid';
  }
}

/* ─── XP ПОСТ-БОЙ АНИМАЦИЯ ──────────────────────── */
function showXpReward(xpData) {
  if (!xpData || App.user.isGuest) return;
  const block = document.getElementById('xp-reward-block');
  if (!block) return;

  App.user.xp = xpData.xpAfter;
  saveJSON('bs_user', App.user);
  updateMenuLevel();

  block.classList.remove('hidden');

  // Случай: нечестный бой — 0 XP
  if (xpData.xpGain === 0) {
    const gainEl = document.getElementById('xp-gained');
    if (gainEl) {
      gainEl.innerHTML = '<span class="xp-zero">+0 XP</span>';
    }
    const prog = getXpProgress(xpData.xpAfter);
    setText('xp-ring-level', prog.level);
    setText('xp-progress-text', prog.xpInLevel + ' / ' + prog.xpNeeded + ' XP');
    const bar  = document.getElementById('xp-progress-bar');
    const ring = document.getElementById('xp-ring-fill');
    if (bar)  { bar.style.transition = 'none'; bar.style.width = prog.pct + '%'; }
    if (ring) { ring.style.transition = 'none'; setRingProgress(ring, prog.pct, 80); }
    return;
  }

  const progBefore = getXpProgress(xpData.xpBefore);
  const progAfter  = getXpProgress(xpData.xpAfter);
  const baseXp  = xpData.baseXp  || Math.min(xpData.xpGain, 1000);
  const bonusXp = xpData.bonusXp || Math.max(0, xpData.xpGain - baseXp);

  // Сначала показываем базовые, потом бонус
  const gainEl = document.getElementById('xp-gained');
  if (gainEl) {
    gainEl.textContent = '+' + baseXp;
  }

  setText('xp-ring-level', progBefore.level);
  setText('xp-progress-text', progBefore.xpInLevel + ' / ' + progBefore.xpNeeded + ' XP');

  const lvlEl  = document.getElementById('xp-ring-level');
  const lvlUpEl = document.getElementById('xp-levelup');
  if (lvlUpEl) { lvlUpEl.classList.add('hidden'); lvlUpEl.classList.remove('anim-levelup'); }

  const bar   = document.getElementById('xp-progress-bar');
  const ring  = document.getElementById('xp-ring-fill');
  const barBonus = document.getElementById('xp-progress-bar-bonus');

  // Шаг 0: мгновенно ставим стартовую позицию (без transition)
  if (bar)  { bar.style.transition = 'none'; bar.style.width = progBefore.pct + '%'; }
  if (ring) { ring.style.transition = 'none'; setRingProgress(ring, progBefore.pct, 80); }
  if (barBonus) { barBonus.style.transition = 'none'; barBonus.style.width = '0%'; }

  // Шаг 1: через 350ms — анимируем базовые XP
  const xpAfterBase = xpData.xpBefore + baseXp;
  const progMid = getXpProgress(xpAfterBase);

  setTimeout(() => {
    // Если level up при базовых — сначала доходим до 100%, потом меняем уровень и идём дальше
    if (progMid.level > progBefore.level) {
      // Анимируем до конца текущего уровня
      if (bar)  { bar.style.transition = ''; bar.style.width = '100%'; }
      if (ring) { ring.style.transition = ''; setRingProgress(ring, 100, 80); }
      setTimeout(() => {
        _levelUpFlash(lvlEl, progMid.level, lvlUpEl);
        if (bar)  { bar.style.transition = 'none'; bar.style.width = '0%'; }
        if (ring) { ring.style.transition = 'none'; setRingProgress(ring, 0, 80); }
        setText('xp-ring-level', progMid.level);
        setTimeout(() => {
          if (bar)  { bar.style.transition = ''; bar.style.width = progMid.pct + '%'; }
          if (ring) { ring.style.transition = ''; setRingProgress(ring, progMid.pct, 80); }
          setText('xp-progress-text', progMid.xpInLevel + ' / ' + progMid.xpNeeded + ' XP');
          _showBonusPhase(xpData, progMid, progAfter, bar, ring, barBonus, gainEl, baseXp, bonusXp, lvlEl, lvlUpEl);
        }, 350);
      }, 700);
    } else {
      if (bar)  { bar.style.transition = ''; bar.style.width = progMid.pct + '%'; }
      if (ring) { ring.style.transition = ''; setRingProgress(ring, progMid.pct, 80); }
      setText('xp-progress-text', progMid.xpInLevel + ' / ' + progMid.xpNeeded + ' XP');
      _showBonusPhase(xpData, progMid, progAfter, bar, ring, barBonus, gainEl, baseXp, bonusXp, lvlEl, lvlUpEl);
    }
  }, 350);
}

function _showBonusPhase(xpData, progMid, progAfter, bar, ring, barBonus, gainEl, baseXp, bonusXp, lvlEl, lvlUpEl) {
  if (bonusXp <= 0) {
    // Нет бонуса — просто финальный шаг level-up если нужен
    if (xpData.levelUp && progAfter.level > progMid.level) {
      _doFinalLevelUp(progAfter, bar, ring, lvlEl, lvlUpEl);
    } else if (xpData.levelUp) {
      setTimeout(() => _levelUpFlash(lvlEl, progAfter.level, lvlUpEl), 400);
    }
    return;
  }

  // Показываем бонусную фазу через 600ms после базовой
  setTimeout(() => {
    if (gainEl) gainEl.innerHTML =
      '<span class="xp-base">+' + baseXp + '</span>' +
      '<span class="xp-bonus"> +' + bonusXp + '</span>';

    // Анимируем бонусный кусок поверх
    if (barBonus) {
      const startPct = progMid.pct;
      barBonus.style.left = startPct + '%';
      barBonus.style.transition = 'none';
      barBonus.style.width = '0%';
      setTimeout(() => {
        barBonus.style.transition = '';
        if (progAfter.level > progMid.level) {
          // Level up в бонусной фазе
          barBonus.style.width = (100 - startPct) + '%';
          setTimeout(() => {
            _levelUpFlash(lvlEl, progAfter.level, lvlUpEl);
            if (bar) { bar.style.transition = 'none'; bar.style.width = '0%'; }
            if (ring) { ring.style.transition = 'none'; setRingProgress(ring, 0, 80); }
            if (barBonus) { barBonus.style.transition = 'none'; barBonus.style.width = '0%'; barBonus.style.left = '0%'; }
            setText('xp-ring-level', progAfter.level);
            setTimeout(() => {
              if (bar)  { bar.style.transition = ''; bar.style.width = progAfter.pct + '%'; }
              if (ring) { ring.style.transition = ''; setRingProgress(ring, progAfter.pct, 80); }
              setText('xp-progress-text', progAfter.xpInLevel + ' / ' + progAfter.xpNeeded + ' XP');
            }, 200);
          }, 700);
        } else {
          barBonus.style.width = (progAfter.pct - startPct) + '%';
          if (ring) { ring.style.transition = ''; setRingProgress(ring, progAfter.pct, 80); }
          setText('xp-progress-text', progAfter.xpInLevel + ' / ' + progAfter.xpNeeded + ' XP');
          if (xpData.levelUp) setTimeout(() => _levelUpFlash(lvlEl, progAfter.level, lvlUpEl), 500);
        }
      }, 50);
    }
  }, 600);
}

function _doFinalLevelUp(progAfter, bar, ring, lvlEl, lvlUpEl) {
  setTimeout(() => {
    if (bar)  { bar.style.transition = ''; bar.style.width = '100%'; }
    if (ring) { ring.style.transition = ''; setRingProgress(ring, 100, 80); }
    setTimeout(() => {
      _levelUpFlash(lvlEl, progAfter.level, lvlUpEl);
      if (bar)  { bar.style.transition = 'none'; bar.style.width = '0%'; }
      if (ring) { ring.style.transition = 'none'; setRingProgress(ring, 0, 80); }
      setText('xp-ring-level', progAfter.level);
      setTimeout(() => {
        if (bar)  { bar.style.transition = ''; bar.style.width = progAfter.pct + '%'; }
        if (ring) { ring.style.transition = ''; setRingProgress(ring, progAfter.pct, 80); }
        setText('xp-progress-text', progAfter.xpInLevel + ' / ' + progAfter.xpNeeded + ' XP');
      }, 300);
    }, 700);
  }, 400);
}

function _levelUpFlash(lvlEl, level, lvlUpEl) {
  if (lvlEl) {
    lvlEl.classList.add('level-pop');
    setTimeout(() => lvlEl.classList.remove('level-pop'), 600);
  }
  if (lvlUpEl) {
    lvlUpEl.classList.remove('hidden');
    lvlUpEl.textContent = 'Новый уровень!';
    lvlUpEl.classList.add('anim-levelup');
    Sound.win && Sound.win();
  }
}

// Рисует заполнение "обводки" квадрата через stroke-dasharray на rect
function setRingProgress(rectEl, pct, size) {
  const perim = 4 * (size - 8);
  const fill  = perim * pct / 100;
  rectEl.style.strokeDasharray  = fill + ' ' + (perim - fill);
  rectEl.style.strokeDashoffset = (perim * 0.25).toString();
}

/* ─── ЭКРАН ПРОФИЛЯ ──────────────────────────────── */
async function renderProfileScreen(tab) {
  const isGuest = !!App.user.isGuest;
  const authBlock  = document.getElementById('profile-content-auth');
  const guestBlock = document.getElementById('profile-content-guest');
  if (authBlock)  authBlock.style.display  = isGuest ? 'none' : '';
  if (guestBlock) guestBlock.style.display = isGuest ? ''     : 'none';
  if (isGuest) return;

  // Инициализируем табы один раз
  if (!renderProfileScreen._tabsInited) {
    renderProfileScreen._tabsInited = true;
    document.querySelectorAll('.profile-tab[data-tab]').forEach(btn => {
      btn.addEventListener('click', () => {
        const t = btn.dataset.tab;
        document.querySelectorAll('.profile-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === t));
        document.querySelectorAll('.profile-tab-content').forEach(el => el.classList.toggle('hidden', el.id !== 'profile-tab-' + t));
        if (t === 'stats') renderStatsScreen();
        if (t === 'inventory') { (_shopItems.length ? Promise.resolve() : loadShopData()).then(() => renderInventory()); }
      });
    });
    document.getElementById('profile-stats-mode-toggle')?.addEventListener('click', e => {
      const btn = e.target.closest('[data-mode]');
      if (btn) renderStatsScreen(btn.dataset.mode);
    });
  }

  // Открываем нужную вкладку если передана
  if (tab) {
    document.querySelectorAll('.profile-tab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
    document.querySelectorAll('.profile-tab-content').forEach(el => el.classList.toggle('hidden', el.id !== 'profile-tab-' + tab));
    if (tab === 'stats') renderStatsScreen();
  }

  const xp    = App.user.xp || 0;
  const prog  = getXpProgress(xp);

  setText('profile-name',  App.user.name);
  setText('profile-rank',  prog.rank);
  setText('profile-level-tag', 'Ур. ' + prog.level);
  setText('profile-ring-level', prog.level);
  setText('profile-xp-current', prog.xpInLevel);
  setText('profile-xp-needed',  prog.xpNeeded);

  // Прогресс-бар
  const bar = document.getElementById('profile-progress-bar');
  if (bar) bar.style.width = prog.pct + '%';

  // Рамка аватара с уровнем
  const frameEl = document.getElementById('profile-avatar-frame');
  if (frameEl) frameEl.className = 'profile-avatar-frame level-frame-' + prog.level;

  const avEl = document.getElementById('profile-avatar');
  if (avEl) {
    avEl.innerHTML = App.user.photo
      ? `<img src="${App.user.photo}" alt="">`
      : (App.user.name[0]||'?').toUpperCase();
  }

  // Уровень-бейдж
  const levelTag = document.getElementById('profile-level-tag');
  if (levelTag) levelTag.className = 'profile-level-tag level-bg-' + prog.level;

  // Ring SVG
  const ringFill = document.getElementById('profile-ring-fill');
  if (ringFill) setRingProgress(ringFill, prog.pct, 120);

  // Обновляем XP с сервера и перерисовываем только числа — без рекурсии
  if (!App.user.isGuest) {
    try {
      const res = await fetch('/api/xp/' + App.user.id);
      const j   = await res.json();
      if (j.ok && j.data && j.data.xp !== App.user.xp) {
        App.user.xp = j.data.xp;
        saveJSON('bs_user', App.user);
        // Обновляем только XP-элементы без полного перерендера
        const p2 = getXpProgress(App.user.xp);
        setText('profile-level-tag', 'Ур. ' + p2.level);
        setText('profile-ring-level', p2.level);
        setText('profile-xp-current', p2.xpInLevel);
        setText('profile-xp-needed',  p2.xpNeeded);
        const rf = document.getElementById('profile-ring-fill');
        if (rf) setRingProgress(rf, p2.pct, 120);
        updateMenuLevel();
      }
    } catch(e) {}
  }
}

function bindNav() {
  document.addEventListener('click', e => {
    const btn = e.target.closest('[data-screen]');
    if (!btn) return;
    const scr    = btn.dataset.screen;
    const isBack = btn.classList.contains('btn-back');
    Sound.click();
    if (scr === 'leaderboard') renderLeaderboard();
    if (scr === 'profile')     renderProfileScreen();
    showScreen(scr, { isBack });
  });

  // Открытие профиля по клику на аватар/имя (работает для всех)
  document.getElementById('btn-open-profile')?.addEventListener('click', () => {
    Sound.click();
    renderProfileScreen();
    showScreen('profile');
  });

  document.getElementById('mode-bot-easy')?.addEventListener('click',   () => startBotGame('bot-easy'));
  document.getElementById('mode-bot-hard')?.addEventListener('click',   () => startBotGame('bot-hard'));
  document.getElementById('mode-random')?.addEventListener('click',     () => startOnline('random'));
  document.getElementById('mode-friend')?.addEventListener('click',     () => startOnline('friend'));

  // Расстановка
  document.getElementById('btn-rotate')?.addEventListener('click',       () => { Placement.vertical = !Placement.vertical; Sound.click(); });
  document.getElementById('btn-random-place')?.addEventListener('click', () => Placement.randomize());
  document.getElementById('btn-clear-place')?.addEventListener('click',  () => Placement.clear());

  // Кнопка-подсказка в расстановке
  document.getElementById('btn-placement-help')?.addEventListener('click', () => {
    showModal('Как расставить корабли', '', [
      { label: 'Понятно!', cls: 'btn-primary', action: closeModal },
    ]);
    const body = document.getElementById('modal-body');
    if (body) body.innerHTML =
      '<div class="ph-row">' +
        '<svg class="ph-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>' +
        '<div><div class="ph-title">Поставить</div><div class="ph-desc">Нажми на корабль в доке — он прикрепится к пальцу. Затем нажми на клетку поля — корабль встанет туда.</div></div>' +
      '</div>' +
      '<div class="ph-row">' +
        '<svg class="ph-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-5.1"/></svg>' +
        '<div><div class="ph-title">Повернуть</div><div class="ph-desc">Двойной тап по кораблю переключает горизонталь / вертикаль.</div></div>' +
      '</div>' +
      '<div class="ph-row">' +
        '<svg class="ph-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>' +
        '<div><div class="ph-title">Убрать с поля</div><div class="ph-desc">Зажми корабль на поле — он вернётся в док.</div></div>' +
      '</div>';
  });

  document.getElementById('btn-ready')?.addEventListener('click', () => {
    if (!Placement.allPlaced()) return;
    Sound.click();
    savePlacement(); // сохраняем для следующей игры
    const myShips = Placement.getShipsForGame();
    if (pendingGameMode === 'online') {
      WS.sendShips(Placement.board);
      showScreen('waiting');
      setText('waiting-title', 'Ждём соперника…');
      setText('waiting-sub',   'Соперник расставляет корабли');
    } else {
      startGame(pendingGameMode, Placement.board, myShips, null, null, { name: 'Бот', username: '' });
    }
  });

  // Переключение полей (мобайл)
  document.getElementById('btn-show-enemy')?.addEventListener('click', () => { setShowingField(true);  renderGameBoard(); });
  document.getElementById('btn-show-mine')?.addEventListener('click',  () => { setShowingField(false); renderGameBoard(); });

  // Сдаться (десктоп-кнопка в футере)
  document.getElementById('btn-surrender')?.addEventListener('click', () => {
    showModal('Сдаться?', 'Ты хочешь завершить игру?', [
      { label: 'Продолжить', cls: 'btn-ghost',  action: closeModal },
      { label: 'Сдаться',   cls: 'btn-danger',  action: () => {
        closeModal();
        if (Game.mode === 'online' && WS.socket && WS.roomId) {
          WS.socket.emit('surrender', { roomId: WS.roomId });
          // Не вызываем endGame сразу — ждём surrender_confirmed от сервера
        } else {
          endGame('loss');
        }
      }},
    ]);
  });

  // Реванш
  document.getElementById('btn-rematch')?.addEventListener('click', () => {
    Sound.click();
    const mode = pendingGameMode || 'bot-easy';
    if (mode === 'online') {
      // Онлайн: WS-реванш только с тем же соперником в той же комнате
      if (WS.socket && WS.roomId) {
        Rematch.request();
      } else {
        // Сокет отвалился — реванш невозможен, показываем сообщение
        const statusEl = document.getElementById('rematch-status');
        if (statusEl) {
          statusEl.textContent = 'Соединение потеряно, реванш недоступен';
          statusEl.classList.remove('hidden', 'opponent-wants');
          statusEl.classList.add('declined');
        }
        const btn = document.getElementById('btn-rematch');
        if (btn) { btn.disabled = true; btn.classList.add('btn-disabled'); }
      }
    } else {
      // Бот: просто заново
      startPlacement(mode);
    }
  });

  // Кнопка "Отправить другу" — открывает список чатов TG
  document.getElementById('btn-share-invite')?.addEventListener('click', () => {
    const link = WS._inviteLink;
    if (!link) return;
    const tg = window.Telegram?.WebApp;
    try {
      // Весь контент в text — так текст гарантированно перед ссылкой
      const fullText = `Сыграем в Морской бой?\n${link}`;
      tg.openTelegramLink(`https://t.me/share/url?text=${encodeURIComponent(fullText)}`);
      return;
    } catch(e) {}
    navigator.clipboard?.writeText(`Сыграем в Морской бой?\n${link}`).then(() => {
      const btn = document.getElementById('btn-share-invite');
      if (btn) { btn.textContent = 'Скопировано!'; setTimeout(() => btn.textContent = 'Отправить другу', 2000); }
    });
  });

  // Кнопка "Скопировать ссылку"
  document.getElementById('btn-copy-link')?.addEventListener('click', () => {
    const link = WS._inviteLink;
    if (!link) return;
    navigator.clipboard?.writeText(link).then(() => {
      const btn = document.getElementById('btn-copy-link');
      if (btn) { btn.textContent = 'Скопировано!'; setTimeout(() => btn.textContent = 'Скопировать ссылку', 2000); }
    });
  });

  // Отмена ожидания / расстановки
  document.getElementById('btn-cancel-wait')?.addEventListener('click', () => {
    if (WS.socket && WS.roomId) {
      WS.socket.emit('leave_room', { roomId: WS.roomId });
    }
    stopSearchUI();
    WS.disconnect();
    showScreen('menu');
  });

  // Закрытие модалки по overlay
  document.getElementById('modal-overlay')?.addEventListener('click', e => { if (e.target === e.currentTarget) closeModal(); });

  // Resize — перерисовать лэйаут при смене ориентации/размера
  window.addEventListener('resize', () => { if (Game.active) { setupGameLayout(); renderGameBoard(); } });
}

/* ─── МОДАЛКА ────────────────────────────────────── */
function showModal(title, body, buttons = []) {
  setText('modal-title', title);
  setText('modal-body', body);
  const btnsEl = document.getElementById('modal-btns');
  if (!btnsEl) return;
  btnsEl.innerHTML = '';
  buttons.forEach(b => {
    const btn = document.createElement('button');
    btn.className = 'btn ' + (b.cls || 'btn-secondary');
    btn.textContent = b.label;
    btn.addEventListener('click', b.action);
    btnsEl.appendChild(btn);
  });
  document.getElementById('modal-overlay')?.classList.remove('hidden');
}
function closeModal() { document.getElementById('modal-overlay')?.classList.add('hidden'); }

/* ─── ХРАНИЛИЩЕ ──────────────────────────────────── */
function loadJSON(key, def) { try { const v = localStorage.getItem(key); return v ? JSON.parse(v) : def; } catch(e) { return def; } }
function saveJSON(key, val) { try { localStorage.setItem(key, JSON.stringify(val)); } catch(e) {} }
function setText(id, val) { const el = document.getElementById(id); if(el) el.textContent = val; }
function setHTML(id, val) { const el = document.getElementById(id); if(el) el.innerHTML = val; }

/* ─── СЧЁТЧИК ОНЛАЙНА ────────────────────────────── */
function initOnlineCounter() {
  const update = (count) => {
    document.querySelectorAll('.online-count-val').forEach(el => el.textContent = count);
  };
  initOnlineCounter.update = update;

  // Загружаем socket.io и создаём один постоянный сокет присутствия
  const connect = () => {
    if (initOnlineCounter._sock) return; // уже есть
    const sock = io(window.location.origin, {
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionDelay: 3000,
    });
    initOnlineCounter._sock = sock;

    sock.on('connect', () => {
      // Сообщаем серверу кто мы — сразу при подключении и при реконнекте
      sock.emit('identify', { playerId: App.user?.id || null });
    });
    sock.on('online_count', ({ count }) => update(count));

    // Heartbeat каждые 4 минуты — обновляем lastActive на сервере
    setInterval(() => {
      if (sock.connected) sock.emit('active');
    }, 4 * 60 * 1000);
  };

  if (window.io) {
    connect();
  } else {
    const s = document.createElement('script');
    s.src = '/socket.io/socket.io.js';
    s.onload = connect;
    document.head.appendChild(s);
  }

  // Фолбэк: первое значение через HTTP пока WS не установился
  fetch('/api/online').then(r => r.json()).then(d => update(d.count)).catch(() => {});
}

/* ─── СВАЙП НАЗАД (от 1/3 экрана) ───────────────── */
function initSwipeBack() {
  let touchStartX = null, touchStartY = null;
  const EDGE_START = 0.33;
  const SWIPE_MIN  = 80;

  document.addEventListener('touchstart', (e) => {
    const t = e.touches[0];
    touchStartX = t.clientX;
    touchStartY = t.clientY;
  }, { passive: true });

  document.addEventListener('touchend', (e) => {
    if (touchStartX === null) return;
    const t  = e.changedTouches[0];
    const dx = t.clientX - touchStartX;
    const dy = t.clientY - touchStartY;
    const startFraction = touchStartX / window.innerWidth;
    touchStartX = null; touchStartY = null;

    if (['game','waiting'].includes(currentScreen)) return;
    if (startFraction >= EDGE_START && startFraction <= 0.66 && dx > SWIPE_MIN && Math.abs(dy) < Math.abs(dx)) {
      Sound.click();
      handleSwipeBack();
    }
  }, { passive: true });
}

function _prevBackTarget() {
  switch (currentScreen) {
    case 'mode':        return 'menu';
    case 'placement':   return 'mode';
    case 'leaderboard': return 'menu';
    case 'profile':     return 'menu';
    case 'shop':        return 'menu';
    case 'shop-item':   return _shopItemBackTarget || 'shop';
    case 'stats':       return 'profile';
    case 'settings':    return document.getElementById('settings-back-btn')?.dataset.screen || 'menu';
    case 'gameover':    return 'menu';
    default:            return 'menu';
  }
}

function handleSwipeBack() {
  // Если уходим со страницы темы — восстанавливаем реальную тему
  if (currentScreen === 'shop-item') {
    const item = _shopItems.find(i => i.id === _currentShopItemId);
    if (item?.type === 'theme' || _currentShopItemId === 'theme_dark') {
      applyEquippedThemeFromState();
    }
  }
  showScreen(_prevBackTarget(), { isBack: true });
}

/* ─── АВАТАР В БОЕВОМ ЭКРАНЕ ─────────────────────── */
function renderOpponentAvatar(name, isBot) {
  // Совместимость — обновляем и старый блок (hidden) и новый футер
  updateGameFooter();
}

function updateGameFooter() {
  const myXp     = App.user.xp || 0;
  const myProg   = getXpProgress(myXp);
  const isBot    = !Game.opponent || Game.opponent.name === 'Бот';
  const oppLevel = Game.opponent?.level || 1;
  const oppRank  = Game.opponent?.rank  || '';
  const oppName  = Game.opponent?.name  || (isBot ? 'Бот' : 'Соперник');

  // Мой аватар — квадратный со скруглёнными углами, рамка по уровню
  const meAvatar = document.getElementById('gf-me-avatar');
  if (meAvatar) {
    if (App.user.photo) {
      meAvatar.innerHTML = `<img src="${App.user.photo}" />`;
    } else {
      meAvatar.textContent = (App.user.name[0]||'?').toUpperCase();
    }
    meAvatar.className = `gf-avatar gf-frame-${myProg.level}`;
  }
  // Уровень: level-bg-N = правильный цвет фона (как в рейтинге, профиле и т.д.)
  const meLevel = document.getElementById('gf-me-level');
  if (meLevel) {
    meLevel.textContent = myProg.level;
    meLevel.className   = `gf-level level-bg-${myProg.level}`;
  }
  setText('gf-me-name', App.user.name || 'Я');
  setText('gf-me-rank', myProg.rank);

  // Аватар соперника
  const oppAvatar = document.getElementById('gf-opp-avatar');
  if (oppAvatar) {
    oppAvatar.textContent = isBot ? 'Б' : (oppName[0]||'?').toUpperCase();
    oppAvatar.className   = `gf-avatar gf-avatar-opp gf-frame-${oppLevel}`;
  }
  const oppLevelEl = document.getElementById('gf-opp-level');
  if (oppLevelEl) {
    if (isBot) {
      oppLevelEl.style.display = 'none';
    } else {
      oppLevelEl.style.display = '';
      oppLevelEl.textContent   = oppLevel;
      oppLevelEl.className     = `gf-level level-bg-${oppLevel}`;
    }
  }
  setText('gf-opp-name', oppName);
  setText('gf-opp-rank', isBot ? '' : oppRank);

  // Счёт дуэлей по центру футера (кнопки сдаться в футере больше нет — только бургер)
  const duelBlock = document.getElementById('gf-duel-score');
  const duel = Game.opponent?.duel;
  if (!isBot && duel && Game.mode === 'online') {
    if (duelBlock) {
      duelBlock.style.display = 'flex';
      const mineEl   = document.getElementById('gf-duel-mine');
      const theirsEl = document.getElementById('gf-duel-theirs');
      if (mineEl)   { mineEl.textContent   = duel.myWins;    mineEl.style.color   = duel.myWins > duel.theirWins ? 'var(--green)' : 'var(--text)'; }
      if (theirsEl) { theirsEl.textContent = duel.theirWins; theirsEl.style.color = duel.theirWins > duel.myWins ? 'var(--red)'   : 'var(--text)'; }
    }
  } else {
    if (duelBlock) duelBlock.style.display = 'none';
  }
}

/* ─── УВЕДОМЛЕНИЯ ────────────────────────────────── */
const Notif = {
  _current: null,
  _open: false,
  _pollInterval: null,
  _POLL_MS: 5 * 60 * 1000,  // проверяем каждые 5 минут

  async init() {
    const btn   = document.getElementById('btn-notif');
    const panel = document.getElementById('notif-panel');
    const close = document.getElementById('notif-panel-close');
    if (!btn || !panel) return;

    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      this._open ? this.close() : this.open();
    });
    close?.addEventListener('click', () => this.close());

    document.addEventListener('click', (e) => {
      if (!this._open) return;
      if (panel.contains(e.target) || btn.contains(e.target)) return;
      this.close();
    }, true);

    // Первая загрузка
    await this.fetch();

    // Polling: проверяем каждые 5 минут пока вкладка открыта
    this._pollInterval = setInterval(() => this._poll(), this._POLL_MS);

    // При возврате вкладки на передний план — проверяем сразу
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) this._poll();
    });
  },

  async _poll() {
    try {
      const res = await fetch('/api/notification');
      const j   = await res.json();
      if (!j.ok) return;

      const incoming = j.data;
      const prevId   = this._current?.id ?? null;
      const newId    = incoming?.id ?? null;

      // Уведомление изменилось — обновляем
      if (newId !== prevId) {
        this._current = incoming || null;
        const readId  = loadJSON('bs_notif_read', null);
        const isRead  = !incoming || readId === incoming.id;

        // Перерисовываем только если панель не открыта (не ломаем интерфейс)
        if (!this._open) {
          this._renderPanel();
        }

        // Бейдж показываем всегда если непрочитанное
        this._setBadge(!isRead);

        // Мини-анимация колокольчика при новом уведомлении
        if (!isRead && incoming) this._ringBell();
      }
    } catch(e) {}
  },

  async fetch() {
    try {
      const res = await fetch('/api/notification');
      const j   = await res.json();
      if (j.ok) {
        this._current = j.data || null;
        const readId  = loadJSON('bs_notif_read', null);
        const isRead  = !this._current || readId === this._current.id;
        this._renderPanel();
        this._setBadge(!isRead);
      }
    } catch(e) {}
  },

  _renderPanel() {
    const body = document.getElementById('notif-panel-body');
    if (!body) return;
    // Нет уведомления ИЛИ уже прочитано — показываем пустое состояние
    const readId = loadJSON('bs_notif_read', null);
    const n      = this._current;
    const isRead = !n || readId === n.id;
    if (!n || isRead) {
      body.innerHTML = '<p class="notif-empty">Нет новых уведомлений</p>';
      return;
    }
    const d       = new Date(n.date * 1000);
    const dateStr = d.toLocaleDateString('ru', { day: '2-digit', month: '2-digit', year: '2-digit' });
    body.innerHTML =
      '<div class="notif-card" id="notif-card">' +
        '<div class="notif-card-head">' +
          '<span class="notif-card-title">' + n.title + '</span>' +
          '<span class="notif-card-date">' + dateStr + '</span>' +
        '</div>' +
        '<p class="notif-card-text">' + n.body + '</p>' +
        '<button class="notif-read-btn" id="notif-read-btn">Прочитал</button>' +
      '</div>';
    document.getElementById('notif-read-btn')?.addEventListener('click', () => this.markRead());
  },

  markRead() {
    if (!this._current) return;
    saveJSON('bs_notif_read', this._current.id);
    this._setBadge(false);
    // Сразу перерисовываем панель в пустое состояние
    this._renderPanel();
  },

  _setBadge(show) {
    document.getElementById('notif-badge')?.classList.toggle('hidden', !show);
  },

  // Лёгкая анимация колокольчика при появлении нового уведомления
  _ringBell() {
    const btn = document.getElementById('btn-notif');
    if (!btn) return;
    btn.classList.add('notif-bell-ring');
    setTimeout(() => btn.classList.remove('notif-bell-ring'), 600);
  },

  open() {
    this._open = true;
    this._renderPanel();  // перерисовываем при открытии — данные могли обновиться
    document.getElementById('notif-panel')?.classList.remove('hidden');
  },
  close() { this._open = false; document.getElementById('notif-panel')?.classList.add('hidden'); },
};

/* ═══════════════════════════════════════════════════
   РЕАКЦИИ В БОЮ — бесконечный круговой слайдер
════════════════════════════════════════════════════ */

let _reactionPickerOpen    = false;
let _reactionTimers        = { my: null, opp: null };
let _sliderItems           = [];
let _sliderIdx             = 0;      // центральный индекс (нормализован в [0, total))
let _sliderDragX           = 0;      // смещение во время drag (px, 0 когда не тянем)
let _sliderIsDragging      = false;
let _sliderStartX          = 0;
let _sliderWasDrag         = false;
let _sliderBusy            = false;
let _sliderPlayerReactions = [];

const ITEM_W    = 64;   // px на ячейку (совпадает с CSS .rslider-cell width)
const SHOW_CNT  = 5;    // видимых ячеек (нечётное — центр выделен)

const DEFAULT_REACTIONS = [
  { type: 'emoji', value: '👍' },
  { type: 'emoji', value: '❤️' },
  { type: 'emoji', value: '😂' },
  { type: 'emoji', value: '👎' },
  { type: 'emoji', value: '🤬' },
];

// ── Загрузка реакций ─────────────────────────────────
async function loadCustomReactions() {
  try {
    const uid     = App.user?.id;
    const isGuest = !uid || uid.startsWith('guest_');

    const pubRes = await fetch('/api/reactions').then(r => r.json());
    const allCat = (pubRes.ok && pubRes.data) ? pubRes.data : [];

    let owned = [];
    if (!isGuest) {
      try {
        const invRes = await authFetch(`/api/inventory/${uid}`).then(r => r.json());
        if (invRes.ok && invRes.data) {
          const ownedIds = new Set(
            (invRes.data.items || [])
              .filter(i => i.type === 'reaction' && i.is_active !== 0)
              .map(i => i.item_id)
          );
          owned = allCat.filter(r => ownedIds.has(r.id));
        }
      } catch(e) {}
    }

    _sliderPlayerReactions = owned.map(r => ({
      item_id: r.id, filename: r.filename, reaction_name: r.name,
    }));
    _rebuildSliderItems();
  } catch(e) {}
}

function _rebuildSliderItems() {
  const custom = _sliderPlayerReactions.map(r => ({
    type: 'custom', id: r.item_id, filename: r.filename, name: r.reaction_name,
  }));
  _sliderItems = [...DEFAULT_REACTIONS, ...custom];
  _sliderIdx   = 0;
  _sliderDragX = 0;
  _sliderRender();
}

// ── Рендер ───────────────────────────────────────────
// Рисуем 3 полных копии списка чтобы прокрутка была бесконечной
function _sliderRender() {
  const track = document.getElementById('rslider-track');
  if (!track || !_sliderItems.length) return;
  const N = _sliderItems.length;
  let html = '';
  for (let copy = 0; copy < 3; copy++) {
    for (let i = 0; i < N; i++) {
      const item = _sliderItems[i];
      const inner = item.type === 'custom'
        ? `<video src="/reactions/${item.filename}" autoplay loop muted playsinline></video>`
        : `<span>${item.value}</span>`;
      html += `<div class="rslider-cell" data-i="${i}">${inner}</div>`;
    }
  }
  track.innerHTML = html;
  _sliderPosition(false);
}

// ── Позиционирование ─────────────────────────────────
// _sliderIdx — накопленный (не нормализован), может быть отрицательным или > N
// Это гарантирует что трек движется непрерывно без телепортации
// Для определения активного элемента используем modulo
function _sliderPosition(animate) {
  const track = document.getElementById('rslider-track');
  if (!track || !_sliderItems.length) return;
  const N      = _sliderItems.length;
  const center = Math.floor(SHOW_CNT / 2) * ITEM_W;  // 120px при SHOW_CNT=5

  // x непрерывен: при _sliderIdx=0 → copy1[0] по центру
  // при _sliderIdx=-1 → copy1[-1] = copy0[N-1] по центру (уехали влево)
  // при _sliderIdx=N  → copy2[0] по центру (уехали вправо)
  const x = Math.round(-(N + _sliderIdx) * ITEM_W + center + _sliderDragX);

  track.style.transition = animate ? 'transform .25s cubic-bezier(.3,1,.4,1)' : 'none';
  track.style.transform  = `translateX(${x}px)`;

  // Подсветка: нормализуем только для определения активного элемента
  const activeI = ((_sliderIdx % N) + N) % N;
  track.querySelectorAll('.rslider-cell').forEach(el => {
    el.classList.toggle('rslider-cell-active', Number(el.dataset.i) === activeI);
  });
}

// Re-anchor: после анимации тихо возвращаем _sliderIdx в диапазон [0,N)
// без изменения визуальной позиции — copy1 становится "домом"
function _sliderReanchor() {
  const N = _sliderItems.length;
  if (!N) return;
  const norm = ((_sliderIdx % N) + N) % N;
  if (norm !== _sliderIdx) {
    _sliderIdx = norm;
    _sliderPosition(false); // мгновенно, без анимации
  }
}

// ── Выбор реакции ────────────────────────────────────
function _sliderSelect() {
  const N      = _sliderItems.length;
  const normI  = ((_sliderIdx % N) + N) % N;
  const item   = _sliderItems[normI];
  if (!item) return;
  _closePicker();
  showReactionDisplay('my', item);

  if (item.type === 'custom') {
    const uid = App.user?.id;
    if (uid) fetch('/api/reaction/use', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: uid, itemId: item.id }),
    }).catch(() => {});
  }

  if (Game.mode === 'online') {
    WS.sendReaction(item.type === 'emoji' ? item.value : 'custom:' + item.id);
  } else if (item.type === 'emoji' && Math.random() < 0.5) {
    const b = DEFAULT_REACTIONS[Math.floor(Math.random() * DEFAULT_REACTIONS.length)];
    setTimeout(() => showReactionDisplay('opp', b), 900 + Math.random() * 1400);
  }
}

function _closePicker() {
  _reactionPickerOpen = false;
  document.getElementById('reaction-picker')?.classList.add('hidden');
  document.getElementById('reaction-trigger-btn')?.classList.remove('open');
}

// ── Инициализация ────────────────────────────────────
function initReactions() {
  const btn    = document.getElementById('reaction-trigger-btn');
  const picker = document.getElementById('reaction-picker');
  if (!btn || !picker) return;

  // Открыть / закрыть
  btn.addEventListener('click', e => {
    e.stopPropagation();
    _reactionPickerOpen = !_reactionPickerOpen;
    picker.classList.toggle('hidden', !_reactionPickerOpen);
    btn.classList.toggle('open', _reactionPickerOpen);
    if (_reactionPickerOpen) {
      _sliderDragX = 0;
      _sliderRender();
    }
  });

  document.addEventListener('click', e => {
    if (!_reactionPickerOpen) return;
    if (picker.contains(e.target) || btn.contains(e.target)) return;
    _closePicker();
  }, true);

  // ── Drag / Swipe ─────────────────────────────────────
  function dragStart(x) {
    if (_sliderBusy) return;
    _sliderIsDragging = true;
    _sliderWasDrag    = false;
    _sliderStartX     = x;
    _sliderDragX      = 0;
    const t = document.getElementById('rslider-track');
    if (t) t.style.transition = 'none';
  }

  function dragMove(x) {
    if (!_sliderIsDragging) return;
    _sliderDragX = x - _sliderStartX;
    if (Math.abs(_sliderDragX) > 5) _sliderWasDrag = true;
    _sliderPosition(false);
  }

  function dragEnd(x) {
    if (!_sliderIsDragging) return;
    _sliderIsDragging = false;
    const dx    = x - _sliderStartX;
    const steps = -Math.round(dx / ITEM_W);
    // Накапливаем — НЕ нормализуем, трек остаётся на месте
    _sliderIdx  += steps;
    _sliderDragX = 0;
    _sliderBusy  = true;
    _sliderPosition(true);
    // После анимации тихо реанкоримся в copy1 (пользователь не видит)
    setTimeout(() => {
      _sliderReanchor();
      _sliderBusy = false;
    }, 280);
  }

  // Mouse
  picker.addEventListener('mousedown', e => {
    if (!e.target.closest('#rslider-track')) return;
    e.preventDefault();
    dragStart(e.clientX);
  });
  window.addEventListener('mousemove', e => { if (_sliderIsDragging) dragMove(e.clientX); });
  window.addEventListener('mouseup',   e => { if (_sliderIsDragging) dragEnd(e.clientX); });

  // Touch — touchstart на треке, но move/end на window чтобы работало за пределами пикера
  picker.addEventListener('touchstart', e => {
    if (!e.target.closest('#rslider-track')) return;
    dragStart(e.touches[0].clientX);
  }, { passive: true });
  window.addEventListener('touchmove', e => {
    if (!_sliderIsDragging) return;
    e.preventDefault();
    dragMove(e.touches[0].clientX);
  }, { passive: false });
  window.addEventListener('touchend', e => {
    if (_sliderIsDragging) dragEnd(e.changedTouches[0].clientX);
  });

  // Клик: центральная — отправить, боковая — прокрутить
  picker.addEventListener('click', e => {
    if (_sliderWasDrag) { _sliderWasDrag = false; return; }
    const cell = e.target.closest('.rslider-cell');
    if (!cell) return;
    const clicked = Number(cell.dataset.i);
    const N       = _sliderItems.length;
    const activeI = ((_sliderIdx % N) + N) % N;
    if (clicked === activeI) {
      _sliderSelect();
    } else {
      // Кратчайший путь по кругу — накапливаем, не нормализуем
      let diff = clicked - activeI;
      if (diff >  N / 2) diff -= N;
      if (diff < -N / 2) diff += N;
      _sliderIdx  += diff;   // накопленный
      _sliderDragX = 0;
      _sliderBusy  = true;
      _sliderPosition(true);
      setTimeout(() => {
        _sliderReanchor();
        _sliderBusy = false;
      }, 280);
    }
  });

  _rebuildSliderItems();
}


// payload: { type: 'emoji', value } | { type: 'custom', id, filename }
function showReactionDisplay(side, payload) {
  const elId      = side === 'my' ? 'my-reaction-display' : 'opp-reaction-display';
  const container = document.getElementById(elId);
  if (!container) return;

  if (_reactionTimers[side]) {
    clearTimeout(_reactionTimers[side]);
    _reactionTimers[side] = null;
  }

  container.innerHTML = '';
  if (!payload) return;

  let el;
  if (payload.type === 'custom' && payload.filename) {
    el = document.createElement('video');
    el.className   = 'reaction-video-show';
    el.src         = '/reactions/' + payload.filename;
    el.autoplay    = true;
    el.loop        = true;
    el.muted       = true;
    el.playsInline = true;
  } else {
    el = document.createElement('span');
    el.className   = 'reaction-emoji-show';
    el.textContent = payload.value || String(payload);
  }

  container.appendChild(el);
  _reactionTimers[side] = setTimeout(() => {
    el.style.animation = 'reactionFadeOut .3s ease forwards';
    setTimeout(() => { if (container.contains(el)) container.innerHTML = ''; }, 300);
  }, 3000);
}
// Обновить прогресс-счётчик на лоадере
function setLoadingProgress(pct) {
  const el = document.getElementById('loading-progress');
  if (el) el.textContent = Math.round(pct) + '%';
}

// Показать лоадер поверх любого экрана (для возврата в меню после боя)
function showLoaderOverMenu(onDone) {
  const loader = document.getElementById('screen-loading');
  if (!loader) { onDone(); return; }
  // Сбрасываем прогресс и показываем
  setLoadingProgress(0);
  loader.style.display = '';
  loader.classList.add('active');

  let pct = 0;
  const tick = setInterval(() => {
    pct = Math.min(pct + Math.random() * 18 + 5, 90);
    setLoadingProgress(pct);
  }, 120);

  onDone(function hideLoader() {
    clearInterval(tick);
    setLoadingProgress(100);
    setTimeout(() => {
      loader.classList.remove('active');
      loader.style.display = 'none';
    }, 250);
  });
}

window.addEventListener('DOMContentLoaded', async () => {
  // Инициализируем всё что не требует сети
  initTelegram();
  initUser();
  initSettings();
  initStats();
  buildLabels();
  initHeroGrid();
  initSoundButton();
  initBurger();
  initVisibilityHandler();
  bindNav();
  initPromoBanner();
  updateBurgerSound();
  updateBurgerEnemyMoves();
  initSwipeBack();
  initOnlineCounter();
  initShop();
  initInventory();
  initReactions();
  loadCustomReactions();
  Notif.init();

  // WebSocket события магазина — вешаем когда сокет будет готов
  const _shopSocketInterval = setInterval(() => {
    if (!WS.socket) return;
    WS.socket.on('purchase_complete', onPurchaseComplete);
    WS.socket.on('item_revoked',      onItemRevoked);
    clearInterval(_shopSocketInterval);
  }, 500);

  // Анимируем прогресс лоадера пока грузим данные с сервера
  startLoadingCellAnim();
  setLoadingProgress(5);
  let pct = 5;
  const progressTick = setInterval(() => {
    pct = Math.min(pct + Math.random() * 12 + 3, 85);
    setLoadingProgress(pct);
  }, 150);

  // Грузим данные с сервера ДО показа меню
  await syncStatsFromServer().catch(() => {});
  // Загружаем инвентарь и применяем тему с сервера — единственный источник правды
  await loadShopData().catch(() => {});

  clearInterval(progressTick);
  setLoadingProgress(100);

  // Обновляем UI уже с актуальными данными — ВСЁ рендерим пока лоадер ещё виден
  updateMenuUI();
  // Пре-рендерим профиль и статистику за лоадером, чтобы не было мигания
  await renderProfileScreen().catch(() => {});
  await renderStatsScreen('online').catch(() => {});

  // Короткая пауза чтобы игрок увидел 100%
  await new Promise(r => setTimeout(r, 300));

  showScreen('menu');

  // Обработка ссылки-приглашения: /?room=<roomId> или TG startapp=room_<roomId>
  const params = new URLSearchParams(window.location.search);
  let room = params.get('room');

  // Telegram Mini App deep link: startapp параметр
  if (!room) {
    try {
      const startParam = window.Telegram?.WebApp?.initDataUnsafe?.start_param;
      if (startParam?.startsWith('room_')) room = startParam.slice(5);
    } catch(e) {}
  }

  if (room) {
    setTimeout(() => {
      showModal('Приглашение в игру', `Тебя пригласили! Подключиться?`, [
        { label: 'Подключиться', cls: 'btn-primary', action: () => { closeModal(); startOnline('friend_join:' + room); }},
        { label: 'Отмена',       cls: 'btn-ghost',   action: closeModal },
      ]);
    }, 400);
  }
});

/* ─── МАГАЗИН ────────────────────────────────────── */

const ITEM_TYPE_LABELS = {
  frame:    'Рамка',
  theme:    'Тема',
  reaction: 'Реакция',
  title:    'Звание',
};

let _shopItems     = [];   // весь каталог
let _shopInventory = {};   // { itemId: true } — что куплено
let _shopEquipped  = {};   // { slot: itemId } — что надето
let _shopFilter    = 'all';
let _currentShopItemId = null;
let _shopItemBackTarget = 'shop'; // откуда открыли страницу товара

// Загрузить каталог и инвентарь
async function loadShopData() {
  try {
    const uid = App.user?.id;
    const isGuest = !uid || uid.startsWith('guest_');

    const [itemsRes, invRes] = await Promise.all([
      fetch('/api/shop/items').then(r => r.json()),
      !isGuest
        ? authFetch(`/api/inventory/${uid}`).then(r => r.json())
        : Promise.resolve({ ok: true, data: { items: [], equipped: {} } }),
    ]);
    if (itemsRes.ok)  _shopItems = itemsRes.data || [];
    if (invRes.ok) {
      _shopInventory = {};
      (invRes.data.items || []).forEach(i => { _shopInventory[i.item_id] = true; });
      _shopEquipped  = invRes.data.equipped || {};
      // НЕ трогаем localStorage здесь — он управляется только через кнопки Применить/Снять
      // Но если сервер говорит что тема экипирована — применяем визуально если ещё не применена
      const serverTheme = _shopEquipped['theme'] || null;
      // Сервер — единственный источник правды для темы
      // Применяем то что сказал сервер, localStorage больше не используем
      resetTheme();
      if (serverTheme) applyEquippedTheme(serverTheme);
    }
  } catch(e) {
    console.error('[Shop] loadShopData error:', e);
  }
}

// Отрисовать карточки с учётом фильтра
function renderShopGrid() {
  const grid = document.getElementById('shop-grid');
  if (!grid) return;

  const items = _shopFilter === 'all'
    ? _shopItems
    : _shopItems.filter(i => i.type === _shopFilter);

  if (!items.length) {
    grid.innerHTML = '<div class="shop-loading">Нет товаров</div>';
    return;
  }

  grid.innerHTML = items.map(item => {
    const owned    = !!_shopInventory[item.id];
    const equipped = Object.values(_shopEquipped).includes(item.id);
    const cls      = equipped ? 'shop-card equipped' : owned ? 'shop-card owned' : 'shop-card';
    const priceHtml = equipped
      ? '<span class="shop-card-price owned">✓ Куплено</span>'
      : owned
        ? '<span class="shop-card-price owned">✓ Куплено</span>'
        : item.price_stars
          ? `<span class="shop-card-price">⭐ ${item.price_stars}</span>`
          : '<span class="shop-card-price owned">Бесплатно</span>';
    const preview = item.preview_url
      ? `<img src="${item.preview_url}" alt="${item.name}" loading="lazy">`
      : '🎁';

    return `<div class="${cls}" data-item-id="${item.id}">
      <div class="shop-card-preview">${preview}</div>
      <div class="shop-card-body">
        <div class="shop-card-type">${ITEM_TYPE_LABELS[item.type] || item.type}</div>
        <div class="shop-card-name">${item.name}</div>
        ${priceHtml}
      </div>
    </div>`;
  }).join('');

  // Клик по карточке — открыть страницу товара
  grid.querySelectorAll('.shop-card').forEach(card => {
    card.addEventListener('click', () => openShopItem(card.dataset.itemId));
  });
}

// Открыть страницу товара
function openShopItem(itemId) {
  const item = _shopItems.find(i => i.id === itemId);
  if (!item) return;
  _currentShopItemId = itemId;

  const owned    = !!_shopInventory[itemId];
  const equipped = Object.values(_shopEquipped).includes(itemId);

  document.getElementById('shop-item-title').textContent = item.name;
  document.getElementById('shop-item-name').textContent  = item.name;
  document.getElementById('shop-item-desc').textContent  = item.description || '';
  document.getElementById('shop-item-type-badge').textContent = ITEM_TYPE_LABELS[item.type] || item.type;

  // Превью — для реакций WebM
  const previewEl = document.getElementById('shop-item-preview-lg');
  previewEl.innerHTML = getItemPreviewHtml(item, true);

  // Цена и статус
  const priceEl  = document.getElementById('shop-item-price');
  const statusEl = document.getElementById('shop-item-status');
  const btnEl    = document.getElementById('shop-item-btn');

  // Реакции не надеваются — просто "В наличии" или "Купить"
  if (item.type === 'reaction') {
    if (owned) {
      priceEl.textContent  = '';
      statusEl.textContent = '✓ В наличии';
      btnEl.textContent    = 'Использовать в бою';
      btnEl.className      = 'btn btn-secondary btn-large';
      btnEl.disabled       = true;
    } else if (item.price_stars) {
      priceEl.textContent  = `⭐ ${item.price_stars}`;
      statusEl.textContent = '';
      btnEl.textContent    = 'Купить';
      btnEl.className      = 'btn btn-primary btn-large';
      btnEl.disabled       = false;
    }
  } else if (equipped) {
    priceEl.textContent  = '';
    statusEl.textContent = '✓ Надето';
    btnEl.textContent    = 'Снять';
    btnEl.className      = 'btn btn-secondary btn-large';
    btnEl.disabled       = false;
  } else if (owned) {
    priceEl.textContent  = '';
    statusEl.textContent = '✓ Куплено';
    btnEl.textContent    = 'Надеть';
    btnEl.className      = 'btn btn-primary btn-large';
    btnEl.disabled       = false;
  } else if (item.price_stars) {
    priceEl.textContent  = `⭐ ${item.price_stars}`;
    statusEl.textContent = '';
    btnEl.textContent    = 'Купить';
    btnEl.className      = 'btn btn-primary btn-large';
    btnEl.disabled       = false;
  } else {
    priceEl.textContent  = 'Бесплатно';
    statusEl.textContent = '';
    btnEl.textContent    = 'Получить';
    btnEl.className      = 'btn btn-primary btn-large';
    btnEl.disabled       = false;
  }

  // Кнопка назад возвращает туда откуда пришли
  _shopItemBackTarget = currentScreen === 'profile' ? 'profile' : 'shop';
  document.getElementById('shop-item-back').onclick = () => showScreen(_shopItemBackTarget, { isBack: true });

  showScreen('shop-item');
}

// Действие кнопки на странице товара
async function handleShopItemBtn() {
  const itemId = _currentShopItemId;
  const item   = _shopItems.find(i => i.id === itemId);
  if (!item) return;

  // Гость — предлагаем войти через TG
  if (!App.user?.id || App.user?.id?.startsWith('guest_')) {
    alert('Для покупки необходимо войти через Telegram');
    return;
  }

  const owned    = !!_shopInventory[itemId];
  const equipped = Object.values(_shopEquipped).includes(itemId);

  // Реакции не надеваются — только покупаются
  if (item.type === 'reaction') {
    if (owned) return; // кнопка задизейблена, но на всякий случай
    // Иначе — купить (падает ниже к логике покупки)
  } else if (equipped) {
    await fetch('/api/unequip', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: App.user?.id, slot: item.type }),
    });
    delete _shopEquipped[item.type];
    if (item.type === 'theme') {
      resetTheme();
      showLoaderOverMenu(hide => {
        setTimeout(() => { hide && hide(); showScreen('menu'); renderInventory(); renderShopGrid(); }, 400);
      });
    }
    openShopItem(itemId);
    renderShopGrid();
    return;
  } else if (owned) {
    await fetch('/api/equip', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: App.user?.id, itemId }),
    });
    _shopEquipped[item.type] = itemId;
    if (item.type === 'theme') {
      showLoaderOverMenu(hide => {
        resetTheme();
        applyEquippedTheme(itemId);
        setTimeout(() => { hide && hide(); showScreen('menu'); renderInventory(); renderShopGrid(); }, 400);
      });
    }
    openShopItem(itemId);
    renderShopGrid();
    return;
  }

  // Купить — создаём invoice
  try {
    const btnEl = document.getElementById('shop-item-btn');
    if (btnEl) { btnEl.textContent = 'Загрузка...'; btnEl.disabled = true; }

    const res = await fetch('/api/shop/buy', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: App.user?.id, itemId }),
    }).then(r => r.json());

    if (btnEl) { btnEl.textContent = 'Купить'; btnEl.disabled = false; }

    if (res.ok && res.free) {
      // Админ — выдано бесплатно
      _shopInventory[itemId] = true;
      openShopItem(itemId);
      renderShopGrid();
      return;
    }

    if (res.ok && res.invoiceUrl) {
      if (window.Telegram?.WebApp?.openInvoice) {
        // Внутри Telegram Mini App
        window.Telegram.WebApp.openInvoice(res.invoiceUrl, status => {
          if (status === 'paid') {
            _shopInventory[itemId] = true;
            openShopItem(itemId);
            renderShopGrid();
          }
        });
      } else {
        // Фолбэк для браузера — открываем ссылку
        window.open(res.invoiceUrl, '_blank');
      }
    } else {
      console.error('[Shop] buy error:', res);
      alert('Ошибка: ' + (res.error || 'неизвестная ошибка'));
    }
  } catch(e) {
    console.error('[Shop] buy exception:', e);
    const btnEl = document.getElementById('shop-item-btn');
    if (btnEl) { btnEl.textContent = 'Купить'; btnEl.disabled = false; }
  }
}

// WebSocket — товар куплен (пришло с сервера)
function onPurchaseComplete(data) {
  _shopInventory[data.itemId] = true;
  renderShopGrid();
}

// WebSocket — товар отозван (рефанд)
function onItemRevoked(data) {
  delete _shopInventory[data.itemId];
  const slot = Object.keys(_shopEquipped).find(k => _shopEquipped[k] === data.itemId);
  if (slot) delete _shopEquipped[slot];
  renderShopGrid();
}

// Инициализация магазина
function initShop() {
  // Кнопка Магазин в главном меню — загружаем данные при входе
  document.getElementById('btn-shop')?.addEventListener('click', async () => {
    // Если товары уже загружены — просто рендерим, не перезагружаем инвентарь
    if (!_shopItems.length) {
      document.getElementById('shop-grid').innerHTML = '<div class="shop-loading">Загрузка...</div>';
      await loadShopData();
    }
    renderShopGrid();
  });

  // Фильтры
  document.querySelectorAll('.shop-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.shop-filter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _shopFilter = btn.dataset.filter;
      renderShopGrid();
    });
  });

  // Кнопка действия на странице товара
  document.getElementById('shop-item-btn')?.addEventListener('click', handleShopItemBtn);
}



/* ─── ФЕЙКОВЫЕ ПРЕВЬЮ (временные SVG) ───────────────────────────────────── */
// Временные превью — используется пока нет реальных файлов
const FAKE_PREVIEWS = {
  theme_dark: `<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
    <rect width="100" height="100" rx="12" fill="#111111"/>
    <rect x="8" y="8" width="84" height="84" rx="10" fill="#1e1e1e"/>
    <rect x="20" y="30" width="60" height="8" rx="4" fill="#333"/>
    <rect x="20" y="46" width="42" height="8" rx="4" fill="#333"/>
    <rect x="20" y="62" width="52" height="8" rx="4" fill="#333"/>
    <circle cx="75" cy="30" r="10" fill="#e0e0e0" opacity=".15"/>
  </svg>`,
};

// Патчим getPreviewHtml — SVG для страницы товара (large=true), PNG для карточки
function getItemPreviewHtml(item, large = false) {
  // Реакция — WebM видео
  if (item.type === 'reaction' && item.preview_url) {
    const cls = large ? 'shop-reaction-preview-lg' : 'shop-reaction-preview';
    return `<video class="${cls}" src="${item.preview_url}" autoplay loop muted playsinline></video>`;
  }

  if (large) {
    // На странице товара — SVG если есть
    const svgUrl = item.preview_url ? item.preview_url.replace(/\.(png|jpg)$/i, '.svg') : null;
    if (svgUrl && !/\.webm$/i.test(item.preview_url)) {
      return `<img src="${svgUrl}" alt="${item.name}" loading="lazy" class="shop-preview-lg-img">`;
    }
    if (FAKE_PREVIEWS[item.id]) {
      return `<div class="shop-preview-lg-svg">${FAKE_PREVIEWS[item.id]}</div>`;
    }
    return '🎁';
  }
  // В карточке сетки
  if (item.preview_url) {
    return `<img src="${item.preview_url}" alt="${item.name}" loading="lazy" class="shop-card-img">`;
  }
  if (FAKE_PREVIEWS[item.id]) {
    return `<div class="shop-card-svg">${FAKE_PREVIEWS[item.id]}</div>`;
  }
  return '🎁';
}

/* ─── ИНВЕНТАРЬ В ПРОФИЛЕ ────────────────────────── */
let _invFilter = 'all';

function renderInventory() {
  const grid     = document.getElementById('inventory-grid');
  const emptyEl  = document.getElementById('inventory-empty');
  if (!grid) return;

  // Тёмная тема всегда в инвентаре для авторизованных
  const darkTheme = { id: 'theme_dark', type: 'theme', name: 'Тёмная тема (по умолчанию)', description: 'Стандартная тёмная цветовая схема', preview_url: '/shop/previews/theme/frame_theme_dark.png' };
  const ownedIds  = new Set(Object.keys(_shopInventory));
  const owned     = [darkTheme, ..._shopItems.filter(i => ownedIds.has(i.id))];

  if (owned.length <= 1 && !ownedIds.size) {
    grid.innerHTML = '';
    emptyEl?.classList.remove('hidden');
    return;
  }
  emptyEl?.classList.add('hidden');

  const filtered = _invFilter === 'all' ? owned : owned.filter(i => i.type === _invFilter);

  if (!filtered.length) {
    grid.innerHTML = '<div class="shop-loading">Нет предметов этой категории</div>';
    return;
  }

  grid.innerHTML = filtered.map(item => {
    // Реакции — всегда активны, не надеваются
    if (item.type === 'reaction') {
      return `<div class="shop-card owned" data-inv-item="${item.id}">
        <div class="shop-card-preview">${getItemPreviewHtml(item)}</div>
        <div class="shop-card-body">
          <div class="shop-card-type">${ITEM_TYPE_LABELS[item.type] || item.type}</div>
          <div class="shop-card-name">${item.name}</div>
          <button class="shop-card-action applied">В наличии</button>
        </div>
      </div>`;
    }
    // Для всех предметов включая темы — источник правды _shopEquipped (с сервера)
    let equipped;
    if (item.type === 'theme') {
      const activeTheme = _shopEquipped['theme'] || null;
      equipped = item.id === 'theme_dark' ? !activeTheme : activeTheme === item.id;
    } else {
      equipped = Object.values(_shopEquipped).includes(item.id);
    }
    const cls = equipped ? 'shop-card equipped' : 'shop-card owned';
    return `<div class="${cls}" data-inv-item="${item.id}">
      <div class="shop-card-preview">${getItemPreviewHtml(item)}</div>
      <div class="shop-card-body">
        <div class="shop-card-type">${ITEM_TYPE_LABELS[item.type] || item.type}</div>
        <div class="shop-card-name">${item.name}</div>
        ${equipped ? '<button class="shop-card-action applied">Применено</button>' : `<button class="shop-card-action" data-inv-action="${item.id}">Применить</button>`}
      </div>
    </div>`;
  }).join('');

  // Клик по карточке — открыть страницу товара
  grid.querySelectorAll('[data-inv-item]').forEach(card => {
    card.addEventListener('click', e => {
      if (e.target.closest('[data-inv-action]')) return;
      const id = card.dataset.invItem;
      (_shopItems.length ? Promise.resolve() : loadShopData()).then(() => {
        // theme_dark — виртуальный, добавляем в _shopItems если нет
        if (id === 'theme_dark' && !_shopItems.find(i => i.id === 'theme_dark')) {
          _shopItems.unshift({ id: 'theme_dark', type: 'theme', name: 'Тёмная тема (по умолчанию)', description: 'Стандартная тёмная цветовая схема', preview_url: '/shop/previews/theme/frame_theme_dark.png' });
        }
        _shopItemBackTarget = 'profile';
        showScreen('shop-item');
        openShopItem(id);
      });
    });
  });

  // Клики по кнопкам
  grid.querySelectorAll('[data-inv-action]').forEach(btn => {
    const itemId = btn.dataset.invAction;
    // theme_dark — виртуальный, не из _shopItems
    const item = itemId === 'theme_dark'
      ? { id: 'theme_dark', type: 'theme', name: 'Тёмная тема (по умолчанию)', description: 'Стандартная тёмная цветовая схема', preview_url: '/shop/previews/theme/frame_theme_dark.png' }
      : _shopItems.find(i => i.id === itemId);
    if (!item) return;
    const activeTheme = _shopEquipped['theme'] || null;
    const equipped = item.type === 'theme'
      ? (item.id === 'theme_dark' ? !activeTheme : activeTheme === itemId)
      : Object.values(_shopEquipped).includes(itemId);
    if (equipped) return; // уже применено — не кликается
    btn.addEventListener('click', async e => {
      e.stopPropagation();
      if (item.type === 'theme') {
        // Сбросить текущую тему (установить тёмную) или применить новую
        if (itemId === 'theme_dark') {
          if (_shopEquipped['theme']) {
            await fetch('/api/unequip', {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ userId: App.user?.id, slot: 'theme' }),
            });
            delete _shopEquipped['theme'];
          }
          showLoaderOverMenu(hide => {
            resetTheme();
            setTimeout(() => { hide && hide(); showScreen('menu'); renderInventory(); renderShopGrid(); }, 400);
          });
        } else {
          await fetch('/api/equip', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ userId: App.user?.id, itemId }),
          });
          _shopEquipped[item.type] = itemId;
          showLoaderOverMenu(hide => {
            resetTheme();
            applyEquippedTheme(itemId);
            setTimeout(() => { hide && hide(); showScreen('menu'); renderInventory(); renderShopGrid(); }, 400);
          });
        }
        return;
      }
      await fetch('/api/equip', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: App.user?.id, itemId }),
      });
      _shopEquipped[item.type] = itemId;
      renderInventory();
      renderShopGrid();
    });
  });
}

function initInventory() {
  // Фильтры
  document.querySelectorAll('[data-inv-filter]').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('[data-inv-filter]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _invFilter = btn.dataset.invFilter;
      renderInventory();
    });
  });

  // Ссылка "перейти в магазин"
  document.getElementById('inventory-go-shop')?.addEventListener('click', e => {
    e.preventDefault();
    showScreen('shop');
  });
}

// Патчим renderShopGrid — используем getItemPreviewHtml
const _origRenderShopGrid = renderShopGrid;
renderShopGrid = function() {
  const grid = document.getElementById('shop-grid');
  if (!grid) return;

  const items = _shopFilter === 'all'
    ? _shopItems
    : _shopItems.filter(i => i.type === _shopFilter);

  if (!items.length) {
    grid.innerHTML = '<div class="shop-loading">Нет товаров</div>';
    return;
  }

  grid.innerHTML = items.map(item => {
    const owned    = !!_shopInventory[item.id];
    const equipped = Object.values(_shopEquipped).includes(item.id);
    const cls      = equipped ? 'shop-card equipped' : owned ? 'shop-card owned' : 'shop-card';
    const priceHtml = equipped
      ? '<span class="shop-card-price owned">✓ Куплено</span>'
      : owned
        ? '<span class="shop-card-price owned">✓ Куплено</span>'
        : item.price_stars
          ? `<span class="shop-card-price">⭐ ${item.price_stars}</span>`
          : '<span class="shop-card-price owned">Бесплатно</span>';

    return `<div class="${cls}" data-item-id="${item.id}">
      <div class="shop-card-preview">${getItemPreviewHtml(item)}</div>
      <div class="shop-card-body">
        <div class="shop-card-type">${ITEM_TYPE_LABELS[item.type] || item.type}</div>
        <div class="shop-card-name">${item.name}</div>
        ${priceHtml}
      </div>
    </div>`;
  }).join('');

  grid.querySelectorAll('.shop-card').forEach(card => {
    card.addEventListener('click', () => openShopItem(card.dataset.itemId));
  });
};

// Патчим openShopItem — используем getItemPreviewHtml
const _origOpenShopItem = openShopItem;
openShopItem = function(itemId) {
  // theme_dark — виртуальный товар, не в _shopItems
  let item = _shopItems.find(i => i.id === itemId);
  if (!item && itemId === 'theme_dark') {
    item = { id: 'theme_dark', type: 'theme', name: 'Тёмная тема (по умолчанию)', description: 'Стандартная тёмная цветовая схема', preview_url: '/shop/previews/theme/frame_theme_dark.png' };
  }
  if (!item) return;
  _currentShopItemId = itemId;

  const owned    = !!_shopInventory[itemId];
  const equipped = Object.values(_shopEquipped).includes(itemId);

  document.getElementById('shop-item-title').textContent = item.name;
  document.getElementById('shop-item-name').textContent  = item.name;
  document.getElementById('shop-item-desc').textContent  = item.description || '';
  document.getElementById('shop-item-type-badge').textContent = ITEM_TYPE_LABELS[item.type] || item.type;

  const previewEl = document.getElementById('shop-item-preview-lg');
  previewEl.innerHTML = getItemPreviewHtml(item, true);

  const priceEl  = document.getElementById('shop-item-price');
  const statusEl = document.getElementById('shop-item-status');
  const btnEl    = document.getElementById('shop-item-btn');

  // Реакции — не надеваются
  if (item.type === 'reaction') {
    if (owned) {
      priceEl.textContent  = '';
      statusEl.textContent = '✓ В наличии';
      btnEl.textContent    = 'Используется в бою автоматически';
      btnEl.className      = 'btn btn-secondary btn-large';
      btnEl.disabled       = true;
    } else if (item.price_stars) {
      priceEl.textContent  = `⭐ ${item.price_stars}`;
      statusEl.textContent = '';
      btnEl.textContent    = 'Купить';
      btnEl.className      = 'btn btn-primary btn-large';
      btnEl.disabled       = false;
    } else {
      priceEl.textContent  = 'Бесплатно';
      statusEl.textContent = '';
      btnEl.textContent    = 'Получить';
      btnEl.className      = 'btn btn-primary btn-large';
      btnEl.disabled       = false;
    }
  } else if (equipped) {
    priceEl.textContent  = '';
    statusEl.textContent = '✓ Применено';
    btnEl.textContent    = 'Снять';
    btnEl.className      = 'btn btn-secondary btn-large';
    btnEl.disabled       = false;
  } else if (owned || itemId === 'theme_dark') {
    priceEl.textContent  = '';
    statusEl.textContent = '✓ Куплено';
    btnEl.textContent    = 'Применить';
    btnEl.className      = 'btn btn-primary btn-large';
    btnEl.disabled       = false;
  } else if (item.price_stars) {
    priceEl.textContent  = `⭐ ${item.price_stars}`;
    statusEl.textContent = '';
    btnEl.textContent    = 'Купить';
    btnEl.className      = 'btn btn-primary btn-large';
    btnEl.disabled       = false;
  } else {
    priceEl.textContent  = 'Бесплатно';
    statusEl.textContent = '';
    btnEl.textContent    = 'Получить';
    btnEl.className      = 'btn btn-primary btn-large';
    btnEl.disabled       = false;
  }

  // ── Предпросмотр темы ──────────────────────────────────────────────────
  // Если это страница темы — применяем её временно для предпросмотра
  if (item.type === 'theme') {
    resetTheme();
    if (itemId === 'theme_dark') {
      // тёмная — это дефолт, просто сбрасываем
    } else {
      applyEquippedTheme(itemId);
    }
    // При уходе со страницы — восстанавливаем реальную тему
    const restoreTheme = () => applyEquippedThemeFromState();
    document.getElementById('shop-item-back').onclick = () => {
      restoreTheme();
      showScreen(_shopItemBackTarget, { isBack: true });
    };
  } else {
    document.getElementById('shop-item-back').onclick = () => showScreen(_shopItemBackTarget, { isBack: true });
  }

  showScreen('shop-item');

  // Слайдер — после showScreen чтобы DOM был виден
  requestAnimationFrame(() => renderShopItemSlider(item));
};


/* ─── СЛАЙДЕР СКРИНШОТОВ ТОВАРА ──────────────────── */

const ITEM_SCREENSHOTS = {
  theme_dark:  [
    '/shop/previews/theme/preview_dark_1.png',
    '/shop/previews/theme/preview_dark_2.png',
    '/shop/previews/theme/preview_dark_3.png',
  ],
  theme_light: [
    '/shop/previews/theme/preview_white_1.png',
    '/shop/previews/theme/preview_white_2.png',
    '/shop/previews/theme/preview_white_3.png',
  ],
  theme_black: [
    '/shop/previews/theme/preview_black_1.png',
    '/shop/previews/theme/preview_black_2.png',
    '/shop/previews/theme/preview_black_3.png',
  ],
};

function renderShopItemSlider(item) {
  const old = document.getElementById('shop-item-slider');
  if (old) old.remove();

  const slides = ITEM_SCREENSHOTS[item.id];
  if (!slides || !slides.length) return;

  const detail = document.querySelector('#screen-shop-item .shop-item-detail');
  if (!detail) return;

  const slider = document.createElement('div');
  slider.id = 'shop-item-slider';
  slider.className = 'shop-item-slider';

  const track = document.createElement('div');
  track.className = 'shop-item-slider-track';
  slider.appendChild(track);

  const dotsWrap = document.createElement('div');
  dotsWrap.className = 'shop-item-slider-dots';
  slider.appendChild(dotsWrap);

  detail.appendChild(slider);

  let cur = 0;
  let loaded = 0;
  let errors = 0;
  const total = slides.length;
  const slideEls = new Array(total).fill(null);

  function getCount() { return slideEls.filter(Boolean).length; }

  function goTo(idx, animate = true) {
    const count = getCount();
    if (count === 0) return;
    cur = ((idx % count) + count) % count;
    track.style.transition = animate ? 'transform .32s cubic-bezier(.4,0,.2,1)' : 'none';
    // Смещаем на ширину одного слайда = ширина контейнера
    const slideW = slider.offsetWidth || window.innerWidth;
    track.style.transform = `translateX(${-cur * slideW}px)`;
    dotsWrap.querySelectorAll('.slider-dot').forEach((d, i) => d.classList.toggle('active', i === cur));
  }

  function buildDots() {
    const count = getCount();
    if (count <= 1) { dotsWrap.innerHTML = ''; return; }
    dotsWrap.innerHTML = '';
    for (let i = 0; i < count; i++) {
      const d = document.createElement('span');
      d.className = 'slider-dot' + (i === 0 ? ' active' : '');
      const idx = i;
      d.addEventListener('click', () => goTo(idx));
      dotsWrap.appendChild(d);
    }
  }

  function onAllSettled() {
    buildDots();
    goTo(0, false);
    // Обновляем позицию при ресайзе (поворот экрана)
    window.addEventListener('resize', () => goTo(cur, false), { passive: true });
  }

  slides.forEach((src, i) => {
    const img = new Image();
    img.onload = () => {
      loaded++;
      const slide = document.createElement('div');
      slide.className = 'shop-item-slide';
      slide.appendChild(img);
      slideEls[i] = slide;

      // Вставляем сразу в правильную позицию
      let ref = null;
      for (let j = i + 1; j < total; j++) {
        if (slideEls[j] && track.contains(slideEls[j])) { ref = slideEls[j]; break; }
      }
      track.insertBefore(slide, ref);

      if (loaded + errors === total) onAllSettled();
    };
    img.onerror = () => {
      errors++;
      if (loaded + errors === total) {
        if (getCount() === 0) { slider.remove(); return; }
        onAllSettled();
      }
    };
    img.src = src;
  });

  // Touch swipe
  let startX = 0, startY = 0, dragging = false, lastDx = 0;

  track.addEventListener('touchstart', e => {
    startX = e.touches[0].clientX;
    startY = e.touches[0].clientY;
    dragging = false;
    lastDx = 0;
    track.style.transition = 'none';
  }, { passive: true });

  track.addEventListener('touchmove', e => {
    const dx = e.touches[0].clientX - startX;
    const dy = e.touches[0].clientY - startY;
    if (!dragging) {
      if (Math.abs(dy) > Math.abs(dx)) return; // вертикальный скролл
      if (Math.abs(dx) > 8) dragging = true;
    }
    if (!dragging) return;
    lastDx = dx;
    const slideW = slider.offsetWidth || window.innerWidth;
    track.style.transform = `translateX(${-cur * slideW + dx}px)`;
  }, { passive: true });

  track.addEventListener('touchend', () => {
    if (!dragging) return;
    dragging = false;
    if (Math.abs(lastDx) > 40) {
      goTo(lastDx < 0 ? cur + 1 : cur - 1);
    } else {
      goTo(cur);
    }
  }, { passive: true });
}

/* ─── ПРИМЕНЕНИЕ ТЕМ ─────────────────────────────── */

// Применяет купленную тему по item_id
function applyEquippedTheme(itemId) {
  if (itemId === 'theme_light') {
    document.body.classList.add('theme-light');
  } else if (itemId === 'theme_black') {
    document.body.classList.add('theme_black');
  }
  // Сюда добавлять новые темы по мере появления:
  // else if (itemId === 'theme_ocean') { ... }
}

// Снимает все темы (возврат к тёмной по умолчанию)
function resetTheme() {
  document.body.classList.remove('theme-light', 'theme_black');
}

// Применяет тему из текущей экипировки игрока
function applyEquippedThemeFromState() {
  const equippedThemeId = _shopEquipped['theme'];
  resetTheme();
  if (equippedThemeId) applyEquippedTheme(equippedThemeId);
}




