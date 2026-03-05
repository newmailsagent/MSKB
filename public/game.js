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

function showScreen(name, opts = {}) {
  const isBack = opts.isBack || false;
  const prev   = document.getElementById('screen-' + currentScreen);
  const next   = document.getElementById('screen-' + name);
  if (!next || currentScreen === name) return;

  // Лоадер убираем насовсем через display:none
  const loader = document.getElementById('screen-loading');
  if (loader && currentScreen === 'loading') {
    loader.style.display = 'none';
  }

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
    // Всегда целое число — TG иногда отдаёт float (364966070.0)
    const cleanId = String(parseInt(tgUser.id, 10));
    App.user = {
      id:       cleanId,
      name:     tgUser.first_name || 'Игрок',
      username: tgUser.username ? '@' + tgUser.username : '',
      photo:    tgUser.photo_url || null,
      isGuest:  false,
    };
  } else {
    // Не в TG — всегда гость
    const saved = loadJSON('bs_user', null);
    if (saved?.isGuest) {
      App.user = saved; // сохраняем guest_id
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
    showModal('Сбросить статистику?', 'Все данные будут удалены.', [
      { label: 'Отмена',   cls: 'btn-ghost',  action: closeModal },
      { label: 'Сбросить', cls: 'btn-danger', action: () => { App.stats = defaultStats(); App.history = []; saveJSON('bs_stats', App.stats); saveJSON('bs_history', App.history); updateMenuStats(); closeModal(); }},
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
      // Показываем только онлайн-статистику (online_wins/losses)
      App.stats.wins       = statsJson.data.online_wins   || 0;
      App.stats.losses     = statsJson.data.online_losses || 0;
      App.stats.totalShots = statsJson.data.online_shots  || 0;
      App.stats.totalHits  = statsJson.data.online_hits   || 0;
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
  // Показываем только тех у кого есть хотя бы 1 победа
  const filtered = data.filter(e => (e.rated_wins || 0) >= 1);
  if (!filtered.length) { list.innerHTML = '<p class="empty-state">Пока никто не набрал побед. Сыграй сетевой бой!</p>'; return; }
  filtered.forEach((entry, i) => {
    const rw  = entry.rated_wins   || 0;
    const rl  = entry.rated_losses || 0;
    const rs  = entry.rated_shots  || 0;
    const rh  = entry.rated_hits   || 0;
    const acc = rs > 0 ? Math.round(rh/rs*100) : 0;
    const tot = rw + rl;
    const wr  = tot ? Math.round(rw/tot*100) : 0;
    const isMe = entry.id === App.user?.id;
    const div  = document.createElement('div');
    div.className = 'lb-item' + (isMe ? ' lb-item-me' : '');
    div.innerHTML =
      '<div class="lb-rank ' + (medals[i]||'') + '">' + (i < 3 ? ['🥇','🥈','🥉'][i] : i+1) + '</div>' +
      '<div class="lb-avatar">' + (entry.name||'?')[0].toUpperCase() + '</div>' +
      '<div class="lb-info"><strong>' + (entry.name||'Игрок') + (isMe ? ' <small>(вы)</small>' : '') + '</strong>' +
      '<small>' + rw + 'W · ' + wr + '% WR</small></div>' +
      '<div class="lb-wins">' + rw + '</div>';
    list.appendChild(div);
  });
}

async function renderStatsScreen(mode) {
  const isGuest = !!App.user.isGuest;
  updatePromoBanner();
  if (!mode) mode = App._statsMode || 'online';
  App._statsMode = mode;

  const statsGrid    = document.querySelector('.stats-grid');
  const statsProfile = document.querySelector('.stats-profile');
  const sectionTitle = document.querySelector('.section-title');
  const historyList  = document.getElementById('history-list');

  if (isGuest) {
    if (statsGrid)    statsGrid.style.display    = 'none';
    if (statsProfile) statsProfile.style.display = 'none';
    if (sectionTitle) sectionTitle.style.display = 'none';
    if (historyList)  historyList.innerHTML = '';
    return;
  }

  if (statsGrid)    statsGrid.style.display    = '';
  if (statsProfile) statsProfile.style.display = '';
  if (sectionTitle) sectionTitle.style.display = '';

  // Аватар
  const statsAvatar = document.getElementById('stats-avatar');
  if (statsAvatar) {
    if (App.user.photo) {
      statsAvatar.innerHTML = `<img src="${App.user.photo}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">`;
    } else {
      statsAvatar.textContent = (App.user.name[0]||'?').toUpperCase();
    }
  }
  setText('stats-name', App.user.name);

  // Тумблер режима
  let toggle = document.getElementById('stats-mode-toggle');
  if (!toggle) {
    const header = document.querySelector('#screen-stats .page-header');
    if (header) {
      toggle = document.createElement('div');
      toggle.id = 'stats-mode-toggle';
      toggle.className = 'stats-mode-toggle';
      toggle.innerHTML =
        '<button class="stm-btn" data-mode="online">По сети</button>' +
        '<button class="stm-btn" data-mode="bot">Против ботов</button>';
      header.appendChild(toggle);
      toggle.addEventListener('click', e => {
        const btn = e.target.closest('[data-mode]');
        if (btn) renderStatsScreen(btn.dataset.mode);
      });
    }
  }
  // Подсвечиваем активный режим
  document.querySelectorAll('.stm-btn').forEach(b => b.classList.toggle('active', b.dataset.mode === mode));

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
  const cells = [];
  for (let i = 0; i < 100; i++) {
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

      // Ghost не нужен — ориентир показывается preview на поле
      this._drag.ghost = null;

      // renderBoard — убираем корабль с поля визуально
      this.renderBoard();
      // renderDock — НЕ вызываем во время drag, чтобы корабль не появлялся в доке
    }

    if (this._drag.moved) {
      this._moveGhost(e.clientX, e.clientY);
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
    const g = document.createElement('div');
    g.className = 'ship-piece' + (v ? ' vertical' : '');
    g.style.cssText = 'position:fixed;z-index:9999;pointer-events:none;opacity:0.7;' +
      'transform:translate(-50%,-50%);border:2px solid var(--accent);' +
      'background:var(--bg2);border-radius:5px;padding:4px;touch-action:none;';
    for (let i = 0; i < ship.size; i++) {
      const c = document.createElement('div'); c.className = 'ship-cell'; g.appendChild(c);
    }
    return g;
  },

  _moveGhost(cx, cy) {
    if (!this._drag?.ghost) return;
    this._drag.ghost.style.left = cx + 'px';
    this._drag.ghost.style.top  = cy + 'px';
  },

  _showPreview(cx, cy) {
    this.clearPreview();
    if (!this.selected) return;
    const el = document.elementFromPoint(cx, cy);
    if (!el) return;
    const cell = el.closest('[data-r][data-c]');
    if (!cell || !document.getElementById('placement-board')?.contains(cell)) return;
    let r = +cell.dataset.r, c = +cell.dataset.c;
    const size = this.selected.size;
    if (this.vertical) r = Math.min(r, BOARD_SIZE - size);
    else               c = Math.min(c, BOARD_SIZE - size);
    r = Math.max(0, r); c = Math.max(0, c);
    const valid = canPlace(this.board, r, c, size, this.vertical);
    for (let i = 0; i < size; i++) {
      const nr = this.vertical ? r+i : r, nc = this.vertical ? c : c+i;
      if (!inBounds(nr, nc)) continue;
      const cl = document.querySelector(`#placement-board [data-r="${nr}"][data-c="${nc}"]`);
      if (cl) cl.classList.add(valid ? 'preview' : 'invalid');
    }
  },

  _dropAt(cx, cy, ship) {
    const el = document.elementFromPoint(cx, cy);
    const cell = el?.closest('[data-r][data-c]');
    const boardEl = document.getElementById('placement-board');
    if (cell && boardEl?.contains(cell)) {
      let r = +cell.dataset.r, c = +cell.dataset.c;
      const size = ship.size;
      if (this.vertical) r = Math.min(r, BOARD_SIZE - size);
      else               c = Math.min(c, BOARD_SIZE - size);
      r = Math.max(0, r); c = Math.max(0, c);
      if (canPlace(this.board, r, c, size, this.vertical)) {
        ship.vertical = this.vertical;
        ship.cells = placeShip(this.board, r, c, size, this.vertical);
        ship.placed = true;
        this.selected = null;
        Sound.place(); vibrate([15]); return;
      }
    }
    // Промах — в dok
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
    document.querySelectorAll('#placement-board .preview, #placement-board .invalid')
      .forEach(c => c.classList.remove('preview','invalid'));
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
  // Счёт дуэлей (только онлайн)
  const oppInfo = document.getElementById('opponent-info');
  if (oppInfo) {
    const duel = opponent?.duel;
    if (duel && mode === 'online') {
      const theirColor = duel.theirWins > duel.myWins ? 'style="color:var(--red)"' : '';
      const myColor    = duel.myWins > duel.theirWins ? 'style="color:var(--green,#4caf50)"' : '';
      oppInfo.innerHTML = `<span id="opp-name">${opponent.name || 'Соперник'}</span>` +
        `<span class="duel-score"><span ${theirColor}>${duel.theirWins}</span>:<span ${myColor}>${duel.myWins}</span></span>`;
    } else {
      oppInfo.innerHTML = `<span id="opp-name">${opponent?.name || 'Соперник'}</span>`;
    }
  }
  renderOpponentAvatar(opponent?.name || 'Бот', !opponent || opponent.name === 'Бот');
  setupGameLayout();
  renderGameBoard();
  updateEnemyFleet();
  showScreen('game');
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
            { label: 'В меню', cls: 'btn-primary', action: () => { closeModal(); showScreen('menu'); }},
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
          { label: 'В меню', cls: 'btn-primary', action: () => { closeModal(); showScreen('menu'); }},
        ]);
      }
    }
  });
}


function startTurnWarningUI(secondsLeft) {
  clearTurnWarningUI();
  const statusEl = document.getElementById('game-status');
  if (!statusEl) return;

  let secs = secondsLeft;
  const update = () => {
    if (!Game.active || !Game.isMyTurn) { clearTurnWarningUI(); return; }
    const mm = String(Math.floor(secs/60)).padStart(2,'0');
    const ss = String(secs % 60).padStart(2,'0');
    statusEl.textContent = `Твой ход  ${mm}:${ss}`;
    statusEl.style.color = secs <= 10 ? 'var(--red)' : 'var(--yellow)';
    if (secs <= 0) { clearTurnWarningUI(); return; }
    secs--;
  };
  update();
  Game._timerInterval = setInterval(update, 1000);
  Game._timerSeconds  = secs;
}

function clearTurnWarningUI() {
  if (Game._timerInterval) { clearInterval(Game._timerInterval); Game._timerInterval = null; }
  Game._timerSeconds = null;
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
      Sound.sunk(); vibrate([50,20,50,20,50]);
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
  setTimeout(() => showScreen('gameover'), 800);
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
        { label: 'В меню', cls: 'btn-primary', action: () => { closeModal(); showScreen('menu'); }},
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
      this.roomId  = roomId;
      Game.roomId  = roomId;
      Game.opponent = { name: opponent.name, id: opponent.playerId };
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
            Sound.sunk(); vibrate([50,20,50,20,50]);
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
      if (Game.active) showModal('Соперник вышел', 'Тебе засчитана победа!', [
        { label: 'Ок', cls: 'btn-primary', action: () => { closeModal(); endGame('win'); }},
      ]);
    });

    this.socket.on('error_msg', ({ message }) => {
      showModal('Ошибка', message, [{ label: 'Ок', cls: 'btn-ghost', action: closeModal }]);
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
      setText('waiting-sub',   'Это займёт несколько секунд');
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
  const btn    = document.getElementById('burger-btn');
  const menu   = document.getElementById('burger-menu');
  const overlay= document.getElementById('burger-overlay');

  function open()  { menu?.classList.add('open'); overlay?.classList.remove('hidden'); }
  function close() { menu?.classList.remove('open'); overlay?.classList.add('hidden'); }

  btn?.addEventListener('click', () => menu?.classList.contains('open') ? close() : open());
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
    renderStatsScreen();
    const statsBackBtn = document.getElementById('stats-back-btn');
    if (statsBackBtn) statsBackBtn.dataset.screen = Game.active ? 'game' : 'menu';
    showScreen('stats');
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
  const icon = document.getElementById('burger-sound-icon');
  if (!btn) return;
  const muted = !App.settings.sound;
  if (icon) icon.textContent = muted ? '🔇' : '🔊';
  btn.classList.toggle('muted', muted);
}

function updateBurgerEnemyMoves() {
  const btn  = document.getElementById('burger-enemy-moves');
  const icon = document.getElementById('burger-enemy-moves-icon');
  if (!btn) return;
  const on = App.settings.showEnemyMoves !== false;
  if (icon) icon.textContent = on ? '👁' : '🙈';
  btn.classList.toggle('muted', !on);
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
  setText('user-tag', App.user.username || (isGuest ? 'гость' : ''));
  const av = document.getElementById('user-avatar');
  if (av) {
    av.innerHTML = App.user.photo ? `<img src="${App.user.photo}" alt="" />` : (App.user.name[0]||'?').toUpperCase();
  }
  // Fix 1: скрываем статистику в шапке для гостей
  const statsMini = document.querySelector('.stats-mini');
  if (statsMini) statsMini.style.display = isGuest ? 'none' : '';
  updateMenuStats();
}

function bindNav() {
  document.addEventListener('click', e => {
    const btn = e.target.closest('[data-screen]');
    if (!btn) return;
    const scr    = btn.dataset.screen;
    const isBack = btn.classList.contains('btn-back');
    Sound.click();
    if (scr === 'leaderboard') renderLeaderboard();
    if (scr === 'stats')       renderStatsScreen();
    showScreen(scr, { isBack });
  });

  document.getElementById('mode-bot-easy')?.addEventListener('click',   () => startBotGame('bot-easy'));
  document.getElementById('mode-bot-hard')?.addEventListener('click',   () => startBotGame('bot-hard'));
  document.getElementById('mode-random')?.addEventListener('click',     () => startOnline('random'));
  document.getElementById('mode-friend')?.addEventListener('click',     () => startOnline('friend'));

  // Расстановка
  document.getElementById('btn-rotate')?.addEventListener('click',       () => { Placement.vertical = !Placement.vertical; Sound.click(); });
  document.getElementById('btn-random-place')?.addEventListener('click', () => Placement.randomize());
  document.getElementById('btn-clear-place')?.addEventListener('click',  () => Placement.clear());

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
      // Онлайн реванш — заново ищем соперника
      WS.disconnect();
      startOnline('random');
    } else {
      startPlacement(mode);
    }
  });

  // Кнопка "Отправить другу" — открывает список чатов TG
  document.getElementById('btn-share-invite')?.addEventListener('click', () => {
    const link = WS._inviteLink;
    if (!link) return;
    const text = `Сыграем в Морской бой?`;
    const tg = window.Telegram?.WebApp;
    try {
      tg.openTelegramLink(`https://t.me/share/url?url=${encodeURIComponent(link)}&text=${encodeURIComponent(text)}`);
      return;
    } catch(e) {}
    // Fallback — копируем в буфер
    navigator.clipboard?.writeText(`${text}\n${link}`).then(() => {
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

  // Отмена ожидания
  document.getElementById('btn-cancel-wait')?.addEventListener('click', () => { WS.disconnect(); showScreen('menu'); });

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

  // Сразу подключаемся лёгким сокетом только для счётчика
  const loadIO = () => new Promise(resolve => {
    if (window.io) { resolve(); return; }
    const s = document.createElement('script');
    s.src = window.location.origin + '/socket.io/socket.io.js';
    s.onload = resolve; s.onerror = resolve;
    document.head.appendChild(s);
  });

  loadIO().then(() => {
    if (!window.io) return;
    try {
      const sock = io(window.location.origin, { transports: ['websocket','polling'] });
      sock.on('online_count', ({ count }) => update(count));
      sock.on('connect_error', () => {});
    } catch(e) {}
  });

  // Резервный HTTP-поллинг
  const poll = () => fetch('/api/online').then(r => r.json()).then(d => update(d.count)).catch(() => {});
  poll();
  setInterval(poll, 20000);
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
    case 'stats':       return document.getElementById('stats-back-btn')?.dataset.screen || 'menu';
    case 'settings':    return document.getElementById('settings-back-btn')?.dataset.screen || 'menu';
    case 'gameover':    return 'menu';
    default:            return 'menu';
  }
}

function handleSwipeBack() {
  showScreen(_prevBackTarget(), { isBack: true });
}

/* ─── АВАТАР В БОЕВОМ ЭКРАНЕ ─────────────────────── */
function renderOpponentAvatar(name, isBot) {
  const info = document.getElementById('opponent-info');
  if (!info) return;
  const letter = isBot ? 'Б' : (name ? name[0].toUpperCase() : '?');
  const duel = Game.opponent?.duel;
  let duelHtml = '';
  if (!isBot && duel) {
    const theirColor = duel.theirWins > duel.myWins ? ' style="color:var(--red)"' : '';
    const myColor    = duel.myWins > duel.theirWins ? ' style="color:var(--green)"' : '';
    duelHtml = `<span class="duel-score"><span${theirColor}>${duel.theirWins}</span>:<span${myColor}>${duel.myWins}</span></span>`;
  }
  info.innerHTML = `<div class="opp-avatar">${letter}</div><span id="opp-name">${name || (isBot ? 'Бот' : 'Соперник')}</span>${duelHtml}`;
}

/* ─── СТАРТ ──────────────────────────────────────── */
window.addEventListener('DOMContentLoaded', async () => {
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
  updateMenuUI();
  initPromoBanner();
  updateBurgerSound();
  updateBurgerEnemyMoves();
  initSwipeBack();
  initOnlineCounter();
  showScreen('menu');

  // Синхронизируем статистику сразу при запуске
  syncStatsFromServer().catch(() => {});

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
