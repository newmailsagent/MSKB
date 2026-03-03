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
  // Fix 10: только на мобайл (touch-устройства), десктоп игнорирует
  if (!App.settings?.vibro) return;
  if (!('ontouchstart' in window) && !navigator.maxTouchPoints) return;
  try { if (navigator?.vibrate) navigator.vibrate(p); } catch(e) {}
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
function showScreen(name) {
  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  const el = document.getElementById('screen-' + name);
  if (el) el.classList.add('active');
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
    App.user = {
      id:       String(tgUser.id),
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
  App.stats   = loadJSON('bs_stats',   defaultStats());
  App.history = loadJSON('bs_history', []);
}

function recordResult(result, shots, hits, oppName) {
  if(result==='win') App.stats.wins++; else if(result==='loss') App.stats.losses++;
  App.stats.totalShots += shots; App.stats.totalHits += hits;
  saveJSON('bs_stats', App.stats);
  App.history.unshift({ result, opponent: oppName || '?', shots, hits, date: Date.now() });
  if (App.history.length > 50) App.history.pop();
  saveJSON('bs_history', App.history);
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
  const list = document.getElementById('leaderboard-list');
  const myCard = document.getElementById('my-record-card');
  const mySection = document.getElementById('my-record-section');
  const btnJoin  = document.getElementById('btn-join-rating');
  const btnLeave = document.getElementById('btn-leave-rating');
  updatePromoBanner();

  if (App.user.isGuest) {
    if (list) list.innerHTML = '<p class="empty-state">Войди через Telegram чтобы видеть рейтинг</p>';
    if (mySection) mySection.style.display = 'none';
    return;
  }

  if (mySection) mySection.style.display = '';
  if (list) list.innerHTML = '<p class="empty-state">Загрузка…</p>';

  // Инфо-кнопка
  const infoBtn = document.getElementById('rating-info-btn');
  if (infoBtn && !infoBtn._bound) {
    infoBtn._bound = true;
    infoBtn.addEventListener('click', () => {
      showModal('Как работает рейтинг', 'В рейтинге учитываются только сетевые бои со случайными соперниками.\n\nУчастие добровольное — нажми «Участвовать» и твои победы пойдут в зачёт.\n\nЕсли покинешь рейтинг и вернёшься — счёт обнуляется.\n\nМесто определяется по количеству побед.', [
        { label: 'Понятно', cls: 'btn-primary', action: closeModal }
      ]);
    });
  }

  try {
    const [ratingRes, statsRes] = await Promise.all([
      fetch('/api/rating'),
      fetch('/api/stats/' + App.user.id),
    ]);
    const ratingJson = await ratingRes.json();
    const statsJson  = await statsRes.json();
    const data    = ratingJson.ok ? (ratingJson.data || []) : [];
    const myStats = statsJson.ok ? statsJson.data : null;
    const isParticipating = myStats?.rating_active === 1;

    if (btnJoin)  { btnJoin.style.display  = isParticipating ? 'none' : ''; btnJoin.onclick  = () => doJoinRating(); }
    if (btnLeave) { btnLeave.style.display = isParticipating ? '' : 'none';  btnLeave.onclick = () => doLeaveRating(); }

    if (myCard) {
      const rw = myStats?.rated_wins   || 0;
      const rl = myStats?.rated_losses || 0;
      const rs = myStats?.rated_shots  || 0;
      const rh = myStats?.rated_hits   || 0;
      const rank = data.findIndex(e => e.id === App.user.id) + 1;

      if (!isParticipating) {
        myCard.innerHTML = '<p class="empty-state" style="margin:0;padding:8px 0">Ты не участвуешь в рейтинге</p>';
      } else if (rw + rl === 0) {
        myCard.innerHTML = '<p class="empty-state" style="margin:0;padding:8px 0">Сыграй сетевой бой чтобы появиться в рейтинге!</p>';
      } else {
        const acc = rs > 0 ? Math.round(rh/rs*100) : 0;
        const tot = rw + rl;
        const wr  = tot ? Math.round(rw/tot*100) : 0;
        myCard.innerHTML =
          '<div class="my-record-name">' + App.user.name +
          (rank > 0 ? ' <span class="rating-score-badge">#' + rank + '</span>' : '') +
          '</div>' +
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
    if (list) list.innerHTML = '<p class="empty-state">Ошибка загрузки</p>';
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
  if (!data.length) { list.innerHTML = '<p class="empty-state">Пока никто не участвует в рейтинге</p>'; return; }
  data.forEach((entry, i) => {
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

function renderStatsScreen() {
  const isGuest = !!App.user.isGuest;
  updatePromoBanner(); // обновляем баннеры

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

  const s = App.stats, total = s.wins + s.losses + s.draws;
  const statsAvatar = document.getElementById('stats-avatar');
  if (statsAvatar) {
    if (App.user.photo) {
      statsAvatar.innerHTML = `<img src="${App.user.photo}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%;">`;
    } else {
      statsAvatar.textContent = (App.user.name[0]||'?').toUpperCase();
    }
  }
  setText('stats-name', App.user.name);
  setText('st-wins', s.wins); setText('st-losses', s.losses); setText('st-draws', s.draws);
  setText('st-total', total);
  setText('st-acc',     s.totalShots ? Math.round(s.totalHits/s.totalShots*100)+'%' : '0%');
  setText('st-winrate', total        ? Math.round(s.wins/total*100)+'%' : '0%');

  if (!historyList) return;
  historyList.innerHTML = '';
  if (!App.history.length) { historyList.innerHTML = '<p class="empty-state">Нет боёв</p>'; return; }
  App.history.slice(0,20).forEach(h => {
    const div = document.createElement('div');
    div.className = 'history-item';
    const icons  = {win:'✅', loss:'❌', draw:'🤝'};
    const labels = {win:'Победа', loss:'Поражение', draw:'Ничья'};
    const d = new Date(h.date);
    const dateStr = d.toLocaleDateString('ru',{day:'2-digit',month:'2-digit'}) + ' ' + d.toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'});
    div.innerHTML = `<div class="history-icon">${icons[h.result]}</div><div class="history-info">${labels[h.result]} vs ${h.opponent}<span>${h.shots} выстрелов, ${h.hits} попаданий</span></div><div class="history-time">${dateStr}</div>`;
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
  for (let i = 0; i < 60; i++) { const d = document.createElement('div'); d.className = 'hero-grid-cell'; grid.appendChild(d); cells.push(d); }
  setInterval(() => {
    const cls = Math.random() < .3 ? 'hit' : 'active';
    const c = cells[Math.floor(Math.random()*cells.length)];
    c.classList.add(cls);
    setTimeout(() => c.classList.remove(cls,'active','hit'), 600+Math.random()*800);
  }, 200);
}

/* ─── РАССТАНОВКА ────────────────────────────────── */
const Placement = {
  board: null, ships: [], selected: null, vertical: false,
  _drag: null, _lastTap: {},

  init() {
    this.board = makeBoard(); this.ships = []; this.selected = null; this.vertical = false; this._drag = null; this._lastTap = {};
    let id = 0;
    for (const def of SHIP_DEFS)
      for (let k = 0; k < def.count; k++)
        this.ships.push({ id: id++, size: def.size, placed: false, vertical: false, cells: [] });
    this.renderDock(); this.renderBoard();
  },

  renderDock() {
    const dock = document.getElementById('ship-dock');
    if (!dock) return;
    dock.innerHTML = '';
    this.ships.forEach(ship => {
      // Fix 10: размещённые корабли не показываем в палитре
      if (ship.placed) return;
      const wrap = document.createElement('div');
      const isSelected = this.selected?.id === ship.id;
      wrap.className = 'ship-piece' + (isSelected ? ' selected' : '') + (ship.vertical ? ' vertical' : '');
      wrap.dataset.id = ship.id;
      for (let i = 0; i < ship.size; i++) { const c = document.createElement('div'); c.className = 'ship-cell'; wrap.appendChild(c); }
      wrap.addEventListener('touchend', (e) => this._handleDoubleTap(e, ship.id));
      wrap.addEventListener('dblclick', (e) => { e.preventDefault(); this.rotateSingleShip(ship.id); });
      wrap.addEventListener('pointerdown', (e) => this._startPointerDrag(e, ship, wrap));
      dock.appendChild(wrap);
    });
    const ready = document.getElementById('btn-ready');
    if (ready) ready.disabled = !this.allPlaced();
  },

  selectShip(id) { this.selected = this.ships.find(s => s.id === id) || null; Sound.click(); this.renderDock(); },

  rotateSingleShip(id) {
    const ship = this.ships.find(s => s.id === id);
    if (!ship || ship.placed) return;
    if (this.selected?.id !== id) this.selectShip(id);
    const newVertical = !this.vertical;
    // Fix 4: проверяем можно ли повернуть — ищем любое место на поле
    const hasSpace = (() => {
      for (let r = 0; r < BOARD_SIZE; r++)
        for (let c = 0; c < BOARD_SIZE; c++)
          if (canPlace(this.board, r, c, ship.size, newVertical)) return true;
      return false;
    })();
    if (!hasSpace) {
      // Тряска — вибрация и CSS-анимация
      vibrate([30,15,30,15,30]);
      const el = document.querySelector(`[data-id="${id}"]`);
      if (el) { el.classList.add('shake'); setTimeout(() => el.classList.remove('shake'), 400); }
      return;
    }
    this.vertical = newVertical; ship.vertical = this.vertical;
    Sound.click(); vibrate([10]); this.renderDock();
  },

  _handleDoubleTap(e, id) {
    const now = Date.now(), last = this._lastTap[id] || 0;
    if (now - last < 350) { e.preventDefault(); this.rotateSingleShip(id); this._lastTap[id] = 0; }
    else this._lastTap[id] = now;
  },

  _startDrag(e, ship, el) {}, _moveDrag(cx, cy) {}, _endDrag(cx, cy) {}, _startDragTouch(e, ship, el) {},

  _startPointerDrag(e, ship, el) {
    e.preventDefault();
    e.stopPropagation();

    const startX = e.clientX, startY = e.clientY;
    this._drag = { ship, el, _wasDrag: false, _ghost: null };

    const onMove = (ev) => {
      const dx = ev.clientX - startX, dy = ev.clientY - startY;
      if (!this._drag._wasDrag && Math.hypot(dx, dy) > 6) {
        this._drag._wasDrag = true;
        this.selectShip(ship.id);
        // Создаём ghost-элемент который следует за курсором
        this._createGhost(ship, el, ev.clientX, ev.clientY);
      }
      if (this._drag._wasDrag) {
        this._moveGhost(ev.clientX, ev.clientY);
        this._highlightCellUnder(ev.clientX, ev.clientY);
      }
    };

    const onUp = (ev) => {
      try { el.releasePointerCapture(e.pointerId); } catch(_) {}
      document.removeEventListener('pointermove', onMove);
      document.removeEventListener('pointerup',   onUp);
      document.removeEventListener('pointercancel', onUp);
      this._removeGhost();

      if (this._drag._wasDrag) {
        this._tryPlaceAt(ev.clientX, ev.clientY);
      } else {
        this.selectShip(ship.id);
      }
      this._drag = null;
      this.clearPreview();
    };

    try { el.setPointerCapture(e.pointerId); } catch(_) {}
    document.addEventListener('pointermove',   onMove);
    document.addEventListener('pointerup',     onUp);
    document.addEventListener('pointercancel', onUp);
  },

  _createGhost(ship, sourceEl, cx, cy) {
    const ghost = sourceEl.cloneNode(true);
    ghost.style.cssText = `
      position: fixed; z-index: 9999; pointer-events: none;
      opacity: 0.75; transform: translate(-50%, -50%) scale(1.1);
      transition: none; border-color: var(--accent);
      border-style: dashed; border-width: 2px;
      background: var(--bg2); border-radius: 5px; padding: 5px;
    `;
    ghost.style.left = cx + 'px';
    ghost.style.top  = cy + 'px';
    document.body.appendChild(ghost);
    if (this._drag) this._drag._ghost = ghost;
  },

  _moveGhost(cx, cy) {
    const ghost = this._drag?._ghost;
    if (!ghost) return;
    ghost.style.left = cx + 'px';
    ghost.style.top  = cy + 'px';
  },

  _removeGhost() {
    const ghost = this._drag?._ghost;
    if (ghost) ghost.remove();
    if (this._drag) this._drag._ghost = null;
  },

  _highlightCellUnder(cx, cy) {
    this.clearPreview();
    if (!this.selected) return;
    const rc = this._getCellFromPoint(cx, cy); if (!rc) return;
    const { r, c } = rc;
    const valid = canPlace(this.board, r, c, this.selected.size, this.vertical);
    for (let i = 0; i < this.selected.size; i++) {
      const nr = this.vertical ? r+i : r, nc = this.vertical ? c : c+i;
      if (!inBounds(nr,nc)) continue;
      const cell = document.querySelector(`#placement-board [data-r="${nr}"][data-c="${nc}"]`);
      if (cell) cell.classList.add(valid ? 'preview' : 'invalid');
    }
  },

  _tryPlaceAt(cx, cy) {
    if (!this.selected) return;
    const rc = this._getCellFromPoint(cx, cy); if (!rc) return;
    const { r, c } = rc;
    if (!canPlace(this.board, r, c, this.selected.size, this.vertical)) { vibrate([20,10,20]); return; }
    this._placeSelectedAt(r, c);
  },

  _getCellFromPoint(cx, cy) {
    const el = document.elementFromPoint(cx, cy); if (!el) return null;
    const cell = el.closest('[data-r][data-c]'); if (!cell) return null;
    const boardEl = document.getElementById('placement-board');
    if (!boardEl?.contains(cell)) return null;
    return { r: +cell.dataset.r, c: +cell.dataset.c };
  },

  _placeSelectedAt(r, c) {
    if (!this.selected) return;
    this.selected.vertical = this.vertical;
    this.selected.cells  = placeShip(this.board, r, c, this.selected.size, this.vertical);
    this.selected.placed = true;
    this.selected = null; // Fix 4: игрок сам берёт следующий корабль
    Sound.place(); vibrate([15]); this.renderDock(); this.renderBoard();
  },

  renderBoard() {
    const boardEl = document.getElementById('placement-board');
    if (!boardEl) return;
    boardEl.innerHTML = '';
    // Строим карту: ячейка -> id корабля (для двойного тапа по полю)
    const cellShipMap = {};
    this.ships.forEach(ship => {
      if (ship.placed) ship.cells.forEach(({r, c}) => { cellShipMap[r+','+c] = ship.id; });
    });

    for (let r = 0; r < BOARD_SIZE; r++) {
      for (let c = 0; c < BOARD_SIZE; c++) {
        const cell = document.createElement('div');
        cell.className = 'cell'; cell.dataset.r = r; cell.dataset.c = c;
        if (this.board[r][c] === CELL_SHIP) cell.classList.add('ship');

        // Fix 2: двойной клик/тап по размещённому кораблю — убрать и повернуть
        const shipId = cellShipMap[r+','+c];
        if (shipId !== undefined) {
          let lastTap = 0;
          const handleDoubleTap = () => {
            const now = Date.now();
            if (now - lastTap < 350) {
              lastTap = 0;
              this._removePlacedShip(shipId);
              this.rotateSingleShip(shipId);
            } else { lastTap = now; }
          };
          cell.addEventListener('pointerup', (e) => { if (!this._drag?._wasDrag) { e.preventDefault(); handleDoubleTap(); } });
          cell.addEventListener('dblclick',  (e) => { e.preventDefault(); this._removePlacedShip(shipId); this.rotateSingleShip(shipId); });
        } else {
          cell.addEventListener('pointerup', (e) => { if (this._drag?._wasDrag) return; e.preventDefault(); this.handleCellClick(r, c); });
        }

        cell.addEventListener('mouseenter', () => this.handleHover(r, c));
        cell.addEventListener('mouseleave', () => { if (!this._drag?._wasDrag) this.clearPreview(); });
        boardEl.appendChild(cell);
      }
    }
    const ready = document.getElementById('btn-ready');
    if (ready) ready.disabled = !this.allPlaced();
  },

  // Убрать размещённый корабль с поля (для повторной расстановки)
  _removePlacedShip(shipId) {
    const ship = this.ships.find(s => s.id === shipId);
    if (!ship || !ship.placed) return;
    // Очищаем клетки на доске
    ship.cells.forEach(({r, c}) => { this.board[r][c] = CELL_EMPTY; });
    ship.placed = false; ship.cells = [];
    this.selected = ship;
  },

  handleHover(r, c) {
    if (this._drag?._wasDrag || !this.selected) return;
    this.clearPreview();
    const valid = canPlace(this.board, r, c, this.selected.size, this.vertical);
    for (let i = 0; i < this.selected.size; i++) {
      const nr = this.vertical?r+i:r, nc = this.vertical?c:c+i;
      if (!inBounds(nr,nc)) continue;
      const cell = document.querySelector(`#placement-board [data-r="${nr}"][data-c="${nc}"]`);
      if (cell) cell.classList.add(valid?'preview':'invalid');
    }
  },

  clearPreview() { document.querySelectorAll('#placement-board .preview, #placement-board .invalid').forEach(c => c.classList.remove('preview','invalid')); },

  handleCellClick(r, c) {
    if (this._drag?._wasDrag || !this.selected) return;
    if (!canPlace(this.board, r, c, this.selected.size, this.vertical)) { vibrate([20,10,20]); return; }
    this._placeSelectedAt(r, c);
  },

  clear() {
    this.board = makeBoard();
    this.ships.forEach(s => { s.placed = false; s.cells = []; s.vertical = false; });
    this.selected = this.ships[0] || null; this.vertical = false;
    this.renderDock(); this.renderBoard();
  },

  randomize() {
    const { board, ships } = randomPlaceAll();
    this.board = board;
    this.ships.forEach((s, i) => { s.placed = true; s.cells = ships[i]?.cells || []; s.vertical = ships[i]?.vertical || false; });
    this.selected = null; Sound.place(); this.renderDock(); this.renderBoard();
  },

  allPlaced() { return this.ships.every(s => s.placed); },
  getShipsForGame() { return this.ships.map(s => ({ cells: [...s.cells], sunk: false, size: s.size })); },
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
    }

    if (allSunk(Game.myShips)) { renderGameBoard(); endGame('loss'); return; }
    renderGameBoard();
    setTimeout(botShoot, 700 + Math.random()*500);
  } else {
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

      // Строим ссылку: tg://t.me/bot?start=room_ID или fallback на origin
      let link;
      try {
        const cfg = await fetch('/api/config').then(r => r.json());
        if (cfg.botUsername) {
          // Telegram deep link: открывает бота и передаёт roomId как startapp параметр
          link = `https://t.me/${cfg.botUsername}/battleship?startapp=room_${roomId}`;
        } else {
          link = `${window.location.origin}/?room=${roomId}`;
        }
      } catch(e) {
        link = `${window.location.origin}/?room=${roomId}`;
      }

      const linkEl = document.getElementById('invite-link-text');
      if (linkEl) linkEl.textContent = link;
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
        if (hit) {
          Game.myBoard[r][c]    = CELL_HIT;
          Game.enemyShots[r][c] = CELL_HIT;
          if (sunk) {
            WS._sinkShipAt(Game.myBoard, r, c);
            for (let rr = 0; rr < BOARD_SIZE; rr++)
              for (let cc = 0; cc < BOARD_SIZE; cc++)
                if (Game.myBoard[rr][cc] === CELL_SUNK || Game.myBoard[rr][cc] === CELL_MISS)
                  Game.enemyShots[rr][cc] = Game.myBoard[rr][cc];
          }
        } else {
          Game.myBoard[r][c]    = CELL_MISS;
          Game.enemyShots[r][c] = CELL_MISS;
          Game.isMyTurn = true;
          // Переключаем обратно на поле врага только если включена настройка
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

    // Светлая тема TG
    if (tg.colorScheme === 'light') {
      document.body.classList.add('theme-light');
    }
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
    const scr = btn.dataset.screen;
    Sound.click();
    if (scr === 'leaderboard') renderLeaderboard();
    if (scr === 'stats')       renderStatsScreen();
    showScreen(scr);
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

  // Копировать ссылку
  document.getElementById('btn-copy-link')?.addEventListener('click', () => {
    const text = document.getElementById('invite-link-text')?.textContent;
    if (!text) return;
    navigator.clipboard?.writeText(text).then(() => {
      const btn = document.getElementById('btn-copy-link');
      if (btn) { btn.textContent = 'Скопировано!'; setTimeout(() => btn.textContent = 'Копировать', 2000); }
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
