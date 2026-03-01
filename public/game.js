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
    sunk:  () => { beep(80,'sawtooth',.5,.5); setTimeout(()=>beep(60,'sawtooth',.4,.4),200); },
    win:   () => [523,659,784,1047].forEach((f,i)=>setTimeout(()=>beep(f,'sine',.3,.4),i*150)),
    lose:  () => [200,180,160].forEach((f,i)=>setTimeout(()=>beep(f,'sawtooth',.4,.3),i*200)),
    click: () => beep(600,'sine',.05,.15),
    place: () => beep(400,'square',.06,.2),
  };
})();

function vibrate(p = [30]) {
  if (App.settings.vibro && navigator.vibrate) navigator.vibrate(p);
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
function initUser() {
  let tgUser = null;
  try { if (window.Telegram?.WebApp?.initDataUnsafe?.user) tgUser = Telegram.WebApp.initDataUnsafe.user; } catch(e) {}
  const saved = loadJSON('bs_user', null);
  if (tgUser) {
    App.user = { id: String(tgUser.id), name: tgUser.first_name || 'Игрок', username: tgUser.username ? '@'+tgUser.username : '', photo: tgUser.photo_url || null, isGuest: false };
  } else if (saved) {
    App.user = saved;
  } else {
    App.user = { id: 'guest_' + Date.now(), name: 'Гость', username: '', photo: null, isGuest: true };
  }
  saveJSON('bs_user', App.user);
}

function initSettings() {
  App.settings = loadJSON('bs_settings', { sound: true, vibro: true, hints: true, anim: true, server: '' });
  ['sound','vibro','hints','anim'].forEach(id => {
    const el = document.getElementById('setting-' + id);
    if (el) { el.checked = !!App.settings[id]; el.addEventListener('change', () => { App.settings[id] = el.checked; saveJSON('bs_settings', App.settings); }); }
  });
  const srv = document.getElementById('setting-server');
  if (srv) { srv.value = App.settings.server || ''; srv.addEventListener('change', () => { App.settings.server = srv.value.trim(); saveJSON('bs_settings', App.settings); }); }
  document.getElementById('btn-reset-stats')?.addEventListener('click', () => {
    showModal('Сбросить статистику?', 'Все данные будут удалены.', [
      { label: 'Отмена',   cls: 'btn-ghost',  action: closeModal },
      { label: 'Сбросить', cls: 'btn-danger', action: () => { App.stats = defaultStats(); App.history = []; saveJSON('bs_stats', App.stats); saveJSON('bs_history', App.history); updateMenuStats(); closeModal(); }},
    ]);
  });
}

function defaultStats() { return { wins:0, losses:0, draws:0, totalShots:0, totalHits:0 }; }

function initStats() {
  App.stats   = loadJSON('bs_stats',   defaultStats());
  App.history = loadJSON('bs_history', []);
}

function recordResult(result, shots, hits, oppName) {
  App.stats[result === 'win' ? 'wins' : result === 'loss' ? 'losses' : 'draws']++;
  App.stats.totalShots += shots; App.stats.totalHits += hits;
  saveJSON('bs_stats', App.stats);
  App.history.unshift({ result, opponent: oppName || '?', shots, hits, date: Date.now() });
  if (App.history.length > 50) App.history.pop();
  saveJSON('bs_history', App.history);
}

function updateMenuStats() {
  setText('stat-wins',  App.stats.wins);
  setText('stat-total', App.stats.wins + App.stats.losses + App.stats.draws);
}

/* ─── ЛИДЕРБОРД / СТАТИСТИКА ─────────────────────── */
function renderLeaderboard() {
  const list = document.getElementById('leaderboard-list');
  if (!list) return;
  let lb = loadJSON('bs_leaderboard', []);
  const me = { ...App.user, wins: App.stats.wins };
  const idx = lb.findIndex(e => e.id === App.user.id);
  if (idx >= 0) lb[idx] = me; else lb.push(me);
  lb.sort((a,b) => b.wins - a.wins);
  lb = lb.slice(0, 10);
  saveJSON('bs_leaderboard', lb);
  const medals = ['gold','silver','bronze'];
  list.innerHTML = '';
  if (!lb.length) { list.innerHTML = '<p class="empty-state">Пока никого нет</p>'; return; }
  lb.forEach((entry, i) => {
    const div = document.createElement('div');
    div.className = 'lb-item';
    div.innerHTML = `
      <div class="lb-rank ${medals[i]||''}">${i < 3 ? ['🥇','🥈','🥉'][i] : i+1}</div>
      <div class="lb-avatar">${(entry.name||'?')[0].toUpperCase()}</div>
      <div class="lb-info"><strong>${entry.name||'Игрок'}</strong>${entry.id===App.user.id?' <small>(вы)</small>':''}<br><small>${entry.username||''}</small></div>
      <div class="lb-wins">${entry.wins}</div>`;
    list.appendChild(div);
  });
}

function renderStatsScreen() {
  const s = App.stats, total = s.wins + s.losses + s.draws;
  setHTML('stats-avatar', (App.user.name[0]||'?').toUpperCase());
  setText('stats-name', App.user.name);
  setText('st-wins', s.wins); setText('st-losses', s.losses); setText('st-draws', s.draws);
  setText('st-total', total);
  setText('st-acc',     s.totalShots ? Math.round(s.totalHits/s.totalShots*100)+'%' : '0%');
  setText('st-winrate', total        ? Math.round(s.wins/total*100)+'%' : '0%');
  const hl = document.getElementById('history-list');
  if (!hl) return;
  hl.innerHTML = '';
  if (!App.history.length) { hl.innerHTML = '<p class="empty-state">Нет боёв</p>'; return; }
  App.history.slice(0,20).forEach(h => {
    const div = document.createElement('div');
    div.className = 'history-item';
    const icons = {win:'✅',loss:'❌',draw:'🤝'};
    const labels = {win:'Победа',loss:'Поражение',draw:'Ничья'};
    div.innerHTML = `<div class="history-icon">${icons[h.result]}</div><div class="history-info">${labels[h.result]} vs ${h.opponent}<span>${h.shots} выстрелов, ${h.hits} попаданий</span></div><div class="history-time">${new Date(h.date).toLocaleTimeString('ru',{hour:'2-digit',minute:'2-digit'})}</div>`;
    hl.appendChild(div);
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
      const wrap = document.createElement('div');
      wrap.className = 'ship-piece'+(ship.placed?' placed':'')+(this.selected?.id===ship.id?' selected':'')+(ship.vertical?' vertical':'');
      wrap.dataset.id = ship.id;
      for (let i = 0; i < ship.size; i++) { const c = document.createElement('div'); c.className = 'ship-cell'; wrap.appendChild(c); }
      if (!ship.placed) {
        wrap.addEventListener('click', (e) => { if (this._drag?._wasDrag) return; this.selectShip(ship.id); });
        wrap.addEventListener('touchend', (e) => this._handleDoubleTap(e, ship.id));
        wrap.addEventListener('dblclick', (e) => { e.preventDefault(); this.rotateSingleShip(ship.id); });
        wrap.addEventListener('mousedown', (e) => this._startDrag(e, ship, wrap));
        wrap.addEventListener('touchstart', (e) => this._startDragTouch(e, ship, wrap), { passive: false });
      }
      dock.appendChild(wrap);
    });
  },

  selectShip(id) { this.selected = this.ships.find(s => s.id === id) || null; Sound.click(); this.renderDock(); },

  rotateSingleShip(id) {
    const ship = this.ships.find(s => s.id === id);
    if (!ship || ship.placed) return;
    if (this.selected?.id !== id) this.selectShip(id);
    this.vertical = !this.vertical; ship.vertical = this.vertical;
    Sound.click(); vibrate([10]); this.renderDock();
  },

  _handleDoubleTap(e, id) {
    const now = Date.now(), last = this._lastTap[id] || 0;
    if (now - last < 350) { e.preventDefault(); this.rotateSingleShip(id); this._lastTap[id] = 0; }
    else this._lastTap[id] = now;
  },

  _startDrag(e, ship, el) {
    if (e.button !== 0) return; e.preventDefault();
    this._drag = { ship, el, _wasDrag: false };
    this._drag._onMove = ev => this._moveDrag(ev.clientX, ev.clientY);
    this._drag._onUp   = ev => this._endDrag(ev.clientX, ev.clientY);
    document.addEventListener('mousemove', this._drag._onMove);
    document.addEventListener('mouseup',   this._drag._onUp);
    this.selectShip(ship.id);
  },
  _moveDrag(cx, cy) { if (!this._drag) return; this._drag._wasDrag = true; this._highlightCellUnder(cx, cy); },
  _endDrag(cx, cy) {
    if (!this._drag) return;
    document.removeEventListener('mousemove', this._drag._onMove);
    document.removeEventListener('mouseup',   this._drag._onUp);
    this._tryPlaceAt(cx, cy); this._drag = null; this.clearPreview();
  },

  _startDragTouch(e, ship, el) {
    const t = e.touches[0];
    this._drag = {
      ship, el, startX: t.clientX, startY: t.clientY, _wasDrag: false,
      _onMove: ev => {
        ev.preventDefault();
        const tt = ev.touches[0];
        if (!this._drag._wasDrag && Math.hypot(tt.clientX-this._drag.startX, tt.clientY-this._drag.startY) > 8) {
          this._drag._wasDrag = true; this.selectShip(ship.id);
        }
        if (this._drag._wasDrag) this._highlightCellUnder(tt.clientX, tt.clientY);
      },
      _onEnd: ev => {
        const tt = ev.changedTouches[0];
        document.removeEventListener('touchmove', this._drag._onMove);
        document.removeEventListener('touchend',  this._drag._onEnd);
        if (this._drag._wasDrag) this._tryPlaceAt(tt.clientX, tt.clientY);
        this._drag = null; this.clearPreview();
      },
    };
    document.addEventListener('touchmove', this._drag._onMove, { passive: false });
    document.addEventListener('touchend',  this._drag._onEnd);
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
    this.selected = this.ships.find(s => !s.placed) || null;
    Sound.place(); vibrate([15]); this.renderDock(); this.renderBoard();
  },

  renderBoard() {
    const boardEl = document.getElementById('placement-board');
    if (!boardEl) return;
    boardEl.innerHTML = '';
    for (let r = 0; r < BOARD_SIZE; r++) {
      for (let c = 0; c < BOARD_SIZE; c++) {
        const cell = document.createElement('div');
        cell.className = 'cell'; cell.dataset.r = r; cell.dataset.c = c;
        if (this.board[r][c] === CELL_SHIP) cell.classList.add('ship');
        cell.addEventListener('click', () => this.handleCellClick(r, c));
        cell.addEventListener('mouseenter', () => this.handleHover(r, c));
        cell.addEventListener('mouseleave', () => { if (!this._drag?._wasDrag) this.clearPreview(); });
        boardEl.appendChild(cell);
      }
    }
    const ready = document.getElementById('btn-ready');
    if (ready) ready.disabled = !this.allPlaced();
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
    setShowingField(true);
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
    renderBoard(enemyEl, display, {
      clickable:   Game.isMyTurn,
      showShips:   false,
      onCellClick: (r, c) => playerShoot(r, c),
      dimmed:      desktop && !Game.isMyTurn,
    });
  }

  // Наше поле (только на десктопе постоянно, на мобайле — по toggle)
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

  // Мобайл — одно поле, переключаемое
  if (!desktop) {
    const singleEl = document.getElementById('game-board');
    if (!Game.showingEnemy) {
      const display = cloneBoard(Game.myBoard);
      for (let r = 0; r < BOARD_SIZE; r++)
        for (let c = 0; c < BOARD_SIZE; c++) {
          const s = Game.enemyShots[r][c];
          if (s !== CELL_EMPTY) display[r][c] = s;
        }
      renderBoard(singleEl, display, { clickable: false, showShips: true });
    }
  }

  updateShipsLeft();
  updateEnemyFleet();
}

function setShowingField(showEnemy) {
  Game.showingEnemy = showEnemy;
  document.getElementById('btn-show-enemy')?.classList.toggle('active',  showEnemy);
  document.getElementById('btn-show-mine')?.classList.toggle('active',  !showEnemy);
}

function updateGameStatus() {
  const el = document.getElementById('game-status');
  if (!el || !Game.active) return;
  el.textContent = Game.isMyTurn ? 'Твой ход' : 'Ход соперника';
  el.style.color = Game.isMyTurn ? 'var(--green)' : 'var(--hint)';

  // На десктопе — подсветка полупрозрачностью через dimmed (уже в renderBoard)
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
    if (!isDesktop()) setShowingField(true);
    renderGameBoard();
  } else {
    Game.isMyTurn = false;
    updateGameStatus();
    if (!isDesktop()) setShowingField(false);
    renderGameBoard();
    if (Game.mode.startsWith('bot')) setTimeout(botShoot, 800 + Math.random()*600);
  }

  // Онлайн — отправляем выстрел
  if (Game.mode === 'online') WS.sendShot(r, c);
}

/* ─── БОТ ────────────────────────────────────────── */
function botGetDifficulty() {
  if (Game.mode === 'bot-easy')   return 'easy';
  if (Game.mode === 'bot-medium') return 'medium';
  return 'hard';
}

function botShoot() {
  if (!Game.active || Game.isMyTurn) return;
  const diff = botGetDifficulty();
  let r, c;

  if (diff === 'easy') {
    const e = getEmptyCells(Game.enemyShots); if (!e.length) return;
    [r, c] = e[Math.floor(Math.random()*e.length)];
  } else if (diff === 'medium') {
    if (Game.botQueue.length) {
      [r, c] = Game.botQueue.shift();
      while (Game.enemyShots[r][c] !== CELL_EMPTY) {
        if (!Game.botQueue.length) { [r, c] = randomEmpty(Game.enemyShots); break; }
        [r, c] = Game.botQueue.shift();
      }
    } else { [r, c] = randomEmpty(Game.enemyShots); }
  } else {
    if (Game.botQueue.length) {
      [r, c] = Game.botQueue.shift();
      while (Game.botQueue.length && Game.enemyShots[r][c] !== CELL_EMPTY) [r, c] = Game.botQueue.shift();
      if (Game.enemyShots[r][c] !== CELL_EMPTY) [r, c] = huntChessEmpty(Game.enemyShots);
    } else { [r, c] = huntChessEmpty(Game.enemyShots); }
  }

  if (r === undefined) return;

  const hit = Game.myBoard[r][c] === CELL_SHIP;
  Game.enemyShots[r][c] = hit ? CELL_HIT : CELL_MISS;

  if (hit) {
    Game.myBoard[r][c] = CELL_HIT; Game.botLastHit = {r, c};
    if (diff !== 'easy') {
      const nb = getNeighbors4(r, c).filter(([nr,nc]) => Game.enemyShots[nr][nc] === CELL_EMPTY);
      if (diff === 'hard' && Game.botDirection) {
        const [dr,dc] = Game.botDirection;
        Game.botQueue = [];
        if (inBounds(r+dr,c+dc) && Game.enemyShots[r+dr][c+dc]===CELL_EMPTY) Game.botQueue.push([r+dr,c+dc]);
        if (inBounds(r-dr,c-dc) && Game.enemyShots[r-dr][c-dc]===CELL_EMPTY) Game.botQueue.push([r-dr,c-dc]);
      } else { Game.botQueue.push(...nb); }
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
    if (!isDesktop()) setShowingField(true);
    renderGameBoard();
  }
}

const getEmptyCells = board => { const r = []; for (let i=0;i<BOARD_SIZE;i++) for (let j=0;j<BOARD_SIZE;j++) if(board[i][j]===CELL_EMPTY) r.push([i,j]); return r; };
const randomEmpty   = board => { const e=getEmptyCells(board); return e[Math.floor(Math.random()*e.length)]||[0,0]; };
function huntChessEmpty(board) {
  const c = [];
  for (let r=0;r<BOARD_SIZE;r++) for (let cc=0;cc<BOARD_SIZE;cc++) if((r+cc)%2===0&&board[r][cc]===CELL_EMPTY) c.push([r,cc]);
  return c.length ? c[Math.floor(Math.random()*c.length)] : randomEmpty(board);
}
const getNeighbors4 = (r,c) => [[r-1,c],[r+1,c],[r,c-1],[r,c+1]].filter(([nr,nc])=>inBounds(nr,nc));

/* ─── КОНЕЦ ИГРЫ ─────────────────────────────────── */
function endGame(result) {
  Game.active = false;
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
    this.socket.on('room_created', ({ roomId }) => {
      this.roomId  = roomId;
      Game.roomId  = roomId;
      const origin = App.settings.server || window.location.origin;
      const link   = `${origin}/?room=${roomId}`;
      setText('waiting-title', 'Ждём друга…');
      setText('waiting-sub',   'Отправь ссылку другу');
      const block = document.getElementById('invite-block');
      if (block) block.classList.remove('hidden');
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
      startGame('online', Placement.board, myShips, makeBoard(), [], Game.opponent);
      Game.isMyTurn = isMyTurn;
      updateGameStatus();
      renderGameBoard();
    });

    // ── Результат выстрела ────────────────────────────
    this.socket.on('shot_result', ({ r, c, hit, sunk, shooter, gameOver, winner }) => {
      if (shooter === App.user.id) {
        // Наш выстрел
        Game.myShots[r][c] = sunk ? CELL_SUNK : (hit ? CELL_HIT : CELL_MISS);
        if (hit) {
          Sound.hit(); vibrate([30,10,30]);
          if (sunk) { Sound.sunk(); vibrate([50,20,50,20,50]); }
        } else {
          Sound.miss(); vibrate([10]);
          Game.isMyTurn = false;
          if (!isDesktop()) setShowingField(false);
        }
      } else {
        // Выстрел соперника
        Game.myBoard[r][c]    = sunk ? CELL_SUNK : (hit ? CELL_HIT : CELL_MISS);
        Game.enemyShots[r][c] = Game.myBoard[r][c];
        if (!hit) {
          Game.isMyTurn = true;
          if (!isDesktop()) setShowingField(true);
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
};

/* ─── ОНЛАЙН ИГРА ────────────────────────────────── */
async function startOnline(mode) {
  showScreen('waiting');
  setText('waiting-title', 'Подключение…');
  setText('waiting-sub',   'Соединяемся с сервером');
  document.getElementById('invite-block')?.classList.add('hidden');

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
      { label: 'С ботом',  cls: 'btn-primary', action: () => { closeModal(); startBotGame('bot-medium'); }},
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
      { label: 'Сдаться',   cls: 'btn-danger',  action: () => { closeModal(); endGame('loss'); }},
    ]);
  });

  document.getElementById('burger-sound')?.addEventListener('click', () => {
    App.settings.sound = !App.settings.sound;
    saveJSON('bs_settings', App.settings);
    updateBurgerSound();
    initSoundButton(); // обновляет иконку в шапке меню
  });

  document.getElementById('burger-stats-btn')?.addEventListener('click', () => {
    close();
    renderStatsScreen();
    showScreen('stats');
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
  try {
    if (!window.Telegram?.WebApp) return;
    Telegram.WebApp.ready();
    Telegram.WebApp.expand();
    Telegram.WebApp.setHeaderColor('secondary_bg_color');
    Telegram.WebApp.enableClosingConfirmation();
  } catch(e) {}
}

/* ─── НАВИГАЦИЯ ──────────────────────────────────── */
function updateMenuUI() {
  setText('user-name', App.user.name);
  setText('user-tag', App.user.username || (App.user.isGuest ? 'гость' : ''));
  const av = document.getElementById('user-avatar');
  if (av) {
    av.innerHTML = App.user.photo ? `<img src="${App.user.photo}" alt="" />` : (App.user.name[0]||'?').toUpperCase();
  }
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
  document.getElementById('mode-bot-medium')?.addEventListener('click', () => startBotGame('bot-medium'));
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
      { label: 'Сдаться',   cls: 'btn-danger',  action: () => { closeModal(); endGame('loss'); }},
    ]);
  });

  // Реванш
  document.getElementById('btn-rematch')?.addEventListener('click', () => { Sound.click(); startPlacement(pendingGameMode || 'bot-medium'); });

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
  bindNav();
  updateMenuUI();
  updateBurgerSound();

  await new Promise(r => setTimeout(r, 1200));
  showScreen('menu');

  // Обработка ссылки-приглашения: /?room=<roomId>
  const params = new URLSearchParams(window.location.search);
  const room   = params.get('room');
  if (room) {
    setTimeout(() => {
      showModal('Приглашение в игру', `Тебя пригласили! Подключиться?`, [
        { label: 'Подключиться', cls: 'btn-primary', action: () => { closeModal(); startOnline('friend_join:' + room); }},
        { label: 'Отмена',       cls: 'btn-ghost',   action: closeModal },
      ]);
    }, 400);
  }
});
