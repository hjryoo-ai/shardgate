// ShardGate 대기실 클라이언트.
//
// 이 페이지가 하는 일은 하나다: **기다리는 사람에게 자기 위치를 정직하게 보여주는 것.**
// 순번은 서버가 정한다(§3.3). 여기서는 어떤 값도 계산하지 않고, 받은 것을 그리기만 한다 —
// 클라이언트가 순번을 계산하기 시작하면 그 값은 곧 조작 대상이 된다.

(() => {
  'use strict';

  const API = {
    enter: '/api/v1/queue/enter',
    verify: '/api/v1/challenge/verify',
    reissue: '/api/v1/challenge/reissue',
    reverify: '/api/v1/challenge/reverify',
    status: '/api/v1/queue/status',
    heartbeat: '/api/v1/queue/heartbeat',
    redeem: '/api/v1/admission/redeem',
    orders: '/api/v1/orders',
  };

  const HEARTBEAT_MS = 5000;
  const REDEEM_MS = 5000;
  // 판에 그리는 최대 칸 수. 이보다 큰 구역은 한 칸이 여러 명을 나타낸다.
  const MAX_CELLS = 480;
  const PLATE_COLS = 24;

  const el = (id) => document.getElementById(id);
  const ui = {
    eventName: el('eventName'), linkState: el('linkState'), linkLabel: el('linkLabel'),
    room: el('room'), shardID: el('shardID'), plate: el('plate'), plateNote: el('plateNote'),
    ticket: el('ticket'), ticketLabel: el('ticketLabel'), ticketMsg: el('ticketMsg'),
    rank: el('rank'), ahead: el('ahead'), estimate: el('estimate'), shardSize: el('shardSize'),
    action: el('action'), rail: el('rail'), log: el('log'), meta: el('meta'),
  };

  // ── 상태 ──────────────────────────────────────────────────────────
  const state = {
    token: sessionStorage.getItem('sg.token') || '',
    fp: '',
    shard: '',
    entryToken: '',
    stage: 'idle', // idle | solving | waiting | flagged | admitted | done | blocked
    // capacity 는 이 구역이 가장 붐볐을 때의 인원이다. 판의 크기를 여기에 고정해야
    // 사람들이 빠져나가는 것이 "판이 비어 가는" 모습으로 보인다.
    capacity: 0,
    // rechallenging 은 재검증이 진행 중인지다. redeem 은 5초마다 도는데 PoW 는
    // 그보다 오래 걸릴 수 있어, 없으면 워커가 여러 개 겹쳐 뜬다.
    rechallenging: false,
    cells: [],
    timers: [],
    stream: null,
  };

  // ── 표시 도우미 ───────────────────────────────────────────────────
  const nf = new Intl.NumberFormat('ko-KR');

  function duration(ms) {
    if (ms === null || ms === undefined || ms < 0) return '—';
    const s = Math.round(ms / 1000);
    if (s < 60) return `${s}초`;
    const m = Math.floor(s / 60);
    if (m < 60) return s % 60 ? `${m}분 ${s % 60}초` : `${m}분`;
    return `${Math.floor(m / 60)}시간 ${m % 60}분`;
  }

  function log(text, kind) {
    const li = document.createElement('li');
    if (kind) li.dataset.kind = kind;
    const t = document.createElement('time');
    t.textContent = new Date().toTimeString().slice(0, 8);
    const span = document.createElement('span');
    span.textContent = text;
    li.append(t, span);
    ui.log.prepend(li);
    while (ui.log.children.length > 60) ui.log.lastChild.remove();
  }

  function link(stateName, label) {
    ui.linkState.dataset.state = stateName;
    ui.linkLabel.textContent = label;
  }

  function rail(step) {
    const order = ['challenge', 'waiting', 'admitted', 'ordered'];
    const at = order.indexOf(step);
    ui.rail.querySelectorAll('.rail__step').forEach((li, i) => {
      li.dataset.active = String(i === at);
      li.dataset.done = String(i < at);
    });
  }

  // ── 구역 판 ───────────────────────────────────────────────────────
  //
  // 한 칸이 사람 하나다. 500,000명의 줄이 아니라 1,000명의 방에 있다는 것이
  // 이 시스템의 요점이고, 그건 숫자보다 판으로 보여야 이해된다.
  function drawPlate(size, ahead, drained) {
    const scale = Math.max(1, Math.ceil(size / MAX_CELLS));
    const cells = Math.max(1, Math.ceil(size / scale));

    if (state.cells.length !== cells) {
      ui.plate.replaceChildren();
      state.cells = [];
      const frag = document.createDocumentFragment();
      for (let i = 0; i < cells; i++) {
        const c = document.createElement('span');
        c.className = 'plate__cell';
        frag.appendChild(c);
        state.cells.push(c);
      }
      ui.plate.style.setProperty('--cols', String(Math.min(PLATE_COLS, cells)));
      ui.plate.appendChild(frag);
    }

    const gone = Math.floor(drained / scale);
    const me = gone + Math.floor(ahead / scale);
    for (let i = 0; i < state.cells.length; i++) {
      const role = i < gone ? 'gone' : i === me ? 'you' : i < me ? 'ahead' : 'behind';
      if (state.cells[i].dataset.role !== role) state.cells[i].dataset.role = role;
    }

    ui.plateNote.textContent = scale === 1
      ? `한 칸이 한 명입니다. 앞의 ${nf.format(ahead)}칸이 비면 당신 차례입니다.`
      : `한 칸이 약 ${nf.format(scale)}명입니다. 구역 인원 ${nf.format(size)}명.`;
  }

  // ── 지문 해시 ─────────────────────────────────────────────────────
  //
  // 원본 지문은 서버로 보내지 않는다(불변식 6). 해시만 보내고, 그 해시는
  // 큐 토큰에 박혀 "다른 기기에서 같은 토큰 쓰기"를 막는 데 쓰인다(§4-L3).
  function fingerprint() {
    let seed = localStorage.getItem('sg.seed');
    if (!seed) {
      seed = crypto.randomUUID();
      localStorage.setItem('sg.seed', seed);
    }
    const parts = [
      seed, navigator.userAgent, navigator.language,
      screen.width + 'x' + screen.height + 'x' + screen.colorDepth,
      new Date().getTimezoneOffset(), navigator.hardwareConcurrency || 0,
    ];
    return self.ShardGateHash.sha256Hex(parts.join('|')).slice(0, 32);
  }

  function headers(extra) {
    const h = Object.assign({ 'X-Device-Fingerprint': state.fp }, extra || {});
    if (state.token) h.Authorization = `Bearer ${state.token}`;
    return h;
  }

  async function post(url, body, extra) {
    const res = await fetch(url, {
      method: 'POST',
      headers: Object.assign({ 'Content-Type': 'application/json' }, headers(extra)),
      body: body === undefined ? null : JSON.stringify(body),
    });
    let payload = null;
    try {
      payload = await res.json();
    } catch (_) { /* 본문이 없는 응답도 있다 */ }
    return { ok: res.ok, status: res.status, body: payload };
  }

  // ── 진입 ──────────────────────────────────────────────────────────

  async function joinQueue() {
    ui.action.disabled = true;
    setStage('solving');
    rail('challenge');

    const entered = await post(API.enter, {});
    if (!entered.ok) {
      fail('대기열이 응답하지 않습니다. 잠시 후 다시 시도하세요.');
      return;
    }

    ui.eventName.textContent = `${entered.body.event} · 티켓 오픈`;
    const c = entered.body.challenge;
    log(`확인 문제 받음 — 난이도 ${c.difficulty}비트`);
    // 푸는 동안 큰 숫자는 시도 횟수를 센다. 기다리는 시간이 무엇에 쓰이는지 보인다.
    ui.ticketLabel.textContent = '확인 시도';
    ui.ticketMsg.textContent = '브라우저가 확인 문제를 푸는 중입니다. 몇 초 걸릴 수 있어요.';

    const solved = await solve(c);
    if (!solved) {
      fail('확인에 실패했습니다. 새로고침 후 다시 시도하세요.');
      return;
    }
    log(`확인 완료 — ${nf.format(solved.tries)}회 시도, ${solved.ms}ms`, 'good');
    ui.meta.textContent = `PoW ${c.difficulty}bit · ${nf.format(solved.tries)} tries · ${solved.ms}ms`;

    const verified = await post(API.verify, {
      challenge: c, solution: solved.solution, solve_ms: solved.ms,
    });
    if (!verified.ok) {
      fail('확인이 거절됐습니다. 새로고침 후 다시 시도하세요.');
      return;
    }

    const v = verified.body;
    state.token = v.token;
    state.shard = v.shard;
    sessionStorage.setItem('sg.token', v.token);

    ui.shardID.textContent = v.shard.toUpperCase();
    log(`${v.shard} 구역 배정 · ${v.segment === 'lottery' ? '추첨 구간' : '도착순 구간'}`, 'good');
    if (v.segment === 'lottery') {
      log('추첨 구간이라 도착 순서와 무관하게 순번이 정해집니다');
    }

    enterWaiting();
  }

  /** solve 는 PoW 를 워커에 맡긴다. 메인 스레드는 화면을 그리는 데 쓴다. */
  function solve(challenge) {
    return new Promise((resolve) => {
      const worker = new Worker('pow-worker.js');
      worker.onmessage = (e) => {
        if (e.data.type === 'progress') {
          ui.rank.textContent = nf.format(e.data.tries);
          return;
        }
        worker.terminate();
        resolve(e.data.type === 'solved' ? e.data : null);
      };
      worker.onerror = () => {
        worker.terminate();
        resolve(null);
      };
      worker.postMessage({ nonce: challenge.nonce, difficulty: challenge.difficulty });
    });
  }

  // ── 대기 ──────────────────────────────────────────────────────────

  function enterWaiting() {
    setStage('waiting');
    rail('waiting');
    ui.action.textContent = '대기 중';
    ui.action.disabled = true;
    ui.ticketMsg.textContent = '자리가 나면 자동으로 입장합니다. 이 창을 열어 두세요.';

    openStream();
    state.timers.push(setInterval(sendHeartbeat, HEARTBEAT_MS));
    state.timers.push(setInterval(tryRedeem, REDEEM_MS));
    sendHeartbeat();
  }

  /** openStream 은 순번을 SSE 로 받는다. 토큰은 쿠키로 실린다(EventSource 는 헤더를 못 붙인다). */
  function openStream() {
    const stream = new EventSource(API.status);
    state.stream = stream;

    stream.addEventListener('status', (e) => {
      link('live', '연결됨');
      render(JSON.parse(e.data));
    });
    stream.onerror = () => {
      link('lost', '재연결 중');
    };
  }

  async function sendHeartbeat() {
    const res = await post(API.heartbeat, {
      pointer_entropy: pointerEntropy(),
      visible: document.visibilityState === 'visible',
      events: recentEvents.splice(0, 12),
    });
    if (res.ok && res.body && res.body.revived) {
      log('연결이 끊겼다 돌아왔습니다 — 순번은 그대로입니다', 'good');
    }
  }

  async function tryRedeem() {
    if (state.stage !== 'waiting' && state.stage !== 'flagged') return;

    const res = await post(API.redeem);
    if (res.status === 403) {
      flagged(res.body && res.body.code);
      return;
    }
    if (!res.ok || !res.body) return;

    if (res.body.status === 'challenge_required') {
      runRechallenge();
      return;
    }
    if (res.body.status === 'admitted') {
      state.entryToken = res.body.entry_token;
      admitted(res.body.waited_ms);
    }
  }

  /**
   * runRechallenge — greylist 에서 돌아 나오는 길(§4).
   *
   * 이 함수가 없으면 flagged() 가 화면에 적어 둔 "통과하면 원래 자리로 돌아갑니다"가
   * 지키지 못할 약속이 된다. 되돌아올 길이 없는 격리는 오탐으로 걸린 사람에게
   * 긴 대기 끝의 거절 하나만 남긴다.
   */
  async function runRechallenge() {
    if (state.rechallenging) return;
    state.rechallenging = true;
    try {
      const issued = await post(API.reissue, {});
      if (!issued.ok || !issued.body) return;

      const c = issued.body.challenge;
      log(`추가 확인 문제 — 난이도 ${c.difficulty}비트 (${issued.body.attempts_left}회 남음)`, 'flag');
      ui.ticketLabel.textContent = '추가 확인 중';
      ui.ticketMsg.textContent = '한 번 더 확인하고 있습니다. 순번은 그대로입니다.';

      const solved = await solve(c);
      if (!solved) {
        log('추가 확인에 실패했습니다', 'flag');
        return;
      }

      const res = await post(API.reverify, {
        challenge: c, solution: solved.solution, solve_ms: solved.ms,
      });
      if (!res.ok || !res.body) return;

      if (res.body.outcome === 'restored') {
        setStage('waiting');
        ui.ticketLabel.textContent = '대기 순번';
        ui.ticketMsg.textContent = '자리가 나면 자동으로 입장합니다. 이 창을 열어 두세요.';
        log('추가 확인 통과 — 원래 자리로 돌아왔습니다', 'good');
        // 복귀하면 관찰 시계가 다시 시작한다(§3.4). 순번은 그대로지만 입장까지
        // 시간이 더 걸리므로, 말해 주지 않으면 "통과했는데 왜 안 들어가지"가 된다.
        // 예상 시간 자체는 서버가 같은 시계로 계산해 status 로 내려준다.
        log('확인 직후에는 잠시 더 지켜봅니다 — 순번은 그대로입니다');
      } else if (res.body.outcome === 'exhausted') {
        // 확인을 반복해서 요구받는 것 자체가 신호다. 순번은 여전히 보존된다.
        flagged('held');
        log('추가 확인 횟수를 모두 썼습니다 — 입장이 보류됩니다', 'flag');
      }
    } finally {
      state.rechallenging = false;
    }
  }

  // ── 상태 전이 ─────────────────────────────────────────────────────

  function setStage(stage) {
    state.stage = stage;
    const look = stage === 'flagged' ? 'flagged'
      : stage === 'blocked' ? 'blocked'
      : stage === 'admitted' ? 'admitted'
      : stage === 'done' ? 'done' : 'idle';
    ui.room.dataset.state = look;
    ui.ticket.dataset.state = look;
  }

  function render(snap) {
    if (state.stage === 'admitted' || state.stage === 'done') return;

    ui.ticketLabel.textContent = '대기 순번';
    ui.shardID.textContent = (snap.shard || state.shard || '—').toUpperCase();
    ui.rank.textContent = nf.format(snap.rank + 1);
    ui.ahead.textContent = `${nf.format(snap.ahead)}명`;
    ui.shardSize.textContent = `${nf.format(snap.shard_size)}명`;
    ui.estimate.textContent = duration(snap.estimated_wait_ms);

    // 판은 가장 붐볐을 때 크기로 고정하고, 줄어든 만큼을 앞에서부터 비운다.
    // 입장은 앞사람부터 나가므로 비는 자리도 앞쪽이다.
    state.capacity = Math.max(state.capacity, snap.shard_size);
    drawPlate(state.capacity, snap.rank, state.capacity - snap.shard_size);

    if (snap.state === 'greylist' || snap.state === 'held') {
      flagged(snap.state);
    } else if (snap.state === 'blocked') {
      blocked();
    }
  }

  /**
   * flagged — greylist/보류. **처벌이 아니라 재검증이다**(§4).
   * 순번을 그대로 들고 가므로, 정상 사용자는 통과하면 아무것도 잃지 않는다.
   * 그 사실을 화면에서 먼저 말해 주지 않으면 사용자는 차단당한 줄 안다.
   */
  function flagged(code) {
    if (state.stage === 'flagged') return;
    setStage('flagged');
    ui.ticketLabel.textContent = '확인 중 · 순번 유지';
    ui.ticketMsg.textContent = code === 'held'
      ? '추가 확인이 끝날 때까지 입장이 잠시 멈춥니다. 순번은 그대로입니다.'
      : '자동 확인이 한 번 더 진행됩니다. 순번은 그대로이고, 통과하면 원래 자리로 돌아갑니다.';
    log(code === 'held' ? '입장 보류 — 순번은 보존됩니다' : '추가 확인 구역으로 이동 — 순번은 그대로', 'flag');
  }

  function blocked() {
    setStage('blocked');
    stopTimers();
    ui.ticketLabel.textContent = '차단됨';
    ui.rank.textContent = '—';
    ui.ticketMsg.textContent = '이 기기의 대기 자격이 해제됐습니다. 문의가 필요하면 고객센터로 연락하세요.';
    ui.action.textContent = '차단됨';
    ui.action.disabled = true;
    log('토큰이 무효화됐습니다', 'bad');
  }

  function admitted(waitedMS) {
    setStage('admitted');
    rail('admitted');
    stopTimers();

    ui.ticketLabel.textContent = '입장';
    ui.rank.textContent = 'GO';
    ui.estimate.textContent = duration(waitedMS);
    ui.ticketMsg.textContent = '5분 안에 구매를 마쳐야 합니다. 입장권은 한 번만 쓸 수 있습니다.';
    ui.action.textContent = '구매하기';
    ui.action.disabled = false;
    ui.action.onclick = buy;
    log(`입장 — ${duration(waitedMS)} 기다렸습니다`, 'good');

    // 판을 비운다: 앞사람이 모두 빠지고 내 자리만 남는다.
    if (state.cells.length) {
      state.cells.forEach((c, i) => { c.dataset.role = i === 0 ? 'you' : 'gone'; });
    }
  }

  async function buy() {
    ui.action.disabled = true;
    ui.action.textContent = '처리 중';

    const key = sessionStorage.getItem('sg.idem') || crypto.randomUUID();
    sessionStorage.setItem('sg.idem', key);

    const res = await post(API.orders,
      { account_id: `demo-${state.fp.slice(0, 12)}`, sku: 'demo-ticket', qty: 1 },
      { 'X-Entry-Token': state.entryToken, 'Idempotency-Key': key });

    if (!res.ok) {
      const code = (res.body && res.body.code) || 'error';
      ui.ticketMsg.textContent = code === 'duplicate_order'
        ? '이 계정은 이미 구매했습니다. 1인 1매입니다.'
        : '구매를 마치지 못했습니다. 다시 시도하세요.';
      ui.action.textContent = '다시 시도';
      ui.action.disabled = false;
      log(`구매 실패 — ${code}`, 'bad');
      return;
    }

    setStage('done');
    rail('ordered');
    ui.ticketLabel.textContent = '구매 완료';
    ui.rank.textContent = '✓';
    ui.ticketMsg.textContent = `주문 ${res.body.order_id}번이 확정됐습니다.`;
    ui.action.textContent = '완료';
    log(`주문 ${res.body.order_id}번 확정${res.body.replayed ? ' (재시도 응답)' : ''}`, 'good');
  }

  function fail(message) {
    setStage('idle');
    ui.ticketMsg.textContent = message;
    ui.action.textContent = '다시 시도';
    ui.action.disabled = false;
    log(message, 'bad');
  }

  function stopTimers() {
    state.timers.forEach(clearInterval);
    state.timers = [];
    if (state.stream) {
      state.stream.close();
      state.stream = null;
    }
    link('idle', '종료');
  }

  // ── 행동 신호 ─────────────────────────────────────────────────────
  //
  // 서버에 보내는 것은 **요약값**뿐이다. 포인터 좌표를 그대로 보내면
  // 대기실이 행동 추적기가 된다 — 필요한 것은 "사람다운 불규칙성"의 유무지
  // 그 사람이 어디를 눌렀는지가 아니다.
  const recentEvents = [];
  let pointerBuckets = new Set();
  let pointerCount = 0;

  function pointerEntropy() {
    if (pointerCount === 0) return 0;
    const value = Math.min(1, pointerBuckets.size / 24);
    pointerBuckets = new Set();
    pointerCount = 0;
    return Number(value.toFixed(3));
  }

  ['pointermove', 'pointerdown', 'keydown', 'scroll'].forEach((name) => {
    addEventListener(name, (e) => {
      if (recentEvents.length < 32) recentEvents.push(name);
      if (name === 'pointermove') {
        pointerCount++;
        pointerBuckets.add(`${Math.floor(e.clientX / 64)},${Math.floor(e.clientY / 64)}`);
      }
    }, { passive: true });
  });

  document.addEventListener('visibilitychange', () => {
    if (recentEvents.length < 32) recentEvents.push('visibilitychange');
  });

  // ── 시작 ──────────────────────────────────────────────────────────

  state.fp = fingerprint();
  ui.action.onclick = joinQueue;

  if (state.token) {
    // 새로고침해도 줄을 다시 서지 않는다. 토큰이 살아 있으면 그대로 이어 간다.
    log('이전 대기 상태를 이어 갑니다');
    enterWaiting();
  } else {
    log('대기실 준비 완료');
    link('idle', '대기');
  }
})();
