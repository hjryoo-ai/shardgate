// ShardGate k6 클라이언트 (DESIGN.md §11).
//
// 세 시나리오(normal_users / bot_farm / mixed)가 공유하는 프로토콜 구현이다.
// 여기에 "어떻게 요청하는가"만 두고, "누가 어떻게 행동하는가"는 각 시나리오가 갖는다.
//
// 기본 진입점은 대기실 오리진(:8088)이다. 실제 배포에서 CDN 이 정적 페이지를
// 서빙하고 API 를 원서버로 넘기는 구조를 그대로 흉내 낸다(§2). 서비스 포트로
// 직접 때리고 싶으면 SG_BASE_URL 로 바꾼다.

import http from 'k6/http';
import crypto from 'k6/crypto';
import { Trend, Counter, Rate } from 'k6/metrics';

export const BASE = __ENV.SG_BASE_URL || 'http://localhost:8088';

// PoW 시도 상한. 난이도 16이면 기댓값 65,536회다.
export const SOLVE_LIMIT = Number(__ENV.SG_SOLVE_LIMIT || 1 << 22);

// ── 난수: 참가자별 결정적 스트림 ────────────────────────────────────────
//
// `SG_SEED` 를 주면 참가자마다 같은 난수열이 재생된다. 폴링 간격의 지터,
// 포인터 엔트로피, 탭 가시성이 반복 실행에서 같은 값을 낸다.
//
// **이것으로 실행이 결정적이 되지는 않는다.** 재실행 간 분산의 나머지는 서버
// 쪽에 있다 — 요청이 도착하는 순서, Redis/Kafka 왕복 시간, 스코어러의 창
// 경계가 어디에 떨어지는가. 시드는 분산의 원인 하나(클라이언트 리듬)를
// 제거해 나머지를 더 잘 보이게 할 뿐이다. 그래서 반복 측정을 대체하지 않는다.
//
// k6 는 VU 마다 독립된 JS 런타임을 주므로 아래 모듈 상태는 VU 별로 하나씩이다.
// 초기화를 미루는 이유는 init 문맥에서 `__VU` 가 아직 0 이기 때문이다.
export const SEED = __ENV.SG_SEED || '';

let vuRand = null;

/** fnv1a 는 시드 문자열을 32비트 정수로 접는다. */
function fnv1a(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/** mulberry32 — 상태 32비트짜리 균등 난수. 재현이 목적이지 암호 강도가 아니다. */
function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** rand 는 [0,1) 균등 난수다. SG_SEED 가 있으면 참가자별로 재현된다. */
export function rand() {
  if (vuRand === null) {
    // 시드가 없으면 매 실행 다른 스트림을 쓴다 — 기존 동작 그대로다.
    const material = SEED === '' ? `${Date.now()}-${__VU}` : `${SEED}-${__VU}`;
    vuRand = mulberry32(fnv1a(material));
  }
  return vuRand();
}

// ── §11 이 요구하는 지표 ────────────────────────────────────────────────
export const powSolveMs = new Trend('sg_pow_solve_ms', true);
export const powDifficulty = new Trend('sg_pow_difficulty');
export const joinFailed = new Rate('sg_join_failed');
export const waitSeconds = new Trend('sg_wait_seconds');
export const admitted = new Counter('sg_admitted');
export const ordered = new Counter('sg_ordered');

// isolated 는 조치(greylist/보류/차단)를 받은 비율이다. 봇에 대해서는 탐지율,
// 사람에 대해서는 오탐율(FPR)이 된다 — 같은 관측을 누구에게 적용하느냐의 차이다.
export const botIsolated = new Rate('sg_bot_isolated');
export const humanIsolated = new Rate('sg_human_isolated');
export const botAdmitted = new Counter('sg_bot_admitted');
export const humanAdmitted = new Counter('sg_human_admitted');

// ── 탐지와 입장의 경주 ──────────────────────────────────────────────────
//
// 조치 파이프라인은 누적 점수로 움직인다(불변식 3). 점수는 지수평활로 오르므로
// 격리에는 최소 몇십 초가 걸린다. 그 동안에도 입장은 계속 나간다. 그래서
// **먼저 입장해 버린 봇은 격리될 기회 자체를 얻지 못한다** — 입장한 VU 는
// heartbeat 을 멈추므로 스코어러가 더 볼 것이 없다.
//
// 이 셋이 그 경주를 숫자로 만든다. 봇의 입장 대기가 격리 지연보다 짧으면
// 탐지율의 상한은 탐지 정확도가 아니라 **입장 속도**가 정한다.
export const botWaitSeconds = new Trend('sg_bot_wait_seconds');
export const humanWaitSeconds = new Trend('sg_human_wait_seconds');
export const isolateSeconds = new Trend('sg_isolate_seconds');

// 추첨 구간에 들어간 참가자 비율.
//
// **이 값이 0 이면 §3.2 의 공정성 모델이 꺼진 채로 측정한 것이다.** EVENT_OPEN_AT 이
// 없으면 enqueue.lua 가 lottery_end=0 을 받아 전원을 FIFO 밴드에 넣는데, 아무것도
// 실패하지 않으므로 표만 봐서는 알 수 없다. 실제로 §11 측정을 한 번 그렇게 돌렸다
// (docs/ROADMAP.md 결함 6). 그래서 매 결과 파일에 이 비율을 남긴다.
export const lotterySegment = new Rate('sg_lottery_segment');

// ── 재검증(greylist 복귀) ───────────────────────────────────────────────
//
// greylist 는 종점이 아니라 검문소다(§4). 나가는 길이 있다는 것은 **누수 채널이
// 하나 열려 있다**는 뜻이기도 하다. 이 지표들이 그 채널의 크기를 잰다.
//
// 주의: 지금 구현된 재챌린지는 PoW 뿐이다. PoW 는 봇에게 CPU 비용일 뿐 못 넘는
// 벽이 아니므로, 봇의 통과율은 원래 100% 다. `SG_CAPTCHA_PROXY` 는 아직 구현되지
// 않은 CAPTCHA 다리(§4-L2 의 Turnstile 대체 경로)를 흉내 내어, 봇이 대행으로
// 그것까지 넘는 비율을 모형화한다.
export const botRechallenged = new Counter('sg_bot_rechallenged');
export const humanRechallenged = new Counter('sg_human_rechallenged');
export const rechallengeRestored = new Counter('sg_rechallenge_restored');
export const rechallengeExhausted = new Counter('sg_rechallenge_exhausted');
// 재검증을 받아 놓고 **풀지 못해 포기한** 횟수. 적응형 난이도가 재검증 자체를
// 비싸게 만드는 지점이 여기 드러난다. 봇팜은 지문을 공유하므로 의심도가 함께
// 올라, 한 마리가 걸릴 때마다 팜 전체의 재검증 비용이 2^bump 배씩 뛴다.
export const rechallengeAbandoned = new Counter('sg_rechallenge_abandoned');

// ── 입장 채널 회계 ──────────────────────────────────────────────────────
//
// **격리와 격리 유지는 다른 지표다.** §3.7 이 그것을 문장으로 말했다면 여기는
// 회계로 굳힌 것이다: 입장한 봇 한 마리는 반드시 어느 한 채널로 들어왔다.
//
//   door      — 격리됐다가 재검증(문)으로 나와 입장했다. §3.7 이 잰 누수.
//   unflagged — 한 번도 조치를 받지 않은 채 입장했다. §12-7 의 경주 + 미탐.
//
// 두 채널의 합이 입장 수보다 적으면 **아무도 세지 않은 경로가 있다**는 뜻이다
// (예: 점수가 스스로 내려와 열리는 restore_shard 경로로 나와 입장한 경우).
// 그 잔차를 0 이 아닌 채로 두면 표는 여전히 그럴듯하고, 새는 자리만 보이지 않는다.
// 그래서 잔차를 메우는 "기타" 버킷을 만들지 않는다 — 메우면 검사가 사라진다.
//
// unflagged 안에서 경주와 미탐은 **실행 중에는 갈라지지 않는다.** 입장한
// 클라이언트는 heartbeat 을 멈추므로 스코어러가 더 볼 것이 없고, 따라서 "늦게
// 잡혔을 사람"과 "영영 안 잡혔을 사람"이 같은 흔적을 남긴다. 그 둘을 가르는 것은
// admit 을 끄고 잰 천장(§3.5: 탐지율 100%)이고, 그건 다른 실행의 값이다.
export const botAdmitDoor = new Counter('sg_bot_admit_door');
export const botAdmitUnflagged = new Counter('sg_bot_admit_unflagged');
export const humanAdmitDoor = new Counter('sg_human_admit_door');
export const humanAdmitUnflagged = new Counter('sg_human_admit_unflagged');

// 복귀 후 **표시 순번**이 앞으로 갔는가 뒤로 갔는가.
//
// 이름을 조심해서 골랐다. 이건 "순번을 잃었는가"가 **아니다.** 보존되는 것은 ZSET
// 점수(줄에서의 자리)이고 그 불변식은 별도로 검증돼 있다
// (queue.TestRechallengeRestoresOriginalRank). 여기서 세는 것은 사용자가 화면에서
// 보는 숫자이고, 그 숫자는 정당하게 뒤로 갈 수 있다:
//
//   격리된 사람은 원 대기열에서 빠져 있다 → 그 사이 뒷사람이 보는 순번은 실제보다
//   **앞당겨져 있다** → 앞사람이 복귀하면 그 앞당김이 사라진다.
//
// 즉 backward 는 결함 지표가 아니라 UX 관측치다. `rank_lost` 같은 이름을 붙이면
// 다음 사람이 있지도 않은 버그를 찾아 나서고, 고치려다 진짜 불변식을 깬다.
// 기전은 queue.TestRestoredRankRisesWhenSomeoneAheadReturns 가 재현해 둔다.
export const rechallengeRankForward = new Counter('sg_rechallenge_rank_forward');
export const rechallengeRankBackward = new Counter('sg_rechallenge_rank_backward');

// 조치를 받은 상태들. 이 중 하나라도 관측되면 그 사용자는 격리된 것이다.
const ISOLATED_STATES = ['greylist', 'held', 'blocked'];

/** 16진 다이제스트의 앞쪽 연속 0 비트 수. */
function leadingZeroBits(hex) {
  let bits = 0;
  for (let i = 0; i < hex.length; i++) {
    const v = parseInt(hex[i], 16);
    if (v === 0) {
      bits += 4;
      continue;
    }
    // 4비트 안에서 남은 0의 개수.
    return bits + (v >= 8 ? 0 : v >= 4 ? 1 : v >= 2 ? 2 : 3);
  }
  return bits;
}

/**
 * solve 는 PoW 를 푼다: SHA256(nonce + "." + solution) 의 상위 difficulty 비트가 0.
 *
 * 이 루프가 봇의 비용이다. 부하 테스트에서는 k6 쪽 CPU 비용이기도 하므로,
 * 서버 처리량을 재려면 SG_POW_BASE_DIFFICULTY 를 낮춰 두고 돌린다(REPORT.md 참고).
 */
export function solve(nonce, difficulty) {
  const started = Date.now();
  for (let i = 0; i < SOLVE_LIMIT; i++) {
    if (leadingZeroBits(crypto.sha256(`${nonce}.${i}`, 'hex')) >= difficulty) {
      return { solution: String(i), ms: Date.now() - started };
    }
  }
  return null;
}

function headersFor(identity, extra) {
  const h = Object.assign(
    { 'Content-Type': 'application/json', 'X-Device-Fingerprint': identity.fp },
    extra || {},
  );
  // 게이트는 X-Forwarded-For 로 클라이언트 IP 를 추정한다(CDN 뒤라고 가정).
  // 큐 토큰이 IP 프리픽스에 묶이므로 한 참가자는 같은 값을 계속 써야 한다.
  if (identity.ip) h['X-Forwarded-For'] = identity.ip;
  if (identity.token) h.Authorization = `Bearer ${identity.token}`;
  return h;
}

/**
 * join 은 진입 전 과정을 수행한다: 챌린지 발급 → PoW → 큐 토큰.
 * identity 는 { fp, ip } 이며, 성공 시 token/shard/rank 가 채워져 돌아온다.
 */
export function join(identity) {
  const headers = headersFor(identity);

  const enter = http.post(`${BASE}/api/v1/queue/enter`, '{}', {
    headers,
    tags: { name: 'enter' },
  });
  if (enter.status !== 200) {
    joinFailed.add(1);
    return null;
  }

  const c = enter.json('challenge');
  powDifficulty.add(c.difficulty);

  const solved = solve(c.nonce, c.difficulty);
  if (!solved) {
    joinFailed.add(1);
    return null;
  }
  powSolveMs.add(solved.ms);

  const verify = http.post(
    `${BASE}/api/v1/challenge/verify`,
    JSON.stringify({ challenge: c, solution: solved.solution, solve_ms: solved.ms }),
    { headers, tags: { name: 'verify' } },
  );
  if (verify.status !== 200) {
    joinFailed.add(1);
    return null;
  }

  joinFailed.add(0);
  identity.token = verify.json('token');
  identity.shard = verify.json('shard');
  identity.rank = verify.json('rank');
  identity.segment = verify.json('segment');
  lotterySegment.add(identity.segment === 'lottery');
  identity.joinedAt = Date.now();
  return identity;
}

/** status 는 순번을 조회한다. 서버가 유일한 진실이므로 클라이언트는 계산하지 않는다. */
export function status(identity) {
  const res = http.get(`${BASE}/api/v1/queue/status`, {
    headers: headersFor(identity),
    tags: { name: 'status' },
  });
  if (res.status !== 200) return null;
  return res.json();
}

/** heartbeat 는 생존 신호와 행동 텔레메트리를 보낸다(§4-L4). */
export function heartbeat(identity, telemetry) {
  const res = http.post(
    `${BASE}/api/v1/queue/heartbeat`,
    JSON.stringify(telemetry || {}),
    { headers: headersFor(identity), tags: { name: 'heartbeat' } },
  );
  if (res.status !== 200) return null;
  return res.json();
}

/**
 * redeem 은 입장 토큰 교환을 시도한다.
 * 조치를 받은 사용자는 403 이고, 그때 code 가 held/blocked 다.
 */
export function redeem(identity) {
  const res = http.post(`${BASE}/api/v1/admission/redeem`, null, {
    headers: headersFor(identity),
    tags: { name: 'redeem' },
  });

  if (res.status === 403) {
    return { state: res.json('code') || 'forbidden' };
  }
  if (res.status !== 200) {
    return { state: 'error' };
  }

  const body = res.json();
  // challenge_required 는 오류가 아니라 "재검증을 받아 오라"다(§4).
  // 서버가 200 으로 주는 이유와 같은 이유로 여기서도 실패로 세지 않는다.
  if (body.status === 'challenge_required') {
    return { state: 'challenge_required', rank: body.rank, budget: body.budget };
  }
  if (body.status !== 'admitted') {
    return { state: body.status, rank: body.rank, budget: body.budget };
  }

  admitted.add(1);
  if (body.waited_ms > 0) waitSeconds.add(body.waited_ms / 1000);
  // waitedMs 를 돌려주는 이유: 유형별로 나눠 기록해야 "봇이 사람보다 먼저
  // 들어가는가"를 볼 수 있는데, 여기서는 호출자가 봇인지 사람인지 모른다.
  return { state: 'admitted', entryToken: body.entry_token, waitedMs: body.waited_ms };
}

/**
 * rechallenge 는 greylist 에서 나오는 문을 두드린다: 발급 → PoW → 제출.
 *
 * 반환: null(발급 거절) 또는 { outcome, state, rank, attemptsLeft }.
 *   outcome: restored | exhausted | noop | no_rank
 */
export function rechallenge(identity) {
  const headers = headersFor(identity);

  const issued = http.post(`${BASE}/api/v1/challenge/reissue`, '{}', {
    headers,
    tags: { name: 'reissue' },
  });
  if (issued.status !== 200) return null;

  const c = issued.json('challenge');
  powDifficulty.add(c.difficulty);

  const solved = solve(c.nonce, c.difficulty);
  if (!solved) {
    // SOLVE_LIMIT 안에서 못 풀었다. 실제 봇팜이라면 더 돌려서 풀 수 있으므로
    // 이 값은 "불가능"이 아니라 **시뮬레이터가 포기한 지점**이다(REPORT §5).
    rechallengeAbandoned.add(1);
    return null;
  }
  powSolveMs.add(solved.ms);

  const res = http.post(
    `${BASE}/api/v1/challenge/reverify`,
    JSON.stringify({ challenge: c, solution: solved.solution, solve_ms: solved.ms }),
    { headers, tags: { name: 'reverify' } },
  );
  if (res.status !== 200) return null;

  const outcome = res.json('outcome');
  if (outcome === 'restored') rechallengeRestored.add(1);
  if (outcome === 'exhausted') rechallengeExhausted.add(1);
  return {
    outcome,
    state: res.json('status'),
    rank: res.json('rank'),
    attemptsLeft: res.json('attempts_left'),
  };
}

/** order 는 mock shop 에서 구매한다. 멱등키는 필수다(불변식 4). */
export function order(identity, entryToken, accountID) {
  const res = http.post(
    `${BASE}/api/v1/orders`,
    JSON.stringify({ account_id: accountID, sku: 'demo-ticket', qty: 1 }),
    {
      headers: headersFor(identity, {
        'X-Entry-Token': entryToken,
        'Idempotency-Key': `${accountID}-1`,
      }),
      tags: { name: 'order' },
    },
  );
  if (res.status === 201 || res.status === 200) {
    ordered.add(1);
    return true;
  }
  return false;
}

/** isIsolated 는 관측된 상태가 조치를 받은 상태인지 본다. */
export function isIsolated(state, shard) {
  if (state && ISOLATED_STATES.indexOf(state) >= 0) return true;
  // greylist 샤드로 옮겨졌다면 샤드 이름이 g 로 시작한다.
  return !!shard && shard.charAt(0) === 'g';
}

/** vuID 는 시나리오 안에서 겹치지 않는 참가자 번호를 만든다. */
export function vuID(prefix) {
  return `${prefix}-${__VU}-${__ITER}`;
}

/** sleepJitter 는 ±pct 만큼 흔들린 초 단위 대기 시간을 만든다. */
export function jitter(seconds, pct) {
  const spread = seconds * pct;
  return seconds - spread + rand() * spread * 2;
}
