// 참가자 행동 모델 (DESIGN.md §11).
//
// 프로토콜은 네 유형이 완전히 같다. 다른 것은 **리듬과 정체성**뿐이다 —
// heartbeat 간격의 분산, 지문의 공유 여부, IP 대역의 분포. 봇을 가려내는 것이
// "무엇을 하는가"가 아니라 "어떤 리듬으로 하는가"라는 §4-L5 의 전제를 그대로 옮겼다.

import { sleep } from 'k6';
import * as sg from './shardgate.js';

// 대기실에 머무는 최대 시간. 이 안에 입장하지 못하면 포기한 것으로 센다.
export const PATIENCE_SEC = Number(__ENV.SG_PATIENCE || 240);
const POLL_SEC = Number(__ENV.SG_POLL || 5);

/**
 * SG_CAPTCHA_PROXY — 봇이 재검증을 통과하는 비율(0~1).
 *
 * greylist 에 출구가 생기면(§4 재챌린지) 누수 채널이 하나 열린다. 그 채널의 크기가
 * 곧 "대행 업체를 쓰는 봇이 얼마나 돌아 나오는가"이므로, 그것을 파라미터로 뺐다.
 *
 * **모형화하는 대상은 아직 구현되지 않은 CAPTCHA 다리다.** 지금 구현된 재챌린지는
 * PoW 뿐이고 PoW 는 봇에게 CPU 비용일 뿐이라, 실제 구현에서 봇의 통과율은 1.0 이다.
 * 즉 이 값을 1.0 으로 두고 잰 것이 **현 구현의 실제 노출**이고, 0.5·0.0 은 CAPTCHA
 * (§4-L2 의 Turnstile 대체 경로)를 붙였을 때를 가정한 값이다.
 *
 * 사람은 이 값과 무관하게 항상 재검증을 시도한다 — 오탐으로 걸린 사람이 돌아 나오는
 * 것이 이 문이 존재하는 이유이고, 그 비율이 곧 오탐의 회복률이다.
 */
export const CAPTCHA_PROXY = Number(__ENV.SG_CAPTCHA_PROXY || 0);

// 봇팜은 한 기기 이미지를 복제해 돌린다 — 지문이 하나로 뭉친다.
const FARM_FP = 'fp-farm-image-a';
const PROXY_FP = 'fp-farm-image-b';
// naive/mimic 은 같은 서버에서 돌아 대역도 하나다.
const FARM_NET = '203.0.113';

// 회사 표준 이미지 + 사무실 NAT. 봇팜과 같은 모양의 신호를 내지만 사람이다.
const OFFICE_FP = 'fp-corp-golden-image';
const OFFICE_NET = '198.51.9';

/**
 * live 는 참가자 한 명의 대기실 체류다.
 *
 * profile: { fp, ip, wait(), telemetry(), bot }
 * 반환값은 없고, 격리 여부와 입장 여부를 지표에 남긴다.
 */
function live(profile) {
  const id = sg.join({ fp: profile.fp, ip: profile.ip });
  if (!id) {
    sleep(1);
    return;
  }

  const account = `acct-${profile.fp}-${__VU}`;
  const joinedAt = Date.now();
  const deadline = joinedAt + PATIENCE_SEC * 1000;
  const isolatedRate = profile.bot ? sg.botIsolated : sg.humanIsolated;
  const admittedCount = profile.bot ? sg.botAdmitted : sg.humanAdmitted;
  const waitTrend = profile.bot ? sg.botWaitSeconds : sg.humanWaitSeconds;
  const rechallengedCount = profile.bot ? sg.botRechallenged : sg.humanRechallenged;
  const admitDoor = profile.bot ? sg.botAdmitDoor : sg.humanAdmitDoor;
  const admitUnflagged = profile.bot ? sg.botAdmitUnflagged : sg.humanAdmitUnflagged;
  let isolated = false;
  let restored = 0;

  // 사람은 언제나 재검증을 시도한다. 봇은 대행 통과율만큼만 시도한다.
  // 판정을 참가자마다 한 번 고정하는 이유: 매 폴링마다 새로 뽑으면 폴링 횟수가
  // 많은 참가자일수록 통과 확률이 올라가, 재는 것이 대행 통과율이 아니라
  // 인내심이 된다.
  const willRechallenge = profile.bot ? sg.rand() < CAPTCHA_PROXY : true;

  // 격리를 처음 관측한 순간을 기록한다. 조치가 늦으면 그 사이에 입장이
  // 나가 버리므로, 이 값과 입장 대기 시간의 대소가 탐지율의 상한을 정한다.
  function markIsolated() {
    if (isolated) return;
    isolated = true;
    sg.isolateSeconds.add((Date.now() - joinedAt) / 1000);
  }

  // 격리 직전에 서 있던 순번. 복귀 후 이 값보다 뒤로 밀렸다면 재검증을 통과하고도
  // 대가를 치른 것이다 — 그러면 이 문은 오탐을 회복시키는 장치가 아니게 된다.
  let rankBeforeIsolation = -1;

  while (Date.now() < deadline) {
    const snap = sg.status(id);
    if (snap && snap.state === 'waiting' && snap.rank >= 0) rankBeforeIsolation = snap.rank;
    if (snap && sg.isIsolated(snap.state, snap.shard)) markIsolated();

    const beat = sg.heartbeat(id, profile.telemetry());
    if (beat && sg.isIsolated(beat.state, id.shard)) markIsolated();

    const res = sg.redeem(id);
    if (res.state === 'admitted') {
      admittedCount.add(1);
      waitTrend.add((Date.now() - joinedAt) / 1000);
      // 어느 문으로 들어왔는지 센다. 셋 중 어디에도 해당하지 않으면 **아무것도
      // 세지 않는다** — 그 차이가 잔차이고, 잔차가 곧 미계측 채널의 크기다.
      // 여기에 else 를 붙여 기타로 몰면 항등식이 항상 성립해 검사가 사라진다.
      if (restored > 0) admitDoor.add(1);
      else if (!isolated) admitUnflagged.add(1);
      sg.order(id, res.entryToken, account);
      break;
    }
    if (sg.isIsolated(res.state, id.shard)) markIsolated();
    // 차단·퇴출은 되돌아오지 않는다. 여기서 더 기다리는 것은 관측 낭비다.
    if (res.state === 'blocked' || res.state === 'evicted') break;

    // 재검증 대기다. 격리는 이미 기록됐으므로(markIsolated), 여기서 돌아 나와도
    // 탐지율의 분자는 줄지 않는다 — 재검증 누수는 **입장 수**로 드러난다.
    if (res.state === 'challenge_required' && willRechallenge) {
      const back = sg.rechallenge(id);
      if (back) {
        rechallengedCount.add(1);
        if (back.outcome === 'restored') {
          restored++;
          // 복귀 순번을 서버가 돌려주는 값(orig_rank, ZSET 점수)이 아니라 **다시
          // 조회한 순번**으로 확인한다. 둘은 단위가 다르다 — 하나는 밴드가 실린
          // 점수이고 하나는 ZRANK 다. 서버가 돌려준 값을 그대로 비교하면 항상
          // 어긋나고, 그 어긋남을 "손해"로 세면 표가 거짓말을 한다.
          const after = sg.status(id);
          if (after && after.rank >= 0 && rankBeforeIsolation >= 0) {
            if (after.rank > rankBeforeIsolation) sg.rechallengeRankBackward.add(1);
            else sg.rechallengeRankForward.add(1);
          }
        }
      }
    }

    sleep(profile.wait());
  }

  isolatedRate.add(isolated ? 1 : 0);
}

/**
 * human — 사람의 특징은 **불규칙성**이다. 폴링 간격에도 heartbeat 간격에도
 * 지터가 있고, 포인터 이벤트가 있으며, 탭이 가려지는 순간도 있다.
 * 여기서 지터를 없애면 시나리오 자체가 봇이 되어 FPR 측정이 무의미해진다.
 */
export function human() {
  live({
    bot: false,
    fp: `fp-human-${sg.vuID('h')}`,
    // 사람은 여러 대역에 흩어져 있다.
    ip: `198.51.${__VU % 200}.${(__ITER % 200) + 1}`,
    wait: () => sg.jitter(POLL_SEC, 0.3),
    telemetry: () => ({
      pointer_entropy: 0.4 + sg.rand() * 0.5,
      visible: sg.rand() > 0.1,
      events: ['visibilitychange', 'pointermove', 'scroll'],
    }),
  });
}

/** naive — 숨기는 것이 없다. 간격의 분산이 0이고 지문·대역이 뭉친다. */
export function naive() {
  live({
    bot: true,
    fp: FARM_FP,
    ip: `${FARM_NET}.10`,
    wait: () => POLL_SEC,
    telemetry: () => ({ visible: true }),
  });
}

/**
 * mimic — 타이밍만 사람처럼 꾸민다. 위조가 공짜인 신호는 이렇게 지워지고,
 * 남는 것은 지문·대역·PoW 처럼 **돈이 드는** 신호뿐이다. 그게 요점이다.
 */
export function mimic() {
  live({
    bot: true,
    fp: FARM_FP,
    ip: `${FARM_NET}.11`,
    wait: () => sg.jitter(POLL_SEC, 0.3),
    telemetry: () => ({
      pointer_entropy: 0.4 + sg.rand() * 0.4,
      visible: true,
      events: ['pointermove', 'scroll'],
    }),
  });
}

/**
 * clumsy — **오탐으로 걸리는 사람.**
 *
 * 문(§4 재챌린지)이 존재하는 이유는 이 사람 때문인데, 지금까지의 측정에서는
 * 오탐율이 0% 라 그 이유가 한 번도 밟히지 않았다. 사람이 걸리지 않으면 "걸린
 * 사람이 자기 순번을 잃지 않고 돌아온다"는 성질은 선언으로만 남는다.
 *
 * 그래서 **걸릴 만한 사람**을 만든다. 지어낸 사람이 아니라 실제로 흔한 조합이다:
 *
 *   - 회사 표준 이미지 단말 → 브라우저 지문이 동료들과 같다(지문 0.25)
 *   - 사무실 NAT → 같은 /24 에서 수십 명이 들어온다(대역 0.15)
 *   - 자동 새로고침 확장/보조기술 → 폴링 간격의 지터가 거의 없다(규칙성 0.25)
 *
 * 이 사람은 **행동이 아니라 정체성 때문에** 걸린다. 탐지기 입장에서 봇팜과
 * 구분할 근거가 없고(§12-1), 그래서 이 사람을 지켜 주는 것은 임계값이 아니라
 * 되돌릴 수 있는 조치다. 임계값을 낮추면 이 사람이 더 걸리고, 높이면 봇이 샌다 —
 * 사다리가 있는 이유가 그 맞교환을 안 해도 되게 하는 것이다.
 *
 * 사람 코호트로 세므로 이 페르소나를 켜면 오탐율이 0 이 아니게 된다. 기본값은
 * 0명이다 — 켜는 순간 §3.7 세 팔과 모집단이 달라져 비교가 성립하지 않는다.
 */
export function clumsy() {
  live({
    bot: false,
    fp: OFFICE_FP,
    ip: `${OFFICE_NET}.${(__VU % 200) + 1}`,
    // 사람이지만 리듬이 규칙적이다. 0 이 아니라 작은 지터를 주는 이유는,
    // 0 으로 두면 이 사람이 naive 봇과 **완전히** 같아져 페르소나가 아니라
    // 라벨만 바꾼 봇이 되기 때문이다. 그러면 오탐이 아니라 오분류를 재게 된다.
    wait: () => sg.jitter(POLL_SEC, 0.05),
    telemetry: () => ({
      // 보조기술 사용자는 포인터를 거의 쓰지 않는다. 엔트로피 신호가 낮다.
      pointer_entropy: 0.05 + sg.rand() * 0.1,
      visible: true,
      events: ['visibilitychange'],
    }),
  });
}

/**
 * distributed — 프록시로 대역을 흩는다. IP 히스토그램 신호는 사라지지만
 * 같은 자동화 이미지를 쓰는 한 지문은 하나로 남는다.
 */
export function distributed() {
  live({
    bot: true,
    fp: PROXY_FP,
    ip: `192.0.${__VU % 250}.${(__VU % 200) + 1}`,
    wait: () => POLL_SEC,
    telemetry: () => ({ visible: true }),
  });
}
