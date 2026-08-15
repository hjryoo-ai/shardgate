// 혼합 시나리오 — Phase 5 완료 기준인 §11 지표 리포트를 산출한다.
//
// 정상 사용자 N명과 봇 M대를 **같은 이벤트에 동시에** 넣는다. 따로 돌리면
// 나오지 않는 값이 여기서 나온다: 샤드 단위 이상탐지는 "주변과 비교해 이상한가"를
// 묻기 때문에, 비교할 정상 모집단이 함께 있어야 신호가 제대로 선다(§4-L5).
//
// 핵심 그래프(§11 마지막 항목)는 **봇 비율을 높여도 정상 사용자의 입장 성공률이
// 유지되는가**다. 같은 스크립트를 비율만 바꿔 여러 번 돌려 표를 만든다:
//
//   for r in 0.1 0.3 0.5 0.7; do
//     k6 run -e SG_BOT_RATIO=$r -e SG_TOTAL=200 loadtest/k6/mixed.js
//   done
//
// 측정 전에 스택 설정을 맞춰야 한다. 자세한 이유는 docs/REPORT.md §3 의
// "부하 테스트를 돌릴 때의 주의" 를 볼 것. 요약하면 (a) 입장 슬롯이 참가자보다
// 적어야 경쟁이 측정되고, (b) PoW 난이도가 높으면 k6 가 CPU 에 묶여 heartbeat
// 규칙성 신호 자체가 사라진다 — 봇과 사람이 똑같이 불규칙해진다.
//
// 결과는 stdout 요약과 loadtest/results/*.json 양쪽에 남는다.

import { human, clumsy, naive, mimic, distributed, PATIENCE_SEC, CAPTCHA_PROXY } from './lib/personas.js';

const TOTAL = Number(__ENV.SG_TOTAL || 200);
const BOT_RATIO = Number(__ENV.SG_BOT_RATIO || 0.2);

// SG_CLUMSY — 오탐으로 걸릴 만한 사람의 수(§4 재챌린지의 존재 이유).
//
// **기본값 0 은 의도다.** 켜는 순간 사람 코호트의 구성이 달라져 §3.7 세 팔과
// 모집단이 같지 않게 되고, 오탐율·사람 입장률을 그 표와 나란히 읽을 수 없다.
// 이 팔은 "문이 사람에게 실제로 작동하는가"를 밟아 보는 별도 측정이다.
const CLUMSY = Number(__ENV.SG_CLUMSY || 0);

// 시나리오 길이는 **인내 시간에서 유도한다.** 직접 주는 값이 아니다.
//
// maxDuration 이 인내 시간보다 짧으면, 아직 줄을 서 있는 VU 가 반복 도중에 잘린다.
// 잘린 VU 는 격리 여부(sg_*_isolated)를 기록하지 못하므로 **분모에서 사라진다.**
// 남는 것은 스스로 반복을 끝낸 사람들 — 즉 입장에 성공한 사람들뿐이다.
// 그러면 입장 성공률이 정의상 100% 로 나오고, 탐지율은 "입장까지 성공한 봇" 만
// 세게 된다. 실제로 이 조합(인내 300s / 길이 4m)으로 한 번 측정해서
// 사람 100% · 봇 100% · 탐지율 0% 라는 무의미한 표를 얻었다.
//
// 여유분은 진입 비용(챌린지 발급 + PoW + 토큰) 과 gracefulStop 몫이다.
const SLACK_SEC = 90;
const DURATION = __ENV.SG_DURATION || `${PATIENCE_SEC + SLACK_SEC}s`;

// SG_DURATION 을 직접 주면 위 유도를 우회한다. 편향은 조용히 통과하는 종류라
// 여기서 막는다 — 잘못된 표가 나오는 것보다 시작하지 않는 편이 낫다.
function seconds(d) {
  let total = 0;
  const re = /(\d+(?:\.\d+)?)(ms|s|m|h)/g;
  const unit = { ms: 0.001, s: 1, m: 60, h: 3600 };
  for (let m = re.exec(d); m !== null; m = re.exec(d)) total += Number(m[1]) * unit[m[2]];
  return total;
}
if (seconds(DURATION) <= PATIENCE_SEC) {
  throw new Error(
    `SG_DURATION(${DURATION}) 이 SG_PATIENCE(${PATIENCE_SEC}s) 보다 길어야 한다. ` +
      '짧으면 아직 대기 중인 VU 가 잘려 지표 분모에서 사라지고, 입장 성공률이 항상 100% 로 나온다.',
  );
}

const BOTS = Math.max(3, Math.round(TOTAL * BOT_RATIO));
const HUMANS = Math.max(1, TOTAL - BOTS - CLUMSY);
const PER_TYPE = Math.max(1, Math.floor(BOTS / 3));

const OUT = __ENV.SG_OUT || `loadtest/results/mixed-ratio-${BOT_RATIO}.json`;

// 참가자 한 명 = VU 하나 = 반복 한 번.
//
// constant-vus 를 쓰면 입장한 VU 가 곧바로 새 사람이 되어 다시 줄을 선다. 그러면
// "몇 명 중 몇 명이 들어갔나"의 분모가 실행 시간에 따라 늘어나 입장 성공률이
// 무조건 100% 로 수렴한다 — §11 의 핵심 지표가 측정 불가능해진다.
// per-vu-iterations 는 분모를 참가자 수로 고정한다.
//
// 전원이 t=0 에 몰려드는 것도 의도다. 티켓 오픈 순간이 정확히 그 모양이고,
// 이 시스템이 막으려는 상황도 그것이다.
function person(exec, vus) {
  return {
    executor: 'per-vu-iterations',
    vus,
    iterations: 1,
    maxDuration: DURATION,
    exec,
    startTime: '0s',
  };
}

export const options = {
  // k6 기본값에는 p(99)가 없다. §11 이 요구하는 지표가 P99 이므로 여기서 켠다 —
  // 켜지 않으면 handleSummary 가 p(99)를 찾지 못해 리포트가 조용히 n/a 로 채워진다.
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: Object.assign(
    {
      humans: person('human', HUMANS),
      naive: person('naive', PER_TYPE),
      mimic: person('mimic', PER_TYPE),
      distributed: person('distributed', PER_TYPE),
    },
    // 0명이면 시나리오 자체를 만들지 않는다. vus:0 은 k6 가 거부하고, 무엇보다
    // 빈 시나리오라도 있으면 VU 번호가 밀려 SG_SEED 로 짝지은 비교가 깨진다.
    CLUMSY > 0 ? { clumsy: person('clumsy', CLUMSY) } : {},
  ),
  thresholds: {
    // 탐지율과 오탐율. 이 둘이 §11 리포트의 본문이다.
    sg_bot_isolated: ['rate>0.8'],
    // 오탐 임계는 clumsy 팔에서 **일부러** 깨진다(SG_CLUMSY). 그 팔의 목적이
    // 오탐을 만들어 문을 밟는 것이므로, 켜 두면 빨간 줄이 "시스템이 나빠졌다"로
    // 읽힌다. 그래서 팔에 맞춰 기대치를 옮긴다 — 끄면 예전 값 그대로다.
    sg_human_isolated: [CLUMSY > 0 ? `rate<${(CLUMSY / (HUMANS + CLUMSY)) * 1.5}` : 'rate<0.02'],
    sg_join_failed: ['rate<0.02'],
    // **부하 생성기가 PoW 에 묶이면 표 전체가 무효가 된다.**
    // k6 의 JS 는 VU 당 단일 스레드라, 풀이가 길어지면 그 VU 의 heartbeat 이 늦어진다.
    // 그 지연은 봇에게만 생기는 것이 아니라 CPU 를 나눠 쓰는 전 참가자에게 생기고,
    // 규칙성·상호상관 신호가 봇과 사람 모두에게서 사라져 **탐지기가 눈이 먼다.**
    // 실제로 재챌린지 난이도 상향을 켜고 재다가 탐지율 96% → 12% 를 얻었는데,
    // 그건 문이 샌 것이 아니라 부하 생성기가 느려진 것이었다(REPORT §3.7).
    // 기본 난이도 8비트면 1~2ms 다. 100ms 는 두 자릿수의 여유다.
    sg_pow_solve_ms: ['avg<100'],
    http_req_failed: ['rate<0.05'],
    'http_req_duration{name:redeem}': ['p(99)<2000'],

    // 엔드포인트별 실패율. 값이 아니라 **존재**가 목적인 선언이다 —
    // k6 는 threshold 로 선언되지 않은 태그 조합을 요약에 넣지 않으므로,
    // 이렇게 두지 않으면 handleSummary 에서 어디가 실패했는지 볼 수 없다.
    // 한 번 전체 실패율 32% 를 보고 원인을 찾지 못한 적이 있다.
    'http_req_failed{name:enter}': ['rate<1'],
    'http_req_failed{name:verify}': ['rate<1'],
    'http_req_failed{name:status}': ['rate<1'],
    'http_req_failed{name:heartbeat}': ['rate<1'],
    'http_req_failed{name:redeem}': ['rate<1'],
    'http_req_failed{name:order}': ['rate<1'],
    // 재검증 경로도 같은 이유로 선언한다. 라우트가 없으면 404 가 여기 뜬다.
    'http_req_failed{name:reissue}': ['rate<1'],
    'http_req_failed{name:reverify}': ['rate<1'],
  },
};

export { human, clumsy, naive, mimic, distributed };

export default human;

// ── 리포트 ──────────────────────────────────────────────────────────────

const ENDPOINTS = ['enter', 'verify', 'status', 'heartbeat', 'redeem', 'order', 'reissue', 'reverify'];

function metric(data, name, field) {
  const m = data.metrics[name];
  if (!m || !m.values || m.values[field] === undefined) return null;
  return m.values[field];
}

function pct(v) {
  return v === null ? 'n/a' : `${(v * 100).toFixed(1)}%`;
}

function num(v, digits) {
  return v === null ? 'n/a' : v.toFixed(digits === undefined ? 1 : digits);
}

/** 참가자 수는 Rate 의 관측 횟수(passes + fails)다. 체류를 끝낸 사람만 센다. */
function observed(data, name) {
  const p = metric(data, name, 'passes');
  const f = metric(data, name, 'fails');
  if (p === null && f === null) return null;
  return (p || 0) + (f || 0);
}

/**
 * admitChannels 는 한 코호트의 입장을 채널로 분해하고 잔차를 남긴다.
 *
 * 잔차 = 입장 − (문 + 미플래그). 두 채널은 입장 시점에 서로 배타적으로 세므로
 * 정상이면 0 이고, 0 이 아니라는 것은 분류기가 모르는 경로로 입장했다는 뜻이다.
 */
function admitChannels(data, cohort, total) {
  const door = metric(data, `sg_${cohort}_admit_door`, 'count') || 0;
  const unflagged = metric(data, `sg_${cohort}_admit_unflagged`, 'count') || 0;
  return { admitted: total, door, unflagged, unaccounted: total - door - unflagged };
}

export function handleSummary(data) {
  const humansSeen = observed(data, 'sg_human_isolated');
  const botsSeen = observed(data, 'sg_bot_isolated');
  const humanAdmitted = metric(data, 'sg_human_admitted', 'count') || 0;
  const botAdmitted = metric(data, 'sg_bot_admitted', 'count') || 0;

  const recall = metric(data, 'sg_bot_isolated', 'rate');
  const fpr = metric(data, 'sg_human_isolated', 'rate');
  // 핵심 지표: 봇이 섞여도 사람이 들어가는가.
  const humanSuccess = humansSeen ? humanAdmitted / humansSeen : null;
  const botSuccess = botsSeen ? botAdmitted / botsSeen : null;

  // 이 값이 0 이면 추첨 구간이 꺼진 채로 잰 것이다(EVENT_OPEN_AT 누락).
  // 아무것도 실패하지 않으므로 표에 남기지 않으면 알아챌 방법이 없다.
  const lotteryRate = metric(data, 'sg_lottery_segment', 'rate');

  const summary = {
    bot_ratio: BOT_RATIO,
    planned: { total: TOTAL, humans: HUMANS, clumsy: CLUMSY, bots: BOTS },
    observed: { humans: humansSeen, bots: botsSeen },
    fairness: { lottery_segment_rate: lotteryRate },
    detection: { recall, fpr },
    admission: {
      human_success_rate: humanSuccess,
      bot_success_rate: botSuccess,
      human_admitted: humanAdmitted,
      bot_admitted: botAdmitted,
      wait_p99_sec: metric(data, 'sg_wait_seconds', 'p(99)'),
      wait_avg_sec: metric(data, 'sg_wait_seconds', 'avg'),
      orders: metric(data, 'sg_ordered', 'count') || 0,
    },
    // 재검증 누수 채널(§4 재챌린지). greylist 에 출구가 생기면 그 문으로 봇도
    // 나온다 — 얼마나 나오는지가 이 블록이다. captcha_proxy 는 아직 구현되지
    // 않은 CAPTCHA 다리를 봇이 대행으로 넘는 비율의 가정값이고, 1.0 이 곧
    // "PoW 만 있는 현 구현의 실제 노출"이다.
    rechallenge: {
      captcha_proxy: CAPTCHA_PROXY,
      bot_attempts: metric(data, 'sg_bot_rechallenged', 'count') || 0,
      human_attempts: metric(data, 'sg_human_rechallenged', 'count') || 0,
      restored: metric(data, 'sg_rechallenge_restored', 'count') || 0,
      exhausted: metric(data, 'sg_rechallenge_exhausted', 'count') || 0,
      // 난이도를 못 이겨 포기한 횟수. 누수 채널을 좁히는 것이 대행 통과율이
      // 아니라 적응형 난이도라는 사실이 이 값에서 보인다.
      abandoned: metric(data, 'sg_rechallenge_abandoned', 'count') || 0,
      // 복귀 후 **표시 순번**의 방향. 결함 지표가 아니다 — 격리된 사람이 대기열에서
      // 빠져 있는 동안 뒷사람이 보는 순번은 앞당겨져 있고, 앞사람이 복귀하면 그
      // 앞당김이 사라진다. 보존되는 것은 ZSET 점수이고 그 불변식은 별도 검증이다.
      rank_forward: metric(data, 'sg_rechallenge_rank_forward', 'count') || 0,
      rank_backward: metric(data, 'sg_rechallenge_rank_backward', 'count') || 0,
    },
    // 입장 채널 분해 — 회계로 굳힌 §3.7 의 교정("격리와 격리 유지는 다른 지표다").
    //
    //   입장 = 문(재검증으로 나와 입장) + 미플래그(경주 + 미탐)
    //
    // 잔차가 0 이 아니면 **아무도 세지 않은 채널이 있다**는 뜻이다. 예컨대 점수가
    // 스스로 내려와 열리는 restore_shard 경로로 나온 참가자는 셋 중 어디에도
    // 해당하지 않는다. 그 경우 표는 여전히 그럴듯하고 새는 자리만 보이지 않으므로,
    // sweep.sh 가 이 값을 검사해 0 이 아니면 실행을 표에 넣지 않는다.
    //
    // 미플래그 안에서 경주와 미탐은 실행 중에 갈라지지 않는다 — 입장한 클라이언트는
    // heartbeat 을 멈춰 스코어러가 더 볼 것이 없기 때문이다. 그 둘을 가르는 값은
    // admit 을 끄고 잰 천장(§3.5: 탐지율 100%)이고, 그건 다른 실행의 값이다.
    channels: {
      bot: admitChannels(data, 'bot', botAdmitted),
      human: admitChannels(data, 'human', humanAdmitted),
    },
    // 탐지가 입장을 따라잡는가. 봇의 입장이 격리보다 빠르면 탐지율의 상한은
    // 탐지 정확도가 아니라 입장 속도가 정한다.
    race: {
      human_wait_med_sec: metric(data, 'sg_human_wait_seconds', 'med'),
      bot_wait_med_sec: metric(data, 'sg_bot_wait_seconds', 'med'),
      isolate_med_sec: metric(data, 'sg_isolate_seconds', 'med'),
      isolate_p90_sec: metric(data, 'sg_isolate_seconds', 'p(90)'),
    },
    cost: {
      pow_solve_avg_ms: metric(data, 'sg_pow_solve_ms', 'avg'),
      pow_solve_p99_ms: metric(data, 'sg_pow_solve_ms', 'p(99)'),
      pow_difficulty_avg: metric(data, 'sg_pow_difficulty', 'avg'),
    },
    // load_generator_cpu_bound 가 true 면 이 파일의 탐지 지표는 무효다.
    load_generator_cpu_bound: null,
    http: {
      requests: metric(data, 'http_reqs', 'count'),
      rps: metric(data, 'http_reqs', 'rate'),
      failed_rate: metric(data, 'http_req_failed', 'rate'),
      p99_ms: metric(data, 'http_req_duration', 'p(99)'),
      failed_by_endpoint: ENDPOINTS.reduce((acc, name) => {
        acc[name] = metric(data, `http_req_failed{name:${name}}`, 'rate');
        return acc;
      }, {}),
    },
  };

  // 부하 생성기가 CPU 에 묶였는지. 묶였다면 아래 표는 시스템이 아니라 k6 를 잰 것이다.
  const powAvg = summary.cost.pow_solve_avg_ms;
  const cpuBound = powAvg !== null && powAvg > 100;

  summary.load_generator_cpu_bound = cpuBound;

  const botCh = summary.channels.bot;
  const humanCh = summary.channels.human;

  const lines = [
    '',
    '════ ShardGate §11 지표 리포트 ══════════════════════════════',
    `봇 비율          ${pct(BOT_RATIO)}  (계획 사람 ${HUMANS}${CLUMSY ? ` + 서투른 사람 ${CLUMSY}` : ''} / 봇 ${BOTS})`,
    `관측 인원        사람 ${humansSeen === null ? 'n/a' : humansSeen} / 봇 ${botsSeen === null ? 'n/a' : botsSeen}`,
    `추첨 구간 진입   ${pct(lotteryRate)}${lotteryRate ? '' : '   ← 0% 면 EVENT_OPEN_AT 이 없어 §3.2 가 꺼진 것이다'}`,
    '',
    `탐지율(recall)   ${pct(recall)}      격리된 봇 / 전체 봇`,
    `오탐율(FPR)      ${pct(fpr)}      격리된 사람 / 전체 사람`,
    '',
    `사람 입장 성공률 ${pct(humanSuccess)}   ← 봇 비율을 높여도 유지돼야 하는 값`,
    `봇  입장 성공률  ${pct(botSuccess)}`,
    `주문 성사        ${summary.admission.orders}건`,
    `대기 시간        평균 ${num(summary.admission.wait_avg_sec)}s / P99 ${num(summary.admission.wait_p99_sec)}s`,
    '',
    `탐지 vs 입장     입장 중앙값 사람 ${num(summary.race.human_wait_med_sec)}s / 봇 ${num(summary.race.bot_wait_med_sec)}s` +
      ` · 격리 중앙값 ${num(summary.race.isolate_med_sec)}s (P90 ${num(summary.race.isolate_p90_sec)}s)`,
    '',
    `재검증           대행 통과율 ${pct(CAPTCHA_PROXY)} · 복귀 ${summary.rechallenge.restored}건 · 소진 ${summary.rechallenge.exhausted}건` +
      ` · 난이도로 포기 ${summary.rechallenge.abandoned}건` +
      ` (시도 사람 ${summary.rechallenge.human_attempts} / 봇 ${summary.rechallenge.bot_attempts})`,
    `복귀 후 표시순번  앞으로/그대로 ${summary.rechallenge.rank_forward}건 · 뒤로 ${summary.rechallenge.rank_backward}건` +
      `${summary.rechallenge.rank_backward ? '  (앞사람이 함께 복귀한 몫 — 줄에서의 자리는 보존된다)' : ''}`,
    '',
    `입장 채널  봇   ${botCh.admitted}명 = 문 ${botCh.door} + 미플래그 ${botCh.unflagged}` +
      `${botCh.unaccounted ? ` + 미계측 ${botCh.unaccounted} ← 잔차` : ''}`,
    `           사람 ${humanCh.admitted}명 = 문 ${humanCh.door} + 미플래그 ${humanCh.unflagged}` +
      `${humanCh.unaccounted ? ` + 미계측 ${humanCh.unaccounted} ← 잔차` : ''}`,
    '',
    `PoW 비용         평균 ${num(summary.cost.pow_solve_avg_ms, 0)}ms / P99 ${num(summary.cost.pow_solve_p99_ms, 0)}ms` +
      ` (난이도 평균 ${num(summary.cost.pow_difficulty_avg)})`,
    `HTTP             ${num(summary.http.rps, 1)} req/s, 실패 ${pct(summary.http.failed_rate)}, P99 ${num(summary.http.p99_ms, 0)}ms`,
    `실패 내역        ${ENDPOINTS.map((n) => `${n} ${pct(summary.http.failed_by_endpoint[n])}`).join(' · ')}`,
    '',
    `→ ${OUT}`,
    '',
  ];

  if (botCh.unaccounted || humanCh.unaccounted) {
    lines.splice(2, 0,
      `⚠ 입장 채널 항등식이 깨졌다 (봇 잔차 ${botCh.unaccounted} · 사람 잔차 ${humanCh.unaccounted}).`,
      '  분류기가 모르는 경로로 입장한 참가자가 있다 — 격리된 뒤 재검증 없이 돌아온',
      '  경로(restore_shard)가 가장 유력하다. 그 경로는 표의 어느 칸에도 나타나지 않으므로,',
      '  잔차를 확인하기 전에는 이 실행의 입장 수치를 누수 채널 분석에 쓸 수 없다.',
      '');
  }

  if (cpuBound) {
    lines.splice(2, 0,
      `⚠ 부하 생성기가 PoW 에 묶였다 (평균 ${num(powAvg, 0)}ms). heartbeat 리듬이 무너져`,
      '  봇과 사람이 똑같이 불규칙해진다 — 아래 탐지율/오탐율은 시스템이 아니라 k6 를 잰 값이다.',
      '  난이도를 낮추거나(SG_POW_BASE_DIFFICULTY / SG_GREYLIST_DIFFICULTY_BUMP) 참가자를 줄일 것.',
      '');
  }

  const out = {};
  out.stdout = lines.join('\n');
  out[OUT] = JSON.stringify(summary, null, 2);
  return out;
}
