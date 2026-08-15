// 봇팜 시나리오 (DESIGN.md §11) — 세 유형.
//
//   naive       시계처럼 정확한 스크립트. 한 기기·한 대역. 아무것도 숨기지 않는다.
//   mimic       heartbeat 간격에 사람 같은 지터를 넣어 규칙성 신호를 지운다.
//   distributed 레지덴셜 프록시로 IP 대역만 흩는다. 자동화 이미지는 하나라 지문은 공유.
//
// 세 유형은 각각 **다른 신호를 피한다.** 어떤 유형도 모든 신호를 동시에 피하지
// 못한다는 것이 다층 방어(§4)의 전제이고, 이 스크립트는 그 전제를 시험한다.
//
// 봇만 돌리면 샤드 전체가 봇이 되어 상대적 신호(주변과의 비교)가 약해진다.
// 그 상태에서도 절대 하한(heartbeat CV floor)과 그룹 신호가 남는지 보는 것이
// 이 스크립트의 목적이고, 실제 탐지율은 mixed.js 로 재야 한다.
//
//   k6 run loadtest/k6/bot_farm.js
//   k6 run -e SG_BOTS=60 loadtest/k6/bot_farm.js

import { naive, mimic, distributed } from './lib/personas.js';

const BOTS = Number(__ENV.SG_BOTS || 30);
const PER_TYPE = Math.max(1, Math.floor(BOTS / 3));
const DURATION = __ENV.SG_DURATION || '3m';

function farm(exec) {
  return { executor: 'constant-vus', vus: PER_TYPE, duration: DURATION, exec };
}

export const options = {
  scenarios: {
    naive: farm('naive'),
    mimic: farm('mimic'),
    distributed: farm('distributed'),
  },
  thresholds: {
    // 봇팜 전체의 격리율.
    sg_bot_isolated: ['rate>0.8'],
  },
};

export { naive, mimic, distributed };

export default naive;
