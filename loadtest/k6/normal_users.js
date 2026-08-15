// 정상 사용자 시나리오 (DESIGN.md §11).
//
// 여기서 재는 것은 두 가지다: 사람이 **끝까지 들어가는가**(입장 성공률·대기 지연),
// 그리고 사람이 **봇으로 오인되지 않는가**(FPR). 두 번째가 이 설계의 핵심 지표다 —
// 봇을 잡는 것은 임계값을 낮추면 언제든 되지만, 그러면 사람이 같이 걸린다.
//
//   k6 run loadtest/k6/normal_users.js
//   k6 run -e SG_USERS=500 -e SG_DURATION=5m loadtest/k6/normal_users.js

import { human } from './lib/personas.js';

const USERS = Number(__ENV.SG_USERS || 100);
const RAMP = __ENV.SG_RAMP || '30s';
const DURATION = __ENV.SG_DURATION || '3m';

export const options = {
  scenarios: {
    humans: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP, target: USERS },
        { duration: DURATION, target: USERS },
        { duration: '10s', target: 0 },
      ],
      exec: 'human',
    },
  },
  thresholds: {
    // 사람이 격리되는 비율. 여기가 무너지면 나머지 지표는 볼 의미가 없다.
    sg_human_isolated: ['rate<0.02'],
    // 진입 실패(챌린지 발급·검증)는 거의 없어야 한다.
    sg_join_failed: ['rate<0.01'],
    http_req_failed: ['rate<0.02'],
    'http_req_duration{name:status}': ['p(99)<1000'],
    'http_req_duration{name:heartbeat}': ['p(99)<1000'],
  },
};

export { human };

export default human;
