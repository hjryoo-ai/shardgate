// PoW 워커.
//
// 풀이를 메인 스레드에서 돌리면 난이도 16(기댓값 65,536회)에서 화면이 멈춘다.
// 대기실은 "백엔드가 다 죽어도 살아 있어야 하는 화면"인데(§7), 정작 자기 자신의
// 계산 때문에 얼어붙으면 곤란하다. 그래서 풀이는 전부 여기서 한다.

importScripts('sha256.js');

// 시도 상한. 난이도 26(설정 상한)이어도 기댓값의 몇 배를 커버한다.
const LIMIT = 1 << 28;

self.onmessage = (e) => {
  const { nonce, difficulty } = e.data;
  const started = Date.now();

  const result = self.ShardGateHash.solve(nonce, difficulty, LIMIT, (tries) => {
    self.postMessage({ type: 'progress', tries });
  });

  if (!result) {
    self.postMessage({ type: 'failed' });
    return;
  }
  self.postMessage({
    type: 'solved',
    solution: result.solution,
    tries: result.tries,
    ms: Date.now() - started,
  });
};
