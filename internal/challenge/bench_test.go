// PoW 해시율 — 봇 비용 계산의 입력(DESIGN §1.4).
//
// §1.4 가 선언한 목표는 "봇의 완전 차단"이 아니라 **봇의 비용을 정상 사용자의
// 가치 이상으로 끌어올리는 것**이다. 그 선언은 지금까지 문장으로만 있었다 —
// 탐지율·오탐율은 재 왔지만 비용은 한 번도 재지 않았다.
//
// 비용은 부하 테스트로 잴 수 없다. 난이도를 올리면 k6 자신이 CPU 에 묶여
// 탐지 지표까지 무너지기 때문이다(REPORT §3.7). 그래서 **부하와 경제를 분리한다**:
// 여기서 참조 하드웨어의 해시율을 재고, 시도 횟수는 §3.7 의 측정에서 가져와
// 곱한다. PoW 스케줄이 결정적(기대 시도 = 2^d)이라 그 곱이 성립한다.
//
// 난이도별 실제 풀이 시간은 BenchmarkSolve(challenge_test.go)가 재고, 그것이
// 여기서 나온 해시율의 외삽과 맞는지 확인해 준다.
//
//	make bench-pow
package challenge

import (
	"strconv"
	"testing"
)

// BenchmarkDigest 는 해시 1회 비용이다. 이 값 하나로 모든 난이도의 기대 비용이
// 나온다 — 26비트를 직접 재려면 6,700만 회를 돌려야 하지만 그럴 필요가 없다.
func BenchmarkDigest(b *testing.B) {
	nonce := "bench-nonce-0123456789abcdef"
	for i := 0; b.Loop(); i++ {
		_ = Digest(nonce, strconv.Itoa(i))
	}
}
