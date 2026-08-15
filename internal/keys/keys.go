// Package keys 는 Redis 키 네이밍의 단일 진실이다.
//
// DESIGN.md §3.3 의 논리 스키마 `{리소스}:{event}:{shard}` 를 따르되,
// 물리 키에는 Redis Cluster 해시태그를 넣는다:
//
//	논리: queue:{event}:{shard}      물리: queue:{evt1:s0042}
//	논리: user:{event}:{token_id}    물리: user:{evt1:s0042}:tok_...
//
// 해시태그(`{...}`) 안의 문자열만 슬롯 계산에 쓰이므로, 한 샤드에 속한 모든 키
// (queue/hold/seq/budget/score/stats/user/entry)가 같은 슬롯에 모인다.
// Lua 스크립트 하나가 이 키들을 동시에 만지는 것이 Cluster 모드에서도 성립해야
// 하기 때문이며(불변식 1: 상태 전이는 단일 원자 Lua), 서로 다른 샤드는 서로 다른
// 태그를 가지므로 핫키 없이 슬롯에 자연 분산된다(§3.3).
//
// 이 패키지 밖에서 Redis 키 문자열을 직접 조립하지 말 것.
package keys

import "strings"

// 리소스 접두사. DESIGN.md §3.3 스키마 외의 접두사를 임의로 추가하지 않는다.
const (
	prefixQueue     = "queue"
	prefixHold      = "hold"
	prefixSeq       = "seq"
	prefixUser      = "user"
	prefixAdmitted  = "admitted"
	prefixBudget    = "budget"
	prefixShards    = "shards"
	prefixEntry     = "entry"
	prefixChallenge = "challenge"
	prefixScore     = "score"
	prefixStats     = "stats"
	prefixSuspicion = "suspicion"
	prefixIdem      = "idem"
)

// greylistPrefix / normalPrefix 는 샤드 ID 의 첫 글자다(internal/shard 와 같은 규약).
const (
	normalPrefix   = 's'
	greylistPrefix = 'g'
)

// slotOf 는 샤드 ID 를 "어느 슬롯에 놓일지"로 정규화한다.
//
// greylist 샤드(g0042)는 원 샤드(s0042)와 같은 슬롯에 둔다. §4 의 조치 파이프라인은
// 의심 사용자를 원 샤드에서 greylist 샤드로 옮기는데, 두 샤드가 다른 슬롯에 있으면
// 그 이동을 Lua 한 번으로 끝낼 수 없다(불변식 1). 슬롯을 공유하면 "ZREM 원본 +
// ZADD 대상 + 상태 갱신"이 한 번의 원자 실행이 된다.
//
// 슬롯을 공유해도 두 샤드는 논리적으로 완전히 분리돼 있다 — 큐도 예산도 통계도
// 각자의 키를 갖는다. 공유하는 것은 물리적 배치뿐이고, greylist 모집단은 원 샤드보다
// 훨씬 작으므로 슬롯 균등화도 사실상 영향받지 않는다.
func slotOf(shard string) string {
	if len(shard) > 1 && shard[0] == greylistPrefix {
		return string(normalPrefix) + shard[1:]
	}
	return shard
}

// ShardTag 는 한 샤드에 속한 키들을 같은 해시 슬롯에 묶는 태그를 만든다.
func ShardTag(event, shard string) string {
	slot := slotOf(shard)
	var sb strings.Builder
	sb.Grow(len(event) + len(slot) + 3)
	sb.WriteByte('{')
	sb.WriteString(event)
	sb.WriteByte(':')
	sb.WriteString(slot)
	sb.WriteByte('}')
	return sb.String()
}

// EventTag 는 이벤트 전역 키를 묶는 태그를 만든다.
func EventTag(event string) string { return "{" + event + "}" }

// shardKey 는 샤드 단위 컬렉션 키를 만든다.
//
// greylist 샤드는 슬롯을 원 샤드와 공유하지만 컬렉션은 따로 가져야 하므로,
// 태그 뒤에 샤드 ID 를 덧붙여 구분한다:
//
//	queue:{evt1:s0042}            원 샤드의 대기열
//	queue:{evt1:s0042}:g0042      같은 슬롯에 있는 greylist 대기열
func shardKey(prefix, event, shard string) string {
	tag := ShardTag(event, shard)
	if slot := slotOf(shard); slot != shard {
		return prefix + ":" + tag + ":" + shard
	}
	return prefix + ":" + tag
}

// Queue 는 샤드 대기열 ZSET 키다. member=token_id, score=순번.
func Queue(event, shard string) string { return shardKey(prefixQueue, event, shard) }

// Hold 는 입장 보류(score 70~89) 사용자를 원 순번 그대로 보관하는 ZSET 키다.
// 대기열에는 남아 있지만 admit 대상에서 제외된다(DESIGN.md §4 조치 파이프라인).
func Hold(event, shard string) string { return shardKey(prefixHold, event, shard) }

// Seq 는 FIFO 구간의 단조 증가 순번 카운터 키다.
func Seq(event, shard string) string { return shardKey(prefixSeq, event, shard) }

// Budget 은 샤드별 남은 입장 예산(token bucket) 키다.
func Budget(event, shard string) string { return shardKey(prefixBudget, event, shard) }

// Score 는 샤드 내 token_id → 봇 점수 HASH 키다.
func Score(event, shard string) string { return shardKey(prefixScore, event, shard) }

// Stats 는 샤드 단위 통계 누적 HASH 키다(스코어러 전용).
func Stats(event, shard string) string { return shardKey(prefixStats, event, shard) }

// User 는 사용자 상태 HASH 키다. 소속 샤드 태그를 공유해 큐 키와 같은 슬롯에 놓인다.
//
// greylist 로 옮겨 가도 이 키는 바뀌지 않는다(태그가 슬롯 기준이므로). 사용자의
// 상태 해시가 이동할 때마다 이름이 바뀌면 해시를 통째로 복사해야 하고, 그 복사는
// 원자적일 수 없다. 옮겨지는 것은 큐 멤버십뿐이고 상태는 제자리에 있는다.
func User(event, shard, tokenID string) string {
	return prefixUser + ":" + ShardTag(event, shard) + ":" + tokenID
}

// UserPrefix 는 한 샤드에 속한 user 키들의 공통 접두사다.
// 스윕처럼 멤버 목록을 돌며 상태를 읽어야 하는 Lua 는 키를 미리 KEYS 로 나열할 수
// 없으므로 이 접두사를 받아 조립한다. 조립된 키는 같은 해시태그를 공유하니
// 언제나 KEYS 로 넘긴 샤드 키와 같은 슬롯에 있다.
func UserPrefix(event, shard string) string {
	return prefixUser + ":" + ShardTag(event, shard) + ":"
}

// Entry 는 1회용 입장 토큰(jti) 키다. redeem 시 소각된다(불변식 2).
func Entry(event, shard, jti string) string {
	return prefixEntry + ":" + ShardTag(event, shard) + ":" + jti
}

// EntryPrefix 는 한 샤드의 입장 토큰 키 공통 접두사다.
// admit.lua 가 "새로 만들 키"를 미리 KEYS 로 선언할 수 없어 접두사로 조립한다.
func EntryPrefix(event, shard string) string {
	return prefixEntry + ":" + ShardTag(event, shard) + ":"
}

// Admitted 는 이벤트 누적 입장 카운터 키다.
func Admitted(event string) string { return prefixAdmitted + ":" + EventTag(event) }

// Shards 는 이벤트의 활성 샤드 목록(SET) 키다. 동적 샤드 확장을 추적한다.
func Shards(event string) string { return prefixShards + ":" + EventTag(event) }

// Challenge 는 1회용 PoW 챌린지 키다. nonce 를 태그에 넣어 슬롯에 분산시킨다.
func Challenge(event, nonce string) string {
	return prefixChallenge + ":{" + event + ":" + nonce + "}"
}

// Suspicion 은 적응형 난이도 산출에 쓰이는 주체별(fp_hash/ip_prefix) 의심도 키다.
func Suspicion(event, subject string) string {
	return prefixSuspicion + ":{" + event + ":" + subject + "}"
}

// Idem 은 멱등키 보관용 키다(불변식 4).
func Idem(event, idemKey string) string {
	return prefixIdem + ":{" + event + ":" + idemKey + "}"
}
