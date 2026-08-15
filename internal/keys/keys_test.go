package keys

import (
	"strings"
	"testing"
)

// hashTag 는 Redis 가 슬롯 계산에 쓰는 부분(첫 번째 비어 있지 않은 {...})을 뽑아낸다.
func hashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	closeIdx := strings.IndexByte(key[open+1:], '}')
	if closeIdx <= 0 {
		return key
	}
	return key[open+1 : open+1+closeIdx]
}

// 한 샤드에 속한 키는 전부 같은 해시 슬롯에 놓여야 한다.
// 그렇지 않으면 Cluster 모드에서 Lua 한 번으로 상태 전이를 끝낼 수 없다(불변식 1).
func TestShardKeysShareHashSlot(t *testing.T) {
	const (
		event   = "evt1"
		shard   = "s0042"
		tokenID = "tok_abc"
		jti     = "jti_xyz"
	)
	want := event + ":" + shard

	shardKeys := map[string]string{
		"queue":  Queue(event, shard),
		"hold":   Hold(event, shard),
		"seq":    Seq(event, shard),
		"budget": Budget(event, shard),
		"score":  Score(event, shard),
		"stats":  Stats(event, shard),
		"user":   User(event, shard, tokenID),
		"entry":  Entry(event, shard, jti),
	}

	for name, k := range shardKeys {
		if got := hashTag(k); got != want {
			t.Errorf("%s key %q has hash tag %q, want %q", name, k, got, want)
		}
	}
}

// greylist 샤드는 원 샤드와 같은 슬롯에 있어야 한다.
// 그래야 §4 의 "greylist 샤드로 이동"이 Lua 한 번으로 끝난다(불변식 1).
func TestGreylistSharesTheOriginSlot(t *testing.T) {
	const (
		event  = "evt1"
		origin = "s0042"
		grey   = "g0042"
	)
	want := event + ":" + origin

	for name, k := range map[string]string{
		"queue":  Queue(event, grey),
		"hold":   Hold(event, grey),
		"budget": Budget(event, grey),
		"score":  Score(event, grey),
		"stats":  Stats(event, grey),
		"user":   User(event, grey, "tok"),
	} {
		if got := hashTag(k); got != want {
			t.Errorf("greylist %s key %q has tag %q, want %q", name, k, got, want)
		}
	}

	// 슬롯은 같아도 컬렉션은 따로여야 한다 — 섞이면 격리가 아니라 합류다.
	if Queue(event, grey) == Queue(event, origin) {
		t.Fatal("greylist queue collides with the origin queue")
	}
	if Budget(event, grey) == Budget(event, origin) {
		t.Fatal("greylist budget collides with the origin budget")
	}

	// 사용자 상태 해시만은 같은 키여야 한다. 이동할 때마다 이름이 바뀌면
	// 해시를 복사해야 하고, 그 복사는 원자적일 수 없다.
	if User(event, grey, "tok") != User(event, origin, "tok") {
		t.Fatalf("user hash key moved with the shard: %q vs %q",
			User(event, grey, "tok"), User(event, origin, "tok"))
	}
}

// 서로 다른 샤드는 서로 다른 태그를 가져야 슬롯에 분산된다(§3.3 핫키 해소).
func TestDifferentShardsGetDifferentTags(t *testing.T) {
	a := hashTag(Queue("evt1", "s0001"))
	b := hashTag(Queue("evt1", "s0002"))
	if a == b {
		t.Fatalf("shards collide on tag %q", a)
	}
}

func TestKeyNaming(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"queue", Queue("evt1", "s0042"), "queue:{evt1:s0042}"},
		{"hold", Hold("evt1", "s0042"), "hold:{evt1:s0042}"},
		{"seq", Seq("evt1", "s0042"), "seq:{evt1:s0042}"},
		{"budget", Budget("evt1", "s0042"), "budget:{evt1:s0042}"},
		{"score", Score("evt1", "s0042"), "score:{evt1:s0042}"},
		{"stats", Stats("evt1", "s0042"), "stats:{evt1:s0042}"},
		{"user", User("evt1", "s0042", "tok"), "user:{evt1:s0042}:tok"},
		{"entry", Entry("evt1", "s0042", "jti"), "entry:{evt1:s0042}:jti"},
		{"admitted", Admitted("evt1"), "admitted:{evt1}"},
		{"shards", Shards("evt1"), "shards:{evt1}"},
		{"challenge", Challenge("evt1", "n1"), "challenge:{evt1:n1}"},
		{"suspicion", Suspicion("evt1", "fp_a"), "suspicion:{evt1:fp_a}"},
		{"idem", Idem("evt1", "k1"), "idem:{evt1:k1}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("= %q, want %q", tc.got, tc.want)
			}
		})
	}
}

// 이벤트 전역 키는 이벤트 태그만 갖는다 — 샤드 키와 슬롯을 공유하지 않는다.
func TestEventScopedKeysUseEventTag(t *testing.T) {
	if got, want := hashTag(Admitted("evt1")), "evt1"; got != want {
		t.Errorf("admitted tag = %q, want %q", got, want)
	}
	if got, want := hashTag(Shards("evt1")), "evt1"; got != want {
		t.Errorf("shards tag = %q, want %q", got, want)
	}
}
