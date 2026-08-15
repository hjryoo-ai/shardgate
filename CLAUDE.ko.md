# ShardGate

*[English](CLAUDE.md) · **한국어** — 이 문서가 원문이고 영어판은 번역이다.*

샤딩 기반 가상 대기열 + 샤드 단위 통계로 매크로를 자동 격리·차단하는 시스템.
전체 설계는 @docs/DESIGN.md — 아키텍처·점수 정책·API를 바꾸기 전에 반드시 해당 문서와 대조할 것.

## Commands

```bash
make dev            # docker compose로 Redis/Kafka/PG + 전 서비스 기동
make test           # 전체 단위 테스트 (go test ./...)
make test-int       # testcontainers 통합 테스트 (Redis/Kafka 필요)
make lint           # golangci-lint run
make loadtest       # k6 혼합 시나리오 (loadtest/k6/mixed.js)
make bench-queue    # enqueue/position 벤치마크
make check-exposure # 포트 노출 점검 (0.0.0.0 바인딩 탐지)
```

- 새 기능은 반드시 `make test && make lint` 통과 후 커밋.
- 통합 테스트는 느리므로 큐/스코어러 로직 변경 시에만 실행.

## Architecture Map

- `cmd/{gate,queue,admission,scorer,shop}` — 서비스 엔트리포인트. 비즈니스 로직 금지, 조립만.
- `internal/shard` — HMAC 기반 샤드 배정. event_salt는 절대 로그에 남기지 않는다.
- `internal/queue` — Redis ZSET 조작. **직접 Redis 명령 조합 금지, 반드시 scripts/lua/ 경유.**
- `internal/challenge` — PoW 발급/검증. 난이도는 botscore가 주는 값만 사용.
  재챌린지(greylist 복귀)도 같은 발급/검증기를 쓴다 — 회차는 `Subject.Attempt` 로 실어 나르기만 한다.
- `internal/token` — JWT 발급/검증. 클레임 스키마 변경은 DESIGN.md §4-L3 갱신과 함께.
- `internal/botscore` — 샤드 단위 스코어링과 조치. 조치 임계값은 config, 하드코딩 금지.
  **채점 모집단은 언제나 원 샤드다** — greylist 를 별도 모집단으로 채점하면 상대 신호가
  0 으로 수렴해 점수가 도로 내려간다(DESIGN §12-6). greylist 가 나누는 것은 대기열과 예산이다.
- `scripts/lua/` — 모든 큐 상태 전이의 단일 진실. Go 코드에서 재구현하지 말 것.

## Invariants (절대 규칙)

1. 큐 상태 전이(enqueue, admit, shard 이동, 차단)는 **Lua 스크립트 단일 원자 실행**으로만. 다중 Redis 호출로 쪼개지 말 것.
2. 토큰 검증 없이 상태를 바꾸는 핸들러를 만들지 않는다. 입장 토큰은 redeem 시 반드시 소각.
3. 봇 조치는 점수 파이프라인(관찰→greylist→보류→차단)을 거친다. 어떤 단일 신호도 즉시 차단의 근거가 될 수 없다.
4. 상태 변경 API는 전부 멱등이어야 한다 (멱등키 또는 Lua 원자성).
5. 탐지 경로(Kafka consumer)와 admit 경로는 결합 금지 — 스코어러가 죽어도 대기열은 진행돼야 한다.
6. 개인정보: 핑거프린트는 해시만 저장. 원본 지문·IP 전체를 PG에 쓰지 않는다.
7. **점수 파이프라인은 참가자의 상태와 무관하게 계속 돈다.** greylist·보류라고 해서
   관측·점수 갱신·상단 승급을 멈추지 않는다. 상태에 따라 달라지는 것은 **조치의
   종류**이지 관측 여부가 아니다. 조치가 관측을 멈추면 격리가 곧 종점이 되어
   사다리를 4단으로 나눈 의미가 사라지고, 계속 봇처럼 구는 참가자가 임계 직상에
   얼어붙는다(실제로 그랬다 — REPORT.md §3.5, 봇 2,400마리가 40.2 에서 동결).
   조치 스크립트가 `noop` 을 돌려주는 경로에서도 점수는 반드시 기록한다.

## Conventions

- Go 표준 레이아웃, 에러는 `fmt.Errorf("...: %w", err)` 래핑, 로그는 slog 구조화 로깅.
- Redis 키 네이밍: `{리소스}:{event}:{shard}` — DESIGN.md §3.3의 스키마 외 임의 키 생성 금지.
- config는 env 기반 (`internal/config`), 매직넘버 금지 (샤드 크기, admit rate, 점수 임계값 등).
- 테스트: 테이블 드리븐. Lua 스크립트는 miniredis가 아닌 실제 Redis(testcontainers)로 검증.

## Don't

- 대기열 순번 계산을 클라이언트 신뢰 값으로 만들지 말 것 (서버가 유일한 진실).
- `scripts/lua/` 로직을 Go로 복제하지 말 것.
- 부하 테스트 없이 admit rate 배분 알고리즘을 변경하지 말 것 (`make loadtest` 먼저).
- 봇 탐지 임계값을 "테스트 통과를 위해" 조정하지 말 것 — 시나리오 쪽을 고칠 것.
- **greylist 를 출구 없는 상태로 만들지 말 것.** 나가는 문(재챌린지)이 없으면 40~69 와
  70~89 가 같아지고, 오탐으로 걸린 사람이 재검증 없이 영구 배제된다. greylist 사용자에게
  가는 응답은 오류(4xx)가 아니라 `200 + challenge_required` 다 — `observing` 과 같은 원칙.
- **대기열로 돌아오는 경로가 관찰 게이트를 우회하게 두지 말 것.** 게이트가 재는 기준은
  진입 시각(`joined_at`)이 아니라 관찰 시계(`observe_from`)이고, 재챌린지 복귀는 그
  시계를 되감는다(`rechallenge.lua`). 되감지 않으면 복귀한 사용자는 조건을 이미 채운
  채로 돌아와, **문이 열릴 때마다 §12-7 의 경주가 한 번씩 재개된다.** 게이트가 첫
  입장만 지키면 그 뒤의 재진입은 전부 무방비다. 생존 신호도 같은 이유로 누적값이
  아니라 `hb_count - hb_base` 를 본다.
  이 결함은 탐지율을 전혀 움직이지 않아 §3.7 측정을 그대로 통과했다 — 격리는 최초
  관측 시점에 기록되므로, 새는 것은 **입장 수**에만 나타난다(REPORT §3.8).
- **greylist 샤드에 입장 예산을 배분하지 말 것.** 거기서 직접 입장하는 경로는 없으므로
  (나가는 길은 원 샤드 복귀뿐이다) 배분된 자리는 아무도 못 쓴 채 사라진다.
- **compose 포트를 `0.0.0.0` 에 게시하지 말 것.** 반드시 `127.0.0.1:<host>:<container>`.
  Docker 의 포트 게시는 macOS 응용 프로그램 방화벽을 우회하고, 개발 머신이 NAT 뒤에
  있으리라는 보장도 없다. 2026-08-12 에 인증 없는 Redis 가 `0.0.0.0:6379` 로 뜬 채
  몇 분 만에 크론 주입을 당했다. 다른 기기에서 붙어야 하면 SSH 터널을 쓴다.
  `make check-exposure` 가 이 규칙을 검사하고 CI 에서도 돈다.
- **"설정이 적용됐는가"를 손잡이마다 따로 검사하지 말 것.** 서비스는 기동 시
  환경에서 실제로 읽은 설정 전체를 한 줄로 찍고(`internal/app`, `EffectiveEnv()`),
  `sweep.sh` 가 그것을 팔 정의와 대조한다. 규칙은 하나다 — 팔이 정의한 모든 `SG_*` 는
  어떤 서비스가 읽었고 값이 같거나, 하네스/클라이언트 전용 목록에 있어야 한다.
  손잡이별로 로그를 심으면 **새 손잡이를 만들 때마다 같은 구멍이 다시 열린다**:
  compose 의 환경 블록은 화이트리스트라 없는 이름은 조용히 무시되고, 서비스는
  기본값으로 정상 기동하며, 측정은 팔만 바뀐 채 그럴듯한 표를 낸다.
  새 `SG_*` 를 만들면 **compose 에 이름을 추가하는 것까지가 그 작업이다.**

## Working Notes

- Phase별 작업 목록: @docs/ROADMAP.md (체크박스로 진행 관리)
- 로컬 Kafka는 KRaft 단일 브로커. 파티션 수 = 샤드 수 상한과 일치시킬 것.
