-- admit.lua — 순번이 도달한 사용자를 입장시키고 1회용 입장 토큰을 발행한다.
--
-- 입장 조건은 단 하나다: `내 순번 < 남은 예산`.
-- 이 규칙이 왜 정확히 예산만큼만, 그리고 앞사람부터 통과시키는지:
--   예산 3, 큐 [u0..u9] 일 때 u0 은 rank 0 < 3 → 통과, 예산 2, 큐 [u1..u9].
--   u1 은 rank 0 < 2 → 통과 … u3 은 rank 0 < 0 → 거절. 정확히 3명.
--   순서가 뒤섞여 u2 가 먼저 와도(rank 2 < 3) 총 3명은 그대로다.
-- ZREM 과 DECR 이 같은 실행 안에서 함께 일어나기 때문에 성립하는 계산이다.
-- 두 번의 왕복으로 쪼개면 그 사이에 낀 요청이 같은 자리를 두 번 쓴다(불변식 1).
--
-- 멱등(불변식 4): 이미 교환한 사용자는 새 입장 토큰을 받지 못하고 원래 jti 를 돌려받는다.
-- 재시도할 때마다 입장 토큰이 하나씩 늘어난다면 1인 1구매는 성립하지 않는다.
--
-- 입장 토큰 키(entry:...)는 KEYS 로 선언하지 않고 접두사로 조립한다.
-- queue/budget/user 와 같은 해시태그를 공유하므로 항상 같은 슬롯에 있다(§3.3).
--
-- 최소 관찰 게이트(§3.4): 진입한 지 min_dwell_ms 가 안 됐거나 생존 신호가
-- min_beats 미만이면 순번이 와도 입장시키지 않는다. 조치 파이프라인은 누적 점수로
-- 움직이므로(불변식 3) 판정에 시간이 걸리는데, 그 전에 입장해 버린 사용자는 격리될
-- 기회 자체를 얻지 못한다(§12-7). "데이터가 부족하다"는 무죄가 아니라 아직 판정
-- 불가라는 뜻이므로, 판정할 수 있을 때까지 자리를 미룬다.
--
-- 검사는 예산 차감(DECR) **앞**에 둔다. 뒤에 두면 관찰 중인 사용자가 자리를 태우고,
-- 그 자리는 아무도 쓰지 못한 채 사라진다. 앞에 두면 예산은 그대로 남아 뒷사람이나
-- 다음 주기가 쓴다 — ADMIT_AFTER_LOTTERY 가 주기를 통째로 건너뛰어 자리 수를 줄이는
-- 것과 달리, 이 게이트는 자리를 없애지 않고 미루기만 한다.
--
-- 재는 기준은 진입 시각(joined_at)이 아니라 **관찰 시계**(observe_from)다. 둘은 보통
-- 같지만 재챌린지 복귀 이후로 갈라진다 — 복귀한 사용자를 진입 시각으로 재면 게이트를
-- 이미 통과한 상태로 돌아와, 문이 열릴 때마다 §12-7 의 경주가 다시 시작된다.
-- 관찰 시계는 rechallenge.lua 가 복귀 시점으로 되감고, 그 근거는 그쪽에 적었다.
-- 생존 신호도 같은 이유로 누적값이 아니라 `hb_count - hb_base` 를 본다.
--
-- KEYS[1] queue:{event:shard}
-- KEYS[2] budget:{event:shard}
-- KEYS[3] user:{event:shard}:<token>
--
-- ARGV[1] token_id
-- ARGV[2] jti             새로 발행할 입장 토큰 ID (서버 CSPRNG)
-- ARGV[3] now_ms
-- ARGV[4] entry_prefix    "entry:{event:shard}:"
-- ARGV[5] entry_ttl_ms
-- ARGV[6] user_ttl_ms
-- ARGV[7] min_dwell_ms    최소 체류 시간. 0 이면 게이트 없음
-- ARGV[8] min_beats       최소 생존 신호 수. 0 이면 게이트 없음
--
-- 반환: { status, rank, budget, jti, waited_ms }
--   status: admitted | observing | not_yet | unknown | evicting | held | blocked | evicted
--   observing 의 waited_ms 는 진입 후 경과 시간이다(남은 관찰 시간 계산용)

local queueKey = KEYS[1]
local budgetKey = KEYS[2]
local userKey = KEYS[3]

local tokenID = ARGV[1]
local jti = ARGV[2]
local nowMs = tonumber(ARGV[3])
local entryPrefix = ARGV[4]
local entryTTLms = tonumber(ARGV[5])
local userTTLms = tonumber(ARGV[6])
local minDwellMs = tonumber(ARGV[7]) or 0
local minBeats = tonumber(ARGV[8]) or 0

local h = redis.call('HMGET', userKey,
  'state', 'joined_at', 'entry_jti', 'hb_count', 'observe_from', 'hb_base')
local state = h[1]
local budget = tonumber(redis.call('GET', budgetKey)) or 0

if not state then
  return { 'unknown', -1, budget, '', -1 }
end

-- observe_from 이 없으면 한 번도 복귀한 적 없다는 뜻이라 진입 시각이 곧 관찰 시작이다.
local obsFrom = tonumber(h[5]) or tonumber(h[2])
local waited = -1
if obsFrom then
  waited = nowMs - obsFrom
end
local beats = (tonumber(h[4]) or 0) - (tonumber(h[6]) or 0)

if state == 'admitted' then
  return { 'admitted', -1, budget, h[3] or '', -1 }
end

if state ~= 'waiting' then
  -- greylist / held / blocked / evicted / evicting: 입장 대상이 아니다.
  -- 특히 held 는 "대기열에는 남기되 admit 에서 제외"가 정의 그 자체다(§4).
  --
  -- greylist 도 여기서 걸리지만 성질이 다르다 — 조치가 아니라 검문이고, 나가는
  -- 길이 있다(rechallenge.lua). 호출자는 이 상태를 오류가 아니라 "재챌린지를
  -- 받아 오라"로 옮긴다. 이 경로에는 쓰기가 하나도 없으므로 예산도 태우지 않는다.
  return { state, -1, budget, '', -1 }
end

local rank = redis.call('ZRANK', queueKey, tokenID)
if rank == false then
  -- 상태는 waiting 인데 큐에 없다 → 신뢰할 수 없는 상태다. 입장시키지 않는다.
  return { 'unknown', -1, budget, '', -1 }
end

-- 아직 판정할 만큼 보지 못했다. 순번·예산과 무관하게 입장시키지 않는다.
-- 예산을 건드리기 전이므로 이 자리는 사라지지 않고 남는다.
--
-- `waited < 0` 은 관찰 시작 시각이 없다는 뜻이다(해시가 깨졌거나 구버전 필드).
-- 그 경우 게이트는 **닫힌 쪽으로** 실패한다 — 관찰 시간을 모르는 것은 관찰이
-- 충분하다는 근거가 못 되고, 여기서 열어 주면 joined_at 을 지우는 것이 곧
-- 게이트 우회가 된다. 자리는 그대로 남으므로 사용자가 잃는 것은 없다.
if minDwellMs > 0 and (waited < 0 or waited < minDwellMs) then
  return { 'observing', rank, budget, '', waited }
end
if minBeats > 0 and beats < minBeats then
  return { 'observing', rank, budget, '', waited }
end

if rank >= budget then
  return { 'not_yet', rank, budget, '', -1 }
end

budget = redis.call('DECR', budgetKey)
redis.call('ZREM', queueKey, tokenID)
redis.call('HSET', userKey, 'state', 'admitted', 'admitted_at', nowMs, 'entry_jti', jti)
redis.call('PEXPIRE', userKey, userTTLms)
redis.call('SET', entryPrefix .. jti, tokenID, 'PX', entryTTLms)

return { 'admitted', rank, budget, jti, waited }
