-- rechallenge.lua — greylist 사용자가 재챌린지를 통과했을 때의 상태 전이(DESIGN.md §4).
--
-- **greylist 는 종점이 아니라 검문소다.** 40~69 구간을 "재검증 후 복귀"로 설계한
-- 이유는 오탐 때문이다. 오탐은 반드시 생기고, 생겼을 때 사용자가 잃는 것이 없어야
-- 의심을 넓게 볼 수 있다. 나가는 길이 없으면 40~69 는 70~89(보류)와 같아지고,
-- 사다리에 칸이 두 개일 이유가 사라진다.
--
-- # 통과가 무죄 판결은 아니다
--
-- 그래서 점수를 0 으로 되돌리지 않는다. 임계 직하(pass_score)로 **클램프**만 한다:
--
--   - CAPTCHA 대행처럼 재챌린지만 사람이 대신 풀어 주는 봇은, 복귀 직후부터
--     행동 신호(간격 규칙성·상호상관·지문·대역)로 다시 올라온다. 0 으로 리셋하면
--     그 재상승에 창 스무 개가 더 필요해져 사실상 무한 면제권이 된다.
--   - 반대로 점수를 그대로 두면 다음 창에서 즉시 다시 격리돼 통과가 무의미해진다.
--
--   클램프는 min(현재, pass_score) 이다. 통과가 점수를 **올리는** 일은 없다.
--
-- # 횟수 제한과 승급
--
-- 재챌린지가 값싼 출구가 되지 않도록 시도 횟수를 센다(rc_count). 상한을 넘겨서
-- 다시 오면 복귀시키지 않고 보류(70~89)로 올린다 — 계속 걸리고 계속 풀어 대는
-- 것 자체가 신호이기 때문이다. 보류는 순번을 보존하므로 이 승급도 되돌릴 수 있다.
--
-- 난이도는 회차마다 올라가지만 그건 여기가 아니라 challenge 발급 쪽 일이다
-- (rc_count 를 난이도 산정 입력으로 넘긴다).
--
-- # 복귀는 관찰 시계를 되감는다
--
-- 최소 관찰 게이트(§3.4)가 재는 것은 `now - joined_at` 이다. 복귀한 사용자는 그 값을
-- 이미 채우고 있으므로, 되돌려 놓기만 하면 **게이트를 통과한 상태로 돌아온다.**
-- 그러면 문은 §12-7 의 경주를 복귀 1회당 한 번씩 다시 열어 준다 — 클램프(35)에서
-- 임계까지 다시 오르는 데 걸리는 시간 동안, 복귀 봇은 관찰 없이 선두에서 redeem 을
-- 두드린다. 게이트가 지키는 것이 첫 입장뿐이면 그 뒤의 모든 재진입은 무방비다.
--
-- 그래서 복귀 시 `observe_from`/`hb_base` 를 지금으로 다시 찍는다. "탐지기에 시간을
-- 사준다"는 원리를 누수 지점에 그대로 재적용하는 것이고, 비용은 **한 번이라도
-- 플래그된 사용자에게만** 간다. 오탐으로 걸린 사람은 복귀 후 MIN_DWELL 만큼 더
-- 기다리지만, 자리는 태워지지 않고(admit.lua 의 DECR 앞 검사) 순번도 보존된다.
--
-- `joined_at` 을 덮어쓰지 않는 이유: 그 값은 진입 시각이고 감사·추첨 구간 판정의
-- 근거다. 관찰 시계와 진입 시각은 복귀 이후로 서로 다른 사실이 되므로 필드를 나눈다.
--
-- restore_shard.lua(점수가 스스로 내려와 열리는 문)에는 이 되감기가 없다. 그쪽 점수는
-- 탐지기가 충분히 보고 내린 결론이지만, 여기 점수는 **양보**다(클램프는 판정이 아니라
-- 임계 직하로 낮춰 준 값이다). 결론에는 추가 관찰이 필요 없고 양보에는 필요하다.
--
-- KEYS[1] queue:{event:slot}            원 샤드 대기열 (복귀 대상)
-- KEYS[2] queue:{event:slot}:<grey>     greylist 대기열
-- KEYS[3] hold:{event:slot}             보류석 (소진 시 승급 대상)
-- KEYS[4] score:{event:slot}
-- KEYS[5] user:{event:slot}:<token>
--
-- ARGV[1] token_id
-- ARGV[2] origin_shard
-- ARGV[3] max_attempts
-- ARGV[4] pass_score       통과 시 클램프 상한
-- ARGV[5] hold_score       소진 시 최소 보장 점수(보류 임계)
-- ARGV[6] now_ms
-- ARGV[7] user_ttl_ms
--
-- 반환: { applied, state, rank, attempts, score }
--   applied: restored | exhausted | noop | unknown | no_rank

local originKey = KEYS[1]
local greyKey = KEYS[2]
local holdKey = KEYS[3]
local scoreKey = KEYS[4]
local userKey = KEYS[5]

local tokenID = ARGV[1]
local originShard = ARGV[2]
local maxAttempts = tonumber(ARGV[3])
local passScore = tonumber(ARGV[4])
local holdScore = tonumber(ARGV[5])
local nowMs = tonumber(ARGV[6])
local userTTLms = tonumber(ARGV[7])

local h = redis.call('HMGET', userKey, 'state', 'orig_rank', 'rc_count', 'score', 'hb_count')
local state = h[1]
if not state then
  return { 'unknown', 'unknown', -1, 0, -1 }
end

local attempts = tonumber(h[3]) or 0
local score = tonumber(h[4]) or 0

if state ~= 'greylist' then
  -- waiting 은 이미 정상이라 풀어 줄 것이 없고, held/blocked 는 재챌린지로 풀지
  -- 않는다. 보류·차단은 greylist 보다 위 칸이고, 위 칸을 아래 칸의 통과 조건으로
  -- 되돌리면 사다리가 사다리가 아니게 된다.
  return { 'noop', state, -1, attempts, score }
end

local rank = tonumber(h[2])
if not rank then
  -- 돌아갈 자리를 모른다. 순번을 지어내느니 상태를 그대로 둔다 —
  -- 지어낸 순번은 누군가의 자리를 빼앗는다.
  return { 'no_rank', state, -1, attempts, score }
end

attempts = attempts + 1
redis.call('HSET', userKey, 'rc_count', attempts, 'rc_at', nowMs)

if attempts > maxAttempts then
  -- 재검증 기회를 소진했다. 보류로 올린다(순번은 보존).
  if score < holdScore then score = holdScore end
  redis.call('ZREM', greyKey, tokenID)
  redis.call('ZADD', holdKey, rank, tokenID)
  redis.call('HSET', scoreKey, tokenID, score)
  redis.call('HSET', userKey,
    'state', 'held',
    'shard', originShard,
    'score', score,
    'scored_at', nowMs,
    'held_at', nowMs)
  redis.call('HDEL', userKey, 'greylisted_at')
  redis.call('PEXPIRE', userKey, userTTLms)
  return { 'exhausted', 'held', rank, attempts, score }
end

if score > passScore then score = passScore end

redis.call('ZREM', greyKey, tokenID)
redis.call('ZADD', originKey, rank, tokenID)
redis.call('HSET', scoreKey, tokenID, score)
redis.call('HSET', userKey,
  'state', 'waiting',
  'shard', originShard,
  'score', score,
  'scored_at', nowMs,
  'restored_at', nowMs,
  -- 관찰 시계를 지금으로 되감는다. admit.lua 는 joined_at 대신 이 값을 본다.
  'observe_from', nowMs,
  'hb_base', tonumber(h[5]) or 0)
redis.call('HDEL', userKey, 'greylisted_at')
redis.call('PEXPIRE', userKey, userTTLms)

return { 'restored', 'waiting', rank, attempts, score }
