-- move_shard.lua — 의심 사용자를 greylist 샤드로 옮긴다(DESIGN.md §4, score 40~69).
--
-- 이 이동이 샤딩 구조의 두 번째 이점이다: 의심군을 별도 모집단으로 몰아 정밀
-- 관찰하고, 정상 샤드의 admit 처리량은 오염시키지 않는다.
--
-- **이건 처벌이 아니라 재검증이다.** 순번은 그대로 들고 간다(orig_rank). greylist
-- 에서는 PoW 난이도가 오르고 재챌린지를 받지만, 통과하면 원 샤드의 원래 자리로
-- 돌아온다(rechallenge.lua). 정상 사용자가 여기 걸려도 잃는 것이 없어야 40~69
-- 구간을 넓게 잡을 수 있고, 넓게 잡을 수 있어야 차단을 좁게 쓸 수 있다.
-- 점수가 스스로 내려가서 풀리는 경로는 restore_shard.lua 가 맡는다.
--
-- greylist 샤드는 원 샤드와 **같은 해시 슬롯**에 있다(internal/keys 의 slotOf).
-- 그래서 "원본에서 빼고 대상에 넣고 상태를 바꾸는" 세 동작이 한 번의 원자 실행이 된다.
-- 슬롯이 달랐다면 이 이동은 중간 상태가 보이는 2단계 작업이 됐을 것이다.
--
-- KEYS[1] queue:{event:slot}            원 샤드 대기열
-- KEYS[2] queue:{event:slot}:<grey>     greylist 대기열
-- KEYS[3] score:{event:slot}
-- KEYS[4] user:{event:slot}:<token>
--
-- ARGV[1] token_id
-- ARGV[2] greylist_shard
-- ARGV[3] score
-- ARGV[4] now_ms
-- ARGV[5] user_ttl_ms
--
-- 반환: { applied, state, rank }

local fromKey = KEYS[1]
local toKey = KEYS[2]
local scoreKey = KEYS[3]
local userKey = KEYS[4]

local tokenID = ARGV[1]
local greyShard = ARGV[2]
local score = tonumber(ARGV[3])
local nowMs = tonumber(ARGV[4])
local userTTLms = tonumber(ARGV[5])

local h = redis.call('HMGET', userKey, 'state', 'shard', 'orig_shard', 'orig_rank')
local state = h[1]
if not state then
  -- 이미 사라진 사용자다. 점수만 남기겠다고 해시를 되살리지 않는다.
  return { 'unknown', 'unknown', -1 }
end

-- **점수는 조치보다 먼저 기록한다**(불변식 7).
--
-- 이 순서가 뒤집혀 있었다. 아래 가드에서 noop 으로 빠지는 경로 — 즉 이미 greylist
-- 인 사용자 — 가 점수 기록을 건너뛰는 바람에, 스코어러는 매 창마다 판정을 계속
-- 내리는데 그 결과가 하나도 저장되지 않았다. greylist 가 점수 냉동고가 되어
-- 사다리의 40~69 칸에서 위로도 아래로도 움직이지 못했다(REPORT §3.5:
-- 봇 2,400마리 전원이 40.2 에서 정지).
--
-- 조치가 관측을 멈추면 격리가 곧 종점이 된다. 상태에 따라 달라지는 것은 조치의
-- 종류이지 관측 여부가 아니다.
redis.call('HSET', scoreKey, tokenID, score)
redis.call('HSET', userKey, 'score', score, 'scored_at', nowMs)
redis.call('PEXPIRE', userKey, userTTLms)

if state ~= 'waiting' and state ~= 'evicting' then
  -- 이미 greylist / held / blocked / admitted 다. 사다리를 되돌리지 않는다.
  -- 점수는 위에서 기록됐으므로, 계속 봇처럼 구는 참가자는 여기 머무는 동안에도
  -- 70(보류) → 90(차단) 으로 올라간다.
  return { 'noop', state, -1 }
end

local rank = tonumber(redis.call('ZSCORE', fromKey, tokenID))
if not rank then
  -- 대기열에 없는데 상태만 waiting 이다. 옮길 자리가 없으므로 점수만 남긴다.
  return { 'noop', state, -1 }
end

redis.call('ZREM', fromKey, tokenID)
redis.call('ZADD', toKey, rank, tokenID)

-- orig_shard / orig_rank 는 최초 배정 시점 값을 지킨다. 여러 번 옮겨 다녀도
-- "돌아갈 곳"은 언제나 처음 그 자리다.
if not h[3] then
  redis.call('HSET', userKey, 'orig_shard', h[2] or '')
end
if not h[4] then
  redis.call('HSET', userKey, 'orig_rank', rank)
end

redis.call('HSET', userKey,
  'state', 'greylist',
  'shard', greyShard,
  'greylisted_at', nowMs)

return { 'greylist', 'greylist', rank }
