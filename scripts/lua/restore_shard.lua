-- restore_shard.lua — 재검증을 통과한 사용자를 원 샤드의 원 순번으로 되돌린다(§4).
--
-- 이 스크립트가 존재하는 이유가 곧 조치 파이프라인이 "즉시 차단"이 아닌 이유다.
-- 오탐은 반드시 생긴다. 생겼을 때 사용자가 잃는 것이 없어야 의심 판정을 넓게 쓸 수
-- 있고, 넓게 쓸 수 있어야 정말 나쁜 것만 좁게 차단할 수 있다.
--
-- greylist 와 hold 양쪽에서 부른다:
--   greylist → 재챌린지(상향된 PoW / CAPTCHA) 통과
--   held     → 재검증 통과 또는 점수 하락
--
-- 되돌리는 자리는 현재 순번이 아니라 orig_rank 다. 그 사이에 앞사람이 빠져나갔다면
-- 오히려 앞당겨지고, 뒷사람이 늘었어도 밀리지 않는다.
--
-- KEYS[1] queue:{event:slot}            원 샤드 대기열 (복귀 대상)
-- KEYS[2] queue:{event:slot}:<grey>     greylist 대기열
-- KEYS[3] hold:{event:slot}[:shard]     보류석
-- KEYS[4] user:{event:slot}:<token>
--
-- ARGV[1] token_id
-- ARGV[2] origin_shard
-- ARGV[3] now_ms
-- ARGV[4] user_ttl_ms
--
-- 반환: { applied, state, rank }

local originKey = KEYS[1]
local greyKey = KEYS[2]
local holdKey = KEYS[3]
local userKey = KEYS[4]

local tokenID = ARGV[1]
local originShard = ARGV[2]
local nowMs = tonumber(ARGV[3])
local userTTLms = tonumber(ARGV[4])

local h = redis.call('HMGET', userKey, 'state', 'orig_rank')
local state = h[1]
if not state then
  return { 'unknown', 'unknown', -1 }
end
if state ~= 'greylist' and state ~= 'held' then
  -- waiting 은 이미 정상이고, blocked 는 여기서 풀지 않는다(차단 해제는 별도 절차다).
  return { 'noop', state, -1 }
end

local rank = tonumber(h[2])
if not rank then
  -- 돌아갈 자리를 모른다. 순번을 지어내느니 상태를 그대로 둔다 —
  -- 지어낸 순번은 누군가의 자리를 빼앗는다.
  return { 'no_rank', state, -1 }
end

redis.call('ZREM', greyKey, tokenID)
redis.call('ZREM', holdKey, tokenID)
redis.call('ZADD', originKey, rank, tokenID)

redis.call('HSET', userKey,
  'state', 'waiting',
  'shard', originShard,
  'restored_at', nowMs)
redis.call('HDEL', userKey, 'held_at', 'greylisted_at')
redis.call('PEXPIRE', userKey, userTTLms)

return { 'restore', 'waiting', rank }
