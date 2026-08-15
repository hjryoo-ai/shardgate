-- apply_action.lua — 점수를 기록하고 그에 따른 조치를 적용한다(DESIGN.md §4).
--
-- 조치 사다리:
--   0~39   observe   점수만 남긴다. 사용자는 아무것도 느끼지 못한다.
--   40~69  greylist  move_shard.lua 가 맡는다(샤드 이동이라 별도 스크립트).
--   70~89  hold      대기열에서 빼되 원 순번을 hold ZSET 에 그대로 보존한다.
--   90~100 block     토큰 무효화. 큐와 hold 양쪽에서 제거한다.
--
-- **되돌릴 수 있는 조치와 없는 조치를 구분하는 것이 이 스크립트의 요점이다.**
-- hold 는 순번을 보존하므로 오탐이어도 복구된다(restore_shard.lua). block 은 그렇지 않다.
-- 그래서 block 은 호출자가 "여러 신호가 함께 가리켰다"를 확인한 뒤에만 부른다 —
-- 어떤 단일 신호도 즉시 차단의 근거가 될 수 없다(불변식 3).
--
-- 점수는 언제나 기록한다. 조치가 없어도 관측은 남아야 다음 판단의 근거가 된다.
--
-- KEYS[1] queue:{event:slot}[:shard]
-- KEYS[2] hold:{event:slot}[:shard]
-- KEYS[3] score:{event:slot}[:shard]
-- KEYS[4] user:{event:slot}:<token>
-- KEYS[5] queue:{event:slot}:<grey>   greylist 대기열 (같은 슬롯)
--
-- KEYS[5] 가 있는 이유: hold·block 은 greylist 에 있는 사용자에게도 내려온다.
-- 멤버가 실제로 어느 ZSET 에 있는지는 상태에 따라 다르므로 양쪽에서 뺀다.
-- 한쪽만 빼면 greylist ZSET 에 유령 멤버가 남아 ZCARD 가 부풀고, 그 값이
-- 예산 배분의 입력이라 남의 자리까지 흔든다.
--
-- ARGV[1] token_id
-- ARGV[2] score        0~100
-- ARGV[3] action       observe | hold | block
-- ARGV[4] now_ms
-- ARGV[5] user_ttl_ms
--
-- 반환: { applied, state, rank_kept }
--   applied   : 실제로 적용된 조치(이미 그 상태였으면 'noop')
--   rank_kept : 보존된 원 순번. 없으면 -1

local queueKey = KEYS[1]
local holdKey = KEYS[2]
local scoreKey = KEYS[3]
local userKey = KEYS[4]
local greyKey = KEYS[5]

local tokenID = ARGV[1]
local score = tonumber(ARGV[2])
local action = ARGV[3]
local nowMs = tonumber(ARGV[4])
local userTTLms = tonumber(ARGV[5])

local state = redis.call('HGET', userKey, 'state')
if not state then
  -- 이미 사라진 사용자에 대한 뒤늦은 판정이다. 되살리지 않는다.
  return { 'unknown', 'unknown', -1 }
end

-- 점수는 조치와 무관하게 항상 남긴다.
redis.call('HSET', scoreKey, tokenID, score)
redis.call('HSET', userKey, 'score', score, 'scored_at', nowMs)
redis.call('PEXPIRE', userKey, userTTLms)

local function rankOf()
  local r = tonumber(redis.call('HGET', userKey, 'orig_rank'))
  if r then return r end
  local s = redis.call('ZSCORE', queueKey, tokenID)
  if s then return tonumber(s) end
  return -1
end

if state == 'blocked' then
  -- 차단은 되돌리지 않는다. 더 낮은 점수가 뒤늦게 와도 상태를 흔들지 않는다.
  return { 'noop', state, -1 }
end

if action == 'observe' then
  return { 'observe', state, rankOf() }
end

if action == 'hold' then
  if state == 'held' then
    return { 'noop', state, rankOf() }
  end
  local rank = rankOf()
  -- 대기열에 있었든 greylist 에 있었든 한 곳에서만 빠진다(둘 다에 있을 수는 없다).
  local removed = redis.call('ZREM', queueKey, tokenID) + redis.call('ZREM', greyKey, tokenID)
  if removed > 0 and rank >= 0 then
    -- 원 순번 그대로 보류석으로 옮긴다. 해제되면 손해 없이 그 자리로 돌아온다.
    redis.call('ZADD', holdKey, rank, tokenID)
  end
  redis.call('HSET', userKey, 'state', 'held', 'held_at', nowMs)
  return { 'hold', 'held', rank }
end

if action == 'block' then
  local rank = rankOf()
  redis.call('ZREM', queueKey, tokenID)
  redis.call('ZREM', greyKey, tokenID)
  redis.call('ZREM', holdKey, tokenID)
  redis.call('HSET', userKey, 'state', 'blocked', 'blocked_at', nowMs)
  return { 'block', 'blocked', rank }
end

return { 'unknown_action', state, -1 }
