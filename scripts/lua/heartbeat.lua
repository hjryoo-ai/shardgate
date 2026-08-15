-- heartbeat.lua — 생존 신호.
--
-- 두 가지 일을 한 번에 한다.
--  1) 대기열 점유 유지: 브라우저를 닫은 사용자가 자리를 계속 차지하면 뒷사람이 못 들어온다.
--     heartbeat 가 끊기면 evict.lua 가 soft-evict 로 정리한다(§5).
--  2) 봇 탐지 신호 수집: 직전 heartbeat 와의 간격(delta)을 돌려준다. 매크로는 간격이
--     지나치게 정확해 분산이 0 에 가깝고, 사람은 지터가 있다(§4-L4).
--     여기서는 관측치를 만들기만 한다 — 판단은 스코어러가, 조치는 점수 파이프라인이 한다(불변식 3).
--
-- soft-evict 유예 중(evicting)에 신호가 돌아오면 원래 순번 그대로 되살린다.
-- 잠깐의 네트워크 끊김으로 순번을 잃게 만들면, 그 피해는 봇이 아니라 사람이 본다.
--
-- KEYS[1] queue:{event:shard}
-- KEYS[2] user:{event:shard}:<token>
--
-- ARGV[1] token_id
-- ARGV[2] now_ms
-- ARGV[3] user_ttl_ms
--
-- 반환: { state, delta_ms, beats, rank, revived }
--   delta_ms: 직전 신호와의 간격. 첫 신호면 -1
--   revived : soft-evict 유예에서 되살아났으면 1

local queueKey = KEYS[1]
local userKey = KEYS[2]

local tokenID = ARGV[1]
local nowMs = tonumber(ARGV[2])
local userTTLms = tonumber(ARGV[3])

local h = redis.call('HMGET', userKey, 'state', 'last_seen')
local state = h[1]
if not state then
  return { 'unknown', -1, 0, -1, 0 }
end

local prevSeen = tonumber(h[2])
local delta = -1
if prevSeen then
  delta = nowMs - prevSeen
end

local revived = 0
if state == 'evicting' then
  redis.call('HDEL', userKey, 'evict_at')
  redis.call('HSET', userKey, 'state', 'waiting')
  state = 'waiting'
  revived = 1
end

redis.call('HSET', userKey, 'last_seen', nowMs)
local beats = redis.call('HINCRBY', userKey, 'hb_count', 1)
redis.call('PEXPIRE', userKey, userTTLms)

local rank = redis.call('ZRANK', queueKey, tokenID)
if rank == false then rank = -1 end

return { state, delta, beats, rank, revived }
