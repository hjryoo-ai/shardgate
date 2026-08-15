-- evict.lua — heartbeat 이 끊긴 사용자를 대기열에서 정리한다(soft-evict, §5).
--
-- 두 단계로 나눈다. 끊김이 곧 이탈은 아니기 때문이다.
--   waiting  → evicting : 마지막 신호가 stale_after 를 넘음. 큐에는 그대로 두고 유예 시작.
--   evicting → evicted  : 유예(grace)까지 지나도 신호가 없음. 이때 비로소 ZREM.
-- 유예 중 신호가 돌아오면 heartbeat.lua 가 순번 그대로 되살린다.
--
-- 한 번에 샤드 전체를 훑지 않고 커서(offset)로 구간을 나눠 돈다. 샤드가 목표(1,000명)를
-- 넘겨 커진 상황에서도 Lua 한 번의 실행 시간이 예측 가능해야 하기 때문이다.
--
-- 사용자 해시 키는 KEYS 로 선언하지 않고 접두사를 받아 조립한다. Cluster 에서 이게
-- 안전한 이유는 user:{event:shard}:* 가 queue:{event:shard} 와 같은 해시태그를 공유해
-- 항상 같은 슬롯에 있기 때문이다(§3.3 물리 키). 태그가 없으면 성립하지 않는다.
--
-- KEYS[1] queue:{event:shard}
--
-- ARGV[1] user_key_prefix   "user:{event:shard}:"
-- ARGV[2] now_ms
-- ARGV[3] stale_after_ms    heartbeat_interval * missed_heartbeats
-- ARGV[4] grace_ms
-- ARGV[5] offset            직전 호출이 돌려준 커서
-- ARGV[6] limit
-- ARGV[7] evicted_ttl_ms    제거된 상태를 감사용으로 남겨 두는 시간
--
-- 반환: { scanned, marked, removed, ghosts, next_offset, size }

local queueKey = KEYS[1]

local prefix = ARGV[1]
local nowMs = tonumber(ARGV[2])
local staleAfter = tonumber(ARGV[3])
local graceMs = tonumber(ARGV[4])
local offset = tonumber(ARGV[5])
local limit = tonumber(ARGV[6])
local evictedTTL = tonumber(ARGV[7])

local size = redis.call('ZCARD', queueKey)
if size == 0 then
  return { 0, 0, 0, 0, 0, 0 }
end
if offset < 0 or offset >= size then
  offset = 0
end

local members = redis.call('ZRANGE', queueKey, offset, offset + limit - 1)
local marked, removed, ghosts = 0, 0, 0

for i = 1, #members do
  local tokenID = members[i]
  local userKey = prefix .. tokenID
  local h = redis.call('HMGET', userKey, 'state', 'last_seen', 'evict_at')
  local state = h[1]
  local lastSeen = tonumber(h[2])
  local evictAt = tonumber(h[3])

  if not state then
    -- 상태 해시가 TTL 로 사라졌는데 ZSET 에만 남은 유령 항목. 순번만 축내므로 제거한다.
    redis.call('ZREM', queueKey, tokenID)
    ghosts = ghosts + 1
  elseif state == 'evicting' then
    if evictAt and nowMs >= evictAt then
      redis.call('ZREM', queueKey, tokenID)
      redis.call('HSET', userKey, 'state', 'evicted', 'evicted_at', nowMs)
      redis.call('PEXPIRE', userKey, evictedTTL)
      removed = removed + 1
    end
  elseif state == 'waiting' then
    if lastSeen and (nowMs - lastSeen) > staleAfter then
      redis.call('HSET', userKey, 'state', 'evicting', 'evict_at', nowMs + graceMs)
      marked = marked + 1
    end
  end
  -- held / blocked / admitted 는 이 스윕의 대상이 아니다.
  -- 특히 blocked 를 여기서 건드리면 조치 파이프라인(§4)의 상태를 스윕이 덮어쓰게 된다.
end

local nextOffset = offset + #members
if nextOffset >= size then
  nextOffset = 0
end

return { #members, marked, removed, ghosts, nextOffset, size }
