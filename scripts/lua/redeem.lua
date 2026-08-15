-- redeem.lua — 입장 토큰을 소각한다(불변식 2: "입장 토큰은 redeem 시 반드시 소각").
--
-- 서명이 유효한 것만으로는 부족하다. 서명은 위조를 막을 뿐 복제를 막지 못하고,
-- 5분 TTL 안에 같은 토큰을 여러 번 쓰면 1인 1구매가 무너진다. 그래서 Redis 에
-- 발행 기록을 두고, 구매 시점에 그 기록을 지우는 것으로 "1회용"을 강제한다.
-- 조회와 삭제 사이에 다른 요청이 끼어들 수 없어야 하므로 한 번의 실행으로 끝낸다.
--
-- KEYS[1] entry:{event:shard}:<jti>
-- KEYS[2] user:{event:shard}:<token>
--
-- ARGV[1] token_id    입장 토큰이 가리키는 큐 토큰 ID
-- ARGV[2] now_ms
--
-- 반환: { status, owner }
--   burned   : 소각 성공. 이 호출만 구매를 진행할 수 있다.
--   missing  : 이미 쓰였거나 TTL 이 지났다.
--   mismatch : 서명은 유효하지만 발행 기록의 주인이 다르다 — 탈취 시도의 신호다.

local entryKey = KEYS[1]
local userKey = KEYS[2]

local tokenID = ARGV[1]
local nowMs = tonumber(ARGV[2])

local owner = redis.call('GET', entryKey)
if not owner then
  return { 'missing', '' }
end
if owner ~= tokenID then
  return { 'mismatch', owner }
end

redis.call('DEL', entryKey)
-- 상태는 admitted 그대로 두고 시각만 남긴다. 상태 값을 늘리는 대신 필드를 더하는 쪽이
-- §3.3 의 상태 집합을 그대로 유지한다.
redis.call('HSET', userKey, 'redeemed_at', nowMs)

return { 'burned', owner }
