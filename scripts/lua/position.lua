-- position.lua — 순번·대기 인원 원자 스냅샷.
--
-- 순번은 서버가 유일한 진실이다. 클라이언트가 보낸 값은 어떤 경우에도 신뢰하지 않고,
-- 여기서 읽은 ZRANK/ZCARD 만 쓴다. 여러 명령으로 쪼개면 그 사이에 앞사람이 빠져나가
-- "내 앞 인원 > 전체 인원" 같은 모순된 화면이 나올 수 있으므로 한 번에 읽는다.
--
-- 예상 대기 시간은 여기서 계산하지 않는다. 남은 시간은 drain rate(샤드 예산 배분)의
-- 함수인데, 그 값은 admission 쪽 상태이고 샤드 간 비교가 필요해 이 슬롯 안에서
-- 원자적으로 알 수 없다. 대신 rank/size 라는 원자 스냅샷을 주고, 서버(Go)가
-- 자신이 아는 실효 admit rate 로 환산한다 — 여전히 클라이언트 값은 개입하지 않는다.
--
-- KEYS[1] queue:{event:shard}
-- KEYS[2] hold:{event:shard}
-- KEYS[3] user:{event:shard}:<token>
--
-- ARGV[1] token_id
--
-- 반환: { state, rank, size, hold_rank, hold_size, orig_rank, segment, joined_at, last_seen,
--         score, rc_count, observe_from }
--   rc_count: 지금까지 통과한 재챌린지 횟수(§4). 남은 기회를 알려 주는 데 쓴다.
--   observe_from: 최소 관찰 게이트의 기산점. 복귀하면 되감기므로 joined_at 과 갈라진다.
--     예상 대기 표시가 이 값을 모르면 복귀한 사용자에게만 남은 시간을 짧게 말한다.
--   state: unknown 이면 이 샤드에 그런 토큰이 없다는 뜻이다.
--   rank / hold_rank: 0-based, 해당 집합에 없으면 -1

local queueKey = KEYS[1]
local holdKey = KEYS[2]
local userKey = KEYS[3]

local tokenID = ARGV[1]

local function rankOf(key)
  local r = redis.call('ZRANK', key, tokenID)
  if r == false then return -1 end
  return r
end

local h = redis.call('HMGET', userKey,
  'state', 'orig_rank', 'segment', 'joined_at', 'last_seen', 'score', 'rc_count', 'observe_from')

local state = h[1]
if not state then
  -- 상태 해시가 없으면 대기열에 남아 있는 항목도 유령이다. 위치를 만들어 주지 않는다.
  return { 'unknown', -1, redis.call('ZCARD', queueKey), -1, redis.call('ZCARD', holdKey), -1, '', -1, -1, -1, 0, -1 }
end

return {
  state,
  rankOf(queueKey),
  redis.call('ZCARD', queueKey),
  rankOf(holdKey),
  redis.call('ZCARD', holdKey),
  tonumber(h[2]) or -1,
  h[3] or '',
  tonumber(h[4]) or -1,
  tonumber(h[5]) or -1,
  tonumber(h[6]) or 0,
  tonumber(h[7]) or 0,
  tonumber(h[8]) or -1,
}
