-- refill_budget.lua — 샤드에 이번 주기의 입장 예산을 채운다(DESIGN.md §3.4).
--
-- 글로벌 admit rate 를 Admission Controller 가 샤드별로 쪼개 여기로 내려보낸다.
-- 쪼개는 비율(잔여 인원 비례, greylist 가중 하향)은 컨트롤러가 정하고, 이 스크립트는
-- "정해진 몫을 원자적으로 반영"하는 일만 한다.
--
-- 두 개의 상한을 둔다:
--   cap     — 한 샤드가 한 번에 쏟아낼 수 있는 인원의 상한. 미사용 예산이 무한히 쌓여
--             나중에 한꺼번에 터지면 다운스트림(재고/결제)이 그 버스트를 그대로 맞는다.
--   waiting — 실제 대기 인원. 아무도 없는 샤드에 예산을 쌓아 둘 이유가 없고,
--             쌓아 두면 나중에 그 샤드로 들어온 사람이 줄도 안 서고 통과한다.
--
-- KEYS[1] budget:{event:shard}
-- KEYS[2] queue:{event:shard}
--
-- ARGV[1] grant       이번 주기에 배분된 몫
-- ARGV[2] cap         샤드당 예산 상한
-- ARGV[3] ttl_ms      예산 키 TTL (배분이 멈추면 자연 소멸)
--
-- 반환: { budget, waiting }

local budgetKey = KEYS[1]
local queueKey = KEYS[2]

local grant = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local ttlMs = tonumber(ARGV[3])

local waiting = redis.call('ZCARD', queueKey)
local budget = (tonumber(redis.call('GET', budgetKey)) or 0) + grant

if budget > cap then budget = cap end
if budget > waiting then budget = waiting end
if budget < 0 then budget = 0 end

redis.call('SET', budgetKey, budget, 'PX', ttlMs)

return { budget, waiting }
