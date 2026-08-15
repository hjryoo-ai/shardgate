-- enqueue.lua — 대기열 진입. 샤드 내 순번을 원자적으로 부여한다.
--
-- 공정성 모델(DESIGN.md §3.2): 순수 FIFO 는 "네트워크가 빠른 자"에게 유리하고,
-- 그건 곧 봇에게 유리하다는 뜻이다. 그래서 두 구간을 score 밴드로 나눈다.
--
--   추첨 밴드 [0, lottery_band)      오픈 후 lottery_end 까지 진입 → 도착 순서와 무관한 난수 순번
--   FIFO 밴드 [lottery_band, ...)    그 이후 진입 → INCR(seq) 로 도착순, 추첨 구간 전원 뒤에 붙는다
--
-- 난수는 Lua 가 아니라 서버(Go)가 CSPRNG 로 만들어 ARGV 로 넘긴다.
-- Lua 쪽 난수는 예측 가능하고, 무엇보다 "순번은 서버가 유일한 진실"이어야 하기 때문이다.
--
-- 멱등(불변식 4): 같은 token_id 로 다시 호출해도 이미 받은 순번은 바뀌지 않는다.
-- 재시도·중복 요청이 순번을 앞당기는 일이 없어야 한다.
--
-- KEYS[1] queue:{event:shard}          대기열 ZSET
-- KEYS[2] seq:{event:shard}            FIFO 순번 카운터
-- KEYS[3] user:{event:shard}:<token>   사용자 상태 HASH
--
-- ARGV[1] token_id
-- ARGV[2] shard
-- ARGV[3] now_ms
-- ARGV[4] lottery_end_ms
-- ARGV[5] lottery_rand      [0, lottery_band) 범위의 정수
-- ARGV[6] lottery_band      두 구간을 가르는 경계
-- ARGV[7] fp_hash           원본 지문이 아닌 해시만 저장한다(불변식 6)
-- ARGV[8] ip_prefix         전체 IP 가 아닌 프리픽스만 저장한다(불변식 6)
-- ARGV[9] user_ttl_ms
--
-- 반환: { status, rank, size, rank_score, segment }
--   status: created | exists | held | blocked | admitted
--   rank  : 0-based 큐 내 위치. 큐에 없으면 -1

local queueKey = KEYS[1]
local seqKey = KEYS[2]
local userKey = KEYS[3]

local tokenID = ARGV[1]
local shard = ARGV[2]
local nowMs = tonumber(ARGV[3])
local lotteryEndMs = tonumber(ARGV[4])
local lotteryRand = tonumber(ARGV[5])
local lotteryBand = tonumber(ARGV[6])
local fpHash = ARGV[7]
local ipPrefix = ARGV[8]
local userTTLms = tonumber(ARGV[9])

local function snapshot(status, rankScore, segment)
  local rank = redis.call('ZRANK', queueKey, tokenID)
  if rank == false then rank = -1 end
  return { status, rank, redis.call('ZCARD', queueKey), rankScore, segment }
end

local prev = redis.call('HMGET', userKey, 'state', 'orig_rank', 'segment')
local state = prev[1]

if state then
  local rankScore = tonumber(prev[2]) or -1
  local segment = prev[3] or ''

  if state == 'waiting' or state == 'evicting' then
    -- 이미 줄 서 있다. 순번은 건드리지 않고 생존 신호만 갱신한다.
    redis.call('HSET', userKey, 'last_seen', nowMs)
    redis.call('PEXPIRE', userKey, userTTLms)
    return snapshot('exists', rankScore, segment)
  end

  if state ~= 'evicted' then
    -- held / blocked / admitted 는 재진입 대상이 아니다. 현재 상태를 그대로 알린다.
    return snapshot(state, rankScore, segment)
  end

  -- evicted: 스스로 자리를 비운 사용자다. 재진입은 허용하되 원래 순번은 복원하지 않는다
  -- (복원하면 "빠져 있다가 돌아오면 이득"이 되어 대기 자체를 무의미하게 만든다).
  redis.call('DEL', userKey)
end

local rankScore
local segment
if nowMs < lotteryEndMs then
  rankScore = lotteryRand
  segment = 'lottery'
else
  rankScore = lotteryBand + redis.call('INCR', seqKey)
  segment = 'fifo'
end

redis.call('ZADD', queueKey, rankScore, tokenID)
redis.call('HSET', userKey,
  'state', 'waiting',
  'shard', shard,
  'orig_shard', shard,
  'orig_rank', rankScore,
  'segment', segment,
  'score', 0,
  'fp_hash', fpHash,
  'ip_prefix', ipPrefix,
  'joined_at', nowMs,
  'last_seen', nowMs,
  'hb_count', 0)
redis.call('PEXPIRE', userKey, userTTLms)

return snapshot('created', rankScore, segment)
