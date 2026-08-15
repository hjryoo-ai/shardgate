-- ShardGate 영속 스키마 (DESIGN.md §6)
--
-- Redis 가 hot path 의 단일 진실이고, PostgreSQL 은 감사·분석·차단 근거 보존용이다.
-- 개인정보 원칙(불변식 6, §12-2): 핑거프린트는 해시만, IP 는 프리픽스만 저장한다.
-- 원본 지문/전체 IP 컬럼은 이 스키마에 존재하지 않는다.

BEGIN;

CREATE TABLE IF NOT EXISTS events (
    id                  TEXT PRIMARY KEY,
    name                TEXT        NOT NULL,
    open_at             TIMESTAMPTZ NOT NULL,
    -- event_salt 원문이 아니라 해시만 보관한다(§3.1: salt 는 오픈 전까지 비공개).
    salt_hash           TEXT        NOT NULL,
    shard_size          INTEGER     NOT NULL CHECK (shard_size > 0),
    admit_rate_per_min  INTEGER     NOT NULL CHECK (admit_rate_per_min > 0),
    lottery_window_sec  INTEGER     NOT NULL CHECK (lottery_window_sec >= 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 조치 파이프라인의 모든 전이를 남긴다(관찰 → greylist → 보류 → 차단, 그리고 복귀).
CREATE TABLE IF NOT EXISTS queue_audit (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id    TEXT        NOT NULL,
    token_id    TEXT        NOT NULL,
    shard_id    TEXT        NOT NULL,
    action      TEXT        NOT NULL,
    score       SMALLINT    NOT NULL CHECK (score BETWEEN 0 AND 100),
    reason_json JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS queue_audit_event_created_idx ON queue_audit (event_id, created_at DESC);
CREATE INDEX IF NOT EXISTS queue_audit_token_idx         ON queue_audit (event_id, token_id);
CREATE INDEX IF NOT EXISTS queue_audit_action_idx        ON queue_audit (event_id, action);

-- 차단 이력. 차단은 되돌리기 어려우므로 근거(evidence_json)를 반드시 함께 남긴다(§4).
CREATE TABLE IF NOT EXISTS blocks (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id       TEXT        NOT NULL,
    token_id       TEXT        NOT NULL,
    shard_id       TEXT        NOT NULL,
    fp_hash        TEXT        NOT NULL,
    score          SMALLINT    NOT NULL CHECK (score BETWEEN 0 AND 100),
    evidence_json  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, token_id)
);

CREATE INDEX IF NOT EXISTS blocks_event_created_idx ON blocks (event_id, created_at DESC);
CREATE INDEX IF NOT EXISTS blocks_fp_idx            ON blocks (event_id, fp_hash);

-- mock shop 주문. 1인 1구매 + 멱등키(불변식 4, §4 "입장 이후 방어").
CREATE TABLE IF NOT EXISTS orders (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        TEXT        NOT NULL,
    token_id        TEXT        NOT NULL,
    account_id      TEXT        NOT NULL,
    entry_jti       TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    sku             TEXT        NOT NULL,
    qty             INTEGER     NOT NULL CHECK (qty > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, idempotency_key),
    -- 1인 1구매: 계정 기준과 큐 토큰 기준 모두를 DB 제약으로 강제한다.
    UNIQUE (event_id, account_id),
    UNIQUE (event_id, token_id),
    -- 입장 토큰(jti)은 1회용이다. Redis 소각과 별개로 DB 에서도 재사용을 막는다.
    UNIQUE (event_id, entry_jti)
);

COMMIT;
