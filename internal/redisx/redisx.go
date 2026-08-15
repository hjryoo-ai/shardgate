// Package redisx 는 Redis 접속과 Lua 스크립트 실행을 감싼다.
//
// hot path 의 단일 진실은 Redis 이고(§7), 모든 큐 상태 전이는 Lua 로만 일어난다(불변식 1).
// 그래서 이 패키지는 "스크립트를 EVALSHA 로 캐시 실행하고, NOSCRIPT 면 다시 로드" 하는
// 책임만 갖는다. 큐 도메인 로직은 internal/queue 에 있다.
package redisx

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hjr/shardgate/internal/config"
)

// New 는 설정에 맞는 Redis 클라이언트를 만든다(단일 노드/Cluster 공통).
func New(cfg config.Redis) redis.UniversalClient {
	return redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:         cfg.Addrs,
		Password:      string(cfg.Password.Bytes()),
		DB:            cfg.DB,
		PoolSize:      cfg.PoolSize,
		DialTimeout:   3 * time.Second,
		ReadTimeout:   3 * time.Second,
		WriteTimeout:  3 * time.Second,
		IsClusterMode: cfg.Cluster,
	})
}

// Script 는 SHA 캐시를 들고 있는 로드된 Lua 스크립트다.
type Script struct {
	name string
	src  string
	sha  string
	once sync.Once
}

// NewScript 는 이름과 소스로 스크립트를 만든다. 첫 실행 때 로드된다.
func NewScript(name, src string) *Script {
	return &Script{name: name, src: src}
}

// Name 은 스크립트 이름(파일명)을 반환한다.
func (s *Script) Name() string { return s.name }

// Run 은 EVALSHA 로 실행하고, 스크립트 캐시가 비어 있으면 한 번 재로드한다.
func (s *Script) Run(ctx context.Context, rdb redis.Scripter, keys []string, args ...any) (any, error) {
	s.once.Do(func() { s.sha = sha1Hex(s.src) })

	res, err := rdb.EvalSha(ctx, s.sha, keys, args...).Result()
	if err != nil && isNoScript(err) {
		if _, lerr := rdb.ScriptLoad(ctx, s.src).Result(); lerr != nil {
			return nil, fmt.Errorf("load lua %s: %w", s.name, lerr)
		}
		res, err = rdb.EvalSha(ctx, s.sha, keys, args...).Result()
	}
	if err != nil {
		return nil, fmt.Errorf("run lua %s: %w", s.name, err)
	}
	return res, nil
}

func isNoScript(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "NOSCRIPT")
}
