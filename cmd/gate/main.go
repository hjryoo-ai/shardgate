// Command gate 는 대기열의 입구다. 진입 요청을 받아 PoW 챌린지를 발급하고,
// 풀이를 검증한 사용자에게 샤드를 배정해 큐 토큰을 발급한다(DESIGN.md §3.5, §4-L2·L3).
package main

import (
	"context"
	"log/slog"

	"github.com/hjr/shardgate/internal/api"
	"github.com/hjr/shardgate/internal/app"
	"github.com/hjr/shardgate/internal/botscore"
	"github.com/hjr/shardgate/internal/challenge"
	"github.com/hjr/shardgate/internal/queue"
	"github.com/hjr/shardgate/internal/shard"
	"github.com/hjr/shardgate/internal/telemetry"
	"github.com/hjr/shardgate/internal/token"
)

func main() { app.Main("gate", run) }

func run() error {
	a, err := app.New("gate", ":8080", "event_salt", "token_signing_key", "challenge_hmac_key")
	if err != nil {
		return err
	}
	defer func() { _ = a.Close() }()

	assigner, err := shard.NewAssigner(a.Cfg.Event.Salt, a.Cfg.Event.ShardCount, a.Cfg.Event.MaxShardCount)
	if err != nil {
		return err
	}
	store, err := queue.New(a.Redis, queue.FromConfig(a.Cfg), a.Log, a.Metrics)
	if err != nil {
		return err
	}
	tokens, err := token.NewIssuer(a.Cfg.Token)
	if err != nil {
		return err
	}

	// 난이도는 challenge 가 아니라 botscore 가 정한다(CLAUDE.md).
	// 의심도가 오른 지문·대역은 다음 진입에서 2^bump 배의 PoW 비용을 문다.
	difficulty := botscore.NewDifficulty(a.Redis, a.Cfg.Event.ID, a.Cfg.Challenge, a.Cfg.BotScore, a.Log)
	chal, err := challenge.NewIssuer(a.Redis, a.Cfg.Event.ID, a.Cfg.Challenge, difficulty)
	if err != nil {
		return err
	}

	// 팔의 정의가 여기서 갈리므로 실제로 적용된 값을 찍는다.
	// 설정 이름이 compose 에 없으면 export 해도 컨테이너에 닿지 않고, 아무것도
	// 실패하지 않은 채 팔만 조용히 바뀐다 — 그 상태로 한 번 측정한 적이 있다.
	// 측정 하네스(loadtest/tools/sweep.sh)가 이 줄로 팔을 확인한다.
	a.Log.Info("gate started",
		slog.Int("pow_base_difficulty", a.Cfg.Challenge.BaseDifficulty),
		slog.Int("pow_max_difficulty", a.Cfg.Challenge.MaxDifficulty),
		slog.Int("greylist_difficulty_bump", a.Cfg.BotScore.GreylistDifficulty),
		slog.Int("rechallenge_max_attempts", a.Cfg.BotScore.RechallengeMaxAttempts),
		slog.Int("rechallenge_pass_score", a.Cfg.BotScore.RechallengePassScore))

	tel := telemetry.NewProducer(a.Cfg.Kafka, a.Log, a.Metrics)
	defer func() { _ = tel.Close() }()

	api.NewGateAPI(chal, assigner, store, tokens, tel, a.Cfg, a.Log, a.Metrics).Register(a.Server.Mux())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go shard.NewGrower(assigner, store, a.Cfg.Event.ShardSize, a.Cfg.Admission.Interval, a.Log).Run(ctx)

	return a.Run(ctx)
}
