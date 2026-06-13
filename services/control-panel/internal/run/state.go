package run

import "errors"

const (
	DefaultEngineMode   = "normal"
	DefaultBotCount     = 10
	DefaultOrdersPerSec = 2
	DefaultDurationSec  = 5
	DefaultSeed         = 42
)

// Upper bounds on caller-supplied load. Without these, a request could ask for
// millions of bots/rate/duration and make the control panel spawn a fleet that
// exhausts host CPU/memory/FDs. The ceilings mirror submission-api and stay well
// above any real benchmark shape.
const (
	maxBotCount     = 5000
	maxOrdersPerSec = 2000
	maxDurationSec  = 300
)

func NormalizeRequest(req RunRequest) (RunRequest, error) {
	if req.TeamID == "" {
		req.TeamID = "local"
	}
	if req.EngineMode == "" {
		req.EngineMode = DefaultEngineMode
	}
	if req.BotCount == 0 {
		req.BotCount = DefaultBotCount
	}
	if req.OrdersPerSec == 0 {
		req.OrdersPerSec = DefaultOrdersPerSec
	}
	if req.DurationSec == 0 {
		req.DurationSec = DefaultDurationSec
	}
	if req.Seed == 0 {
		req.Seed = DefaultSeed
	}

	if req.EngineMode != "normal" && req.EngineMode != "broken-price-time-priority" {
		return req, errors.New("engine_mode must be normal or broken-price-time-priority")
	}
	if req.BotCount < 1 {
		return req, errors.New("bot_count must be greater than zero")
	}
	if req.BotCount > maxBotCount {
		req.BotCount = maxBotCount
	}
	if req.OrdersPerSec < 1 {
		return req, errors.New("orders_per_sec must be greater than zero")
	}
	if req.OrdersPerSec > maxOrdersPerSec {
		req.OrdersPerSec = maxOrdersPerSec
	}
	if req.DurationSec < 1 {
		return req, errors.New("duration_sec must be greater than zero")
	}
	if req.DurationSec > maxDurationSec {
		req.DurationSec = maxDurationSec
	}

	return req, nil
}

func IsTerminal(status Status) bool {
	return status == StatusFinished || status == StatusFailed || status == StatusCancelled
}
