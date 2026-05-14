package main

import (
	"strings"

	"github.com/normahq/balda/internal/apps/balda"
	"github.com/normahq/balda/internal/logging"
)

type relayLoggingSettings struct {
	level string
	json  bool
}

func resolveRelayLoggingSettings(cfg balda.LoggerConfig, debugFlag, traceFlag bool) relayLoggingSettings {
	level := strings.TrimSpace(cfg.Level)
	if level == "" {
		level = logging.LevelInfo
	}
	if debugFlag {
		level = logging.LevelDebug
	}
	if traceFlag {
		level = logging.LevelTrace
	}

	return relayLoggingSettings{
		level: level,
		json:  !cfg.Pretty,
	}
}

func applyRelayLogging(cfg balda.LoggerConfig) error {
	settings := resolveRelayLoggingSettings(cfg, debug, trace)
	return logging.Init(
		logging.WithLevel(settings.level),
		logging.WithJson(settings.json),
	)
}
