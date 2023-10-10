package namedpipeconsumer

import (
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/decode"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/split"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/trim"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer/emit"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer/internal/splitter"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
)

const defaultEncoding = "utf-8"

type Config struct {
	Path        string          `mapstructure:"path"`
	Encoding    string          `mapstructure:"encoding,omitempty"`
	SplitConfig split.Config    `mapstructure:"multiline,omitempty"`
	MaxLogSize  helper.ByteSize `mapstructure:"max_log_size,omitempty"`
	FlushPeriod time.Duration   `mapstructure:"flush_period,omitempty"`
	TrimConfig  trim.Config     `mapstructure:",squash,omitempty"`
}

// NewConfig creates a new input config with default values
func NewConfig() *Config {
	return &Config{
		Encoding: defaultEncoding,
	}
}

func (c *Config) validate() error {
	return nil
}

// Build will build a file input operator from the supplied configuration
func (c Config) buildManager(logger *zap.SugaredLogger, emit emit.Callback, factory splitter.Factory) (*Manager, error) {
	if emit == nil {
		return nil, fmt.Errorf("must provide emit function")
	}

	enc, err := decode.LookupEncoding(c.Encoding)
	if err != nil {
		return nil, fmt.Errorf("failed to find encoding: %w", err)
	}

	return &Manager{
		SugaredLogger: logger.With("component", "namedpipeconsumer"),
		readerFactory: &readerFactory{
			SugaredLogger:   logger.With("component", "namedpipeconsumer"),
			readerConfig:    &readerConfig{emit: emit},
			splitterFactory: factory,
			encoding:        enc,
		},
		cancel: func() {},
	}, nil
}

// Build will build a file input operator from the supplied configuration
func (c Config) Build(logger *zap.SugaredLogger, emit emit.Callback) (*Manager, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	enc, err := decode.LookupEncoding(c.Encoding)
	if err != nil {
		return nil, err
	}

	splitFunc, err := c.SplitConfig.Func(enc, false, int(c.MaxLogSize))
	if err != nil {
		return nil, err
	}

	trimFunc := trim.Nop
	if enc != encoding.Nop {
		trimFunc = c.TrimConfig.Func()
	}

	// Ensure that splitter is buildable
	factory := splitter.NewFactory(splitFunc, trimFunc, c.FlushPeriod)
	return c.buildManager(logger, emit, factory)
}
