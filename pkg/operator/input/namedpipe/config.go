package namedpipe

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/decode"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer"
	"go.uber.org/zap"
)

const operatorType = "named_pipe_input"

func init() {
	operator.Register(operatorType, func() operator.Builder { return NewConfig() })
}

// NewConfig creates a new input config with default values
func NewConfig() *Config {
	return NewConfigWithID(operatorType)
}

// NewConfigWithID creates a new input config with default values
func NewConfigWithID(operatorID string) *Config {
	return &Config{
		InputConfig: helper.NewInputConfig(operatorID, operatorType),
		Config:      *namedpipeconsumer.NewConfig(),
	}
}

type Config struct {
	helper.InputConfig       `mapstructure:",squash"`
	namedpipeconsumer.Config `mapstructure:",squash"`
}

func (c *Config) Build(logger *zap.SugaredLogger) (operator.Operator, error) {
	inputOperator, err := c.InputConfig.Build(logger)
	if err != nil {
		return nil, err
	}

	var toBody toBodyFunc = func(token []byte) interface{} {
		return string(token)
	}

	if decode.IsNop(c.Config.Encoding) {
		toBody = func(token []byte) interface{} {
			copied := make([]byte, len(token))
			copy(copied, token)
			return copied
		}
	}

	input := &Input{
		InputOperator: inputOperator,
		toBody:        toBody,
	}

	input.namedPipeConsumer, err = c.Config.Build(logger, input.emit)
	if err != nil {
		return nil, err
	}

	return input, nil
}

type toBodyFunc func([]byte) interface{}
