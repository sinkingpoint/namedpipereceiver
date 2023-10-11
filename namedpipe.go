package namedpipereceiver

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/sinkingpoint/namedpipereceiver/internal/metadata"
	"github.com/sinkingpoint/namedpipereceiver/pkg/operator/input/namedpipe"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
)

func NewFactory() receiver.Factory {
	return adapter.NewFactory(ReceiverType{}, metadata.LogsStability)
}

type ReceiverType struct{}

func (r ReceiverType) Type() component.Type {
	return metadata.Type
}

func (f ReceiverType) CreateDefaultConfig() component.Config {
	return &NamedPipeConfig{
		BaseConfig: adapter.BaseConfig{
			Operators: []operator.Config{},
		},
		InputConfig: *namedpipe.NewConfig(),
	}
}

func (f ReceiverType) BaseConfig(cfg component.Config) adapter.BaseConfig {
	return cfg.(*NamedPipeConfig).BaseConfig
}

func (f ReceiverType) InputConfig(cfg component.Config) operator.Config {
	return operator.NewConfig(&cfg.(*NamedPipeConfig).InputConfig)
}

type NamedPipeConfig struct {
	InputConfig        namedpipe.Config `mapstructure:",squash"`
	adapter.BaseConfig `mapstructure:",squash"`
}
