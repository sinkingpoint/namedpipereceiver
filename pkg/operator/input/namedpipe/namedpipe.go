package namedpipe

import (
	"context"
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer"
)

// Input is an operator that monitors files for entries
type Input struct {
	helper.InputOperator

	namedPipeConsumer *namedpipeconsumer.Manager

	toBody toBodyFunc
}

// Start will start the file monitoring process
func (f *Input) Start(persister operator.Persister) error {
	return f.namedPipeConsumer.Start(persister)
}

// Stop will stop the file monitoring process
func (f *Input) Stop() error {
	return f.namedPipeConsumer.Stop()
}

func (f *Input) emit(ctx context.Context, token []byte, attrs map[string]any) error {
	if len(token) == 0 {
		return nil
	}

	ent, err := f.NewEntry(f.toBody(token))
	if err != nil {
		return fmt.Errorf("create entry: %w", err)
	}

	for k, v := range attrs {
		if err := ent.Set(entry.NewAttributeField(k), v); err != nil {
			f.Errorf("set attribute: %w", err)
		}
	}
	f.Write(ctx, ent)
	return nil
}
