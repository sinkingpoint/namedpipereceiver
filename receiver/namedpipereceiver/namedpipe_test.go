package namedpipereceiver

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer"
	"github.com/sinkingpoint/namedpipereceiver/pkg/operator/input/namedpipe"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg, "failed to create default config")
	require.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestReadPipe(t *testing.T) {
	f := NewFactory()

	sink := &consumertest.LogsSink{}

	cfg := testConfig()

	rcvr, err := f.CreateLogsReceiver(context.Background(), receivertest.NewNopCreateSettings(), cfg, sink)
	require.NoError(t, err, "failed to create receiver")
	require.NoError(t, rcvr.Start(context.Background(), componenttest.NewNopHost()))

	pipe, err := os.OpenFile("/tmp/testpipe", os.O_WRONLY, os.ModeNamedPipe)
	require.NoError(t, err, "failed to open pipe")

	write := func(s string) {
		_, err := pipe.WriteString(s)
		require.NoError(t, err, "failed to write to pipe")
	}

	write("hello world\n")
	write("some error\n")
	write("some error\n")

	require.Eventually(t, expectNLogs(sink, 3), 2*time.Second, 10*time.Millisecond,
		"expected %d but got %d logs",
		3, sink.LogRecordCount(),
	)

	require.NoError(t, rcvr.Shutdown(context.Background()))
}

func expectNLogs(sink *consumertest.LogsSink, expected int) func() bool {
	return func() bool { return sink.LogRecordCount() == expected }
}

func testConfig() *NamedPipeConfig {
	return &NamedPipeConfig{
		BaseConfig: adapter.BaseConfig{},
		InputConfig: func() namedpipe.Config {
			cfg := namedpipe.NewConfig()
			cfg.Config = func() namedpipeconsumer.Config {
				cfg := *namedpipeconsumer.NewConfig()
				cfg.Paths = []string{"/tmp/testpipe"}

				return cfg
			}()

			return *cfg
		}(),
	}
}
