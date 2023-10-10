package namedpipeconsumer

import (
	"bufio"
	"context"
	"os"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/decode"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer/emit"
	"go.uber.org/zap"
)

type readerConfig struct {
	emit                    emit.Callback
	maxLogSize              int
	includeFileName         bool
	includeFilePath         bool
	includeFileNameResolved bool
	includeFilePathResolved bool
}

type reader struct {
	*zap.SugaredLogger
	file          *os.File
	fileName      string
	lineSplitFunc bufio.SplitFunc
	decoder       *decode.Decoder
	readerConfig  *readerConfig

	attributes map[string]any
}

func (r *reader) Run(ctx context.Context) error {
	scanner := bufio.NewScanner(r.file)
	scanner.Split(r.lineSplitFunc)

	for scanner.Scan() {
		token, err := r.decoder.Decode(scanner.Bytes())
		if err != nil {
			r.Errorw("Failed to decode token", zap.Error(err))
			continue
		}

		if r.readerConfig.maxLogSize > 0 && len(token) > r.readerConfig.maxLogSize {
			token = token[:r.readerConfig.maxLogSize]
		}

		if err := r.readerConfig.emit(ctx, token, r.attributes); err != nil {
			r.Errorw("Failed to emit token", zap.Error(err))
			continue
		}
	}

	return scanner.Err()
}

func (r *reader) Close() {
	if r.file != nil {
		if err := r.file.Close(); err != nil {
			r.Debugw("Problem closing reader", zap.Error(err))
		}
		r.file = nil
	}
}
