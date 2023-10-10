package namedpipeconsumer

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Manager struct {
	*zap.SugaredLogger
	wg     sync.WaitGroup
	cancel context.CancelFunc

	paths   []string
	readers []*reader

	readerFactory *readerFactory
	persister     operator.Persister
}

func (m *Manager) Start(persister operator.Persister) error {
	ctx, cancel := context.WithCancel(context.Background())

	m.cancel = cancel
	m.persister = persister

	m.readers = make([]*reader, len(m.paths))

	var readerErr error

	for i := range m.paths {
		file, err := os.OpenFile(m.paths[i], os.O_RDONLY, os.ModeNamedPipe)
		if err != nil {
			readerErr = multierr.Append(err, fmt.Errorf("failed to open file %q: %w", m.paths[i], err))
		}

		m.readers[i], err = m.readerFactory.newReader(file)
		if err != nil {
			m.readers[i] = nil
			file.Close()
			readerErr = multierr.Append(err, fmt.Errorf("failed to create reader for file %q: %w", m.paths[i], err))
		}
	}

	if readerErr != nil {
		return readerErr
	}

	for _, r := range m.readers {
		m.startPipe(ctx, r)
	}

	return nil
}

func (m *Manager) startPipe(ctx context.Context, reader *reader) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := reader.Run(ctx); err != nil {
			m.Errorf("failed to run reader: %w", err)
			m.cancel()
		}
	}()
}

func (m *Manager) Stop() error {
	m.cancel()

	for _, r := range m.readers {
		r.Close()
	}

	m.wg.Wait()
	return nil
}
