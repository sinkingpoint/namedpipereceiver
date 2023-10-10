package namedpipeconsumer

import (
	"bufio"
	"os"
	"path/filepath"
	"runtime"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/decode"
	"github.com/sinkingpoint/namedpipereceiver/pkg/namedpipeconsumer/internal/splitter"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
)

const (
	logFileName         = "log.file.name"
	logFilePath         = "log.file.path"
	logFileNameResolved = "log.file.name.resolved"
	logFilePathResolved = "log.file.path.resolved"
)

type readerFactory struct {
	*zap.SugaredLogger
	readerConfig    *readerConfig
	splitterFactory splitter.Factory
	encoding        encoding.Encoding
}

func (f *readerFactory) newReader(file *os.File) (*reader, error) {
	return f.build(file, f.splitterFactory.SplitFunc())
}

func (f *readerFactory) build(file *os.File, lineSplitFunc bufio.SplitFunc) (r *reader, err error) {
	r = &reader{
		readerConfig:  f.readerConfig,
		attributes:    map[string]any{},
		file:          file,
		fileName:      file.Name(),
		SugaredLogger: f.SugaredLogger.With("path", file.Name()),
		decoder:       decode.New(f.encoding),
		lineSplitFunc: lineSplitFunc,
	}

	// Resolve file name and path attributes
	resolved := r.fileName

	// Dirty solution, waiting for this permanent fix https://github.com/golang/go/issues/39786
	// EvalSymlinks on windows is partially working depending on the way you use Symlinks and Junctions
	if runtime.GOOS != "windows" {
		resolved, err = filepath.EvalSymlinks(r.fileName)
		if err != nil {
			f.Errorf("resolve symlinks: %w", err)
		}
	}
	abs, err := filepath.Abs(resolved)
	if err != nil {
		f.Errorf("resolve abs: %w", err)
	}

	if f.readerConfig.includeFileName {
		r.attributes[logFileName] = filepath.Base(r.fileName)
	} else if r.attributes[logFileName] != nil {
		delete(r.attributes, logFileName)
	}
	if f.readerConfig.includeFilePath {
		r.attributes[logFilePath] = r.fileName
	} else if r.attributes[logFilePath] != nil {
		delete(r.attributes, logFilePath)
	}
	if f.readerConfig.includeFileNameResolved {
		r.attributes[logFileNameResolved] = filepath.Base(abs)
	} else if r.attributes[logFileNameResolved] != nil {
		delete(r.attributes, logFileNameResolved)
	}
	if f.readerConfig.includeFilePathResolved {
		r.attributes[logFilePathResolved] = abs
	} else if r.attributes[logFilePathResolved] != nil {
		delete(r.attributes, logFilePathResolved)
	}

	return r, nil
}
