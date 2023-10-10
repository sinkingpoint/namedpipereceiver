package emit

import (
	"context"
)

type Callback func(ctx context.Context, token []byte, attrs map[string]any) error
