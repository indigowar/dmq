package communication

import (
	"context"
	"errors"
)

var (
	ErrOperationIsCancelled = errors.New("operation is cancelled by context")
)

func Sync[In any, Out any](ctx context.Context, target chan<- Request[In, Out], arg In) (Out, error) {
	var emptyOutput Out

	req, out, err := NewRequest[In, Out](arg)

	target <- req
	select {
	case <-ctx.Done():
		return emptyOutput, ErrOperationIsCancelled
	case output := <-out:
		return output, nil
	case e := <-err:
		return emptyOutput, e
	}
}
