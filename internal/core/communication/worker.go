package communication

import (
	"context"
)

func Worker[In any, Out any](ctx context.Context, action func(context.Context, In) (Out, error)) chan Request[In, Out] {
	requestChannel := make(chan Request[In, Out])

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case request := <-requestChannel:
				output, err := action(ctx, request.Input)
				if err != nil {
					request.Error <- err
				} else {
					request.Output <- output
				}

				close(request.Output)
				close(request.Error)
			}
		}
	}()

	return requestChannel
}

func Workers[In any, Out any](ctx context.Context, action func(context.Context, In) (Out, error), count int) chan Request[In, Out] {
	requestChannel := make(chan Request[In, Out])

	for i := 0; i != count; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case request := <-requestChannel:
					output, err := action(ctx, request.Input)
					if err != nil {
						request.Error <- err
					} else {
						request.Output <- output
					}

					close(request.Output)
					close(request.Error)
				}
			}
		}()
	}

	return requestChannel
}
