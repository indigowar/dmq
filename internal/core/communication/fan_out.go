package communication

import "context"

func FanOut[T any](ctx context.Context, source <-chan T, consumers ...chan<- T) {
	for {
		select {
		case <-ctx.Done():
			return
		case v := <-source:
			for _, c := range consumers {
				c <- v
			}
		}
	}
}
