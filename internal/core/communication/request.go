package communication

type Request[In any, Out any] struct {
	Input  In
	Output chan<- Out
	Error  chan<- error
}

func NewRequest[In any, Out any](input In) (Request[In, Out], <-chan Out, <-chan error) {
	out := make(chan Out)
	err := make(chan error)

	return Request[In, Out]{
		Input:  input,
		Output: out,
		Error:  err,
	}, out, err
}
