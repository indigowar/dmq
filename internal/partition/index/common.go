package index

type Pair struct {
	Key   int64 `json:"key"`
	Value int64 `json:"value"`
}

// pairSize is the size of Pair struct, which is two int64 -> 8 + 8
const pairSize = 16

// noResponse is used when operation does not return anything besides an error
type noResponse = struct{}
