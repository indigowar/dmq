package topic

type Topic struct {
	Name      string `json:"name"`
	Partition int64  `json:"partition"`
}
