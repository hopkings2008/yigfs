package types

type Message struct {
	Topic   string
	Key     string
	ErrChan chan error
	Value   []byte
}