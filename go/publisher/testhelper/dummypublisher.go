package testhelper

type DummyPublisher struct {
	PublishedMessages []DummyMessage
}

type DummyMessage struct {
	AppEnv         string
	RoutingKey     string
	Data           []byte
	CompressedWith uint8
}

func NewDummyPublisher() *DummyPublisher {
	return &DummyPublisher{
		PublishedMessages: []DummyMessage{},
	}
}

func (p *DummyPublisher) Publish(appEnv string, routingKey string, data []byte, compressedWith uint8) {
	p.PublishedMessages = append(p.PublishedMessages, DummyMessage{
		AppEnv:         appEnv,
		RoutingKey:     routingKey,
		Data:           data,
		CompressedWith: compressedWith,
	})
}
