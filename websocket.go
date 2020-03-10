package bitstamp

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

const bitstampWsUrl = "wss://ws.bitstamp.net"

type WsEvent struct {
	Event   string          `json:"event"`
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type WsClient struct {
	ws       *websocket.Conn
	done     chan bool
	sendLock sync.Mutex
	Stream   chan *WsEvent
	Errors   chan error
}

func NewWsClient() (*WsClient, error) {
	c := WsClient{
		done:   make(chan bool, 1),
		Stream: make(chan *WsEvent),
		Errors: make(chan error),
	}

	// set up websocket
	ws, _, err := websocket.DefaultDialer.Dial(bitstampWsUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("error dialing websocket: %w", err)
	}
	c.ws = ws

	go func() {
		defer c.ws.Close()
		for {
			select {
			case <-c.done:
				return
			default:
				var message []byte
				var err error
				_, message, err = c.ws.ReadMessage()
				if err != nil {
					c.Errors <- err
					continue
				}
				e := &WsEvent{}
				err = json.Unmarshal(message, e)
				if err != nil {
					c.Errors <- err
					continue
				}
				c.Stream <- e
			}
		}
	}()

	return &c, nil
}

func (c *WsClient) Close() {
	c.done <- true
}

func (c *WsClient) Subscribe(channels ...string) error {
	for _, channel := range channels {
		sub := WsEvent{
			Event: "bts:subscribe",
			Data:  json.RawMessage(fmt.Sprintf(`{"channel":"%s"}`, channel)),
		}
		if err := c.sendEvent(sub); err != nil {
			return err
		}
	}

	return nil
}

func (c *WsClient) Unsubscribe(channels ...string) error {
	for _, channel := range channels {
		sub := WsEvent{
			Event: "bts:unsubscribe",
			Data:  json.RawMessage(fmt.Sprintf(`{"channel":"%s"}`, channel)),
		}
		if err := c.sendEvent(sub); err != nil {
			return err
		}
	}

	return nil
}

func (c *WsClient) sendEvent(sub WsEvent) error {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()

	return c.ws.WriteJSON(&sub)
}
