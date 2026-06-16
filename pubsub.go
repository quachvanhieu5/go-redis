package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

type Message struct {
	Channel      string
	Pattern      string
	Payload      string
	PayloadSlice []string
}

type Subscription struct {
	Kind    string // "subscribe", "unsubscribe", "psubscribe", "punsubscribe"
	Channel string
	Count   int
}

type Pong struct {
	Payload string
}

type PubSub struct {
	opt *Options

	newConn   func(ctx context.Context, channels []string) (*pool.Conn, error)
	closeConn func(*pool.Conn) error

	mu       sync.Mutex
	cn       *pool.Conn
	channels map[string]struct{}
	patterns map[string]struct{}
	closed   bool
	exit     chan struct{}

	chOnce sync.Once
	ch     chan *Message
}

func (c *PubSub) init() {
	c.channels = make(map[string]struct{})
	c.patterns = make(map[string]struct{})
	c.exit = make(chan struct{})
}

func (c *PubSub) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("PubSub(%s)", mapKeys(c.channels))
}

func (c *PubSub) conn(ctx context.Context) (*pool.Conn, error) {
	if c.cn != nil {
		return c.cn, nil
	}

	cn, err := c.reconnect(ctx)
	if err != nil {
		return nil, err
	}

	c.cn = cn
	if err := c.resubscribeAll(ctx); err != nil {
		_ = c.closeConn(cn)
		c.cn = nil
		return nil, err
	}

	return cn, nil
}

func (c *PubSub) reconnect(ctx context.Context) (*pool.Conn, error) {
	cn, err := c.newConn(ctx, mapKeys(c.channels))
	if err != nil {
		return nil, err
	}
	return cn, nil
}

func (c *PubSub) resubscribe(ctx context.Context, err error) error { 
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return err
	}

	if err == nil {
		return nil
	}

	if c.cn != nil {
		_ = c.closeConn(c.cn)
		c.cn = nil
	}

	cn, err := c.reconnect(ctx)
	if err != nil {
		return err
	}

	c.cn = cn
	if err := c.resubscribeAll(ctx); err != nil {
		_ = c.closeConn(cn)
		c.cn = nil
		return err
	}

	return nil
}

func (c *PubSub) resubscribeAll(ctx context.Context) error {
	var firstErr error
	if len(c.channels) > 0 {
		channels := mapKeys(c.channels)
		if err := c._subscribe(ctx, "subscribe", channels); err != nil {
			firstErr = err
		}
	}
	if len(c.patterns) > 0 {
		patterns := mapKeys(c.patterns)
		if err := c._subscribe(ctx, "psubscribe", patterns); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *PubSub) _subscribe(ctx context.Context, cmd string, channels []string) error {
	cn, err := c.conn(ctx)
	if err != nil {
		return err
	}

	args := make([]interface{}, 1+len(channels))
	args[0] = cmd
	for i, channel := range channels {
		args[i+1] = channel
	}

	err = cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, args...)
	})
	if err != nil {
		_ = c.resubscribe(ctx, err)
		return err
	}

	return nil
}

func (c *PubSub) Subscribe(ctx context.Context, channels ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c._subscribe(ctx, "subscribe", channels)
	if err != nil {
		return err
	}

	for _, channel := range channels {
		c.channels[channel] = struct{}{}
	}
	return nil
}

func (c *PubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c._subscribe(ctx, "psubscribe", patterns)
	if err != nil {
		return err
	}

	for _, pattern := range patterns {
		c.patterns[pattern] = struct{}{}
	}
	return nil
}

func (c *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c._subscribe(ctx, "unsubscribe", channels)
	if err != nil {
		return err
	}

	for _, channel := range channels {
		delete(c.channels, channel)
	}
	return nil
}

func (c *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c._subscribe(ctx, "punsubscribe", patterns)
	if err != nil {
		return err
	}

	for _, pattern := range patterns {
		delete(c.patterns, pattern)
	}
	return nil
}

func (c *PubSub) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	close(c.exit)

	var err error
	if c.cn != nil {
		err = c.closeConn(c.cn)
		c.cn = nil
	}
	return err
}

func (c *PubSub) Ping(ctx context.Context, payload ...string) error {
	args := []interface{}{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	} else if len(payload) > 1 {
		return fmt.Errorf("redis: too many arguments for ping")
	}

	c.mu.Lock()
	cn, err := c.conn(ctx)
	c.mu.Unlock()
	if err != nil {
		return err
	}

	return cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmd(wr, args...)
	})
}

func (c *PubSub) Receive(ctx context.Context) (interface{}, error) {
	c.mu.Lock()
	cn, err := c.conn(ctx)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}

	msg, err := c.readMsg(cn)
	if err != nil {
		if err := c.resubscribe(ctx, err); err != nil {
			return nil, err
		}
		return c.Receive(ctx)
	}

	return msg, nil
}

func (c *PubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	for {
		msgi, err := c.Receive(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msgi.(type) {
		case *Subscription:
			// Ignore.
		case *Message:
			return msg, nil
		case *Pong:
			// Ignore.
		default:
			return nil, fmt.Errorf("redis: unknown message: %T", msgi)
		}
	}
}

func (c *PubSub) ReceiveTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	c.mu.Lock()
	cn, err := c.conn(ctx)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}

	msg, err := c.readMsgTimeout(cn, timeout)
	if err != nil {
		if err := c.resubscribe(ctx, err); err != nil {
			return nil, err
		}
		return c.ReceiveTimeout(ctx, timeout)
	}

	return msg, nil
}

func (c *PubSub) readMsg(cn *pool.Conn) (interface{}, error) {
	var msg interface{}
	err := cn.WithReader(context.Background(), 0, func(rd *proto.Reader) error {
		var err error
		msg, err = c.parseMsg(rd)
		return err
	})
	return msg, err
}

func (c *PubSub) readMsgTimeout(cn *pool.Conn, timeout time.Duration) (interface{}, error) {
	var msg interface{}
	err := cn.WithReader(context.Background(), timeout, func(rd *proto.Reader) error {
		var err error
		msg, err = c.parseMsg(rd)
		return err
	})
	return msg, err
}

func (c *PubSub) parseMsg(rd *proto.Reader) (interface{}, error) {
	name, err := rd.ReadArrayReply()
	if err != nil {
		return nil, err
	}

	switch name {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		channel, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		count, err := rd.ReadInt()
		if err != nil {
			return nil, err
		}

		return &Subscription{
			Kind:    name,
			Channel: channel,
			Count:   int(count),
		}, nil
	case "message":
		channel, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		payload, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		return &Message{
			Channel: channel,
			Payload: payload,
		}, nil
	case "pmessage":
		pattern, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		channel, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		payload, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		return &Message{
			Pattern: pattern,
			Channel: channel,
			Payload: payload,
		}, nil
	case "pong":
		payload, err := rd.ReadString()
		if err != nil {
			return nil, err
		}

		return &Pong{
			Payload: payload,
		}, nil
	default:
		return nil, fmt.Errorf("redis: unsupported direct message name: %q", name)
	}
}

func (c *PubSub) Channel() <-chan *Message {
	return c.ChannelSize(100)
}

func (c *PubSub) ChannelSize(size int) <-chan *Message {
	c.chOnce.Do(func() { 
		c.ch = make(chan *Message, size)
		go c.initChan()
	})
	return c.ch
}

func (c *PubSub) initChan() {
	ctx := context.TODO()
	for {
		msg, err := c.ReceiveMessage(ctx)
		if err != nil {
			if c.closed {
				break
			}
			internal.Logger.Printf(ctx, "redis: pubsub channel closed: %s", err)
			continue
		}
		c.ch <- msg
	}
	close(c.ch)
}

func mapKeys(m map[string]struct{}) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}
