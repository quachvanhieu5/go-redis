package redis_test

import (
	"context"
	"net"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("PubSub Reconnect", func() {
	var client *redis.Client
	var mu sync.Mutex
	var conns []net.Conn

	BeforeEach(func() {
		opts := redisOptions()
		oldDialer := opts.Dialer
		opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			var conn net.Conn
			var err error
			if oldDialer != nil {
				conn, err = oldDialer(ctx, network, addr)
			} else {
				conn, err = net.Dial(network, addr)
			}
			if err != nil {
				return nil, err
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			return conn, nil
		}
		client = redis.NewClient(opts)
		mu.Lock()
		conns = nil
		mu.Unlock()
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should automatically resubscribe after reconnecting", func() {
		ctx := context.Background()
		pubsub := client.Subscribe(ctx, "test_channel")
		defer pubsub.Close()

		ch := pubsub.Channel()

		// Verify we can receive a message
		go func() {
			time.Sleep(100 * time.Millisecond)
			err := client.Publish(ctx, "test_channel", "hello").Err()
			Expect(err).NotTo(HaveOccurred())
		}()

		var msg *redis.Message
		Eventually(ch).Should(Receive(&msg))
		Expect(msg.Payload).To(Equal("hello"))

		// Simulate network disruption by closing all active connections
		mu.Lock()
		for _, conn := range conns {
			conn.Close()
		}
		conns = nil
		mu.Unlock()

		// Publish a new message from a separate client
		pubClient := redis.NewClient(redisOptions())
		defer pubClient.Close()

		// Wait a bit for the pubsub client to detect the closed connection and attempt to reconnect.
		time.Sleep(500 * time.Millisecond)

		go func() {
			for i := 0; i < 10; i++ {
				_ = pubClient.Publish(ctx, "test_channel", "world").Err()
				time.Sleep(200 * time.Millisecond)
			}
		}()

		Eventually(ch).Should(Receive(&msg))
		Expect(msg.Payload).To(Equal("world"))
	})
})
