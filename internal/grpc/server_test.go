package grpc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"testing"

	"github.com/DaniilZ77/PubSub/internal/service"
	pubsub "github.com/DaniilZ77/PubSub/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type dependencies struct {
	pubSub       *service.MockSubPub
	subscription *service.MockSubscription
	conn         *grpc.ClientConn
	cleanup      func()
}

func newBufServer(t *testing.T) *dependencies {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pubSub := service.NewMockSubPub(t)
	subscription := service.NewMockSubscription(t)
	Register(srv, pubSub, slog.New(slog.DiscardHandler))
	go func() { _ = srv.Serve(lis) }()
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
	return &dependencies{
		pubSub:       pubSub,
		subscription: subscription,
		conn:         conn,
		cleanup:      cleanup,
	}
}

func TestPublish(t *testing.T) {
	t.Parallel()

	dependencies := newBufServer(t)
	defer dependencies.cleanup()

	dependencies.pubSub.EXPECT().Publish("key", "data").Return(nil).Once()

	client := pubsub.NewPubSubClient(dependencies.conn)

	_, err := client.Publish(context.Background(), &pubsub.PublishRequest{
		Key:  "key",
		Data: "data",
	})
	require.NoError(t, err)
}

func TestPublish_Failed(t *testing.T) {
	t.Parallel()

	dependencies := newBufServer(t)
	defer dependencies.cleanup()

	dependencies.pubSub.EXPECT().Publish(mock.Anything, mock.Anything).Return(errors.New("publish error")).Once()

	client := pubsub.NewPubSubClient(dependencies.conn)

	_, err := client.Publish(context.Background(), &pubsub.PublishRequest{})
	require.Error(t, err)
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	dependencies := newBufServer(t)
	defer dependencies.cleanup()

	client := pubsub.NewPubSubClient(dependencies.conn)

	const messagesCount = 10
	dependencies.pubSub.EXPECT().Subscribe("key", mock.MatchedBy(func(cb func(any)) bool {
		for i := range messagesCount {
			cb("data" + strconv.Itoa(i))
		}
		return true
	})).Return(dependencies.subscription, nil).Once()
	dependencies.subscription.EXPECT().Unsubscribe().Return().Once()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Subscribe(ctx, &pubsub.SubscribeRequest{Key: "key"})
	require.NoError(t, err)

	for i := range messagesCount {
		msg, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, "data"+strconv.Itoa(i), msg.Data)
	}
	cancel()
}

func TestSubscribe_Failed(t *testing.T) {
	t.Parallel()

	dependencies := newBufServer(t)
	defer dependencies.cleanup()

	client := pubsub.NewPubSubClient(dependencies.conn)

	dependencies.pubSub.EXPECT().Subscribe("key", mock.Anything).Return(nil, errors.New("subscribe error")).Once()

	stream, err := client.Subscribe(context.Background(), &pubsub.SubscribeRequest{Key: "key"})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err)
}
