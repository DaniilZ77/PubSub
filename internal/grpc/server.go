package grpc

import (
	"context"
	"log/slog"

	"github.com/DaniilZ77/vk-task/internal/service"
	pubsub "github.com/DaniilZ77/vk-task/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	pubsub.UnimplementedPubSubServer
	pubSubService service.SubPub
	log           *slog.Logger
}

func Register(
	pubSubServer *grpc.Server,
	pubSubService service.SubPub,
	log *slog.Logger,
) {
	pubsub.RegisterPubSubServer(pubSubServer, &server{
		pubSubService: pubSubService,
		log:           log,
	})
}

func (s *server) Publish(ctx context.Context, req *pubsub.PublishRequest) (*emptypb.Empty, error) {
	if err := s.pubSubService.Publish(req.Key, req.Data); err != nil {
		s.log.Error("failed to publish event", slog.Any("error", err))
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *server) Subscribe(req *pubsub.SubscribeRequest, stream grpc.ServerStreamingServer[pubsub.Event]) error {
	done := make(chan struct{}, 1)
	subs, err := s.pubSubService.Subscribe(req.Key, func(msg any) {
		if err := stream.Send(&pubsub.Event{Data: msg.(string)}); err != nil {
			s.log.Error("failed to send event", slog.Any("error", err))
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	if err != nil {
		s.log.Error("failed to subscribe to subject", slog.Any("error", err))
		return status.Error(codes.Internal, err.Error())
	}

	defer subs.Unsubscribe()
	select {
	case <-done:
		return status.Error(codes.Internal, "stream terminated unexpectedly")
	case <-stream.Context().Done():
		s.log.Warn("context cancelled")
		return stream.Context().Err()
	}
}
