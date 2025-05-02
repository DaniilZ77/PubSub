package app

import (
	"context"
	"errors"
	"log/slog"
	"net"

	"github.com/DaniilZ77/PubSub/internal/config"
	pubSubGrpc "github.com/DaniilZ77/PubSub/internal/grpc"
	"github.com/DaniilZ77/PubSub/internal/service"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type App struct {
	server        *grpc.Server
	port          string
	pubSubService service.SubPub
	log           *slog.Logger
}

const defaultGrpcPort = ":50051"

func NewApp(config *config.Config, log *slog.Logger) (*App, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}
	if log == nil {
		return nil, errors.New("logger cannot be nil")
	}

	if config.GrpcPort == "" {
		config.GrpcPort = defaultGrpcPort
	}

	pubSubService, err := service.NewSubPub(config.QueueSize, log)
	if err != nil {
		return nil, err
	}

	var opts []grpc.ServerOption

	loggingOpts := []logging.Option{
		logging.WithLogOnEvents(
			logging.PayloadReceived,
			logging.PayloadSent,
		),
	}

	recoveryOpts := []recovery.Option{
		recovery.WithRecoveryHandler(func(p any) (err error) {
			log.Error("recovered from panic", slog.Any("error", p))
			return status.Error(codes.Internal, "internal error")
		}),
	}

	opts = append(opts, grpc.ChainUnaryInterceptor(
		recovery.UnaryServerInterceptor(recoveryOpts...),
		logging.UnaryServerInterceptor(interceptorLogger(log), loggingOpts...),
	))

	opts = append(opts, grpc.Creds(insecure.NewCredentials()))
	server := grpc.NewServer(opts...)
	pubSubGrpc.Register(server, pubSubService, log)

	return &App{
		server:        server,
		port:          config.GrpcPort,
		pubSubService: pubSubService,
		log:           log,
	}, nil
}

func (a *App) MustRun() {
	if err := a.Run(); err != nil {
		a.log.Warn("failed to run server", slog.Any("error", err))
	}
}

func (a *App) Run() error {
	listener, err := net.Listen("tcp", a.port)
	if err != nil {
		return err
	}
	a.log.Info("grpc server started", slog.String("port", a.port))
	if err := a.server.Serve(listener); err != nil {
		return err
	}

	return nil
}

func (a *App) Close(ctx context.Context) {
	a.server.GracefulStop()
	if err := a.pubSubService.Close(ctx); err != nil {
		a.log.Info("closed pub sub", slog.Any("error", err))
	}
}

func interceptorLogger(log *slog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		switch lvl {
		case logging.LevelDebug:
			log.DebugContext(ctx, msg, fields...)
		case logging.LevelInfo:
			log.InfoContext(ctx, msg, fields...)
		case logging.LevelWarn:
			log.WarnContext(ctx, msg, fields...)
		case logging.LevelError:
			log.ErrorContext(ctx, msg, fields...)
		default:
			log.Error("unknown level", slog.Any("level", lvl))
			panic("unknown level")
		}
	})
}
