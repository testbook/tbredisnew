package hooks

import (
	"context"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/testbook/tbredis/utils"
)

var (
	labelNames = []string{"key", "command"}
)

type (
	Hook struct {
		options           *Options
		singleCommands    *prometheus.HistogramVec
		pipelinedCommands *prometheus.CounterVec
		singleErrors      *prometheus.CounterVec
		pipelinedErrors   *prometheus.CounterVec
	}
	startKey struct{}
)

func (hook *Hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (hook *Hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		ctx, err := hook.BeforeProcess(ctx, cmd)
		if err != nil {
			return err
		}
		err = next(ctx, cmd)
		if err != nil {
			return err
		}
		return hook.AfterProcess(ctx, cmd)
	}
}

func (hook *Hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmd []redis.Cmder) error {
		ctx, err := hook.BeforeProcessPipeline(ctx, cmd)
		if err != nil {
			return err
		}
		err = next(ctx, cmd)
		if err != nil {
			return err
		}
		return hook.AfterProcessPipeline(ctx, cmd)
	}
}

// NewHook creates a new go-redis hook instance and registers Prometheus collectors.
func NewHook(opts ...Option) *Hook {
	options := DefaultOptions()
	options.Merge(opts...)

	singleCommands := register(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: options.Namespace,
		Name:      "redis_single_commands",
		Help:      "Histogram of single Redis commands",
		Buckets:   options.DurationBuckets,
	}, labelNames)).(*prometheus.HistogramVec)

	pipelinedCommands := register(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Name:      "redis_pipelined_commands",
		Help:      "Number of pipelined Redis commands",
	}, labelNames)).(*prometheus.CounterVec)

	singleErrors := register(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Name:      "redis_single_errors",
		Help:      "Number of single Redis commands that have failed",
	}, labelNames)).(*prometheus.CounterVec)

	pipelinedErrors := register(prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: options.Namespace,
		Name:      "redis_pipelined_errors",
		Help:      "Number of pipelined Redis commands that have failed",
	}, labelNames)).(*prometheus.CounterVec)

	return &Hook{
		options:           options,
		singleCommands:    singleCommands,
		pipelinedCommands: pipelinedCommands,
		singleErrors:      singleErrors,
		pipelinedErrors:   pipelinedErrors,
	}
}

func (hook *Hook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, startKey{}, time.Now()), nil
}

func (hook *Hook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if start, ok := ctx.Value(startKey{}).(time.Time); ok {
		duration := time.Since(start).Seconds()
		hook.singleCommands.WithLabelValues(utils.GetKeyTemplate(cmd.String()), cmd.Name()).Observe(duration)
	}

	if isActualErr(cmd.Err()) {
		hook.singleErrors.WithLabelValues(utils.GetKeyTemplate(cmd.String()), cmd.Name()).Inc()
	}

	return nil
}

func (hook *Hook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, startKey{}, time.Now()), nil
}

func (hook *Hook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return hook.AfterProcess(ctx, redis.NewCmd(ctx, "pipeline"))
}

func register(collector prometheus.Collector) prometheus.Collector {
	err := prometheus.DefaultRegisterer.Register(collector)
	if err == nil {
		return collector
	}

	if arErr, ok := err.(prometheus.AlreadyRegisteredError); ok {
		return arErr.ExistingCollector
	}

	return collector
}

func isActualErr(err error) bool {
	return err != nil && err != redis.Nil
}
