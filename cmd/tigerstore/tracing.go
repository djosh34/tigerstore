package main

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// maxSamplingRate is etcd's maxSamplingRatePerMillion.
const maxSamplingRate = 1000000

// validateTracing is etcd's check of --distributed-tracing-sampling-rate.
func validateTracing(rate int) error {
	switch {
	case rate < 0:
		return errors.New("distributed tracing configurition is not valid: (tracing sampling rate must be positive)")
	case rate > maxSamplingRate:
		return fmt.Errorf("distributed tracing configurition is not valid: (tracing sampling rate must be less than %d)", maxSamplingRate)
	}
	return nil
}

// newTracerProvider is etcd's newTracingExporter: spans go in batches to
// the OTLP collector at --distributed-tracing-address over gRPC without
// TLS, under the service name and instance ID of the flags, and a trace
// is sampled as its parent was or, at its root, rate times in a million.
func newTracerProvider(ctx context.Context, f *flags) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(f.tracingAddress))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(f.tracingService)))
	if err == nil && f.tracingInstance != "" {
		res, err = resource.Merge(res, resource.NewSchemaless(semconv.ServiceInstanceIDKey.String(f.tracingInstance)))
	}
	if err != nil {
		return nil, err
	}
	sampler := sdktrace.NeverSample()
	if f.tracingRate > 0 {
		sampler = sdktrace.TraceIDRatioBased(float64(f.tracingRate) / maxSamplingRate)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sampler)),
	), nil
}

// tracerProvider is tp as the server takes it: nil without tracing.
func tracerProvider(tp *sdktrace.TracerProvider) trace.TracerProvider {
	if tp == nil {
		return nil
	}
	return tp
}
