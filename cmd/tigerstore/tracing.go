package main

import (
	"context"

	"go.etcd.io/etcd/server/v3/embed"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// maxSamplingRate is etcd's maxSamplingRatePerMillion.
const maxSamplingRate = 1000000

// newTracerProvider is etcd's newTracingExporter: spans go in batches to
// the OTLP collector at --distributed-tracing-address over gRPC without
// TLS, under the service name and instance ID of the flags, and a trace
// is sampled as its parent was or, at its root, rate times in a million.
func newTracerProvider(ctx context.Context, ec *embed.Config) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(ec.DistributedTracingAddress))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(ec.DistributedTracingServiceName)))
	if err == nil && ec.DistributedTracingServiceInstanceID != "" {
		res, err = resource.Merge(res, resource.NewSchemaless(semconv.ServiceInstanceIDKey.String(ec.DistributedTracingServiceInstanceID)))
	}
	if err != nil {
		return nil, err
	}
	sampler := sdktrace.NeverSample()
	if ec.DistributedTracingSamplingRatePerMillion > 0 {
		sampler = sdktrace.TraceIDRatioBased(float64(ec.DistributedTracingSamplingRatePerMillion) / maxSamplingRate)
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
