package io.opencensus.impl.trace;

import io.opencensus.common.Clock;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Link;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.TraceId;
import io.opencensus.trace.TraceOptions;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.ExportComponent;
import io.opencensus.trace.propagation.PropagationComponent;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class TraceComponentImpl extends TraceComponent {

    static class DRSpanBuilder extends SpanBuilder {

        @Override
        public SpanBuilder setSampler(Sampler sampler) {
            return this;
        }

        @Override
        public SpanBuilder setParentLinks(List<Span> parentLinks) {
            return this;
        }

        @Override
        public SpanBuilder setRecordEvents(boolean recordEvents) {
            return this;
        }

        @Override
        public Span startSpan() {
            return new Span(SpanContext.create(TraceId.generateRandomId(new Random()),
                SpanId.generateRandomId(new Random()),
                TraceOptions.DEFAULT), null) {
                @Override
                public void addAnnotation(String description, Map<String, AttributeValue> attributes) {

                }

                @Override
                public void addAnnotation(Annotation annotation) {

                }

                @Override
                public void addLink(Link link) {

                }

                @Override
                public void end(EndSpanOptions options) {

                }
            };
        }
    }
    @Override
    public Tracer getTracer() {
        return new Tracer() {
            @Override
            public SpanBuilder spanBuilderWithExplicitParent(String spanName, @Nullable Span parent) {
                return new DRSpanBuilder();
            }

            @Override
            public SpanBuilder spanBuilderWithRemoteParent(String spanName, @Nullable SpanContext remoteParentSpanContext) {
                return new DRSpanBuilder();
            }
        };
    }

    @Override
    public PropagationComponent getPropagationComponent() {
        return null;
    }

    @Override
    public Clock getClock() {
        return null;
    }

    @Override
    public ExportComponent getExportComponent() {
        return ExportComponent.newNoopExportComponent();
    }

    @Override
    public TraceConfig getTraceConfig() {
        return null;
    }
}
