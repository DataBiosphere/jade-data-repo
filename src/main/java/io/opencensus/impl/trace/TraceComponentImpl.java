package io.opencensus.impl.trace;

import io.opencensus.common.Clock;
import io.opencensus.internal.ZeroTimeClock;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.ExportComponent;
import io.opencensus.trace.propagation.PropagationComponent;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
            return new DRSpan();
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
        return PropagationComponent.getNoopPropagationComponent();
    }

    @Override
    public Clock getClock() {
        return ZeroTimeClock.getInstance();
    }

    @Override
    public ExportComponent getExportComponent() {
        return ExportComponent.newNoopExportComponent();
    }

    @Override
    public TraceConfig getTraceConfig() {
        return TraceConfig.getNoopTraceConfig();
    }
}
