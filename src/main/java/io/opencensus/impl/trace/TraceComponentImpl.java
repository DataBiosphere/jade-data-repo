package io.opencensus.impl.trace;

import io.opencensus.common.Clock;
import io.opencensus.internal.ZeroTimeClock;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Link;
import io.opencensus.trace.MessageEvent;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.ExportComponent;
import io.opencensus.trace.propagation.PropagationComponent;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TraceComponentImpl extends TraceComponent {

    static class LogSpanBuilder extends SpanBuilder {
        static final LogSpan SPAN = new LogSpan();

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
            return SPAN;
        }
    }

    @Override
    public Tracer getTracer() {
        return new Tracer() {
            @Override
            public SpanBuilder spanBuilderWithExplicitParent(String spanName, @Nullable Span parent) {
                return new LogSpanBuilder();
            }

            @Override
            public SpanBuilder spanBuilderWithRemoteParent(String spanName, @Nullable SpanContext remoteParentSpanContext) {
                return new LogSpanBuilder();
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

    static final class LogSpan extends Span {
        private static final Logger logger = LoggerFactory.getLogger(LogSpan.class);

        LogSpan() {
            super(SpanContext.INVALID, null);
        }

        @Override
        public void putAttributes(Map<String, AttributeValue> attributes) {
            logger.info("putAttributes: {}", attributes);
        }

        @Override
        public void addAnnotation(String description, Map<String, AttributeValue> attributes) {
            logger.info("addAnnotation: {}, attributes: {}", description, attributes);
        }

        @Override
        public void addAnnotation(Annotation annotation) {
            logger.info("addAnnotation: {}", annotation);
        }

        @Override
        public void addMessageEvent(MessageEvent messageEvent) {
            logger.info("addMessageEvent: {}", messageEvent);
        }

        @Override
        public void addLink(Link link) {
            logger.info("addLink: {}", link);
        }

        @Override
        public void setStatus(Status status) {
            logger.info("setStatus: {}", status);
        }

        @Override
        public void end(EndSpanOptions options) {
            logger.info("end: {}", options);
        }
    }
}
