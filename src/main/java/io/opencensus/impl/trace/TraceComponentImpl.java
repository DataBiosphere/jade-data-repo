package io.opencensus.impl.trace;

import bio.terra.app.utils.startup.StartupInitializer;
import io.opencensus.common.Clock;
import io.opencensus.internal.ZeroTimeClock;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.BlankSpan;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class TraceComponentImpl extends TraceComponent {
    private static final Logger logger = LoggerFactory.getLogger(TraceComponentImpl.class);

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

    public static class DRSpan extends Span {

        DRSpan() {
            super(SpanContext.create(TraceId.generateRandomId(new Random()),
                SpanId.generateRandomId(new Random()),
                TraceOptions.DEFAULT), null);
        }

        @Override
        public void addAnnotation(String description, Map<String, AttributeValue> attributes) {
            logger.info("Tracer: {}, attributes: {}", description, attributes.toString());
        }

        @Override
        public void addAnnotation(Annotation annotation) {
            logger.info("addAnnotation");
        }

        @Override
        public void addLink(Link link) {
            logger.info("addLink");
        }

        @Override
        public void end(EndSpanOptions options) {
            logger.info("Tracer end");
        }


        @Override
        public String toString() {
            return "DRSpan";
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
