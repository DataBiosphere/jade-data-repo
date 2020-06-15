package bio.terra.service.job;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.HookAction;
import bio.terra.stairway.StairwayHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class StairwayLoggingHooks implements StairwayHook {
    private static final String FlightLogFormat = "Operation: {}, flightClass: {}, flightId: {}, timestamp: {}";
    private static final String StepLogFormat = "Operation: {}, flightClass: {}, flightId: {}, stepIndex: {}," +
        "timestamp: {}";
    private static final Logger logger = LoggerFactory.getLogger(StairwayHook.class);

    private PerformanceLogger performanceLogger;

    public StairwayLoggingHooks(PerformanceLogger performanceLogger) {
        this.performanceLogger = performanceLogger;
    }

    @Override
    public HookAction startFlight(FlightContext context) {
        logger.info(FlightLogFormat, "startFlight", context.getFlightClassName(),
            context.getFlightId(), Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
        return HookAction.CONTINUE;
    }

    @Override
    public HookAction startStep(FlightContext context) {
        logger.info(StepLogFormat, "startStep", context.getFlightClassName(), context.getFlightId(),
            context.getStepIndex(), Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));

        performanceLogger.timerStart(context.getFlightId());

        return HookAction.CONTINUE;
    }

    @Override
    public HookAction endFlight(FlightContext context) {
        logger.info(FlightLogFormat, "endFlight", context.getFlightClassName(),
            context.getFlightId(), Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
        return HookAction.CONTINUE;
    }

    @Override
    public HookAction endStep(FlightContext context) {
        logger.info(StepLogFormat, "endStep", context.getFlightClassName(), context.getFlightId(),
            context.getStepIndex(), Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));

        performanceLogger.timerEndAndLog(context.getFlightId(), context.getFlightId(),
            context.getFlightClassName(), "endStep", context.getStepIndex());

        return HookAction.CONTINUE;
    }
}
