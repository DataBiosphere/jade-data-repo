package bio.terra.service.job;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.HookAction;
import bio.terra.stairway.StairwayHook;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StairwayLoggingHooks implements StairwayHook {
  private static final String FlightLogFormat =
      "Operation: {}, flightClass: {}, flightId: {}, timestamp: {}";
  private static final String StepLogFormat =
      "Operation: {}, flightClass: {}, flightId: {}, stepClass: {}, "
          + "stepIndex: {}, direction: {}, timestamp: {}";
  private static final Logger logger = LoggerFactory.getLogger(StairwayHook.class);

  private PerformanceLogger performanceLogger;

  public StairwayLoggingHooks(PerformanceLogger performanceLogger) {
    this.performanceLogger = performanceLogger;
    logger.info("Performance logging " + (performanceLogger.isEnabled() ? "ON" : "OFF"));
  }

  @Override
  public HookAction startFlight(FlightContext context) {
    logger.info(
        FlightLogFormat,
        "startFlight",
        context.getFlightClassName(),
        context.getFlightId(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.log(context.getFlightId(), context.getFlightClassName(), "startFlight");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction startStep(FlightContext context) {
    logger.info(
        StepLogFormat,
        "startStep",
        context.getFlightClassName(),
        context.getFlightId(),
        context.getStepClassName(),
        context.getStepIndex(),
        context.getDirection().name(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.timerStart("stairwayStep" + context.getFlightId());
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endFlight(FlightContext context) {
    logger.info(
        FlightLogFormat,
        "endFlight",
        context.getFlightClassName(),
        context.getFlightId(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.log(context.getFlightId(), context.getFlightClassName(), "endFlight");
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endStep(FlightContext context) {
    logger.info(
        StepLogFormat,
        "endStep",
        context.getFlightClassName(),
        context.getFlightId(),
        context.getStepClassName(),
        context.getStepIndex(),
        context.getDirection().name(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.timerEndAndLog(
        "stairwayStep" + context.getFlightId(),
        context.getFlightId(),
        context.getFlightClassName(),
        "endStep",
        context.getStepIndex());
    return HookAction.CONTINUE;
  }
}
