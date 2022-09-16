package bio.terra.service.job;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.HookAction;
import bio.terra.stairway.StairwayHook;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class StairwayLoggingHooks implements StairwayHook {
  private static final String FLIGHT_LOG_FORMAT =
      "Operation: {}, flightClass: {}, flightId: {}, status: {}, timestamp: {}";
  private static final String STEP_LOG_FORMAT =
      "Operation: {}, flightClass: {}, flightId: {}, stepClass: {}, "
          + "stepIndex: {}, direction: {}, timestamp: {}";
  /** Id of the flight */
  private static final String FLIGHT_ID_KEY = "flightId";
  /** Id of the upstream parent flight, if one exists */
  private static final String FLIGHT_PARENT_ID_KEY = JobMapKeys.PARENT_FLIGHT_ID.getKeyName();
  /** Class of the flight */
  private static final String FLIGHT_CLASS_KEY = "flightClass";
  /** Class of the flight step */
  private static final String FLIGHT_STEP_CLASS_KEY = "flightStepClass";
  /** Direction of the step (START, DO, UNDO or SWITCH) */
  private static final String FLIGHT_STEP_DIRECTION_KEY = "flightStepDirection";
  /** The step's execution order */
  private static final String FLIGHT_STEP_NUMBER_KEY = "flightStepNumber";
  /** The type of operation primarily being logged (startFlight, startStep, endStep or endFlight) */
  private static final String FLIGHT_OPERATION_KEY = "flightStepOperation";

  private static final String FLIGHT_OPERATION_START = "startFlight";
  private static final String FLIGHT_OPERATION_END = "endFlight";
  private static final String FLIGHT_STEP_OPERATION_START = "startStep";
  private static final String FLIGHT_STEP_OPERATION_END = "endStep";

  private static final Logger logger = LoggerFactory.getLogger(StairwayHook.class);

  private final PerformanceLogger performanceLogger;

  public StairwayLoggingHooks(PerformanceLogger performanceLogger) {
    this.performanceLogger = performanceLogger;
    logger.info("Performance logging {}", performanceLogger.isEnabled() ? "ON" : "OFF");
  }

  @Override
  public HookAction startFlight(FlightContext context) {
    MDC.put(FLIGHT_ID_KEY, context.getFlightId());
    MDC.put(FLIGHT_CLASS_KEY, context.getFlightClassName());
    MDC.put(FLIGHT_OPERATION_KEY, FLIGHT_OPERATION_START);
    getParentFlightId(context).ifPresent(pfid -> MDC.put(FLIGHT_PARENT_ID_KEY, pfid));
    logger.info(
        FLIGHT_LOG_FORMAT,
        FLIGHT_OPERATION_START,
        context.getFlightClassName(),
        context.getFlightId(),
        context.getFlightStatus(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.log(
        context.getFlightId(), context.getFlightClassName(), FLIGHT_OPERATION_START);
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction startStep(FlightContext context) {
    MDC.put(FLIGHT_ID_KEY, context.getFlightId());
    MDC.put(FLIGHT_CLASS_KEY, context.getFlightClassName());
    MDC.put(FLIGHT_STEP_CLASS_KEY, context.getStepClassName());
    MDC.put(FLIGHT_STEP_DIRECTION_KEY, context.getDirection().toString());
    MDC.put(FLIGHT_STEP_NUMBER_KEY, Integer.toString(context.getStepIndex()));
    MDC.put(FLIGHT_OPERATION_KEY, FLIGHT_STEP_OPERATION_START);
    getParentFlightId(context).ifPresent(pfid -> MDC.put(FLIGHT_PARENT_ID_KEY, pfid));
    logger.info(
        STEP_LOG_FORMAT,
        FLIGHT_STEP_OPERATION_START,
        context.getFlightClassName(),
        context.getFlightId(),
        context.getStepClassName(),
        context.getStepIndex(),
        context.getDirection().name(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.timerStart(getStepTimerName(context.getFlightId()));
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endFlight(FlightContext context) {
    MDC.put(FLIGHT_OPERATION_KEY, FLIGHT_OPERATION_END);
    logger.info(
        FLIGHT_LOG_FORMAT,
        FLIGHT_OPERATION_END,
        context.getFlightClassName(),
        context.getFlightId(),
        context.getFlightStatus(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.log(
        context.getFlightId(), context.getFlightClassName(), FLIGHT_OPERATION_END);
    clearMdcKeys();
    return HookAction.CONTINUE;
  }

  @Override
  public HookAction endStep(FlightContext context) {
    MDC.put(FLIGHT_OPERATION_KEY, FLIGHT_STEP_OPERATION_END);
    logger.info(
        STEP_LOG_FORMAT,
        FLIGHT_STEP_OPERATION_END,
        context.getFlightClassName(),
        context.getFlightId(),
        context.getStepClassName(),
        context.getStepIndex(),
        context.getDirection().name(),
        Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT));
    performanceLogger.timerEndAndLog(
        getStepTimerName(context.getFlightId()),
        context.getFlightId(),
        context.getFlightClassName(),
        FLIGHT_STEP_OPERATION_END,
        context.getStepIndex());
    clearMdcKeys();
    return HookAction.CONTINUE;
  }

  private void clearMdcKeys() {
    MDC.remove(FLIGHT_ID_KEY);
    MDC.remove(FLIGHT_CLASS_KEY);
    MDC.remove(FLIGHT_STEP_CLASS_KEY);
    MDC.remove(FLIGHT_STEP_DIRECTION_KEY);
    MDC.remove(FLIGHT_STEP_NUMBER_KEY);
    MDC.remove(FLIGHT_OPERATION_KEY);
    MDC.remove(FLIGHT_PARENT_ID_KEY);
  }

  private String getStepTimerName(final String flightId) {
    return String.format("stairwayStep%s", flightId);
  }

  private Optional<String> getParentFlightId(FlightContext context) {
    FlightMap parameterMap = context.getInputParameters();
    return Optional.ofNullable(parameterMap.get(FLIGHT_PARENT_ID_KEY, String.class));
  }
}
