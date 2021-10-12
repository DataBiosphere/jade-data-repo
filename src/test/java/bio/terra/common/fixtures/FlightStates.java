package bio.terra.common.fixtures;

import static bio.terra.common.fixtures.DatasetFixtures.buildMinimalDatasetSummary;

import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import java.time.Instant;
import org.springframework.http.HttpStatus;

public final class FlightStates {
  private FlightStates() {}

  public static final String testFlightId = "test-flight-id";
  public static final Instant submittedTime = Instant.now();
  public static final String submittedTimeFormatted = submittedTime.toString();
  public static final Instant completedTime = Instant.now();
  public static final String completedTimeFormatted = completedTime.toString();

  public static FlightState makeFlightSimpleState() {
    // Construct a mock FlightState
    FlightMap resultMap = new FlightMap();
    JobMapKeys.RESPONSE.put(resultMap, buildMinimalDatasetSummary());
    JobMapKeys.STATUS_CODE.put(resultMap, HttpStatus.I_AM_A_TEAPOT);

    FlightState flightState = new FlightState();
    flightState.setFlightId(testFlightId);
    flightState.setFlightStatus(FlightStatus.SUCCESS);
    flightState.setSubmitted(Instant.now());
    flightState.setInputParameters(resultMap); // unused
    flightState.setResultMap(resultMap);
    flightState.setCompleted(Instant.now());
    return flightState;
  }

  public static FlightState makeFlightRunningState() {
    DatasetSummaryModel req = buildMinimalDatasetSummary();
    FlightMap resultMap = new FlightMap();
    JobMapKeys.RESPONSE.put(resultMap, req);
    JobMapKeys.STATUS_CODE.put(resultMap, HttpStatus.I_AM_A_TEAPOT);
    JobMapKeys.DESCRIPTION.put(resultMap, req.getDescription());

    FlightState flightState = new FlightState();
    flightState.setFlightId(testFlightId);
    flightState.setFlightStatus(FlightStatus.RUNNING);
    flightState.setSubmitted(submittedTime);
    flightState.setInputParameters(resultMap);
    flightState.setResultMap(resultMap);
    return flightState;
  }

  public static FlightState makeFlightCompletedState() {
    DatasetSummaryModel req = buildMinimalDatasetSummary();
    FlightMap resultMap = new FlightMap();
    JobMapKeys.RESPONSE.put(resultMap, req);
    JobMapKeys.STATUS_CODE.put(resultMap, HttpStatus.I_AM_A_TEAPOT);
    JobMapKeys.DESCRIPTION.put(resultMap, req.getDescription());

    FlightState flightState = new FlightState();
    flightState.setFlightId(testFlightId);
    flightState.setFlightStatus(FlightStatus.SUCCESS);
    flightState.setSubmitted(submittedTime);
    flightState.setInputParameters(resultMap);
    flightState.setResultMap(resultMap);
    flightState.setCompleted(completedTime);
    return flightState;
  }
}
