package bio.terra.common.fixtures;

import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.Optional;

import static bio.terra.common.fixtures.DatasetFixtures.buildMinimalDatasetSummary;


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
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), buildMinimalDatasetSummary());
        resultMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.I_AM_A_TEAPOT);

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(Instant.now());
        flightState.setInputParameters(resultMap); // unused
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(Instant.now()));
        flightState.setException(Optional.empty());
        return flightState;
    }

    public static FlightState makeFlightRunningState() {
        DatasetSummaryModel req = buildMinimalDatasetSummary();
        FlightMap resultMap = new FlightMap();
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), req);
        resultMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.I_AM_A_TEAPOT);
        resultMap.put(JobMapKeys.DESCRIPTION.getKeyName(), req.getDescription());

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.RUNNING);
        flightState.setSubmitted(submittedTime);
        flightState.setInputParameters(resultMap);
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.empty());
        flightState.setException(Optional.empty());
        return flightState;
    }

    public static FlightState makeFlightCompletedState() {
        DatasetSummaryModel req = buildMinimalDatasetSummary();
        FlightMap resultMap = new FlightMap();
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), req);
        resultMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.I_AM_A_TEAPOT);
        resultMap.put(JobMapKeys.DESCRIPTION.getKeyName(), req.getDescription());

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(submittedTime);
        flightState.setInputParameters(resultMap);
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(completedTime));
        flightState.setException(Optional.empty());
        return flightState;
    }
}
