package bio.terra.fixtures;

import bio.terra.model.StudySummaryModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import org.springframework.http.HttpStatus;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Optional;


public final class FlightStates {
    private FlightStates() {}

    public static final String testFlightId = "test-flight-id";
    public static final Timestamp submittedTime = Timestamp.from(Instant.now());
    public static final String submittedTimeFormatted = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .format(submittedTime);
    public static final Timestamp completedTime = Timestamp.from(Instant.now());
    public static final String completedTimeFormatted = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
            .format(completedTime);
    public static final StudySummaryModel studySummary = new StudySummaryModel()
                .name("Minimal")
                .description("This is a sample study definition");
    public static final StudySummaryModel minimalStudySummary = new StudySummaryModel()
                .id("Minimal")
                .name("Minimal")
                .description("This is a sample study definition");


    public static FlightState makeFlightSimpleState() {
        // Construct a mock FlightState
        FlightMap resultMap = new FlightMap();
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), studySummary);

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(Timestamp.from(Instant.now()));
        flightState.setInputParameters(resultMap); // unused
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(Timestamp.from(Instant.now())));
        flightState.setErrorMessage(Optional.empty());
        return flightState;
    }

    public static FlightState makeFlightRunningState() {
        FlightMap resultMap = new FlightMap();
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), minimalStudySummary);
        resultMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.I_AM_A_TEAPOT);
        resultMap.put(JobMapKeys.DESCRIPTION.getKeyName(), minimalStudySummary.getDescription());

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(submittedTime);
        flightState.setInputParameters(resultMap);
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.empty());
        flightState.setErrorMessage(Optional.empty());
        return flightState;
    }

    public static FlightState makeFlightCompletedState() {
        FlightMap resultMap = new FlightMap();
        resultMap.put(JobMapKeys.RESPONSE.getKeyName(), minimalStudySummary);
        resultMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.I_AM_A_TEAPOT);
        resultMap.put(JobMapKeys.DESCRIPTION.getKeyName(), minimalStudySummary.getDescription());

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(submittedTime);
        flightState.setInputParameters(resultMap);
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(completedTime));
        flightState.setErrorMessage(Optional.empty());
        return flightState;
    }
}
