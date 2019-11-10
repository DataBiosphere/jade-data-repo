package bio.terra.stairway;

import java.time.Instant;
import java.util.Optional;

/**
 * Class that holds the state of the flight returned to the caller.
 */
public class FlightState {

    private String flightId;
    private FlightStatus flightStatus;
    private FlightMap inputParameters;
    private Instant submitted;
    private UserRequestInfo user;
    private Instant completed;
    private FlightMap resultMap;  // filled in when flightStatus is SUCCESS
    private Exception exception;  // filled in when flightStatus is ERROR or FATAL

    public FlightState() {
    }

    public String getFlightId() {
        return flightId;
    }

    public void setFlightId(String flightId) {
        this.flightId = flightId;
    }

    public FlightStatus getFlightStatus() {
        return flightStatus;
    }

    public void setFlightStatus(FlightStatus flightStatus) {
        this.flightStatus = flightStatus;
    }

    public FlightMap getInputParameters() {
        return inputParameters;
    }

    public void setInputParameters(FlightMap inputParameters) {
        this.inputParameters = inputParameters;
        this.inputParameters.makeImmutable();
    }

    public Instant getSubmitted() {
        return submitted;
    }

    public void setSubmitted(Instant submitted) {
        // Make our own copy of the incoming object
        this.submitted = submitted;
    }

    public UserRequestInfo getUser() {
        return user;
    }

    public void setUser(UserRequestInfo user) {
        this.user = user;
    }

    public Optional<Instant> getCompleted() {
        return Optional.ofNullable(completed);
    }

    public void setCompleted(Instant completed) {
        this.completed = completed;
    }

    public Optional<FlightMap> getResultMap() {
        return Optional.ofNullable(resultMap);
    }

    public void setResultMap(FlightMap resultMap) {
        if (resultMap != null) {
            resultMap.makeImmutable();
        }
        this.resultMap = resultMap;
    }

    public Optional<Exception> getException() {
        return Optional.ofNullable(exception);
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

}
