package bio.terra.stairway;

import bio.terra.controller.UserInfo;

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
    private UserInfo user;
    private Optional<Instant> completed;
    private Optional<FlightMap> resultMap;  // filled in when flightStatus is SUCCESS
    private Optional<Exception> exception;  // filled in when flightStatus is ERROR or FATAL

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

    public UserInfo getUser() {
        return user;
    }

    public void setUser(UserInfo user) {
        this.user = user;
    }

    public Optional<Instant> getCompleted() {
        return completed;
    }

    public void setCompleted(Optional<Instant> completed) {
        this.completed = completed;
    }

    public Optional<FlightMap> getResultMap() {
        return resultMap;
    }

    public void setResultMap(Optional<FlightMap> resultMap) {
        if (resultMap.isPresent()) {
            resultMap.get().makeImmutable();
        }
        this.resultMap = resultMap;
    }

    public Optional<Exception> getException() {
        return exception;
    }

    public FlightState setException(Optional<Exception> exception) {
        this.exception = exception;
        return this;
    }

}
