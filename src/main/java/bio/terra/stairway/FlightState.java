package bio.terra.stairway;

import java.sql.Timestamp;
import java.util.Optional;

/**
 * Class that holds the state of the flight returned to the caller.
 */
public class FlightState {

    private String flightId;
    private FlightStatus flightStatus;
    private FlightMap inputParameters;
    private Timestamp submitted;
    private Optional<Timestamp> completed;
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

    public Timestamp getSubmitted() {
        // Return an immutable copy of the timestamp - just for findbugs
        return new Timestamp(submitted.getTime());
    }

    public void setSubmitted(Timestamp submitted) {
        // Make our own copy of the incoming object
        this.submitted = new Timestamp(submitted.getTime());
    }

    public Optional<Timestamp> getCompleted() {
        return completed;
    }

    public void setCompleted(Optional<Timestamp> completed) {
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
