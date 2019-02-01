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
    private Optional<String> errorMessage;  // filled in when flightStatus is ERROR or FATAL

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
    }

    public Timestamp getSubmitted() {
        return submitted;
    }

    public void setSubmitted(Timestamp submitted) {
        this.submitted = submitted;
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
        this.resultMap = resultMap;
    }

    public Optional<String> getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(Optional<String> errorMessage) {
        this.errorMessage = errorMessage;
    }
}
