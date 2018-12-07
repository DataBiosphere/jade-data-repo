package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Context for a flight. This contains the full state for a flight.
 * It is what is held in the database for the flight and it is passed into the steps
 */
public class FlightContext {
    private String flightId; // unique id for the flight
    private SafeHashMap inputParameters; // allows for reconstructing the flight; set unmodifiable
    private SafeHashMap workingMap; // open-ended state used by the steps
    private int stepIndex; // what step we are on
    private FlightDirection direction; // what direction we are going
    private StepResult result; // current status


    // Construct the context with defaults
    public FlightContext(SafeHashMap inputParameters) {
        this.inputParameters = inputParameters;
        this.inputParameters.setImmutable();
        this.workingMap = new SafeHashMap();
        this.stepIndex = 0;
        this.direction = FlightDirection.FORWARD;
        this.result = StepResult.stepResultSuccess;
    }

    public FlightContext flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }

    public String getFlightId() {
        return flightId;
    }

    public SafeHashMap getInputParameters() {
        return inputParameters;
    }

    // Normally, I don't hand out mutable maps, but in this case, the steps
    // will be making heavy use of the map. There does not seem to be a reason
    // to encapsulate it in this class.
    public SafeHashMap getWorkingMap() {
        return workingMap;
    }

    public int getStepIndex() {
        return stepIndex;
    }

    public FlightContext stepIndex(int stepIndex) {
        this.stepIndex = stepIndex;
        return this;
    }

    public void incrStepIndex() {
        this.stepIndex++;
    }

    public void decrStepIndex() {
        this.stepIndex--;
    }

    public FlightDirection getDirection() {
        return direction;
    }

    public FlightContext direction(FlightDirection direction) {
        this.direction = direction;
        return this;
    }



    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("flightId", flightId)
                .append("inputParameters", inputParameters)
                .append("workingMap", workingMap)
                .append("stepIndex", stepIndex)
                .append("direction", direction)
                .append("result", result)
                .toString();
    }
}
