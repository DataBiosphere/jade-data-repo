package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Context for a flight. This contains the full state for a flight.
 * It is what is held in the database for the flight and it is passed into the steps
 */
public class FlightContext {
    private String flightId; // unique id for the flight
    private String flightClassName; // class name of the flight; sufficient for recreating the flight object
    private FlightMap inputParameters; // allows for reconstructing the flight; set unmodifiable
    private FlightMap workingMap; // open-ended state used by the steps
    private int stepIndex; // what step we are on
    private boolean doing; // true - executing do's; false - executing undo's
    private StepResult result; // current step status
    private FlightStatus flightStatus; // Status: RUNNING while the flight is running; SUCCESS/FAILED when it completes
    private UserRequestInfo user;

    // Construct the context with defaults
    public FlightContext(FlightMap inputParameters, String flightClassName, UserRequestInfo user) {
        this.inputParameters = inputParameters;
        this.inputParameters.makeImmutable();
        this.flightClassName = flightClassName;
        this.workingMap = new FlightMap();
        this.stepIndex = 0;
        this.doing = true;
        this.result = StepResult.getStepResultSuccess();
        this.flightStatus = FlightStatus.RUNNING;
        this.user = user;
    }

    public String getFlightId() {
        return flightId;
    }

    public void setFlightId(String flightId) {
        this.flightId = flightId;
    }

    public String getFlightClassName() {
        return flightClassName;
    }

    public FlightMap getInputParameters() {
        return inputParameters;
    }

    // Normally, I don't hand out mutable maps, but in this case, the steps
    // will be making heavy use of the map. There does not seem to be a reason
    // to encapsulate it in this class.
    public FlightMap getWorkingMap() {
        return workingMap;
    }

    public UserRequestInfo getUser() {
        return user;
    }

    public void setUser(UserRequestInfo user) {
        this.user = user;
    }

    public int getStepIndex() {
        return stepIndex;
    }

    public void setStepIndex(int stepIndex) {
        this.stepIndex = stepIndex;
    }

    public FlightStatus getFlightStatus() {
        return flightStatus;
    }

    public void setFlightStatus(FlightStatus flightStatus) {
        this.flightStatus = flightStatus;
    }

    public boolean isDoing() {
        return doing;
    }

    public void setDoing(boolean doing) {
        this.doing = doing;
    }

    public StepResult getResult() {
        return result;
    }

    public void setResult(StepResult result) {
        this.result = result;
    }

    /**
     * Set the step index to the next step. If we are doing, then we progress forwards.
     * If we are undoing, we progress backwards.
     */
    public void nextStepIndex() {
        if (isDoing()) {
            stepIndex++;
        } else {
            stepIndex--;
        }
    }

    /**
     * Check the termination condition (either undo to 0 or do to stepListSize)
     * depending on which direction we are going.
     *
     * @param stepListSize
     * @return true if there is a step to be executed
     */
    public boolean haveStepToDo(int stepListSize) {
        if (isDoing()) {
            return (stepIndex < stepListSize);
        } else {
            return (stepIndex >= 0);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("flightId", flightId)
                .append("flightClassName", flightClassName)
                .append("inputParameters", inputParameters)
                .append("workingMap", workingMap)
                .append("stepIndex", stepIndex)
                .append("doing", doing)
                .append("result", result)
                .append("flightStatus", flightStatus)
                .toString();
    }
}
