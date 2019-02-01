package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Optional;

/**
 * Class that holds the result of the flight returned to the caller.
 */
public class FlightResult {
    private boolean success;
    private Optional<Throwable> throwable;
    private FlightMap resultMap;


    public FlightResult(StepResult stepResult, FlightMap workingMap) {
        this.success = (stepResult.getStepStatus() == StepStatus.STEP_RESULT_SUCCESS);
        this.throwable = stepResult.getThrowable();
        this.resultMap = workingMap;
        this.resultMap.makeImmutable();
    }

    // Make a failed fatal flight result with an exception
    public static FlightResult flightResultFatal(Exception ex) {
        return new FlightResult(
                new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex),
                new FlightMap());
    }

    public boolean isSuccess() {
        return success;
    }

    public Optional<Throwable> getThrowable() {
        return throwable;
    }

    public FlightMap getResultMap() {
        return resultMap;
    }

    @Override
    public String toString() {
        return new org.apache.commons.lang3.builder.ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("success", success)
                .append("throwable", throwable)
                .append("resultMap", resultMap)
                .toString();
    }
}
