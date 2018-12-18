package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Optional;

public class StepResult {
    private StepStatus stepStatus;
    private Optional<Throwable> throwable;

    // Static version of success result
    private static StepResult stepResultSuccess = new StepResult(StepStatus.STEP_RESULT_SUCCESS);
    public static StepResult getStepResultSuccess() {
        return stepResultSuccess;
    }

    public StepResult(StepStatus stepStatus, Throwable throwable) {
        this.stepStatus = stepStatus;
        this.throwable = Optional.ofNullable(throwable);
    }

    public StepResult(StepStatus stepStatus) {
        this.stepStatus = stepStatus;
        this.throwable = Optional.empty();
    }

    public StepStatus getStepStatus() {
        return stepStatus;
    }

    public Optional<Throwable> getThrowable() {
        return throwable;
    }

    public boolean isSuccess() {
        return (stepStatus == StepStatus.STEP_RESULT_SUCCESS);
    }

    /**
     * Extract the error message from the throwable. If there is no throwable,
     * then return an empty string.
     */
    public String getErrorMessage() {
        if (getThrowable().isPresent()) {
            return getThrowable().get().toString();
        }
        return "";
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("stepStatus", stepStatus)
                .append("throwable", throwable)
                .toString();
    }
}
