package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Optional;

public class StepResult {
    private StepStatus stepStatus;
    private Optional<Exception> exception;

    // Static version of success result
    private static StepResult stepResultSuccess = new StepResult(StepStatus.STEP_RESULT_SUCCESS);
    public static StepResult getStepResultSuccess() {
        return stepResultSuccess;
    }

    public StepResult(StepStatus stepStatus, Exception exception) {
        this.stepStatus = stepStatus;
        this.exception = Optional.ofNullable(exception);
    }

    public StepResult(StepStatus stepStatus) {
        this.stepStatus = stepStatus;
        this.exception = Optional.empty();
    }

    public StepStatus getStepStatus() {
        return stepStatus;
    }

    public Optional<Exception> getException() {
        return exception;
    }

    public boolean isSuccess() {
        return (stepStatus == StepStatus.STEP_RESULT_SUCCESS);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("stepStatus", stepStatus)
                .append("exception", exception)
                .toString();
    }
}
