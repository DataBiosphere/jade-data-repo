package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Optional;

public class StepResult {
    StepStatus stepStatus;
    Optional<Throwable> throwable;

    // Static version of success result
    static public StepResult stepResultSuccess;
    static {
        stepResultSuccess = new StepResult(StepStatus.STEP_RESULT_SUCCESS);
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

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("stepStatus", stepStatus)
                .append("throwable", throwable)
                .toString();
    }
}
