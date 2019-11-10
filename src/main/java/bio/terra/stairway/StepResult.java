package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class StepResult {
    private static final Logger logger = LoggerFactory.getLogger(StepResult.class);

    private StepStatus stepStatus;
    private Exception exception;

    // Static version of success result
    private static StepResult stepResultSuccess = new StepResult(StepStatus.STEP_RESULT_SUCCESS);
    public static StepResult getStepResultSuccess() {
        return stepResultSuccess;
    }

    public StepResult(StepStatus stepStatus, Exception exception) {
        this.stepStatus = stepStatus;
        this.exception = exception;
        if (exception != null) {
            logger.error("Step Result exception", exception);
        }
    }

    // This constructor is used in the DAO to silently reconstruct the exception from the class and message.
    StepResult(StepStatus stepStatus, String exceptionClass, String exceptionMessage) {
        this.stepStatus = stepStatus;
        ExceptionFields exceptionFields = new ExceptionFields(exceptionClass, exceptionMessage);
        this.exception = exceptionFields.getException();
    }

    public StepResult(StepStatus stepStatus) {
        this.stepStatus = stepStatus;
    }

    public StepStatus getStepStatus() {
        return stepStatus;
    }

    public Optional<Exception> getException() {
        return Optional.ofNullable(exception);
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
