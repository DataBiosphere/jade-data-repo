package bio.terra.stairway;

import bio.terra.stairway.exception.StairwayExecutionException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Manage the atomic execution of a series of Steps
 * This base class has the mechanisms for executing the series of steps.
 *
 *  In order for the flight to be re-created on recovery, the construction and
 *  configuration have to result in the same flight given the same input.
 *
 *  ISSUE: handling InterruptedException - is there anything that the Flight level should do
 *  for handling this?
 */
public class Flight implements Callable<FlightResult> {
    class StepRetry {
        Step step;
        RetryRule retryRule;

        StepRetry(Step step, RetryRule retryRule) {
            this.step = step;
            this.retryRule = retryRule;
        }
    }

    private List<StepRetry> steps;
    private FlightContext flightContext;

    public Flight(SafeHashMap inputParameters) {
        flightContext = new FlightContext(inputParameters);
        steps = new LinkedList<>();
    }

    public FlightContext context() {
        return flightContext;
    }

    // Used by subclasses to build the step list with default no-retry rule
    protected void addStep(Step step) {
        steps.add(new StepRetry(step, RetryRuleNone.retryRuleNone));
    }

    // Used by subclasses to build the step list with a retry rule
    protected void addStep(Step step, RetryRule retryRule) {
        steps.add(new StepRetry(step, retryRule));
    }

    public FlightResult call() {
        try {
            StepResult doResult = runForward();
            if (doResult.isSuccess()) {
                return new FlightResult(doResult, context().getWorkingMap());
            }

            // Run it backward performing the undos
            context().direction(FlightDirection.BACKWARD);

            // TODO: I *think* we need to record the direction change in the database

            StepResult undoResult = runBackward();
            if (undoResult.isSuccess()) {
                // Return the error from the doResult - that is why we failed
                return new FlightResult(doResult, context().getWorkingMap());
            }

            // Dismal failure - undo failed!
            return FlightResult.flightResultFatal(
                    new StairwayExecutionException("Dismal failure: " + undoResult.getThrowable().toString(),
                            doResult.getThrowable().orElse(null)));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return FlightResult.flightResultFatal(ex);
        }
    }

    private StepResult runForward() throws InterruptedException {
        while (true) {
            // Do the current step
            StepResult result = stepWithRetry();

            // TODO: write state to database

            if (!result.isSuccess()) {
                return result;
            }

            flightContext.incrStepIndex();
            if (flightContext.getStepIndex() >= steps.size()) {
                return result;
            }
        }
    }

    private StepResult runBackward() throws InterruptedException {
        while (true) {
            // Undo the current step
            StepResult result = stepWithRetry();

            // TODO: write state to database

            if (!result.isSuccess()) {
                return result;
            }

            if (flightContext.getStepIndex() == 0) {
                return result;
            }

            flightContext.decrStepIndex();
        }
    }

    private StepResult stepWithRetry() throws InterruptedException {
        StepRetry currentStep = getCurrentStep();
        currentStep.retryRule.initialize();

        StepResult result;

        // Retry loop
        do {
            // Do or undo based on direction we are headed
            if (flightContext.getDirection() == FlightDirection.FORWARD) {
                result = currentStep.step.doStep(flightContext);
            } else {
                result = currentStep.step.undoStep(flightContext);

            }
            switch (result.getStepStatus()) {
                case STEP_RESULT_SUCCESS:
                case STEP_RESULT_FAILURE_FATAL:
                    return result;

                case STEP_RESULT_FAILURE_RETRY:
                default:
                    break;
            }

        } while (currentStep.retryRule.retrySleep()); // retry rule decides if we should try again or not

        return result;
    }

    private StepRetry getCurrentStep() {
        int stepIndex = flightContext.getStepIndex();
        if (stepIndex < 0|| stepIndex >= steps.size()) {
            throw new StairwayExecutionException("Invalid step index: " + stepIndex);
        }

        return steps.get(stepIndex);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("steps", steps)
                .append("flightContext", flightContext)
                .toString();
    }
}
