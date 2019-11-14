package bio.terra.stairway;

import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.RetryException;
import bio.terra.stairway.exception.StairwayExecutionException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Flight implements Callable<FlightState> {
    static class StepRetry {
        private Step step;
        private RetryRule retryRule;

        StepRetry(Step step, RetryRule retryRule) {
            this.step = step;
            this.retryRule = retryRule;
        }
    }

    private static Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

    private List<StepRetry> steps;
    private FlightDao flightDao;
    private FlightContext flightContext;
    private Object applicationContext;

    public Flight(FlightMap inputParameters, Object applicationContext, UserRequestInfo userRequestInfo) {
        flightContext = new FlightContext(inputParameters, this.getClass().getName(), userRequestInfo);
        this.applicationContext = applicationContext;
        steps = new LinkedList<>();
    }

    public FlightContext context() {
        return flightContext;
    }

    public Object getApplicationContext() {
        return applicationContext;
    }

    public void setFlightDao(FlightDao flightDao) {
        this.flightDao = flightDao;
    }

    public void setFlightContext(FlightContext flightContext) {
        this.flightContext = flightContext;
    }

    // Used by subclasses to build the step list with default no-retry rule
    protected void addStep(Step step) {
        steps.add(new StepRetry(step, RetryRuleNone.getRetryRuleNone()));
    }

    // Used by subclasses to build the step list with a retry rule
    protected void addStep(Step step, RetryRule retryRule) {
        steps.add(new StepRetry(step, retryRule));
    }

    /**
     * Call may be called for a flight that has been interrupted and is being recovered
     * so we may be headed either direction.
     */
    public FlightState call() throws DatabaseOperationException {
        logger.debug("Executing flight class: " + context().getFlightClassName() + " id: " + context().getFlightId());
        FlightStatus flightStatus = fly();
        context().setFlightStatus(flightStatus);
        flightDao.complete(context());
        return flightDao.getFlightState(context().getFlightId());
    }

    /**
     * Perform the flight, until we do all steps, undo to the beginning, or declare a dismal failure.
     */
    private FlightStatus fly() {
        try {
            // Part 1 - running forward (doing). We either succeed or we record the failure and
            // fall through to running backward (undoing)
            if (context().isDoing()) {
                StepResult doResult = runSteps();
                if (doResult.isSuccess()) {
                    return FlightStatus.SUCCESS;
                }

                // Remember the failure from the do; that is what we want to return
                // after undo completes
                context().setResult(doResult);
                context().setDoing(false);

                // Record the step failure and direction change in the database
                flightDao.step(context());
            }

            // Part 2 - running backwards. We either succeed and return the original failure
            // status or we have a 'dismal failure'
            StepResult undoResult = runSteps();
            if (undoResult.isSuccess()) {
                // Return the error from the doResult - that is why we failed
                return FlightStatus.ERROR;
            }

            // Part 3 - dismal failure
            // Record the undo failure
            flightDao.step(context());

            // Dismal failure - undo failed!
            context().setResult(undoResult);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            context().setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex));
        } catch (Exception ex) {
            logger.error("Unhandled flight exception", ex);
            context().setResult(new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex));
        }

        return FlightStatus.FATAL;
    }

    /**
     * run the steps in sequence, either forward or backward, until either we
     * complete successfully or we encounter an error.
     * Note that this only records the step in the database if there is success.
     * Otherwise, it returns out and lets the outer logic setup the failure state
     * before recording it into the database.
     *
     * @return StepResult recording the success or failure of the most recent step
     * @throws InterruptedException
     */
    private StepResult runSteps() throws InterruptedException, StairwayExecutionException, DatabaseOperationException {
        // Initialize with current result, in case we are all done already
        StepResult result = context().getResult();

        while (context().haveStepToDo(steps.size())) {
            result = stepWithRetry();

            // Exit if we hit a failure (result shows failed)
            if (!result.isSuccess()) {
                return result;
            }

            flightDao.step(context());

            context().nextStepIndex();
        }
        return result;
    }

    private StepResult stepWithRetry() throws InterruptedException, StairwayExecutionException {
        logger.debug("Executing flight id: " + context().getFlightId() + " step: " + context().getStepIndex() +
            " direction: " + (context().isDoing() ? "doing" : "undoing"));

        StepRetry currentStep = getCurrentStep();
        currentStep.retryRule.initialize();

        StepResult result;

        // Retry loop
        do {
            try {
                // Do or undo based on direction we are headed
                if (context().isDoing()) {
                    result = currentStep.step.doStep(context());
                } else {
                    result = currentStep.step.undoStep(context());
                }

            } catch (Exception ex) {
                // The purpose of this catch is to relieve steps of implementing their own repetitive try-catch
                // simply to turn exceptions into StepResults.
                logger.info("Caught exception: (" + ex.toString() +
                    ")\nexecuting flight id: " + context().getFlightId() +
                    " step: " + context().getStepIndex() +
                    " direction: " + (context().isDoing() ? "doing" : "undoing"), ex);

                StepStatus stepStatus = (ex instanceof RetryException)
                    ? StepStatus.STEP_RESULT_FAILURE_RETRY
                    : StepStatus.STEP_RESULT_FAILURE_FATAL;
                result = new StepResult(stepStatus, ex);
            }

            switch (result.getStepStatus()) {
                case STEP_RESULT_SUCCESS:
                case STEP_RESULT_FAILURE_FATAL:
                    return result;

                case STEP_RESULT_FAILURE_RETRY:
                    logger.info("Retrying flight id: " + context().getFlightId() +
                        " step: " + context().getStepIndex() +
                        " direction: " + (context().isDoing() ? "doing" : "undoing"));
                default:
                    break;
            }
        } while (currentStep.retryRule.retrySleep()); // retry rule decides if we should try again or not

        return result;
    }

    private StepRetry getCurrentStep() throws StairwayExecutionException {
        int stepIndex = context().getStepIndex();
        if (stepIndex < 0 || stepIndex >= steps.size()) {
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
