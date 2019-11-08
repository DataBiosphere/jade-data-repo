package bio.terra.stairway;

import bio.terra.stairway.exception.FlightException;

class TriggerUndoStep implements Step {

    @Override
    public StepResult doStep(FlightContext context) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new FlightException("TestTriggerUndoStep"));
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
