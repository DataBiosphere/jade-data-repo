package bio.terra.flight.study.create;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
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
