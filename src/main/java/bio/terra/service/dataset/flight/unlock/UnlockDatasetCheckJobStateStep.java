package bio.terra.service.dataset.flight.unlock;

import bio.terra.model.JobModel;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;

public class UnlockDatasetCheckJobStateStep extends DefaultUndoStep {
  private final JobService jobService;
  private final String lockName;
  public UnlockDatasetCheckJobStateStep(JobService jobService, String lockName) {
    this.jobService = jobService;
    this.lockName = lockName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightStatus status = jobService.unauthRetrieveJobState(lockName);
    switch(status) {
      case RUNNING, WAITING, READY, QUEUED, READY_TO_RESTART:
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, new DatasetLockException("Locking flight " + lockName + " has state " + status.name() + ". Unlocking a running flight could have unintended consequences. Please proceed with unlock with caution (You still still unlock by setting failOnRunningJob to false)."));
      default:
        return StepResult.getStepResultSuccess();
    }

  }
}
