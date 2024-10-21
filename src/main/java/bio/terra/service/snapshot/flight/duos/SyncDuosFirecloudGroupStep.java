package bio.terra.service.snapshot.flight.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class SyncDuosFirecloudGroupStep extends DefaultUndoStep {

  private final DuosService duosService;
  private final String duosId;

  public SyncDuosFirecloudGroupStep(DuosService duosService, String duosId) {
    this.duosService = duosService;
    this.duosId = duosId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    DuosFirecloudGroupModel synced = duosService.syncDuosDatasetAuthorizedUsers(duosId);

    context.getWorkingMap().put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, synced);

    return StepResult.getStepResultSuccess();
  }
}
