package bio.terra.service.snapshot.flight.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.duos.DuosService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class CreateDuosFirecloudGroupStep implements Step {

  private final DuosService duosService;
  private final IamService iamService;
  private final String duosId;

  public CreateDuosFirecloudGroupStep(
      DuosService duosService, IamService iamService, String duosId) {
    this.duosService = duosService;
    this.iamService = iamService;
    this.duosId = duosId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    DuosFirecloudGroupModel created = duosService.createFirecloudGroup(duosId);

    context.getWorkingMap().put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, created);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    DuosFirecloudGroupModel created = SnapshotDuosFlightUtils.getFirecloudGroup(context);
    if (created != null) {
      iamService.deleteGroup(created.getFirecloudGroupName());
    }
    return StepResult.getStepResultSuccess();
  }
}
