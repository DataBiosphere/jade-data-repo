package bio.terra.service.snapshot.flight.create;

import bio.terra.common.BaseStep;
import bio.terra.common.StepInput;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateSnapshotSetResponseStep extends BaseStep {

  private final SnapshotService snapshotService;

  @StepInput private UUID snapshotId;

  public CreateSnapshotSetResponseStep(SnapshotService snapshotService) {
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult perform() {
    setResponse(snapshotService.retrieveSnapshotSummary(snapshotId), HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }
}
