package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class CreateSnapshotSetResponseStep extends DefaultUndoStep {

  private final SnapshotService snapshotService;

  public CreateSnapshotSetResponseStep(SnapshotService snapshotService) {
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    UUID snapshotId = context.getWorkingMap().get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    SnapshotSummaryModel response = snapshotService.retrieveSnapshotSummary(snapshotId);
    FlightUtils.setResponse(context, response, HttpStatus.CREATED);
    return StepResult.getStepResultSuccess();
  }
}
