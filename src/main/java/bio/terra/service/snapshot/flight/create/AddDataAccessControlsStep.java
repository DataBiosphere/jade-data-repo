package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class AddDataAccessControlsStep extends DefaultUndoStep {
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userRequest;
  private final List<String> dataAccessControlGroups;

  public AddDataAccessControlsStep(
      SnapshotService snapshotService,
      AuthenticatedUserRequest userRequest,
      List<String> dataAccessControlGroups) {
    this.snapshotService = snapshotService;
    this.userRequest = userRequest;
    this.dataAccessControlGroups = dataAccessControlGroups;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    snapshotService.addSnapshotDataAccessControls(
        userRequest,
        Objects.requireNonNull(
            context.getWorkingMap().get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class)),
        dataAccessControlGroups);
    return StepResult.getStepResultSuccess();
  }
}
