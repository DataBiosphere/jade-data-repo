package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.ws.rs.BadRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class CheckChildSnapshotAuthDomainStep extends DefaultUndoStep {
  private final SnapshotService snapshotService;
  private final AuthenticatedUserRequest userReq;

  public CheckChildSnapshotAuthDomainStep(
      SnapshotService snapshotService, AuthenticatedUserRequest userReq) {
    this.snapshotService = snapshotService;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    List<UUID> snapshotsWithAuthDomains = new ArrayList<>();
    List<UUID> snapshots =
        context.getWorkingMap().get(DatasetWorkingMapKeys.SNAPSHOT_IDS, new TypeReference<>() {});
    Objects.requireNonNull(snapshots)
        .forEach(
            snapshotId -> {
              List<String> authDomains = snapshotService.retrieveAuthDomains(snapshotId, userReq);
              if (!authDomains.isEmpty()) {
                snapshotsWithAuthDomains.add(snapshotId);
              }
            });
    if (!snapshotsWithAuthDomains.isEmpty()) {
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL,
          new BadRequestException(
              "The following snapshots have auth domains: "
                  + snapshotsWithAuthDomains
                  + ". Please delete any snapshots with auth domains before setting inherit steward."));
    }

    return StepResult.getStepResultSuccess();
  }
}
