package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DeleteSnapshotDeleteSamGroupStep extends DefaultUndoStep {
  private final IamService iamService;

  private final UUID snapshotId;

  /**
   * On Snapshot Create byRequestId, we generate a Sam Group and set it as an auth domain On delete
   * of the Snapshot, we need to delete this Sam group that we created We do not want to delete any
   * other auth domain groups that may have been added to the snapshot
   *
   * @param iamService
   * @param snapshotId
   */
  public DeleteSnapshotDeleteSamGroupStep(IamService iamService, UUID snapshotId) {
    this.iamService = iamService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    List<String> authDomains =
        context
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_AUTH_DOMAIN_GROUPS, new TypeReference<>() {});
    // Only delete the Sam group if it matches the expected naming pattern
    var expectedName = IamService.constructSamGroupName(snapshotId.toString());
    if (Objects.nonNull(authDomains) && authDomains.contains(expectedName)) {
      try {
        // Only delete the group that we created as a part of snapshot create
        iamService.deleteGroup(expectedName);
      } catch (IamNotFoundException ex) {
        // if group does not exist, nothing to delete
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
