package bio.terra.service.snapshot.flight.delete;

import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshotbuilder.SnapshotAccessRequestModel;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteSnapshotDeleteSamGroupStep extends DefaultUndoStep {
  private static Logger logger = LoggerFactory.getLogger(DeleteSnapshotDeleteSamGroupStep.class);
  private final IamService iamService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotId;

  /**
   * On Snapshot Create byRequestId, we generate a Sam Group and set it as an auth domain. On
   * snapshot delete, we need to delete this Sam group. We do not want to delete any other auth
   * domain groups that may have been added to the snapshot.
   *
   * @param iamService
   * @param snapshotId
   */
  public DeleteSnapshotDeleteSamGroupStep(
      IamService iamService, SnapshotRequestDao snapshotRequestDao, UUID snapshotId) {
    this.iamService = iamService;
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    // Only delete the Sam group if it matches the expected name
    SnapshotAccessRequestModel request = snapshotRequestDao.getByCreatedSnapshotId(snapshotId);
    if (Objects.nonNull(request)) {
      var expectedName = request.samGroupName();
      try {
        iamService.deleteGroup(expectedName);
      } catch (IamNotFoundException ex) {
        // if group does not exist, nothing to delete)
      } catch (Exception ex) {
        logger.error("Error deleting Sam group: {}", expectedName, ex);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
