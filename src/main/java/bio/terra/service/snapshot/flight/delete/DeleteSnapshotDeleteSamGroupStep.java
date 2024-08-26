package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.exception.NotFoundException;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshotbuilder.SnapshotAccessRequestModel;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
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
    // The request will only exist if the snapshot was created with byRequestId mode
    // If it exists, the request will have the sam group name to be deleted for this snapshot
    SnapshotAccessRequestModel request = null;
    try {
      request = snapshotRequestDao.getByCreatedSnapshotId(snapshotId);
    } catch (NotFoundException ex) {
      // If the request does not exist, nothing to delete
    }

    if (request != null) {
      var samGroupName = request.samGroupName();
      try {
        iamService.deleteGroup(samGroupName);
      } catch (IamNotFoundException ex) {
        // If group does not exist, nothing to delete
      } catch (Exception ex) {
        // If there is some other error, log and continue
        logger.error("Error deleting Sam group: {}", samGroupName, ex);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}
