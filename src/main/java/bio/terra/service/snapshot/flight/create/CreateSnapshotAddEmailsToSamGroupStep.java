package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotAddEmailsToSamGroupStep extends DefaultUndoStep {
  private final AuthenticatedUserRequest userRequest;
  private final IamService iamService;
  private final SnapshotRequestDao snapshotRequestDao;
  private final UUID snapshotRequestId;

  /**
   * For Snapshot byRequestId, add two emails to the SAM group: (1) Snapshot Creator (The email
   * associated with the userRequest) and (2) Snapshot Request Creator (The email in the createdBy
   * field on the snapshot request)
   *
   * @param userRequest authenticated user request for the user that is creating the snapshot
   * @param iamService
   * @param snapshotRequestDao
   * @param snapshotRequestId id of the snapshot request, used together with the snapshotRequestDao
   *     to get the snapshot request, which contains the createdBy field
   */
  public CreateSnapshotAddEmailsToSamGroupStep(
      AuthenticatedUserRequest userRequest,
      IamService iamService,
      SnapshotRequestDao snapshotRequestDao,
      UUID snapshotRequestId) {
    this.userRequest = userRequest;
    this.iamService = iamService;
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotRequestId = snapshotRequestId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    String groupName =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class);
    List<String> emailsToAddToGroup =
        List.of(snapshotRequestDao.getById(snapshotRequestId).getCreatedBy());
    iamService.overwriteGroupPolicyEmailsIncludeRequestingUser(
        userRequest, groupName, IamRole.MEMBER.toString(), emailsToAddToGroup);
    return StepResult.getStepResultSuccess();
  }
}
