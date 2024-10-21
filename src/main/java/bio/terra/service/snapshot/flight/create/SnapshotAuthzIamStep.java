package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.flight.duos.SnapshotDuosFlightUtils;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotAuthzIamStep implements Step {
  private final IamService sam;
  private final SnapshotService snapshotService;
  private final SnapshotRequestModel snapshotRequestModel;
  private final AuthenticatedUserRequest userReq;
  private final UUID snapshotId;
  private static final Logger logger = LoggerFactory.getLogger(SnapshotAuthzIamStep.class);

  public SnapshotAuthzIamStep(
      IamService sam,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotRequestModel,
      AuthenticatedUserRequest userReq,
      UUID snapshotId) {
    this.sam = sam;
    this.snapshotService = snapshotService;
    this.snapshotRequestModel = snapshotRequestModel;
    this.userReq = userReq;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    SnapshotRequestModelPolicies derivedPolicies = sam.deriveSnapshotPolicies(snapshotRequestModel);
    if (snapshotRequestModel.getDuosId() != null) {
      DuosFirecloudGroupModel duosFirecloudGroup =
          SnapshotDuosFlightUtils.getFirecloudGroup(context);
      derivedPolicies.addReadersItem(duosFirecloudGroup.getFirecloudGroupEmail());
    }
    if (snapshotRequestModel.getContents().get(0).getMode()
        == SnapshotRequestContentsModel.ModeEnum.BYREQUESTID) {
      var snapshotFirecloudGroupEmail =
          workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, String.class);
      derivedPolicies.addReadersItem(snapshotFirecloudGroupEmail);
    }
    Map<IamRole, String> policies =
        sam.createSnapshotResource(userReq, snapshotId, derivedPolicies);
    workingMap.put(SnapshotWorkingMapKeys.POLICY_MAP, policies);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    try {
      sam.deleteSnapshotResource(userReq, snapshotId);
      // We do not need to remove the ACL from the files or BigQuery. It disappears
      // when SAM deletes the ACL. How 'bout that!
    } catch (UnauthorizedException ex) {
      // suppress exception
      logger.error("NEEDS CLEANUP: delete sam resource for snapshot " + snapshotId.toString());
      logger.warn(ex.getMessage());
    } catch (NotFoundException ex) {
      // suppress exception
      logger.warn("Snapshot resource wasn't found to delete", ex);
    }
    return StepResult.getStepResultSuccess();
  }
}
